//! HTTP-level tests for the management API (`/api/admin/*`).
//!
//! These exercise the full production stack — `NormalizePath`, identity and
//! session middleware, and the per-scope `Protected` wrappers from `lib.rs` —
//! by logging in through `POST /api/admin/auth/login` and replaying the
//! session cookie, exactly as the admin UI does. They complement the signed
//! SQS endpoint tests in `crate::sqs::endpoint_tests`.

use std::collections::{HashMap, HashSet};

use actix_identity::{Identity, IdentityMiddleware};
use actix_session::SessionMiddleware;
use actix_web::{
    body::MessageBody,
    dev::{Service as ActixService, ServiceResponse},
    http::{header, Method, StatusCode},
    middleware::{NormalizePath, TrailingSlash},
    test,
    web::{self, Data},
    App,
};

use crate::{
    api,
    auth::{
        middleware::{authentication::Authentication, protected_route::Protected},
        session::SqliteSessionStore,
    },
    config::Config,
    kms::memory::InMemoryKeyManager,
    service::Service,
};

const ADMIN_EMAIL: &str = "admin@example.com";
const USER_EMAIL: &str = "user@example.com";
const PASSWORD: &str = "hunter2hunter2";

/// Spins up a Service backed by a throwaway on-disk SQLite database with an
/// admin and a regular user (neither granted any namespace permissions). The
/// admin is the root account `Service::connect_with` provisions from the
/// config. The returned `TempDir` must be kept alive for the duration of the
/// test.
async fn setup() -> (Data<Service>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let cfg: Config = serde_json::from_value(serde_json::json!({
        "db_path": db_path,
        "default_max_retries": 5,
        "root_email": ADMIN_EMAIL,
        "root_password": PASSWORD,
    }))
    .unwrap();

    let svc = Service::connect_with()
        .config(cfg)
        .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
        .call()
        .await
        .unwrap();

    svc.create_user(
        USER_EMAIL.try_into().unwrap(),
        PASSWORD.into(),
        Some(api::auth::Role::User),
        vec![],
    )
    .await
    .unwrap();

    (Data::new(svc), dir)
}

/// Builds the same admin app the server runs (sans CORS/tracing), with the
/// per-scope `Protected` wrappers mirroring `lib.rs`.
async fn init_app(
    data: Data<Service>,
) -> impl ActixService<
    actix_http::Request,
    Response = ServiceResponse<impl MessageBody>,
    Error = actix_web::Error,
> {
    let session_store = SqliteSessionStore::new(data.db().clone());
    let secret_key = actix_web::cookie::Key::generate();

    test::init_service(
        App::new()
            .wrap(NormalizePath::new(TrailingSlash::Trim))
            .wrap(Authentication)
            .wrap(IdentityMiddleware::default())
            .wrap(
                SessionMiddleware::builder(session_store, secret_key)
                    .cookie_secure(false)
                    .build(),
            )
            .app_data(data)
            .service(
                web::scope("/api").service(
                    web::scope("/admin")
                        .service(api::queue::service().wrap(Protected::authenticated()))
                        .service(api::data::service().wrap(Protected::authenticated()))
                        .service(api::tokens::service().wrap(Protected::authenticated()))
                        .service(api::namespace::service().wrap(Protected::admin_only()))
                        .service(api::admin::service().wrap(Protected::admin_only()))
                        .service(api::auth::service()),
                ),
            ),
    )
    .await
}

/// Sends a request with an optional session cookie and JSON body, returning
/// (status, parsed JSON body). Middleware rejections (e.g. a missing session)
/// surface as service-level errors rather than responses, so convert those to
/// the response actix would send on the wire.
async fn call<S, B>(
    app: &S,
    method: Method,
    uri: &str,
    cookie: Option<&str>,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let mut req = test::TestRequest::default().method(method).uri(uri);
    if let Some(cookie) = cookie {
        req = req.insert_header((header::COOKIE, cookie));
    }
    if let Some(body) = body {
        req = req.set_json(body);
    }

    match test::try_call_service(app, req.to_request()).await {
        Ok(resp) => {
            let status = resp.status();
            let bytes = actix_web::body::to_bytes(resp.into_body())
                .await
                .unwrap_or_default();
            let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
            (status, json)
        }
        Err(err) => {
            let resp = err.error_response();
            let status = resp.status();
            let bytes = actix_web::body::to_bytes(resp.into_body())
                .await
                .unwrap_or_default();
            let json = serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| serde_json::Value::String(format!("{err}")));
            (status, json)
        }
    }
}

/// Logs in and returns the session cookie to replay on subsequent requests.
async fn login<S, B>(app: &S, email: &str, password: &str) -> String
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let req = test::TestRequest::post()
        .uri("/api/admin/auth/login")
        .set_json(serde_json::json!({ "email": email, "password": password }))
        .to_request();

    let resp = test::call_service(app, req).await;
    assert_eq!(resp.status(), StatusCode::OK, "login failed for {email}");

    let cookies: Vec<String> = resp
        .headers()
        .get_all(header::SET_COOKIE)
        .map(|v| {
            v.to_str()
                .unwrap()
                .split(';')
                .next()
                .unwrap()
                .to_string()
        })
        .collect();
    assert!(!cookies.is_empty(), "login should set a session cookie");

    cookies.join("; ")
}

// ---------------------------------------------------------------------------
// Auth: /api/admin/auth
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn login_returns_the_users_email_and_role() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/auth/login",
        None,
        Some(serde_json::json!({ "email": ADMIN_EMAIL, "password": PASSWORD })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "login failed: {body}");
    assert_eq!(body["email"], ADMIN_EMAIL);
    assert_eq!(body["role"], "admin");
}

#[actix_web::test]
async fn login_with_a_wrong_password_is_rejected() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/login",
        None,
        Some(serde_json::json!({ "email": ADMIN_EMAIL, "password": "wrong-password" })),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn login_with_an_unknown_user_is_rejected() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/login",
        None,
        Some(serde_json::json!({ "email": "nobody@example.com", "password": PASSWORD })),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn verify_reports_the_session_and_rejects_the_anonymous() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, _) = call(&app, Method::POST, "/api/admin/auth/verify", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/auth/verify",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "verify failed: {body}");
    assert_eq!(body["email"], ADMIN_EMAIL);
    assert_eq!(body["role"], "admin");
}

#[actix_web::test]
async fn logout_invalidates_the_session() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/logout",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/verify",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "session should be gone");
}

// ---------------------------------------------------------------------------
// Authorization boundaries
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn protected_scopes_reject_anonymous_requests() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    for uri in [
        "/api/admin/queue",
        "/api/admin/stats/queue",
        "/api/admin/tokens",
        "/api/admin/ns",
        "/api/admin/users",
    ] {
        let (status, _) = call(&app, Method::GET, uri, None, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri} allowed anonymous access");
    }
}

#[actix_web::test]
async fn admin_only_scopes_reject_regular_users() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;

    let cookie = login(&app, USER_EMAIL, PASSWORD).await;

    for uri in ["/api/admin/ns", "/api/admin/users"] {
        let (status, _) = call(&app, Method::GET, uri, Some(&cookie), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri} allowed a non-admin");
    }

    // But the same session can use the authenticated-only scopes.
    let (status, _) = call(&app, Method::GET, "/api/admin/queue", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Namespaces: /api/admin/ns
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn namespace_create_list_delete_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let (status, body) = call(&app, Method::GET, "/api/admin/ns", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 0);

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/ns/demo",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace failed: {body}");
    assert!(body["id"].is_u64());

    let (status, body) = call(&app, Method::GET, "/api/admin/ns", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    let namespaces = body.as_array().unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0]["name"], "demo");
    assert_eq!(namespaces[0]["created_by"], ADMIN_EMAIL);

    let (status, _) = call(
        &app,
        Method::DELETE,
        "/api/admin/ns/demo",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, "/api/admin/ns", Some(&cookie), None).await;
    assert_eq!(body.as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// Queues: /api/admin/queue
// ---------------------------------------------------------------------------

/// Creates `demo/jobs` through the management API and returns the session.
async fn setup_queue<S, B>(app: &S) -> String
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let cookie = login(app, ADMIN_EMAIL, PASSWORD).await;

    let (status, body) = call(
        app,
        Method::POST,
        "/api/admin/ns/demo",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create namespace failed: {body}");

    let (status, body) = call(
        app,
        Method::POST,
        "/api/admin/queue/demo/jobs",
        Some(&cookie),
        Some(serde_json::json!({ "attributes": {}, "tags": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create queue failed: {body}");

    cookie
}

#[actix_web::test]
async fn queue_create_list_and_delete_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    let (status, body) = call(&app, Method::GET, "/api/admin/queue", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["name"], "jobs");
    assert_eq!(queues[0]["ns"], "demo");

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["queues"].as_array().unwrap().len(), 1);

    let (status, _) = call(
        &app,
        Method::DELETE,
        "/api/admin/queue/demo/jobs",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, "/api/admin/queue", Some(&cookie), None).await;
    assert_eq!(body["queues"].as_array().unwrap().len(), 0);
}

#[actix_web::test]
async fn queue_stats_count_pending_messages() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let cookie = setup_queue(&app).await;

    // Seed two messages directly through the service layer; the management
    // API has no send endpoint.
    let queue_id = data
        .get_queue_id("demo", "jobs", data.db())
        .await
        .unwrap()
        .unwrap();
    for body in ["one", "two"] {
        let req: crate::types::send_message::SendMessageRequest =
            serde_json::from_value(serde_json::json!({
                "QueueUrl": "http://localhost:8080/api/sqs/demo/jobs",
                "MessageBody": body,
            }))
            .unwrap();
        data.sqs_send(queue_id, req, None).await.unwrap();
    }

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "queue stats failed: {body}");
    // `QueueStatistics` flattens the queue fields into the top level.
    assert_eq!(body["name"], "jobs");
    assert_eq!(body["ns"], "demo");
    assert_eq!(body["pending"], 2);
}

#[actix_web::test]
async fn queue_messages_lists_message_details() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let cookie = setup_queue(&app).await;

    let queue_id = data
        .get_queue_id("demo", "jobs", data.db())
        .await
        .unwrap()
        .unwrap();
    let req: crate::types::send_message::SendMessageRequest =
        serde_json::from_value(serde_json::json!({
            "QueueUrl": "http://localhost:8080/api/sqs/demo/jobs",
            "MessageBody": "inspect-me",
        }))
        .unwrap();
    data.sqs_send(queue_id, req, None).await.unwrap();

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list messages failed: {body}");
    let messages = body.as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["body"], "inspect-me");
    assert_eq!(messages[0]["status"], "pending");
}

#[actix_web::test]
async fn queue_config_get_and_update_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    // Default comes from the service config (default_max_retries = 5).
    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/config",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get config failed: {body}");
    assert_eq!(body["max_retries"], 5);
    assert!(body["dead_letter_queue"].is_null());

    // Point the DLQ at a second queue and lower the retry limit.
    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/dead-letters",
        Some(&cookie),
        Some(serde_json::json!({ "attributes": {}, "tags": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/jobs/config",
        Some(&cookie),
        Some(serde_json::json!({ "max_retries": 3, "dead_letter_queue": "dead-letters" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "update config failed: {body}");

    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/config",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["max_retries"], 3);
    assert!(body["dead_letter_queue"].is_u64());

    // A nonexistent DLQ is rejected.
    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/jobs/config",
        Some(&cookie),
        Some(serde_json::json!({ "max_retries": 3, "dead_letter_queue": "does-not-exist" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn queue_messages_require_namespace_access() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let _admin_cookie = setup_queue(&app).await;

    // The regular user has no permission on the `demo` namespace.
    let cookie = login(&app, USER_EMAIL, PASSWORD).await;
    let (status, _) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Statistics: /api/admin/stats
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn stats_report_per_queue_and_per_namespace() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let cookie = setup_queue(&app).await;

    let queue_id = data
        .get_queue_id("demo", "jobs", data.db())
        .await
        .unwrap()
        .unwrap();
    let req: crate::types::send_message::SendMessageRequest =
        serde_json::from_value(serde_json::json!({
            "QueueUrl": "http://localhost:8080/api/sqs/demo/jobs",
            "MessageBody": "stat-me",
        }))
        .unwrap();
    data.sqs_send(queue_id, req, None).await.unwrap();

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/stats/queue",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "queue stats failed: {body}");
    let stats = body.as_object().unwrap();
    assert_eq!(stats.len(), 1);
    // `QueueStatistics` flattens the queue fields into the top level.
    assert_eq!(stats.values().next().unwrap()["name"], "jobs");

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/stats/ns",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "namespace stats failed: {body}");
    let stats = body.as_array().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0]["name"], "demo");
}

// ---------------------------------------------------------------------------
// API keys: /api/admin/tokens
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn token_create_list_delete_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/tokens",
        Some(&cookie),
        Some(serde_json::json!({ "name": "ci-key", "namespace": "demo" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create token failed: {body}");
    assert_eq!(body["name"], "ci-key");
    assert_eq!(body["namespace"], "demo");
    assert!(!body["access_key"].as_str().unwrap().is_empty());
    assert!(!body["secret_key"].as_str().unwrap().is_empty());

    let (status, body) = call(&app, Method::GET, "/api/admin/tokens", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    let keys = body.as_array().unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["name"], "ci-key");
    assert_eq!(keys[0]["namespace"], "demo");

    let (status, _) = call(
        &app,
        Method::DELETE,
        "/api/admin/tokens",
        Some(&cookie),
        Some(serde_json::json!({ "name": "ci-key" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, "/api/admin/tokens", Some(&cookie), None).await;
    assert_eq!(body.as_array().unwrap().len(), 0);
}

#[actix_web::test]
async fn deleting_a_missing_token_is_not_found() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let (status, _) = call(
        &app,
        Method::DELETE,
        "/api/admin/tokens",
        Some(&cookie),
        Some(serde_json::json!({ "name": "no-such-key" })),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Users & permissions: /api/admin/users
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn user_create_list_delete_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/users",
        Some(&cookie),
        Some(serde_json::json!({
            "email": "carol@example.com",
            "password": PASSWORD,
            "role": "user",
            "namespaces": [],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create user failed: {body}");

    let (status, body) = call(&app, Method::GET, "/api/admin/users", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    let emails: HashSet<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["email"].as_str().unwrap())
        .collect();
    assert_eq!(
        emails,
        HashSet::from([ADMIN_EMAIL, USER_EMAIL, "carol@example.com"])
    );

    // The new user can actually log in.
    login(&app, "carol@example.com", PASSWORD).await;

    let (status, body) = call(
        &app,
        Method::DELETE,
        "/api/admin/users",
        Some(&cookie),
        Some(serde_json::json!({ "email": "carol@example.com" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete user failed: {body}");

    let (_, body) = call(&app, Method::GET, "/api/admin/users", Some(&cookie), None).await;
    assert_eq!(body.as_array().unwrap().len(), 2);
}

#[actix_web::test]
async fn user_permissions_grant_replace_and_revoke_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let admin = || Identity::mock(ADMIN_EMAIL.to_string());
    data.create_namespace("demo", admin()).await.unwrap();
    data.create_namespace("staging", admin()).await.unwrap();

    let permissions_uri = format!("/api/admin/users/{USER_EMAIL}/permissions");

    let (status, body) = call(&app, Method::GET, &permissions_uri, Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().unwrap().len(), 0);

    // Grant adds to the existing set.
    let (status, _) = call(
        &app,
        Method::PUT,
        &permissions_uri,
        Some(&cookie),
        Some(serde_json::json!(["demo"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, &permissions_uri, Some(&cookie), None).await;
    assert_eq!(body, serde_json::json!(["demo"]));

    // Update replaces the whole set.
    let (status, _) = call(
        &app,
        Method::POST,
        &permissions_uri,
        Some(&cookie),
        Some(serde_json::json!(["staging"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, &permissions_uri, Some(&cookie), None).await;
    assert_eq!(body, serde_json::json!(["staging"]));

    // The grant is what gates namespace-scoped endpoints.
    data.create_queue("staging", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();
    let user_cookie = login(&app, USER_EMAIL, PASSWORD).await;
    let (status, _) = call(
        &app,
        Method::GET,
        "/api/admin/queue/staging/q/messages",
        Some(&user_cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "granted namespace should be accessible");

    // Revoke removes it again.
    let (status, _) = call(
        &app,
        Method::DELETE,
        &permissions_uri,
        Some(&cookie),
        Some(serde_json::json!(["staging"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, &permissions_uri, Some(&cookie), None).await;
    assert_eq!(body.as_array().unwrap().len(), 0);

    let (status, _) = call(
        &app,
        Method::GET,
        "/api/admin/queue/staging/q/messages",
        Some(&user_cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "revoked namespace should be gone");
}

#[actix_web::test]
async fn user_role_get_and_set_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let role_uri = format!("/api/admin/users/{USER_EMAIL}/role");

    let (status, body) = call(&app, Method::GET, &role_uri, Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!("user"));

    let (status, _) = call(
        &app,
        Method::POST,
        &role_uri,
        Some(&cookie),
        Some(serde_json::json!({ "role": "admin" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = call(&app, Method::GET, &role_uri, Some(&cookie), None).await;
    assert_eq!(body, serde_json::json!("admin"));

    // The promotion takes effect: the user can now reach admin-only scopes.
    let user_cookie = login(&app, USER_EMAIL, PASSWORD).await;
    let (status, _) = call(&app, Method::GET, "/api/admin/users", Some(&user_cookie), None).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn queue_panel_message_management_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    // Send a message (with an attribute) from the management plane.
    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        Some(serde_json::json!({
            "body": "from the admin UI",
            "attributes": {
                "Origin": { "DataType": "String", "StringValue": "panel" }
            }
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "send failed: {body}");
    let message_id = body["MessageId"].as_str().unwrap().to_string();

    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body[0]["body"], "from the admin UI");
    assert_eq!(body[0]["status"], "pending");
    assert_eq!(body[0]["message_attributes"]["Origin"], "panel");

    // The send stamped the queue-received time (SentTimestamp equivalent).
    let received_at = body[0]["received_at"].as_u64().expect("received_at set");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        now.abs_diff(received_at) < 60,
        "received_at {received_at} should be about now ({now})"
    );
    // Never delivered yet.
    assert!(body[0]["delivered_at"].is_null());

    // Force it to failed: no longer deliverable.
    let (status, body) = call(
        &app,
        Method::POST,
        &format!("/api/admin/queue/demo/jobs/messages/{message_id}/status"),
        Some(&cookie),
        Some(serde_json::json!({ "status": "failed" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set failed status failed: {body}");
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body[0]["status"], "failed");

    // And back to pending: redeliverable with a clean retry budget.
    let (status, _) = call(
        &app,
        Method::POST,
        &format!("/api/admin/queue/demo/jobs/messages/{message_id}/status"),
        Some(&cookie),
        Some(serde_json::json!({ "status": "pending" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body[0]["status"], "pending");
    assert_eq!(body[0]["tries"], 0);

    // `delivered` is not a settable target.
    let (status, _) = call(
        &app,
        Method::POST,
        &format!("/api/admin/queue/demo/jobs/messages/{message_id}/status"),
        Some(&cookie),
        Some(serde_json::json!({ "status": "delivered" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Delete the message by ID.
    let (status, body) = call(
        &app,
        Method::DELETE,
        &format!("/api/admin/queue/demo/jobs/messages/{message_id}"),
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete failed: {body}");
    let (status, _) = call(
        &app,
        Method::DELETE,
        &format!("/api/admin/queue/demo/jobs/messages/{message_id}"),
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "double delete should 404");

    // Purge: enqueue a few and wipe them all.
    for i in 0..3 {
        let (status, _) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/messages",
            Some(&cookie),
            Some(serde_json::json!({ "body": format!("purge-{i}") })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/jobs/purge",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body.as_array().map(|a| a.len()), Some(0));
}

#[actix_web::test]
async fn queue_attributes_get_and_set_roundtrip() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    // Fresh queue: no attributes set.
    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/attributes",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "get attributes failed: {body}");
    assert_eq!(body, serde_json::json!({}));

    // Set the standard attributes through the admin API (SQS wire shape).
    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/jobs/attributes",
        Some(&cookie),
        Some(serde_json::json!({
            "VisibilityTimeout": "45",
            "DelaySeconds": "2",
            "MaximumMessageSize": "2048",
            "MessageRetentionPeriod": "3600",
            "ReceiveMessageWaitTimeSeconds": "1"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "set attributes failed: {body}");

    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/attributes",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["VisibilityTimeout"], "45");
    assert_eq!(body["DelaySeconds"], "2");
    assert_eq!(body["MaximumMessageSize"], "2048");
    assert_eq!(body["MessageRetentionPeriod"], "3600");
    assert_eq!(body["ReceiveMessageWaitTimeSeconds"], "1");
}
