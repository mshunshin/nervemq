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
        credential::KeyAccess,
        middleware::{authentication::Authentication, protected_route::Protected},
        session::SqliteSessionStore,
    },
    config::Config,
    kms::memory::InMemoryKeyManager,
    service::Service,
};

pub(super) const ADMIN_EMAIL: &str = "admin@example.com";
pub(super) const USER_EMAIL: &str = "user@example.com";
pub(super) const PASSWORD: &str = "hunter2hunter2";

/// Spins up a Service backed by a throwaway on-disk SQLite database with an
/// admin and a regular user (neither granted any namespace permissions). The
/// admin is the root account `Service::connect_with` provisions from the
/// config. The returned `TempDir` must be kept alive for the duration of the
/// test.
pub(super) async fn setup() -> (Data<Service>, tempfile::TempDir) {
    setup_with_host(None).await
}

/// As [`setup`], with `NERVEMQ_HOST` set when `host` is given.
async fn setup_with_host(host: Option<&str>) -> (Data<Service>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let mut cfg = serde_json::json!({
        "db_path": db_path,
        "default_max_retries": 5,
        "root_email": ADMIN_EMAIL,
        "root_password": PASSWORD,
    });
    if let Some(host) = host {
        cfg["host"] = host.into();
    }
    let cfg: Config = serde_json::from_value(cfg).unwrap();

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
pub(super) async fn init_app(
    data: Data<Service>,
) -> impl ActixService<
    actix_http::Request,
    Response = ServiceResponse<impl MessageBody>,
    Error = actix_web::Error,
> {
    // Sessions live in their own database in production; an in-memory
    // store keeps the same separation here.
    let session_store = SqliteSessionStore::in_memory().await;
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
            .wrap(actix_web::middleware::from_fn(
                crate::auth::middleware::same_origin::refuse_cross_origin_cookie_writes,
            ))
            .wrap(actix_web::middleware::from_fn(
                crate::auth::middleware::host::refuse_unknown_hosts,
            ))
            .app_data(data)
            // As in lib.rs: lenient app-wide (for SQS clients), strict for
            // the admin API.
            .app_data(web::JsonConfig::default().content_type_required(false))
            .service(
                web::scope("/api").service(api::health::service()).service(
                    web::scope("/admin")
                        .app_data(web::JsonConfig::default())
                        .service(api::queue::service().wrap(Protected::authenticated()))
                        .service(api::data::service().wrap(Protected::authenticated()))
                        .service(api::tokens::service().wrap(Protected::authenticated()))
                        .service(api::namespace::service().wrap(Protected::authenticated()))
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
pub(super) async fn call<S, B>(
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
pub(super) async fn login<S, B>(app: &S, email: &str, password: &str) -> String
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

    let (status, _) = call(&app, Method::GET, "/api/admin/users", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "/api/admin/users allowed a non-admin");

    // But the same session can use the authenticated-only scopes. The
    // namespace scope is one: members list their namespaces there, and each
    // route enforces its own rule (see the ownership tests).
    for uri in ["/api/admin/queue", "/api/admin/ns"] {
        let (status, _) = call(&app, Method::GET, uri, Some(&cookie), None).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
    }

    // Creating a namespace is still for admins only.
    let (status, _) = call(&app, Method::POST, "/api/admin/ns/mine", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
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

/// Unlike SQS CreateQueue, the admin API reports a taken name as a conflict
/// even when the attributes match, rather than silently succeeding (or, as it
/// once did, failing with a 500).
#[actix_web::test]
async fn creating_an_existing_queue_is_a_conflict() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    for attributes in [
        serde_json::json!({}),
        serde_json::json!({ "VisibilityTimeout": "60" }),
    ] {
        let (status, body) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs",
            Some(&cookie),
            Some(serde_json::json!({ "attributes": attributes, "tags": {} })),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{attributes}: {body}");
    }
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
        data.sqs_send(queue_id, req, None, None).await.unwrap();
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
    data.sqs_send(queue_id, req, None, None).await.unwrap();

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "list messages failed: {body}");
    assert_eq!(body["total"], 1);
    let messages = body["messages"].as_array().unwrap();
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

/// Every receive counts against `max_retries`, so 0 would stop the queue
/// delivering anything, and so would a value past `i64::MAX`, which SQLite
/// stores as a negative number. Both are refused, and the queue keeps its
/// limit.
#[actix_web::test]
async fn queue_config_refuses_a_max_retries_that_stops_delivery() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    for max_retries in [0, i64::MAX as u64 + 1, u64::MAX] {
        let (status, body) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/config",
            Some(&cookie),
            Some(serde_json::json!({ "max_retries": max_retries, "dead_letter_queue": null })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "max_retries {max_retries}: {body}");
    }

    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/config",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["max_retries"], 5);

    // The bounds themselves are allowed.
    for max_retries in [1, i64::MAX as u64] {
        let (status, body) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/config",
            Some(&cookie),
            Some(serde_json::json!({ "max_retries": max_retries, "dead_letter_queue": null })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "max_retries {max_retries}: {body}");

        let (_, body) = call(
            &app,
            Method::GET,
            "/api/admin/queue/demo/jobs/config",
            Some(&cookie),
            None,
        )
        .await;
        assert_eq!(body["max_retries"], max_retries);
    }
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
    data.sqs_send(queue_id, req, None, None).await.unwrap();

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
    assert_eq!(body["total"], 1);
    let first = &body["messages"][0];
    assert_eq!(first["id"], message_id.as_str(), "the list shows the MessageId");
    assert_eq!(first["body"], "from the admin UI");
    assert_eq!(first["status"], "pending");
    assert_eq!(first["message_attributes"]["Origin"], "panel");

    // The send stamped the queue-received time (SentTimestamp equivalent).
    let received_at = first["received_at"].as_u64().expect("received_at set");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        now.abs_diff(received_at) < 60,
        "received_at {received_at} should be about now ({now})"
    );
    // Never delivered yet.
    assert!(first["delivered_at"].is_null());

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
    assert_eq!(body["messages"][0]["status"], "failed");

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
    assert_eq!(body["messages"][0]["status"], "pending");
    assert_eq!(body["messages"][0]["tries"], 0);

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
    assert_eq!(body["total"], 0);
    assert_eq!(body["messages"].as_array().map(|a| a.len()), Some(0));
}

#[actix_web::test]
async fn out_of_range_queue_attributes_are_bad_requests() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/jobs/attributes",
        Some(&cookie),
        Some(serde_json::json!({ "VisibilityTimeout": "43201" })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/queue/demo/other",
        Some(&cookie),
        Some(serde_json::json!({ "attributes": { "DelaySeconds": "901" }, "tags": {} })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
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

#[actix_web::test]
async fn message_list_paginates_with_limit_and_offset() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    for i in 0..5 {
        let (status, _) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/messages",
            Some(&cookie),
            Some(serde_json::json!({ "body": format!("page-{i}") })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // First page: two oldest messages, total reflects the whole queue.
    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?limit=2&offset=0",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "paginated list failed: {body}");
    assert_eq!(body["total"], 5);
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["body"], "page-0");
    assert_eq!(messages[1]["body"], "page-1");

    // Middle page.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?limit=2&offset=2",
        Some(&cookie),
        None,
    )
    .await;
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["body"], "page-2");

    // Last, short page.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?limit=2&offset=4",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["total"], 5);
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["body"], "page-4");

    // Beyond the end: empty page, total intact.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?limit=2&offset=10",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["total"], 5);
    assert_eq!(body["messages"].as_array().map(|a| a.len()), Some(0));

    // No params: server defaults (limit 50) return everything here.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["messages"].as_array().map(|a| a.len()), Some(5));
}

#[actix_web::test]
async fn message_list_sorts_by_column() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    // Bodies deliberately out of alphabetical order relative to send order.
    for body in ["cherry", "apple", "banana"] {
        let (status, _) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/messages",
            Some(&cookie),
            Some(serde_json::json!({ "body": body })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    let bodies = |body: &serde_json::Value| -> Vec<String> {
        body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["body"].as_str().unwrap().to_string())
            .collect()
    };

    // Default: send (id) order.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(bodies(&body), ["cherry", "apple", "banana"]);

    // Sort by body, both directions.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?sort=body&order=asc",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(bodies(&body), ["apple", "banana", "cherry"]);

    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?sort=body&order=desc",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(bodies(&body), ["cherry", "banana", "apple"]);

    // Sorting composes with pagination: page 2 of the by-body order.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?sort=body&order=asc&limit=2&offset=2",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["total"], 3);
    assert_eq!(bodies(&body), ["cherry"]);

    // id desc reverses send order.
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?sort=id&order=desc",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(bodies(&body), ["banana", "apple", "cherry"]);

    // Unknown sort keys are rejected, not silently ignored.
    let (status, _) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages?sort=evil",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn clear_failed_messages_removes_only_exhausted_messages() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    // Clearing a queue with nothing failed is a no-op, not an error.
    let (status, body) = call(
        &app,
        Method::DELETE,
        "/api/admin/queue/demo/jobs/messages/failed",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["deleted"], 0);

    // Three messages; mark the first two failed via the admin status API.
    let mut ids = Vec::new();
    for i in 0..3 {
        let (status, body) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/messages",
            Some(&cookie),
            Some(serde_json::json!({ "body": format!("m-{i}") })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        ids.push(body["MessageId"].as_str().unwrap().to_string());
    }
    for id in &ids[..2] {
        let (status, _) = call(
            &app,
            Method::POST,
            &format!("/api/admin/queue/demo/jobs/messages/{id}/status"),
            Some(&cookie),
            Some(serde_json::json!({ "status": "failed" })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, body) = call(
        &app,
        Method::DELETE,
        "/api/admin/queue/demo/jobs/messages/failed",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["deleted"], 2);

    // Only the healthy message survives...
    let (_, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(body["total"], 2 + 1 - 2);
    assert_eq!(body["messages"][0]["body"], "m-2");

    // ...and the literal `failed` segment did not shadow delete-by-id: the
    // survivor is still individually deletable.
    let (status, _) = call(
        &app,
        Method::DELETE,
        &format!("/api/admin/queue/demo/jobs/messages/{}", ids[2]),
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Ownership, queue management and user controls
// ---------------------------------------------------------------------------

/// Admin creates namespace `team` with queue `jobs` and grants `USER_EMAIL`
/// plain membership. Returns the admin's and the user's session cookies.
async fn team_with_member<S, B>(app: &S, data: &Data<Service>) -> (String, String)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let admin = login(app, ADMIN_EMAIL, PASSWORD).await;
    let (status, _) = call(app, Method::POST, "/api/admin/ns/team", Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    data.create_queue(
        "team",
        "jobs",
        Default::default(),
        HashMap::new(),
        Identity::mock(ADMIN_EMAIL.to_string()),
    )
    .await
    .unwrap();
    let (status, _) = call(
        app,
        Method::PUT,
        &format!("/api/admin/users/{USER_EMAIL}/permissions"),
        Some(&admin),
        Some(serde_json::json!(["team"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let user = login(app, USER_EMAIL, PASSWORD).await;
    (admin, user)
}

#[actix_web::test]
async fn owners_and_admins_delete_namespaces_members_cannot() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;

    let (status, _) = call(&app, Method::DELETE, "/api/admin/ns/team", Some(&user), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "a plain member deleted the namespace");

    let owner_uri = format!("/api/admin/ns/team/owners/{USER_EMAIL}");
    let (status, _) = call(&app, Method::PUT, &owner_uri, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, Method::DELETE, "/api/admin/ns/team", Some(&user), None).await;
    assert_eq!(status, StatusCode::OK, "an owner could not delete the namespace");

    // An admin needs neither ownership nor a grant.
    data.create_namespace("other", Identity::mock(ADMIN_EMAIL.to_string()))
        .await
        .unwrap();
    sqlx::query("DELETE FROM user_permissions").execute(data.db()).await.unwrap();
    let (status, _) = call(&app, Method::DELETE, "/api/admin/ns/other", Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn replacing_a_users_namespaces_keeps_their_ownership() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;
    data.create_namespace("extra", Identity::mock(ADMIN_EMAIL.to_string()))
        .await
        .unwrap();

    let owner_uri = format!("/api/admin/ns/team/owners/{USER_EMAIL}");
    let (status, _) = call(&app, Method::PUT, &owner_uri, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);

    // What the UI's namespace editor sends: the whole new set.
    let (status, _) = call(
        &app,
        Method::POST,
        &format!("/api/admin/users/{USER_EMAIL}/permissions"),
        Some(&admin),
        Some(serde_json::json!(["team", "extra"])),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, members) = call(&app, Method::GET, "/api/admin/ns/team/members", Some(&admin), None).await;
    assert!(
        members
            .as_array()
            .unwrap()
            .contains(&serde_json::json!({"email": USER_EMAIL, "owner": true})),
        "ownership was stripped: {members}"
    );
    let (status, _) = call(&app, Method::DELETE, "/api/admin/ns/team", Some(&user), None).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn only_admins_create_namespaces_or_change_owners() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;
    let owner_uri = format!("/api/admin/ns/team/owners/{USER_EMAIL}");
    let (status, _) = call(&app, Method::PUT, &owner_uri, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);

    // Even an owner cannot.
    let (status, _) = call(&app, Method::POST, "/api/admin/ns/mine", Some(&user), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let admin_owner_uri = format!("/api/admin/ns/team/owners/{ADMIN_EMAIL}");
    for method in [Method::PUT, Method::DELETE] {
        let (status, _) = call(&app, method.clone(), &admin_owner_uri, Some(&user), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method}");
    }

    // Losing ownership keeps membership.
    let (status, _) = call(&app, Method::DELETE, &owner_uri, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, namespaces) = call(&app, Method::GET, "/api/admin/stats/ns", Some(&user), None).await;
    assert_eq!(namespaces[0]["name"], "team");
    assert_eq!(namespaces[0]["can_manage"], false);
}

#[actix_web::test]
async fn namespace_stats_show_owners_and_who_can_manage() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;

    let (_, as_admin) = call(&app, Method::GET, "/api/admin/stats/ns", Some(&admin), None).await;
    assert_eq!(as_admin[0]["name"], "team");
    assert_eq!(as_admin[0]["created_by"], ADMIN_EMAIL);
    assert_eq!(as_admin[0]["owners"], serde_json::json!([ADMIN_EMAIL]));
    assert_eq!(as_admin[0]["can_manage"], true);
    assert_eq!(as_admin[0]["queue_count"], 1);

    let (_, as_user) = call(&app, Method::GET, "/api/admin/stats/ns", Some(&user), None).await;
    assert_eq!(as_user[0]["can_manage"], false);

    // Members cannot see who else is in the namespace.
    let (status, _) = call(&app, Method::GET, "/api/admin/ns/team/members", Some(&user), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
async fn members_send_messages_but_cannot_manage_queues_in_the_ui() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;

    for (method, uri, body) in [
        (Method::GET, "/api/admin/queue/team", None),
        (Method::GET, "/api/admin/queue/team/jobs", None),
        (Method::GET, "/api/admin/queue/team/jobs/attributes", None),
        (Method::GET, "/api/admin/queue/team/jobs/messages", None),
        (
            Method::POST,
            "/api/admin/queue/team/jobs/messages",
            Some(serde_json::json!({"body": "hi"})),
        ),
    ] {
        let (status, body) = call(&app, method.clone(), uri, Some(&user), body).await;
        assert_eq!(status, StatusCode::OK, "{method} {uri}: {body}");
    }

    for (method, uri, body) in [
        (
            Method::POST,
            "/api/admin/queue/team/new",
            Some(serde_json::json!({"attributes": {}, "tags": {}})),
        ),
        (Method::DELETE, "/api/admin/queue/team/jobs", None),
        (Method::POST, "/api/admin/queue/team/jobs/purge", None),
        (Method::POST, "/api/admin/queue/team/jobs/pause", None),
        (Method::POST, "/api/admin/queue/team/jobs/resume", None),
        (
            Method::POST,
            "/api/admin/queue/team/jobs/config",
            Some(serde_json::json!({"max_retries": 3, "dead_letter_queue": null})),
        ),
        (
            Method::POST,
            "/api/admin/queue/team/jobs/attributes",
            Some(serde_json::json!({"DelaySeconds": "1"})),
        ),
        (Method::DELETE, "/api/admin/queue/team/jobs/messages/failed", None),
        (Method::DELETE, "/api/admin/queue/team/jobs/messages/1", None),
        (
            Method::POST,
            "/api/admin/queue/team/jobs/messages/1/status",
            Some(serde_json::json!({"status": "failed"})),
        ),
    ] {
        let (status, body) = call(&app, method.clone(), uri, Some(&user), body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri} allowed a member: {body}");
    }

    // The admin can.
    let (status, _) = call(&app, Method::POST, "/api/admin/queue/team/jobs/purge", Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn pausing_a_queue_is_reported_until_it_is_resumed() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let cookie = setup_queue(&app).await;

    let paused_at = |body: &serde_json::Value| body["paused_at"].clone();

    let (_, body) = call(&app, Method::GET, "/api/admin/queue/demo/jobs", Some(&cookie), None).await;
    assert!(paused_at(&body).is_null(), "a new queue is running: {body}");

    let (status, body) = call(&app, Method::POST, "/api/admin/queue/demo/jobs/pause", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK, "pause failed: {body}");
    let (_, body) = call(&app, Method::GET, "/api/admin/queue/demo/jobs", Some(&cookie), None).await;
    let first = paused_at(&body);
    assert!(first.is_u64(), "statistics should report the pause: {body}");

    // Pausing again keeps the original time.
    sqlx::query("UPDATE queues SET paused_at = paused_at - 100")
        .execute(data.db())
        .await
        .unwrap();
    let (status, _) = call(&app, Method::POST, "/api/admin/queue/demo/jobs/pause", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = call(&app, Method::GET, "/api/admin/queue/demo/jobs", Some(&cookie), None).await;
    assert_eq!(paused_at(&body).as_u64(), first.as_u64().map(|t| t - 100));

    // Every listing carries it.
    let (_, body) = call(&app, Method::GET, "/api/admin/stats/queue", Some(&cookie), None).await;
    assert!(body["demo/jobs"]["paused_at"].is_u64(), "{body}");
    let (_, body) = call(&app, Method::GET, "/api/admin/queue/demo", Some(&cookie), None).await;
    assert!(body["queues"][0]["paused_at"].is_u64(), "{body}");
    let (_, body) = call(&app, Method::GET, "/api/admin/queue", Some(&cookie), None).await;
    assert!(body["queues"][0]["paused_at"].is_u64(), "{body}");

    let (status, body) = call(&app, Method::POST, "/api/admin/queue/demo/jobs/resume", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK, "resume failed: {body}");
    let (_, body) = call(&app, Method::GET, "/api/admin/queue/demo/jobs", Some(&cookie), None).await;
    assert!(paused_at(&body).is_null(), "resume should clear the pause: {body}");

    // Resuming a running queue is a no-op too.
    let (status, _) = call(&app, Method::POST, "/api/admin/queue/demo/jobs/resume", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);

    for uri in ["/api/admin/queue/demo/missing/pause", "/api/admin/queue/nope/jobs/pause"] {
        let (status, body) = call(&app, Method::POST, uri, Some(&cookie), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body}");
    }
}

#[actix_web::test]
async fn owners_pause_and_resume_their_queues() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;

    let owner_uri = format!("/api/admin/ns/team/owners/{USER_EMAIL}");
    let (status, _) = call(&app, Method::PUT, &owner_uri, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);

    for action in ["pause", "resume"] {
        let uri = format!("/api/admin/queue/team/jobs/{action}");
        let (status, body) = call(&app, Method::POST, &uri, Some(&user), None).await;
        assert_eq!(status, StatusCode::OK, "an owner could not {action}: {body}");
    }
}

/// A logged-in browser sends a "simple" request (a form or plain-text POST,
/// no preflight) with the session cookie even from another origin, as long
/// as that origin counts as the same site; CORS only hides the answer.
/// Cookie-authenticated writes are refused unless they come from the
/// server's own origin.
#[actix_web::test]
async fn cookie_writes_from_another_origin_are_refused() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;

    // The request as a browser would send it to http://mq.example.com.
    let status = |method: Method, uri: String, origin: Option<&str>, authorization: Option<&str>| {
        let mut req = test::TestRequest::default()
            .method(method)
            .uri(&uri)
            .insert_header((header::HOST, "mq.example.com"))
            .insert_header((header::COOKIE, cookie.clone()));
        if let Some(origin) = origin {
            req = req.insert_header((header::ORIGIN, origin.to_owned()));
        }
        if let Some(authorization) = authorization {
            req = req.insert_header((header::AUTHORIZATION, authorization.to_owned()));
        }
        let req = req.to_request();
        let app = &app;
        async move {
            match test::try_call_service(app, req).await {
                Ok(resp) => resp.status(),
                Err(e) => e.as_response_error().status_code(),
            }
        }
    };
    let create = |name: &str| (Method::POST, format!("/api/admin/ns/{name}"));

    // Another origin: refused, and nothing happens.
    for origin in ["http://localhost:9999", "http://evil.example.com", "null"] {
        let (method, uri) = create("forged");
        assert_eq!(status(method, uri, Some(origin), None).await, StatusCode::FORBIDDEN, "{origin}");
    }
    let (_, namespaces) = call(&app, Method::GET, "/api/admin/stats/ns", Some(&cookie), None).await;
    assert_eq!(namespaces.as_array().map(Vec::len), Some(0), "{namespaces}");

    // The server's own origin, by Host or by the configured NERVEMQ_HOST
    // (http://localhost:8080 here), and a client that sends no Origin.
    for (name, origin) in [
        ("same-host", Some("http://mq.example.com")),
        ("configured", Some("http://localhost:8080")),
        ("no-origin", None),
    ] {
        let (method, uri) = create(name);
        assert_eq!(status(method, uri, origin, None).await, StatusCode::OK, "{name}");
    }

    // Reads are not affected: CORS already keeps the answer from the page.
    let status_of_read = status(
        Method::GET,
        "/api/admin/stats/ns".to_owned(),
        Some("http://localhost:9999"),
        None,
    )
    .await;
    assert_eq!(status_of_read, StatusCode::OK);

    // A request carrying its own credentials is left to authentication,
    // which rejects this made-up key.
    let (method, uri) = create("own-credentials");
    let with_header = status(method, uri, Some("http://localhost:9999"), Some("Bearer made-up")).await;
    assert_eq!(with_header, StatusCode::UNAUTHORIZED);
}

/// The admin API's JSON bodies must be labelled `application/json`: a
/// browser sends `text/plain` from any origin without a preflight, so
/// accepting it would let another origin's page post JSON with the cookie.
#[actix_web::test]
async fn admin_json_must_be_labelled_as_json() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    let post = |content_type: &'static str, name: &str| {
        test::TestRequest::post()
            .uri(&format!("/api/admin/queue/demo/{name}"))
            .insert_header((header::COOKIE, cookie.clone()))
            .insert_header((header::CONTENT_TYPE, content_type))
            .set_payload(r#"{"attributes":{},"tags":{}}"#)
            .to_request()
    };

    for content_type in ["text/plain", "application/x-www-form-urlencoded"] {
        let resp = test::call_service(&app, post(content_type, "unlabelled")).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{content_type}");
        let body = test::read_body(resp).await;
        assert!(
            String::from_utf8_lossy(&body).contains("Content type error"),
            "{content_type}: {body:?}"
        );
    }
    let (_, queues) = call(&app, Method::GET, "/api/admin/queue/demo", Some(&cookie), None).await;
    assert_eq!(queues["queues"].as_array().map(Vec::len), Some(1), "{queues}");

    let resp = test::call_service(&app, post("application/json", "labelled")).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// With NERVEMQ_HOST set, the UI and admin API answer only requests for that
/// name or a loopback one, which a DNS-rebinding page (whose Host is its own
/// domain) never sends. The SQS API is exempt.
#[actix_web::test]
async fn a_configured_host_refuses_requests_for_other_names() {
    let (data, _dir) = setup_with_host(Some("https://mq.example.com")).await;
    let app = init_app(data).await;

    let status = |host: &'static str, uri: &'static str| {
        let req = test::TestRequest::get()
            .uri(uri)
            .insert_header((header::HOST, host))
            .to_request();
        let app = &app;
        async move {
            match test::try_call_service(app, req).await {
                Ok(resp) => resp.status(),
                Err(e) => e.as_response_error().status_code(),
            }
        }
    };

    for host in ["evil.test", "evil.test:8080", "mq.example.com:8443"] {
        assert_eq!(
            status(host, "/api/admin/stats/ns").await,
            StatusCode::MISDIRECTED_REQUEST,
            "{host}"
        );
    }
    // Allowed names reach authentication, which wants a session.
    for host in ["mq.example.com", "localhost:8080", "127.0.0.1:3000"] {
        assert_eq!(status(host, "/api/admin/stats/ns").await, StatusCode::UNAUTHORIZED, "{host}");
    }
    // Not refused for its host (this app has no SQS routes, hence 404).
    assert_eq!(status("evil.test", "/api/sqs").await, StatusCode::NOT_FOUND);
}

/// Without NERVEMQ_HOST, any name is answered.
#[actix_web::test]
async fn without_a_configured_host_any_name_is_answered() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let req = test::TestRequest::get()
        .uri("/api/admin/stats/ns")
        .insert_header((header::HOST, "evil.test:8080"))
        .to_request();
    let status = test::try_call_service(&app, req)
        .await
        .map_or_else(|e| e.as_response_error().status_code(), |r| r.status());
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// The health check needs no login, and says so when the database can't
/// answer.
#[actix_web::test]
async fn health_reports_whether_the_database_answers() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, body) = call(&app, Method::GET, "/api/health", None, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, serde_json::json!({ "status": "ok" }));
    let (status, _) = call(&app, Method::GET, "/api/health/", None, None).await;
    assert_eq!(status, StatusCode::OK, "trailing slash");
    let (status, _) = call(&app, Method::HEAD, "/api/health", None, None).await;
    assert_eq!(status, StatusCode::OK, "HEAD");

    data.db().close().await;
    let (status, body) = call(&app, Method::GET, "/api/health", None, None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body, serde_json::json!({ "status": "unavailable" }));
}

/// A database that doesn't answer in time counts as down, rather than
/// leaving the probe waiting for the pool's 30-second acquire timeout.
#[actix_web::test]
async fn health_gives_up_on_a_stuck_database() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let pool = data.db();
    let mut held = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        held.push(pool.acquire().await.unwrap());
    }

    let started = std::time::Instant::now();
    let (status, _) = call(&app, Method::GET, "/api/health", None, None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    drop(held);
}

/// Through the production app (`crate::build_app`), an admin request's span
/// is named after its route and records the session's user, and a refused
/// one is traced too.
#[actix_web::test]
async fn admin_request_spans_name_the_route_and_the_session_user() {
    let (data, _dir) = setup().await;
    let (captured, _guard) = crate::telemetry::test_support::Captured::install();
    let app = test::init_service(crate::build_app(
        data,
        SqliteSessionStore::in_memory().await,
        actix_web::cookie::Key::generate(),
    ))
    .await;

    let (status, _) = call(&app, Method::GET, "/api/admin/stats/queue", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let span = captured.last_span("HTTP request").unwrap();
    assert_eq!(span.field("otel.name").as_deref(), Some("GET /api/admin/stats/queue"));
    assert_eq!(span.field("http.route").as_deref(), Some("/api/admin/stats/queue"));
    assert_eq!(span.field("http.response.status_code").as_deref(), Some("401"));
    assert_eq!(span.field("enduser.id"), None);

    let cookie = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let (status, _) = call(&app, Method::GET, "/api/admin/stats/queue", Some(&cookie), None).await;
    assert_eq!(status, StatusCode::OK);
    let span = captured.last_span("HTTP request").unwrap();
    assert_eq!(span.field("enduser.id").as_deref(), Some(ADMIN_EMAIL));
    assert_eq!(span.field("rpc.method"), None);
}

/// Probes address the server by IP, so the health check is answered under
/// any name even when NERVEMQ_HOST is set.
#[actix_web::test]
async fn health_is_answered_under_any_host() {
    let (data, _dir) = setup_with_host(Some("https://mq.example.com")).await;
    let app = init_app(data).await;
    for (host, uri) in [
        ("10.0.0.5:8080", "/api/health"),
        ("evil.test", "/api/health"),
        ("evil.test", "/api/health/"),
    ] {
        let req = test::TestRequest::get()
            .uri(uri)
            .insert_header((header::HOST, host))
            .to_request();
        let status = test::try_call_service(&app, req)
            .await
            .map_or_else(|e| e.as_response_error().status_code(), |r| r.status());
        assert_eq!(status, StatusCode::OK, "{host}{uri}");
    }
}

#[actix_web::test]
async fn listing_a_namespaces_queues_needs_access_to_it() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let (status, _) = call(&app, Method::POST, "/api/admin/ns/secret", Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);

    let user = login(&app, USER_EMAIL, PASSWORD).await;
    let (status, body) = call(&app, Method::GET, "/api/admin/queue/secret", Some(&user), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "listed another namespace's queues: {body}");
}

#[actix_web::test]
async fn deleting_a_creator_keeps_their_namespaces_and_queues_listed() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;

    data.create_user(
        "ops@example.com".try_into().unwrap(),
        PASSWORD.into(),
        Some(api::auth::Role::Admin),
        vec![],
    )
    .await
    .unwrap();
    let ops = || Identity::mock("ops@example.com".to_string());
    data.create_namespace("built", ops()).await.unwrap();
    data.create_queue("built", "jobs", Default::default(), HashMap::new(), ops())
        .await
        .unwrap();

    // This used to fail on the namespaces.created_by foreign key.
    let (status, body) = call(
        &app,
        Method::DELETE,
        "/api/admin/users",
        Some(&admin),
        Some(serde_json::json!({"email": "ops@example.com"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (_, namespaces) = call(&app, Method::GET, "/api/admin/stats/ns", Some(&admin), None).await;
    assert_eq!(namespaces[0]["name"], "built");
    assert_eq!(namespaces[0]["created_by"], "ops@example.com", "creator record lost");
    assert_eq!(namespaces[0]["owners"], serde_json::json!([]));

    // Its queue used to vanish from every listing (inner join on the creator).
    let (_, queues) = call(&app, Method::GET, "/api/admin/stats/queue", Some(&admin), None).await;
    assert!(queues["built/jobs"]["created_by"].is_null(), "{queues}");
    let (_, listed) = call(&app, Method::GET, "/api/admin/queue/built", Some(&admin), None).await;
    assert_eq!(listed["queues"][0]["name"], "jobs");
}

#[actix_web::test]
async fn queue_stats_list_same_named_queues_in_every_namespace() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    for ns in ["a", "b"] {
        data.create_namespace(ns, Identity::mock(ADMIN_EMAIL.to_string()))
            .await
            .unwrap();
        data.create_queue(ns, "jobs", Default::default(), HashMap::new(), Identity::mock(ADMIN_EMAIL.to_string()))
            .await
            .unwrap();
    }

    let (_, queues) = call(&app, Method::GET, "/api/admin/stats/queue", Some(&admin), None).await;
    let keys: HashSet<_> = queues.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, HashSet::from(["a/jobs".to_string(), "b/jobs".to_string()]));
}

#[actix_web::test]
async fn disabled_users_are_locked_out_until_reenabled() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let user = login(&app, USER_EMAIL, PASSWORD).await;

    let disable = format!("/api/admin/users/{USER_EMAIL}/disable");
    let (status, _) = call(&app, Method::POST, &disable, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);

    // The open session stops working, and so does logging in again.
    let (status, _) = call(&app, Method::GET, "/api/admin/queue", Some(&user), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&app, Method::POST, "/api/admin/auth/verify", Some(&user), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/login",
        None,
        Some(serde_json::json!({"email": USER_EMAIL, "password": PASSWORD})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (_, users) = call(&app, Method::GET, "/api/admin/users", Some(&admin), None).await;
    assert!(users
        .as_array()
        .unwrap()
        .contains(&serde_json::json!({"email": USER_EMAIL, "role": "user", "disabled": true})));

    let enable = format!("/api/admin/users/{USER_EMAIL}/enable");
    let (status, _) = call(&app, Method::POST, &enable, Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    login(&app, USER_EMAIL, PASSWORD).await;
}

#[actix_web::test]
async fn the_last_active_admin_cannot_be_removed() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let role_uri = format!("/api/admin/users/{ADMIN_EMAIL}/role");
    for (method, uri, body) in [
        (Method::POST, role_uri.clone(), Some(serde_json::json!({"role": "user"}))),
        (Method::POST, format!("/api/admin/users/{ADMIN_EMAIL}/disable"), None),
        (Method::DELETE, "/api/admin/users".to_string(), Some(serde_json::json!({"email": ADMIN_EMAIL}))),
    ] {
        let (status, body) = call(&app, method.clone(), &uri, Some(&admin), body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{method} {uri}: {body}");
    }

    // A disabled admin does not count: promoting a user who is then disabled
    // still leaves one active admin.
    let user_role = format!("/api/admin/users/{USER_EMAIL}/role");
    let (status, _) = call(&app, Method::POST, &user_role, Some(&admin), Some(serde_json::json!({"role": "admin"}))).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, Method::POST, &format!("/api/admin/users/{USER_EMAIL}/disable"), Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, Method::POST, &role_uri, Some(&admin), Some(serde_json::json!({"role": "user"}))).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // With a second active admin, the first can step down.
    let (status, _) = call(&app, Method::POST, &format!("/api/admin/users/{USER_EMAIL}/enable"), Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(&app, Method::POST, &role_uri, Some(&admin), Some(serde_json::json!({"role": "user"}))).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn admins_list_and_revoke_other_users_keys() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/tokens",
        Some(&user),
        Some(serde_json::json!({"name": "worker", "namespace": "team"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let tokens_uri = format!("/api/admin/users/{USER_EMAIL}/tokens");
    let (_, tokens) = call(&app, Method::GET, &tokens_uri, Some(&admin), None).await;
    assert_eq!(
        tokens,
        serde_json::json!([{"name": "worker", "namespace": "team", "access": "member"}])
    );

    let (status, _) = call(&app, Method::DELETE, &format!("{tokens_uri}/worker"), Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let (_, tokens) = call(&app, Method::GET, &tokens_uri, Some(&admin), None).await;
    assert_eq!(tokens, serde_json::json!([]));

    // Users cannot reach other users' keys.
    let (status, _) = call(&app, Method::GET, &tokens_uri, Some(&user), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn users_change_their_own_password() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let user = login(&app, USER_EMAIL, PASSWORD).await;

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/password",
        Some(&user),
        Some(serde_json::json!({"current_password": "wrong", "new_password": "n3w-password"})),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/password",
        Some(&user),
        Some(serde_json::json!({"current_password": PASSWORD, "new_password": "n3w-password"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    login(&app, USER_EMAIL, "n3w-password").await;
    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/auth/login",
        None,
        Some(serde_json::json!({"email": USER_EMAIL, "password": PASSWORD})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn admins_reset_passwords() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;

    let uri = format!("/api/admin/users/{USER_EMAIL}/password");
    let (status, _) = call(&app, Method::POST, &uri, Some(&admin), Some(serde_json::json!({"password": ""}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(&app, Method::POST, &uri, Some(&admin), Some(serde_json::json!({"password": "reset-pass-1"}))).await;
    assert_eq!(status, StatusCode::OK);
    login(&app, USER_EMAIL, "reset-pass-1").await;

    let (status, _) = call(
        &app,
        Method::POST,
        "/api/admin/users/nobody@example.com/password",
        Some(&admin),
        Some(serde_json::json!({"password": "whatever-1"})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// API key access levels
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn only_admin_access_keys_reach_the_admin_api() {
    let (data, _dir) = setup().await;
    data.create_namespace("ns", Identity::mock(ADMIN_EMAIL.to_string()))
        .await
        .unwrap();
    let app = init_app(data.clone()).await;

    for (access, expected) in [
        (KeyAccess::Admin, StatusCode::OK),
        (KeyAccess::Owner, StatusCode::UNAUTHORIZED),
        (KeyAccess::Member, StatusCode::UNAUTHORIZED),
    ] {
        let key = data
            .create_token_with(
                access.as_str().into(),
                "ns".into(),
                Identity::mock(ADMIN_EMAIL.to_string()),
                None,
                Some(access),
            )
            .await
            .unwrap();
        let req = test::TestRequest::get()
            .uri("/api/admin/users")
            .insert_header((
                header::AUTHORIZATION,
                format!("NerveMqApiV1 nervemq_{}_{}", key.access_key, key.secret_key),
            ))
            .to_request();
        let status = match test::try_call_service(&app, req).await {
            Ok(resp) => resp.status(),
            Err(err) => err.error_response().status(),
        };
        assert_eq!(status, expected, "{access:?} key on the admin API");
    }
}

#[actix_web::test]
async fn creating_a_key_offers_at_most_the_callers_level() {
    let (data, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let (admin, user) = team_with_member(&app, &data).await;
    let mint = |name: &str, access: &str| {
        serde_json::json!({"name": name, "namespace": "team", "access": access})
    };

    // A member: only member access.
    for access in ["owner", "admin"] {
        let (status, _) =
            call(&app, Method::POST, "/api/admin/tokens", Some(&user), Some(mint("k", access))).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "member minted {access}");
    }
    let (status, body) =
        call(&app, Method::POST, "/api/admin/tokens", Some(&user), Some(mint("k", "member"))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["access"], "member");

    // Without a level, a key gets its owner's own: the admin's is admin.
    let (status, body) = call(
        &app,
        Method::POST,
        "/api/admin/tokens",
        Some(&admin),
        Some(serde_json::json!({"name": "full", "namespace": "team"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["access"], "admin");

    let (_, keys) = call(&app, Method::GET, "/api/admin/tokens", Some(&user), None).await;
    assert_eq!(keys[0]["access"], "member");
}

/// The admin panel's sends go through the same checks as SQS sends: AWS's
/// rules for bodies and attributes. Nothing refused is stored.
#[actix_web::test]
async fn admin_sends_are_checked_as_sqs_sends_are() {
    let (data, _dir) = setup().await;
    let app = init_app(data).await;
    let cookie = setup_queue(&app).await;

    for (case, payload) in [
        ("an empty body", serde_json::json!({ "body": "" })),
        ("a control character", serde_json::json!({ "body": "\u{1}" })),
        (
            "a reserved attribute name",
            serde_json::json!({
                "body": "x",
                "attributes": { "AWS.x": { "DataType": "String", "StringValue": "v" } },
            }),
        ),
    ] {
        let (status, body) = call(
            &app,
            Method::POST,
            "/api/admin/queue/demo/jobs/messages",
            Some(&cookie),
            Some(payload),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{case}: {body}");
    }

    let (status, body) = call(
        &app,
        Method::GET,
        "/api/admin/queue/demo/jobs/messages",
        Some(&cookie),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 0);
}
