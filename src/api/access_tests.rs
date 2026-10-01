//! Access-control tests for the management API: who may call what, and the
//! edge cases of user, permission, password and API key administration.
//!
//! The route matrix at the top is the guard against a new admin route that
//! forgets its protection: every user-admin route is tried by every kind of
//! caller that must be refused, and must leave nothing changed.

use std::collections::HashMap;

use actix_identity::Identity;
use actix_web::{
    body::MessageBody,
    dev::{Service as ActixService, ServiceResponse},
    http::{header, Method, StatusCode},
    test,
    web::Data,
};
use serde_json::{json, Value};

use super::endpoint_tests::{call, init_app, login, setup, ADMIN_EMAIL, PASSWORD, USER_EMAIL};
use crate::{
    api::{auth::Role, tokens::CreateTokenResponse},
    auth::credential::KeyAccess,
    service::Service,
};

const OWNER: &str = "owner@example.com";
const MEMBER: &str = "member@example.com";
const ADMIN2: &str = "admin2@example.com";

/// Everyone the tests need, built on `setup`'s root admin (`ADMIN_EMAIL`)
/// and grantless user (`USER_EMAIL`):
///
/// - namespaces `team` and `other`, each with a queue `jobs`
/// - `OWNER` owns `team`; `MEMBER` is a plain member of `team`
/// - `ADMIN2`, a second admin with no grants
struct World {
    data: Data<Service>,
    _dir: tempfile::TempDir,
}

async fn world() -> World {
    let (data, dir) = setup().await;
    let root = || Identity::mock(ADMIN_EMAIL.to_string());
    for ns in ["team", "other"] {
        data.create_namespace(ns, root()).await.unwrap();
        data.create_queue(ns, "jobs", Default::default(), HashMap::new(), root())
            .await
            .unwrap();
    }
    for (email, role, namespaces) in [
        (OWNER, Role::User, vec!["team".to_string()]),
        (MEMBER, Role::User, vec!["team".to_string()]),
        (ADMIN2, Role::Admin, vec![]),
    ] {
        data.create_user(email.try_into().unwrap(), PASSWORD.into(), Some(role), namespaces)
            .await
            .unwrap();
    }
    data.set_namespace_owner("team", &OWNER.try_into().unwrap(), true)
        .await
        .unwrap();
    World { data, _dir: dir }
}

/// A key for `team` (or `namespace`) owned by `email`, at `access`.
async fn key(data: &Service, email: &str, namespace: &str, access: KeyAccess) -> CreateTokenResponse {
    data.create_token_with(
        format!("{email}-{namespace}-{}", access.as_str()),
        namespace.into(),
        Identity::mock(email.to_string()),
        None,
        Some(access),
    )
    .await
    .unwrap()
}

/// Sends a request authenticated by an API key (NerveMQ scheme, no session).
async fn call_with_key<S, B>(
    app: &S,
    method: Method,
    uri: &str,
    key: &CreateTokenResponse,
    body: Option<Value>,
) -> (StatusCode, Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let mut req = test::TestRequest::default().method(method).uri(uri).insert_header((
        header::AUTHORIZATION,
        format!("NerveMqApiV1 nervemq_{}_{}", key.access_key, key.secret_key),
    ));
    if let Some(body) = body {
        req = req.set_json(body);
    }
    match test::try_call_service(app, req.to_request()).await {
        Ok(resp) => {
            let status = resp.status();
            let bytes = actix_web::body::to_bytes(resp.into_body())
                .await
                .unwrap_or_default();
            (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
        }
        Err(err) => (err.error_response().status(), Value::Null),
    }
}

/// Every route of the user-admin API, aimed at `MEMBER` so that a call that
/// slipped through would visibly change something.
fn user_admin_routes() -> Vec<(Method, String, Option<Value>)> {
    let e = MEMBER;
    vec![
        (Method::GET, "/api/admin/users".into(), None),
        (
            Method::POST,
            "/api/admin/users".into(),
            Some(json!({"email": "intruder@example.com", "password": "intruder-pw", "role": "admin", "namespaces": []})),
        ),
        (Method::DELETE, "/api/admin/users".into(), Some(json!({"email": e}))),
        (Method::GET, format!("/api/admin/users/{e}/permissions"), None),
        (Method::PUT, format!("/api/admin/users/{e}/permissions"), Some(json!(["other"]))),
        (Method::POST, format!("/api/admin/users/{e}/permissions"), Some(json!(["other"]))),
        (Method::DELETE, format!("/api/admin/users/{e}/permissions"), Some(json!(["team"]))),
        (Method::GET, format!("/api/admin/users/{e}/role"), None),
        (Method::POST, format!("/api/admin/users/{e}/role"), Some(json!({"role": "admin"}))),
        (Method::POST, format!("/api/admin/users/{e}/disable"), None),
        (Method::POST, format!("/api/admin/users/{e}/enable"), None),
        (Method::POST, format!("/api/admin/users/{e}/password"), Some(json!({"password": "taken-over"}))),
        (Method::GET, format!("/api/admin/users/{e}/tokens"), None),
        (Method::DELETE, format!("/api/admin/users/{e}/tokens/{e}-team-member"), None),
    ]
}

/// What `user_admin_routes` would change, read straight from the database.
async fn user_admin_state(data: &Service) -> Value {
    let users = data.list_users().await.unwrap();
    let grants: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT u.email, ns.name, p.is_owner FROM user_permissions p
         JOIN users u ON u.id = p.user JOIN namespaces ns ON ns.id = p.namespace
         ORDER BY u.email, ns.name",
    )
    .fetch_all(data.db())
    .await
    .unwrap();
    let keys: Vec<(String, String)> = sqlx::query_as(
        "SELECT u.email, k.name FROM api_keys k JOIN users u ON u.id = k.user ORDER BY k.name",
    )
    .fetch_all(data.db())
    .await
    .unwrap();
    let hashes: Vec<String> = sqlx::query_scalar("SELECT hashed_pass FROM users ORDER BY email")
        .fetch_all(data.db())
        .await
        .unwrap();
    json!({"users": users, "grants": grants, "keys": keys, "hashes": hashes})
}

// ---------------------------------------------------------------------------
// Route matrix
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn every_user_admin_route_refuses_everyone_but_active_admins() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;

    // A key MEMBER owns: the token-deleting route's target, and one of the
    // refused callers.
    let members_key = key(data, MEMBER, "team", KeyAccess::Member).await;

    let member = login(&app, MEMBER, PASSWORD).await;
    let owner = login(&app, OWNER, PASSWORD).await;
    let outsider = login(&app, USER_EMAIL, PASSWORD).await;
    let disabled_admin = login(&app, ADMIN2, PASSWORD).await;
    data.set_user_disabled(&ADMIN2.try_into().unwrap(), true)
        .await
        .unwrap();

    let sessions: Vec<(&str, Option<&str>)> = vec![
        ("anonymous", None),
        ("member session", Some(&member)),
        ("owner session", Some(&owner)),
        ("grantless user session", Some(&outsider)),
        ("disabled admin session", Some(&disabled_admin)),
    ];
    let keys = vec![
        ("admin's owner-level key", key(data, ADMIN_EMAIL, "team", KeyAccess::Owner).await),
        ("admin's member-level key", key(data, ADMIN_EMAIL, "team", KeyAccess::Member).await),
        ("owner's key", key(data, OWNER, "team", KeyAccess::Owner).await),
        ("member's key", members_key),
    ];

    let before = user_admin_state(data).await;

    for (method, uri, body) in user_admin_routes() {
        for (who, cookie) in &sessions {
            let (status, _) = call(&app, method.clone(), &uri, *cookie, body.clone()).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{who}: {method} {uri}");
        }
        for (who, key) in &keys {
            let (status, _) = call_with_key(&app, method.clone(), &uri, key, body.clone()).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{who}: {method} {uri}");
        }
    }

    assert_eq!(user_admin_state(data).await, before, "a refused call changed something");

    // The routes are reachable at all: an active admin's session and an
    // admin-level key both read the user list.
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let (status, _) = call(&app, Method::GET, "/api/admin/users", Some(&admin), None).await;
    assert_eq!(status, StatusCode::OK);
    let admin_key = key(data, ADMIN_EMAIL, "team", KeyAccess::Admin).await;
    let (status, _) = call_with_key(&app, Method::GET, "/api/admin/users", &admin_key, None).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn only_admins_create_namespaces_or_choose_owners() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;

    let owner = login(&app, OWNER, PASSWORD).await;
    let member = login(&app, MEMBER, PASSWORD).await;
    let owner_uri = format!("/api/admin/ns/team/owners/{MEMBER}");

    for (who, cookie) in [("owner", &owner), ("member", &member)] {
        let (status, _) = call(&app, Method::POST, "/api/admin/ns/new", Some(cookie), None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{who} created a namespace");
        for method in [Method::PUT, Method::DELETE] {
            let (status, _) = call(&app, method.clone(), &owner_uri, Some(cookie), None).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{who}: {method} owner");
        }
    }
    let (status, _) = call(&app, Method::POST, "/api/admin/ns/new", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    assert!(data.get_namespace_id("new", data.db()).await.unwrap().is_none());
    let members = data.list_namespace_members("team").await.unwrap();
    assert!(members.iter().any(|m| m.email == MEMBER && !m.owner));
}

#[actix_web::test]
async fn session_only_routes_refuse_api_keys_even_admin_level() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;
    let admin_key = key(data, ADMIN_EMAIL, "team", KeyAccess::Admin).await;

    // API keys authenticate the SQS API and the user-admin API; everything
    // else the UI uses needs a login.
    for (method, uri, body) in [
        (Method::GET, "/api/admin/queue", None),
        (Method::GET, "/api/admin/queue/team", None),
        (Method::GET, "/api/admin/stats/queue", None),
        (Method::GET, "/api/admin/stats/ns", None),
        (Method::GET, "/api/admin/tokens", None),
        (Method::POST, "/api/admin/tokens", Some(json!({"name": "x", "namespace": "team"}))),
        (Method::GET, "/api/admin/ns", None),
        (Method::POST, "/api/admin/ns/new", None),
        (
            Method::POST,
            "/api/admin/auth/password",
            Some(json!({"current_password": PASSWORD, "new_password": "changed-pw-1"})),
        ),
    ] {
        let (status, _) = call_with_key(&app, method.clone(), uri, &admin_key, body).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
    }
}

#[actix_web::test]
async fn a_demoted_admins_admin_level_key_loses_the_admin_api() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;

    let admin2_key = key(data, ADMIN2, "team", KeyAccess::Admin).await;
    let (status, _) = call_with_key(&app, Method::GET, "/api/admin/users", &admin2_key, None).await;
    assert_eq!(status, StatusCode::OK);

    // The level caps the owner's current role; it is not a role of its own.
    data.set_user_role(&ADMIN2.try_into().unwrap(), Role::User)
        .await
        .unwrap();
    let (status, _) = call_with_key(&app, Method::GET, "/api/admin/users", &admin2_key, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // And disabling an admin shuts their key out too.
    let root_key = key(data, ADMIN_EMAIL, "team", KeyAccess::Admin).await;
    data.set_user_role(&ADMIN2.try_into().unwrap(), Role::Admin)
        .await
        .unwrap();
    data.set_user_disabled(&ADMIN_EMAIL.try_into().unwrap(), true)
        .await
        .unwrap();
    let (status, _) = call_with_key(&app, Method::GET, "/api/admin/users", &root_key, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// What each caller sees
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn listings_show_only_what_the_caller_can_access() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;

    let names = |v: &Value, field: &str| -> Vec<String> {
        let mut out: Vec<String> = match v {
            Value::Array(items) => items.iter().map(|i| i[field].as_str().unwrap().to_owned()).collect(),
            Value::Object(map) => map.keys().cloned().collect(),
            _ => panic!("unexpected listing: {v}"),
        };
        out.sort();
        out
    };

    for (who, email, namespaces, queues) in [
        ("admin", ADMIN_EMAIL, vec!["other", "team"], vec!["other/jobs", "team/jobs"]),
        ("grantless admin", ADMIN2, vec!["other", "team"], vec!["other/jobs", "team/jobs"]),
        ("owner", OWNER, vec!["team"], vec!["team/jobs"]),
        ("member", MEMBER, vec!["team"], vec!["team/jobs"]),
        ("grantless user", USER_EMAIL, vec![], vec![]),
    ] {
        let cookie = login(&app, email, PASSWORD).await;
        let (_, ns) = call(&app, Method::GET, "/api/admin/ns", Some(&cookie), None).await;
        assert_eq!(names(&ns, "name"), namespaces, "{who}: /ns");
        let (_, stats) = call(&app, Method::GET, "/api/admin/stats/ns", Some(&cookie), None).await;
        assert_eq!(names(&stats, "name"), namespaces, "{who}: /stats/ns");
        let (_, qstats) = call(&app, Method::GET, "/api/admin/stats/queue", Some(&cookie), None).await;
        assert_eq!(names(&qstats, ""), queues, "{who}: /stats/queue");
        let (_, all) = call(&app, Method::GET, "/api/admin/queue", Some(&cookie), None).await;
        let mut listed: Vec<String> = all["queues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| format!("{}/{}", q["ns"].as_str().unwrap(), q["name"].as_str().unwrap()))
            .collect();
        listed.sort();
        assert_eq!(listed, queues, "{who}: /queue");
    }

    // Outside their namespaces, members and owners reach nothing.
    for email in [OWNER, MEMBER] {
        let cookie = login(&app, email, PASSWORD).await;
        for uri in [
            "/api/admin/queue/other",
            "/api/admin/queue/other/jobs",
            "/api/admin/queue/other/jobs/messages",
            "/api/admin/queue/other/jobs/attributes",
            "/api/admin/queue/other/jobs/config",
        ] {
            let (status, _) = call(&app, Method::GET, uri, Some(&cookie), None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{email}: {uri}");
        }
        let (status, _) = call(
            &app,
            Method::POST,
            "/api/admin/queue/other/jobs/messages",
            Some(&cookie),
            Some(json!({"body": "intrusion"})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{email} sent into another namespace");
    }
    let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(data.db())
        .await
        .unwrap();
    assert_eq!(messages, 0);
}

#[actix_web::test]
async fn owners_list_their_namespaces_members_but_not_others() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;
    let owner = login(&app, OWNER, PASSWORD).await;

    let (status, members) = call(&app, Method::GET, "/api/admin/ns/team/members", Some(&owner), None).await;
    assert_eq!(status, StatusCode::OK);
    let emails: Vec<&str> = members.as_array().unwrap().iter().map(|m| m["email"].as_str().unwrap()).collect();
    assert!(emails.contains(&MEMBER) && emails.contains(&OWNER), "{members}");

    let (status, _) = call(&app, Method::GET, "/api/admin/ns/other/members", Some(&owner), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&app, Method::DELETE, "/api/admin/ns/other", Some(&owner), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "an owner deleted a namespace they don't belong to");
    assert!(data.get_namespace_id("other", data.db()).await.unwrap().is_some());
}

// ---------------------------------------------------------------------------
// User administration edge cases
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn creating_users_validates_input() {
    let w = world().await;
    let app = init_app(w.data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let create = |email: &str, password: &str, namespaces: Value| {
        json!({"email": email, "password": password, "role": "user", "namespaces": namespaces})
    };

    for (case, body, expected) in [
        ("empty password", create("new@example.com", "", json!([])), StatusCode::BAD_REQUEST),
        ("invalid email", create("not-an-email", "pw-123456", json!([])), StatusCode::BAD_REQUEST),
        ("existing email", create(MEMBER, "pw-123456", json!([])), StatusCode::CONFLICT),
        ("unknown namespace", create("new@example.com", "pw-123456", json!(["nope"])), StatusCode::NOT_FOUND),
    ] {
        let (status, body) = call(&app, Method::POST, "/api/admin/users", Some(&admin), Some(body)).await;
        assert_eq!(status, expected, "{case}: {body}");
    }

    // None of them created anyone, nor left a half-made account behind.
    let users = w.data.list_users().await.unwrap();
    assert_eq!(users.len(), 5, "{users:?}");
    login(&app, MEMBER, PASSWORD).await;
}

#[actix_web::test]
async fn acting_on_a_missing_user_or_namespace_is_not_found() {
    let w = world().await;
    let app = init_app(w.data.clone()).await;
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let ghost = "ghost@example.com";

    for (method, uri, body) in [
        (Method::DELETE, "/api/admin/users".to_string(), Some(json!({"email": ghost}))),
        (Method::GET, format!("/api/admin/users/{ghost}/role"), None),
        (Method::POST, format!("/api/admin/users/{ghost}/role"), Some(json!({"role": "admin"}))),
        (Method::POST, format!("/api/admin/users/{ghost}/disable"), None),
        (Method::POST, format!("/api/admin/users/{ghost}/enable"), None),
        (Method::POST, format!("/api/admin/users/{ghost}/password"), Some(json!({"password": "pw-123456"}))),
        (Method::PUT, format!("/api/admin/users/{ghost}/permissions"), Some(json!(["team"]))),
        (Method::POST, format!("/api/admin/users/{ghost}/permissions"), Some(json!(["team"]))),
        (Method::PUT, format!("/api/admin/users/{MEMBER}/permissions"), Some(json!(["nope"]))),
        (Method::POST, format!("/api/admin/users/{MEMBER}/permissions"), Some(json!(["nope"]))),
        (Method::PUT, format!("/api/admin/ns/team/owners/{ghost}"), None),
        (Method::PUT, format!("/api/admin/ns/nope/owners/{MEMBER}"), None),
        (Method::GET, "/api/admin/ns/nope/members".to_string(), None),
        (Method::DELETE, "/api/admin/ns/nope".to_string(), None),
        (Method::DELETE, format!("/api/admin/users/{MEMBER}/tokens/nope"), None),
    ] {
        let (status, body) = call(&app, method.clone(), &uri, Some(&admin), body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {uri}: {body}");
    }

    // A failed replace left the member's grants alone.
    assert_eq!(
        w.data.list_namespace_members("team").await.unwrap().iter().filter(|m| m.email == MEMBER).count(),
        1
    );
}

#[actix_web::test]
async fn revoking_a_grant_removes_ownership_and_regranting_does_not_restore_it() {
    let w = world().await;
    let data = &w.data;
    let owner = OWNER.try_into().unwrap();

    data.revoke_user_namespaces(&owner, &["team".to_string()]).await.unwrap();
    assert!(!data.list_namespace_members("team").await.unwrap().iter().any(|m| m.email == OWNER));

    data.grant_user_namespaces(&owner, &["team".to_string()]).await.unwrap();
    let members = data.list_namespace_members("team").await.unwrap();
    assert!(members.iter().any(|m| m.email == OWNER && !m.owner), "{members:?}");
}

#[actix_web::test]
async fn deleting_a_user_removes_their_grants_and_keys() {
    let w = world().await;
    let data = &w.data;
    key(data, OWNER, "team", KeyAccess::Owner).await;

    data.delete_user(OWNER.try_into().unwrap()).await.unwrap();

    assert!(!data.list_namespace_members("team").await.unwrap().iter().any(|m| m.email == OWNER));
    assert!(data.list_user_tokens(OWNER).await.unwrap().is_empty());
    let orphans: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM api_keys WHERE user NOT IN (SELECT id FROM users)",
    )
    .fetch_one(data.db())
    .await
    .unwrap();
    assert_eq!(orphans, 0);
}

// ---------------------------------------------------------------------------
// Passwords
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn password_changes_validate_and_need_a_live_session() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;
    let member = login(&app, MEMBER, PASSWORD).await;
    let change = |new: &str| json!({"current_password": PASSWORD, "new_password": new});

    let (status, _) = call(&app, Method::POST, "/api/admin/auth/password", Some(&member), Some(change(""))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(&app, Method::POST, "/api/admin/auth/password", None, Some(change("pw-123456"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A disabled user's open session cannot change it either.
    data.set_user_disabled(&MEMBER.try_into().unwrap(), true).await.unwrap();
    let (status, _) = call(&app, Method::POST, "/api/admin/auth/password", Some(&member), Some(change("pw-123456"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    data.set_user_disabled(&MEMBER.try_into().unwrap(), false).await.unwrap();
    login(&app, MEMBER, PASSWORD).await;
}

#[actix_web::test]
async fn a_disabled_users_correct_password_still_does_not_log_in() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;
    data.set_user_disabled(&MEMBER.try_into().unwrap(), true).await.unwrap();

    // The wrong password is refused as wrong (401), not as disabled, so a
    // guess learns nothing about the account.
    let login_as = |password: &str| json!({"email": MEMBER, "password": password});
    let (status, _) = call(&app, Method::POST, "/api/admin/auth/login", None, Some(login_as("wrong"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&app, Method::POST, "/api/admin/auth/login", None, Some(login_as(PASSWORD))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// API keys over the admin API
// ---------------------------------------------------------------------------

#[actix_web::test]
async fn creating_keys_validates_names_namespaces_and_levels() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;
    let member = login(&app, MEMBER, PASSWORD).await;
    let owner = login(&app, OWNER, PASSWORD).await;
    let mint = |cookie: &str, body: Value| {
        let app = &app;
        let cookie = cookie.to_owned();
        async move { call(app, Method::POST, "/api/admin/tokens", Some(&cookie), Some(body)).await }
    };

    let (status, body) = mint(&member, json!({"name": "k", "namespace": "team"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["access"], "member");

    for (case, cookie, body, expected) in [
        ("same name twice", &member, json!({"name": "k", "namespace": "team"}), StatusCode::CONFLICT),
        ("namespace without access", &member, json!({"name": "x", "namespace": "other"}), StatusCode::UNAUTHORIZED),
        ("unknown namespace", &member, json!({"name": "x", "namespace": "nope"}), StatusCode::NOT_FOUND),
        ("unknown access level", &member, json!({"name": "x", "namespace": "team", "access": "root"}), StatusCode::BAD_REQUEST),
        ("owner asking for admin", &owner, json!({"name": "x", "namespace": "team", "access": "admin"}), StatusCode::FORBIDDEN),
    ] {
        let (status, body) = mint(cookie, body).await;
        assert_eq!(status, expected, "{case}: {body}");
    }

    // The same name is fine for someone else.
    let (status, body) = mint(&owner, json!({"name": "k", "namespace": "team"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["access"], "owner");

    // Listings never carry secrets.
    let (_, keys) = call(&app, Method::GET, "/api/admin/tokens", Some(&member), None).await;
    assert_eq!(keys, json!([{"name": "k", "namespace": "team", "access": "member"}]));
    let admin = login(&app, ADMIN_EMAIL, PASSWORD).await;
    let (_, keys) = call(&app, Method::GET, &format!("/api/admin/users/{OWNER}/tokens"), Some(&admin), None).await;
    assert_eq!(keys, json!([{"name": "k", "namespace": "team", "access": "owner"}]));
}

#[actix_web::test]
async fn users_only_delete_their_own_keys() {
    let w = world().await;
    let data = &w.data;
    let app = init_app(data.clone()).await;
    let owners_key = key(data, OWNER, "team", KeyAccess::Owner).await;
    let member = login(&app, MEMBER, PASSWORD).await;

    let (status, _) = call(
        &app,
        Method::DELETE,
        "/api/admin/tokens",
        Some(&member),
        Some(json!({"name": owners_key.name})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(data.list_user_tokens(OWNER).await.unwrap().len(), 1);
}
