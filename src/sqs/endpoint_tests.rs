//! HTTP-level tests for the SQS-compatible endpoint (`POST /api/sqs`).
//!
//! These exercise the full production stack — `NormalizePath`, SigV4
//! `Authentication`, identity/session, `Protected` and the `SqsApi` dispatch
//! middleware — by sending real AWS-JSON requests signed with an API key
//! minted via `Service::create_token`. They complement the service-layer
//! `visibility_tests` in `crate::service`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use serde_json::json;
use std::rc::Rc;
use std::time::SystemTime;

use futures_util::future::{join, join_all};

use actix_identity::{Identity, IdentityMiddleware};
use actix_session::SessionMiddleware;
use actix_web::{
    body::MessageBody,
    dev::{Service as ActixService, ServiceResponse},
    http::StatusCode,
    middleware::{NormalizePath, TrailingSlash},
    test,
    web::{self, Data},
    App,
};
use aws_sigv4::sign::v4::generate_signing_key;
use hmac::{digest::FixedOutput, Mac};
use sha2::Sha256;

use crate::{
    api::tokens::CreateTokenResponse,
    auth::{
        credential::KeyAccess,
        crypto::sha256_hex,
        middleware::{authentication::Authentication, protected_route::Protected},
        session::SqliteSessionStore,
    },
    config::Config,
    kms::memory::InMemoryKeyManager,
    service::Service,
    sqs::service::SqsApi,
};

pub(super) const HOST: &str = "localhost:8080";
pub(super) const REGION: &str = "us-east-1";
pub(super) const SQS_SERVICE: &str = "sqs";
pub(super) const QUEUE_URL: &str = "http://localhost:8080/api/sqs/ns/q";

/// Spins up a Service backed by a throwaway on-disk SQLite database with one
/// namespace (`ns`), one queue (`q`) and one API key authorized for it. The
/// returned `TempDir` must be kept alive for the duration of the test.
pub(super) async fn setup() -> (Data<Service>, CreateTokenResponse, tempfile::TempDir) {
    setup_with(crate::telemetry::Telemetry::default()).await
}

/// As [`setup`], recording metrics through `telemetry`.
pub(super) async fn setup_with(
    telemetry: crate::telemetry::Telemetry,
) -> (Data<Service>, CreateTokenResponse, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    let cfg: Config = serde_json::from_value(serde_json::json!({
        "db_path": db_path,
        "default_max_retries": 5,
    }))
    .unwrap();

    let svc = Service::connect_with()
        .config(cfg)
        .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
        .telemetry(telemetry)
        .call()
        .await
        .unwrap();

    let admin = || Identity::mock("admin@example.com".to_string());

    svc.create_namespace("ns", admin()).await.unwrap();
    svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();

    let creds = svc
        .create_token("endpoint-tests".to_string(), "ns".to_string(), admin())
        .await
        .unwrap();

    (Data::new(svc), creds, dir)
}

/// Builds the same app the server runs (sans CORS/tracing): NormalizePath must
/// stay first in the stack so it can't break SigV4 path hashing, and the SQS
/// scope is wrapped with the same `Protected` + `SqsApi` middleware as in
/// `lib.rs`.
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
            .app_data(data)
            .service(
                web::scope("/api")
                    .service(super::service().wrap(Protected::authenticated()).wrap(SqsApi)),
            ),
    )
    .await
}

/// Signs an AWS-JSON request for `POST /api/sqs` with SigV4, mirroring the
/// canonicalization the server performs in `auth::protocols::sigv4`, and
/// returns the ready-to-send test request.
pub(super) fn signed_request(
    target: &str,
    body: &serde_json::Value,
    access_key: &str,
    secret_key: &str,
) -> actix_http::Request {
    let payload = serde_json::to_vec(body).unwrap();

    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();

    let canonical_headers =
        format!("host:{HOST}\nx-amz-date:{amz_date}\nx-amz-target:{target}\n");
    let signed_headers = "host;x-amz-date;x-amz-target";
    let payload_hash = sha256_hex(&payload);

    let canonical_request = [
        "POST",
        "/api/sqs",
        "",
        &canonical_headers,
        signed_headers,
        &payload_hash,
    ]
    .join("\n");

    let scope = format!("{date}/{REGION}/{SQS_SERVICE}/aws4_request");
    let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

    let string_to_sign = [
        "AWS4-HMAC-SHA256",
        &amz_date,
        &scope,
        &canonical_request_hash,
    ]
    .join("\n");

    let signing_key = generate_signing_key(secret_key, SystemTime::now(), REGION, SQS_SERVICE);
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(signing_key.as_ref()).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize_fixed());

    test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-date", amz_date))
        .insert_header(("x-amz-target", target))
        .insert_header((
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
                 SignedHeaders={signed_headers}, Signature={signature}"
            ),
        ))
        .set_payload(payload)
        .to_request()
}

/// Calls the app and returns (status, parsed JSON body). Middleware rejections
/// (e.g. failed authentication) surface as service-level errors rather than
/// responses, so convert those to the response actix would send on the wire.
pub(super) async fn call<S, B>(app: &S, req: actix_http::Request) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    match test::try_call_service(app, req).await {
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

async fn send_message<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    body: &str,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    call(
        app,
        signed_request(
            "AmazonSQS.SendMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": body }),
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await
}

async fn receive_messages<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    receive_with_max(app, creds, 10).await
}

async fn receive_with_max<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    max: u64,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    call(
        app,
        signed_request(
            "AmazonSQS.ReceiveMessage",
            &serde_json::json!({
                "QueueUrl": QUEUE_URL,
                "MaxNumberOfMessages": max,
                "VisibilityTimeout": 300,
            }),
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await
}

/// Receives until the queue yields nothing more, returning every message
/// handed out. Claimed messages stay invisible for the duration of the test,
/// so this observes each available message exactly once.
async fn drain_queue<S, B>(app: &S, creds: &CreateTokenResponse) -> Vec<serde_json::Value>
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let mut all = Vec::new();
    loop {
        let (status, body) = receive_messages(app, creds).await;
        assert_eq!(status, StatusCode::OK, "ReceiveMessage failed: {body}");
        let msgs = messages(&body);
        if msgs.is_empty() {
            return all;
        }
        all.extend(msgs.iter().cloned());
    }
}

async fn delete_message<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    receipt_handle: &str,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    call(
        app,
        signed_request(
            "AmazonSQS.DeleteMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "ReceiptHandle": receipt_handle }),
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await
}

async fn change_visibility<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    receipt_handle: &str,
    visibility_timeout: u64,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    call(
        app,
        signed_request(
            "AmazonSQS.ChangeMessageVisibility",
            &serde_json::json!({
                "QueueUrl": QUEUE_URL,
                "ReceiptHandle": receipt_handle,
                "VisibilityTimeout": visibility_timeout,
            }),
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await
}

/// Pulls every in-flight message's visibility deadline into the past so the
/// next receive treats them as expired — lets us assert re-availability
/// without sleeping through a real timeout.
async fn expire_inflight(svc: &Service) {
    sqlx::query(
        "UPDATE messages SET invisible_until = unixepoch('now') - 1 WHERE invisible_until IS NOT NULL",
    )
    .execute(svc.db())
    .await
    .unwrap();
}

fn messages(body: &serde_json::Value) -> &Vec<serde_json::Value> {
    body["Messages"]
        .as_array()
        .expect("response should contain a Messages array")
}

#[actix_web::test]
async fn unsigned_request_is_rejected() {
    let (data, _creds, _dir) = setup().await;
    let app = init_app(data).await;

    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("x-amz-target", "AmazonSQS.SendMessage"))
        .set_payload(
            serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "hello" }).to_string(),
        )
        .to_request();

    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn bad_signature_is_rejected() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    // Signed with the wrong secret: the server-side signature won't match.
    let req = signed_request(
        "AmazonSQS.SendMessage",
        &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "hello" }),
        &creds.access_key,
        "not-the-real-secret-key",
    );

    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn send_message_enqueues_and_is_received_intact() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, body) = send_message(&app, &creds, "hello world").await;
    assert_eq!(status, StatusCode::OK, "SendMessage failed: {body}");
    // AWS wire format: MessageId is a string.
    assert!(
        body["MessageId"].as_str().is_some_and(|id| !id.is_empty()),
        "missing MessageId: {body}"
    );
    assert_eq!(
        body["MD5OfMessageBody"],
        format!("{:x}", md5::compute("hello world")),
        "MD5OfMessageBody should match the sent body"
    );

    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK, "ReceiveMessage failed: {body}");

    let msgs = messages(&body);
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["Body"], "hello world");
    assert_eq!(
        msgs[0]["MD5OfBody"],
        format!("{:x}", md5::compute("hello world"))
    );
    assert!(
        !msgs[0]["ReceiptHandle"].as_str().unwrap().is_empty(),
        "received message should carry a receipt handle"
    );
}

#[actix_web::test]
async fn received_message_is_invisible_until_timeout_expires() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "only-once").await;
    assert_eq!(status, StatusCode::OK);

    // First receive hands the message out and starts the visibility window.
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    let first_handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Still within the visibility window: must not be handed out again.
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        messages(&body).is_empty(),
        "in-flight message should be invisible: {body}"
    );

    expire_inflight(&data).await;

    // Timeout elapsed without an ack: the message is redelivered with a fresh
    // receipt handle.
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    let msgs = messages(&body);
    assert_eq!(msgs.len(), 1, "message should be available again: {body}");
    assert_eq!(msgs[0]["Body"], "only-once");
    assert_ne!(
        msgs[0]["ReceiptHandle"].as_str().unwrap(),
        first_handle,
        "redelivery should mint a new receipt handle"
    );
}

#[actix_web::test]
async fn delete_message_acknowledges_and_removes_it() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "ack me").await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    let handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, body) = delete_message(&app, &creds, &handle).await;
    assert_eq!(status, StatusCode::OK, "DeleteMessage failed: {body}");

    // Even once the visibility window would have lapsed, an acknowledged
    // message must never be redelivered.
    expire_inflight(&data).await;
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        messages(&body).is_empty(),
        "acknowledged message should be gone for good: {body}"
    );
}

#[actix_web::test]
async fn delete_with_stale_receipt_handle_fails() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "contested").await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = receive_messages(&app, &creds).await;
    let stale_handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Visibility timeout expires and the message is redelivered to a new
    // consumer, invalidating the first receipt handle.
    expire_inflight(&data).await;
    let (_, body) = receive_messages(&app, &creds).await;
    let current_handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(stale_handle, current_handle);

    let (status, _) = delete_message(&app, &creds, &stale_handle).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "stale receipt handle should not acknowledge a redelivered message"
    );

    // The current handle still acknowledges the message.
    let (status, body) = delete_message(&app, &creds, &current_handle).await;
    assert_eq!(status, StatusCode::OK, "DeleteMessage failed: {body}");

    expire_inflight(&data).await;
    let (_, body) = receive_messages(&app, &creds).await;
    assert!(messages(&body).is_empty(), "queue should be empty: {body}");
}

#[actix_web::test]
async fn concurrent_sends_enqueue_every_message_exactly_once() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    const N: usize = 20;
    let bodies: Vec<String> = (0..N).map(|i| format!("concurrent-{i}")).collect();

    // Fire all sends at once; each response must be a success carrying a
    // unique message id and the digest of the body it was paired with.
    let responses = join_all(bodies.iter().map(|body| send_message(&app, &creds, body))).await;

    let mut ids = HashSet::new();
    for ((status, body), sent) in responses.iter().zip(&bodies) {
        assert_eq!(*status, StatusCode::OK, "SendMessage failed: {body}");
        assert_eq!(
            body["MD5OfMessageBody"],
            format!("{:x}", md5::compute(sent)),
            "response digest should match the body sent by this request"
        );
        assert!(
            ids.insert(body["MessageId"].to_string()),
            "concurrent sends minted a duplicate MessageId: {body}"
        );
    }

    // Draining the queue yields exactly the sent bodies — none lost to the
    // concurrent writes, none duplicated.
    let received = drain_queue(&app, &creds).await;
    let mut got: Vec<&str> = received
        .iter()
        .map(|m| m["Body"].as_str().unwrap())
        .collect();
    got.sort_unstable();
    let mut want: Vec<&str> = bodies.iter().map(String::as_str).collect();
    want.sort_unstable();
    assert_eq!(got, want);
}

#[actix_web::test]
async fn concurrent_receives_deliver_each_message_to_exactly_one_consumer() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    const N: usize = 10;
    for i in 0..N {
        let (status, body) = send_message(&app, &creds, &format!("claim-{i}")).await;
        assert_eq!(status, StatusCode::OK, "SendMessage failed: {body}");
    }

    // N consumers race for N messages, one message each: every claim must be
    // satisfied and no message may be handed to two consumers.
    let responses = join_all((0..N).map(|_| receive_with_max(&app, &creds, 1))).await;

    let mut ids = HashSet::new();
    let mut handles = HashSet::new();
    for (status, body) in &responses {
        assert_eq!(*status, StatusCode::OK, "ReceiveMessage failed: {body}");
        let msgs = messages(body);
        assert_eq!(
            msgs.len(),
            1,
            "each concurrent receive should claim exactly one message: {body}"
        );
        assert!(
            ids.insert(msgs[0]["MessageId"].to_string()),
            "message delivered to two consumers at once: {body}"
        );
        assert!(
            handles.insert(msgs[0]["ReceiptHandle"].as_str().unwrap().to_string()),
            "receipt handle reused across consumers: {body}"
        );
    }
    assert_eq!(ids.len(), N);

    // Everything is now in flight; nothing is left to claim.
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        messages(&body).is_empty(),
        "all messages should be in flight: {body}"
    );
}

#[actix_web::test]
async fn concurrent_deletes_of_one_receipt_handle_succeed_exactly_once() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "contested-ack").await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = receive_messages(&app, &creds).await;
    let handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Five acknowledgers race with the same receipt handle: exactly one may
    // win; the rest must observe the handle as already consumed.
    let responses = join_all((0..5).map(|_| delete_message(&app, &creds, &handle))).await;

    let statuses: Vec<StatusCode> = responses.iter().map(|(status, _)| *status).collect();
    assert_eq!(
        statuses.iter().filter(|s| **s == StatusCode::OK).count(),
        1,
        "exactly one concurrent delete should win: {statuses:?}"
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::NOT_FOUND)
            .count(),
        4,
        "losing deletes should report the handle as gone: {statuses:?}"
    );

    // The message was acknowledged once and is gone for good.
    expire_inflight(&data).await;
    let (_, body) = receive_messages(&app, &creds).await;
    assert!(messages(&body).is_empty(), "queue should be empty: {body}");
}

#[actix_web::test]
async fn interleaved_sends_and_receives_deliver_every_message_exactly_once() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    // Seed the queue so every racing receive has something to claim no matter
    // how the interleaving with the concurrent sends plays out.
    let seed_bodies: Vec<String> = (0..5).map(|i| format!("seed-{i}")).collect();
    for body in &seed_bodies {
        let (status, resp) = send_message(&app, &creds, body).await;
        assert_eq!(status, StatusCode::OK, "SendMessage failed: {resp}");
    }

    let live_bodies: Vec<String> = (0..5).map(|i| format!("live-{i}")).collect();

    // Producers and consumers run against the queue at the same time.
    let (send_responses, recv_responses) = join(
        join_all(live_bodies.iter().map(|body| send_message(&app, &creds, body))),
        join_all((0..5).map(|_| receive_with_max(&app, &creds, 1))),
    )
    .await;

    for (status, body) in &send_responses {
        assert_eq!(*status, StatusCode::OK, "SendMessage failed: {body}");
    }

    let mut received = Vec::new();
    for (status, body) in &recv_responses {
        assert_eq!(*status, StatusCode::OK, "ReceiveMessage failed: {body}");
        let msgs = messages(body);
        assert_eq!(
            msgs.len(),
            1,
            "the seeded queue should satisfy every racing receive: {body}"
        );
        received.push(msgs[0].clone());
    }

    // Drain whatever the racing receives didn't claim; combined, every message
    // must have been delivered exactly once.
    received.extend(drain_queue(&app, &creds).await);

    let mut ids = HashSet::new();
    for m in &received {
        assert!(
            ids.insert(m["MessageId"].to_string()),
            "message delivered twice: {m}"
        );
    }

    let mut got: Vec<&str> = received
        .iter()
        .map(|m| m["Body"].as_str().unwrap())
        .collect();
    got.sort_unstable();
    let mut want: Vec<&str> = seed_bodies
        .iter()
        .chain(&live_bodies)
        .map(String::as_str)
        .collect();
    want.sort_unstable();
    assert_eq!(got, want);
}

/// Sustained mixed load: a fleet of producers and a fleet of consumers hammer
/// the endpoint at the same time, with consumers acknowledging everything they
/// receive. Every message must be delivered and acknowledged exactly once.
///
/// Exactly-once is enforced structurally: a double delivery would invalidate
/// the first receipt handle, so one of the two acknowledgers would fail its
/// delete; a lost or duplicated message breaks the final body-set comparison.
#[actix_web::test]
async fn sustained_concurrent_load_is_processed_exactly_once() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 25;
    const CONSUMERS: usize = 8;
    const TOTAL: usize = PRODUCERS * PER_PRODUCER;

    // Bodies acknowledged across all consumers. Single-threaded test runtime:
    // RefCell borrows are never held across an await.
    let acked: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));

    let producers = join_all((0..PRODUCERS).map(|p| {
        let app = &app;
        let creds = &creds;
        async move {
            for i in 0..PER_PRODUCER {
                let body = format!("load-{p}-{i}");
                let (status, resp) = send_message(app, creds, &body).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "SendMessage failed under load: {resp}"
                );
            }
        }
    }));

    let consumers = join_all((0..CONSUMERS).map(|_| {
        let acked = Rc::clone(&acked);
        let app = &app;
        let creds = &creds;
        async move {
            // Poll until the whole workload is acknowledged. The attempt cap
            // only exists so a delivery bug fails the assertions below instead
            // of hanging the test; it is far above what a correct run needs.
            for _ in 0..TOTAL * 4 {
                if acked.borrow().len() >= TOTAL {
                    return;
                }

                let (status, body) = receive_with_max(app, creds, 10).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "ReceiveMessage failed under load: {body}"
                );

                for msg in messages(&body) {
                    let handle = msg["ReceiptHandle"].as_str().unwrap();
                    let (status, resp) = delete_message(app, creds, handle).await;
                    assert_eq!(
                        status,
                        StatusCode::OK,
                        "a freshly received message should always be ackable \
                         (a failure here means it was delivered twice): {resp}"
                    );
                    acked
                        .borrow_mut()
                        .push(msg["Body"].as_str().unwrap().to_string());
                }
            }
        }
    }));

    join(producers, consumers).await;

    // Every produced message was acknowledged exactly once, none lost, none
    // duplicated.
    let acked = acked.borrow();
    let mut got: Vec<&str> = acked.iter().map(String::as_str).collect();
    got.sort_unstable();
    let want_owned: Vec<String> = (0..PRODUCERS)
        .flat_map(|p| (0..PER_PRODUCER).map(move |i| format!("load-{p}-{i}")))
        .collect();
    let mut want: Vec<&str> = want_owned.iter().map(String::as_str).collect();
    want.sort_unstable();
    assert_eq!(got, want, "acknowledged messages should match the workload");

    // Nothing lingers: even after every visibility window lapses, the queue
    // has been fully drained.
    expire_inflight(&data).await;
    let (_, body) = receive_messages(&app, &creds).await;
    assert!(
        messages(&body).is_empty(),
        "queue should be empty after the load run: {body}"
    );
}

#[actix_web::test]
async fn change_visibility_to_zero_releases_the_message_immediately() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "give-it-back").await;
    assert_eq!(status, StatusCode::OK);

    // Received with a 300s visibility window...
    let (_, body) = receive_messages(&app, &creds).await;
    let first_handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // ...but setting the timeout to 0 counts from now, overriding the
    // remaining window and releasing the message immediately.
    let (status, body) = change_visibility(&app, &creds, &first_handle, 0).await;
    assert_eq!(status, StatusCode::OK, "ChangeMessageVisibility failed: {body}");

    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    let msgs = messages(&body);
    assert_eq!(msgs.len(), 1, "released message should be available: {body}");
    assert_eq!(msgs[0]["Body"], "give-it-back");
    let second_handle = msgs[0]["ReceiptHandle"].as_str().unwrap().to_string();
    assert_ne!(second_handle, first_handle, "redelivery mints a new handle");

    // The AWS-documented failure mode: once the changed window has lapsed and
    // the message was redelivered, operations with the old handle error.
    let (status, _) = delete_message(&app, &creds, &first_handle).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the first receipt handle should have been invalidated"
    );
    let (status, body) = delete_message(&app, &creds, &second_handle).await;
    assert_eq!(status, StatusCode::OK, "DeleteMessage failed: {body}");
}

#[actix_web::test]
async fn change_visibility_restamps_the_window_from_the_call_time() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "hold-it-longer").await;
    assert_eq!(status, StatusCode::OK);

    // Received with a 300s window.
    let (_, body) = receive_messages(&app, &creds).await;
    let handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Extend to 1000s. Per AWS semantics the new timeout counts from the time
    // of this call, not from the original receive.
    let (status, body) = change_visibility(&app, &creds, &handle, 1000).await;
    assert_eq!(status, StatusCode::OK, "ChangeMessageVisibility failed: {body}");

    // The deadline now lies beyond anything the original 300s window could
    // produce, proving it was re-stamped from the call time rather than
    // adjusted relative to the receive.
    let (deadline,): (i64,) = sqlx::query_as("SELECT invisible_until FROM messages")
        .fetch_one(data.db())
        .await
        .unwrap();
    let now = chrono::Utc::now().timestamp();
    assert!(
        deadline > now + 900 && deadline <= now + 1000,
        "visibility deadline should be ~1000s from the call: deadline={deadline}, now={now}"
    );

    // And the message is still in flight.
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        messages(&body).is_empty(),
        "extended message should remain invisible: {body}"
    );
}

#[actix_web::test]
async fn change_visibility_requires_an_in_flight_message() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "too-late").await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = receive_messages(&app, &creds).await;
    let handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // The visibility window lapses without an ack: the message is no longer
    // in flight, so changing its visibility errors (the AWS-documented
    // "a total of 25 seconds might result in an error" case).
    expire_inflight(&data).await;
    let (status, _) = change_visibility(&app, &creds, &handle, 60).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a lapsed window means the message is no longer in flight"
    );

    // The message itself is unharmed and redeliverable.
    let (_, body) = receive_messages(&app, &creds).await;
    assert_eq!(messages(&body).len(), 1);
}

#[actix_web::test]
async fn change_visibility_with_stale_receipt_handle_fails() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "stale-extend").await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = receive_messages(&app, &creds).await;
    let stale_handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // Redelivery invalidates the first handle.
    expire_inflight(&data).await;
    let (_, body) = receive_messages(&app, &creds).await;
    let current_handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(stale_handle, current_handle);

    let (status, _) = change_visibility(&app, &creds, &stale_handle, 60).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a superseded receipt handle should not change visibility"
    );

    // The current handle still works.
    let (status, body) = change_visibility(&app, &creds, &current_handle, 60).await;
    assert_eq!(status, StatusCode::OK, "ChangeMessageVisibility failed: {body}");
}

#[actix_web::test]
async fn change_visibility_rejects_out_of_range_timeout() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "out-of-range").await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = receive_messages(&app, &creds).await;
    let handle = messages(&body)[0]["ReceiptHandle"]
        .as_str()
        .unwrap()
        .to_string();

    // 43200s (12 hours) is the AWS maximum; one past it is a client error.
    let (status, _) = change_visibility(&app, &creds, &handle, 43201).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The rejected call must not have touched the message's window.
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        messages(&body).is_empty(),
        "message should still be in flight with its original window: {body}"
    );

    // The boundary value itself is accepted.
    let (status, body) = change_visibility(&app, &creds, &handle, 43200).await;
    assert_eq!(status, StatusCode::OK, "ChangeMessageVisibility failed: {body}");
}

/// A ReceiveMessage `VisibilityTimeout` override is bounded to 0–43200 s,
/// like ChangeMessageVisibility. A rejected call claims nothing, and the
/// check runs before the queue lookup.
#[actix_web::test]
async fn receive_rejects_visibility_override_beyond_aws_maximum() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "still available").await;
    assert_eq!(status, StatusCode::OK);

    let receive = |queue_url: &str, visibility_timeout: u64| {
        sqs_op(
            &app,
            &creds,
            "ReceiveMessage",
            serde_json::json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 10,
                "VisibilityTimeout": visibility_timeout,
            }),
        )
    };

    for visibility_timeout in [43_201, u64::MAX] {
        let (status, body) = receive(QUEUE_URL, visibility_timeout).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{visibility_timeout}: {body}");
    }

    // The rejected calls claimed nothing; the maximum itself is accepted.
    let (status, body) = receive(QUEUE_URL, 43_200).await;
    assert_eq!(status, StatusCode::OK, "ReceiveMessage failed: {body}");
    assert_eq!(messages(&body).len(), 1, "message should still be available: {body}");

    let (status, _) = receive("http://localhost:8080/api/sqs/ns/does-not-exist", 43_201).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn receive_rejects_out_of_range_max_number_of_messages() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "still available").await;
    assert_eq!(status, StatusCode::OK);

    // AWS accepts 1–10. u64::MAX used to wrap to a negative SQL LIMIT,
    // which SQLite treats as unbounded.
    for max in [0, 11, u64::MAX] {
        let (status, body) = receive_with_max(&app, &creds, max).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "MaxNumberOfMessages={max}: {body}");
    }

    // The rejected calls claimed nothing, and the upper bound is accepted.
    let (status, body) = receive_with_max(&app, &creds, 10).await;
    assert_eq!(status, StatusCode::OK, "ReceiveMessage failed: {body}");
    assert_eq!(messages(&body).len(), 1, "message should still be available: {body}");

    // The bound is checked before the queue lookup: 400, not 404.
    let (status, _) = sqs_op(
        &app,
        &creds,
        "ReceiveMessage",
        serde_json::json!({
            "QueueUrl": "http://localhost:8080/api/sqs/ns/does-not-exist",
            "MaxNumberOfMessages": 11,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// WaitTimeSeconds is bounded to 0–20 s and rejected beyond it, as on AWS
/// (it used to be clamped to 20), before the queue lookup.
#[actix_web::test]
async fn receive_rejects_wait_time_beyond_aws_maximum() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let (status, _) = send_message(&app, &creds, "ready").await;
    assert_eq!(status, StatusCode::OK);

    let receive = |queue_url: &str, wait: u64| {
        sqs_op(
            &app,
            &creds,
            "ReceiveMessage",
            serde_json::json!({ "QueueUrl": queue_url, "WaitTimeSeconds": wait }),
        )
    };

    let (status, body) = receive(QUEUE_URL, 21).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _) = receive("http://localhost:8080/api/sqs/ns/does-not-exist", 21).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The maximum is accepted; with a message ready it returns at once.
    let (status, body) = receive(QUEUE_URL, 20).await;
    assert_eq!(status, StatusCode::OK, "ReceiveMessage failed: {body}");
    assert_eq!(messages(&body).len(), 1, "{body}");
}

/// Sends an arbitrary signed SQS operation. Covers the operations the
/// dedicated helpers above don't (queue management, tags, attributes,
/// batches).
pub(super) async fn sqs_op<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    op: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    call(
        app,
        signed_request(
            &format!("AmazonSQS.{op}"),
            &body,
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await
}

/// Hyphens and underscores, which the UI allows in new names, survive the
/// round trip through queue URLs: GetQueueUrl builds the URL, and send and
/// receive parse the namespace and queue back out of it.
#[actix_web::test]
async fn names_with_hyphens_and_underscores_work_end_to_end() {
    let (data, _, _dir) = setup().await;
    let admin = || Identity::mock("admin@example.com".to_string());
    data.create_namespace("team-a_1", admin()).await.unwrap();
    let creds = data
        .create_token("dash".into(), "team-a_1".into(), admin())
        .await
        .unwrap();
    let app = init_app(data).await;

    let (status, body) = sqs_op(
        &app,
        &creds,
        "CreateQueue",
        serde_json::json!({"QueueName": "order-events_v2"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let url = "http://localhost:8080/api/sqs/team-a_1/order-events_v2";
    assert_eq!(body["QueueUrl"], url);

    let (status, body) = sqs_op(
        &app,
        &creds,
        "GetQueueUrl",
        serde_json::json!({"QueueName": "order-events_v2"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["QueueUrl"], url);

    let (status, body) = sqs_op(
        &app,
        &creds,
        "SendMessage",
        serde_json::json!({"QueueUrl": url, "MessageBody": "hello"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) =
        sqs_op(&app, &creds, "ReceiveMessage", serde_json::json!({"QueueUrl": url})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["Messages"][0]["Body"], "hello");
}

/// CreateQueue follows AWS's naming rule: 1 to 80 letters, digits, hyphens
/// and underscores, optionally ending in `.fifo` (counted in the 80).
#[actix_web::test]
async fn create_queue_enforces_the_aws_queue_name_rule() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let fifo_80 = format!("{}.fifo", "x".repeat(75));
    for name in ["x".repeat(80), "jobs.fifo".to_string(), fifo_80] {
        let (status, body) =
            sqs_op(&app, &creds, "CreateQueue", serde_json::json!({"QueueName": name})).await;
        assert_eq!(status, StatusCode::OK, "{name}: {body}");
    }

    let fifo_81 = format!("{}.fifo", "x".repeat(76));
    for name in [
        "x".repeat(81),
        fifo_81,
        String::new(),
        ".fifo".to_string(),
        "a.b".to_string(),
        "a b".to_string(),
        "a/b".to_string(),
        "caf\u{e9}".to_string(),
    ] {
        let (status, body) =
            sqs_op(&app, &creds, "CreateQueue", serde_json::json!({"QueueName": name})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name:?} was accepted: {body}");
        assert_eq!(body["__type"], "com.amazonaws.sqs#InvalidParameterValue", "{name:?}");
    }
}

/// A queue whose name predates the rule keeps working: re-creating it, as
/// applications do at start-up, still returns its URL.
#[actix_web::test]
async fn create_queue_still_resolves_an_existing_queue_named_before_the_rule() {
    let (data, creds, _dir) = setup().await;
    sqlx::query(
        "INSERT INTO queues (ns, name) VALUES ((SELECT id FROM namespaces WHERE name = 'ns'), 'legacy.q')",
    )
    .execute(data.db())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO queue_configurations (queue, max_retries)
         VALUES ((SELECT id FROM queues WHERE name = 'legacy.q'), 5)",
    )
    .execute(data.db())
    .await
    .unwrap();
    let app = init_app(data).await;

    let (status, body) = sqs_op(
        &app,
        &creds,
        "CreateQueue",
        serde_json::json!({"QueueName": "legacy.q"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["QueueUrl"], "http://localhost:8080/api/sqs/ns/legacy.q");
}

#[actix_web::test]
async fn get_queue_url_returns_the_url_for_an_existing_queue() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, body) = sqs_op(&app, &creds, "GetQueueUrl", serde_json::json!({"QueueName": "q"})).await;
    assert_eq!(status, StatusCode::OK, "GetQueueUrl failed: {body}");
    assert_eq!(body["QueueUrl"].as_str().unwrap(), QUEUE_URL);
}

#[actix_web::test]
async fn get_queue_url_for_a_missing_queue_fails() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, _) = sqs_op(
        &app,
        &creds,
        "GetQueueUrl",
        serde_json::json!({"QueueName": "does-not-exist"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn create_queue_returns_its_url_and_the_queue_is_usable() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, body) = sqs_op(&app, &creds, "CreateQueue", serde_json::json!({"QueueName": "q2"})).await;
    assert_eq!(status, StatusCode::OK, "CreateQueue failed: {body}");
    let url = body["QueueUrl"].as_str().unwrap().to_string();
    assert!(url.ends_with("/api/sqs/ns/q2"), "unexpected queue url: {url}");

    // The new queue accepts and yields messages independently of `q`.
    let (status, body) = sqs_op(
        &app,
        &creds,
        "SendMessage",
        serde_json::json!({"QueueUrl": url, "MessageBody": "to-q2"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "SendMessage to new queue failed: {body}");

    let (status, body) = sqs_op(
        &app,
        &creds,
        "ReceiveMessage",
        serde_json::json!({"QueueUrl": url, "MaxNumberOfMessages": 10}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ReceiveMessage from new queue failed: {body}");
    assert_eq!(messages(&body).len(), 1);
    assert_eq!(messages(&body)[0]["Body"].as_str().unwrap(), "to-q2");

    // The original queue is unaffected.
    let (_, body) = receive_messages(&app, &creds).await;
    assert!(messages(&body).is_empty());
}

#[actix_web::test]
async fn list_queues_returns_queue_urls_with_optional_prefix_filter() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    for name in ["q-jobs", "worker"] {
        let (status, body) =
            sqs_op(&app, &creds, "CreateQueue", serde_json::json!({"QueueName": name})).await;
        assert_eq!(status, StatusCode::OK, "CreateQueue failed: {body}");
    }

    let (status, body) = sqs_op(&app, &creds, "ListQueues", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK, "ListQueues failed: {body}");
    let urls: HashSet<&str> = body["QueueUrls"]
        .as_array()
        .expect("QueueUrls array")
        .iter()
        .map(|u| u.as_str().unwrap())
        .collect();
    assert_eq!(
        urls,
        HashSet::from([
            "http://localhost:8080/api/sqs/ns/q",
            "http://localhost:8080/api/sqs/ns/q-jobs",
            "http://localhost:8080/api/sqs/ns/worker",
        ])
    );

    // Prefix filtering matches `q` and `q-jobs` but not `worker`.
    let (status, body) = sqs_op(
        &app,
        &creds,
        "ListQueues",
        serde_json::json!({"QueueNamePrefix": "q"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ListQueues failed: {body}");
    let urls: HashSet<&str> = body["QueueUrls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u.as_str().unwrap())
        .collect();
    assert_eq!(
        urls,
        HashSet::from([
            "http://localhost:8080/api/sqs/ns/q",
            "http://localhost:8080/api/sqs/ns/q-jobs",
        ])
    );
}

#[actix_web::test]
async fn set_and_get_queue_attributes_roundtrip() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    // A freshly created queue has no attributes set.
    let (status, body) = sqs_op(
        &app,
        &creds,
        "GetQueueAttributes",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GetQueueAttributes failed: {body}");
    assert!(body["Attributes"]["VisibilityTimeout"].is_null());

    let (status, body) = sqs_op(
        &app,
        &creds,
        "SetQueueAttributes",
        serde_json::json!({
            "QueueUrl": QUEUE_URL,
            "Attributes": { "VisibilityTimeout": 120, "DelaySeconds": 5 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "SetQueueAttributes failed: {body}");

    let (status, body) = sqs_op(
        &app,
        &creds,
        "GetQueueAttributes",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "GetQueueAttributes failed: {body}");
    // Attribute values are carried as strings in the AWS wire format.
    assert_eq!(body["Attributes"]["VisibilityTimeout"], "120");
    assert_eq!(body["Attributes"]["DelaySeconds"], "5");

    // Updating an existing attribute overwrites rather than duplicates.
    let (status, _) = sqs_op(
        &app,
        &creds,
        "SetQueueAttributes",
        serde_json::json!({
            "QueueUrl": QUEUE_URL,
            "Attributes": { "VisibilityTimeout": 60 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = sqs_op(
        &app,
        &creds,
        "GetQueueAttributes",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert_eq!(body["Attributes"]["VisibilityTimeout"], "60");
    assert_eq!(body["Attributes"]["DelaySeconds"], "5");
}

#[actix_web::test]
async fn tag_queue_list_and_untag_roundtrip() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, body) = sqs_op(
        &app,
        &creds,
        "TagQueue",
        serde_json::json!({
            "QueueUrl": QUEUE_URL,
            "Tags": { "env": "prod", "team": "core" },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "TagQueue failed: {body}");

    let (status, body) = sqs_op(
        &app,
        &creds,
        "ListQueueTags",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "ListQueueTags failed: {body}");
    assert_eq!(body["Tags"]["env"], "prod");
    assert_eq!(body["Tags"]["team"], "core");

    let (status, body) = sqs_op(
        &app,
        &creds,
        "UntagQueue",
        serde_json::json!({"QueueUrl": QUEUE_URL, "TagKeys": ["env"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "UntagQueue failed: {body}");

    let (_, body) = sqs_op(
        &app,
        &creds,
        "ListQueueTags",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert!(body["Tags"]["env"].is_null(), "untagged key should be gone: {body}");
    assert_eq!(body["Tags"]["team"], "core");
}

#[actix_web::test]
async fn purge_queue_removes_all_messages_but_keeps_the_queue() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    for i in 0..3 {
        let (status, _) = send_message(&app, &creds, &format!("purge-{i}")).await;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, body) = sqs_op(
        &app,
        &creds,
        "PurgeQueue",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "PurgeQueue failed: {body}");
    assert_eq!(body["Success"], true);

    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(messages(&body).is_empty(), "purged queue should be empty: {body}");

    // The queue itself survives and accepts new messages.
    let (status, _) = send_message(&app, &creds, "after-purge").await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = receive_messages(&app, &creds).await;
    assert_eq!(messages(&body).len(), 1);
    assert_eq!(messages(&body)[0]["Body"].as_str().unwrap(), "after-purge");
}

/// The drain-and-swap workflow: a paused queue keeps accepting messages and
/// acknowledgements, but hands nothing out until it is resumed.
#[actix_web::test]
async fn paused_queue_accepts_and_acknowledges_but_delivers_nothing() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = || Identity::mock("admin@example.com".to_string());

    for body in ["in-flight-1", "in-flight-2"] {
        let (status, _) = send_message(&app, &creds, body).await;
        assert_eq!(status, StatusCode::OK);
    }
    let (_, body) = receive_messages(&app, &creds).await;
    let handles: Vec<String> = messages(&body)
        .iter()
        .map(|m| m["ReceiptHandle"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(handles.len(), 2);

    data.set_queue_paused("ns", "q", true, admin()).await.unwrap();

    let (status, body) = send_message(&app, &creds, "while-paused").await;
    assert_eq!(status, StatusCode::OK, "a paused queue should accept messages: {body}");
    let (status, body) = receive_messages(&app, &creds).await;
    assert_eq!(status, StatusCode::OK);
    assert!(messages(&body).is_empty(), "a paused queue handed out a message: {body}");

    // Consumers still holding messages can acknowledge them or hand them
    // back; neither a released message nor one whose visibility lapses is
    // redelivered while paused.
    let (status, body) = delete_message(&app, &creds, &handles[0]).await;
    assert_eq!(status, StatusCode::OK, "DeleteMessage failed while paused: {body}");
    let (status, body) = change_visibility(&app, &creds, &handles[1], 0).await;
    assert_eq!(status, StatusCode::OK, "ChangeMessageVisibility failed while paused: {body}");
    expire_inflight(&data).await;
    let (_, body) = receive_messages(&app, &creds).await;
    assert!(messages(&body).is_empty(), "a paused queue redelivered: {body}");

    data.set_queue_paused("ns", "q", false, admin()).await.unwrap();
    let mut bodies: Vec<String> = drain_queue(&app, &creds)
        .await
        .iter()
        .map(|m| m["Body"].as_str().unwrap().to_string())
        .collect();
    bodies.sort();
    assert_eq!(bodies, ["in-flight-2", "while-paused"]);
}

/// A consumer long-polling a paused queue gets its message as soon as the
/// queue resumes, not at the end of its wait.
#[actix_web::test]
async fn long_poll_on_a_paused_queue_delivers_once_it_resumes() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let admin = || Identity::mock("admin@example.com".to_string());

    data.set_queue_paused("ns", "q", true, admin()).await.unwrap();
    let (status, _) = send_message(&app, &creds, "waiting").await;
    assert_eq!(status, StatusCode::OK);

    let started = std::time::Instant::now();
    // Timed inside the future: `join!` itself waits for the resume too.
    let poll = async {
        let response = call(
            &app,
            signed_request(
                "AmazonSQS.ReceiveMessage",
                &serde_json::json!({ "QueueUrl": QUEUE_URL, "WaitTimeSeconds": 10 }),
                &creds.access_key,
                &creds.secret_key,
            ),
        )
        .await;
        (response, started.elapsed())
    };
    let resume = async {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        data.set_queue_paused("ns", "q", false, admin()).await.unwrap();
    };
    let (((status, body), elapsed), ()) = tokio::join!(poll, resume);

    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body).len(), 1, "{body}");
    assert_eq!(messages(&body)[0]["Body"], "waiting");
    assert!(
        elapsed >= std::time::Duration::from_millis(500),
        "the poll returned before the queue resumed: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "the poll waited out its timeout: {elapsed:?}"
    );
}

#[actix_web::test]
async fn delete_queue_removes_the_queue() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, body) = sqs_op(
        &app,
        &creds,
        "DeleteQueue",
        serde_json::json!({"QueueUrl": QUEUE_URL}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "DeleteQueue failed: {body}");

    let (status, _) = send_message(&app, &creds, "into-the-void").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deleted queue should not accept messages");

    let (status, _) = sqs_op(&app, &creds, "GetQueueUrl", serde_json::json!({"QueueName": "q"})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn send_message_batch_enqueues_every_entry_exactly_once() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let bodies: Vec<String> = (0..3).map(|i| format!("batch-{i}")).collect();
    let entries: Vec<serde_json::Value> = bodies
        .iter()
        .enumerate()
        .map(|(i, body)| serde_json::json!({"Id": i.to_string(), "MessageBody": body}))
        .collect();

    let (status, body) = sqs_op(
        &app,
        &creds,
        "SendMessageBatch",
        serde_json::json!({"QueueUrl": QUEUE_URL, "Entries": entries}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "SendMessageBatch failed: {body}");

    let successful = body["Successful"].as_array().expect("Successful array");
    assert_eq!(successful.len(), bodies.len());
    assert_eq!(body["Failed"].as_array().expect("Failed array").len(), 0);

    // Each entry is acknowledged under its caller-assigned id with the MD5 of
    // its own body, as the AWS SDKs use these to correlate batch results.
    for (i, body) in bodies.iter().enumerate() {
        let entry = successful
            .iter()
            .find(|e| e["Id"] == i.to_string())
            .unwrap_or_else(|| panic!("no result entry for id {i}"));
        assert_eq!(
            entry["MD5OfMessageBody"].as_str().unwrap(),
            format!("{:x}", md5::compute(body))
        );
    }

    let received = drain_queue(&app, &creds).await;
    let received_bodies: HashSet<&str> =
        received.iter().map(|m| m["Body"].as_str().unwrap()).collect();
    assert_eq!(
        received_bodies,
        bodies.iter().map(String::as_str).collect::<HashSet<_>>()
    );
}

#[actix_web::test]
async fn operations_on_a_queue_url_outside_the_keys_namespace_are_rejected() {
    let (data, creds, _dir) = setup().await;

    // A queue in a second namespace the API key is not scoped to, even though
    // the key's owner (an admin) could access it through the management API.
    let admin = || Identity::mock("admin@example.com".to_string());
    data.create_namespace("other", admin()).await.unwrap();
    data.create_queue("other", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();

    let app = init_app(data).await;
    let foreign_url = "http://localhost:8080/api/sqs/other/q";

    let (status, _) = sqs_op(
        &app,
        &creds,
        "TagQueue",
        serde_json::json!({"QueueUrl": foreign_url, "Tags": {"env": "prod"}}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "TagQueue crossed namespaces");

    let (status, _) = sqs_op(
        &app,
        &creds,
        "GetQueueAttributes",
        serde_json::json!({"QueueUrl": foreign_url}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "GetQueueAttributes crossed namespaces"
    );
}

/// Builds an AWS-JSON request authenticated with the custom `NerveMqApiV1`
/// bearer scheme (`Authorization: NerveMqApiV1 nervemq_<key_id>_<secret>`)
/// instead of SigV4. Unlike SigV4 nothing is signed: the middleware verifies
/// the presented secret against its Argon2 hash via
/// `auth::protocols::nervemq::authenticate_api_key`.
fn bearer_request(target: &str, body: &serde_json::Value, token: &str) -> actix_http::Request {
    test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-target", target))
        .insert_header(("authorization", format!("NerveMqApiV1 {token}")))
        .set_payload(serde_json::to_vec(body).unwrap())
        .to_request()
}

#[actix_web::test]
async fn nervemq_api_key_authenticates_requests() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    // The same key minted for SigV4 works as a bearer token: the access key
    // is the key id and the secret key is the long token.
    let token = format!("nervemq_{}_{}", creds.access_key, creds.secret_key);

    let (status, body) = call(
        &app,
        bearer_request(
            "AmazonSQS.SendMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "via bearer token" }),
            &token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["MessageId"].as_str().is_some_and(|id| !id.is_empty()));

    // The whole roundtrip works without SigV4: receive the message back.
    let (status, body) = call(
        &app,
        bearer_request(
            "AmazonSQS.ReceiveMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MaxNumberOfMessages": 1 }),
            &token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["Messages"][0]["Body"], "via bearer token");
}

#[actix_web::test]
async fn nervemq_api_key_with_wrong_secret_is_rejected() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    // Right key id, wrong secret: parses fine, fails Argon2 verification.
    let token = format!("nervemq_{}_{}", creds.access_key, "WrongSecretWrongSecret12");

    let (status, _) = call(
        &app,
        bearer_request(
            "AmazonSQS.SendMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "should not land" }),
            &token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn nervemq_api_key_with_unknown_key_id_is_rejected() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let token = format!("nervemq_{}_{}", "NoSuchKey", creds.secret_key);

    let (status, _) = call(
        &app,
        bearer_request(
            "AmazonSQS.SendMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "should not land" }),
            &token,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// Like `signed_request`, but sends the request to a URI carrying a query
/// string and includes the canonical query in the signature. The canonical
/// form (keys and values url-encoded, sorted by key, bare keys as `key=`)
/// follows the SigV4 spec; the raw query is sent deliberately unsorted so
/// the test fails if the server skips sorting or encoding.
fn signed_request_with_query(
    target: &str,
    body: &serde_json::Value,
    raw_query: &str,
    canonical_query: &str,
    access_key: &str,
    secret_key: &str,
) -> actix_http::Request {
    let payload = serde_json::to_vec(body).unwrap();

    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();

    let canonical_headers =
        format!("host:{HOST}\nx-amz-date:{amz_date}\nx-amz-target:{target}\n");
    let signed_headers = "host;x-amz-date;x-amz-target";
    let payload_hash = sha256_hex(&payload);

    let canonical_request = [
        "POST",
        "/api/sqs",
        canonical_query,
        &canonical_headers,
        signed_headers,
        &payload_hash,
    ]
    .join("\n");

    let scope = format!("{date}/{REGION}/{SQS_SERVICE}/aws4_request");
    let canonical_request_hash = sha256_hex(canonical_request.as_bytes());

    let string_to_sign = [
        "AWS4-HMAC-SHA256",
        &amz_date,
        &scope,
        &canonical_request_hash,
    ]
    .join("\n");

    let signing_key = generate_signing_key(secret_key, SystemTime::now(), REGION, SQS_SERVICE);
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(signing_key.as_ref()).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize_fixed());

    test::TestRequest::post()
        .uri(&format!("/api/sqs?{raw_query}"))
        .insert_header(("host", HOST))
        .insert_header(("x-amz-date", amz_date))
        .insert_header(("x-amz-target", target))
        .insert_header((
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
                 SignedHeaders={signed_headers}, Signature={signature}"
            ),
        ))
        .set_payload(payload)
        .to_request()
}

#[actix_web::test]
async fn sigv4_canonicalizes_query_parameters() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    // Unsorted on the wire, sorted in the canonical request; `flag` has no
    // value and must canonicalize to `flag=` per the SigV4 spec.
    let (status, body) = call(
        &app,
        signed_request_with_query(
            "AmazonSQS.SendMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "signed with query" }),
            "b=2&flag&a=1",
            "a=1&b=2&flag=",
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["MessageId"].as_str().is_some_and(|id| !id.is_empty()));
}

#[actix_web::test]
async fn sigv4_rejects_signatures_that_omit_the_query_string() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    // Signature computed over an empty canonical query while the request
    // carries one: must not verify, otherwise the query string would be
    // outside the signature's integrity protection.
    let (status, _) = call(
        &app,
        signed_request_with_query(
            "AmazonSQS.SendMessage",
            &serde_json::json!({ "QueueUrl": QUEUE_URL, "MessageBody": "should not land" }),
            "a=1",
            "",
            &creds.access_key,
            &creds.secret_key,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Members, owners and admins
// ---------------------------------------------------------------------------

/// Creates a plain member of `ns` (not an owner, not an admin) and a key for
/// them.
async fn member_key(svc: &Service, email: &str) -> CreateTokenResponse {
    svc.create_user(
        email.try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(crate::api::auth::Role::User),
        vec!["ns".into()],
    )
    .await
    .unwrap();
    svc.create_token("k".into(), "ns".into(), Identity::mock(email.to_string()))
        .await
        .unwrap()
}

#[actix_web::test]
async fn member_keys_send_and_receive_but_cannot_manage_queues() {
    let (data, _, _dir) = setup().await;
    let member = member_key(&data, "member@example.com").await;
    let app = init_app(data).await;

    for (op, body) in [
        ("SendMessage", serde_json::json!({"QueueUrl": QUEUE_URL, "MessageBody": "hi"})),
        ("ReceiveMessage", serde_json::json!({"QueueUrl": QUEUE_URL})),
        ("GetQueueAttributes", serde_json::json!({"QueueUrl": QUEUE_URL, "AttributeNames": ["All"]})),
        ("ListQueues", serde_json::json!({})),
        ("GetQueueUrl", serde_json::json!({"QueueName": "q"})),
        ("ListQueueTags", serde_json::json!({"QueueUrl": QUEUE_URL})),
    ] {
        let (status, body) = sqs_op(&app, &member, op, body).await;
        assert_eq!(status, StatusCode::OK, "{op}: {body}");
    }

    for (op, body) in [
        ("CreateQueue", serde_json::json!({"QueueName": "new"})),
        ("DeleteQueue", serde_json::json!({"QueueUrl": QUEUE_URL})),
        ("PurgeQueue", serde_json::json!({"QueueUrl": QUEUE_URL})),
        ("SetQueueAttributes", serde_json::json!({"QueueUrl": QUEUE_URL, "Attributes": {"DelaySeconds": "1"}})),
        ("TagQueue", serde_json::json!({"QueueUrl": QUEUE_URL, "Tags": {"team": "a"}})),
        ("UntagQueue", serde_json::json!({"QueueUrl": QUEUE_URL, "TagKeys": ["team"]})),
    ] {
        let (status, body) = sqs_op(&app, &member, op, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{op} allowed a member: {body}");
        assert_eq!(body["__type"], "com.amazonaws.sqs#AccessDeniedException", "{op}");
    }

    // Nothing the refused calls asked for happened: the queue is still there
    // with its message, and no queue "new" was made.
    let (status, body) = sqs_op(&app, &member, "ListQueues", serde_json::json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["QueueUrls"], serde_json::json!([QUEUE_URL]));
}

#[actix_web::test]
async fn owner_keys_manage_queues() {
    let (data, _, _dir) = setup().await;
    data.create_user(
        "owner@example.com".try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(crate::api::auth::Role::User),
        vec![],
    )
    .await
    .unwrap();
    data.set_namespace_owner("ns", &"owner@example.com".try_into().unwrap(), true)
        .await
        .unwrap();
    // Minted once the user owns the namespace: a key gets its owner's level
    // when created and does not gain more if they are promoted later.
    let owner = data
        .create_token("k".into(), "ns".into(), Identity::mock("owner@example.com".into()))
        .await
        .unwrap();
    assert_eq!(owner.access, KeyAccess::Owner);
    let app = init_app(data).await;

    let new_url = "http://localhost:8080/api/sqs/ns/new";
    for (op, body) in [
        ("CreateQueue", serde_json::json!({"QueueName": "new"})),
        ("SetQueueAttributes", serde_json::json!({"QueueUrl": new_url, "Attributes": {"DelaySeconds": "1"}})),
        ("TagQueue", serde_json::json!({"QueueUrl": new_url, "Tags": {"team": "a"}})),
        ("PurgeQueue", serde_json::json!({"QueueUrl": new_url})),
        ("DeleteQueue", serde_json::json!({"QueueUrl": new_url})),
    ] {
        let (status, body) = sqs_op(&app, &owner, op, body).await;
        assert_eq!(status, StatusCode::OK, "{op}: {body}");
    }
}

#[actix_web::test]
async fn admins_reach_namespaces_without_a_permission_row() {
    let (data, _, _dir) = setup().await;
    data.create_user(
        "admin2@example.com".try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(crate::api::auth::Role::Admin),
        vec![],
    )
    .await
    .unwrap();
    // No grant on `ns`, yet the admin can mint a key for it and manage it.
    let creds = data
        .create_token("k".into(), "ns".into(), Identity::mock("admin2@example.com".into()))
        .await
        .unwrap();
    let app = init_app(data).await;

    for (op, body) in [
        ("SendMessage", serde_json::json!({"QueueUrl": QUEUE_URL, "MessageBody": "hi"})),
        ("CreateQueue", serde_json::json!({"QueueName": "new"})),
    ] {
        let (status, body) = sqs_op(&app, &creds, op, body).await;
        assert_eq!(status, StatusCode::OK, "{op}: {body}");
    }
}

#[actix_web::test]
async fn disabled_users_keys_stop_working_until_reenabled() {
    let (data, _, _dir) = setup().await;
    let member = member_key(&data, "member@example.com").await;
    let app = init_app(data.clone()).await;
    let send = serde_json::json!({"QueueUrl": QUEUE_URL, "MessageBody": "hi"});

    // Resolved once, so the signing key is cached: disabling must evict it.
    let (status, _) = sqs_op(&app, &member, "SendMessage", send.clone()).await;
    assert_eq!(status, StatusCode::OK);

    let email = "member@example.com".try_into().unwrap();
    data.set_user_disabled(&email, true).await.unwrap();
    let (status, _) = sqs_op(&app, &member, "SendMessage", send.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    data.set_user_disabled(&email, false).await.unwrap();
    let (status, _) = sqs_op(&app, &member, "SendMessage", send).await;
    assert_eq!(status, StatusCode::OK);
}

#[actix_web::test]
async fn delete_queue_stays_in_the_keys_namespace() {
    let (data, creds, _dir) = setup().await;
    let admin = || Identity::mock("admin@example.com".to_string());
    data.create_namespace("other", admin()).await.unwrap();
    data.create_queue("other", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();
    let app = init_app(data.clone()).await;

    // The key is for `ns`; its owner (the admin) could delete `other/q`, but
    // the key must not.
    let (status, _) = sqs_op(
        &app,
        &creds,
        "DeleteQueue",
        serde_json::json!({"QueueUrl": "http://localhost:8080/api/sqs/other/q"}),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(data.get_queue_id("other", "q", data.db()).await.unwrap().is_some());
}

// ---------------------------------------------------------------------------
// API key access levels
// ---------------------------------------------------------------------------

/// A key for `ns` owned by the admin, restricted to `access`.
async fn admin_key(svc: &Service, name: &str, access: KeyAccess) -> CreateTokenResponse {
    svc.create_token_with(
        name.into(),
        "ns".into(),
        Identity::mock("admin@example.com".to_string()),
        None,
        Some(access),
    )
    .await
    .unwrap()
}

#[actix_web::test]
async fn member_access_keys_only_send_and_receive_even_for_an_admin() {
    let (data, _, _dir) = setup().await;
    let member = admin_key(&data, "member", KeyAccess::Member).await;
    let owner = admin_key(&data, "owner", KeyAccess::Owner).await;
    let app = init_app(data).await;

    let send = serde_json::json!({"QueueUrl": QUEUE_URL, "MessageBody": "hi"});
    let (status, body) = sqs_op(&app, &member, "SendMessage", send).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let create = serde_json::json!({"QueueName": "new"});
    let (status, body) = sqs_op(&app, &member, "CreateQueue", create.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["__type"], "com.amazonaws.sqs#AccessDeniedException");

    // The same admin's owner-level key may.
    let (status, body) = sqs_op(&app, &owner, "CreateQueue", create).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[actix_web::test]
async fn a_keys_access_cannot_exceed_its_owners() {
    let (data, _, _dir) = setup().await;
    data.create_user(
        "member@example.com".try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(crate::api::auth::Role::User),
        vec!["ns".into()],
    )
    .await
    .unwrap();
    let mint = |user: &str, access: KeyAccess| {
        let data = data.clone();
        let user = user.to_string();
        async move {
            data.create_token_with(
                format!("{user}-{}", access.as_str()),
                "ns".into(),
                Identity::mock(user),
                None,
                Some(access),
            )
            .await
        }
    };

    for access in [KeyAccess::Owner, KeyAccess::Admin] {
        assert!(
            matches!(
                mint("member@example.com", access).await,
                Err(crate::error::Error::Forbidden { .. })
            ),
            "a member minted a {access:?} key"
        );
    }
    assert!(mint("member@example.com", KeyAccess::Member).await.is_ok());

    data.set_namespace_owner("ns", &"member@example.com".try_into().unwrap(), true)
        .await
        .unwrap();
    assert!(matches!(
        mint("member@example.com", KeyAccess::Admin).await,
        Err(crate::error::Error::Forbidden { .. })
    ));
    assert!(mint("member@example.com", KeyAccess::Owner).await.is_ok());

    assert!(mint("admin@example.com", KeyAccess::Admin).await.is_ok());
}

/// The access level caps the owner's level and never raises it: an
/// owner-level key stops managing queues once its owner loses ownership.
#[actix_web::test]
async fn a_key_never_does_more_than_its_owner_now_can() {
    let (data, _, _dir) = setup().await;
    data.create_user(
        "lead@example.com".try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(crate::api::auth::Role::User),
        vec![],
    )
    .await
    .unwrap();
    let lead = "lead@example.com".try_into().unwrap();
    data.set_namespace_owner("ns", &lead, true).await.unwrap();
    let key = data
        .create_token("k".into(), "ns".into(), Identity::mock("lead@example.com".into()))
        .await
        .unwrap();
    assert_eq!(key.access, KeyAccess::Owner);
    let app = init_app(data.clone()).await;

    let (status, _) =
        sqs_op(&app, &key, "CreateQueue", serde_json::json!({"QueueName": "a"})).await;
    assert_eq!(status, StatusCode::OK);

    data.set_namespace_owner("ns", &lead, false).await.unwrap();
    let (status, _) =
        sqs_op(&app, &key, "CreateQueue", serde_json::json!({"QueueName": "b"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// One signed SQS call, with an optional `X-Amzn-Trace-Id` header (outside
/// the signature, as SDKs send it).
async fn sqs_call<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    target: &str,
    body: serde_json::Value,
    trace_id_header: Option<&'static str>,
) -> (StatusCode, serde_json::Value)
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let mut request = signed_request(target, &body, &creds.access_key, &creds.secret_key);
    if let Some(value) = trace_id_header {
        request.headers_mut().insert(
            actix_web::http::header::HeaderName::from_static("x-amzn-trace-id"),
            actix_web::http::header::HeaderValue::from_static(value),
        );
    }
    call(app, request).await
}

const TRACE_HEADER: &str =
    "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
const OTHER_TRACE_HEADER: &str =
    "Root=1-67891233-abcdef012345678912345678;Parent=463ac35c9f6413ad;Sampled=0";

/// Receives everything with the given system attribute names, releasing it
/// again straight away (visibility 0) so the next receive sees it too.
async fn received_system_attributes<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    names: serde_json::Value,
) -> Vec<serde_json::Value>
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let (status, body) = sqs_call(
        app,
        creds,
        "AmazonSQS.ReceiveMessage",
        json!({
            "QueueUrl": QUEUE_URL,
            "MaxNumberOfMessages": 10,
            "VisibilityTimeout": 0,
            "AttributeNames": names,
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    messages(&body)
        .iter()
        .map(|message| message["Attributes"].clone())
        .collect()
}

/// MD5OfMessageAttributes as AWS computes it, on send, batch send and
/// receive, and left out when there is nothing to digest. The digest of
/// these three attributes is moto's (see `types::attribute_digest_tests`).
#[actix_web::test]
async fn attribute_digests_match_aws() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;
    let attributes = json!({
        "traceparent": {
            "DataType": "String",
            "StringValue": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        },
        "count": { "DataType": "Number", "StringValue": "42" },
        "blob": { "DataType": "Binary", "BinaryValue": "AAEC/w==" },
    });
    const DIGEST: &str = "ff985ad1603ea377a2934ea42c7253c7";

    let (status, sent) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "a", "MessageAttributes": attributes }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{sent}");
    assert_eq!(sent["MD5OfMessageAttributes"], DIGEST);
    assert!(sent.get("MD5OfMessageSystemAttributes").is_none(), "{sent}");

    let (status, plain) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "b" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(plain.get("MD5OfMessageAttributes").is_none(), "{plain}");

    let (status, batch) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessageBatch",
        json!({ "QueueUrl": QUEUE_URL, "Entries": [
            { "Id": "with", "MessageBody": "c", "MessageAttributes": attributes },
            { "Id": "without", "MessageBody": "d" },
        ]}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{batch}");
    let entry = |id: &str| {
        batch["Successful"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["Id"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(entry("with")["MD5OfMessageAttributes"], DIGEST);
    assert!(entry("without").get("MD5OfMessageAttributes").is_none());

    // On receive the digest covers the attributes returned.
    let (status, received) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({ "QueueUrl": QUEUE_URL, "MaxNumberOfMessages": 10, "MessageAttributeNames": ["All"] }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for message in messages(&received) {
        match message["Body"].as_str().unwrap() {
            "a" | "c" => assert_eq!(message["MD5OfMessageAttributes"], DIGEST),
            _ => assert!(message.get("MD5OfMessageAttributes").is_none(), "{message}"),
        }
    }
}

/// `AWSTraceHeader`: set on the message, else taken from the request's
/// `X-Amzn-Trace-Id`, and returned only to consumers that ask for it.
#[actix_web::test]
async fn aws_trace_header_is_stored_and_returned_when_asked_for() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;
    let system = |value: &str| json!({ "AWSTraceHeader": { "DataType": "String", "StringValue": value } });

    // Explicit, with the request header too: the message's own one wins.
    let (status, sent) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "explicit", "MessageSystemAttributes": system(TRACE_HEADER) }),
        Some(OTHER_TRACE_HEADER),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{sent}");
    assert_eq!(sent["MD5OfMessageSystemAttributes"], "5ae4d5d7636402d80f4eb6d213245a88");

    // Only the request header.
    let (status, sent) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "from-header" }),
        Some(OTHER_TRACE_HEADER),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // The digest covers what the request set, which was nothing.
    assert!(sent.get("MD5OfMessageSystemAttributes").is_none(), "{sent}");

    // A batch: one entry sets its own, the other takes the header.
    let (status, batch) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessageBatch",
        json!({ "QueueUrl": QUEUE_URL, "Entries": [
            { "Id": "own", "MessageBody": "batch-own", "MessageSystemAttributes": system(TRACE_HEADER) },
            { "Id": "header", "MessageBody": "batch-header" },
        ]}),
        Some(OTHER_TRACE_HEADER),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{batch}");

    // Neither.
    let (status, _) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "none" }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, received) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({
            "QueueUrl": QUEUE_URL,
            "MaxNumberOfMessages": 10,
            "VisibilityTimeout": 0,
            "MessageSystemAttributeNames": ["AWSTraceHeader"],
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let headers: HashMap<String, Option<String>> = messages(&received)
        .iter()
        .map(|message| {
            (
                message["Body"].as_str().unwrap().to_owned(),
                message["Attributes"]["AWSTraceHeader"].as_str().map(str::to_owned),
            )
        })
        .collect();
    assert_eq!(
        headers,
        HashMap::from([
            ("explicit".to_owned(), Some(TRACE_HEADER.to_owned())),
            ("from-header".to_owned(), Some(OTHER_TRACE_HEADER.to_owned())),
            ("batch-own".to_owned(), Some(TRACE_HEADER.to_owned())),
            ("batch-header".to_owned(), Some(OTHER_TRACE_HEADER.to_owned())),
            ("none".to_owned(), None),
        ])
    );

    // `All` includes it; asking for something else doesn't.
    let with_all = received_system_attributes(&app, &creds, json!(["All"])).await;
    assert!(with_all.iter().any(|a| a["AWSTraceHeader"] == TRACE_HEADER));
    let with_other = received_system_attributes(&app, &creds, json!(["SentTimestamp"])).await;
    assert!(with_other.iter().all(|a| a.get("AWSTraceHeader").is_none()), "{with_other:?}");
    assert!(with_other.iter().all(|a| a.get("SentTimestamp").is_some()));
}

#[actix_web::test]
async fn other_system_attributes_are_refused() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    for (case, system) in [
        ("another name", json!({ "SenderId": { "DataType": "String", "StringValue": "me" } })),
        ("not a string", json!({ "AWSTraceHeader": { "DataType": "Number", "StringValue": "1" } })),
        ("empty", json!({ "AWSTraceHeader": { "DataType": "String", "StringValue": "" } })),
        (
            "over the cap",
            json!({ "AWSTraceHeader": {
                "DataType": "String",
                "StringValue": "x".repeat(crate::sqs::types::MAX_AWS_TRACE_HEADER_BYTES + 1),
            }}),
        ),
    ] {
        let (status, body) = sqs_call(
            &app,
            &creds,
            "AmazonSQS.SendMessage",
            json!({ "QueueUrl": QUEUE_URL, "MessageBody": "x", "MessageSystemAttributes": system }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{case}: {body}");
        assert!(body["__type"].as_str().unwrap_or_default().contains("InvalidParameterValue"), "{case}: {body}");
    }

    let (_, received) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({ "QueueUrl": QUEUE_URL, "MaxNumberOfMessages": 10 }),
        None,
    )
    .await;
    assert!(messages(&received).is_empty(), "a refused send stored a message");
}

/// As on AWS, a system attribute doesn't count towards the message's size.
#[actix_web::test]
async fn the_trace_header_does_not_count_towards_the_size_limit() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;

    let (status, body) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SetQueueAttributes",
        json!({ "QueueUrl": QUEUE_URL, "Attributes": { "MaximumMessageSize": "1024" } }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = sqs_call(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({
            "QueueUrl": QUEUE_URL,
            "MessageBody": "x".repeat(1024),
            "MessageSystemAttributes": {
                "AWSTraceHeader": { "DataType": "String", "StringValue": TRACE_HEADER }
            },
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// When the server starts stopping, a long poll answers at once, as an
/// empty queue would, rather than hold up the shutdown for up to 20 s.
#[actix_web::test]
async fn a_long_poll_answers_at_once_when_the_server_stops() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;

    let started = std::time::Instant::now();
    let poll = signed_request(
        "AmazonSQS.ReceiveMessage",
        &serde_json::json!({ "QueueUrl": QUEUE_URL, "WaitTimeSeconds": 20 }),
        &creds.access_key,
        &creds.secret_key,
    );
    let ((status, body), ()) = tokio::join!(call(&app, poll), async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        data.stopping().cancel();
    });

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("Messages").is_none_or(|m| m.as_array().unwrap().is_empty()),
        "{body}"
    );
    let took = started.elapsed();
    assert!(took < std::time::Duration::from_secs(2), "the poll waited {took:?}");
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// `SentTimestamp` is in milliseconds, as on AWS (migration 0015). A
/// message stored before falls back to its whole-second `received_at`.
#[actix_web::test]
async fn sent_timestamps_have_millisecond_precision() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let receive = |names: &[&str]| {
        signed_request(
            "AmazonSQS.ReceiveMessage",
            &json!({ "QueueUrl": QUEUE_URL, "VisibilityTimeout": 0, "AttributeNames": names }),
            &creds.access_key,
            &creds.secret_key,
        )
    };

    let before = unix_ms();
    let (status, _) = send_message(&app, &creds, "timed").await;
    let after = unix_ms();
    assert_eq!(status, StatusCode::OK);

    let timestamp = |body: &serde_json::Value, name: &str| -> u64 {
        messages(body)[0]["Attributes"][name].as_str().unwrap().parse().unwrap()
    };
    let first_receive = unix_ms();
    let (_, body) = call(&app, receive(&["All"])).await;
    let received = unix_ms();
    let sent = timestamp(&body, "SentTimestamp");
    let first = timestamp(&body, "ApproximateFirstReceiveTimestamp");
    // Whole seconds would round down to before `before`, most of the time,
    // and could put the first receive before the send.
    assert!((before..=after).contains(&sent), "{before} <= {sent} <= {after}");
    assert!((first_receive..=received).contains(&first), "{first_receive} <= {first} <= {received}");
    assert!(first >= sent);

    // The first receive's time sticks across redeliveries.
    let (_, body) = call(&app, receive(&["All"])).await;
    assert_eq!(timestamp(&body, "ApproximateFirstReceiveTimestamp"), first);

    sqlx::query("UPDATE messages SET sent_at_ms = NULL")
        .execute(data.db())
        .await
        .unwrap();
    let received_at: i64 = sqlx::query_scalar("SELECT received_at FROM messages")
        .fetch_one(data.db())
        .await
        .unwrap();
    let (_, body) = call(&app, receive(&["SentTimestamp"])).await;
    assert_eq!(
        messages(&body)[0]["Attributes"]["SentTimestamp"],
        (received_at * 1000).to_string()
    );
}
