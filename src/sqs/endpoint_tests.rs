//! HTTP-level tests for the SQS-compatible endpoint (`POST /api/sqs`).
//!
//! These exercise the full production stack — `NormalizePath`, SigV4
//! `Authentication`, identity/session, `Protected` and the `SqsApi` dispatch
//! middleware — by sending real AWS-JSON requests signed with an API key
//! minted via `Service::create_token`. They complement the service-layer
//! `visibility_tests` in `crate::service`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
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
        crypto::sha256_hex,
        middleware::{authentication::Authentication, protected_route::Protected},
        session::SqliteSessionStore,
    },
    config::Config,
    kms::memory::InMemoryKeyManager,
    service::Service,
    sqs::service::SqsApi,
};

const HOST: &str = "localhost:8080";
const REGION: &str = "us-east-1";
const SQS_SERVICE: &str = "sqs";
const QUEUE_URL: &str = "http://localhost:8080/api/sqs/ns/q";

/// Spins up a Service backed by a throwaway on-disk SQLite database with one
/// namespace (`ns`), one queue (`q`) and one API key authorized for it. The
/// returned `TempDir` must be kept alive for the duration of the test.
async fn setup() -> (Data<Service>, CreateTokenResponse, tempfile::TempDir) {
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
        .call()
        .await
        .unwrap();

    let admin = || Identity::mock("admin@example.com".to_string());

    svc.create_namespace("ns", admin()).await.unwrap();
    svc.create_queue("ns", "q", HashMap::new(), HashMap::new(), admin())
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
                web::scope("/api")
                    .service(super::service().wrap(Protected::authenticated()).wrap(SqsApi)),
            ),
    )
    .await
}

/// Signs an AWS-JSON request for `POST /api/sqs` with SigV4, mirroring the
/// canonicalization the server performs in `auth::protocols::sigv4`, and
/// returns the ready-to-send test request.
fn signed_request(
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
async fn call<S, B>(app: &S, req: actix_http::Request) -> (StatusCode, serde_json::Value)
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
    assert!(body["MessageId"].is_number(), "missing MessageId: {body}");
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
