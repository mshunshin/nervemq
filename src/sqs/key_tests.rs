//! API key tests over the SQS API: every way of taking access away must stop
//! a key at once, even one whose credentials are cached, and a SigV4
//! signature must cover what it claims to.

use std::{collections::HashMap, time::SystemTime};

use actix_identity::Identity;
use actix_web::{http::StatusCode, test};
use aws_sigv4::sign::v4::generate_signing_key;
use hmac::{digest::FixedOutput, Mac};
use serde_json::json;
use sha2::Sha256;

use super::endpoint_tests::{
    call, init_app, setup, signed_request, sqs_op, HOST, QUEUE_URL, REGION, SQS_SERVICE,
};
use crate::{
    api::{auth::Role, tokens::CreateTokenResponse},
    auth::{credential::KeyAccess, crypto::sha256_hex},
    service::Service,
};

const ADMIN: &str = "admin@example.com";
const USER: &str = "worker@example.com";

/// A user with member access to `ns` and a key for it.
async fn user_with_key(data: &Service) -> CreateTokenResponse {
    data.create_user(
        USER.try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(Role::User),
        vec!["ns".into()],
    )
    .await
    .unwrap();
    data.create_token("k".into(), "ns".into(), Identity::mock(USER.into()))
        .await
        .unwrap()
}

fn send() -> serde_json::Value {
    json!({"QueueUrl": QUEUE_URL, "MessageBody": "hi"})
}

// ---------------------------------------------------------------------------
// Taking access away
// ---------------------------------------------------------------------------

/// Each way of removing a user's access must stop their key on the very next
/// request. The first request caches the key's credentials and the queue
/// authorization, so a revocation that missed a cache would let it through.
#[actix_web::test]
async fn every_revocation_stops_a_cached_key_at_once() {
    type Revoke = fn(&Service) -> futures_util::future::LocalBoxFuture<'_, ()>;
    let cases: [(&str, Revoke); 6] = [
        ("user deletes the key", |d| {
            Box::pin(async move {
                d.delete_token("k", Identity::mock(USER.into())).await.unwrap();
            })
        }),
        ("admin revokes the key", |d| {
            Box::pin(async move { d.delete_user_token(USER, "k").await.unwrap() })
        }),
        ("user deleted", |d| {
            Box::pin(async move { d.delete_user(USER.try_into().unwrap()).await.unwrap() })
        }),
        ("user disabled", |d| {
            Box::pin(async move {
                d.set_user_disabled(&USER.try_into().unwrap(), true).await.unwrap()
            })
        }),
        ("grant revoked", |d| {
            Box::pin(async move {
                d.revoke_user_namespaces(&USER.try_into().unwrap(), &["ns".to_string()])
                    .await
                    .unwrap()
            })
        }),
        ("namespace deleted", |d| {
            Box::pin(async move {
                d.delete_namespace("ns", Identity::mock(ADMIN.into())).await.unwrap()
            })
        }),
    ];

    for (case, revoke) in cases {
        let (data, _, _dir) = setup().await;
        let key = user_with_key(&data).await;
        let app = init_app(data.clone()).await;

        let (status, body) = sqs_op(&app, &key, "SendMessage", send()).await;
        assert_eq!(status, StatusCode::OK, "{case}: before: {body}");

        revoke(&data).await;

        let (status, body) = sqs_op(&app, &key, "SendMessage", send()).await;
        assert!(
            status.is_client_error(),
            "{case}: the key still worked ({status}): {body}"
        );
        let delivered: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(data.db())
            .await
            .unwrap();
        assert!(delivered <= 1, "{case}: a message got in after the revocation");
    }
}

/// An admin reaches namespaces without a grant; demoting them must end that
/// for their keys at once, cached or not.
#[actix_web::test]
async fn demoting_an_admin_stops_their_grantless_keys_at_once() {
    let (data, _, _dir) = setup().await;
    data.create_user(
        "ops@example.com".try_into().unwrap(),
        "hunter2hunter2".into(),
        Some(Role::Admin),
        vec![],
    )
    .await
    .unwrap();
    let key = data
        .create_token("k".into(), "ns".into(), Identity::mock("ops@example.com".into()))
        .await
        .unwrap();
    let app = init_app(data.clone()).await;

    let (status, _) = sqs_op(&app, &key, "SendMessage", send()).await;
    assert_eq!(status, StatusCode::OK);

    data.set_user_role(&"ops@example.com".try_into().unwrap(), Role::User)
        .await
        .unwrap();
    let (status, _) = sqs_op(&app, &key, "SendMessage", send()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
async fn a_disabled_users_nervemq_scheme_key_is_rejected() {
    let (data, _, _dir) = setup().await;
    let key = user_with_key(&data).await;
    data.set_user_disabled(&USER.try_into().unwrap(), true)
        .await
        .unwrap();
    let app = init_app(data).await;

    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("x-amz-target", "AmazonSQS.SendMessage"))
        .insert_header((
            "authorization",
            format!("NerveMqApiV1 nervemq_{}_{}", key.access_key, key.secret_key),
        ))
        .set_json(send())
        .to_request();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// A key minted while its user had access keeps no access of its own: it
/// works again only if the grant comes back.
#[actix_web::test]
async fn a_regranted_users_existing_key_works_again() {
    let (data, _, _dir) = setup().await;
    let key = user_with_key(&data).await;
    let app = init_app(data.clone()).await;
    let user = USER.try_into().unwrap();

    data.revoke_user_namespaces(&user, &["ns".to_string()]).await.unwrap();
    let (status, _) = sqs_op(&app, &key, "SendMessage", send()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    data.grant_user_namespaces(&user, &["ns".to_string()]).await.unwrap();
    let (status, _) = sqs_op(&app, &key, "SendMessage", send()).await;
    assert_eq!(status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// The namespace boundary
// ---------------------------------------------------------------------------

/// A key works only in its own namespace, for every operation that names a
/// queue, even when its user may act in the other namespace.
#[actix_web::test]
async fn a_key_never_reaches_another_namespace() {
    let (data, creds, _dir) = setup().await;
    let admin = || Identity::mock(ADMIN.to_string());
    data.create_namespace("other", admin()).await.unwrap();
    data.create_queue("other", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();
    let app = init_app(data.clone()).await;
    let url = "http://localhost:8080/api/sqs/other/q";

    for (op, body) in [
        ("SendMessage", json!({"QueueUrl": url, "MessageBody": "x"})),
        ("SendMessageBatch", json!({"QueueUrl": url, "Entries": [{"Id": "1", "MessageBody": "x"}]})),
        ("ReceiveMessage", json!({"QueueUrl": url})),
        ("DeleteMessage", json!({"QueueUrl": url, "ReceiptHandle": "h"})),
        ("DeleteMessageBatch", json!({"QueueUrl": url, "Entries": [{"Id": "1", "ReceiptHandle": "h"}]})),
        ("ChangeMessageVisibility", json!({"QueueUrl": url, "ReceiptHandle": "h", "VisibilityTimeout": 0})),
        ("ChangeMessageVisibilityBatch", json!({"QueueUrl": url, "Entries": [{"Id": "1", "ReceiptHandle": "h", "VisibilityTimeout": 0}]})),
        ("GetQueueAttributes", json!({"QueueUrl": url, "AttributeNames": ["All"]})),
        ("SetQueueAttributes", json!({"QueueUrl": url, "Attributes": {"DelaySeconds": "1"}})),
        ("ListQueueTags", json!({"QueueUrl": url})),
        ("TagQueue", json!({"QueueUrl": url, "Tags": {"a": "b"}})),
        ("UntagQueue", json!({"QueueUrl": url, "TagKeys": ["a"]})),
        ("PurgeQueue", json!({"QueueUrl": url})),
        ("DeleteQueue", json!({"QueueUrl": url})),
    ] {
        let (status, body) = sqs_op(&app, &creds, op, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{op}: {body}");
    }

    // Name-based operations stay in the key's own namespace.
    let (_, body) = sqs_op(&app, &creds, "ListQueues", json!({})).await;
    assert_eq!(body["QueueUrls"], json!([QUEUE_URL]));
    let (status, body) = sqs_op(&app, &creds, "GetQueueUrl", json!({"QueueName": "missing"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["__type"], "com.amazonaws.sqs#QueueDoesNotExist");

    assert!(data.get_queue_id("other", "q", data.db()).await.unwrap().is_some());
    let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(data.db())
        .await
        .unwrap();
    assert_eq!(messages, 0);
}

// ---------------------------------------------------------------------------
// SigV4
// ---------------------------------------------------------------------------

/// The `Authorization` header for a SigV4 request whose canonical headers are
/// `headers` (lower-case, in order) and whose signed payload is `payload`.
/// Signed as a client whose clock reads the `x-amz-date` header (scope date
/// and signing key both from it), as a drifted client would; now if it has
/// none or it doesn't parse.
fn sigv4_authorization(
    target: &str,
    headers: &[(&str, &str)],
    payload: &[u8],
    access_key: &str,
    secret_key: &str,
) -> String {
    let amz_date = headers
        .iter()
        .find(|(k, _)| *k == "x-amz-date")
        .map(|(_, v)| *v)
        .unwrap_or_default();
    let signed_at = chrono::NaiveDateTime::parse_from_str(amz_date, "%Y%m%dT%H%M%SZ")
        .map(|t| t.and_utc())
        .unwrap_or_else(|_| chrono::Utc::now());
    let date = signed_at.format("%Y%m%d").to_string();
    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = headers.iter().map(|(k, _)| *k).collect::<Vec<_>>().join(";");
    let canonical_request = [
        "POST",
        "/api/sqs",
        "",
        &canonical_headers,
        &signed_headers,
        &sha256_hex(payload),
    ]
    .join("\n");
    let scope = format!("{date}/{REGION}/{SQS_SERVICE}/aws4_request");
    let string_to_sign = [
        "AWS4-HMAC-SHA256",
        amz_date,
        &scope,
        &sha256_hex(canonical_request.as_bytes()),
    ]
    .join("\n");
    let key = generate_signing_key(secret_key, SystemTime::from(signed_at), REGION, SQS_SERVICE);
    let mut mac = hmac::Hmac::<Sha256>::new_from_slice(key.as_ref()).unwrap();
    mac.update(string_to_sign.as_bytes());
    let _ = target;
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, \
         Signature={}",
        hex::encode(mac.finalize_fixed())
    )
}

#[actix_web::test]
async fn sigv4_rejects_what_the_signature_does_not_cover() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data.clone()).await;
    let target = "AmazonSQS.SendMessage";
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let signed = serde_json::to_vec(&send()).unwrap();
    let headers = [("host", HOST), ("x-amz-date", amz_date.as_str()), ("x-amz-target", target)];

    // Control: signing what is sent works with this helper.
    let auth = sigv4_authorization(target, &headers, &signed, &creds.access_key, &creds.secret_key);
    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-date", amz_date.as_str()))
        .insert_header(("x-amz-target", target))
        .insert_header(("authorization", auth.clone()))
        .set_payload(signed.clone())
        .to_request();
    let (status, body) = call(&app, req).await;
    assert_eq!(status, StatusCode::OK, "control: {body}");

    // A different body under the same signature.
    let tampered =
        serde_json::to_vec(&json!({"QueueUrl": QUEUE_URL, "MessageBody": "forged"})).unwrap();
    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-date", amz_date.as_str()))
        .insert_header(("x-amz-target", target))
        .insert_header(("authorization", auth.clone()))
        .set_payload(tampered)
        .to_request();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "tampered body accepted");

    // A signed header left off the request.
    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-target", target))
        .insert_header(("authorization", auth.clone()))
        .set_payload(signed.clone())
        .to_request();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "missing x-amz-date accepted");

    // A signed header changed after signing: here the operation itself.
    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-date", amz_date.as_str()))
        .insert_header(("x-amz-target", "AmazonSQS.PurgeQueue"))
        .insert_header(("authorization", auth))
        .set_payload(signed.clone())
        .to_request();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "re-targeted request accepted");

    // A header the signature lists but the request lacks.
    let with_extra = [
        ("host", HOST),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-security-token", "t"),
        ("x-amz-target", target),
    ];
    let auth = sigv4_authorization(target, &with_extra, &signed, &creds.access_key, &creds.secret_key);
    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-date", amz_date.as_str()))
        .insert_header(("x-amz-target", target))
        .insert_header(("authorization", auth))
        .set_payload(signed)
        .to_request();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "unsent signed header accepted");

    let forged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(data.db())
        .await
        .unwrap();
    assert_eq!(forged, 1, "only the control message may have landed");
}

#[actix_web::test]
async fn sigv4_with_an_unknown_access_key_is_rejected() {
    let (data, _, _dir) = setup().await;
    let app = init_app(data).await;
    let req = signed_request("AmazonSQS.SendMessage", &send(), "NOSUCHKEY", "whatever");
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[actix_web::test]
async fn a_member_level_key_is_refused_every_management_operation() {
    let (data, _, _dir) = setup().await;
    let key = data
        .create_token_with(
            "m".into(),
            "ns".into(),
            Identity::mock(ADMIN.into()),
            None,
            Some(KeyAccess::Member),
        )
        .await
        .unwrap();
    let app = init_app(data.clone()).await;

    for (op, body) in [
        ("CreateQueue", json!({"QueueName": "new"})),
        ("DeleteQueue", json!({"QueueUrl": QUEUE_URL})),
        ("PurgeQueue", json!({"QueueUrl": QUEUE_URL})),
        ("SetQueueAttributes", json!({"QueueUrl": QUEUE_URL, "Attributes": {"DelaySeconds": "1"}})),
        ("TagQueue", json!({"QueueUrl": QUEUE_URL, "Tags": {"a": "b"}})),
        ("UntagQueue", json!({"QueueUrl": QUEUE_URL, "TagKeys": ["a"]})),
    ] {
        let (status, body) = sqs_op(&app, &key, op, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{op}: {body}");
    }
    // Every other operation is open to it.
    for (op, body) in [
        ("SendMessage", send()),
        ("ReceiveMessage", json!({"QueueUrl": QUEUE_URL})),
        ("GetQueueAttributes", json!({"QueueUrl": QUEUE_URL})),
        ("ListQueueTags", json!({"QueueUrl": QUEUE_URL})),
        ("ListQueues", json!({})),
        ("GetQueueUrl", json!({"QueueName": "q"})),
    ] {
        let (status, body) = sqs_op(&app, &key, op, body).await;
        assert_eq!(status, StatusCode::OK, "{op}: {body}");
    }
    assert!(data.get_queue_id("ns", "q", data.db()).await.unwrap().is_some());
}

/// An `Authorization` header NerveMQ cannot parse is a failed
/// authentication (AWS's `IncompleteSignature`, a 400), never a server error.
#[actix_web::test]
async fn unparseable_authorization_headers_are_refused() {
    let (data, _, _dir) = setup().await;
    let app = init_app(data).await;

    for value in [
        "Bearer some-token".to_string(),
        "Basic dXNlcjpwYXNz".to_string(),
        "NerveMqApiV1".to_string(),
        "NerveMqApiV1 nervemq_only-two-parts".to_string(),
        "NerveMqApiV1 wrongprefix_a_b".to_string(),
        "AWS4-HMAC-SHA256 Credential=garbage".to_string(),
        String::new(),
    ] {
        let req = test::TestRequest::post()
            .uri("/api/sqs")
            .insert_header(("x-amz-target", "AmazonSQS.ListQueues"))
            .insert_header(("authorization", value.clone()))
            .set_json(json!({}))
            .to_request();
        let (status, _) = call(&app, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value:?}");
    }

    // A header value that is not visible ASCII.
    let req = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("x-amz-target", "AmazonSQS.ListQueues"))
        .insert_header((
            "authorization",
            actix_web::http::header::HeaderValue::from_bytes(b"NerveMqApiV1 \xff\xfe").unwrap(),
        ))
        .set_json(json!({}))
        .to_request();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-ASCII header");
}

// ---------------------------------------------------------------------------
// The format of authentication failures
// ---------------------------------------------------------------------------

/// (status, `x-amzn-query-error`, content type, body) of a request.
async fn call_raw<S, B>(app: &S, req: actix_http::Request) -> (StatusCode, String, String, String)
where
    S: actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse<B>,
        Error = actix_web::Error,
    >,
    B: actix_web::body::MessageBody + 'static,
{
    let resp = match test::try_call_service(app, req).await {
        Ok(resp) => resp.map_into_boxed_body().into_parts().1,
        Err(err) => err.error_response(),
    };
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    };
    let (query_error, content_type) = (header("x-amzn-query-error"), header("content-type"));
    let status = resp.status();
    let body = actix_web::body::to_bytes(resp.into_body()).await.unwrap_or_default();
    (status, query_error, content_type, String::from_utf8_lossy(&body).into_owned())
}

fn nervemq_scheme(access_key: &str, secret_key: &str) -> actix_http::Request {
    test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("x-amz-target", "AmazonSQS.ListQueues"))
        .insert_header((
            "authorization",
            format!("NerveMqApiV1 nervemq_{access_key}_{secret_key}"),
        ))
        .set_json(json!({}))
        .to_request()
}

/// Every way authentication can fail on the SQS API answers in AWS's JSON
/// error format, with the code and status AWS uses for it, so SDKs can read
/// it. It used to be a plain-text 401.
#[actix_web::test]
async fn sqs_authentication_failures_use_aws_error_codes() {
    let (data, creds, _dir) = setup().await;
    let worker = user_with_key(&data).await;
    data.set_user_disabled(&USER.try_into().unwrap(), true)
        .await
        .unwrap();
    let app = init_app(data).await;

    let unsigned = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("x-amz-target", "AmazonSQS.ListQueues"))
        .set_json(json!({}))
        .to_request();
    let garbage = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("x-amz-target", "AmazonSQS.ListQueues"))
        .insert_header(("authorization", "Bearer some-token"))
        .set_json(json!({}))
        .to_request();
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let payload = serde_json::to_vec(&json!({})).unwrap();
    let without_date = test::TestRequest::post()
        .uri("/api/sqs")
        .insert_header(("host", HOST))
        .insert_header(("x-amz-target", "AmazonSQS.ListQueues"))
        .insert_header((
            "authorization",
            sigv4_authorization(
                "AmazonSQS.ListQueues",
                &[("host", HOST), ("x-amz-date", amz_date.as_str()), ("x-amz-target", "AmazonSQS.ListQueues")],
                &payload,
                &creds.access_key,
                &creds.secret_key,
            ),
        ))
        .set_payload(payload)
        .to_request();

    use StatusCode as S;
    for (case, req, code, want) in [
        ("no credentials", unsigned, "MissingAuthenticationToken", S::FORBIDDEN),
        ("unparseable header", garbage, "IncompleteSignature", S::BAD_REQUEST),
        ("signed header not sent", without_date, "IncompleteSignature", S::BAD_REQUEST),
        (
            "unknown SigV4 key",
            signed_request("AmazonSQS.ListQueues", &json!({}), "NOSUCHKEY", "x"),
            "InvalidClientTokenId",
            S::FORBIDDEN,
        ),
        (
            "wrong SigV4 secret",
            signed_request("AmazonSQS.ListQueues", &json!({}), &creds.access_key, "wrong"),
            "SignatureDoesNotMatch",
            S::FORBIDDEN,
        ),
        (
            "disabled user's key",
            signed_request("AmazonSQS.ListQueues", &json!({}), &worker.access_key, &worker.secret_key),
            "InvalidClientTokenId",
            S::FORBIDDEN,
        ),
        (
            "unknown NerveMQ key",
            nervemq_scheme("NOSUCHKEY", "x"),
            "InvalidClientTokenId",
            S::FORBIDDEN,
        ),
        (
            "wrong NerveMQ secret",
            nervemq_scheme(&creds.access_key, "wrong"),
            "AccessDenied",
            S::FORBIDDEN,
        ),
    ] {
        let (status, query_error, content_type, body) = call_raw(&app, req).await;
        assert_eq!(status, want, "{case}");
        assert_eq!(query_error, format!("{code};Sender"), "{case}");
        assert_eq!(content_type, "application/x-amz-json-1.0", "{case}");
        let body: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|e| panic!("{case}: {e}: {body}"));
        // AWS's shape name, which only for a refusal isn't its code.
        let shape = if code == "AccessDenied" { "AccessDeniedException" } else { code };
        assert_eq!(body["__type"], format!("com.amazonaws.sqs#{shape}"), "{case}");
        assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()), "{case}");
    }
}

/// The SQS API's format stays on the SQS API: the admin API's failures are
/// still plain 401s.
#[actix_web::test]
async fn admin_api_authentication_failures_stay_plain() {
    let (data, _, _dir) = setup().await;
    let app = test::init_service(
        actix_web::App::new()
            .wrap(crate::auth::middleware::authentication::Authentication)
            .wrap(actix_identity::IdentityMiddleware::default())
            .wrap(
                actix_session::SessionMiddleware::builder(
                    crate::auth::session::SqliteSessionStore::in_memory().await,
                    actix_web::cookie::Key::generate(),
                )
                .cookie_secure(false)
                .build(),
            )
            .app_data(data)
            .service(actix_web::web::scope("/api").service(
                actix_web::web::scope("/admin").service(
                    crate::api::admin::service()
                        .wrap(crate::auth::middleware::protected_route::Protected::admin_only()),
                ),
            )),
    )
    .await;

    for auth in [None, Some("Bearer some-token")] {
        let mut req = test::TestRequest::get().uri("/api/admin/users");
        if let Some(auth) = auth {
            req = req.insert_header(("authorization", auth));
        }
        let (status, query_error, content_type, _) = call_raw(&app, req.to_request()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{auth:?}");
        assert!(query_error.is_empty(), "{auth:?}: {query_error}");
        assert_ne!(content_type, "application/x-amz-json-1.0", "{auth:?}");
    }
}

/// Clients' clocks may drift up to two hours either way (AWS allows 15
/// minutes): a client that far off, signing with its own clock, is
/// accepted at any time of day, including across midnight UTC. Further off,
/// it is refused with SignatureDoesNotMatch and AWS's "Signature expired" /
/// "not yet current" wording. See "Clock drift" in
/// docs/architecture/namespaces.md; the exact boundaries are unit-tested in
/// auth::protocols::sigv4.
#[actix_web::test]
async fn clients_may_drift_up_to_two_hours() {
    let (data, creds, _dir) = setup().await;
    let app = init_app(data).await;
    let target = "AmazonSQS.ListQueues";
    let payload = serde_json::to_vec(&json!({})).unwrap();
    let now = chrono::Utc::now();
    let minutes = chrono::Duration::minutes;

    for (drift, accepted, wording) in [
        (minutes(-115), true, ""),
        (minutes(115), true, ""),
        (minutes(-125), false, "Signature expired"),
        (minutes(125), false, "Signature not yet current"),
        (chrono::Duration::days(-365 * 27), false, "Signature expired"),
    ] {
        let amz_date = (now + drift).format("%Y%m%dT%H%M%SZ").to_string();
        let headers = [("host", HOST), ("x-amz-date", amz_date.as_str()), ("x-amz-target", target)];
        let auth =
            sigv4_authorization(target, &headers, &payload, &creds.access_key, &creds.secret_key);
        let req = test::TestRequest::post()
            .uri("/api/sqs")
            .insert_header(("host", HOST))
            .insert_header(("x-amz-date", amz_date.as_str()))
            .insert_header(("x-amz-target", target))
            .insert_header(("authorization", auth))
            .set_payload(payload.clone())
            .to_request();
        let (status, query_error, _, body) = call_raw(&app, req).await;
        if accepted {
            assert_eq!(status, StatusCode::OK, "drift {drift}: {body}");
        } else {
            assert_eq!(status, StatusCode::FORBIDDEN, "drift {drift}: {body}");
            assert_eq!(query_error, "SignatureDoesNotMatch;Sender", "drift {drift}");
            assert!(body.contains(wording), "drift {drift}: {body}");
        }
    }
}
