//! What SQS requests leave in spans and events, through the production app
//! (`crate::build_app`): the action, queue and caller on the request's span,
//! the signature check as its child, and never message content or
//! credentials, which telemetry exports.

use actix_web::{
    body::MessageBody,
    dev::{Service as ActixService, ServiceResponse},
    http::StatusCode,
    test,
    web::Data,
    HttpMessage,
};
use serde_json::json;

use super::endpoint_tests::{call, setup, signed_request, QUEUE_URL};
use crate::{
    auth::session::SqliteSessionStore, service::Service, telemetry::test_support::Captured,
};

async fn production_app(
    data: Data<Service>,
) -> impl ActixService<
    actix_http::Request,
    Response = ServiceResponse<impl MessageBody>,
    Error = actix_web::Error,
> {
    test::init_service(crate::build_app(
        data,
        SqliteSessionStore::in_memory().await,
        actix_web::cookie::Key::generate(),
    ))
    .await
}

/// The `Signature=` part of a signed request's `Authorization` header.
fn signature(request: &actix_http::Request) -> String {
    let header = request.headers().get("authorization").unwrap().to_str().unwrap();
    header.rsplit_once("Signature=").unwrap().1.to_owned()
}

#[actix_web::test]
async fn message_content_and_credentials_are_never_recorded() {
    let (data, creds, _dir) = setup().await;
    let (captured, _guard) = Captured::install();
    let app = production_app(data).await;
    let sign = |target: &str, body: serde_json::Value| {
        signed_request(target, &body, &creds.access_key, &creds.secret_key)
    };

    let body = "a-message-body-7f3a";
    let attribute = "an-attribute-value-91c2";
    let send = sign(
        "AmazonSQS.SendMessage",
        json!({
            "QueueUrl": QUEUE_URL,
            "MessageBody": body,
            "MessageAttributes": { "note": { "DataType": "String", "StringValue": attribute } },
        }),
    );
    let send_signature = signature(&send);
    assert_eq!(call(&app, send).await.0, StatusCode::OK);

    let (status, received) = call(
        &app,
        sign(
            "AmazonSQS.ReceiveMessage",
            json!({ "QueueUrl": QUEUE_URL, "MessageAttributeNames": ["All"] }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(received["Messages"][0]["Body"], body);
    let handle = received["Messages"][0]["ReceiptHandle"].as_str().unwrap().to_owned();

    for (target, request) in [
        (
            "AmazonSQS.ChangeMessageVisibility",
            json!({ "QueueUrl": QUEUE_URL, "ReceiptHandle": handle, "VisibilityTimeout": 60 }),
        ),
        (
            "AmazonSQS.DeleteMessage",
            json!({ "QueueUrl": QUEUE_URL, "ReceiptHandle": handle }),
        ),
    ] {
        assert_eq!(call(&app, sign(target, request)).await.0, StatusCode::OK, "{target}");
    }

    let recorded = captured.all_values();
    assert!(recorded.iter().any(|value| value == "ns/q"), "nothing was captured");
    for secret in [body, attribute, &handle, &send_signature, &creds.secret_key] {
        let leaks: Vec<_> = recorded.iter().filter(|value| value.contains(secret)).collect();
        assert!(leaks.is_empty(), "{secret} was recorded: {leaks:?}");
    }
}

#[actix_web::test]
async fn the_request_span_names_the_action_queue_and_caller() {
    let (data, creds, _dir) = setup().await;
    let (captured, _guard) = Captured::install();
    let app = production_app(data).await;

    let send = signed_request(
        "AmazonSQS.SendMessage",
        &json!({ "QueueUrl": QUEUE_URL, "MessageBody": "hello" }),
        &creds.access_key,
        &creds.secret_key,
    );
    assert_eq!(call(&app, send).await.0, StatusCode::OK);

    let span = captured.last_span("HTTP request").unwrap();
    for (field, value) in [
        ("otel.name", "SQS.SendMessage"),
        ("rpc.system", "aws-api"),
        ("rpc.method", "SendMessage"),
        ("messaging.system", "aws_sqs"),
        ("messaging.operation.type", "send"),
        ("messaging.destination.name", "ns/q"),
        ("nervemq.namespace", "ns"),
        ("enduser.id", "admin@example.com"),
        ("http.response.status_code", "200"),
    ] {
        assert_eq!(span.field(field).as_deref(), Some(value), "{field}");
    }
    assert_eq!(span.field("otel.status_code"), None);

    // The signature check runs inside the request's span, recording the key
    // id and nothing else from the header.
    let check = captured.last_span("authenticate_sigv4").unwrap();
    assert_eq!(check.parent.as_deref(), Some("HTTP request"));
    let fields: Vec<_> = check.fields.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(fields, ["key_id"]);
    assert_eq!(check.field("key_id"), Some(creds.access_key));
}

/// Requests the authentication middleware refuses are traced too.
#[actix_web::test]
async fn refused_requests_get_a_span() {
    let (data, creds, _dir) = setup().await;
    let (captured, _guard) = Captured::install();
    let app = production_app(data).await;

    let forged = signed_request(
        "AmazonSQS.SendMessage",
        &json!({ "QueueUrl": QUEUE_URL, "MessageBody": "hello" }),
        &creds.access_key,
        "not-the-secret",
    );
    let (status, _) = call(&app, forged).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let span = captured.last_span("HTTP request").unwrap();
    assert_eq!(span.field("otel.name").as_deref(), Some("SQS.SendMessage"));
    assert_eq!(span.field("http.response.status_code").as_deref(), Some("401"));
    // A client's mistake, not the server's failure.
    assert_eq!(span.field("otel.status_code"), None);
    assert_eq!(span.field("enduser.id"), None);
}
