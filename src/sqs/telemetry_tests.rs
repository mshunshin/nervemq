//! Message traces and metrics, through the production app with traces,
//! logs and metrics exported into memory (`telemetry::test_support::otel`).

use std::collections::BTreeMap;

use actix_identity::Identity;
use actix_web::{
    body::MessageBody,
    dev::{Service as ActixService, ServiceResponse},
    http::StatusCode,
};
use opentelemetry::trace::SpanId;
use opentelemetry_sdk::trace::SpanData;
use serde_json::{json, Value};

use super::{
    endpoint_tests::{call, setup_with, signed_request, QUEUE_URL},
    span_tests::production_app,
};
use crate::{api::tokens::CreateTokenResponse, telemetry::test_support::otel::Harness};

const W3C: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const OTHER_W3C: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// One signed SQS call, with headers outside the signature (as tracing
/// instrumentations add them).
async fn sqs<S, B>(
    app: &S,
    creds: &CreateTokenResponse,
    target: &str,
    body: Value,
    headers: &[(&'static str, &'static str)],
) -> Value
where
    S: ActixService<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    B: MessageBody,
{
    let mut request = signed_request(target, &body, &creds.access_key, &creds.secret_key);
    for (name, value) in headers {
        request.headers_mut().insert(
            actix_web::http::header::HeaderName::from_static(name),
            actix_web::http::header::HeaderValue::from_static(value),
        );
    }
    let (status, body) = call(app, request).await;
    assert_eq!(status, StatusCode::OK, "{target}: {body}");
    body
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}

/// A span's links, as (trace id, span id, attributes).
fn links(span: &SpanData) -> Vec<(String, String, BTreeMap<String, String>)> {
    span.links
        .links
        .iter()
        .map(|link| {
            let attributes = link
                .attributes
                .iter()
                .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                .collect();
            (
                link.span_context.trace_id().to_string(),
                link.span_context.span_id().to_string(),
                attributes,
            )
        })
        .collect()
}

/// The X-Ray form of a W3C trace and span id.
fn xray(trace_id: &str, span_id: &str) -> String {
    format!("Root=1-{}-{};Parent={span_id};Sampled=1", &trace_id[..8], &trace_id[8..])
}

const DESTINATION: (&str, &str) = ("messaging.destination.name", "ns/q");

#[actix_web::test]
async fn a_message_sent_in_a_trace_carries_it_to_its_consumers() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;

    let sent = sqs(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "twelve bytes" }),
        &[("traceparent", W3C)],
    )
    .await;
    let send = harness.span("SQS.SendMessage").await;
    assert_eq!(send.span_context.trace_id().to_string(), "0af7651916cd43dd8448eb211c80319c");
    assert_eq!(attribute(&send, "messaging.message.id"), sent["MessageId"].as_str().map(str::to_owned));
    assert_eq!(attribute(&send, "messaging.message.body.size").as_deref(), Some("12"));

    // The consumer that asks gets the producer's context: the send's span.
    let received = sqs(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageSystemAttributeNames": ["AWSTraceHeader"] }),
        &[],
    )
    .await;
    let send_span_id = send.span_context.span_id().to_string();
    assert_eq!(
        received["Messages"][0]["Attributes"]["AWSTraceHeader"],
        xray("0af7651916cd43dd8448eb211c80319c", &send_span_id)
    );

    // The receive's own trace links to it, naming the message and attempt.
    let receive = harness.span("SQS.ReceiveMessage").await;
    assert_eq!(receive.parent_span_id, SpanId::INVALID);
    assert_eq!(attribute(&receive, "messaging.batch.message_count").as_deref(), Some("1"));
    assert_eq!(
        links(&receive),
        [(
            "0af7651916cd43dd8448eb211c80319c".to_owned(),
            send_span_id,
            BTreeMap::from([
                ("messaging.message.id".to_owned(), sent["MessageId"].as_str().unwrap().to_owned()),
                ("nervemq.message.delivery_attempt".to_owned(), "1".to_owned()),
            ]),
        )]
    );

    assert_eq!(harness.total("nervemq.messages.sent", &[DESTINATION]), 1.0);
    assert_eq!(harness.total("nervemq.message.body.size", &[DESTINATION]), 1.0);
    assert_eq!(
        harness.total("nervemq.messages.delivered", &[DESTINATION, ("nervemq.redelivery", "false")]),
        1.0
    );
    assert_eq!(harness.total("nervemq.message.queue_time", &[DESTINATION]), 1.0);
}

/// A message's own context, a `traceparent` attribute or an explicit
/// `AWSTraceHeader`, is its creation context: stored, and linked from the
/// send.
#[actix_web::test]
async fn a_message_s_own_context_is_where_it_was_created() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;

    let explicit = xray("5759e988bd862e3fe1be46a994272793", "53995c3f42cd8ad8");
    for (body, message) in [
        (
            "attribute",
            json!({ "MessageAttributes": { "traceparent": { "DataType": "String", "StringValue": OTHER_W3C } } }),
        ),
        (
            "explicit",
            json!({ "MessageSystemAttributes": {
                "AWSTraceHeader": { "DataType": "String", "StringValue": explicit }
            } }),
        ),
    ] {
        let mut request = json!({ "QueueUrl": QUEUE_URL, "MessageBody": body });
        request.as_object_mut().unwrap().extend(message.as_object().unwrap().clone());
        sqs(&app, &creds, "AmazonSQS.SendMessage", request, &[("traceparent", W3C)]).await;

        let send = harness.span("SQS.SendMessage").await;
        let linked: Vec<_> = links(&send).into_iter().map(|(trace, span, _)| (trace, span)).collect();
        let expected = match body {
            "attribute" => ("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"),
            _ => ("5759e988bd862e3fe1be46a994272793", "53995c3f42cd8ad8"),
        };
        assert_eq!(linked, [(expected.0.to_owned(), expected.1.to_owned())], "{body}");
    }

    let received = sqs(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({ "QueueUrl": QUEUE_URL, "MaxNumberOfMessages": 10, "MessageSystemAttributeNames": ["AWSTraceHeader"] }),
        &[],
    )
    .await;
    let headers: BTreeMap<String, String> = received["Messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["Body"].as_str().unwrap().to_owned(),
                m["Attributes"]["AWSTraceHeader"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(headers["attribute"], xray("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"));
    assert_eq!(headers["explicit"], explicit);
}

#[actix_web::test]
async fn redeliveries_are_counted_and_linked_with_their_attempt() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;

    sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": "again" }), &[]).await;
    for _ in 0..2 {
        sqs(
            &app,
            &creds,
            "AmazonSQS.ReceiveMessage",
            json!({ "QueueUrl": QUEUE_URL, "VisibilityTimeout": 0 }),
            &[],
        )
        .await;
    }

    let receive = harness.span("SQS.ReceiveMessage").await;
    assert_eq!(links(&receive)[0].2["nervemq.message.delivery_attempt"], "2");
    let delivered = |redelivery| {
        harness.total("nervemq.messages.delivered", &[DESTINATION, ("nervemq.redelivery", redelivery)])
    };
    assert_eq!((delivered("false"), delivered("true")), (1.0, 1.0));
    // Waiting time is measured once, at the first delivery.
    assert_eq!(harness.total("nervemq.message.queue_time", &[DESTINATION]), 1.0);
}

#[actix_web::test]
async fn acknowledgements_and_visibility_changes_are_linked_and_counted() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;

    for body in ["one", "two"] {
        sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": body }), &[]).await;
    }
    let received = sqs(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({ "QueueUrl": QUEUE_URL, "MaxNumberOfMessages": 2 }),
        &[],
    )
    .await;
    let handle = |body: &str| {
        received["Messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["Body"] == body)
            .unwrap()["ReceiptHandle"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let (one, two) = (handle("one"), handle("two"));

    // "one": extended, then acknowledged. "two": released, received
    // again, then acknowledged in a batch.
    sqs(
        &app,
        &creds,
        "AmazonSQS.ChangeMessageVisibilityBatch",
        json!({ "QueueUrl": QUEUE_URL, "Entries": [
            { "Id": "one", "ReceiptHandle": one, "VisibilityTimeout": 60 },
            { "Id": "two", "ReceiptHandle": two, "VisibilityTimeout": 0 },
        ]}),
        &[],
    )
    .await;
    assert_eq!(links(&harness.span("SQS.ChangeMessageVisibilityBatch").await).len(), 2);
    let again = sqs(&app, &creds, "AmazonSQS.ReceiveMessage", json!({ "QueueUrl": QUEUE_URL }), &[]).await;
    let two = again["Messages"][0]["ReceiptHandle"].as_str().unwrap().to_owned();

    sqs(
        &app,
        &creds,
        "AmazonSQS.DeleteMessage",
        json!({ "QueueUrl": QUEUE_URL, "ReceiptHandle": one }),
        &[],
    )
    .await;
    assert_eq!(links(&harness.span("SQS.DeleteMessage").await).len(), 1);
    sqs(
        &app,
        &creds,
        "AmazonSQS.DeleteMessageBatch",
        json!({ "QueueUrl": QUEUE_URL, "Entries": [{ "Id": "two", "ReceiptHandle": two }] }),
        &[],
    )
    .await;
    let batch = harness.span("SQS.DeleteMessageBatch").await;
    assert_eq!(links(&batch).len(), 1);
    assert_eq!(attribute(&batch, "messaging.batch.message_count").as_deref(), Some("1"));

    let changed = |change| {
        harness.total("nervemq.messages.visibility_changed", &[DESTINATION, ("nervemq.visibility.change", change)])
    };
    assert_eq!((changed("extend"), changed("release")), (1.0, 1.0));
    assert_eq!(
        harness.total("nervemq.messages.removed", &[DESTINATION, ("nervemq.removal.reason", "acknowledged")]),
        2.0
    );
    assert_eq!(harness.total("nervemq.message.lifetime", &[DESTINATION]), 2.0);
    let attempts = harness.points("nervemq.message.delivery_attempts");
    assert_eq!((attempts[0].value, attempts[0].sum), (2.0, 3.0), "one took 1 delivery, two took 2");
}

#[actix_web::test]
async fn every_way_out_of_a_queue_is_counted() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data.clone()).await;
    let admin = || Identity::mock("admin@example.com".to_owned());
    let send = |body: &'static str| {
        let (app, creds) = (&app, &creds);
        async move {
            let sent = sqs(app, creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": body }), &[]).await;
            sent["MessageId"].as_str().unwrap().to_owned()
        }
    };

    let deleted = send("deleted from the UI").await;
    data.admin_delete_message("ns", "q", &deleted, admin()).await.unwrap();
    let failed = send("failed").await;
    data.admin_set_message_status("ns", "q", &failed, crate::message::MessageStatus::Failed, admin())
        .await
        .unwrap();
    assert_eq!(data.admin_clear_failed_messages("ns", "q", admin()).await.unwrap(), 1);
    send("purged").await;
    send("purged too").await;
    data.purge_queue("ns", "q", admin()).await.unwrap();

    send("expired").await;
    for statement in [
        "INSERT INTO queue_attributes (queue, k, v)
         SELECT id, 'message_retention_period', '60' FROM queues WHERE name = 'q'",
        "UPDATE messages SET received_at = received_at - 120",
    ] {
        sqlx::query(statement).execute(data.db()).await.unwrap();
    }
    crate::service::Service::sweep_retention(data.db(), data.telemetry()).await;

    let removed = |reason| {
        harness.total("nervemq.messages.removed", &[DESTINATION, ("nervemq.removal.reason", reason)])
    };
    for (reason, count) in [("admin", 1.0), ("failed_cleared", 1.0), ("purged", 2.0), ("expired", 1.0)] {
        assert_eq!(removed(reason), count, "{reason}");
    }
    assert_eq!(removed("acknowledged"), 0.0);
}

#[actix_web::test]
async fn the_gauges_report_each_queue_by_state() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data.clone()).await;

    // One message in each state.
    sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": "in flight" }), &[]).await;
    sqs(&app, &creds, "AmazonSQS.ReceiveMessage", json!({ "QueueUrl": QUEUE_URL }), &[]).await;
    let failed = sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": "failed" }), &[]).await;
    data.admin_set_message_status(
        "ns",
        "q",
        failed["MessageId"].as_str().unwrap(),
        crate::message::MessageStatus::Failed,
        Identity::mock("admin@example.com".to_owned()),
    )
    .await
    .unwrap();
    sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": "available" }), &[]).await;
    sqs(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageBody": "delayed", "DelaySeconds": 600 }),
        &[],
    )
    .await;

    data.telemetry().set_queue_gauges(data.queue_gauges().await.unwrap());
    let states: BTreeMap<String, f64> = harness
        .points("nervemq.queue.messages")
        .into_iter()
        .filter(|point| point.attributes["messaging.destination.name"] == "ns/q")
        .map(|point| (point.attributes["nervemq.message.state"].clone(), point.value))
        .collect();
    assert_eq!(
        states,
        BTreeMap::from([
            ("available".to_owned(), 1.0),
            ("in_flight".to_owned(), 1.0),
            ("delayed".to_owned(), 1.0),
            ("failed".to_owned(), 1.0),
        ])
    );
    assert_eq!(harness.points("nervemq.queue.oldest_message.age").len(), 1);
    assert_eq!(harness.total("nervemq.queue.paused", &[DESTINATION]), 0.0);

    data.set_queue_paused("ns", "q", true, Identity::mock("admin@example.com".to_owned()))
        .await
        .unwrap();
    data.telemetry().set_queue_gauges(data.queue_gauges().await.unwrap());
    assert_eq!(harness.total("nervemq.queue.paused", &[DESTINATION]), 1.0);
}

/// A receive that found nothing, in no caller's trace, leaves no trace:
/// neither its span nor its child's.
#[actix_web::test]
async fn receives_that_find_nothing_are_not_traced() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;
    let spans = || harness.finished_spans().len();

    sqs(&app, &creds, "AmazonSQS.ReceiveMessage", json!({ "QueueUrl": QUEUE_URL }), &[]).await;
    harness.settle().await;
    assert_eq!(spans(), 0, "{:#?}", harness.finished_spans());

    // In a caller's trace, it's kept.
    sqs(&app, &creds, "AmazonSQS.ReceiveMessage", json!({ "QueueUrl": QUEUE_URL }), &[("traceparent", W3C)]).await;
    let kept = harness.span("SQS.ReceiveMessage").await;
    assert_eq!(attribute(&kept, "messaging.batch.message_count").as_deref(), Some("0"));

    // One that finds something is kept, with its child.
    sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": "x" }), &[]).await;
    harness.settle().await;
    let before = spans();
    sqs(&app, &creds, "AmazonSQS.ReceiveMessage", json!({ "QueueUrl": QUEUE_URL }), &[]).await;
    harness.settle().await;
    let new: Vec<String> = harness.finished_spans()[before..].iter().map(|s| s.name.to_string()).collect();
    assert_eq!(new, ["authenticate_sigv4", "SQS.ReceiveMessage"]);
}

/// Across a message's whole life, none of its content reaches any signal.
#[actix_web::test]
async fn no_message_content_reaches_any_signal() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;

    let body = "a-body-only-the-consumer-may-read";
    let value = "an-attribute-only-the-consumer-may-read";
    sqs(
        &app,
        &creds,
        "AmazonSQS.SendMessage",
        json!({
            "QueueUrl": QUEUE_URL,
            "MessageBody": body,
            "MessageAttributes": { "secret": { "DataType": "String", "StringValue": value } },
        }),
        &[("traceparent", W3C)],
    )
    .await;
    let received = sqs(
        &app,
        &creds,
        "AmazonSQS.ReceiveMessage",
        json!({ "QueueUrl": QUEUE_URL, "MessageAttributeNames": ["All"] }),
        &[],
    )
    .await;
    let handle = received["Messages"][0]["ReceiptHandle"].as_str().unwrap().to_owned();
    sqs(&app, &creds, "AmazonSQS.DeleteMessage", json!({ "QueueUrl": QUEUE_URL, "ReceiptHandle": handle }), &[]).await;

    harness.meter.force_flush().unwrap();
    let exported = format!(
        "{:?}\n{:?}\n{:?}",
        harness.finished_spans(),
        harness.logs.get_emitted_logs().unwrap(),
        harness.metrics.get_finished_metrics().unwrap()
    );
    assert!(exported.contains("ns/q"), "nothing was exported");
    for secret in [body, value, handle.as_str()] {
        assert!(!exported.contains(secret), "{secret} was exported");
    }
}

/// Message times are measured in milliseconds (migration 0015): a message
/// received 50 ms after it was sent waited 50 ms, not 0 s or 1 s.
#[actix_web::test]
async fn message_times_are_measured_below_a_second() {
    let (harness, _captured, _guard) = Harness::install();
    let (data, creds, _dir) = setup_with(harness.telemetry()).await;
    let app = production_app(data).await;

    sqs(&app, &creds, "AmazonSQS.SendMessage", json!({ "QueueUrl": QUEUE_URL, "MessageBody": "quick" }), &[]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let received = sqs(&app, &creds, "AmazonSQS.ReceiveMessage", json!({ "QueueUrl": QUEUE_URL }), &[]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let handle = received["Messages"][0]["ReceiptHandle"].as_str().unwrap().to_owned();
    sqs(&app, &creds, "AmazonSQS.DeleteMessage", json!({ "QueueUrl": QUEUE_URL, "ReceiptHandle": handle }), &[]).await;

    for (metric, at_least) in [("nervemq.message.queue_time", 0.05), ("nervemq.message.lifetime", 0.1)] {
        let points = harness.points(metric);
        let seconds = points[0].sum;
        assert!((at_least..1.0).contains(&seconds), "{metric}: {seconds} s");
    }
}
