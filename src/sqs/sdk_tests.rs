//! Tests driving the SQS-compatible endpoint through the official AWS Rust
//! SDK (`aws-sdk-sqs`), as a real client would (see `examples/rust` and the
//! README). Unlike `endpoint_tests`, which hand-roll SigV4 requests against
//! an in-process test service, these start an actual HTTP server on an
//! ephemeral port and let the SDK do its own signing, serialization and
//! response parsing — so they catch wire-format incompatibilities the
//! hand-rolled tests can't.

use std::collections::HashMap;

use actix_identity::{Identity, IdentityMiddleware};
use actix_session::SessionMiddleware;
use actix_web::{
    middleware::{NormalizePath, TrailingSlash},
    web::Data,
    App, HttpServer,
};
use aws_sdk_sqs::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_sqs::types::{MessageAttributeValue, QueueAttributeName};

use crate::{
    auth::{
        middleware::{authentication::Authentication, protected_route::Protected},
        session::SqliteSessionStore,
    },
    config::Config,
    kms::memory::InMemoryKeyManager,
    service::Service,
    sqs::service::SqsApi,
};

/// A running NerveMQ server plus an SDK client pointed at it. Dropping it
/// stops neither the spawned server task nor deletes the database eagerly —
/// both die with the test's runtime/tempdir.
struct SdkHarness {
    client: aws_sdk_sqs::Client,
    service: Data<Service>,
    queue_url: String,
    base_url: String,
    #[allow(unused)]
    dir: tempfile::TempDir,
}

/// Spins up the production app (same middleware stack as `lib.rs`) on an
/// ephemeral localhost port, backed by a throwaway database with one
/// namespace (`ns`), one queue (`q`) and one API key, and returns an
/// `aws-sdk-sqs` client configured exactly like the README example.
async fn setup() -> SdkHarness {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db").to_string_lossy().to_string();

    // Bind first so the service config's host — which queue URLs are derived
    // from — can reference the actual port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());

    let cfg: Config = serde_json::from_value(serde_json::json!({
        "db_path": db_path,
        "default_max_retries": 5,
        "host": base_url,
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
    svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();

    let creds = svc
        .create_token("sdk-tests".to_string(), "ns".to_string(), admin())
        .await
        .unwrap();

    let service = Data::new(svc);

    let data = service.clone();
    let session_store = SqliteSessionStore::new(data.db().clone());
    let secret_key = actix_web::cookie::Key::generate();

    let server = HttpServer::new(move || {
        App::new()
            .wrap(NormalizePath::new(TrailingSlash::Trim))
            .wrap(Authentication)
            .wrap(IdentityMiddleware::default())
            .wrap(
                SessionMiddleware::builder(session_store.clone(), secret_key.clone())
                    .cookie_secure(false)
                    .build(),
            )
            .app_data(data.clone())
            .service(
                actix_web::web::scope("/api")
                    .service(super::service().wrap(Protected::authenticated()).wrap(SqsApi)),
            )
    })
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    actix_web::rt::spawn(server);

    let sdk_config = aws_sdk_sqs::Config::builder()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds.access_key,
            creds.secret_key,
            None,
            None,
            "Static",
        ))
        .endpoint_url(format!("{base_url}/api/sqs"))
        .behavior_version(BehaviorVersion::latest())
        .build();

    SdkHarness {
        client: aws_sdk_sqs::Client::from_conf(sdk_config),
        service,
        queue_url: format!("{base_url}/api/sqs/ns/q"),
        base_url,
        dir,
    }
}

#[actix_web::test]
async fn sdk_sends_receives_and_deletes_a_message() {
    let h = setup().await;

    let sent = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("hello from the aws sdk")
        .send()
        .await
        .expect("SendMessage should succeed via the SDK");
    assert!(sent.message_id().is_some_and(|id| !id.is_empty()));
    assert_eq!(
        sent.md5_of_message_body().unwrap(),
        format!("{:x}", md5::compute("hello from the aws sdk"))
    );

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .visibility_timeout(300)
        .send()
        .await
        .expect("ReceiveMessage should succeed via the SDK");
    let messages = received.messages();
    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert_eq!(message.body().unwrap(), "hello from the aws sdk");
    assert_eq!(
        message.md5_of_body().unwrap(),
        format!("{:x}", md5::compute("hello from the aws sdk"))
    );

    h.client
        .delete_message()
        .queue_url(&h.queue_url)
        .receipt_handle(message.receipt_handle().unwrap())
        .send()
        .await
        .expect("DeleteMessage should succeed via the SDK");

    // Deleting acknowledged the message for good: nothing left to receive.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty());
}

#[actix_web::test]
async fn sdk_roundtrips_message_attributes() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("attributed")
        .message_attributes(
            "TraceId",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("abc-123")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("SendMessage with attributes should succeed via the SDK");

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(1)
        // As on AWS, message attributes only come back when requested.
        .message_attribute_names("All")
        .send()
        .await
        .unwrap();
    let message = &received.messages()[0];
    let attr = message
        .message_attributes()
        .and_then(|attrs| attrs.get("TraceId"))
        .expect("message attribute should roundtrip");
    assert_eq!(attr.data_type(), "String");
    assert_eq!(attr.string_value(), Some("abc-123"));
}

#[actix_web::test]
async fn sdk_queue_urls_resolve_and_list() {
    let h = setup().await;

    let url = h
        .client
        .get_queue_url()
        .queue_name("q")
        .send()
        .await
        .expect("GetQueueUrl should succeed via the SDK");
    assert_eq!(url.queue_url().unwrap(), h.queue_url);

    let created = h
        .client
        .create_queue()
        .queue_name("q2")
        .send()
        .await
        .expect("CreateQueue should succeed via the SDK");
    assert_eq!(
        created.queue_url().unwrap(),
        format!("{}/api/sqs/ns/q2", h.base_url)
    );

    let listed = h
        .client
        .list_queues()
        .send()
        .await
        .expect("ListQueues should succeed via the SDK");
    let mut urls: Vec<&str> = listed.queue_urls().iter().map(String::as_str).collect();
    urls.sort_unstable();
    assert_eq!(
        urls,
        vec![h.queue_url.as_str(), created.queue_url().unwrap()]
    );

    let missing = h.client.get_queue_url().queue_name("nope").send().await;
    assert!(missing.is_err(), "a missing queue should be an SDK error");
}

#[actix_web::test]
async fn sdk_sends_message_batches() {
    let h = setup().await;

    let mut batch = h.client.send_message_batch().queue_url(&h.queue_url);
    for i in 0..3 {
        batch = batch.entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(format!("batch-{i}"))
                .build()
                .unwrap(),
        );
    }
    let result = batch
        .send()
        .await
        .expect("SendMessageBatch should succeed via the SDK");

    assert!(result.failed().is_empty());
    let mut ids: Vec<&str> = result.successful().iter().map(|e| e.id()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["0", "1", "2"]);

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    let mut bodies: Vec<&str> = received
        .messages()
        .iter()
        .map(|m| m.body().unwrap())
        .collect();
    bodies.sort_unstable();
    assert_eq!(bodies, vec!["batch-0", "batch-1", "batch-2"]);
}

#[actix_web::test]
async fn sdk_queue_attributes_roundtrip() {
    let h = setup().await;

    // The AWS wire format carries attribute values as strings.
    h.client
        .set_queue_attributes()
        .queue_url(&h.queue_url)
        .attributes(QueueAttributeName::VisibilityTimeout, "120")
        .attributes(QueueAttributeName::DelaySeconds, "5")
        .send()
        .await
        .expect("SetQueueAttributes should succeed via the SDK");

    let attrs = h
        .client
        .get_queue_attributes()
        .queue_url(&h.queue_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .expect("GetQueueAttributes should succeed via the SDK");
    let attrs = attrs.attributes().expect("attributes map");
    assert_eq!(
        attrs.get(&QueueAttributeName::VisibilityTimeout).unwrap(),
        "120"
    );
    assert_eq!(attrs.get(&QueueAttributeName::DelaySeconds).unwrap(), "5");
}

#[actix_web::test]
async fn sdk_queue_tags_roundtrip() {
    let h = setup().await;

    h.client
        .tag_queue()
        .queue_url(&h.queue_url)
        .tags("env", "prod")
        .tags("team", "core")
        .send()
        .await
        .expect("TagQueue should succeed via the SDK");

    let listed = h
        .client
        .list_queue_tags()
        .queue_url(&h.queue_url)
        .send()
        .await
        .expect("ListQueueTags should succeed via the SDK");
    let tags = listed.tags().expect("tags map");
    assert_eq!(tags.get("env").unwrap(), "prod");
    assert_eq!(tags.get("team").unwrap(), "core");

    h.client
        .untag_queue()
        .queue_url(&h.queue_url)
        .tag_keys("env")
        .send()
        .await
        .expect("UntagQueue should succeed via the SDK");

    let listed = h
        .client
        .list_queue_tags()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    let tags = listed.tags().expect("tags map");
    assert!(!tags.contains_key("env"));
    assert_eq!(tags.get("team").unwrap(), "core");
}

#[actix_web::test]
async fn sdk_changes_message_visibility() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("come back soon")
        .send()
        .await
        .unwrap();

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(300)
        .send()
        .await
        .unwrap();
    let handle = received.messages()[0].receipt_handle().unwrap().to_string();

    // While in flight the message is invisible.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty());

    // Releasing it with VisibilityTimeout=0 makes it receivable again.
    h.client
        .change_message_visibility()
        .queue_url(&h.queue_url)
        .receipt_handle(&handle)
        .visibility_timeout(0)
        .send()
        .await
        .expect("ChangeMessageVisibility should succeed via the SDK");

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 1);
    assert_eq!(received.messages()[0].body().unwrap(), "come back soon");
}

#[actix_web::test]
async fn sdk_purges_and_deletes_queues() {
    let h = setup().await;

    for i in 0..2 {
        h.client
            .send_message()
            .queue_url(&h.queue_url)
            .message_body(format!("purge-{i}"))
            .send()
            .await
            .unwrap();
    }

    h.client
        .purge_queue()
        .queue_url(&h.queue_url)
        .send()
        .await
        .expect("PurgeQueue should succeed via the SDK");

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty());

    h.client
        .delete_queue()
        .queue_url(&h.queue_url)
        .send()
        .await
        .expect("DeleteQueue should succeed via the SDK");

    let send = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("into the void")
        .send()
        .await;
    assert!(send.is_err(), "a deleted queue should reject sends");

    // The database agrees the queue is gone.
    assert!(h
        .service
        .get_queue_id("ns", "q", h.service.db())
        .await
        .unwrap()
        .is_none());
}

#[actix_web::test]
async fn sdk_roundtrips_large_message_bodies() {
    let h = setup().await;

    // Well past any single network read (reads buffer at 8 KiB): the body
    // must be reassembled from multiple chunks before it can parse as JSON.
    let body = "0123456789abcdef".repeat(4096); // 64 KiB

    let sent = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body(&body)
        .send()
        .await
        .expect("a 64 KiB SendMessage should succeed via the SDK");
    assert_eq!(
        sent.md5_of_message_body().unwrap(),
        format!("{:x}", md5::compute(&body))
    );

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages()[0].body().unwrap(), body);

    // A full-size batch request (10 entries × 8 KiB ≈ 80 KiB of JSON) also
    // spans many reads.
    let entry_body = "x".repeat(8 * 1024);
    let mut batch = h.client.send_message_batch().queue_url(&h.queue_url);
    for i in 0..10 {
        batch = batch.entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(&entry_body)
                .build()
                .unwrap(),
        );
    }
    let result = batch
        .send()
        .await
        .expect("a ~80 KiB SendMessageBatch should succeed via the SDK");
    assert_eq!(result.successful().len(), 10);
}

#[actix_web::test]
async fn sdk_message_size_limits_match_aws_policy() {
    use crate::types::MAX_MESSAGE_SIZE_BYTES;

    let h = setup().await;

    // Exactly the AWS maximum (1 MiB) round-trips...
    let body = "y".repeat(MAX_MESSAGE_SIZE_BYTES);
    let sent = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body(&body)
        .send()
        .await
        .expect("a message of exactly 1 MiB should be accepted");
    assert_eq!(
        sent.md5_of_message_body().unwrap(),
        format!("{:x}", md5::compute(&body))
    );
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages()[0].body().unwrap().len(), body.len());

    // ...one byte more is rejected with 400.
    let err = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("y".repeat(MAX_MESSAGE_SIZE_BYTES + 1))
        .send()
        .await
        .expect_err("a message over 1 MiB should be rejected");
    let status = err.raw_response().map(|res| res.status().as_u16());
    assert_eq!(status, Some(400), "expected 400 Bad Request: {err:?}");

    // Message attributes count toward the limit: name + data type label +
    // value bytes push an at-the-limit body over it.
    let err = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body(&body)
        .message_attributes(
            "Tip",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("over")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect_err("attributes must count toward the 1 MiB message size");
    let status = err.raw_response().map(|res| res.status().as_u16());
    assert_eq!(status, Some(400), "expected 400 Bad Request: {err:?}");

    // The queue still works after the rejections.
    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("small and fine")
        .send()
        .await
        .expect("normal sends should still succeed after a rejected one");
}

#[actix_web::test]
async fn sdk_batch_total_payload_is_capped_at_1mib() {
    let h = setup().await;

    // Three entries of ~400 KiB are each individually legal, but their sum
    // exceeds the 1 MiB total payload limit: the whole request fails
    // (AWS: BatchRequestTooLong), not individual entries.
    let entry_body = "y".repeat(400 * 1024);
    let mut batch = h.client.send_message_batch().queue_url(&h.queue_url);
    for i in 0..3 {
        batch = batch.entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(&entry_body)
                .build()
                .unwrap(),
        );
    }
    let err = batch
        .send()
        .await
        .expect_err("a batch with a >1 MiB combined payload should be rejected");
    let status = err.raw_response().map(|res| res.status().as_u16());
    assert_eq!(status, Some(400), "expected 400 Bad Request: {err:?}");

    // Nothing from the failed batch was enqueued.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty());

    // Two entries totalling well under 1 MiB go through.
    let mut batch = h.client.send_message_batch().queue_url(&h.queue_url);
    for i in 0..2 {
        batch = batch.entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(&entry_body)
                .build()
                .unwrap(),
        );
    }
    let result = batch
        .send()
        .await
        .expect("an under-limit batch should succeed");
    assert_eq!(result.successful().len(), 2);
}

#[actix_web::test]
async fn sdk_queue_maximum_message_size_attribute_is_enforced() {
    let h = setup().await;

    h.client
        .set_queue_attributes()
        .queue_url(&h.queue_url)
        .attributes(QueueAttributeName::MaximumMessageSize, "1024")
        .send()
        .await
        .unwrap();

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("y".repeat(1024))
        .send()
        .await
        .expect("a message at the queue's MaximumMessageSize should be accepted");

    let err = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("y".repeat(1025))
        .send()
        .await
        .expect_err("a message over the queue's MaximumMessageSize should be rejected");
    let status = err.raw_response().map(|res| res.status().as_u16());
    assert_eq!(status, Some(400), "expected 400 Bad Request: {err:?}");
}

#[actix_web::test]
async fn sdk_numeric_tag_values_roundtrip_verbatim() {
    let h = setup().await;

    // Tag values that look like numbers must come back as the exact strings
    // they were set to. (queue_tags originally had NUMERIC column affinity,
    // which coerced "1" to INTEGER on insert and made ListQueueTags fail to
    // decode it — and would have collapsed "01" to "1" and "2.50" to "2.5".)
    h.client
        .tag_queue()
        .queue_url(&h.queue_url)
        .tags("tier", "1")
        .tags("version", "01")
        .tags("ratio", "2.50")
        .send()
        .await
        .expect("TagQueue with numeric-looking values should succeed");

    let listed = h
        .client
        .list_queue_tags()
        .queue_url(&h.queue_url)
        .send()
        .await
        .expect("ListQueueTags should succeed for numeric-looking tag values");
    let tags = listed.tags().expect("tags map");
    assert_eq!(tags.get("tier").unwrap(), "1");
    assert_eq!(tags.get("version").unwrap(), "01");
    assert_eq!(tags.get("ratio").unwrap(), "2.50");
}

#[actix_web::test]
async fn sdk_numeric_queue_name_roundtrips() {
    let h = setup().await;

    // A queue literally named "123" exercises the queues.name column, which
    // originally had NUMERIC affinity and stored such names as integers —
    // breaking every read that decodes the name as text.
    let created = h
        .client
        .create_queue()
        .queue_name("123")
        .send()
        .await
        .expect("CreateQueue with a numeric name should succeed");
    let queue_url = created.queue_url().unwrap().to_string();
    assert!(queue_url.ends_with("/ns/123"));

    let resolved = h
        .client
        .get_queue_url()
        .queue_name("123")
        .send()
        .await
        .expect("GetQueueUrl should resolve a numeric queue name");
    assert_eq!(resolved.queue_url().unwrap(), queue_url);

    let listed = h
        .client
        .list_queues()
        .send()
        .await
        .expect("ListQueues should decode numeric queue names");
    assert!(listed.queue_urls().iter().any(|url| url == &queue_url));

    h.client
        .send_message()
        .queue_url(&queue_url)
        .message_body("to a numeric queue")
        .send()
        .await
        .unwrap();
    let received = h
        .client
        .receive_message()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages()[0].body().unwrap(), "to a numeric queue");

    h.client
        .delete_queue()
        .queue_url(&queue_url)
        .send()
        .await
        .expect("DeleteQueue should succeed for a numeric queue name");
}

#[actix_web::test]
async fn sdk_numeric_message_attribute_name_roundtrips() {
    let h = setup().await;

    // Message attributes are stored in kv_pairs, whose key column had the
    // same NUMERIC-affinity wart: an attribute named "123" was stored as an
    // integer key and made ReceiveMessage fail to decode it.
    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("numeric attribute name")
        .message_attributes(
            "123",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("value")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("SendMessage with a numeric attribute name should succeed");

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .message_attribute_names("All")
        .send()
        .await
        .expect("ReceiveMessage should decode a numeric attribute name");
    let attr = received.messages()[0]
        .message_attributes()
        .and_then(|attrs| attrs.get("123"))
        .expect("numeric-named attribute should roundtrip");
    assert_eq!(attr.string_value(), Some("value"));
}

#[actix_web::test]
async fn sdk_delete_message_batch_deletes_per_entry() {
    let h = setup().await;

    for i in 0..3 {
        h.client
            .send_message()
            .queue_url(&h.queue_url)
            .message_body(format!("batch-delete-{i}"))
            .send()
            .await
            .unwrap();
    }
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 3);

    // Two valid handles and one bogus one: entries succeed or fail
    // independently, like AWS.
    let mut batch = h.client.delete_message_batch().queue_url(&h.queue_url);
    for (i, message) in received.messages().iter().take(2).enumerate() {
        batch = batch.entries(
            aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .receipt_handle(message.receipt_handle().unwrap())
                .build()
                .unwrap(),
        );
    }
    let batch = batch.entries(
        aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
            .id("bogus")
            .receipt_handle("0:deadbeef")
            .build()
            .unwrap(),
    );

    let result = batch
        .send()
        .await
        .expect("DeleteMessageBatch should succeed via the SDK");
    let successful: Vec<_> = result.successful().iter().map(|e| e.id()).collect();
    assert_eq!(successful, vec!["0", "1"]);
    assert_eq!(result.failed().len(), 1);
    let failure = &result.failed()[0];
    assert_eq!(failure.id(), "bogus");
    assert_eq!(failure.code(), "ReceiptHandleIsInvalid");
    assert!(failure.sender_fault());

    // Only the third (still in-flight) message remains.
    let remaining: Vec<u64> = sqlx::query_scalar("SELECT id FROM messages")
        .fetch_all(h.service.db())
        .await
        .unwrap()
        .into_iter()
        .map(|id: i64| id as u64)
        .collect();
    assert_eq!(remaining.len(), 1);
}

#[actix_web::test]
async fn sdk_delay_seconds_defers_delivery() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("delayed")
        .delay_seconds(2)
        .send()
        .await
        .expect("SendMessage with DelaySeconds should succeed");

    // Hidden while the delay runs...
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty(), "delay should hide the message");

    // ...and the delay does not count as a delivery attempt.
    let tries: i64 = sqlx::query_scalar("SELECT tries FROM messages")
        .fetch_one(h.service.db())
        .await
        .unwrap();
    assert_eq!(tries, 0);

    // Fast-forward instead of sleeping out the delay in real time.
    sqlx::query("UPDATE messages SET invisible_until = unixepoch('now') - 1")
        .execute(h.service.db())
        .await
        .unwrap();
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages()[0].body().unwrap(), "delayed");
}

#[actix_web::test]
async fn sdk_delay_seconds_over_aws_maximum_is_rejected() {
    let h = setup().await;

    let err = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("too late")
        .delay_seconds(901) // AWS maximum is 900 (15 minutes).
        .send()
        .await
        .expect_err("a delay beyond 900s should be rejected");
    let status = err.raw_response().map(|res| res.status().as_u16());
    assert_eq!(status, Some(400), "expected 400 Bad Request: {err:?}");
}

#[actix_web::test]
async fn sdk_long_polling_returns_early_when_a_message_arrives() {
    let h = setup().await;

    // Send from a concurrent task while a long poll is in flight.
    let sender = {
        let client = h.client.clone();
        let queue_url = h.queue_url.clone();
        actix_web::rt::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
            client
                .send_message()
                .queue_url(&queue_url)
                .message_body("worth the wait")
                .send()
                .await
                .unwrap();
        })
    };

    let started = std::time::Instant::now();
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .wait_time_seconds(10)
        .send()
        .await
        .expect("long poll should succeed");
    let elapsed = started.elapsed();

    assert_eq!(received.messages()[0].body().unwrap(), "worth the wait");
    assert!(
        elapsed >= std::time::Duration::from_millis(500),
        "long poll returned before the message was sent: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "long poll should return as soon as the message arrives, not at the deadline: {elapsed:?}"
    );

    sender.await.unwrap();
}

#[actix_web::test]
async fn sdk_binary_message_attribute_roundtrips() {
    let h = setup().await;

    // The AWS JSON protocol carries BinaryValue base64-encoded.
    let payload: &[u8] = &[0x00, 0x01, 0x02, 0xff, 0xfe];
    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("binary attribute")
        .message_attributes(
            "Blob",
            MessageAttributeValue::builder()
                .data_type("Binary")
                .binary_value(aws_sdk_sqs::primitives::Blob::new(payload))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("SendMessage with a Binary attribute should succeed");

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .message_attribute_names("All")
        .send()
        .await
        .unwrap();
    let attr = received.messages()[0]
        .message_attributes()
        .and_then(|attrs| attrs.get("Blob"))
        .expect("binary attribute should roundtrip");
    assert_eq!(attr.data_type(), "Binary");
    assert_eq!(attr.binary_value().unwrap().as_ref(), payload);
}

#[actix_web::test]
async fn sdk_create_queue_attributes_and_tags_are_applied() {
    let h = setup().await;

    // Create-time attributes used to be stored under their PascalCase wire
    // names (never read back), and create-time tags arrive under the
    // lowercase `tags` wire key (an AWS protocol quirk) and were dropped.
    h.client
        .create_queue()
        .queue_name("configured")
        .attributes(QueueAttributeName::VisibilityTimeout, "120")
        .attributes(QueueAttributeName::MaximumMessageSize, "2048")
        .tags("team", "core")
        .send()
        .await
        .expect("CreateQueue with attributes and tags should succeed");
    let queue_url = format!("{}/api/sqs/ns/configured", h.base_url);

    let attributes = h
        .client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .unwrap();
    let attributes = attributes.attributes().expect("attributes map");
    assert_eq!(
        attributes.get(&QueueAttributeName::VisibilityTimeout).map(String::as_str),
        Some("120")
    );
    // Round trip under the AWS name, not the abbreviated `MaxMessageSize`.
    assert_eq!(
        attributes.get(&QueueAttributeName::MaximumMessageSize).map(String::as_str),
        Some("2048")
    );

    let tags = h
        .client
        .list_queue_tags()
        .queue_url(&queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().expect("tags map").get("team").unwrap(), "core");
}

#[actix_web::test]
async fn sdk_message_system_attributes_roundtrip() {
    use aws_sdk_sqs::types::MessageSystemAttributeName;

    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("system attributes")
        .send()
        .await
        .unwrap();

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Current SDK spelling: MessageSystemAttributeNames.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(0)
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .send()
        .await
        .unwrap();
    let message = &received.messages()[0];
    let attrs = message.attributes().expect("system attributes map");

    let sent_ts: i64 = attrs
        .get(&MessageSystemAttributeName::SentTimestamp)
        .expect("SentTimestamp")
        .parse()
        .unwrap();
    assert!(
        (now_ms - sent_ts).abs() < 120_000,
        "SentTimestamp {sent_ts} should be about now ({now_ms}) in milliseconds"
    );
    assert_eq!(
        attrs
            .get(&MessageSystemAttributeName::ApproximateReceiveCount)
            .map(String::as_str),
        Some("1")
    );
    let first_ts: i64 = attrs
        .get(&MessageSystemAttributeName::ApproximateFirstReceiveTimestamp)
        .expect("ApproximateFirstReceiveTimestamp")
        .parse()
        .unwrap();
    assert!(first_ts >= sent_ts, "first receive cannot precede the send");
    // SenderId is the sending principal: the API key's owner.
    assert_eq!(
        attrs
            .get(&MessageSystemAttributeName::SenderId)
            .map(String::as_str),
        Some("admin@example.com")
    );

    // Filtering by name returns only what was asked for.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(0)
        .message_system_attribute_names(MessageSystemAttributeName::SentTimestamp)
        .send()
        .await
        .unwrap();
    let attrs = received.messages()[0].attributes().expect("attributes map");
    assert_eq!(attrs.len(), 1);
    assert!(attrs.contains_key(&MessageSystemAttributeName::SentTimestamp));

    // The deprecated `AttributeNames` spelling still selects system
    // attributes; the receive count keeps climbing and the first-receive
    // timestamp is sticky across redeliveries.
    #[allow(deprecated)]
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(0)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .unwrap();
    let attrs = received.messages()[0].attributes().expect("attributes map");
    assert_eq!(
        attrs
            .get(&MessageSystemAttributeName::ApproximateReceiveCount)
            .map(String::as_str),
        Some("3")
    );
    assert_eq!(
        attrs
            .get(&MessageSystemAttributeName::ApproximateFirstReceiveTimestamp)
            .map(|v| v.parse::<i64>().unwrap()),
        Some(first_ts),
        "first-receive timestamp must not move on redelivery"
    );

    // Nothing requested: AWS omits the attributes map entirely.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();
    assert!(received.messages()[0].attributes().is_none());
}

/// The signing-key cache must not outlive the key: deleting an API key
/// drops it from the cache eagerly, so the very next request fails — not
/// the first one after the cache TTL.
#[actix_web::test]
async fn sdk_revoked_api_key_is_rejected_immediately() {
    let h = setup().await;

    let admin = || Identity::mock("admin@example.com".to_string());
    let creds = h
        .service
        .create_token("revoked-key".to_string(), "ns".to_string(), admin())
        .await
        .unwrap();

    let sdk_config = aws_sdk_sqs::Config::builder()
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds.access_key,
            creds.secret_key,
            None,
            None,
            "Static",
        ))
        .endpoint_url(format!("{}/api/sqs", h.base_url))
        .behavior_version(BehaviorVersion::latest())
        .build();
    let client = aws_sdk_sqs::Client::from_conf(sdk_config);

    // Use the key once so the signing-key cache holds it.
    client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("warm the cache")
        .send()
        .await
        .expect("fresh key should authenticate");

    h.service.delete_token("revoked-key", admin()).await.unwrap();

    let err = client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("should be rejected")
        .send()
        .await
        .expect_err("revoked key must stop authenticating immediately");
    let status = err
        .raw_response()
        .map(|r| r.status().as_u16())
        .unwrap_or_default();
    assert_eq!(status, 401, "expected 401, got {err:?}");
}

/// The visibility timeout is a lease, not a delete: when it lapses the same
/// message (same MessageId) is delivered again and the receive count climbs.
#[actix_web::test]
async fn sdk_visibility_timeout_expiry_redelivers_the_same_message() {
    use aws_sdk_sqs::types::MessageSystemAttributeName;

    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("leased, not gone")
        .send()
        .await
        .unwrap();

    let first = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(300)
        .send()
        .await
        .unwrap();
    let first = &first.messages()[0];
    let message_id = first.message_id().unwrap().to_string();

    // Fast-forward the lease instead of sleeping it out.
    sqlx::query("UPDATE messages SET invisible_until = unixepoch('now') - 1")
        .execute(h.service.db())
        .await
        .unwrap();

    let second = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .send()
        .await
        .unwrap();
    let second = &second.messages()[0];
    assert_eq!(second.message_id().unwrap(), message_id);
    assert_eq!(second.body().unwrap(), "leased, not gone");
    assert_eq!(
        second
            .attributes()
            .unwrap()
            .get(&MessageSystemAttributeName::ApproximateReceiveCount)
            .map(String::as_str),
        Some("2")
    );
    // Redelivery issues a fresh receipt handle.
    assert_ne!(second.receipt_handle(), first.receipt_handle());
}

/// Every delivery counts against max_retries (5 in this harness); once
/// exhausted the message parks as failed and stops being delivered.
#[actix_web::test]
async fn sdk_messages_stop_delivering_after_max_retries() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("poison")
        .send()
        .await
        .unwrap();

    // VisibilityTimeout=0 returns the message to the queue immediately, so
    // each receive is one delivery attempt.
    for attempt in 1..=5 {
        let received = h
            .client
            .receive_message()
            .queue_url(&h.queue_url)
            .visibility_timeout(0)
            .send()
            .await
            .unwrap();
        assert_eq!(received.messages().len(), 1, "attempt {attempt} should deliver");
    }

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert!(
        received.messages().is_empty(),
        "the message must park after its fifth delivery"
    );

    // Parked, not deleted: still in the database with its tries exhausted.
    let tries: i64 = sqlx::query_scalar("SELECT tries FROM messages")
        .fetch_one(h.service.db())
        .await
        .unwrap();
    assert_eq!(tries, 5);
}

/// Delivery order is strict FIFO by send order, and a message released back
/// (requeued) re-enters at its original position, not the back of the queue —
/// unlike AWS standard queues, which are best-effort ordered.
#[actix_web::test]
async fn sdk_delivery_order_is_fifo_and_requeues_keep_their_position() {
    let h = setup().await;

    for body in ["first", "second", "third"] {
        h.client
            .send_message()
            .queue_url(&h.queue_url)
            .message_body(body)
            .send()
            .await
            .unwrap();
    }

    // Take the head of the queue out on lease...
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(300)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages()[0].body().unwrap(), "first");
    let handle = received.messages()[0].receipt_handle().unwrap().to_string();

    // ...release it, then drain: it comes back at the front.
    h.client
        .change_message_visibility()
        .queue_url(&h.queue_url)
        .receipt_handle(&handle)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    let bodies: Vec<&str> = received.messages().iter().map(|m| m.body().unwrap()).collect();
    assert_eq!(bodies, vec!["first", "second", "third"]);
}

#[actix_web::test]
async fn sdk_invalid_receipt_handles_are_rejected() {
    let h = setup().await;

    let err = h
        .client
        .delete_message()
        .queue_url(&h.queue_url)
        .receipt_handle("0:deadbeef")
        .send()
        .await
        .expect_err("DeleteMessage with a bogus handle must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(404), "expected 404 Not Found: {err:?}");

    let err = h
        .client
        .change_message_visibility()
        .queue_url(&h.queue_url)
        .receipt_handle("0:deadbeef")
        .visibility_timeout(0)
        .send()
        .await
        .expect_err("ChangeMessageVisibility with a bogus handle must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(404), "expected 404 Not Found: {err:?}");
}

#[actix_web::test]
async fn sdk_receive_caps_at_max_number_of_messages() {
    let h = setup().await;

    for i in 0..5 {
        h.client
            .send_message()
            .queue_url(&h.queue_url)
            .message_body(format!("m{i}"))
            .send()
            .await
            .unwrap();
    }

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(3)
        .visibility_timeout(300)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 3);

    // The rest are still there; the three in flight are not redelivered.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 2);
}

#[actix_web::test]
async fn sdk_short_poll_on_an_empty_queue_returns_immediately() {
    let h = setup().await;

    let started = std::time::Instant::now();
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .wait_time_seconds(0)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "a short poll must not block: {:?}",
        started.elapsed()
    );
}

#[actix_web::test]
async fn sdk_list_queues_filters_by_name_prefix() {
    let h = setup().await;

    for name in ["alpha-1", "alpha-2", "beta"] {
        h.client.create_queue().queue_name(name).send().await.unwrap();
    }

    let listed = h
        .client
        .list_queues()
        .queue_name_prefix("alpha")
        .send()
        .await
        .expect("ListQueues with a prefix should succeed");
    let mut urls: Vec<&str> = listed.queue_urls().iter().map(String::as_str).collect();
    urls.sort_unstable();
    assert_eq!(
        urls,
        vec![
            format!("{}/api/sqs/ns/alpha-1", h.base_url).as_str(),
            format!("{}/api/sqs/ns/alpha-2", h.base_url).as_str(),
        ],
        "neither 'beta' nor the harness queue 'q' matches the prefix"
    );
}

#[actix_web::test]
async fn sdk_get_queue_attributes_returns_only_requested_names() {
    let h = setup().await;

    h.client
        .set_queue_attributes()
        .queue_url(&h.queue_url)
        .attributes(QueueAttributeName::VisibilityTimeout, "120")
        .attributes(QueueAttributeName::DelaySeconds, "5")
        .send()
        .await
        .unwrap();

    let attrs = h
        .client
        .get_queue_attributes()
        .queue_url(&h.queue_url)
        .attribute_names(QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .unwrap();
    let attrs = attrs.attributes().expect("attributes map");
    assert_eq!(
        attrs.get(&QueueAttributeName::VisibilityTimeout).map(String::as_str),
        Some("120")
    );
    assert!(
        !attrs.contains_key(&QueueAttributeName::DelaySeconds),
        "unrequested attributes must not be returned: {attrs:?}"
    );
}

/// One oversized entry fails alone; the rest of the batch lands. (The 1 MiB
/// whole-request cap is a different rule, tested above — this is the
/// per-entry queue MaximumMessageSize check.)
#[actix_web::test]
async fn sdk_batch_entries_fail_independently() {
    let h = setup().await;

    h.client
        .set_queue_attributes()
        .queue_url(&h.queue_url)
        .attributes(QueueAttributeName::MaximumMessageSize, "1024")
        .send()
        .await
        .unwrap();

    let result = h
        .client
        .send_message_batch()
        .queue_url(&h.queue_url)
        .entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id("fits")
                .message_body("small")
                .build()
                .unwrap(),
        )
        .entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id("oversized")
                .message_body("y".repeat(2048))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("the batch call itself should succeed");

    let successful: Vec<&str> = result.successful().iter().map(|e| e.id()).collect();
    assert_eq!(successful, vec!["fits"]);
    assert_eq!(result.failed().len(), 1);
    assert_eq!(result.failed()[0].id(), "oversized");

    // Only the fitting entry was enqueued.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 1);
    assert_eq!(received.messages()[0].body().unwrap(), "small");
}

#[actix_web::test]
async fn sdk_creating_a_duplicate_queue_is_an_error() {
    let h = setup().await;

    // The harness queue `q` already exists. (AWS would answer
    // QueueNameExists; here it surfaces as a generic SDK error.)
    let result = h.client.create_queue().queue_name("q").send().await;
    assert!(result.is_err(), "duplicate CreateQueue must not succeed");

    // The original queue is unharmed.
    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("still standing")
        .send()
        .await
        .unwrap();
}

/// The API key is scoped to namespace `ns`: a syntactically valid queue URL
/// in another namespace must be rejected even though the queue exists.
#[actix_web::test]
async fn sdk_cross_namespace_queue_urls_are_rejected() {
    let h = setup().await;

    let admin = || Identity::mock("admin@example.com".to_string());
    h.service.create_namespace("other", admin()).await.unwrap();
    h.service
        .create_queue("other", "q", Default::default(), HashMap::new(), admin())
        .await
        .unwrap();

    let foreign_url = format!("{}/api/sqs/other/q", h.base_url);

    let err = h
        .client
        .send_message()
        .queue_url(&foreign_url)
        .message_body("crossing the fence")
        .send()
        .await
        .expect_err("a send outside the key's namespace must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(401), "expected 401 Unauthorized: {err:?}");

    let err = h
        .client
        .receive_message()
        .queue_url(&foreign_url)
        .send()
        .await
        .expect_err("a receive outside the key's namespace must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(401), "expected 401 Unauthorized: {err:?}");
}

#[actix_web::test]
async fn sdk_operations_on_a_missing_queue_are_not_found() {
    let h = setup().await;

    let ghost_url = format!("{}/api/sqs/ns/ghost", h.base_url);

    let err = h
        .client
        .receive_message()
        .queue_url(&ghost_url)
        .send()
        .await
        .expect_err("receiving from a missing queue must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(404), "expected 404 Not Found: {err:?}");

    let err = h
        .client
        .delete_queue()
        .queue_url(&ghost_url)
        .send()
        .await
        .expect_err("deleting a missing queue must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(404), "expected 404 Not Found: {err:?}");
}

#[actix_web::test]
async fn sdk_unicode_bodies_roundtrip() {
    let h = setup().await;

    let body = "héllo wörld — 日本語 🦀 emoji and ümlauts";
    let sent = h
        .client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body(body)
        .send()
        .await
        .expect("a unicode body should be accepted");
    // The MD5 is over the UTF-8 bytes.
    assert_eq!(
        sent.md5_of_message_body().unwrap(),
        format!("{:x}", md5::compute(body))
    );

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages()[0].body().unwrap(), body);
}

/// Each delivery mints a new receipt handle and invalidates the previous
/// one: an acknowledgement from a lapsed delivery cannot delete the message.
#[actix_web::test]
async fn sdk_stale_receipt_handle_is_rejected_after_redelivery() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("hold on to your handle")
        .send()
        .await
        .unwrap();

    let take = || async {
        h.client
            .receive_message()
            .queue_url(&h.queue_url)
            .visibility_timeout(0)
            .send()
            .await
            .unwrap()
            .messages()[0]
            .receipt_handle()
            .unwrap()
            .to_string()
    };
    let stale = take().await;
    let fresh = take().await;
    assert_ne!(stale, fresh);

    let err = h
        .client
        .delete_message()
        .queue_url(&h.queue_url)
        .receipt_handle(&stale)
        .send()
        .await
        .expect_err("the superseded handle must not acknowledge the message");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(404), "expected 404 Not Found: {err:?}");

    // The current handle still works.
    h.client
        .delete_message()
        .queue_url(&h.queue_url)
        .receipt_handle(&fresh)
        .send()
        .await
        .expect("the current handle should acknowledge the message");
}

#[actix_web::test]
async fn sdk_change_message_visibility_extends_the_lease() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("keep hidden")
        .send()
        .await
        .unwrap();

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(1)
        .send()
        .await
        .unwrap();
    let handle = received.messages()[0].receipt_handle().unwrap().to_string();

    h.client
        .change_message_visibility()
        .queue_url(&h.queue_url)
        .receipt_handle(&handle)
        .visibility_timeout(300)
        .send()
        .await
        .expect("extending an in-flight lease should succeed");

    // The deadline moved to ~now+300 (instead of sleeping out the original
    // 1s lease and asserting non-redelivery, check the stamped deadline).
    let remaining: i64 =
        sqlx::query_scalar("SELECT invisible_until - unixepoch('now') FROM messages")
            .fetch_one(h.service.db())
            .await
            .unwrap();
    assert!(
        (298..=301).contains(&remaining),
        "lease should now expire in ~300s, found {remaining}s"
    );
}

#[actix_web::test]
async fn sdk_change_message_visibility_rejects_oversized_timeouts() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("bounded lease")
        .send()
        .await
        .unwrap();
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    let handle = received.messages()[0].receipt_handle().unwrap().to_string();

    // 43200s (12 hours) is the AWS maximum and is accepted...
    h.client
        .change_message_visibility()
        .queue_url(&h.queue_url)
        .receipt_handle(&handle)
        .visibility_timeout(43200)
        .send()
        .await
        .expect("the AWS maximum visibility timeout should be accepted");

    // ...one second past it is a client error.
    let err = h
        .client
        .change_message_visibility()
        .queue_url(&h.queue_url)
        .receipt_handle(&handle)
        .visibility_timeout(43201)
        .send()
        .await
        .expect_err("a visibility timeout beyond 12 hours must be rejected");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(400), "expected 400 Bad Request: {err:?}");
}

#[actix_web::test]
async fn sdk_long_poll_waits_out_an_empty_queue() {
    let h = setup().await;

    // With nothing to deliver, the poll holds the connection for the
    // requested window and then returns empty.
    let started = std::time::Instant::now();
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .wait_time_seconds(2)
        .send()
        .await
        .expect("an empty long poll should succeed");
    let elapsed = started.elapsed();

    assert!(received.messages().is_empty());
    assert!(
        elapsed >= std::time::Duration::from_millis(1500),
        "long poll returned too early: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "long poll overstayed its deadline: {elapsed:?}"
    );
}

/// Message attributes are returned only when (and as) requested: filtering
/// by name returns just that attribute, and an unfiltered receive omits the
/// map entirely. Also exercises the Number data type.
#[actix_web::test]
async fn sdk_message_attributes_filter_by_name_and_are_omitted_unless_requested() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("selective attributes")
        .message_attributes(
            "TraceId",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("abc-123")
                .build()
                .unwrap(),
        )
        .message_attributes(
            "Retries",
            MessageAttributeValue::builder()
                .data_type("Number")
                .string_value("42")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Ask for one attribute by name: only it comes back.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(0)
        .message_attribute_names("Retries")
        .send()
        .await
        .unwrap();
    let attrs = received.messages()[0]
        .message_attributes()
        .expect("requested attributes should be present");
    assert_eq!(attrs.len(), 1, "only the requested attribute: {attrs:?}");
    let attr = attrs.get("Retries").expect("Retries attribute");
    assert_eq!(attr.data_type(), "Number");
    assert_eq!(attr.string_value(), Some("42"));

    // Ask for nothing: the map is omitted entirely.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();
    assert!(received.messages()[0].message_attributes().is_none());
}

#[actix_web::test]
async fn sdk_batch_entries_carry_message_attributes() {
    let h = setup().await;

    let mut batch = h.client.send_message_batch().queue_url(&h.queue_url);
    for i in 0..2 {
        batch = batch.entries(
            aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(i.to_string())
                .message_body(format!("attributed-{i}"))
                .message_attributes(
                    "Index",
                    MessageAttributeValue::builder()
                        .data_type("Number")
                        .string_value(i.to_string())
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        );
    }
    let result = batch.send().await.expect("batch with attributes should succeed");
    assert_eq!(result.successful().len(), 2);

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .message_attribute_names("All")
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 2);
    for message in received.messages() {
        let index = message
            .body()
            .unwrap()
            .strip_prefix("attributed-")
            .expect("batch body");
        let attr = message
            .message_attributes()
            .and_then(|attrs| attrs.get("Index"))
            .expect("per-entry attribute should roundtrip");
        assert_eq!(attr.string_value(), Some(index));
    }
}

/// The queue's VisibilityTimeout attribute applies to receives that don't
/// override it: the claim is stamped with the queue's own lease length.
#[actix_web::test]
async fn sdk_queue_visibility_timeout_attribute_is_honored() {
    let h = setup().await;

    h.client
        .set_queue_attributes()
        .queue_url(&h.queue_url)
        .attributes(QueueAttributeName::VisibilityTimeout, "120")
        .send()
        .await
        .unwrap();

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("queue-paced lease")
        .send()
        .await
        .unwrap();

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 1);

    let remaining: i64 =
        sqlx::query_scalar("SELECT invisible_until - unixepoch('now') FROM messages")
            .fetch_one(h.service.db())
            .await
            .unwrap();
    assert!(
        (118..=121).contains(&remaining),
        "claim should use the queue's 120s timeout, found {remaining}s"
    );
}

#[actix_web::test]
async fn sdk_get_queue_attributes_on_a_fresh_queue_is_empty() {
    let h = setup().await;

    let attrs = h
        .client
        .get_queue_attributes()
        .queue_url(&h.queue_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .expect("GetQueueAttributes on a fresh queue should succeed");
    assert!(
        attrs.attributes().map_or(true, |map| map.is_empty()),
        "a fresh queue has no attributes: {:?}",
        attrs.attributes()
    );

    let ghost_url = format!("{}/api/sqs/ns/ghost", h.base_url);
    let err = h
        .client
        .get_queue_attributes()
        .queue_url(&ghost_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .expect_err("GetQueueAttributes on a missing queue must fail");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(404), "expected 404 Not Found: {err:?}");
}

#[actix_web::test]
async fn sdk_wrong_or_unknown_credentials_are_rejected() {
    let h = setup().await;

    let client_with = |access: &str, secret: &str| {
        let cfg = aws_sdk_sqs::Config::builder()
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(access, secret, None, None, "Static"))
            .endpoint_url(format!("{}/api/sqs", h.base_url))
            .behavior_version(BehaviorVersion::latest())
            .build();
        aws_sdk_sqs::Client::from_conf(cfg)
    };

    // A real access key with the wrong secret: the signature won't verify.
    let real_key = sqlx::query_scalar::<_, String>("SELECT key_id FROM api_keys LIMIT 1")
        .fetch_one(h.service.db())
        .await
        .unwrap();
    let err = client_with(&real_key, "nervemqInvalidSecretKey")
        .list_queues()
        .send()
        .await
        .expect_err("a wrong secret must not authenticate");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(401), "expected 401 Unauthorized: {err:?}");

    // A well-formed key id the server never minted.
    let err = client_with("1unknownKey", "irrelevantSecret")
        .list_queues()
        .send()
        .await
        .expect_err("an unknown access key must not authenticate");
    let status = err.raw_response().map(|r| r.status().as_u16());
    assert_eq!(status, Some(401), "expected 401 Unauthorized: {err:?}");
}

/// ChangeMessageVisibilityBatch applies each entry independently: one entry
/// releases its message (timeout 0), one extends its lease, one carries a
/// bogus handle and fails alone, and an out-of-range timeout fails with the
/// parameter error rather than the handle error.
#[actix_web::test]
async fn sdk_change_message_visibility_batch_applies_per_entry() {
    use aws_sdk_sqs::types::ChangeMessageVisibilityBatchRequestEntry;

    let h = setup().await;

    for body in ["release me", "keep me leased"] {
        h.client
            .send_message()
            .queue_url(&h.queue_url)
            .message_body(body)
            .send()
            .await
            .unwrap();
    }
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .visibility_timeout(300)
        .send()
        .await
        .unwrap();
    assert_eq!(received.messages().len(), 2);
    let handle_of = |body: &str| {
        received
            .messages()
            .iter()
            .find(|m| m.body() == Some(body))
            .and_then(|m| m.receipt_handle())
            .unwrap()
            .to_string()
    };

    let entry = |id: &str, handle: &str, timeout: i32| {
        ChangeMessageVisibilityBatchRequestEntry::builder()
            .id(id)
            .receipt_handle(handle)
            .visibility_timeout(timeout)
            .build()
            .unwrap()
    };

    let result = h
        .client
        .change_message_visibility_batch()
        .queue_url(&h.queue_url)
        .entries(entry("release", &handle_of("release me"), 0))
        .entries(entry("extend", &handle_of("keep me leased"), 600))
        .entries(entry("bogus", "0:deadbeef", 0))
        .entries(entry("oversized", &handle_of("keep me leased"), 43201))
        .send()
        .await
        .expect("ChangeMessageVisibilityBatch should succeed via the SDK");

    let mut successful: Vec<&str> = result.successful().iter().map(|e| e.id()).collect();
    successful.sort_unstable();
    assert_eq!(successful, vec!["extend", "release"]);

    let failed: Vec<(&str, &str)> = result
        .failed()
        .iter()
        .map(|e| (e.id(), e.code()))
        .collect();
    assert!(failed.contains(&("bogus", "ReceiptHandleIsInvalid")), "{failed:?}");
    assert!(failed.contains(&("oversized", "InvalidParameterValue")), "{failed:?}");
    assert!(result.failed().iter().all(|e| e.sender_fault()));

    // Only the released message is receivable again; the extended one keeps
    // its (now 600s) lease.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();
    let bodies: Vec<&str> = received.messages().iter().map(|m| m.body().unwrap()).collect();
    assert_eq!(bodies, vec!["release me"]);
}

/// NerveMQ-specific guarantee (AWS leaves this unspecified): a receipt
/// handle outlives its visibility timeout. After the window lapses the
/// original consumer can still delete the message, right up until it is
/// delivered to another consumer — only redelivery mints a new handle and
/// invalidates the old one.
#[actix_web::test]
async fn sdk_lapsed_handle_still_deletes_until_redelivery() {
    let h = setup().await;

    h.client
        .send_message()
        .queue_url(&h.queue_url)
        .message_body("slow consumer, valid ack")
        .send()
        .await
        .unwrap();

    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .visibility_timeout(300)
        .send()
        .await
        .unwrap();
    let handle = received.messages()[0].receipt_handle().unwrap().to_string();

    // The visibility window lapses (fast-forwarded) but nobody else has
    // received the message: the original handle is still the latest.
    sqlx::query("UPDATE messages SET invisible_until = unixepoch('now') - 1")
        .execute(h.service.db())
        .await
        .unwrap();

    h.client
        .delete_message()
        .queue_url(&h.queue_url)
        .receipt_handle(&handle)
        .send()
        .await
        .expect("a lapsed handle must still acknowledge until redelivery");

    // Deleted for good, not redelivered.
    let received = h
        .client
        .receive_message()
        .queue_url(&h.queue_url)
        .send()
        .await
        .unwrap();
    assert!(received.messages().is_empty());
}
