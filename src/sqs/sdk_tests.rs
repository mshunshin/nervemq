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
    svc.create_queue("ns", "q", HashMap::new(), HashMap::new(), admin())
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
