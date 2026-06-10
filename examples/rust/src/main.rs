//! Minimal NerveMQ example using the official AWS SDK for Rust.
//!
//! Credentials are a NerveMQ API key (the queue is created in the key's
//! namespace), minted via the admin UI or the CLI:
//!
//! ```sh
//! nervemq apikey add --name rust-example --namespace <namespace>
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... cargo run
//! ```

use std::env;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_sqs::types::MessageAttributeValue;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    tracing_subscriber::fmt::init();

    let endpoint = env::var("NERVEMQ_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:8080/api/sqs".to_string());

    let (Ok(access_key), Ok(secret_key)) = (
        env::var("AWS_ACCESS_KEY_ID"),
        env::var("AWS_SECRET_ACCESS_KEY"),
    ) else {
        eyre::bail!(
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to a NerveMQ API key \
             (create one with: nervemq apikey add --name rust-example --namespace <namespace>)"
        );
    };

    let credentials =
        aws_sdk_sqs::config::Credentials::new(access_key, secret_key, None, None, "Static");

    let config = aws_sdk_sqs::Config::builder()
        .region(Region::new("us-west-1"))
        .credentials_provider(credentials)
        .endpoint_url(endpoint)
        .behavior_version(BehaviorVersion::latest())
        .build();

    let sqs = aws_sdk_sqs::Client::from_conf(config);

    // Get the queue's URL, creating it on first run. The queue lives in the
    // API key's namespace.
    let queue_url = match sqs.get_queue_url().queue_name("test").send().await {
        Ok(res) => res.queue_url.expect("queue url"),
        Err(_) => sqs
            .create_queue()
            .queue_name("test")
            .send()
            .await?
            .queue_url
            .expect("queue url"),
    };

    tracing::info!("Queue URL: {queue_url}");

    let sent = sqs
        .send_message()
        .queue_url(&queue_url)
        .message_body("Hello World!")
        .message_attributes(
            "Test",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("TestString")
                .build()?,
        )
        .send()
        .await?;

    tracing::info!("Message ID: {:?}", sent.message_id());

    let received = sqs
        .receive_message()
        .queue_url(&queue_url)
        .message_attribute_names("All")
        .max_number_of_messages(10)
        .visibility_timeout(0)
        .send()
        .await?;

    tracing::info!("Messages: {:#?}", received.messages());

    Ok(())
}
