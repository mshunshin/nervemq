//! Core service implementation for NerveMQ message queue system.
//!
//! This module implements the main service layer that provides:
//!
//! - Queue management (create, delete, list, purge)
//! - Message operations (send, receive, delete)
//! - Namespace management (create, delete, list)
//! - User management and authentication
//! - Statistics and monitoring
//!
//! # Key Types
//!
//! - [`Service`] - The main service struct that handles all operations
//! - [`QueueAttributes`] - Configuration options for queues
//! - [`QueueConfig`] - Internal queue configuration
//! - [`MessageDetails`] - Detailed message information
//!
//! # Examples
//!
//! ```no_run
//! use std::collections::HashMap;
//!
//! use actix_identity::Identity;
//! use nervemq::service::Service;
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to the service (configuration is read from the environment)
//!     let service = Service::connect().await?;
//!
//!     // The identity of the acting user; in a request handler this is
//!     // extracted from the session rather than mocked.
//!     let identity = || Identity::mock("admin@example.com".to_string());
//!
//!     // Create a namespace
//!     service.create_namespace("my-namespace", identity()).await?;
//!
//!     // Create a queue (attributes use the typed wire representation)
//!     service.create_queue(
//!         "my-namespace",
//!         "my-queue",
//!         Default::default(),
//!         HashMap::new(),
//!         identity()
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Database Schema
//!
//! The service uses SQLite with the following main tables:
//!
//! - `namespaces` - Namespace definitions
//! - `queues` - Queue definitions
//! - `messages` - Message storage
//! - `users` - User accounts
//! - `user_permissions` - Access control
//! - `queue_configurations` - Queue settings
//! - `queue_attributes` - Queue attributes
//! - `queue_tags` - Queue metadata
//! - `kv_pairs` - Message attributes
//!
//! # Architecture
//!
//! The service implements an AWS SQS-compatible message queue with:
//!
//! - Multi-tenant support via namespaces
//! - Role-based access control
//! - Dead letter queues
//! - Message attributes
//! - Configurable retry policies
//! - Queue tags and attributes
//!
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use actix_identity::Identity;
use actix_web::{error::ErrorUnauthorized, web};
use argon2::password_hash::PasswordHashString;
use base64::Engine;
use serde::{Deserialize, Serialize};
use secrecy::SecretString;
use serde_email::Email;
use sqlx::{
    sqlite::{
        SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode,
        SqlitePoolOptions, SqliteSynchronous,
    },
    Acquire, ConnectOptions, Connection, FromRow, Sqlite, SqlitePool,
};
use tokio_stream::StreamExt as _;

use crate::{
    api::{
        auth::{Role, User},
        tokens::CreateTokenResponse,
    },
    auth::{
        credential::KeyAccess,
        crypto::{api_key_from_parts, generate_api_key, hash_secret, verify_secret, GeneratedKey},
    },
    config::Config,
    error::{AwsCode, Error},
    kms::{memory::InMemoryKeyManager, KeyManager},
    message::{Message, MessageStatus},
    namespace::{Namespace, NamespaceStatistics},
    queue::{Queue, QueueDepth, QueueStatistics},
    sqs::types::{SqsMessage, SqsMessageAttribute},
    types::{
        send_message::{SendMessageRequest, SendMessageResponse},
        send_message_batch::{
            SendMessageBatchRequest, SendMessageBatchResponse, SendMessageBatchResultEntry,
            SendMessageBatchResultErrorEntry,
        },
    },
};

/// Configuration for dead-letter queue redrive policy.
///
/// This defines how failed messages should be moved to a dead-letter queue
/// after exceeding the maximum number of receive attempts.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedrivePolicy {
    /// The field is named ARN, but for NerveMQ we use the format `namespace:queue`
    dead_letter_target_arn: String,
    max_receive_count: u64,
}

/// Configurable attributes for a queue.
///
/// These attributes control the queue's behavior including:
/// - Message delay
/// - Message size limits
/// - Message retention
/// - Visibility timeout
/// - Dead letter queue configuration
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueueAttributes {
    pub delay_seconds: Option<u64>,
    #[serde(rename = "MaximumMessageSize", alias = "MaxMessageSize")]
    pub max_message_size: Option<u64>,
    pub message_retention_period: Option<u64>,
    pub receive_message_wait_time_seconds: Option<u64>,
    pub visibility_timeout: Option<u64>,

    // TODO: RedrivePolicy, RedriveAllowPolicy
    pub redrive_policy: Option<RedrivePolicy /* Must be JSON serialized to a string */>,

    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

/// Serializes an optional integer attribute in the AWS wire format, where
/// attribute values are carried as strings (`"VisibilityTimeout": "120"`).
mod u64_attribute_value {
    use serde::Serializer;

    pub fn serialize<S: Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(v) => s.serialize_str(&v.to_string()),
            None => s.serialize_none(),
        }
    }
}

/// A request's queue attributes as sent: AWS's names, and values that ought
/// to be strings. [`QueueAttributesSer::from_request`] checks them.
pub type QueueAttributeMap = HashMap<String, serde_json::Value>;

/// The attributes `CreateQueue` and `SetQueueAttributes` accept on every
/// queue, as AWS names them. AWS's other names are computed and read-only,
/// or only for FIFO queues.
const SETTABLE_ATTRIBUTES: [&str; 11] = [
    "DelaySeconds",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    "ReceiveMessageWaitTimeSeconds",
    "VisibilityTimeout",
    "RedrivePolicy",
    "Policy",
    "RedriveAllowPolicy",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
    "SqsManagedSseEnabled",
];

/// The attributes AWS accepts only on FIFO queues. NerveMQ, which doesn't
/// implement FIFO queues, accepts them on a queue named `.fifo` and stores
/// them without acting on them.
const FIFO_ATTRIBUTES: [&str; 4] = [
    "FifoQueue",
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "FifoThroughputLimit",
];

/// Whether `name` is an attribute NerveMQ stores as given, under its AWS
/// name, without acting on it. These ride in [`QueueAttributesSer::other`].
fn is_untyped_attribute(name: &str) -> bool {
    matches!(
        name,
        "Policy"
            | "RedriveAllowPolicy"
            | "KmsMasterKeyId"
            | "KmsDataKeyReusePeriodSeconds"
            | "SqsManagedSseEnabled"
    ) || FIFO_ATTRIBUTES.contains(&name)
}

/// AWS's value for an untyped attribute a queue never set, which a
/// `CreateQueue` of an existing name compares against.
fn untyped_attribute_default(name: &str) -> Option<&'static str> {
    Some(match name {
        "SqsManagedSseEnabled" => "true",
        "KmsDataKeyReusePeriodSeconds" => "300",
        "ContentBasedDeduplication" => "false",
        "DeduplicationScope" => "queue",
        "FifoThroughputLimit" => "perQueue",
        // Only `.fifo` queues take it, and those are FIFO queues on AWS.
        "FifoQueue" => "true",
        "Policy" | "RedriveAllowPolicy" | "KmsMasterKeyId" => "",
        _ => return None,
    })
}

/// The request whose attributes [`QueueAttributesSer::from_request`]
/// parses: `CreateQueue` or `SetQueueAttributes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributeWrite {
    Create,
    Set,
}

/// A queue's attributes, parsed: the five integer attributes NerveMQ acts
/// on, the redrive policy, and in `other` the AWS attributes it stores
/// without acting on them.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueueAttributesSer {
    #[serde(with = "u64_attribute_value", skip_serializing_if = "Option::is_none")]
    pub delay_seconds: Option<u64>,
    #[serde(
        with = "u64_attribute_value",
        skip_serializing_if = "Option::is_none",
        rename = "MaximumMessageSize"
    )]
    pub max_message_size: Option<u64>,
    #[serde(with = "u64_attribute_value", skip_serializing_if = "Option::is_none")]
    pub message_retention_period: Option<u64>,
    #[serde(with = "u64_attribute_value", skip_serializing_if = "Option::is_none")]
    pub receive_message_wait_time_seconds: Option<u64>,
    #[serde(with = "u64_attribute_value", skip_serializing_if = "Option::is_none")]
    pub visibility_timeout: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub redrive_policy: Option<String /* Must be JSON serialized to a string */>,

    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

/// A request's integer attribute value.
fn whole_number(name: &str, value: &str) -> Result<u64, Error> {
    value.parse().map_err(|_| {
        Error::aws(
            AwsCode::InvalidAttributeValue,
            format!("Invalid value for the parameter {name}: {value:?} isn't a whole number."),
        )
    })
}

impl QueueAttributesSer {
    /// Parses a request's attributes, for the queue `queue`, as AWS does:
    ///
    /// - a name a request can't set is `InvalidAttributeName`: AWS's
    ///   computed attributes, FIFO attributes on a queue not named `.fifo`,
    ///   names AWS doesn't have, and NerveMQ's internal storage keys
    ///   (`visibility_timeout`, …), which used to be stored as given and so
    ///   escaped every check;
    /// - `FifoQueue` can't change after creation;
    /// - values must be strings, integers must parse, and both are
    ///   `InvalidAttributeValue` when not;
    /// - integers must be in AWS's ranges ([`Self::validate`]).
    pub fn from_request(
        attributes: QueueAttributeMap,
        queue: &str,
        write: AttributeWrite,
    ) -> Result<Self, Error> {
        let fifo = queue.ends_with(".fifo");
        // In name order, so a request with several faults always gets the
        // same answer.
        let mut attributes: Vec<_> = attributes.into_iter().collect();
        attributes.sort_by(|a, b| a.0.cmp(&b.0));

        let mut parsed = Self::default();
        for (name, value) in attributes {
            let settable = SETTABLE_ATTRIBUTES.contains(&name.as_str())
                || (fifo && FIFO_ATTRIBUTES.contains(&name.as_str()));
            if !settable {
                // AWS's wording.
                return Err(Error::aws(
                    AwsCode::InvalidAttributeName,
                    format!("Unknown Attribute {name}."),
                ));
            }
            if name == "FifoQueue" && write == AttributeWrite::Set {
                // AWS's wording.
                return Err(Error::aws(
                    AwsCode::InvalidAttributeValue,
                    "Invalid value for the parameter FifoQueue. Reason: Modifying queue \
                     type is not supported.",
                ));
            }
            let serde_json::Value::String(value) = value else {
                return Err(Error::aws(
                    AwsCode::InvalidAttributeValue,
                    format!("Invalid value for the parameter {name}: values are strings."),
                ));
            };
            match name.as_str() {
                "DelaySeconds" => parsed.delay_seconds = Some(whole_number(&name, &value)?),
                "MaximumMessageSize" => {
                    parsed.max_message_size = Some(whole_number(&name, &value)?)
                }
                "MessageRetentionPeriod" => {
                    parsed.message_retention_period = Some(whole_number(&name, &value)?)
                }
                "ReceiveMessageWaitTimeSeconds" => {
                    parsed.receive_message_wait_time_seconds = Some(whole_number(&name, &value)?)
                }
                "VisibilityTimeout" => {
                    parsed.visibility_timeout = Some(whole_number(&name, &value)?)
                }
                "RedrivePolicy" => parsed.redrive_policy = Some(value),
                _ => {
                    parsed.other.insert(name, serde_json::Value::String(value));
                }
            }
        }
        parsed.validate()?;
        Ok(parsed)
    }

    /// Whether no attribute is set.
    fn is_empty(&self) -> bool {
        self.delay_seconds.is_none()
            && self.max_message_size.is_none()
            && self.message_retention_period.is_none()
            && self.receive_message_wait_time_seconds.is_none()
            && self.visibility_timeout.is_none()
            && self.redrive_policy.is_none()
            && self.other.is_empty()
    }

    /// Checks the typed attributes against AWS's ranges, so an out-of-range
    /// value is refused rather than stored. (A `VisibilityTimeout` past
    /// `i64::MAX` used to be stored negative, making received messages
    /// immediately redeliverable.) `MessageRetentionPeriod` may also be 0,
    /// NerveMQ's "retain forever".
    pub fn validate(&self) -> Result<(), Error> {
        use crate::sqs::limits::{self, check_range};

        let checks = [
            ("DelaySeconds", self.delay_seconds, &limits::DELAY_SECONDS, "seconds"),
            (
                "MaximumMessageSize",
                self.max_message_size,
                &limits::MAXIMUM_MESSAGE_SIZE,
                "bytes",
            ),
            (
                "ReceiveMessageWaitTimeSeconds",
                self.receive_message_wait_time_seconds,
                &limits::RECEIVE_MESSAGE_WAIT_TIME_SECONDS,
                "seconds",
            ),
            (
                "VisibilityTimeout",
                self.visibility_timeout,
                &limits::VISIBILITY_TIMEOUT,
                "seconds",
            ),
        ];
        for (name, value, range, unit) in checks {
            if let Some(value) = value {
                check_range(name, value, range, unit).map_err(Error::invalid_attribute_value)?;
            }
        }

        match self.message_retention_period {
            None | Some(0) => Ok(()),
            Some(value) => check_range(
                "MessageRetentionPeriod",
                value,
                &limits::MESSAGE_RETENTION_PERIOD,
                "seconds (or 0 to retain forever)",
            )
            .map_err(Error::invalid_attribute_value),
        }
    }

    pub fn deser(self) -> Result<QueueAttributes, Error> {
        Ok(QueueAttributes {
            delay_seconds: self.delay_seconds,
            max_message_size: self.max_message_size,
            message_retention_period: self.message_retention_period,
            receive_message_wait_time_seconds: self.receive_message_wait_time_seconds,
            visibility_timeout: self.visibility_timeout,
            redrive_policy: self
                .redrive_policy
                .map(|rp| serde_json::from_str(&rp))
                .transpose()?,
            other: self.other,
        })
    }
}

impl QueueAttributes {
    pub fn ser(self) -> Result<QueueAttributesSer, Error> {
        Ok(QueueAttributesSer {
            delay_seconds: self.delay_seconds,
            max_message_size: self.max_message_size,
            message_retention_period: self.message_retention_period,
            receive_message_wait_time_seconds: self.receive_message_wait_time_seconds,
            visibility_timeout: self.visibility_timeout,
            redrive_policy: self
                .redrive_policy
                .map(|rp| serde_json::to_string(&rp))
                .transpose()?,
            other: self.other,
        })
    }
}

/// Trait for type-safe queue attributes.
///
/// Used to define queue attribute names and types for extraction from
/// the database.
#[allow(unused)]
pub trait QueueAttribute {
    type Value;

    /// Returns the name of the queue attribute's column in the database.
    fn name(&self) -> &str;
}

#[allow(unused)]
pub(crate) mod queue_attributes {
    use super::QueueAttribute;

    /// Represents the delay_seconds queue attribute.
    pub struct DelaySeconds;

    impl QueueAttribute for DelaySeconds {
        type Value = u64;

        fn name(&self) -> &str {
            "delay_seconds"
        }
    }

    /// Represents the max_message_size queue
    pub struct MaxMessageSize;

    impl QueueAttribute for MaxMessageSize {
        type Value = u64;

        fn name(&self) -> &str {
            "max_message_size"
        }
    }

    /// Represents the message_retention_period queue attribute.
    pub struct MessageRetentionPeriod;

    impl QueueAttribute for MessageRetentionPeriod {
        type Value = u64;
        fn name(&self) -> &str {
            "message_retention_period"
        }
    }

    /// Represents the receive_message_wait_time_seconds queue attribute.
    pub struct ReceiveMessageWaitTimeSeconds;

    impl QueueAttribute for ReceiveMessageWaitTimeSeconds {
        type Value = u64;
        fn name(&self) -> &str {
            "receive_message_wait_time_seconds"
        }
    }

    /// Represents the visibility_timeout queue.
    pub struct VisibilityTimeout;

    impl QueueAttribute for VisibilityTimeout {
        type Value = u64;
        fn name(&self) -> &str {
            "visibility_timeout"
        }
    }

    /// Represents the redrive_policy queue attribute.
    pub struct RedrivePolicy;

    impl QueueAttribute for RedrivePolicy {
        type Value = String;

        fn name(&self) -> &str {
            "redrive_policy"
        }
    }

    /// Represents an arbitrary stringly-typed queue attribute.
    pub struct Other(String);

    impl QueueAttribute for Other {
        type Value = String;

        fn name(&self) -> &str {
            &self.0
        }
    }
}

/// Internal configuration for a queue stored in the database.
///
/// Contains:
/// - Queue ID
/// - Maximum retry attempts
/// - Optional dead letter queue ID
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct QueueConfig {
    pub queue: u64,
    pub max_retries: u64,
    pub dead_letter_queue: Option<u64>,
}

/// Represents the details of a message for display in the UI.
/// Detailed information about a message for display in the UI.
///
/// Includes:
/// - Message ID and queue
/// - Delivery status and attempts
/// - Message body and attributes
/// - Timestamps
#[derive(Debug, Serialize)]
pub struct MessageDetails {
    /// The MessageId: a UUID, as SQS returns it.
    pub id: String,
    pub queue: String,

    pub received_at: Option<u64>,
    pub delivered_at: Option<u64>,
    pub sent_by: Option<u64>,
    pub body: String,
    pub tries: u64,

    pub status: MessageStatus,

    pub message_attributes: HashMap<String, serde_json::Value>,
}

/// One page of a queue's messages plus the total count, for the UI's
/// paginated message list.
#[derive(Debug, Serialize)]
pub struct MessageList {
    pub messages: Vec<MessageDetails>,
    /// Total messages in the queue, ignoring pagination.
    pub total: u64,
}

/// Sortable columns of the message list. Deserialized from the `sort` query
/// parameter; the variant-to-SQL mapping is a fixed whitelist because column
/// names cannot be bound as query parameters.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageSortKey {
    #[default]
    Id,
    Body,
    Status,
    Tries,
    ReceivedAt,
    DeliveredAt,
}

impl MessageSortKey {
    fn sql(self) -> &'static str {
        match self {
            Self::Id => "m.id",
            Self::Body => "m.body",
            // The derived-status CASE expression's alias, usable in ORDER BY.
            Self::Status => "status",
            Self::Tries => "m.tries",
            Self::ReceivedAt => "m.received_at",
            Self::DeliveredAt => "m.delivered_at",
        }
    }
}

/// Sort direction for the message list (`order` query parameter).
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

impl SortOrder {
    fn sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

/// API-key credentials supplied by the caller rather than generated.
///
/// Both halves are required: an access key paired with a generated secret would
/// still leave the secret unrecoverable, which defeats the purpose.
#[derive(Debug, Clone)]
pub struct SuppliedCredentials {
    pub access_key: String,
    pub secret_key: String,
}

impl SuppliedCredentials {
    /// Rejects credentials that cannot work: sigv4 carries the access key in the
    /// credential scope, a slash-separated field, and compares it verbatim.
    fn validate(&self) -> Result<(), Error> {
        for (field, value) in [
            ("access key", &self.access_key),
            ("secret key", &self.secret_key),
        ] {
            if value.is_empty() {
                return Err(Error::invalid_parameter(format!("{field} is empty")));
            }
            if value.chars().any(|c| c.is_whitespace() || c == '/') {
                return Err(Error::invalid_parameter(format!(
                    "{field} may not contain whitespace or '/'"
                )));
            }
        }
        Ok(())
    }
}

/// What [`Service::create_queue`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum CreateQueueOutcome {
    Created,
    /// The name was taken by a queue matching every requested attribute;
    /// nothing was changed.
    AlreadyExists,
}

/// The AWS name of the first attribute `requested` sets to something other
/// than the queue's current value. An unset attribute compares as the value
/// `GetQueueAttributes` reports for it: the default NerveMQ applies in its
/// place for the typed ones, and AWS's default for the attributes NerveMQ
/// stores without acting on.
fn first_attribute_mismatch(
    requested: &QueueAttributesSer,
    current: &QueueAttributesSer,
) -> Option<String> {
    let typed = [
        ("DelaySeconds", requested.delay_seconds, current.delay_seconds.unwrap_or(0)),
        (
            "MaximumMessageSize",
            requested.max_message_size,
            current
                .max_message_size
                .unwrap_or(crate::sqs::types::MAX_MESSAGE_SIZE_BYTES as u64),
        ),
        // Unset and 0 both mean "retain forever".
        (
            "MessageRetentionPeriod",
            requested.message_retention_period,
            current.message_retention_period.unwrap_or(0),
        ),
        (
            "ReceiveMessageWaitTimeSeconds",
            requested.receive_message_wait_time_seconds,
            current.receive_message_wait_time_seconds.unwrap_or(0),
        ),
        (
            "VisibilityTimeout",
            requested.visibility_timeout,
            current
                .visibility_timeout
                .unwrap_or(crate::config::defaults::VISIBILITY_TIMEOUT),
        ),
    ];
    if let Some((name, ..)) = typed
        .iter()
        .find(|(_, want, have)| want.is_some_and(|want| want != *have))
    {
        return Some(name.to_string());
    }

    // The policy is a JSON document; compare it parsed so formatting
    // differences don't count.
    let json = |s: &str| serde_json::from_str::<serde_json::Value>(s).ok();
    if let Some(want) = &requested.redrive_policy {
        let same = current
            .redrive_policy
            .as_deref()
            .is_some_and(|have| have == want || json(have).is_some_and(|h| Some(h) == json(want)));
        if !same {
            return Some("RedrivePolicy".to_string());
        }
    }

    let text = |v: &serde_json::Value| match v {
        serde_json::Value::String(s) => s.clone(),
        v => v.to_string(),
    };
    requested
        .other
        .iter()
        .find(|(k, want)| {
            let have = current
                .other
                .get(*k)
                .map(text)
                .or_else(|| untyped_attribute_default(k).map(str::to_owned));
            have != Some(text(want))
        })
        .map(|(k, _)| k.clone())
}

/// Whether a database error is a unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|e| e.code())
        .is_some_and(|code| code == "2067" || code == "1555")
}

/// Whether every character is a letter, digit, hyphen or underscore — the
/// alphabet AWS SQS allows in queue names, and all of it URL-safe.
fn is_name_alphabet(name: &str) -> bool {
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Checks a new queue's name against AWS SQS's rule: 1 to 80 letters,
/// digits, hyphens and underscores. As on AWS, a name may end in `.fifo`,
/// which counts toward the 80.
fn validate_queue_name(name: &str) -> Result<(), Error> {
    let stem = name.strip_suffix(".fifo").unwrap_or(name);
    if stem.is_empty() || name.len() > 80 || !is_name_alphabet(stem) {
        return Err(Error::invalid_parameter(format!(
            "QueueName: can only include letters, digits, hyphens and \
             underscores (optionally ending in .fifo), 1 to 80 characters; got {name:?}"
        )));
    }
    Ok(())
}

/// Checks a new namespace's name: 1 to 32 letters, digits, hyphens and
/// underscores.
fn validate_namespace_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.len() > 32 || !is_name_alphabet(name) {
        return Err(Error::invalid_parameter(format!(
            "namespace name: can only include letters, digits, hyphens and \
             underscores, 1 to 32 characters; got {name:?}"
        )));
    }
    Ok(())
}

/// Main service struct that handles all queue operations.
///
/// The service manages:
/// - Queue and message operations
/// - User authentication and authorization
/// - Database connections
/// - Key management for encryption
#[derive(Clone)]
pub struct Service {
    kms: Arc<dyn KeyManager>,
    db: SqlitePool,
    config: Arc<crate::config::Config>,
    /// Resolved SigV4 signing material by access key id — see
    /// [`Service::signing_key`].
    signing_keys: Arc<std::sync::RwLock<HashMap<String, CachedSigningKey>>>,
    /// Resolved queue authorizations by (namespace, queue, caller email) —
    /// see [`Service::resolve_authorized_queue`].
    authorized_queues:
        Arc<std::sync::RwLock<HashMap<(String, String, String), CachedAuthorizedQueue>>>,
    /// Where metrics are recorded (`crate::telemetry`); records nothing
    /// unless they're exported.
    telemetry: crate::telemetry::Telemetry,
    /// Cancelled when the server starts stopping: long polls then answer
    /// at once rather than hold up the shutdown (see `crate::run`).
    stopping: tokio_util::sync::CancellationToken,
}

/// The columns a statement that changes or deletes messages returns for
/// telemetry ([`MessageFactsRow`]): identifiers and timings, never content.
const MESSAGE_FACTS: &str =
    "message_id, tries, COALESCE(sent_at_ms, received_at * 1000) AS sent_at_ms, aws_trace_header";

#[derive(Clone, sqlx::FromRow)]
struct MessageFactsRow {
    message_id: String,
    tries: i64,
    sent_at_ms: Option<i64>,
    aws_trace_header: Option<String>,
}

impl From<MessageFactsRow> for crate::telemetry::MessageFacts {
    fn from(row: MessageFactsRow) -> Self {
        crate::telemetry::MessageFacts {
            id: row.message_id,
            tries: row.tries as u64,
            sent_at_ms: row.sent_at_ms.map(|at| at as u64),
            trace_header: row.aws_trace_header,
            traceparent: None,
        }
    }
}

/// [`MessageFactsRow`] with the receipt handle that matched it.
#[derive(sqlx::FromRow)]
struct HandledMessageRow {
    receipt_handle: String,
    #[sqlx(flatten)]
    facts: MessageFactsRow,
}

/// A cached [`AuthorizedQueue`] with its resolution time, for TTL expiry.
#[derive(Clone, Copy)]
struct CachedAuthorizedQueue {
    value: AuthorizedQueue,
    cached_at: std::time::Instant,
}

/// A queue resolved together with the caller's authorization in one read —
/// see [`Service::resolve_authorized_queue`].
#[derive(Debug, Clone, Copy)]
pub struct AuthorizedQueue {
    pub queue_id: u64,
    /// The caller's user id (from their permission row); the send paths
    /// record it as `sent_by`.
    pub user_id: u64,
}

/// SQL condition on a `users` row: it is the only active admin left. User
/// updates that would leave no admin to run the server (delete, demote,
/// disable) carry `NOT (...)` of it in their WHERE clause, so the check and
/// the write are one statement and concurrent requests cannot both pass it.
const LAST_ACTIVE_ADMIN: &str = "role = 'admin' AND disabled_at IS NULL \
     AND (SELECT COUNT(*) FROM users WHERE role = 'admin' AND disabled_at IS NULL) <= 1";

/// A user as the admin API lists them.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
pub struct UserInfo {
    pub email: String,
    pub role: Role,
    /// Disabled users can neither log in nor use their API keys.
    pub disabled: bool,
}

/// A user holding a permission on a namespace.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
pub struct NamespaceMember {
    pub email: String,
    /// Owners may delete the namespace and manage its queues.
    pub owner: bool,
}

/// An API key as listed: never its secret.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq)]
pub struct ApiKeyInfo {
    pub name: String,
    pub namespace: String,
    /// The most the key may do; its owner's own level still applies.
    pub access: KeyAccess,
}

/// What a caller may do in one namespace — see [`Service::check_user_access`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceAccess {
    pub user_id: u64,
    /// Admins can do everything in every namespace, with or without a
    /// permission row.
    pub is_admin: bool,
    /// Owners can delete the namespace and manage its queues.
    pub is_owner: bool,
}

impl NamespaceAccess {
    /// Whether the caller may delete the namespace and create, delete,
    /// purge or configure its queues. Members without it may only send and
    /// receive messages.
    pub fn can_manage(&self) -> bool {
        self.is_admin || self.is_owner
    }
}

/// A fully resolved SigV4 credential: the decrypted signing secret plus the
/// scope it authenticates (cached by [`Service::signing_key`]).
#[derive(Clone)]
pub struct CachedSigningKey {
    pub secret: secrecy::SecretString,
    pub namespace: String,
    /// The most the key may do; its owner's own level still applies.
    pub access: KeyAccess,
    pub user: crate::api::auth::User,
    cached_at: std::time::Instant,
}

#[bon::bon]
impl Service {
    /// Returns a reference to the underlying SQLite connection pool.
    pub fn db(&self) -> &SqlitePool {
        &self.db
    }

    pub fn telemetry(&self) -> &crate::telemetry::Telemetry {
        &self.telemetry
    }

    /// Cancelled when the server starts stopping.
    pub fn stopping(&self) -> &tokio_util::sync::CancellationToken {
        &self.stopping
    }

    /// Creates a new Service instance with default configuration and in-memory key management.
    ///
    /// Mostly useful for tests and debugging.
    #[allow(unused)]
    pub async fn connect() -> Result<Self, Error> {
        Self::connect_with()
            .config(Config::default())
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
    }

    /// Returns a reference to the service configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Creates a new Service instance with custom configuration and key management.
    ///
    /// # Arguments
    /// * `config` - Custom service configuration
    /// * `kms_factory` - Factory function to create a key management service
    /// * `telemetry` - Where to record metrics; by default, nowhere
    #[builder]
    pub async fn connect_with<K, F, R>(
        config: Config,
        kms_factory: F,
        #[builder(default)] telemetry: crate::telemetry::Telemetry,
    ) -> Result<Self, Error>
    where
        F: FnOnce(SqlitePool) -> R,
        R: Future<Output = Result<K, Error>>,
        K: KeyManager,
    {
        let opts = SqliteConnectOptions::new()
            .filename(config.db_path())
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            // The canonical WAL pairing: skips the per-commit fsync of the
            // default FULL while staying durable for everything except,
            // at worst, the very last commits on power loss — the WAL
            // itself is still synced at checkpoints, so corruption cannot
            // occur. FULL's extra fsync dominated write latency.
            .synchronous(SqliteSynchronous::Normal)
            .locking_mode(SqliteLockingMode::Normal)
            .optimize_on_close(true, None)
            // FULL relocates freed pages on *every* deleting commit, taxing
            // the hot ack/purge paths. INCREMENTAL keeps the same page
            // bookkeeping (so the switch applies to existing databases
            // without a VACUUM) but defers the actual reclamation to the
            // periodic `incremental_vacuum` in `spawn_db_maintenance`.
            .auto_vacuum(SqliteAutoVacuum::Incremental);

        Self::migrate(&opts).await?;

        let pool = SqlitePoolOptions::new().connect_with(opts).await?;

        let kms = kms_factory(pool.clone()).await?;

        let svc = Self {
            kms: Arc::new(kms),
            db: pool,
            config: Arc::new(config),
            signing_keys: Arc::new(std::sync::RwLock::new(HashMap::new())),
            authorized_queues: Arc::new(std::sync::RwLock::new(HashMap::new())),
            telemetry,
            stopping: tokio_util::sync::CancellationToken::new(),
        };

        let root_email = Email::from_str(svc.config.root_email()).map_err(Error::internal)?;
        let root_password = svc.config().root_password().to_owned();

        // Checked first rather than inferred from `create_user` failing:
        // that path ran on every start and every CLI command, hashing the
        // password and minting a KMS key each time only to discard them.
        let root_exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)")
                .bind(root_email.as_str())
                .fetch_one(svc.db())
                .await?;

        if !root_exists {
            match svc
                .create_user(root_email, root_password, Some(Role::Admin), vec![])
                .await
            {
                // Name any defaults the root user was created with.
                Ok(()) => match (
                    svc.config().root_email_provided(),
                    svc.config().root_password_provided(),
                ) {
                    (true, true) => tracing::info!("Root user created"),
                    (false, true) => tracing::warn!(
                        "Root user created with the default email ({}) - set \
                         NERVEMQ_ROOT_EMAIL to choose your own",
                        svc.config().root_email()
                    ),
                    (true, false) => tracing::warn!(
                        "Root user created with the default password - set \
                         NERVEMQ_ROOT_PASSWORD or change it (`nervemq user passwd`); \
                         don't do this in production!"
                    ),
                    (false, false) => tracing::warn!(
                        "Root user created with the default email ({}) and password - \
                         set NERVEMQ_ROOT_EMAIL and NERVEMQ_ROOT_PASSWORD, or change \
                         the password (`nervemq user passwd`); don't do this in production!",
                        svc.config().root_email()
                    ),
                },
                // Another process (e.g. a CLI command started alongside the
                // server) created it after the check.
                Err(Error::Conflict { .. }) => {
                    tracing::info!("Root user already exists")
                }
                Err(e) => return Err(e),
            }
        } else if svc.config().root_password_provided() {
            // A root password was explicitly configured: overwrite the stored
            // hash so NERVEMQ_ROOT_PASSWORD stays authoritative on every
            // start, not only when the database is first created.
            svc.set_user_password(root_email, root_password).await?;
            tracing::info!("Root user password reset from configuration");
        } else {
            // No password configured: leave the existing one untouched so a
            // password set via the UI/API/CLI survives restarts.
            tracing::info!(
                "Root user already exists; keeping stored password \
                 (no root password configured)"
            );
        }

        Ok(svc)
    }

    /// Applies pending migrations on a dedicated connection that does **not**
    /// enforce foreign keys, then verifies referential integrity.
    ///
    /// SQLite cannot change a column or constraint in place, so migrations
    /// rebuild tables (create, copy, drop, rename). Dropping a table that
    /// others reference runs an implicit `DELETE`, and with enforcement on
    /// that fires their `ON DELETE CASCADE` actions — `defer_foreign_keys`
    /// postpones the checks, not the actions. Migration 0005 rebuilt
    /// `namespaces` and `queues` that way and emptied every table below them.
    /// The SQLite documentation's procedure for such changes is to turn
    /// enforcement off, and `PRAGMA foreign_keys` is a no-op inside the
    /// transaction sqlx wraps each migration in, so it is set on the
    /// connection instead. The pool the service then uses enforces them.
    async fn migrate(opts: &SqliteConnectOptions) -> Result<(), Error> {
        let mut conn = opts.clone().foreign_keys(false).connect().await?;

        sqlx::migrate!("./migrations").run(&mut conn).await?;

        // A migration that orphaned rows would otherwise go unnoticed until
        // some later write tripped over them.
        let violations: Vec<(String, Option<i64>, String)> =
            sqlx::query_as("SELECT \"table\", rowid, parent FROM pragma_foreign_key_check")
                .fetch_all(&mut conn)
                .await?;
        conn.close().await?;

        if let Some((table, rowid, parent)) = violations.first() {
            return Err(Error::internal(eyre::eyre!(
                "migrations left {} foreign key violation(s), first: {table} row {} \
                 references a missing {parent} row",
                violations.len(),
                rowid.map_or("?".to_string(), |r| r.to_string()),
            )));
        }

        Ok(())
    }

    /// Deletes a user account and their associated encryption key.
    ///
    /// The user row is removed with a single atomic statement rather than a
    /// transaction spanning the KMS call: a key manager backed by the same
    /// SQLite pool (e.g. `SqliteKeyManager`) needs the write lock that an
    /// open delete transaction would still hold, deadlocking until the busy
    /// timeout failed the request — deleting a user could never succeed.
    ///
    /// The last active admin cannot be deleted: nobody would be left to
    /// administer the server. The guard is part of the delete statement, so
    /// two concurrent deletes cannot both pass it.
    ///
    /// # Arguments
    /// * `email` - Email address of the user to delete
    pub async fn delete_user(&self, email: Email) -> Result<(), Error> {
        let key_id: Option<String> = sqlx::query_scalar(&format!(
            "
            DELETE FROM users
            WHERE email = $1 AND NOT ({LAST_ACTIVE_ADMIN})
            RETURNING kms_key_id
            "
        ))
        .bind(email.as_str())
        .fetch_optional(self.db())
        .await?;

        let Some(key_id) = key_id else {
            return Err(self.explain_admin_guard(&email).await?);
        };

        // Best effort once the user row is gone: a failure here orphans the
        // KMS key (harmless) and is still reported to the caller.
        self.kms.delete_key(&key_id).await?;

        // The user's API keys were cascade-deleted with the row.
        self.clear_signing_keys();
        // Their queue authorizations died with their permission rows.
        self.clear_authorized_queues();

        Ok(())
    }

    /// The error for a user update that the last-admin guard (or a missing
    /// user) made match no row: `UserNotFound` if there is no such user,
    /// otherwise a conflict, since the user must be the last active admin.
    async fn explain_admin_guard(&self, email: &Email) -> Result<Error, Error> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)")
                .bind(email.as_str())
                .fetch_one(self.db())
                .await?;

        Ok(if exists {
            Error::conflict(format!(
                "{email} is the last active admin; make another user an admin first"
            ))
        } else {
            // Not `UserNotFound`: that is a login failure (401), and this is
            // an admin acting on someone else's account.
            Error::not_found(format!("user {email}"))
        })
    }

    /// Changes a user's role. Demoting the last active admin is refused.
    ///
    /// Admins can reach every namespace without a permission row, so cached
    /// queue authorizations are dropped: a demoted admin must lose that at
    /// once, not when the cache expires.
    pub async fn set_user_role(&self, email: &Email, role: Role) -> Result<(), Error> {
        let result = sqlx::query(&format!(
            "
            UPDATE users SET role = $2
            WHERE email = $1 AND ($2 = 'admin' OR NOT ({LAST_ACTIVE_ADMIN}))
            "
        ))
        .bind(email.as_str())
        .bind(&role)
        .execute(self.db())
        .await?;

        if result.rows_affected() == 0 {
            return Err(self.explain_admin_guard(email).await?);
        }

        self.clear_authorized_queues();
        self.clear_signing_keys();

        Ok(())
    }

    /// Disables or re-enables a user. A disabled user keeps their account,
    /// permissions and API keys but can neither log in nor authenticate with
    /// those keys; their open sessions stop working on the next request.
    /// Disabling the last active admin is refused.
    pub async fn set_user_disabled(&self, email: &Email, disabled: bool) -> Result<(), Error> {
        let result = if disabled {
            sqlx::query(&format!(
                "
                UPDATE users SET disabled_at = IFNULL(disabled_at, unixepoch('now'))
                WHERE email = $1 AND NOT ({LAST_ACTIVE_ADMIN})
                "
            ))
            .bind(email.as_str())
            .execute(self.db())
            .await?
        } else {
            sqlx::query("UPDATE users SET disabled_at = NULL WHERE email = $1")
                .bind(email.as_str())
                .execute(self.db())
                .await?
        };

        if result.rows_affected() == 0 {
            return Err(self.explain_admin_guard(email).await?);
        }

        // Cached signing keys would keep authenticating the user's API keys
        // until they expired.
        self.clear_signing_keys();
        self.clear_authorized_queues();

        Ok(())
    }

    /// Lists every user with their role and whether they are disabled.
    pub async fn list_users(&self) -> Result<Vec<UserInfo>, Error> {
        Ok(sqlx::query_as(
            "
            SELECT email, role, disabled_at IS NOT NULL AS disabled
            FROM users
            ORDER BY email
            ",
        )
        .fetch_all(self.db())
        .await?)
    }

    /// Changes a user's own password after checking their current one.
    pub async fn change_password(
        &self,
        email: Email,
        current_password: String,
        new_password: String,
    ) -> Result<(), Error> {
        if new_password.is_empty() {
            return Err(Error::invalid_parameter("password must not be empty"));
        }

        let hashed: Option<String> =
            sqlx::query_scalar("SELECT hashed_pass FROM users WHERE email = $1 AND disabled_at IS NULL")
                .bind(email.as_str())
                .fetch_optional(self.db())
                .await?;
        let Some(hashed) = hashed else {
            return Err(Error::Unauthorized);
        };

        let hashed = PasswordHashString::new(&hashed).map_err(Error::internal)?;
        web::block(move || verify_secret(SecretString::from(current_password), hashed))
            .await
            .map_err(Error::internal)?
            .map_err(|_| Error::forbidden("current password is incorrect"))?;

        self.set_user_password(email, new_password).await?;

        Ok(())
    }

    /// Lists the members of a namespace — users holding a permission row on
    /// it — and which of them own it. Admins without a row are not listed:
    /// they reach every namespace by their role.
    pub async fn list_namespace_members(
        &self,
        namespace: &str,
    ) -> Result<Vec<NamespaceMember>, Error> {
        let ns_id = self
            .get_namespace_id(namespace, self.db())
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;

        Ok(sqlx::query_as(
            "
            SELECT u.email, p.is_owner AS owner
            FROM user_permissions p
            JOIN users u ON u.id = p.user
            WHERE p.namespace = $1
            ORDER BY u.email
            ",
        )
        .bind(ns_id as i64)
        .fetch_all(self.db())
        .await?)
    }

    /// Makes a user an owner of a namespace, or stops them being one.
    /// Becoming an owner grants access to the namespace if the user did not
    /// have it; losing ownership keeps their access as a plain member. Any
    /// number of users, including none, can own a namespace.
    pub async fn set_namespace_owner(
        &self,
        namespace: &str,
        email: &Email,
        owner: bool,
    ) -> Result<(), Error> {
        let ns_id = self
            .get_namespace_id(namespace, self.db())
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;
        let user_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(email.as_str())
            .fetch_optional(self.db())
            .await?
            .ok_or_else(|| Error::not_found(format!("user {email}")))?;

        if owner {
            sqlx::query(
                "
                INSERT INTO user_permissions (user, namespace, is_owner)
                VALUES ($1, $2, true)
                ON CONFLICT (user, namespace) DO UPDATE SET is_owner = true
                ",
            )
            .bind(user_id)
            .bind(ns_id as i64)
            .execute(self.db())
            .await?;
        } else {
            sqlx::query(
                "
                UPDATE user_permissions SET is_owner = false
                WHERE user = $1 AND namespace = $2
                ",
            )
            .bind(user_id)
            .bind(ns_id as i64)
            .execute(self.db())
            .await?;
        }

        Ok(())
    }

    /// Grants a user access to namespaces as a member. Existing grants,
    /// ownership included, are left as they are.
    pub async fn grant_user_namespaces(
        &self,
        email: &Email,
        namespaces: &[String],
    ) -> Result<(), Error> {
        let user_id = self.user_id_for_grants(email, namespaces).await?;

        let mut tx = self.db().begin().await?;
        for namespace in namespaces {
            sqlx::query(
                "
                INSERT INTO user_permissions (user, namespace)
                VALUES ($1, (SELECT id FROM namespaces WHERE name = $2))
                ON CONFLICT DO NOTHING
                ",
            )
            .bind(user_id)
            .bind(namespace)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        Ok(())
    }

    /// Removes a user's access to namespaces, ownership included. Their API
    /// keys for those namespaces stop working at once.
    pub async fn revoke_user_namespaces(
        &self,
        email: &Email,
        namespaces: &[String],
    ) -> Result<(), Error> {
        sqlx::query(
            "
            DELETE FROM user_permissions
            WHERE user = (SELECT id FROM users WHERE email = $1)
            AND namespace IN (
                SELECT ns.id FROM namespaces ns JOIN json_each($2) j ON j.value = ns.name
            )
            ",
        )
        .bind(email.as_str())
        .bind(serde_json::to_string(namespaces)?)
        .execute(self.db())
        .await?;

        // Revoked access must stop authorizing immediately, not at the cache TTL.
        self.clear_authorized_queues();

        Ok(())
    }

    /// Makes `namespaces` exactly the set a user can access. Grants that
    /// stay keep their ownership — replacing the set used to delete and
    /// re-insert every row, which silently stripped owners of ownership.
    pub async fn set_user_namespaces(
        &self,
        email: &Email,
        namespaces: &[String],
    ) -> Result<(), Error> {
        let user_id = self.user_id_for_grants(email, namespaces).await?;

        let mut tx = self.db().begin().await?;
        sqlx::query(
            "
            DELETE FROM user_permissions
            WHERE user = $1
            AND namespace NOT IN (
                SELECT ns.id FROM namespaces ns JOIN json_each($2) j ON j.value = ns.name
            )
            ",
        )
        .bind(user_id)
        .bind(serde_json::to_string(namespaces)?)
        .execute(&mut *tx)
        .await?;
        for namespace in namespaces {
            sqlx::query(
                "
                INSERT INTO user_permissions (user, namespace)
                VALUES ($1, (SELECT id FROM namespaces WHERE name = $2))
                ON CONFLICT DO NOTHING
                ",
            )
            .bind(user_id)
            .bind(namespace)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;

        // The new set may have removed namespaces; revocations must stop
        // authorizing immediately, not at the cache TTL.
        self.clear_authorized_queues();

        Ok(())
    }

    /// Resolves the user a grant is for, and checks every namespace named in
    /// it exists — reported as not found rather than as the NOT NULL failure
    /// the insert would hit.
    async fn user_id_for_grants(&self, email: &Email, namespaces: &[String]) -> Result<i64, Error> {
        let user_id: i64 = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(email.as_str())
            .fetch_optional(self.db())
            .await?
            .ok_or_else(|| Error::not_found(format!("user {email}")))?;

        for namespace in namespaces {
            if self.get_namespace_id(namespace, self.db()).await?.is_none() {
                return Err(Error::namespace_not_found(namespace));
            }
        }

        Ok(user_id)
    }

    /// Lists a user's API keys (name and namespace; secrets are never
    /// returned).
    pub async fn list_user_tokens(&self, email: &str) -> Result<Vec<ApiKeyInfo>, Error> {
        Ok(sqlx::query_as(
            "
            SELECT k.name, ns.name AS namespace, k.access FROM api_keys k
            JOIN users u ON u.id = k.user
            JOIN namespaces ns ON ns.id = k.ns
            WHERE u.email = $1
            ORDER BY k.name
            ",
        )
        .bind(email)
        .fetch_all(self.db())
        .await?)
    }

    /// Deletes one of a user's API keys by name and drops it from the
    /// signing-key cache, so it stops authenticating at once.
    pub async fn delete_user_token(&self, email: &str, name: &str) -> Result<(), Error> {
        let key_id: Option<String> = sqlx::query_scalar(
            "
            DELETE FROM api_keys
            WHERE name = $1
            AND user IN (SELECT id FROM users WHERE email = $2)
            RETURNING key_id
            ",
        )
        .bind(name)
        .bind(email)
        .fetch_optional(self.db())
        .await?;

        let Some(key_id) = key_id else {
            return Err(Error::not_found(format!("api key {name}")));
        };

        self.invalidate_signing_key(&key_id);

        Ok(())
    }

    /// Checks that a user is an active admin: `Forbidden` for anyone else,
    /// `Unauthorized` if the user is unknown or disabled.
    pub async fn require_admin(&self, identity: &Identity) -> Result<(), Error> {
        let role: Option<Role> =
            sqlx::query_scalar("SELECT role FROM users WHERE email = $1 AND disabled_at IS NULL")
                .bind(identity.id()?)
                .fetch_optional(self.db())
                .await?;

        match role {
            Some(Role::Admin) => Ok(()),
            Some(Role::User) => Err(Error::forbidden("only admins can do this")),
            None => Err(Error::Unauthorized),
        }
    }

    /// Gets the internal ID for a queue given its namespace and name.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `name` - Name of the queue
    /// * `exec` - Database executor to use
    pub async fn get_queue_id(
        &self,
        namespace: &str,
        name: &str,
        exec: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<Option<u64>, Error> {
        Ok(sqlx::query_scalar(
            "
            SELECT q.id FROM queues q
            JOIN namespaces n ON q.ns = n.id
            WHERE n.name = $1 AND q.name = $2
            ",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&mut *exec.acquire().await?)
        .await?)
    }

    /// When a queue was created and when its attributes last changed, in
    /// unix seconds (AWS's `CreatedTimestamp` and `LastModifiedTimestamp`).
    /// The caller checks access.
    pub async fn queue_times(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<(Option<u64>, Option<u64>), Error> {
        let times: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
            "
            SELECT q.created_at, q.attributes_modified_at FROM queues q
            JOIN namespaces n ON q.ns = n.id
            WHERE n.name = $1 AND q.name = $2
            ",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(self.db())
        .await?;
        let (created, modified) = times.ok_or_else(|| Error::queue_not_found(name, namespace))?;
        let unsigned = |t: Option<i64>| t.and_then(|t| u64::try_from(t).ok());
        Ok((unsigned(created), unsigned(modified)))
    }

    /// Gets the internal ID for the user behind an authenticated identity.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user (session or API key)
    /// * `ex` - Database executor to use
    pub async fn get_user_id(
        &self,
        identity: &Identity,
        ex: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<Option<u64>, Error> {
        Ok(sqlx::query_scalar(
            "
            SELECT id FROM users WHERE email = $1
            ",
        )
        .bind(identity.id()?)
        .fetch_optional(&mut *ex.acquire().await?)
        .await?)
    }

    /// Gets the internal ID for a namespace given its name.
    ///
    /// # Arguments
    /// * `name` - Name of the namespace
    /// * `ex` - Database executor to use
    pub async fn get_namespace_id<'a>(
        &self,
        name: &str,
        ex: impl Acquire<'a, Database = Sqlite>,
    ) -> Result<Option<u64>, Error> {
        Ok(sqlx::query_scalar(
            "
            SELECT id FROM namespaces WHERE name = $1
            ",
        )
        .bind(name)
        .fetch_optional(&mut *ex.acquire().await?)
        .await?)
    }

    /// Lists the namespaces the authenticated user can access: every
    /// namespace for an admin, otherwise those they hold a permission on.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn list_namespaces(&self, identity: Identity) -> Result<Vec<Namespace>, Error> {
        let email = identity.id()?;

        Ok(sqlx::query_as(
            "
            SELECT ns.id, ns.name, ns.created_by_email AS created_by
            FROM users u
            JOIN namespaces ns
            LEFT JOIN user_permissions p ON p.namespace = ns.id AND p.user = u.id
            WHERE u.email = $1 AND (u.role = 'admin' OR p.id IS NOT NULL)
            ORDER BY ns.name
        ",
        )
        .bind(email)
        .fetch_all(&mut *self.db.acquire().await?)
        .await?)
    }

    /// Verifies that a user is active and has at least the specified role
    /// level. A disabled user fails this for every role, which is what shuts
    /// them out of every protected route, session or API key alike.
    ///
    /// # Arguments
    /// * `identity` - Identity of the user to check
    /// * `role` - Minimum required role level
    pub async fn check_user_role(&self, identity: Identity, role: Role) -> Result<(), Error> {
        let email = identity.id()?;
        let user_role: Option<Role> =
            sqlx::query_scalar("SELECT role FROM users WHERE email = $1 AND disabled_at IS NULL")
                .bind(email)
                .fetch_optional(&mut *self.db.acquire().await?)
                .await?;

        match user_role {
            Some(user_role) if user_role >= role => Ok(()),
            _ => Err(Error::Unauthorized),
        }
    }

    /// Creates a new namespace. Only admin users can create namespaces; the
    /// creator is recorded and made an owner.
    ///
    /// # Arguments
    /// * `name` - Name of the namespace to create
    /// * `identity` - Identity of the authenticated admin user
    ///
    /// # Returns
    /// The new namespace's id.
    pub async fn create_namespace(&self, name: &str, identity: Identity) -> Result<u64, Error> {
        let user_email = identity.id()?;
        validate_namespace_name(name)?;

        // Checked on the pool, before the write transaction, so the
        // transaction starts with its write (see "Concurrency notes" in
        // docs/architecture/message-lifecycle.md).
        let user: User = sqlx::query_as("SELECT * FROM users WHERE email = $1")
            .bind(&user_email)
            .fetch_optional(self.db())
            .await?
            .ok_or_else(|| Error::Unauthorized)?;

        if user.role != Role::Admin {
            return Err(Error::forbidden("only admins can create namespaces"));
        }

        let mut tx = self.db().begin().await?;

        let ns_id: u64 = sqlx::query_scalar(
            "
            INSERT INTO namespaces (name, created_by, created_by_email)
            VALUES ($1, $2, $3)
            RETURNING id
            ",
        )
        .bind(name)
        .bind(user.id as i64)
        .bind(&user.email)
        .fetch_one(&mut *tx.as_mut().acquire().await?)
        .await?;

        sqlx::query(
            "
            INSERT INTO user_permissions (user, namespace, is_owner)
            VALUES ($1, $2, true)
        ",
        )
        .bind(user.id as i64)
        .bind(ns_id as i64)
        .execute(&mut *tx.as_mut().acquire().await?)
        .await?;

        tx.commit().await?;

        Ok(ns_id)
    }

    /// Deletes a namespace and all its queues. Admins and the namespace's
    /// owners may do this.
    ///
    /// # Arguments
    /// * `name` - Name of the namespace to delete
    /// * `identity` - Identity of the authenticated user
    pub async fn delete_namespace(&self, name: &str, identity: Identity) -> Result<(), Error> {
        // Checked on the pool, before the write transaction, so the
        // transaction starts with its write (see "Concurrency notes" in
        // docs/architecture/message-lifecycle.md).
        let namespace = self
            .get_namespace_id(name, self.db())
            .await?
            .ok_or_else(|| Error::namespace_not_found(name))?;

        let access = self
            .check_user_access(&identity, namespace, self.db())
            .await?;

        if !access.can_manage() {
            return Err(Error::forbidden(format!(
                "only admins and owners of namespace {name} can delete it"
            )));
        }

        let mut tx = self.db().begin().await?;

        sqlx::query(
            "
            DELETE FROM namespaces WHERE name = $1
        ",
        )
        .bind(name)
        .execute(&mut *tx)
        .await
        .map(|_| ())?;

        tx.commit().await?;

        // The namespace's API keys were cascade-deleted with it.
        self.clear_signing_keys();
        // So were its queues and permission rows.
        self.clear_authorized_queues();

        Ok(())
    }

    /// Resolves a queue for an authenticated caller in **one read**:
    /// namespace existence, the caller's permission on it, and the queue id
    /// — replacing the three separate lookups (`get_namespace_id` +
    /// `check_user_access` + `get_queue_id`) the hot SQS paths used to run
    /// per request. Also returns the caller's user id, sparing the send
    /// paths their separate `get_user_id` read for `sent_by`.
    ///
    /// Error semantics match the separate lookups: unknown namespace →
    /// `namespace_not_found`; neither an admin nor holding a
    /// `user_permissions` row → `Unauthorized`; unknown queue →
    /// `queue_not_found`.
    ///
    /// This authorizes membership only — sending, receiving and acking.
    /// Managing the queue additionally needs [`Self::require_queue_manager`].
    pub async fn resolve_authorized_queue(
        &self,
        namespace: &str,
        queue: &str,
        identity: &Identity,
    ) -> Result<AuthorizedQueue, Error> {
        let email = identity.id()?;

        // Cache hit: this runs on every send/receive/ack, and a hit makes
        // the whole authorization step memory-only. Only successful
        // resolutions are cached (never errors), entries expire after
        // [`Self::AUTHORIZED_QUEUE_TTL`], and every mutation that could
        // *revoke* a cached answer invalidates eagerly (queue create/delete,
        // namespace delete, user delete, permission revocation, role change,
        // disabling a user). Permission *grants* need no invalidation: they
        // only turn future misses into hits.
        let key = (
            namespace.to_owned(),
            queue.to_owned(),
            email.clone(),
        );
        if let Some(hit) = self
            .authorized_queues
            .read()
            .expect("authorized queue cache poisoned")
            .get(&key)
        {
            if hit.cached_at.elapsed() < Self::AUTHORIZED_QUEUE_TTL {
                return Ok(hit.value);
            }
        }

        let row: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
            "
            SELECT q.id, u.id
            FROM namespaces n
            LEFT JOIN queues q ON q.ns = n.id AND q.name = $2
            LEFT JOIN users u ON u.email = $3 AND (
                u.role = 'admin'
                OR EXISTS (
                    SELECT 1 FROM user_permissions p
                    WHERE p.namespace = n.id AND p.user = u.id
                )
            )
            WHERE n.name = $1
            ",
        )
        .bind(namespace)
        .bind(queue)
        .bind(email)
        .fetch_optional(self.db())
        .await?;

        let Some((queue_id, user_id)) = row else {
            return Err(Error::namespace_not_found(namespace));
        };
        let Some(user_id) = user_id else {
            return Err(Error::Unauthorized);
        };
        let Some(queue_id) = queue_id else {
            return Err(Error::queue_not_found(queue, namespace));
        };

        let authorized = AuthorizedQueue {
            queue_id: queue_id as u64,
            user_id: user_id as u64,
        };

        self.authorized_queues
            .write()
            .expect("authorized queue cache poisoned")
            .insert(
                key,
                CachedAuthorizedQueue {
                    value: authorized,
                    cached_at: std::time::Instant::now(),
                },
            );

        Ok(authorized)
    }

    /// How long a cached queue authorization may be served without
    /// re-reading the database; bounds staleness for any revocation path
    /// that slips past the eager invalidation hooks.
    const AUTHORIZED_QUEUE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

    /// Drops cached authorizations for one queue (all callers). Used when
    /// the queue is created or deleted: ids must never be served across a
    /// delete/re-create of the same name.
    pub fn invalidate_authorized_queue(&self, namespace: &str, queue: &str) {
        self.authorized_queues
            .write()
            .expect("authorized queue cache poisoned")
            .retain(|(ns, q, _), _| !(ns == namespace && q == queue));
    }

    /// Drops every cached queue authorization. Used for coarse events whose
    /// affected key set is unknown or unbounded: namespace deletion (queues
    /// cascade), user deletion, permission revocation, a role change or a
    /// user being disabled.
    pub fn clear_authorized_queues(&self) {
        self.authorized_queues
            .write()
            .expect("authorized queue cache poisoned")
            .clear();
    }

    /// Checks that a user may access a namespace — they are an admin, or
    /// hold a permission row for it — and returns what they may do there.
    ///
    /// # Arguments
    /// * `identity` - Identity of the user to check
    /// * `ns` - ID of the namespace
    /// * `exec` - Database executor to use
    pub async fn check_user_access<'a>(
        &self,
        identity: &Identity,
        ns: u64,
        exec: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<NamespaceAccess, Error> {
        let email = identity.id()?;
        let mut db = exec.acquire().await?;

        let row: Option<(i64, Role, Option<i64>, Option<bool>)> = sqlx::query_as(
            "
            SELECT u.id, u.role, p.id, p.is_owner
            FROM users u
            LEFT JOIN user_permissions p ON p.user = u.id AND p.namespace = $2
            WHERE u.email = $1
        ",
        )
        .bind(email)
        .bind(ns as i64)
        .fetch_optional(&mut *db)
        .await?;

        let Some((user_id, role, permission, is_owner)) = row else {
            return Err(Error::Unauthorized);
        };
        let is_admin = role == Role::Admin;
        if !is_admin && permission.is_none() {
            return Err(Error::Unauthorized);
        }

        Ok(NamespaceAccess {
            user_id: user_id as u64,
            is_admin,
            is_owner: is_owner.unwrap_or(false),
        })
    }

    /// Checks that a user may manage the queues of a namespace — create,
    /// delete, purge and configure them — which takes an admin or one of the
    /// namespace's owners. Plain members may only send and receive messages.
    ///
    /// # Arguments
    /// * `identity` - Identity of the user to check
    /// * `namespace` - Name of the namespace
    pub async fn require_queue_manager(
        &self,
        identity: &Identity,
        namespace: &str,
    ) -> Result<NamespaceAccess, Error> {
        let ns_id = self
            .get_namespace_id(namespace, self.db())
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;

        let access = self.check_user_access(identity, ns_id, self.db()).await?;
        if !access.can_manage() {
            return Err(Error::forbidden(format!(
                "only admins and owners of namespace {namespace} can manage its queues"
            )));
        }

        Ok(access)
    }

    /// Creates a new queue in a namespace.
    ///
    /// A name that is already taken follows AWS: if every attribute in the
    /// request matches the existing queue (attributes left out are not
    /// compared), nothing changes and the outcome is
    /// [`CreateQueueOutcome::AlreadyExists`]; otherwise it is
    /// [`Error::QueueAlreadyExists`]. Tags are neither compared nor applied
    /// to an existing queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace to create the queue in
    /// * `name` - Name of the queue
    /// * `attributes` - Queue configuration attributes
    /// * `tags` - Metadata tags for the queue
    /// * `identity` - Identity of the authenticated user
    pub async fn create_queue(
        &self,
        namespace: &str,
        name: &str,
        attributes: QueueAttributeMap,
        tags: HashMap<String, String>,
        identity: Identity,
    ) -> Result<CreateQueueOutcome, Error> {
        let attributes = QueueAttributesSer::from_request(attributes, name, AttributeWrite::Create)?;
        crate::sqs::limits::check_tags(&tags)?;

        // New names follow AWS's rule. A queue that already exists under a
        // name from before the rule still resolves as usual, so an
        // idempotent CreateQueue at application start keeps working.
        if let Err(e) = validate_queue_name(name) {
            if self.get_queue_id(namespace, name, self.db()).await?.is_none() {
                return Err(e);
            }
        }

        // Resolved on the pool, before the write transaction: a transaction
        // that reads before its first write fails with SQLITE_BUSY_SNAPSHOT
        // if another writer commits in between (see "Concurrency notes" in
        // docs/architecture/message-lifecycle.md).
        let namespace_id = self
            .get_namespace_id(namespace, self.db())
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;

        let access = self
            .check_user_access(&identity, namespace_id, self.db())
            .await?;
        if !access.can_manage() {
            return Err(Error::forbidden(format!(
                "only admins and owners of namespace {namespace} can manage its queues"
            )));
        }
        let user_id = access.user_id;

        let mut tx = self.db().begin().await?;

        // `DO NOTHING` instead of checking for the name first, so two
        // concurrent creates can't both pass the check and leave the loser
        // with a unique-constraint failure.
        let queue_id: Option<u64> = sqlx::query_scalar(
            "
            INSERT INTO queues (ns, name, created_by, created_at, attributes_modified_at)
            VALUES ($1, $2, $3, unixepoch(), unixepoch())
            ON CONFLICT DO NOTHING
            RETURNING id
        ",
        )
        .bind(namespace_id as i64)
        .bind(name)
        .bind(user_id as i64)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(queue_id) = queue_id else {
            let queue_id = self
                .get_queue_id(namespace, name, &mut *tx)
                .await?
                .ok_or_else(|| Error::queue_not_found(name, namespace))?;
            let current = Self::read_queue_attributes(&mut tx, queue_id).await?;
            return match first_attribute_mismatch(&attributes, &current) {
                None => Ok(CreateQueueOutcome::AlreadyExists),
                Some(attribute) => Err(Error::QueueAlreadyExists {
                    queue: name.to_owned(),
                    namespace: namespace.to_owned(),
                    attribute,
                }),
            };
        };

        sqlx::query(
            "
            INSERT INTO queue_configurations (queue, max_retries)
            VALUES ($1, $2)
        ",
        )
        .bind(queue_id as i64)
        .bind(self.config.default_max_retries() as i64)
        .execute(&mut *tx)
        .await?;

        // Stored through the same path as SetQueueAttributes so create-time
        // attributes are actually honored by the receive path.
        Self::write_queue_attributes(&mut tx, queue_id, attributes).await?;

        for (k, v) in tags.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_tags (queue, k, v)
                VALUES ($1, $2, $3)
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        // Closes the delete/re-create race: a resolve that read the old
        // queue's id before its delete committed may have repopulated the
        // cache after the delete's invalidation ran. Never serve the dead
        // id for the re-created name.
        self.invalidate_authorized_queue(namespace, name);

        Ok(CreateQueueOutcome::Created)
    }

    pub fn kms(&self) -> &dyn KeyManager {
        self.kms.as_ref()
    }

    /// Updates the attributes of an existing queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `attributes` - New queue attributes
    /// * `identity` - Identity of the authenticated user
    pub async fn set_queue_attributes(
        &self,
        ns: &str,
        queue: &str,
        attributes: QueueAttributeMap,
        identity: Identity,
    ) -> Result<(), Error> {
        let attributes = QueueAttributesSer::from_request(attributes, queue, AttributeWrite::Set)?;
        let changes = !attributes.is_empty();

        // Checked on the pool, before the write transaction, so the
        // transaction starts with its write (see "Concurrency notes" in
        // docs/architecture/message-lifecycle.md).
        self.require_queue_manager(&identity, ns).await?;

        let queue_id = self
            .get_queue_id(ns, queue, self.db())
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        let mut tx = self.db().begin().await?;

        Self::write_queue_attributes(&mut tx, queue_id, attributes).await?;
        if changes {
            // AWS's LastModifiedTimestamp.
            sqlx::query("UPDATE queues SET attributes_modified_at = unixepoch() WHERE id = $1")
                .bind(queue_id as i64)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Upserts queue attributes under their snake_case storage keys. Shared
    /// by `set_queue_attributes` and `create_queue` so create-time attributes
    /// land under the same keys the receive path and `get_queue_attributes`
    /// look up.
    async fn write_queue_attributes(
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        queue_id: u64,
        attributes: QueueAttributesSer,
    ) -> Result<(), Error> {
        let tx = &mut **tx;

        // NOTE: the `v` column has TEXT affinity (since migration 0005), so
        // the integers bound here are stored as their text rendering;
        // `get_queue_attributes` parses them back leniently.
        if let Some(delay_seconds) = attributes.delay_seconds {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'delay_seconds', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(delay_seconds as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(max_message_size) = attributes.max_message_size {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'max_message_size', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(max_message_size as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(message_retention_period) = attributes.message_retention_period {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'message_retention_period', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(message_retention_period as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(receive_message_wait_time_seconds) =
            attributes.receive_message_wait_time_seconds
        {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'receive_message_wait_time_seconds', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(receive_message_wait_time_seconds as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(visibility_timeout) = attributes.visibility_timeout {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'visibility_timeout', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(visibility_timeout as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(redrive_policy) = attributes.redrive_policy {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'redrive_policy', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(serde_json::Value::String(redrive_policy))
            .execute(&mut *tx)
            .await?;
        }

        for (k, v) in attributes.other.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, $2, $3)
                ON CONFLICT (queue, k) DO UPDATE SET v = $3
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await?;
        }

        Ok(())
    }

    /// Gets the current attributes of a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `names` - Names of attributes to retrieve
    /// * `identity` - Identity of the authenticated user
    pub async fn get_queue_attributes(
        &self,
        ns: &str,
        queue: &str,
        names: &[String],
        identity: &Identity,
    ) -> Result<QueueAttributesSer, Error> {
        let mut db = self.db().acquire().await?;

        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        // As on AWS, only the requested attributes are returned. An empty
        // list is treated like "All" rather than AWS's "none": the dispatch
        // layer always forwards a list, and internal callers expect the lot.
        let set = names.iter().map(String::as_str).collect::<HashSet<_>>();
        let want_all = set.is_empty() || set.contains("All");

        let mut attributes = Self::read_queue_attributes(&mut db, queue_id).await?;
        attributes
            .other
            .retain(|k, _| want_all || set.contains(k.as_str()));

        if !want_all {
            let want = |wire: &str| set.contains(wire);
            if !want("DelaySeconds") {
                attributes.delay_seconds = None;
            }
            if !want("MaximumMessageSize") {
                attributes.max_message_size = None;
            }
            if !want("MessageRetentionPeriod") {
                attributes.message_retention_period = None;
            }
            if !want("ReceiveMessageWaitTimeSeconds") {
                attributes.receive_message_wait_time_seconds = None;
            }
            if !want("VisibilityTimeout") {
                attributes.visibility_timeout = None;
            }
            if !want("RedrivePolicy") {
                attributes.redrive_policy = None;
            }
        }

        Ok(attributes)
    }

    /// Reads every stored attribute of a queue.
    async fn read_queue_attributes(
        conn: &mut sqlx::SqliteConnection,
        queue_id: u64,
    ) -> Result<QueueAttributesSer, Error> {
        // Values are stored as text (TEXT affinity since migration 0005) but
        // aren't uniformly JSON: integers written by `set_queue_attributes`
        // parse as JSON numbers, while plain strings stored at queue creation
        // are not valid JSON. Parse leniently and take non-JSON values
        // verbatim.
        let mut res = sqlx::query_as::<_, (String, String)>(
            "
            SELECT k, v FROM queue_attributes WHERE queue = $1
            ",
        )
        .bind(queue_id as i64)
        .fetch(conn);

        let mut attributes = QueueAttributesSer::default();
        while let Some((k, raw)) = res.next().await.transpose()? {
            let v = serde_json::from_str(&raw).unwrap_or(serde_json::Value::String(raw));
            // An integer that isn't one counts as unset rather than failing
            // the read. Migration 0017 removed the ones that the internal-key
            // bypass stored; this keeps any other from breaking the queue.
            let integer = |v: serde_json::Value| {
                let n = serde_json::from_value::<u64>(v.clone()).ok();
                if n.is_none() {
                    tracing::warn!(attribute = %k, value = %v, "ignoring a stored attribute that isn't a whole number");
                }
                n
            };
            match &*k {
                "delay_seconds" => attributes.delay_seconds = integer(v),
                "max_message_size" => attributes.max_message_size = integer(v),
                "message_retention_period" => attributes.message_retention_period = integer(v),
                "receive_message_wait_time_seconds" => {
                    attributes.receive_message_wait_time_seconds = integer(v)
                }
                "visibility_timeout" => attributes.visibility_timeout = integer(v),
                "redrive_policy" => {
                    attributes.redrive_policy = Some(match v {
                        serde_json::Value::String(s) => s,
                        v => v.to_string(),
                    })
                }
                k if is_untyped_attribute(k) => {
                    let v = match v {
                        serde_json::Value::String(s) => s,
                        v => v.to_string(),
                    };
                    attributes.other.insert(k.to_owned(), serde_json::Value::String(v));
                }
                // Not an attribute a request can now set: stored before names
                // were checked, under an unknown name, or under an AWS name
                // before NerveMQ used internal keys. Never acted on, so not
                // reported either.
                _ => {}
            }
        }

        Ok(attributes)
    }

    /// The queue's message counts by visibility state, for the depth
    /// attributes SQS computes rather than stores (`ApproximateNumberOfMessages`
    /// and friends, #83). Kept apart from [`get_queue_attributes`] so the
    /// admin API's attribute editor, which reads the stored set, never sees
    /// them.
    ///
    /// The three states are the ones the admin statistics already use:
    /// available = visible now with retries left; not visible = received and
    /// neither deleted nor timed out; delayed = sent with a delay that has not
    /// elapsed. A retry-exhausted message counts in none of them.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `identity` - Identity of the authenticated user
    ///
    /// [`get_queue_attributes`]: Self::get_queue_attributes
    pub async fn queue_depth(
        &self,
        ns: &str,
        queue: &str,
        identity: &Identity,
    ) -> Result<QueueDepth, Error> {
        let mut db = self.db().acquire().await?;

        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        Ok(sqlx::query_as(
            "
            SELECT
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                            AND (conf.max_retries IS NULL OR m.tries < conf.max_retries) THEN 1 END) AS available,
                COUNT(CASE WHEN m.delivered_at IS NOT NULL AND m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 1 END) AS not_visible,
                COUNT(CASE WHEN m.delivered_at IS NULL AND m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 1 END) AS delayed
            FROM messages m
            LEFT JOIN queue_configurations conf ON conf.queue = m.queue
            WHERE m.queue = $1
            ",
        )
        .bind(queue_id as i64)
        .fetch_one(&mut *db)
        .await?)
    }

    /// Adds or updates tags on a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `tags` - Tags to set
    /// * `identity` - Identity of the authenticated user
    pub async fn tag_queue(
        &self,
        ns: &str,
        queue: &str,
        tags: HashMap<String, String>,
        identity: Identity,
    ) -> Result<(), Error> {
        crate::sqs::limits::check_tags(&tags)?;
        self.require_queue_manager(&identity, ns).await?;

        let mut db = self.db().acquire().await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        for (k, v) in tags.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_tags (queue, k, v)
                VALUES ($1, $2, $3)
                ON CONFLICT (queue, k) DO UPDATE SET v = $3
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *db)
            .await?;
        }

        Ok(())
    }

    /// Removes tags from a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `tags` - Tags to remove
    /// * `identity` - Identity of the authenticated user
    pub async fn untag_queue(
        &self,
        ns: &str,
        queue: &str,
        tags: Vec<String>,
        identity: Identity,
    ) -> Result<(), Error> {
        self.require_queue_manager(&identity, ns).await?;

        let mut db = self.db().acquire().await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        for tag in tags {
            sqlx::query(
                "
                DELETE FROM queue_tags WHERE queue = $1 AND k = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(tag)
            .execute(&mut *db)
            .await?;
        }

        Ok(())
    }

    /// Gets all tags for a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `identity` - Identity of the authenticated user
    pub async fn get_queue_tags(
        &self,
        ns: &str,
        queue: &str,
        identity: Identity,
    ) -> Result<HashMap<String, String>, Error> {
        let mut db = self.db().acquire().await?;

        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(&identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        let res = sqlx::query_as(
            "
            SELECT k, v FROM queue_tags WHERE queue = $1
            ",
        )
        .bind(queue_id as i64)
        .fetch_all(&mut *db)
        .await?;

        Ok(res.into_iter().collect())
    }

    /// Deletes a queue and all its messages.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `name` - Name of the queue
    /// * `identity` - Identity of the authenticated user
    pub async fn delete_queue(
        &self,
        namespace: &str,
        name: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        // Checked on the pool, before the write transaction, so the
        // transaction starts with its write (see "Concurrency notes" in
        // docs/architecture/message-lifecycle.md).
        self.require_queue_manager(&identity, namespace).await?;

        let id = self
            .get_queue_id(namespace, name, self.db())
            .await?
            .ok_or_else(|| Error::queue_not_found(name, namespace))?;

        let mut tx = self.db().begin().await?;

        sqlx::query("DELETE FROM queues WHERE id = $1")
            .bind(id as i64)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        // Sends and acks against the dead queue id must fail immediately,
        // not until the cache TTL lapses.
        self.invalidate_authorized_queue(namespace, name);

        Ok(())
    }

    /// Lists queues, optionally filtered by namespace.
    ///
    /// # Arguments
    /// * `namespace` - Optional namespace to filter by
    /// * `identity` - Identity of the authenticated user
    pub async fn list_queues(
        &self,
        namespace: Option<&str>,
        identity: Identity,
    ) -> Result<Vec<Queue>, Error> {
        let mut conn = self.db().acquire().await?;

        if let Some(namespace) = namespace {
            let namespace_id = self
                .get_namespace_id(namespace, &mut *conn)
                .await?
                .ok_or_else(|| Error::namespace_not_found(namespace))?;

            self.check_user_access(&identity, namespace_id, &mut *conn)
                .await?;
        }

        // Queue::list(conn.acquire().await?, namespace, identity).await

        match namespace {
            Some(ns) => self.list_queues_for_namespace(ns).await,
            None => self.list_all_queues(identity).await,
        }
    }

    /// Lists all queues in a specific namespace. Does not check access:
    /// callers must (see [`Self::list_queues`]).
    ///
    /// # Arguments
    /// * `namespace` - Namespace to list queues from
    pub async fn list_queues_for_namespace(&self, namespace: &str) -> Result<Vec<Queue>, Error> {
        let mut db = self.db().acquire().await?;
        // LEFT JOIN: `created_by` becomes NULL when the creator is deleted,
        // and an inner join would drop the queue from the listing.
        let mut stream = sqlx::query_as(
            "
            SELECT q.id, q.name, n.name as ns, u.email as created_by, q.paused_at FROM queues q
            JOIN namespaces n ON q.ns = n.id
            LEFT JOIN users u on q.created_by = u.id
            WHERE n.name = $1
            ORDER BY q.name",
        )
        .bind(namespace)
        .fetch(&mut *db);

        let mut queues = Vec::new();

        while let Some(res) = stream.next().await.transpose()? {
            queues.push(res);
        }

        Ok(queues)
    }

    /// Lists all queues accessible to the authenticated user: every queue
    /// for an admin, otherwise those in namespaces they hold a permission on.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn list_all_queues(&self, identity: Identity) -> Result<Vec<Queue>, Error> {
        let email = identity.id()?;

        let queues = sqlx::query_as(
            "
            SELECT q.id, q.name, qu.email as created_by, n.name as ns, q.paused_at
            FROM users u
            JOIN queues q
            JOIN namespaces n ON n.id = q.ns
            LEFT JOIN users qu ON qu.id = q.created_by
            WHERE u.email = $1 AND (
                u.role = 'admin'
                OR EXISTS (
                    SELECT 1 FROM user_permissions p
                    WHERE p.user = u.id AND p.namespace = q.ns
                )
            )
            ORDER BY n.name, q.name
            ",
        )
        .bind(email)
        .fetch_all(&mut *self.db().acquire().await?)
        .await?;

        Ok(queues)
    }

    /// Gets the KMS key ID associated with a user.
    ///
    /// # Arguments
    /// * `user_email` - Email of the user
    pub async fn get_key_id(&self, user_email: &str) -> Result<String, Error> {
        let key_id = sqlx::query_scalar(
            "
            SELECT kms_key_id FROM users
            WHERE email = $1
            ",
        )
        .bind(user_email)
        .fetch_one(self.db())
        .await?;

        Ok(key_id)
    }

    /// Spawns the periodic database maintenance task. Every tick it
    /// enforces `MessageRetentionPeriod` ([`Self::sweep_expired_messages`])
    /// and then runs an `incremental_vacuum` to reclaim pages freed by
    /// deletes (acks, purges, the retention sweep, session GC) in bounded
    /// chunks, off the hot path — the counterpart of
    /// `auto_vacuum = INCREMENTAL` in the connect options, which only
    /// *tracks* free pages without reclaiming them.
    pub fn spawn_db_maintenance(&self) {
        /// How often expired messages are swept and freed pages reclaimed.
        /// Retention is therefore enforced with up to this much lag past
        /// the configured period.
        const MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10 * 60);
        /// Upper bound on pages reclaimed per tick, to keep each sweep's
        /// write work (and lock hold) small.
        const MAX_PAGES_PER_TICK: i64 = 1000;

        let db = self.db.clone();
        let telemetry = self.telemetry.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(MAINTENANCE_INTERVAL);
            loop {
                interval.tick().await;

                Self::sweep_retention(&db, &telemetry).await;

                if let Err(e) = sqlx::query(&format!(
                    "PRAGMA incremental_vacuum({MAX_PAGES_PER_TICK})"
                ))
                .execute(&db)
                .await
                {
                    tracing::warn!("incremental_vacuum failed: {e}");
                }
            }
        });
    }

    /// One retention sweep, as the maintenance task runs it: logged, and
    /// counted as removals (`expired`) per queue.
    pub(crate) async fn sweep_retention(db: &SqlitePool, telemetry: &crate::telemetry::Telemetry) {
        match Self::sweep_expired_messages_by_queue(db).await {
            Ok(swept) if !swept.is_empty() => {
                let total: u64 = swept.iter().map(|(_, _, count)| count).sum();
                tracing::info!(swept = total, "Expired messages deleted (MessageRetentionPeriod)");
                for (namespace, queue, count) in &swept {
                    telemetry.removed_count(
                        crate::telemetry::Queue {
                            namespace,
                            name: queue,
                        },
                        crate::telemetry::Removal::Expired,
                        *count,
                    );
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("message retention sweep failed: {e}"),
        }
    }

    /// Deletes messages that have outlived their queue's
    /// `MessageRetentionPeriod` (seconds since the message arrived,
    /// measured against `received_at`). Returns the number deleted.
    ///
    /// A queue with no retention attribute — or with the explicit value
    /// `0` — retains messages forever. (`0` is a safe "forever" sentinel:
    /// AWS's minimum is 60 s, so it can never be a real period.) The sweep
    /// applies to every lifecycle state, including in-flight and `failed`
    /// messages, matching AWS, where retention trumps visibility.
    ///
    /// An associated function over the pool (rather than `&self`) so the
    /// maintenance task can call it without holding a `Service` clone.
    pub async fn sweep_expired_messages(db: &SqlitePool) -> Result<u64, Error> {
        let swept = Self::sweep_expired_messages_by_queue(db).await?;
        Ok(swept.iter().map(|(_, _, count)| count).sum())
    }

    /// As [`Self::sweep_expired_messages`], returning how many each queue
    /// lost, as (namespace, queue, count), for the removal metric.
    pub async fn sweep_expired_messages_by_queue(
        db: &SqlitePool,
    ) -> Result<Vec<(String, String, u64)>, Error> {
        let queues: Vec<i64> = sqlx::query_scalar(
            "
            DELETE FROM messages WHERE id IN (
                SELECT m.id FROM messages m
                JOIN queue_attributes a
                    ON a.queue = m.queue
                    AND a.k = 'message_retention_period'
                WHERE CAST(a.v AS INTEGER) > 0
                AND m.received_at IS NOT NULL
                AND m.received_at + CAST(a.v AS INTEGER) <= unixepoch('now')
            )
            RETURNING queue
            ",
        )
        .fetch_all(db)
        .await?;
        if queues.is_empty() {
            return Ok(Vec::new());
        }

        let mut counts: HashMap<i64, u64> = HashMap::new();
        for queue in queues {
            *counts.entry(queue).or_default() += 1;
        }
        // Names for the queues the sweep touched, in one read after the
        // delete (never a second connection while one is held).
        let ids = serde_json::to_string(&counts.keys().collect::<Vec<_>>()).map_err(Error::internal)?;
        let names: Vec<(i64, String, String)> = sqlx::query_as(
            "
            SELECT q.id, n.name, q.name FROM queues q
            JOIN namespaces n ON n.id = q.ns
            WHERE q.id IN (SELECT value FROM json_each($1))
            ",
        )
        .bind(ids)
        .fetch_all(db)
        .await?;

        Ok(names
            .into_iter()
            .map(|(id, namespace, queue)| (namespace, queue, counts[&id]))
            .collect())
    }

    /// Every queue's messages by state, for the queue gauges: one pass,
    /// reading no bodies. Each message is in exactly one state, unlike the
    /// buckets of `queue_statistics`, which miss delayed messages.
    pub async fn queue_gauges(&self) -> Result<Vec<crate::telemetry::QueueGauge>, Error> {
        Ok(sqlx::query_as(
            "
            SELECT
                n.name AS namespace,
                q.name AS queue,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                    AND m.tries < conf.max_retries THEN 1 END) AS available,
                COUNT(CASE WHEN m.invisible_until > unixepoch('now')
                    AND m.delivered_at IS NOT NULL THEN 1 END) AS in_flight,
                COUNT(CASE WHEN m.invisible_until > unixepoch('now')
                    AND m.delivered_at IS NULL THEN 1 END) AS delayed,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                    AND m.tries >= conf.max_retries THEN 1 END) AS failed,
                MIN(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                    AND m.tries < conf.max_retries THEN m.received_at END) AS oldest_available_at,
                q.paused_at IS NOT NULL AS paused
            FROM queues q
            JOIN namespaces n ON n.id = q.ns
            JOIN queue_configurations conf ON conf.queue = q.id
            LEFT JOIN messages m ON m.queue = q.id
            GROUP BY q.id
            ORDER BY n.name, q.name
            ",
        )
        .fetch_all(self.db())
        .await?)
    }

    /// Refreshes the queue gauges (`crate::telemetry`) every `every`, for
    /// as long as the server runs. Only worth running when metrics are
    /// exported.
    pub fn spawn_queue_gauges(&self, every: std::time::Duration) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            loop {
                interval.tick().await;
                let started = std::time::Instant::now();
                match service.queue_gauges().await {
                    Ok(queues) => {
                        let count = queues.len();
                        service.telemetry.set_queue_gauges(queues);
                        let took = started.elapsed();
                        if took > std::time::Duration::from_secs(1) {
                            tracing::warn!(?took, queues = count, "refreshing the queue gauges is slow");
                        } else {
                            tracing::debug!(?took, queues = count, "refreshed the queue gauges");
                        }
                    }
                    Err(e) => tracing::warn!("refreshing the queue gauges failed: {e}"),
                }
            }
        });
    }

    /// Resolves the SigV4 signing material for an access key id: the
    /// decrypted signing secret, the key's namespace and its owning user.
    ///
    /// Cached in memory for [`Self::SIGNING_KEY_TTL`]: this runs on every
    /// SQS request, and resolving from scratch costs two database reads
    /// plus a KMS decrypt. Entries are invalidated eagerly when a key is
    /// deleted ([`Self::delete_token`]) and wholesale on user/namespace
    /// deletion (which cascade keys); the TTL bounds staleness for any
    /// path that slips past those hooks.
    ///
    /// Returns `None` for an unknown key id (not negatively cached).
    pub async fn signing_key(&self, key_id: &str) -> Result<Option<CachedSigningKey>, Error> {
        if let Some(hit) = self
            .signing_keys
            .read()
            .expect("signing key cache poisoned")
            .get(key_id)
        {
            if hit.cached_at.elapsed() < Self::SIGNING_KEY_TTL {
                return Ok(Some(hit.clone()));
            }
        }

        let Some((id, email, role, kms_key_id, encrypted_key, namespace, access)) =
            sqlx::query_as::<_, (i64, String, Role, String, Vec<u8>, String, KeyAccess)>(
                "
                SELECT u.id, u.email, u.role, u.kms_key_id, k.encrypted_key, ns.name, k.access
                FROM api_keys k
                JOIN users u ON u.id = k.user
                JOIN namespaces ns ON ns.id = k.ns
                WHERE k.key_id = $1 AND u.disabled_at IS NULL
                ",
            )
            .bind(key_id)
            .fetch_optional(self.db())
            .await?
        else {
            return Ok(None);
        };

        let secret = String::from_utf8(self.kms.decrypt(&kms_key_id, encrypted_key).await?)
            .map_err(Error::internal)?;

        let entry = CachedSigningKey {
            secret: secret.into(),
            namespace,
            access,
            user: crate::api::auth::User {
                id: id as u64,
                email,
                role,
            },
            cached_at: std::time::Instant::now(),
        };

        self.signing_keys
            .write()
            .expect("signing key cache poisoned")
            .insert(key_id.to_owned(), entry.clone());

        Ok(Some(entry))
    }

    /// How long a resolved signing key may be served from cache.
    const SIGNING_KEY_TTL: std::time::Duration = std::time::Duration::from_secs(60);

    /// Drops one access key from the signing-key cache.
    pub fn invalidate_signing_key(&self, key_id: &str) {
        self.signing_keys
            .write()
            .expect("signing key cache poisoned")
            .remove(key_id);
    }

    /// Drops every cached signing key. Used by coarse-grained deletions
    /// (user, namespace) whose cascades remove an unknown set of keys.
    pub fn clear_signing_keys(&self) {
        self.signing_keys
            .write()
            .expect("signing key cache poisoned")
            .clear();
    }

    /// Deletes an API key owned by the calling user, by key name, and
    /// eagerly drops it from the signing-key cache.
    pub async fn delete_token(&self, name: &str, identity: Identity) -> Result<(), Error> {
        self.delete_user_token(&identity.id()?, name).await
    }

    /// Creates an API token for accessing a namespace, generating its
    /// credentials.
    ///
    /// # Arguments
    /// * `name` - Name of the token
    /// * `namespace` - Namespace to grant access to
    /// * `identity` - Identity of the authenticated user
    pub async fn create_token(
        &self,
        name: String,
        namespace: String,
        identity: Identity,
    ) -> Result<CreateTokenResponse, Error> {
        self.create_token_with(name, namespace, identity, None, None)
            .await
    }

    /// Creates an API token, optionally with credentials the CALLER supplies.
    ///
    /// `credentials = None` generates them, exactly as [`Service::create_token`]
    /// does. Supplying them lets the credentials exist before the key does,
    /// which a generated secret cannot: it is printed once and never recoverable.
    /// A supplied secret is hashed the same way, so the stored key is
    /// indistinguishable from a generated one.
    ///
    /// # Arguments
    /// * `name` - Name of the token
    /// * `namespace` - Namespace to grant access to
    /// * `identity` - Identity of the authenticated user
    /// * `credentials` - Access key and secret to use instead of generating them
    /// * `access` - The most the key may do: at most the caller's own level in
    ///   the namespace (admins any, owners `Owner`, members `Member`). `None`
    ///   gives the caller's own level.
    pub async fn create_token_with(
        &self,
        name: String,
        namespace: String,
        identity: Identity,
        credentials: Option<SuppliedCredentials>,
        access: Option<KeyAccess>,
    ) -> Result<CreateTokenResponse, Error> {
        let GeneratedKey {
            short_token,
            long_token,
            long_token_hash,
        } = match credentials {
            Some(supplied) => {
                supplied.validate()?;
                let SuppliedCredentials {
                    access_key,
                    secret_key,
                } = supplied;
                web::block(move || api_key_from_parts(access_key, secret_key))
                    .await
                    .map_err(Error::internal)?
                    .map_err(Error::internal)?
            }
            None => web::block(generate_api_key)
                .await
                .map_err(Error::internal)?
                .map_err(Error::internal)?,
        };

        // Everything is read and encrypted before the write transaction
        // opens, so the transaction starts with its write (see "Concurrency
        // notes" in docs/architecture/message-lifecycle.md).
        let namespace_id = self
            .get_namespace_id(&namespace, self.db())
            .await
            .map_err(Error::internal)?
            .ok_or_else(|| Error::namespace_not_found(&namespace))?;

        let caller = self
            .check_user_access(&identity, namespace_id, self.db())
            .await?;
        let allowed = if caller.is_admin {
            KeyAccess::Admin
        } else if caller.is_owner {
            KeyAccess::Owner
        } else {
            KeyAccess::Member
        };
        let access = access.unwrap_or(allowed);
        if access > allowed {
            return Err(Error::forbidden(format!(
                "a key's access cannot exceed its owner's: '{}' is the most \
                 available in namespace {namespace}",
                allowed.as_str()
            )));
        }

        let key_id = self.get_key_id(&identity.id()?).await?;

        let encrypted_key = self
            .kms
            .encrypt(&key_id, long_token.as_bytes().to_vec())
            .await?;

        let mut tx = self.db().begin().await?;

        sqlx::query(
            "
            INSERT INTO api_keys (name, user, key_id, hashed_key, encrypted_key, ns, access)
            VALUES ($1, (SELECT id FROM users WHERE email = $2), $3, $4, $5, $6, $7)
            ",
        )
        .bind(&name)
        .bind(identity.id().map_err(ErrorUnauthorized)?)
        .bind(&short_token)
        .bind(long_token_hash.to_string())
        .bind(encrypted_key)
        .bind(namespace_id as i64)
        .bind(access)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if !is_unique_violation(&e) {
                return Error::internal(e);
            }
            // Two unique indexes can refuse the insert. key_id: sigv4 looks
            // keys up by it, so a supplied access key already in use is
            // refused as a bad parameter. (user, name): the caller already
            // has a key by that name — a conflict, which used to be
            // misreported as the access key being in use.
            let on_key_id = e
                .as_database_error()
                .is_some_and(|d| d.message().contains("api_keys.key_id"));
            if on_key_id {
                Error::invalid_parameter(format!(
                    "access key '{short_token}' is already in use"
                ))
            } else {
                Error::conflict(format!("you already have an API key named '{name}'"))
            }
        })?;

        tx.commit().await?;

        // Return the plain API key (should be securely sent/stored by the user).
        Ok(CreateTokenResponse {
            name,
            namespace,
            access,
            access_key: short_token,
            secret_key: long_token,
        })
    }

    /// Creates a new user account.
    ///
    /// # Arguments
    /// * `email` - User's email address
    /// * `password` - User's password
    /// * `role` - Optional role to assign
    /// * `namespaces` - Namespaces to grant access to
    pub async fn create_user(
        &self,
        email: Email,
        password: String,
        role: Option<Role>,
        namespaces: Vec<String>,
    ) -> Result<(), Error> {
        // Checked first, for a 404 rather than the NOT NULL failure the
        // permission insert would hit — and before any KMS key is made.
        for namespace in &namespaces {
            if self.get_namespace_id(namespace, self.db()).await?.is_none() {
                return Err(Error::namespace_not_found(namespace));
            }
        }

        let hashed_password = web::block(move || hash_secret(password))
            .await
            .map_err(|e| Error::internal(e))??;

        // Created before the transaction opens: a key manager on the same
        // pool would otherwise take a second connection while this one is
        // held (see "Concurrency notes" in docs/architecture/message-lifecycle.md).
        let key_id = self.kms.create_key().await?;

        let created = async {
            let mut tx = self.db().begin().await?;

            let user_id: u64 = sqlx::query_scalar(
                "
                INSERT INTO users (email, hashed_pass, role, kms_key_id)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            ",
            )
            .bind(email.as_str())
            .bind(hashed_password.to_string())
            .bind(role.unwrap_or(Role::User))
            .bind(&key_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    Error::conflict(format!("user {email} already exists"))
                } else {
                    e.into()
                }
            })?;

            for namespace in namespaces {
                sqlx::query(
                    "
                    INSERT INTO user_permissions (user, namespace, is_owner)
                    VALUES ($1, (SELECT id FROM namespaces WHERE name = $2), false)
                ",
                )
                .bind(user_id as i64)
                .bind(namespace)
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
            Ok::<_, Error>(())
        }
        .await;

        // No user, no use for the key: don't leave it orphaned (e.g. when the
        // email is already taken).
        if created.is_err() {
            if let Err(e) = self.kms.delete_key(&key_id).await {
                tracing::warn!("failed to delete the unused KMS key {key_id}: {e}");
            }
        }

        created
    }

    /// Replaces a user's password with a freshly hashed one.
    ///
    /// Returns `true` if a user with that email existed and was updated,
    /// `false` if there was no such user. Other account fields (role,
    /// namespaces, KMS key) are left untouched.
    ///
    /// # Arguments
    /// * `email` - Email address identifying the user
    /// * `password` - The new plaintext password
    pub async fn set_user_password(&self, email: Email, password: String) -> Result<bool, Error> {
        let hashed_password = web::block(move || hash_secret(password))
            .await
            .map_err(|e| Error::internal(e))??;

        let result = sqlx::query(
            "
            UPDATE users
            SET hashed_pass = $2
            WHERE email = $1
        ",
        )
        .bind(email.as_str())
        .bind(hashed_password.to_string())
        .execute(self.db())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Sends a single message to a queue.
    ///
    /// `sent_by` is the id of the authenticated sending user (the API key's
    /// owner for SQS sends, the session user for admin-panel sends); it is
    /// surfaced to consumers as the SenderId system attribute.
    /// `trace_header` is the request's `X-Amzn-Trace-Id`: stored as the
    /// message's `AWSTraceHeader` when the message doesn't set one, as AWS
    /// does.
    pub async fn sqs_send(
        &self,
        queue: u64,
        req: SendMessageRequest,
        sent_by: Option<u64>,
        trace_header: Option<&str>,
    ) -> Result<SendMessageResponse, Error> {
        let mut tx = self.db().begin().await?;

        let res = self
            .sqs_send_internal(queue, req, sent_by, trace_header, &mut tx)
            .await?;

        tx.commit().await?;

        Ok(res)
    }

    async fn sqs_send_internal(
        &self,
        queue: u64,
        mut req: SendMessageRequest,
        sent_by: Option<u64>,
        trace_header: Option<&str>,
        exec: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<SendMessageResponse, Error> {
        // Checked before any database work, so a batch entry that fails
        // leaves nothing behind in the batch's transaction.
        crate::sqs::limits::check_message(&req.message_body, &req.message_attributes)?;
        req.message_attributes = std::mem::take(&mut req.message_attributes)
            .into_iter()
            .map(|(name, attribute)| (name, attribute.normalized()))
            .collect();

        // The message's own header wins
        // over the request's; with neither, and traces exported, the header
        // names the context the message was created in.
        let trace_header = match crate::sqs::types::trace_header(&req.message_system_attributes)
            .map_err(Error::invalid_parameter)?
            .or(trace_header)
        {
            Some(header) => Some(header.to_owned()),
            None => crate::telemetry::derived_trace_header(crate::sqs::types::string_attribute(
                &req.message_attributes,
                "traceparent",
            )),
        };

        if let Some(delay) = req.delay_seconds {
            crate::sqs::limits::check_range(
                "DelaySeconds",
                delay,
                &crate::sqs::limits::DELAY_SECONDS,
                "seconds",
            )
            .map_err(Error::invalid_parameter)?;
        }

        let mut tx = exec.acquire().await?;

        let size = crate::sqs::types::message_size(&req.message_body, &req.message_attributes);

        // A delivery delay hides the message exactly like a visibility window
        // does, without counting as a delivery attempt. The effective delay
        // is the request value, else the queue's `delay_seconds` attribute,
        // else none.
        //
        // The size limit — the queue's MaximumMessageSize attribute, capped
        // by and defaulting to the AWS maximum of 1 MiB — is enforced by the
        // WHERE clause rather than a preceding SELECT: the first statement on
        // this transaction must be a write, or a concurrent sender's read
        // snapshot would fail to upgrade (SQLITE_BUSY_SNAPSHOT).
        //
        // The client's MessageId is a fresh v4 UUID, as on AWS, not the row
        // id: SQLite hands a deleted row's id out again (migration 0016).
        let message_id = uuid::Uuid::new_v4().to_string();
        let msg_id: Option<u64> = sqlx::query_scalar(
            "
            INSERT INTO messages
                (queue, message_id, body, received_at, sent_at_ms, sent_by, aws_trace_header,
                 invisible_until)
            SELECT $1, $8, $2, unixepoch('now'), CAST(unixepoch('subsec') * 1000 AS INTEGER), $6, $7,
                CASE
                    WHEN COALESCE(
                        $3,
                        (SELECT CAST(v AS INTEGER) FROM queue_attributes
                         WHERE queue = $1 AND k = 'delay_seconds'),
                        0
                    ) > 0
                    THEN unixepoch('now') + COALESCE(
                        $3,
                        (SELECT CAST(v AS INTEGER) FROM queue_attributes
                         WHERE queue = $1 AND k = 'delay_seconds')
                    )
                    ELSE NULL
                END
            WHERE $4 <= COALESCE(
                (SELECT MIN(CAST(v AS INTEGER), $5) FROM queue_attributes
                 WHERE queue = $1 AND k = 'max_message_size'),
                $5
            )
            RETURNING id
            ",
        )
        .bind(queue as i64)
        .bind(&req.message_body)
        .bind(req.delay_seconds.map(|d| d as i64))
        .bind(size as i64)
        .bind(crate::sqs::types::MAX_MESSAGE_SIZE_BYTES as i64)
        .bind(sent_by.map(|id| id as i64))
        .bind(trace_header.as_deref())
        .bind(&message_id)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(msg_id) = msg_id else {
            // Zero rows means the WHERE clause rejected the size. Look the
            // effective limit up for the error message (rare path).
            let limit: usize = sqlx::query_scalar::<_, Option<i64>>(
                "SELECT CAST(v AS INTEGER) FROM queue_attributes
                 WHERE queue = $1 AND k = 'max_message_size'",
            )
            .bind(queue as i64)
            .fetch_optional(&mut *tx)
            .await?
            .flatten()
            .map(|v| (v as usize).min(crate::sqs::types::MAX_MESSAGE_SIZE_BYTES))
            .unwrap_or(crate::sqs::types::MAX_MESSAGE_SIZE_BYTES);

            return Err(Error::invalid_parameter(format!(
                "MessageBody: the message is {size} bytes (body plus attributes); \
                 this queue accepts at most {limit} bytes"
            )));
        };

        for (k, v) in &req.message_attributes {
            sqlx::query("INSERT INTO kv_pairs (message, k, v) VALUES ($1, $2, $3)")
                .bind(msg_id as i64)
                .bind(k)
                .bind(serde_json::to_vec(v).map_err(Error::internal)?)
                .execute(&mut *tx)
                .await?;
        }

        Ok(SendMessageResponse {
            message_id,
            md5_of_message_body: hex::encode(md5::compute(&req.message_body).as_ref()),
            md5_of_message_attributes: crate::sqs::types::attributes_md5(&req.message_attributes),
            md5_of_message_system_attributes: crate::sqs::types::attributes_md5(
                &req.message_system_attributes,
            ),
        })
    }

    /// Sends multiple messages to a queue in one operation.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `messages` - Vector of (message body, attributes) pairs
    #[allow(unused)]
    pub async fn sqs_send_batch(
        &self,
        namespace_name: &str,
        queue_name: &str,
        req: SendMessageBatchRequest,
        sent_by: Option<u64>,
        trace_header: Option<&str>,
    ) -> Result<SendMessageBatchResponse, Error> {
        // Resolve the queue on the pool, NOT inside the write transaction: a
        // deferred transaction whose first statement is a read takes a
        // snapshot that fails to upgrade (SQLITE_BUSY_SNAPSHOT) when any
        // other writer commits before our first INSERT. Under concurrent
        // load that surfaced as every entry in a batch failing with a 500.
        // The transaction below must start with a write, like `sqs_send`.
        let queue_id = self
            .get_queue_id(namespace_name, queue_name, self.db())
            .await?
            .ok_or_else(|| Error::queue_not_found(queue_name, namespace_name))?;

        // The total batch payload — the sum of the individual lengths of all
        // batched messages — shares the 1 MiB maximum. Exceeding it fails
        // the whole request (AWS: BatchRequestTooLong), not just one entry.
        let total_payload: usize = req
            .entries
            .iter()
            .map(|entry| {
                crate::sqs::types::message_size(&entry.message_body, &entry.message_attributes)
            })
            .sum();
        if total_payload > crate::sqs::types::MAX_MESSAGE_SIZE_BYTES {
            // AWS's wording.
            return Err(Error::invalid_batch(
                crate::error::BatchFault::TooLong,
                format!(
                    "Batch requests cannot be longer than {} bytes. You have sent \
                     {total_payload} bytes.",
                    crate::sqs::types::MAX_MESSAGE_SIZE_BYTES
                ),
            ));
        }

        let mut tx = self.db().begin().await?;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in req.entries {
            let message_attributes = entry.message_attributes;
            let message_body = entry.message_body;

            match self
                .sqs_send_internal(
                    queue_id,
                    SendMessageRequest {
                        queue_url: req.queue_url.clone(),
                        message_body,
                        delay_seconds: entry.delay_seconds,
                        message_attributes,
                        message_system_attributes: entry.message_system_attributes,
                        message_deduplication_id: entry.message_deduplication_id,
                        message_group_id: entry.message_group_id,
                    },
                    sent_by,
                    trace_header,
                    &mut *tx,
                )
                .await
            {
                Ok(res) => {
                    successful.push(SendMessageBatchResultEntry {
                        id: entry.id,
                        message_id: res.message_id,
                        md5_of_message_body: res.md5_of_message_body,
                        md5_of_message_attributes: res.md5_of_message_attributes,
                        md5_of_message_system_attributes: res.md5_of_message_system_attributes,
                    });
                }
                Err(e) => {
                    failed.push(SendMessageBatchResultErrorEntry {
                        id: entry.id,
                        sender_fault: crate::sqs::error::is_sender_fault(&e),
                        code: crate::sqs::error::aws_error_code(&e).code.to_string(),
                        message: Some(e.to_string()),
                    });
                }
            }
        }

        tx.commit().await?;

        Ok(SendMessageBatchResponse { successful, failed })
    }

    /// Fetches the raw message attributes for a set of messages in one query
    /// per 500-id chunk, instead of one query per message (N+1). Runs on the
    /// caller's connection — see the pool-deadlock note in `sqs_recv_batch`.
    /// Inner maps are ordered by key, which the attribute-digest computation
    /// relies on.
    async fn message_attributes_for(
        &self,
        ids: impl Iterator<Item = u64>,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<HashMap<u64, BTreeMap<String, Vec<u8>>>, Error> {
        /// Stays far below SQLite's bound-parameter limit.
        const CHUNK: usize = 500;

        let ids: Vec<u64> = ids.collect();
        let mut by_message: HashMap<u64, BTreeMap<String, Vec<u8>>> = HashMap::new();

        for chunk in ids.chunks(CHUNK) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let sql =
                format!("SELECT message, k, v FROM kv_pairs WHERE message IN ({placeholders})");

            let mut query = sqlx::query_as::<_, (i64, String, Vec<u8>)>(&sql);
            for id in chunk {
                query = query.bind(*id as i64);
            }

            for (message, k, v) in query.fetch_all(&mut *conn).await? {
                by_message.entry(message as u64).or_default().insert(k, v);
            }
        }

        Ok(by_message)
    }

    /// Builds the AWS system-attribute map (the `Attributes` field of a
    /// received message) for the names requested via `AttributeNames` /
    /// `MessageSystemAttributeNames`. Timestamps are epoch **milliseconds**
    /// and every value travels as a string, AWS-style.
    ///
    /// Runs its lookups on the claim transaction's connection — see the
    /// deadlock note in `sqs_recv_batch`.
    async fn system_attributes(
        &self,
        message: &Message,
        names: &HashSet<String>,
        conn: &mut sqlx::SqliteConnection,
    ) -> Result<HashMap<String, String>, Error> {
        // `All` (and AWS's legacy `.*`) requests every system attribute.
        let want =
            |name: &str| names.contains("All") || names.contains(".*") || names.contains(name);

        let mut attributes = HashMap::new();

        if want("SentTimestamp") {
            if let Some(sent_at_ms) = message.sent_at_ms() {
                attributes.insert("SentTimestamp".to_owned(), sent_at_ms.to_string());
            }
        }
        if want("ApproximateReceiveCount") {
            attributes.insert(
                "ApproximateReceiveCount".to_owned(),
                message.tries.to_string(),
            );
        }
        if want("ApproximateFirstReceiveTimestamp") {
            if let Some(first) = message.first_delivered_at_ms() {
                attributes.insert("ApproximateFirstReceiveTimestamp".to_owned(), first.to_string());
            }
        }
        if want(crate::sqs::types::AWS_TRACE_HEADER) {
            if let Some(header) = &message.aws_trace_header {
                attributes.insert(crate::sqs::types::AWS_TRACE_HEADER.to_owned(), header.clone());
            }
        }
        if want("SenderId") {
            if let Some(sent_by) = message.sent_by {
                // NerveMQ's principal identifier is the sending user's email
                // (AWS returns the opaque IAM principal id here).
                let email: Option<String> =
                    sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
                        .bind(sent_by as i64)
                        .fetch_optional(&mut *conn)
                        .await?;
                if let Some(email) = email {
                    attributes.insert("SenderId".to_owned(), email);
                }
            }
        }

        Ok(attributes)
    }

    /// Receives multiple messages from a queue in one operation.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `max_messages` - Maximum number of messages to receive
    pub async fn sqs_recv_batch(
        &self,
        namespace: &str,
        queue: &str,
        max_messages: u64,
        visibility_timeout: Option<u64>,
        attribute_names: HashSet<String>,
        system_attribute_names: HashSet<String>,
    ) -> Result<Vec<SqsMessage>, Error> {
        // The `visibility_timeout` override is range-checked by the
        // ReceiveMessage handler, once, before any database work.
        let mut tx = self.db().begin().await?;

        // Atomically claim up to `max_messages` available messages: those whose
        // visibility window has elapsed (or were never received) and which still
        // have retries remaining. Claiming stamps `invisible_until`, bumps the
        // delivery counter, and mints a fresh receipt handle.
        //
        // The effective visibility timeout is the request override, else the
        // queue's `visibility_timeout` attribute, else the global default.
        let claimed: Vec<Message> = sqlx::query_as::<_, Message>(
            "
            WITH next_messages AS (
                SELECT
                    m.id
                FROM messages m
                JOIN queues q ON m.queue = q.id
                JOIN queue_configurations conf ON q.id = conf.queue
                JOIN namespaces n ON q.ns = n.id
                WHERE n.name = $1
                AND q.name = $2
                -- A paused queue hands out nothing. Checked in the claim
                -- itself, so no receive that commits after the pause can
                -- return a message.
                AND q.paused_at IS NULL
                AND (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                AND m.tries < conf.max_retries
                ORDER BY m.id ASC
                LIMIT $3
            )
            UPDATE messages
            SET delivered_at = unixepoch('now'),
                first_delivered_at = COALESCE(first_delivered_at, unixepoch('now')),
                first_delivered_at_ms = COALESCE(
                    first_delivered_at_ms,
                    CAST(unixepoch('subsec') * 1000 AS INTEGER)
                ),
                tries = tries + 1,
                invisible_until = unixepoch('now') + COALESCE(
                    $4,
                    (SELECT CAST(v AS INTEGER) FROM queue_attributes qa
                     WHERE qa.queue = messages.queue AND qa.k = 'visibility_timeout'),
                    $5
                ),
                receipt_handle = messages.id || ':' || lower(hex(randomblob(16)))
            WHERE id IN (SELECT id FROM next_messages)
            RETURNING
                *,
                (SELECT q.name FROM queues q WHERE q.id = messages.queue) as queue,
                (CASE
                    WHEN messages.delivered_at IS NOT NULL AND messages.invisible_until IS NOT NULL AND messages.invisible_until > unixepoch('now') THEN 'delivered'
                    WHEN messages.tries >= (SELECT max_retries FROM queue_configurations WHERE queue = messages.queue) THEN 'failed'
                    ELSE 'pending'
                END) as status
            ",
        )
        .bind(namespace)
        .bind(queue)
        .bind(max_messages as i64)
        .bind(visibility_timeout.map(|v| v as i64))
        .bind(crate::config::defaults::VISIBILITY_TIMEOUT as i64)
        .fetch_all(&mut *tx)
        .await?;

        // One query for every claimed message's attributes, not one per
        // message. IMPORTANT: this lookup must run on the claim transaction,
        // not on `self.db()`. Acquiring a second pool connection while the
        // write transaction is held deadlocks under concurrent receives:
        // every pool slot is occupied by a transaction waiting for the write
        // lock, while the lock holder waits for a free slot — until a busy
        // timeout kills one of the waiters.
        let mut kv_by_message = self
            .message_attributes_for(claimed.iter().map(|m| m.id), &mut tx)
            .await?;

        let mut messages = vec![];
        let mut delivered = Vec::with_capacity(claimed.len());
        for message in claimed {
            let kv = kv_by_message.remove(&message.id).unwrap_or_default();
            delivered.push(crate::telemetry::MessageFacts {
                id: message.message_id.clone(),
                tries: message.tries,
                sent_at_ms: message.sent_at_ms(),
                trace_header: message.aws_trace_header.clone(),
                traceparent: kv
                    .get("traceparent")
                    .and_then(|v| serde_json::from_slice::<SqsMessageAttribute>(v).ok())
                    .and_then(|attribute| attribute.as_string().map(str::to_owned)),
            });

            let mut message_attributes = HashMap::new();
            for (k, v) in kv.into_iter().filter(|(k, _)| {
                crate::sqs::types::message_attribute_wanted(&attribute_names, k)
            }) {
                let v: SqsMessageAttribute = serde_json::from_slice(&v).map_err(Error::internal)?;
                message_attributes.insert(k, v);
            }

            let attributes = self
                .system_attributes(&message, &system_attribute_names, &mut tx)
                .await?;

            let sqs_message = SqsMessage {
                message_id: message.message_id.clone(),

                receipt_handle: message.receipt_handle.clone().unwrap_or_default(),

                md5_of_body: hex::encode(md5::compute(&message.body.as_bytes()).as_slice()),
                body: message.body,

                md5_of_message_attributes: crate::sqs::types::attributes_md5(&message_attributes),
                message_attributes,
                attributes,
            };
            messages.push(sqs_message);
        }

        tx.commit().await?;

        self.telemetry.delivered(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            &delivered,
        );
        Ok(messages)
    }

    /// Lists one page of a queue's messages, in id (send) order, along with
    /// the total message count for pagination controls.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `limit` - Page size
    /// * `offset` - Rows to skip
    pub async fn list_messages(
        &self,
        namespace: &str,
        queue: &str,
        limit: u64,
        offset: u64,
        sort: MessageSortKey,
        order: SortOrder,
    ) -> Result<MessageList, Error> {
        let mut db = self.db().acquire().await?;

        let total: u64 = sqlx::query_scalar::<_, i64>(
            "
            SELECT COUNT(*)
            FROM messages m
            JOIN queues q ON m.queue = q.id
            WHERE q.ns = (SELECT id FROM namespaces WHERE name = $1) AND q.name = $2
        ",
        )
        .bind(namespace)
        .bind(queue)
        .fetch_one(&mut *db)
        .await? as u64;

        // Everything runs on this single connection. Holding it while
        // acquiring further pool connections (the previous implementation
        // spawned a task per message, each taking its own connection) is the
        // pool-deadlock hazard documented in `sqs_recv_batch`: concurrent
        // listers hold every slot for their streams while their per-message
        // lookups wait for a free slot, until PoolTimedOut fails them all.
        // The ORDER BY columns come from fixed whitelists (`MessageSortKey`
        // / `SortOrder`), never from user input; `m.id` breaks ties so pages
        // remain stable when the sort key has duplicates.
        let messages = sqlx::query_as::<_, Message>(&format!(
            "
            SELECT
                m.*,
                q.name as queue,
                (CASE
                    WHEN m.delivered_at IS NOT NULL AND m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 'delivered'
                    WHEN m.tries >= conf.max_retries THEN 'failed'
                    ELSE 'pending'
                END) as status
            FROM messages m
            JOIN queues q ON m.queue = q.id
            JOIN queue_configurations conf ON q.id = conf.queue
            WHERE q.ns = (SELECT id FROM namespaces WHERE name = $1) AND q.name = $2
            ORDER BY {sort_sql} {order_sql}, m.id ASC
            LIMIT $3 OFFSET $4
        ",
            sort_sql = sort.sql(),
            order_sql = order.sql(),
        ))
        .bind(namespace)
        .bind(queue)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&mut *db)
        .await?;

        // One query for the whole page's attributes, not one per message.
        let mut kv_by_message = self
            .message_attributes_for(messages.iter().map(|m| m.id), &mut db)
            .await?;

        let mut out = Vec::with_capacity(messages.len());
        for message in messages {
            let kv = kv_by_message.remove(&message.id).unwrap_or_default();

            let mut message_attributes = HashMap::new();
            for (k, v) in kv {
                let attr = match serde_json::from_slice(&v) {
                    Ok(attr) => attr,
                    Err(e) => {
                        tracing::warn!(
                            attribute = k,
                            message = %message.message_id,
                            "Failed to deserialize message attribute: {e}",
                        );

                        continue;
                    }
                };
                let attr: SqsMessageAttribute = attr;
                let text = attr.string_value.clone().unwrap_or_default();
                let value = match attr.kind() {
                    Some(crate::sqs::types::AttributeKind::Binary) => serde_json::Value::String(
                        base64::prelude::BASE64_STANDARD.encode(attr.binary_value.unwrap_or_default()),
                    ),
                    // As a JSON number only when that is exactly the value
                    // sent, else as its text. AWS's numbers aren't all JSON's
                    // ("007" used to fail the whole listing), and JSON's are
                    // f64 here, which would round a 38-digit value.
                    Some(crate::sqs::types::AttributeKind::Number) => text
                        .parse::<serde_json::Number>()
                        .ok()
                        .filter(|number| number.to_string() == text)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::String(text)),
                    _ => serde_json::Value::String(text),
                };
                message_attributes.insert(k, value);
            }

            out.push(MessageDetails {
                id: message.message_id,
                queue: message.queue,
                status: message.status,
                sent_by: message.sent_by,
                received_at: message.received_at,
                delivered_at: message.delivered_at,
                tries: message.tries,
                body: message.body,

                message_attributes,
            });
        }

        Ok(MessageList {
            messages: out,
            total,
        })
    }

    /// Gets the configuration for a queue.
    ///
    /// # Arguments
    /// * `queue` - Queue ID
    pub async fn get_queue_configuration(&self, queue: u64) -> Result<QueueConfig, Error> {
        let mut db = self.db().acquire().await?;
        Ok(sqlx::query_as(
            "
            SELECT * FROM queue_configurations WHERE queue = $1
            ",
        )
        .bind(queue as i64)
        .fetch_one(&mut *db)
        .await?)
    }

    /// Updates the configuration for a queue.
    ///
    /// # Arguments
    /// * `queue` - Queue ID
    /// * `new_config` - New configuration settings
    pub async fn update_queue_configuration(
        &self,
        queue: u64,
        new_config: QueueConfig,
    ) -> Result<(), Error> {
        let mut db = self.db().acquire().await?;

        sqlx::query(
            "
            UPDATE queue_configurations
            SET max_retries = $1, dead_letter_queue = $2
            WHERE queue = $3
            ",
        )
        .bind(new_config.max_retries as i64)
        .bind(new_config.dead_letter_queue.map(|id| id as i64))
        .bind(queue as i64)
        .execute(&mut *db)
        .await?;

        Ok(())
    }

    /// Gets statistics for a specific queue.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    pub async fn queue_statistics(
        &self,
        identity: Identity,
        namespace: &str,
        queue: &str,
    ) -> Result<QueueStatistics, Error> {
        let ns_id = self
            .get_namespace_id(namespace, self.db())
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;
        self.check_user_access(&identity, ns_id, self.db()).await?;

        let mut db = self.db().acquire().await?;

        sqlx::query_as(
            "
            SELECT
                q.id,
                q.name,
                qu.email as created_by,
                n.name as ns,
                q.paused_at,
                COUNT(m.id) AS message_count,
                IFNULL(AVG(LENGTH(m.body)), 0.0) as avg_size_bytes,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries < conf.max_retries THEN 1 END) as pending,
                COUNT(CASE WHEN m.delivered_at IS NOT NULL AND m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 1 END) as delivered,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries >= conf.max_retries THEN 1 END) as failed
            FROM queues q
            JOIN queue_configurations conf ON q.id = conf.queue
            JOIN namespaces n ON n.id = q.ns
            LEFT JOIN messages m ON q.id = m.queue
            LEFT JOIN users qu ON q.created_by = qu.id
            WHERE n.id = $1 AND q.name = $2
            GROUP BY q.id
        ",
        )
        .bind(ns_id as i64)
        .bind(queue)
        .fetch_optional(&mut *db)
        .await?
        .ok_or_else(|| Error::queue_not_found(queue, namespace))
    }

    /// Gets statistics for all queues accessible to the user, keyed by
    /// `namespace/queue`.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn global_queue_statistics(
        &self,
        identity: Identity,
    ) -> Result<HashMap<String, QueueStatistics>, Error> {
        let mut db = self.db().acquire().await?;
        let email = identity.id()?;

        let res = sqlx::query_as(
            "
            SELECT
                q.id,
                q.name,
                qu.email as created_by,
                n.name as ns,
                q.paused_at,
                COUNT(m.id) AS message_count,
                IFNULL(AVG(LENGTH(m.body)), 0.0) as avg_size_bytes,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries < conf.max_retries THEN 1 END) as pending,
                COUNT(CASE WHEN m.delivered_at IS NOT NULL AND m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 1 END) as delivered,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries >= conf.max_retries THEN 1 END) as failed
            FROM users u
            JOIN queues q
            JOIN queue_configurations conf ON q.id = conf.queue
            JOIN namespaces n ON n.id = q.ns
            LEFT JOIN messages m ON q.id = m.queue
            LEFT JOIN users qu ON q.created_by = qu.id
            WHERE u.email = $1 AND (
                u.role = 'admin'
                OR EXISTS (
                    SELECT 1 FROM user_permissions p
                    WHERE p.user = u.id AND p.namespace = q.ns
                )
            )
            GROUP BY q.id
        ",
        )
        .bind(email)
        .fetch_all(&mut *db)
        .await?
        .into_iter()
        // Keyed by namespace too: queue names are only unique within one, so
        // keying by name alone dropped all but one of `a/jobs` and `b/jobs`.
        .map(|row: QueueStatistics| (format!("{}/{}", row.queue.ns, row.queue.name), row))
        .collect::<HashMap<_, _>>();

        Ok(res)
    }

    /// Deletes multiple messages from a queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `message_ids` - IDs of messages to delete
    /// * `identity` - Identity of the authenticated user
    ///
    /// # Returns
    /// Tuple of (successfully deleted IDs, failed deletions with errors)
    #[allow(unused)]
    /// Deletes a batch of messages by receipt handle, mirroring AWS
    /// `DeleteMessageBatch`: each entry succeeds or fails independently and
    /// the same stale-handle rule as `delete_message` applies per entry.
    /// Entries are `(entry id, receipt handle)`; the returned vectors carry
    /// the entry ids back for correlation.
    pub async fn delete_message_batch(
        &self,
        namespace: &str,
        queue: &str,
        entries: Vec<(String, String)>,
        identity: Identity,
    ) -> Result<
        (
            Vec<String>,          // Entry IDs deleted successfully
            Vec<(String, Error)>, // Entry IDs that failed, with the cause
        ),
        Error,
    > {
        let queue_id = self
            .resolve_authorized_queue(namespace, queue, &identity)
            .await?
            .queue_id;

        if entries.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        // One set-based DELETE instead of one statement per entry; RETURNING
        // identifies which handles actually existed. A single statement is
        // atomic on its own, so the per-entry loop's explicit transaction
        // (and its write-first ordering rule) is no longer needed.
        // INDEXED BY: see delete_message.
        let mut builder = sqlx::QueryBuilder::new(
            "DELETE FROM messages INDEXED BY messages_receipt_handle_idx WHERE queue = ",
        );
        builder.push_bind(queue_id as i64);
        builder.push(" AND receipt_handle IN (");
        let mut handles = builder.separated(", ");
        for (_, receipt_handle) in &entries {
            handles.push_bind(receipt_handle);
        }
        handles.push_unseparated(format!(") RETURNING receipt_handle, {MESSAGE_FACTS}"));

        let rows: Vec<HandledMessageRow> = builder.build_query_as().fetch_all(self.db()).await?;
        let deleted: std::collections::HashSet<String> =
            rows.iter().map(|row| row.receipt_handle.clone()).collect();
        let facts: Vec<crate::telemetry::MessageFacts> =
            rows.into_iter().map(|row| row.facts.into()).collect();
        self.telemetry.removed(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            crate::telemetry::Removal::Acknowledged,
            &facts,
        );

        // Correlate per entry, preserving the old loop's duplicate-handle
        // semantics: a deleted handle acknowledges the first entry bearing
        // it; later duplicates fail like any other unknown handle. A
        // malformed handle (which matched nothing) is refused as one.
        let mut spent = std::collections::HashSet::new();
        let mut success = Vec::new();
        let mut failure = Vec::new();
        for (entry_id, receipt_handle) in entries {
            if !crate::sqs::limits::is_receipt_handle(&receipt_handle) {
                let error = crate::sqs::limits::malformed_receipt_handle(&receipt_handle);
                failure.push((entry_id, error));
            } else if deleted.contains(&receipt_handle) && spent.insert(receipt_handle) {
                success.push(entry_id);
            } else {
                failure.push((
                    entry_id,
                    Error::invalid_receipt_handle(format!(
                        "receipt handle invalid or expired in queue {queue}"
                    )),
                ));
            }
        }

        Ok((success, failure))
    }

    /// Reads a single integer-valued queue attribute, if set.
    pub async fn get_queue_attribute_u64(
        &self,
        namespace: &str,
        queue: &str,
        key: &str,
    ) -> Result<Option<u64>, Error> {
        let value: Option<i64> = sqlx::query_scalar(
            "
            SELECT CAST(qa.v AS INTEGER) FROM queue_attributes qa
            JOIN queues q ON qa.queue = q.id
            JOIN namespaces n ON q.ns = n.id
            WHERE n.name = $1 AND q.name = $2 AND qa.k = $3
            ",
        )
        .bind(namespace)
        .bind(queue)
        .bind(key)
        .fetch_optional(self.db())
        .await?;

        Ok(value.map(|v| v as u64))
    }

    /// Deletes a single message by its ID, regardless of in-flight state.
    ///
    /// Management-plane counterpart of SQS `DeleteMessage`, which requires a
    /// receipt handle the admin UI does not hold.
    pub async fn admin_delete_message(
        &self,
        namespace: &str,
        queue: &str,
        message_id: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        self.require_queue_manager(&identity, namespace).await?;
        let queue_id = self
            .get_queue_id(namespace, queue, self.db())
            .await?
            .ok_or_else(|| Error::queue_not_found(queue, namespace))?;

        let deleted: Option<MessageFactsRow> = sqlx::query_as(&format!(
            "DELETE FROM messages WHERE queue = $1 AND message_id = $2 RETURNING {MESSAGE_FACTS}"
        ))
        .bind(queue_id as i64)
        .bind(message_id)
        .fetch_optional(self.db())
        .await?;

        let Some(deleted) = deleted else {
            return Err(Error::not_found(format!(
                "message {message_id} in queue {queue}"
            )));
        };

        self.telemetry.removed(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            crate::telemetry::Removal::Admin,
            &[deleted.into()],
        );
        Ok(())
    }

    /// Deletes every exhausted (`failed`) message in a queue from the
    /// management plane. `failed` is a computed state — delivery attempts
    /// reached the queue's retry limit — so the delete uses the same
    /// predicate the claim queries use to exclude such messages. Returns
    /// the number deleted (zero is not an error: clearing an already-clean
    /// queue is a no-op).
    pub async fn admin_clear_failed_messages(
        &self,
        namespace: &str,
        queue: &str,
        identity: Identity,
    ) -> Result<u64, Error> {
        self.require_queue_manager(&identity, namespace).await?;
        let queue_id = self
            .get_queue_id(namespace, queue, self.db())
            .await?
            .ok_or_else(|| Error::queue_not_found(queue, namespace))?;

        let result = sqlx::query(
            "
            DELETE FROM messages
            WHERE queue = $1
            AND tries >= (SELECT max_retries FROM queue_configurations WHERE queue = $1)
            ",
        )
        .bind(queue_id as i64)
        .execute(self.db())
        .await?;

        self.telemetry.removed_count(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            crate::telemetry::Removal::FailedCleared,
            result.rows_affected(),
        );
        Ok(result.rows_affected())
    }

    /// Forces a message's lifecycle state from the management plane.
    ///
    /// - `pending`: makes the message deliverable again immediately — clears
    ///   the visibility window and resets the delivery counter, whether the
    ///   message is in flight or has exhausted its retries.
    /// - `failed`: stops further deliveries by saturating the delivery
    ///   counter to the queue's retry limit.
    ///
    /// `delivered` is not a settable target: it only ever results from a real
    /// receive minting a receipt handle.
    pub async fn admin_set_message_status(
        &self,
        namespace: &str,
        queue: &str,
        message_id: &str,
        status: MessageStatus,
        identity: Identity,
    ) -> Result<(), Error> {
        self.require_queue_manager(&identity, namespace).await?;
        let queue_id = self
            .get_queue_id(namespace, queue, self.db())
            .await?
            .ok_or_else(|| Error::queue_not_found(queue, namespace))?;

        let result = match status {
            MessageStatus::Pending => {
                sqlx::query(
                    "
                    UPDATE messages
                    SET invisible_until = NULL, tries = 0
                    WHERE queue = $1 AND message_id = $2
                    ",
                )
                .bind(queue_id as i64)
                .bind(message_id)
                .execute(self.db())
                .await?
            }
            MessageStatus::Failed => {
                sqlx::query(
                    "
                    UPDATE messages
                    SET invisible_until = NULL,
                        tries = (SELECT max_retries FROM queue_configurations WHERE queue = $1)
                    WHERE queue = $1 AND message_id = $2
                    ",
                )
                .bind(queue_id as i64)
                .bind(message_id)
                .execute(self.db())
                .await?
            }
            MessageStatus::Delivered => {
                return Err(Error::invalid_parameter(
                    "status: only 'pending' and 'failed' can be set; 'delivered' \
                     results from an actual receive",
                ));
            }
        };

        if result.rows_affected() == 0 {
            return Err(Error::not_found(format!(
                "message {message_id} in queue {queue}"
            )));
        }

        Ok(())
    }

    /// Deletes a single message from a queue, acknowledging its receipt.
    ///
    /// The delete only succeeds if `receipt_handle` matches the handle issued on
    /// the message's most recent receive. A stale handle — e.g. from a message
    /// whose visibility timeout expired and which was redelivered to another
    /// consumer — matches nothing and is reported as not found.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `receipt_handle` - Receipt handle returned by the latest ReceiveMessage
    /// * `identity` - Identity of the authenticated user
    pub async fn delete_message(
        &self,
        namespace: &str,
        queue: &str,
        receipt_handle: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        // Namespace, permission and queue resolved in one read, so a refusal
        // comes before anything about the handle.
        let queue_id = self
            .resolve_authorized_queue(namespace, queue, &identity)
            .await?
            .queue_id;
        if !crate::sqs::limits::is_receipt_handle(receipt_handle) {
            return Err(crate::sqs::limits::malformed_receipt_handle(receipt_handle));
        }

        // Delete the in-flight message identified by this receipt handle. This
        // single statement is atomic on its own; wrapping the preceding reads
        // and the delete in one deferred transaction would make concurrent
        // acknowledgers fail with SQLITE_BUSY_SNAPSHOT (a stale read snapshot
        // upgrading to a write) instead of cleanly losing the race with a
        // zero-row delete.
        // INDEXED BY, here and in the other acknowledgement statements
        // (delete_message_batch, change_message_visibility and its batch):
        // they find messages by `queue = ? AND receipt_handle = ?`, and
        // without ANALYZE statistics, which a server rarely has
        // (`PRAGMA optimize` only runs as a pooled connection closes),
        // SQLite's planner answered that with `messages(queue)`, scanning the
        // whole queue per acknowledgement. Draining slowed in proportion to
        // the backlog: per-message drain fell from 1,817 to 329 msg/s at
        // 20,000 messages (`just bench`). Migration 0008's partial index did
        // not settle it, as the planner only prefers it once statistics
        // exist. Naming the index makes the point lookup unconditional, and
        // fails loudly at prepare time if the index is ever missing.
        let deleted: Option<MessageFactsRow> = sqlx::query_as(&format!(
            "
            DELETE FROM messages INDEXED BY messages_receipt_handle_idx
            WHERE queue = $1 AND receipt_handle = $2
            RETURNING {MESSAGE_FACTS}
            "
        ))
        .bind(queue_id as i64)
        .bind(receipt_handle)
        .fetch_optional(self.db())
        .await?;

        let Some(deleted) = deleted else {
            return Err(Error::invalid_receipt_handle(format!(
                "receipt handle invalid or expired in queue {queue}"
            )));
        };

        self.telemetry.removed(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            crate::telemetry::Removal::Acknowledged,
            &[deleted.into()],
        );
        Ok(())
    }

    /// Changes the visibility timeout of the message a receipt handle names.
    ///
    /// The new timeout is counted from the time of this call, not from when
    /// the message was received — setting it to 0 makes the message
    /// immediately available again. Mirrors AWS SQS `ChangeMessageVisibility`:
    /// the latest handle works even after its window lapsed, hiding the
    /// message again, but not past 12 hours from the receive. A handle the
    /// next receive replaced is refused, where AWS would accept it: NerveMQ
    /// keeps only the latest (see `hide_from_now`).
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `receipt_handle` - Receipt handle from the most recent delivery
    /// * `visibility_timeout` - New timeout in seconds (0 to 43200), from now
    /// * `identity` - Identity of the authenticated user
    pub async fn change_message_visibility(
        &self,
        namespace: &str,
        queue: &str,
        receipt_handle: &str,
        visibility_timeout: u64,
        identity: Identity,
    ) -> Result<(), Error> {
        crate::sqs::limits::check_range(
            "VisibilityTimeout",
            visibility_timeout,
            &crate::sqs::limits::VISIBILITY_TIMEOUT,
            "seconds",
        )
        .map_err(Error::invalid_parameter)?;

        // Namespace, permission and queue resolved in one read, so a refusal
        // comes before anything about the handle.
        let queue_id = self
            .resolve_authorized_queue(namespace, queue, &identity)
            .await?
            .queue_id;
        if !crate::sqs::limits::is_receipt_handle(receipt_handle) {
            return Err(crate::sqs::limits::malformed_receipt_handle(receipt_handle));
        }

        let changed = self
            .hide_from_now(queue_id, receipt_handle, visibility_timeout)
            .await?;
        let Some(changed) = changed else {
            // Why not: the 12-hour cap, or no message the handle can hide.
            let hideable = !self
                .hideable_handles(queue_id, &[receipt_handle])
                .await?
                .is_empty();
            return Err(if hideable {
                crate::sqs::limits::visibility_beyond_limit(visibility_timeout)
            } else {
                crate::sqs::limits::message_not_available(receipt_handle)
            });
        };

        self.telemetry.visibility_changed(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            crate::telemetry::VisibilityChange::of(visibility_timeout),
            &[changed.into()],
        );
        Ok(())
    }

    /// Hides the message `handle` names for `timeout` seconds from now, and
    /// returns its facts, or `None` when the handle can't: it names no
    /// message, or one an admin requeued, or the timeout would keep the
    /// message hidden past 12 hours from its receive.
    ///
    /// The latest handle works even after its window lapsed, as on AWS. A
    /// message an admin requeued (`invisible_until` NULL) is no longer its
    /// old handle's to hide. A single atomic statement for the same reason
    /// as `delete_message`: a read-then-write transaction would fail
    /// concurrent callers with SQLITE_BUSY_SNAPSHOT instead of letting them
    /// lose the race cleanly. INDEXED BY: see delete_message.
    async fn hide_from_now(
        &self,
        queue_id: u64,
        handle: &str,
        timeout: u64,
    ) -> Result<Option<MessageFactsRow>, Error> {
        Ok(sqlx::query_as(&format!(
            "
            UPDATE messages INDEXED BY messages_receipt_handle_idx
            SET invisible_until = unixepoch('now') + $3
            WHERE queue = $1
            AND receipt_handle = $2
            AND invisible_until IS NOT NULL
            AND unixepoch('now') + $3 <= delivered_at + $4
            RETURNING {MESSAGE_FACTS}
            "
        ))
        .bind(queue_id as i64)
        .bind(handle)
        .bind(timeout as i64)
        .bind(crate::sqs::limits::MAX_TOTAL_VISIBILITY as i64)
        .fetch_optional(self.db())
        .await?)
    }

    /// Which of `handles` belong to a message in `queue` that its handle can
    /// still hide: received, and neither deleted, received again since, nor
    /// requeued. Explains, after the fact, why a visibility change matched
    /// nothing.
    async fn hideable_handles(
        &self,
        queue_id: u64,
        handles: &[&str],
    ) -> Result<std::collections::HashSet<String>, Error> {
        if handles.is_empty() {
            return Ok(Default::default());
        }
        let mut builder = sqlx::QueryBuilder::new(
            "SELECT receipt_handle FROM messages INDEXED BY messages_receipt_handle_idx \
             WHERE queue = ",
        );
        builder.push_bind(queue_id as i64);
        builder.push(" AND invisible_until IS NOT NULL AND receipt_handle IN (");
        let mut separated = builder.separated(", ");
        for handle in handles {
            separated.push_bind(*handle);
        }
        builder.push(")");
        Ok(builder
            .build_query_scalar::<String>()
            .fetch_all(self.db())
            .await?
            .into_iter()
            .collect())
    }

    /// Changes the visibility timeout of a batch of messages, mirroring AWS
    /// `ChangeMessageVisibilityBatch`: each entry succeeds or fails
    /// independently under the same rules as `change_message_visibility`.
    /// Entries are `(entry id, receipt handle, visibility timeout)`; AWS
    /// makes an entry's timeout optional, and one without fails on its own.
    /// The returned vectors carry the entry ids back for correlation.
    pub async fn change_message_visibility_batch(
        &self,
        namespace: &str,
        queue: &str,
        entries: Vec<(String, String, Option<u64>)>,
        identity: Identity,
    ) -> Result<
        (
            Vec<String>,          // Entry IDs updated successfully
            Vec<(String, Error)>, // Entry IDs that failed, with the cause
        ),
        Error,
    > {
        let queue_id = self
            .resolve_authorized_queue(namespace, queue, &identity)
            .await?
            .queue_id;

        // A missing or out-of-range timeout, or a malformed handle, fails its
        // entry before any SQL runs.
        let mut valid = Vec::new();
        let mut failure = Vec::new();
        for (entry_id, receipt_handle, visibility_timeout) in entries {
            let Some(visibility_timeout) = visibility_timeout else {
                // AWS's wording for a missing parameter.
                failure.push((
                    entry_id,
                    Error::aws(
                        crate::error::AwsCode::MissingParameter,
                        "The request must contain the parameter VisibilityTimeout.",
                    ),
                ));
                continue;
            };
            if let Err(message) = crate::sqs::limits::check_range(
                "VisibilityTimeout",
                visibility_timeout,
                &crate::sqs::limits::VISIBILITY_TIMEOUT,
                "seconds",
            ) {
                failure.push((entry_id, Error::invalid_parameter(message)));
            } else if !crate::sqs::limits::is_receipt_handle(&receipt_handle) {
                let error = crate::sqs::limits::malformed_receipt_handle(&receipt_handle);
                failure.push((entry_id, error));
            } else {
                valid.push((entry_id, receipt_handle, visibility_timeout));
            }
        }

        // Each entry's outcome: the facts of the message it hid, or `None`.
        let mut outcomes: Vec<Option<MessageFactsRow>> = vec![None; valid.len()];

        // A handle in one entry only: one set-based UPDATE for all of them,
        // carrying each entry's own timeout through a VALUES table (SQLite
        // names its columns column1/column2), instead of one statement per
        // entry. Single statement, so no explicit transaction; the rules of
        // `hide_from_now` apply per row. INDEXED BY: see delete_message.
        let mut uses: HashMap<&str, usize> = HashMap::new();
        for (_, handle, _) in &valid {
            *uses.entry(handle.as_str()).or_default() += 1;
        }
        let single: Vec<usize> = (0..valid.len())
            .filter(|&i| uses[valid[i].1.as_str()] == 1)
            .collect();
        if !single.is_empty() {
            let mut builder = sqlx::QueryBuilder::new(
                "UPDATE messages INDEXED BY messages_receipt_handle_idx \
                 SET invisible_until = unixepoch('now') + e.column2 \
                 FROM (",
            );
            builder.push_values(&single, |mut row, &i| {
                row.push_bind(valid[i].1.clone()).push_bind(valid[i].2 as i64);
            });
            builder.push(
                ") AS e \
                 WHERE messages.queue = ",
            );
            builder.push_bind(queue_id as i64);
            builder.push(
                " AND messages.receipt_handle = e.column1 \
                 AND messages.invisible_until IS NOT NULL \
                 AND unixepoch('now') + e.column2 <= messages.delivered_at + ",
            );
            builder.push_bind(crate::sqs::limits::MAX_TOTAL_VISIBILITY as i64);
            builder.push(" RETURNING receipt_handle, ");
            builder.push(MESSAGE_FACTS);

            let rows: Vec<HandledMessageRow> =
                builder.build_query_as().fetch_all(self.db()).await?;
            let mut by_handle: HashMap<String, MessageFactsRow> = rows
                .into_iter()
                .map(|row| (row.receipt_handle, row.facts))
                .collect();
            for &i in &single {
                outcomes[i] = by_handle.remove(&valid[i].1);
            }
        }

        // A handle in several entries: applied one entry at a time, in order,
        // as AWS applies them, so each entry's own timeout and cap decide its
        // outcome and the last that succeeds is the one that holds. (One
        // UPDATE would apply an arbitrary one of them to the message.)
        for i in (0..valid.len()).filter(|&i| uses[valid[i].1.as_str()] > 1) {
            outcomes[i] = self.hide_from_now(queue_id, &valid[i].1, valid[i].2).await?;
        }

        for change in [
            crate::telemetry::VisibilityChange::Release,
            crate::telemetry::VisibilityChange::Extend,
        ] {
            let facts: Vec<crate::telemetry::MessageFacts> = valid
                .iter()
                .zip(&outcomes)
                .filter(|((_, _, timeout), _)| {
                    crate::telemetry::VisibilityChange::of(*timeout) == change
                })
                .filter_map(|(_, facts)| facts.clone().map(Into::into))
                .collect();
            self.telemetry.visibility_changed(
                crate::telemetry::Queue {
                    namespace,
                    name: queue,
                },
                change,
                &facts,
            );
        }

        // Why the rest matched nothing: the 12-hour cap, or no message the
        // handle can hide. One lookup for all of them.
        let missed: Vec<&str> = valid
            .iter()
            .zip(&outcomes)
            .filter(|(_, outcome)| outcome.is_none())
            .map(|((_, handle, _), _)| handle.as_str())
            .collect();
        let hideable = self.hideable_handles(queue_id, &missed).await?;

        let mut success = Vec::new();
        for ((entry_id, receipt_handle, timeout), outcome) in valid.into_iter().zip(outcomes) {
            if outcome.is_some() {
                success.push(entry_id);
            } else if hideable.contains(&receipt_handle) {
                failure.push((entry_id, crate::sqs::limits::visibility_beyond_limit(timeout)));
            } else {
                let error = crate::sqs::limits::message_not_available(&receipt_handle);
                failure.push((entry_id, error));
            }
        }

        Ok((success, failure))
    }

    /// Deletes all messages from a queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `identity` - Identity of the authenticated user
    pub async fn purge_queue(
        &self,
        namespace: &str,
        queue: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        // Authorization on the pool, before the delete — this used to read
        // inside the write transaction (the SQLITE_BUSY_SNAPSHOT hazard the
        // batch paths were already cured of). Purging is management, so
        // membership alone is not enough.
        self.require_queue_manager(&identity, namespace).await?;
        let queue_id = self
            .resolve_authorized_queue(namespace, queue, &identity)
            .await?
            .queue_id;

        // Delete all messages from the queue
        let purged = sqlx::query(
            "
            DELETE FROM messages
            WHERE queue = $1
            ",
        )
        .bind(queue_id as i64)
        .execute(self.db())
        .await?;

        self.telemetry.removed_count(
            crate::telemetry::Queue {
                namespace,
                name: queue,
            },
            crate::telemetry::Removal::Purged,
            purged.rows_affected(),
        );
        Ok(())
    }

    /// Pauses or resumes a queue. A paused queue still accepts messages,
    /// deletes and visibility changes, but every receive returns no
    /// messages, so its consumers can be drained and replaced: messages
    /// already in flight can still be deleted, and once the pause has
    /// returned no further message is handed out until the queue resumes.
    /// Pausing a paused queue keeps its original pause time.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `paused` - `true` to pause, `false` to resume
    /// * `identity` - Identity of the authenticated user
    pub async fn set_queue_paused(
        &self,
        namespace: &str,
        queue: &str,
        paused: bool,
        identity: Identity,
    ) -> Result<(), Error> {
        // On the pool, before the write: pausing is management.
        self.require_queue_manager(&identity, namespace).await?;

        let res = sqlx::query(
            "
            UPDATE queues
            SET paused_at = CASE WHEN $3 THEN COALESCE(paused_at, unixepoch('now')) END
            WHERE name = $2 AND ns = (SELECT id FROM namespaces WHERE name = $1)
            ",
        )
        .bind(namespace)
        .bind(queue)
        .bind(paused)
        .execute(self.db())
        .await?;

        if res.rows_affected() == 0 {
            return Err(Error::queue_not_found(queue, namespace));
        }

        Ok(())
    }

    /// Gets statistics for all namespaces accessible to the user (every
    /// namespace for an admin), with each namespace's owners and whether the
    /// caller may manage it.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn list_namespace_statistics(
        &self,
        identity: Identity,
    ) -> Result<Vec<NamespaceStatistics>, Error> {
        let email = identity.id()?;

        let mut namespaces: Vec<NamespaceStatistics> = sqlx::query_as(
            "
            SELECT
                ns.id,
                ns.name,
                ns.created_by_email AS created_by,
                (SELECT COUNT(*) FROM queues q WHERE q.ns = ns.id) AS queue_count,
                (u.role = 'admin' OR IFNULL(p.is_owner, false)) AS can_manage
            FROM users u
            JOIN namespaces ns
            LEFT JOIN user_permissions p ON p.namespace = ns.id AND p.user = u.id
            WHERE u.email = $1 AND (u.role = 'admin' OR p.id IS NOT NULL)
            ORDER BY ns.name
        ",
        )
        .bind(email)
        .fetch_all(self.db())
        .await?;

        let owners: Vec<(i64, String)> = sqlx::query_as(
            "
            SELECT p.namespace, u.email FROM user_permissions p
            JOIN users u ON u.id = p.user
            WHERE p.is_owner
            ORDER BY u.email
            ",
        )
        .fetch_all(self.db())
        .await?;

        for ns in &mut namespaces {
            ns.owners = owners
                .iter()
                .filter(|(id, _)| *id as u64 == ns.namespace.id)
                .map(|(_, email)| email.clone())
                .collect();
        }

        Ok(namespaces)
    }
}

#[cfg(test)]
mod visibility_tests {
    use super::*;
    use actix_identity::Identity;
    use std::collections::{HashMap, HashSet};

    /// Spins up a Service backed by a throwaway on-disk SQLite database (a real
    /// file is required so every pooled connection sees the same schema). The
    /// returned `TempDir` must be kept alive for the duration of the test.
    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        // Config has private fields but derives Deserialize; `Option` fields
        // absent from the JSON fall back to their `defaults` (root user becomes
        // admin@example.com / "password").
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

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    fn send_req(body: &str) -> SendMessageRequest {
        SendMessageRequest {
            queue_url: "http://localhost:8080/api/sqs/ns/q".parse().unwrap(),
            message_body: body.to_string(),
            delay_seconds: None,
            message_attributes: HashMap::new(),
            message_system_attributes: Default::default(),
            message_deduplication_id: None,
            message_group_id: None,
        }
    }

    /// Pulls every in-flight message's visibility deadline into the past so the
    /// next receive treats them as expired — lets us assert re-availability
    /// without sleeping through a real timeout.
    async fn expire_inflight(svc: &Service) {
        sqlx::query("UPDATE messages SET invisible_until = unixepoch('now') - 1 WHERE invisible_until IS NOT NULL")
            .execute(svc.db())
            .await
            .unwrap();
    }

    async fn seed_queue_with_one_message(svc: &Service) -> u64 {
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();
        svc.sqs_send(qid, send_req("hello"), None, None).await.unwrap();
        qid
    }

    #[tokio::test]
    async fn received_message_is_invisible_until_timeout() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(!first[0].receipt_handle.is_empty());

        // Still within the visibility window: must not be handed out again.
        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert!(second.is_empty(), "in-flight message should be invisible");
    }

    #[tokio::test]
    async fn message_becomes_available_again_after_timeout() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        let handle1 = first[0].receipt_handle.clone();

        expire_inflight(&svc).await;

        // Timeout elapsed without a delete: the message is available again and
        // gets a fresh receipt handle.
        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_ne!(
            second[0].receipt_handle, handle1,
            "redelivery should mint a new receipt handle"
        );
    }

    #[tokio::test]
    async fn delete_requires_current_receipt_handle() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        let stale_handle = first[0].receipt_handle.clone();

        // Timeout expires and the message is redelivered to a new consumer.
        expire_inflight(&svc).await;
        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        let current_handle = second[0].receipt_handle.clone();

        // The stale handle from the first receive must no longer delete anything.
        assert!(
            svc.delete_message("ns", "q", &stale_handle, admin())
                .await
                .is_err(),
            "stale receipt handle should not delete a redelivered message"
        );

        // The handle from the latest receive succeeds and removes the message.
        svc.delete_message("ns", "q", &current_handle, admin())
            .await
            .unwrap();

        expire_inflight(&svc).await;
        let after = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert!(after.is_empty(), "deleted message should be gone for good");
    }

    #[tokio::test]
    async fn delete_succeeds_with_expired_handle_before_redelivery() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        let handle = first[0].receipt_handle.clone();

        expire_inflight(&svc).await;

        // The window lapsed but nobody re-received the message, so the handle
        // is still the latest one issued. AWS standard-queue semantics: a
        // receipt handle outlives the visibility timeout until the next
        // receive replaces it, so the late acknowledgement still lands.
        svc.delete_message("ns", "q", &handle, admin())
            .await
            .unwrap();

        let after = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert!(after.is_empty(), "acknowledged message should be gone");
    }

    /// The visibility error a change gets, and its message.
    fn visibility_error(result: Result<(), Error>) -> (AwsCode, String) {
        match result {
            Err(error) => error
                .aws_refusal()
                .unwrap_or_else(|| panic!("expected an AWS refusal, got {error:?}")),
            Ok(()) => panic!("expected an AWS refusal, got Ok"),
        }
    }

    /// As on AWS, the latest handle changes visibility even after its window
    /// lapsed, hiding the message again, up to 12 hours from the receive.
    #[tokio::test]
    async fn change_visibility_follows_the_latest_handle() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;
        let receive = || svc.sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new());

        let handle = receive().await.unwrap()[0].receipt_handle.clone();
        // In flight: extending the window works.
        svc.change_message_visibility("ns", "q", &handle, 600, admin())
            .await
            .unwrap();

        // Lapsed, not yet received again: the handle still hides it.
        expire_inflight(&svc).await;
        svc.change_message_visibility("ns", "q", &handle, 600, admin())
            .await
            .expect("the latest handle works after its window lapses");
        assert!(receive().await.unwrap().is_empty(), "hidden again");

        // No further than 12 hours from the receive.
        sqlx::query("UPDATE messages SET delivered_at = unixepoch('now') - 43000")
            .execute(svc.db())
            .await
            .unwrap();
        let (code, message) =
            visibility_error(svc.change_message_visibility("ns", "q", &handle, 300, admin()).await);
        assert_eq!(code, AwsCode::InvalidParameterValue);
        assert_eq!(
            message,
            "Value 300 for parameter VisibilityTimeout is invalid. Reason: Total \
             VisibilityTimeout for the message is beyond the limit [43200 seconds]."
        );
        svc.change_message_visibility("ns", "q", &handle, 100, admin())
            .await
            .expect("within the 12 hours");

        // The 12 hours are inclusive: exactly reaching them is allowed. The
        // clock only moves forward, so `<` would refuse this one.
        sqlx::query("UPDATE messages SET delivered_at = unixepoch('now') - 43100")
            .execute(svc.db())
            .await
            .unwrap();
        svc.change_message_visibility("ns", "q", &handle, 100, admin())
            .await
            .expect("exactly 12 hours from the receive");
    }

    /// Each receive starts the 12 hours afresh, from that receive.
    #[tokio::test]
    async fn each_receive_restarts_the_twelve_hours() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;
        let receive = || svc.sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new());

        receive().await.unwrap();
        sqlx::query("UPDATE messages SET delivered_at = unixepoch('now') - 43000")
            .execute(svc.db())
            .await
            .unwrap();
        expire_inflight(&svc).await;
        let handle = receive().await.unwrap()[0].receipt_handle.clone();
        svc.change_message_visibility("ns", "q", &handle, 3600, admin())
            .await
            .expect("a new receive, a new 12 hours");
    }

    /// A well-formed handle with no message it can hide, and a malformed one,
    /// are refused as AWS refuses them.
    #[tokio::test]
    async fn change_visibility_refuses_handles_with_nothing_to_hide() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;
        let handle = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap()[0]
            .receipt_handle
            .clone();
        let not_available = (
            AwsCode::InvalidParameterValue,
            format!(
                "Value {handle} for parameter ReceiptHandle is invalid. Reason: Message does \
                 not exist or is not available for visibility timeout change."
            ),
        );

        // Requeued by an admin: no longer the old handle's to hide.
        let message_id = svc
            .list_messages("ns", "q", 100, 0, Default::default(), Default::default())
            .await
            .unwrap()
            .messages[0]
            .id
            .clone();
        svc.admin_set_message_status("ns", "q", &message_id, MessageStatus::Pending, admin())
            .await
            .unwrap();
        assert_eq!(
            visibility_error(svc.change_message_visibility("ns", "q", &handle, 60, admin()).await),
            not_available
        );

        // Deleted.
        svc.delete_message("ns", "q", &handle, admin()).await.unwrap();
        assert_eq!(
            visibility_error(svc.change_message_visibility("ns", "q", &handle, 60, admin()).await),
            not_available
        );

        // Not a handle at all.
        assert_eq!(
            visibility_error(svc.change_message_visibility("ns", "q", "garbage", 60, admin()).await),
            (
                AwsCode::ReceiptHandleIsInvalid,
                "The input receipt handle \"garbage\" is not a valid receipt handle.".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn change_visibility_zero_releases_and_redelivery_invalidates_handle() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        let handle1 = first[0].receipt_handle.clone();

        // Visibility 0 releases the message immediately.
        svc.change_message_visibility("ns", "q", &handle1, 0, admin())
            .await
            .unwrap();

        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(second.len(), 1, "released message should be available");
        let handle2 = second[0].receipt_handle.clone();
        assert_ne!(handle2, handle1, "redelivery should mint a new handle");

        // The pre-release handle died with the redelivery; only the new one acks.
        assert!(
            svc.delete_message("ns", "q", &handle1, admin())
                .await
                .is_err(),
            "handle from before the release should be stale"
        );
        svc.delete_message("ns", "q", &handle2, admin())
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn exhausted_message_reports_failed_and_admin_requeue_revives_it() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        // Test config sets max_retries = 5: every receive counts as a try.
        let mut message_id = None;
        for round in 0..5 {
            let got = svc
                .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
                .await
                .unwrap();
            assert_eq!(got.len(), 1, "delivery {round} should succeed");
            message_id = Some(got[0].message_id.clone());
            expire_inflight(&svc).await;
        }
        let message_id = message_id.unwrap();

        // Retries exhausted: not claimable, listed as failed (not deleted).
        let after = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert!(after.is_empty(), "exhausted message must stop delivering");

        let listed = svc.list_messages("ns", "q", 100, 0, Default::default(), Default::default()).await.unwrap().messages;
        assert_eq!(listed.len(), 1);
        assert!(
            matches!(listed[0].status, MessageStatus::Failed),
            "exhausted message should report failed, got {:?}",
            listed[0].status
        );
        assert_eq!(listed[0].tries, 5);

        // Admin requeue resets the counter and makes it deliverable again.
        svc.admin_set_message_status("ns", "q", &message_id, MessageStatus::Pending, admin())
            .await
            .unwrap();
        let revived = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(revived.len(), 1, "requeued message should deliver again");
    }

    /// Characterization, not endorsement: forcing a message back to `pending`
    /// clears its visibility window and retry counter but leaves the receipt
    /// handle from the pre-requeue delivery in place — so the old consumer
    /// can still acknowledge a message the admin just requeued. See
    /// docs/architecture/message-lifecycle.md ("Sharp edge").
    #[actix_web::test]
    async fn admin_requeue_leaves_prior_receipt_handle_deletable() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        let handle = first[0].receipt_handle.clone();
        let message_id = first[0].message_id.clone();

        svc.admin_set_message_status("ns", "q", &message_id, MessageStatus::Pending, admin())
            .await
            .unwrap();

        // The requeued message is pending again...
        let listed = svc.list_messages("ns", "q", 100, 0, Default::default(), Default::default()).await.unwrap().messages;
        assert!(matches!(listed[0].status, MessageStatus::Pending));

        // ...yet the handle minted before the requeue still deletes it.
        svc.delete_message("ns", "q", &handle, admin())
            .await
            .unwrap();
        assert_eq!(svc.list_messages("ns", "q", 100, 0, Default::default(), Default::default()).await.unwrap().total, 0);
    }

    /// MessageIds are v4 UUIDs, as AWS issues, and never come round again.
    /// The row id does: SQLite starts again at 1 once the table empties.
    #[tokio::test]
    async fn message_ids_are_uuids_and_never_reused() {
        let (svc, _dir) = setup().await;
        let qid = seed_queue_with_one_message(&svc).await;

        let mut seen = HashSet::new();
        let mut sent_id = None;
        for _ in 0..3 {
            let got = svc
                .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
                .await
                .unwrap();
            assert_eq!(got.len(), 1);
            let id = got[0].message_id.clone();
            if let Some(sent_id) = &sent_id {
                assert_eq!(&id, sent_id, "the receive reports the id the send returned");
            }
            assert_eq!(
                uuid::Uuid::parse_str(&id).ok().map(|u| u.get_version_num()),
                Some(4),
                "{id} is not a v4 UUID"
            );
            assert!(seen.insert(id.clone()), "MessageId {id} was issued twice");

            // Deleting the only message empties the table.
            svc.delete_message("ns", "q", &got[0].receipt_handle, admin())
                .await
                .unwrap();
            sent_id = Some(svc.sqs_send(qid, send_req("next"), None, None).await.unwrap().message_id);
        }

        let row_id: i64 = sqlx::query_scalar("SELECT id FROM messages")
            .fetch_one(svc.db())
            .await
            .unwrap();
        assert_eq!(row_id, 1, "every message above reused row id 1");
    }

    /// Attributes hang off the row id, which SQLite reuses, so they must go
    /// with their message (`ON DELETE CASCADE`, foreign keys on): a new
    /// message that takes a deleted one's row id must not inherit them.
    #[tokio::test]
    async fn reused_row_id_does_not_inherit_attributes() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();
        let all = || HashSet::from(["All".to_string()]);

        let mut with_attribute = send_req("with an attribute");
        with_attribute.message_attributes.insert(
            "Origin".to_string(),
            SqsMessageAttribute::string("first"),
        );
        svc.sqs_send(qid, with_attribute, None, None).await.unwrap();
        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), all(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(first[0].message_attributes.len(), 1);
        svc.delete_message("ns", "q", &first[0].receipt_handle, admin())
            .await
            .unwrap();

        let orphans: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM kv_pairs")
            .fetch_one(svc.db())
            .await
            .unwrap();
        assert_eq!(orphans, 0, "the delete left the message's attributes behind");

        svc.sqs_send(qid, send_req("without"), None, None).await.unwrap();
        let row_id: i64 = sqlx::query_scalar("SELECT id FROM messages")
            .fetch_one(svc.db())
            .await
            .unwrap();
        assert_eq!(row_id, 1, "the new message reuses the deleted one's row id");

        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), all(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(second[0].body, "without");
        assert!(
            second[0].message_attributes.is_empty(),
            "inherited {:?}",
            second[0].message_attributes
        );
    }

    /// Characterization: a delayed (never-delivered, currently invisible)
    /// message is listed as `pending` by `list_messages` but counted in none
    /// of the pending/delivered/failed statistics buckets, so the buckets
    /// don't sum to `message_count`. See
    /// docs/architecture/message-lifecycle.md ("Delayed messages").
    #[actix_web::test]
    async fn delayed_message_is_listed_pending_but_counted_in_no_stats_bucket() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();

        let mut req = send_req("later");
        req.delay_seconds = Some(900);
        svc.sqs_send(qid, req, None, None).await.unwrap();

        let listed = svc.list_messages("ns", "q", 100, 0, Default::default(), Default::default()).await.unwrap().messages;
        assert_eq!(listed.len(), 1);
        assert!(matches!(listed[0].status, MessageStatus::Pending));

        let stats = svc.queue_statistics(admin(), "ns", "q").await.unwrap();
        assert_eq!(stats.message_count, 1);
        assert_eq!(
            (stats.pending, stats.delivered, stats.failed),
            (0, 0, 0),
            "delayed message falls through every statistics bucket"
        );
    }

    /// The admin message list shows each attribute's value. A number that
    /// isn't also a JSON number, such as "007", used to fail the whole
    /// listing with a 500; it is shown as its text, as is one JSON would
    /// round or reformat.
    #[actix_web::test]
    async fn the_message_list_shows_numbers_json_can_t_hold_as_text() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();

        let mut req = send_req("numbers");
        let digits38 = "12345678901234567890123456789012345678";
        for (name, value) in [
            ("padded", "007"),
            ("plain", "42"),
            ("decimal", "2.5"),
            // JSON numbers here are f64s, which would round these.
            ("max", digits38),
            ("long_fraction", "0.1000000000000000000001"),
            ("tiny", "1e-128"),
            ("exponent", "1e5"),
        ] {
            req.message_attributes
                .insert(name.to_owned(), SqsMessageAttribute::number(value));
        }
        req.message_attributes.insert(
            "labelled".to_owned(),
            SqsMessageAttribute {
                data_type: "Number.int".to_owned(),
                string_value: Some("8".to_owned()),
                binary_value: None,
            },
        );
        svc.sqs_send(qid, req, None, None).await.unwrap();

        let listed = svc
            .list_messages("ns", "q", 100, 0, Default::default(), Default::default())
            .await
            .unwrap()
            .messages;
        let attributes = &listed[0].message_attributes;
        assert_eq!(attributes["padded"], serde_json::json!("007"));
        assert_eq!(attributes["plain"], serde_json::json!(42));
        assert_eq!(attributes["decimal"], serde_json::json!(2.5));
        assert_eq!(attributes["labelled"], serde_json::json!(8));
        // Shown as sent rather than rounded or reformatted.
        assert_eq!(attributes["max"], serde_json::json!(digits38));
        assert_eq!(attributes["long_fraction"], serde_json::json!("0.1000000000000000000001"));
        assert_eq!(attributes["tiny"], serde_json::json!("1e-128"));
        assert_eq!(attributes["exponent"], serde_json::json!("1e5"));
    }

    /// Regression test: `delete_user` used to hold a write transaction open
    /// across the KMS `delete_key` call. With a key manager backed by the
    /// same SQLite pool (the production default, `SqliteKeyManager`), the KMS
    /// write deadlocked against the open transaction until the busy timeout
    /// failed the request — so deleting a user never succeeded.
    #[tokio::test]
    async fn delete_user_works_with_same_pool_kms() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path,
            "default_max_retries": 5,
        }))
        .unwrap();

        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(crate::kms::sqlite::SqliteKeyManager::new)
            .call()
            .await
            .unwrap();

        let email: Email = "doomed@example.com".parse().unwrap();
        svc.create_user(email.clone(), "password".into(), None, vec![])
            .await
            .unwrap();

        svc.delete_user(email.clone()).await.unwrap();

        let remaining: Option<i64> = sqlx::query_scalar("SELECT id FROM users WHERE email = $1")
            .bind(email.as_str())
            .fetch_optional(svc.db())
            .await
            .unwrap();
        assert!(remaining.is_none(), "user should be deleted");
    }

    /// Sets the queue's MessageRetentionPeriod attribute (seconds).
    async fn set_retention(svc: &Service, seconds: u64) {
        svc.set_queue_attributes(
            "ns",
            "q",
            HashMap::from([(
                "MessageRetentionPeriod".to_owned(),
                serde_json::Value::String(seconds.to_string()),
            )]),
            admin(),
        )
        .await
        .unwrap();
    }

    /// Backdates every message's arrival so the sweep sees it as `age`
    /// seconds old, without sleeping.
    async fn backdate_messages(svc: &Service, age: i64) {
        sqlx::query("UPDATE messages SET received_at = unixepoch('now') - $1")
            .bind(age)
            .execute(svc.db())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn authorized_queue_cache_serves_hits_and_honors_invalidation() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        // A plain member: an admin needs no permission row, so revoking one
        // would not revoke anything.
        svc.create_user(
            "member@example.com".try_into().unwrap(),
            "hunter2hunter2".into(),
            Some(Role::User),
            vec!["ns".into()],
        )
        .await
        .unwrap();
        let ident = Identity::mock("member@example.com".to_string());
        let first = svc
            .resolve_authorized_queue("ns", "q", &ident)
            .await
            .unwrap();

        // The cache (not the database) now answers: dropping the permission
        // row behind the cache's back still resolves until invalidated.
        sqlx::query("DELETE FROM user_permissions")
            .execute(svc.db())
            .await
            .unwrap();
        let cached = svc
            .resolve_authorized_queue("ns", "q", &ident)
            .await
            .unwrap();
        assert_eq!(cached.queue_id, first.queue_id);

        // The revocation hook clears the cache; authorization fails on the
        // very next resolve.
        svc.clear_authorized_queues();
        assert!(matches!(
            svc.resolve_authorized_queue("ns", "q", &ident).await,
            Err(Error::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn authorized_queue_cache_never_serves_a_deleted_queues_id() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let ident = admin();
        let old = svc
            .resolve_authorized_queue("ns", "q", &ident)
            .await
            .unwrap();

        // Deleting the queue invalidates eagerly: the dead id must not be
        // served even within the TTL window.
        svc.delete_queue("ns", "q", admin()).await.unwrap();
        assert!(svc
            .resolve_authorized_queue("ns", "q", &ident)
            .await
            .is_err());

        // Advance the rowid sequence so the re-created queue cannot reuse
        // the deleted queue's id (which would mask a stale cache hit).
        svc.create_queue("ns", "decoy", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();

        // Re-creating the same name resolves to the new queue, not the
        // cached corpse.
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let new = svc
            .resolve_authorized_queue("ns", "q", &ident)
            .await
            .unwrap();
        assert_ne!(new.queue_id, old.queue_id);

        // And sends land in the new queue.
        svc.sqs_send(new.queue_id, send_req("fresh"), None, None)
            .await
            .unwrap();
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM messages WHERE queue = $1")
                .bind(new.queue_id as i64)
                .fetch_one(svc.db())
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn retention_sweep_deletes_messages_past_their_period() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;
        set_retention(&svc, 60).await;

        // Younger than the period: kept.
        backdate_messages(&svc, 30).await;
        assert_eq!(Service::sweep_expired_messages(svc.db()).await.unwrap(), 0);

        // Older than the period: deleted, and gone for receivers.
        backdate_messages(&svc, 120).await;
        assert_eq!(Service::sweep_expired_messages(svc.db()).await.unwrap(), 1);
        let after = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert!(after.is_empty(), "expired message should be gone");
    }

    #[tokio::test]
    async fn retention_zero_or_unset_keeps_messages_forever() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;
        backdate_messages(&svc, 10_000_000).await; // ~4 months old

        // No attribute set: retained.
        assert_eq!(Service::sweep_expired_messages(svc.db()).await.unwrap(), 0);

        // Explicit 0 is the "forever" sentinel (AWS's minimum is 60, so 0
        // can never be a real period): still retained.
        set_retention(&svc, 0).await;
        assert_eq!(Service::sweep_expired_messages(svc.db()).await.unwrap(), 0);

        let still_there = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(still_there.len(), 1, "message should be retained forever");
    }

    #[tokio::test]
    async fn retention_trumps_visibility_and_exhaustion() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;
        set_retention(&svc, 60).await;

        // In flight on a long lease — retention still applies, as on AWS.
        let received = svc
            .sqs_recv_batch("ns", "q", 10, Some(3000), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(received.len(), 1);

        backdate_messages(&svc, 120).await;
        assert_eq!(Service::sweep_expired_messages(svc.db()).await.unwrap(), 1);

        // Sweeping only touches queues with a configured period: a second
        // queue without one is untouched by the same sweep.
        svc.create_queue("ns", "q2", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let q2 = svc.get_queue_id("ns", "q2", svc.db()).await.unwrap().unwrap();
        svc.sqs_send(q2, send_req("durable"), None, None).await.unwrap();
        backdate_messages(&svc, 120).await;
        assert_eq!(Service::sweep_expired_messages(svc.db()).await.unwrap(), 0);
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use crate::sqs::types::send_message_batch::{
        SendMessageBatchRequest, SendMessageBatchRequestEntry,
    };
    use actix_identity::Identity;
    use futures_util::future::join_all;
    use std::collections::{HashMap, HashSet};

    /// Same throwaway on-disk database setup as `visibility_tests`.
    async fn setup() -> (Service, tempfile::TempDir) {
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

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    fn batch_req(label: usize) -> SendMessageBatchRequest {
        SendMessageBatchRequest {
            queue_url: "http://localhost:8080/api/sqs/ns/q".parse().unwrap(),
            entries: (0..10)
                .map(|i| SendMessageBatchRequestEntry {
                    id: i.to_string(),
                    message_body: format!("batch {label} entry {i}"),
                    delay_seconds: None,
                    message_attributes: HashMap::new(),
                    message_system_attributes: Default::default(),
                    message_deduplication_id: None,
                    message_group_id: None,
                })
                .collect(),
        }
    }

    fn send_req(body: String) -> SendMessageRequest {
        SendMessageRequest {
            queue_url: "http://localhost:8080/api/sqs/ns/q".parse().unwrap(),
            message_body: body,
            delay_seconds: None,
            message_attributes: HashMap::new(),
            message_system_attributes: Default::default(),
            message_deduplication_id: None,
            message_group_id: None,
        }
    }

    /// Regression test: `sqs_send_batch` used to open its transaction with a
    /// read (`get_queue_id`), so any other writer committing before the
    /// batch's first INSERT poisoned the snapshot (SQLITE_BUSY_SNAPSHOT) and
    /// every entry failed with a 500 — observed live with the dashboard
    /// polling alongside a bulk send. The futures below run on one executor
    /// and interleave at every await point, exercising exactly that window.
    #[actix_web::test]
    async fn concurrent_batch_sends_survive_interleaved_reads_and_writes() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();

        const BATCHES: usize = 10;
        const SINGLES: usize = 10;

        let batches = (0..BATCHES).map(|i| {
            let svc = &svc;
            async move { svc.sqs_send_batch("ns", "q", batch_req(i), None, None).await }
        });
        let singles = (0..SINGLES).map(|i| {
            let svc = &svc;
            async move {
                svc.sqs_send(qid, send_req(format!("single {i}")), None, None)
                    .await
                    .map(|_| ())
            }
        });
        let readers = (0..10).map(|_| {
            let svc = &svc;
            async move {
                svc.queue_statistics(admin(), "ns", "q").await?;
                svc.list_messages("ns", "q", 100, 0, Default::default(), Default::default()).await.map(|_| ())
            }
        });

        let (batch_results, single_results, reader_results) = futures_util::join!(
            join_all(batches),
            join_all(singles),
            join_all(readers)
        );

        for res in single_results {
            res.expect("concurrent single send should succeed");
        }
        for res in reader_results {
            res.expect("concurrent reads should succeed");
        }
        for res in batch_results {
            let res = res.expect("batch request should succeed");
            assert!(
                res.failed.is_empty(),
                "no batch entry may fail under concurrency: {:?}",
                res.failed
                    .iter()
                    .map(|f| (&f.id, &f.message))
                    .collect::<Vec<_>>()
            );
            assert_eq!(res.successful.len(), 10);
        }

        let stats = svc.queue_statistics(admin(), "ns", "q").await.unwrap();
        assert_eq!(
            stats.message_count,
            (BATCHES * 10 + SINGLES) as u64,
            "every concurrently sent message must have landed"
        );
    }

    /// The same hazard existed in `delete_message_batch`, which ran its
    /// namespace/access/queue lookups inside the write transaction.
    #[actix_web::test]
    async fn concurrent_batch_deletes_survive_interleaved_writes() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();

        // Seed and receive 50 messages so we hold 5 batches of valid handles.
        for i in 0..50 {
            svc.sqs_send(qid, send_req(format!("doomed {i}")), None, None)
                .await
                .unwrap();
        }
        let received = svc
            .sqs_recv_batch("ns", "q", 50, Some(300), HashSet::new(), HashSet::new())
            .await
            .unwrap();
        assert_eq!(received.len(), 50);
        let handle_batches: Vec<Vec<(String, String)>> = received
            .chunks(10)
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|m| (m.message_id.clone(), m.receipt_handle.clone()))
                    .collect()
            })
            .collect();

        let deletes = handle_batches.into_iter().map(|entries| {
            let svc = &svc;
            async move { svc.delete_message_batch("ns", "q", entries, admin()).await }
        });
        let writers = (0..10).map(|i| {
            let svc = &svc;
            async move {
                svc.sqs_send(qid, send_req(format!("bystander {i}")), None, None)
                    .await
                    .map(|_| ())
            }
        });

        let (delete_results, writer_results) =
            futures_util::join!(join_all(deletes), join_all(writers));

        for res in writer_results {
            res.expect("concurrent send should succeed");
        }
        for res in delete_results {
            let (success, failure) = res.expect("batch delete request should succeed");
            assert!(
                failure.is_empty(),
                "no delete entry may fail under concurrency: {:?}",
                failure
                    .iter()
                    .map(|(id, e)| (id, e.to_string()))
                    .collect::<Vec<_>>()
            );
            assert_eq!(success.len(), 10);
        }

        // Only the bystander sends remain.
        let stats = svc.queue_statistics(admin(), "ns", "q").await.unwrap();
        assert_eq!(stats.message_count, 10);
    }

    /// Regression test: the admin write paths opened their transactions with
    /// reads, so any other writer committing before their first write failed
    /// them with SQLITE_BUSY_SNAPSHOT ("database is locked", a 500). Seen as
    /// an intermittent API-key creation failure on a freshly started server.
    /// Each admin write here runs while a loop of sends keeps committing.
    #[actix_web::test]
    async fn admin_writes_survive_interleaved_writes() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();

        let done = std::cell::Cell::new(false);
        let writer = async {
            let mut sent = 0;
            while !done.get() {
                svc.sqs_send(qid, send_req(format!("bystander {sent}")), None, None)
                    .await
                    .expect("concurrent send should succeed");
                sent += 1;
            }
        };
        let admin_writes = async {
            for i in 0..3 {
                let (ns, q) = (format!("ns{i}"), format!("q{i}"));
                svc.create_token(format!("key{i}"), "ns".to_string(), admin())
                    .await
                    .unwrap_or_else(|e| panic!("create_token {i}: {e:?}"));
                svc.create_namespace(&ns, admin())
                    .await
                    .unwrap_or_else(|e| panic!("create_namespace {i}: {e:?}"));
                svc.create_queue("ns", &q, Default::default(), HashMap::new(), admin())
                    .await
                    .unwrap_or_else(|e| panic!("create_queue {i}: {e:?}"));
                let attributes = HashMap::from([(
                    "VisibilityTimeout".to_owned(),
                    serde_json::Value::String("60".to_owned()),
                )]);
                svc.set_queue_attributes("ns", &q, attributes, admin())
                    .await
                    .unwrap_or_else(|e| panic!("set_queue_attributes {i}: {e:?}"));
                svc.delete_queue("ns", &q, admin())
                    .await
                    .unwrap_or_else(|e| panic!("delete_queue {i}: {e:?}"));
                svc.delete_namespace(&ns, admin())
                    .await
                    .unwrap_or_else(|e| panic!("delete_namespace {i}: {e:?}"));
            }
            done.set(true);
        };

        futures_util::join!(writer, admin_writes);
    }
}

#[cfg(test)]
mod attribute_validation_tests {
    use super::*;
    use actix_identity::Identity;

    /// Same throwaway on-disk database setup as `visibility_tests`.
    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path,
        }))
        .unwrap();

        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", Default::default(), HashMap::new(), admin())
            .await
            .unwrap();

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    fn attrs(name: &str, value: u64) -> QueueAttributeMap {
        serde_json::from_value(serde_json::json!({ name: value.to_string() })).unwrap()
    }

    /// (attribute, highest accepted value, lowest rejected value above it).
    const UPPER_BOUNDS: [(&str, u64, u64); 5] = [
        ("DelaySeconds", 900, 901),
        ("MaximumMessageSize", 1_048_576, 1_048_577),
        ("MessageRetentionPeriod", 1_209_600, 1_209_601),
        ("ReceiveMessageWaitTimeSeconds", 20, 21),
        ("VisibilityTimeout", 43_200, 43_201),
    ];

    /// (attribute, lowest accepted value, highest rejected value below it),
    /// for the attributes whose minimum isn't 0.
    const LOWER_BOUNDS: [(&str, u64, u64); 2] = [
        ("MaximumMessageSize", 1_024, 1_023),
        // 0 ("retain forever") is also accepted; checked below.
        ("MessageRetentionPeriod", 60, 59),
    ];

    #[actix_web::test]
    async fn set_queue_attributes_enforces_aws_ranges() {
        let (svc, _dir) = setup().await;
        let set = |name: &str, value: u64| svc.set_queue_attributes("ns", "q", attrs(name, value), admin());

        let accepted = UPPER_BOUNDS.iter().chain(&LOWER_BOUNDS).map(|&(name, ok, _)| (name, ok));
        for (name, value) in accepted {
            set(name, value).await.unwrap_or_else(|e| panic!("{name}={value}: {e:?}"));
        }

        let rejected = UPPER_BOUNDS
            .iter()
            .chain(&LOWER_BOUNDS)
            .map(|&(name, _, bad)| (name, bad))
            .chain(UPPER_BOUNDS.iter().map(|&(name, ..)| (name, u64::MAX)));
        for (name, value) in rejected {
            let err = set(name, value).await.unwrap_err();
            assert!(
                matches!(&err, Error::InvalidAttributeValue { message } if message.starts_with(name)),
                "{name}={value}: expected InvalidAttributeValue, got {err:?}"
            );
        }
        set("MessageRetentionPeriod", 0).await.expect("0 means retain forever");

        // A rejected value is not stored: VisibilityTimeout keeps its maximum.
        let stored = svc
            .get_queue_attributes("ns", "q", &["VisibilityTimeout".to_string()], &admin())
            .await
            .unwrap();
        assert_eq!(stored.visibility_timeout, Some(43_200));
    }

    /// Regression test for the original bug: a VisibilityTimeout past
    /// i64::MAX was stored negative, making received messages immediately
    /// redeliverable. Creating a queue with one is now refused outright.
    #[actix_web::test]
    async fn create_queue_refuses_out_of_range_attributes() {
        let (svc, _dir) = setup().await;

        let err = svc
            .create_queue("ns", "huge", attrs("VisibilityTimeout", u64::MAX), HashMap::new(), admin())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidAttributeValue { .. }), "{err:?}");
        assert!(svc.get_queue_id("ns", "huge", svc.db()).await.unwrap().is_none());
    }
}

#[cfg(test)]
mod create_queue_tests {
    use super::*;
    use actix_identity::Identity;

    /// Same throwaway on-disk database setup as `visibility_tests`.
    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path,
        }))
        .unwrap();

        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();
        svc.create_namespace("ns", admin()).await.unwrap();

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    fn attrs(json: serde_json::Value) -> QueueAttributeMap {
        serde_json::from_value(json).unwrap()
    }

    async fn create(
        svc: &Service,
        attributes: serde_json::Value,
    ) -> Result<CreateQueueOutcome, Error> {
        svc.create_queue("ns", "q", attrs(attributes), HashMap::new(), admin())
            .await
    }

    /// The attribute a rejected re-create names, or a panic if it succeeded.
    fn conflicting_attribute(result: Result<CreateQueueOutcome, Error>) -> String {
        match result {
            Err(Error::QueueAlreadyExists { attribute, .. }) => attribute,
            other => panic!("expected QueueAlreadyExists, got {other:?}"),
        }
    }

    /// Requested attributes are compared; attributes left out are not.
    #[actix_web::test]
    async fn recreating_with_matching_attributes_changes_nothing() {
        let (svc, _dir) = setup().await;
        let first = serde_json::json!({ "VisibilityTimeout": "60", "DelaySeconds": "5" });
        assert_eq!(create(&svc, first.clone()).await.unwrap(), CreateQueueOutcome::Created);

        for again in [
            first,
            serde_json::json!({ "VisibilityTimeout": "60" }),
            serde_json::json!({}),
        ] {
            assert_eq!(
                create(&svc, again.clone()).await.unwrap(),
                CreateQueueOutcome::AlreadyExists,
                "{again}"
            );
        }

        // Tags on a re-create are not applied to the existing queue.
        svc.create_queue(
            "ns",
            "q",
            Default::default(),
            HashMap::from([("team".to_string(), "late".to_string())]),
            admin(),
        )
        .await
        .unwrap();
        assert!(svc.get_queue_tags("ns", "q", admin()).await.unwrap().is_empty());
    }

    #[actix_web::test]
    async fn recreating_with_a_differing_attribute_is_rejected_untouched() {
        let (svc, _dir) = setup().await;
        create(&svc, serde_json::json!({ "VisibilityTimeout": "60" }))
            .await
            .unwrap();

        let result = create(&svc, serde_json::json!({ "VisibilityTimeout": "61" })).await;
        assert_eq!(conflicting_attribute(result), "VisibilityTimeout");

        let stored = svc
            .get_queue_attributes("ns", "q", &["VisibilityTimeout".to_string()], &admin())
            .await
            .unwrap();
        assert_eq!(stored.visibility_timeout, Some(60));
    }

    /// An attribute the queue never stored compares as the default NerveMQ
    /// applies in its place.
    #[actix_web::test]
    async fn unset_attributes_compare_as_their_defaults() {
        let (svc, _dir) = setup().await;
        create(&svc, serde_json::json!({})).await.unwrap();

        let defaults = serde_json::json!({
            "DelaySeconds": "0",
            "MaximumMessageSize": "1048576",
            "MessageRetentionPeriod": "0",
            "ReceiveMessageWaitTimeSeconds": "0",
            "VisibilityTimeout": "30",
        });
        assert_eq!(create(&svc, defaults).await.unwrap(), CreateQueueOutcome::AlreadyExists);

        // NerveMQ retains forever when unset, so AWS's 4-day default differs.
        let result = create(&svc, serde_json::json!({ "MessageRetentionPeriod": "345600" })).await;
        assert_eq!(conflicting_attribute(result), "MessageRetentionPeriod");
    }

    /// Attributes NerveMQ stores verbatim have no default: they match only
    /// a stored, equal value.
    #[actix_web::test]
    async fn untyped_attributes_compare_as_stored_or_aws_s_default() {
        let (svc, _dir) = setup().await;
        create(&svc, serde_json::json!({ "Policy": "p1" })).await.unwrap();

        assert_eq!(
            create(&svc, serde_json::json!({ "Policy": "p1" })).await.unwrap(),
            CreateQueueOutcome::AlreadyExists
        );
        let result = create(&svc, serde_json::json!({ "Policy": "p2" })).await;
        assert_eq!(conflicting_attribute(result), "Policy");

        // Never set: they compare as AWS's defaults.
        assert_eq!(
            create(
                &svc,
                serde_json::json!({
                    "SqsManagedSseEnabled": "true",
                    "KmsDataKeyReusePeriodSeconds": "300",
                    "RedriveAllowPolicy": "",
                })
            )
            .await
            .unwrap(),
            CreateQueueOutcome::AlreadyExists
        );
        let result = create(&svc, serde_json::json!({ "SqsManagedSseEnabled": "false" })).await;
        assert_eq!(conflicting_attribute(result), "SqsManagedSseEnabled");

        // FIFO attributes don't exist for a standard queue, as on AWS.
        let err = create(&svc, serde_json::json!({ "FifoQueue": "false" }))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Aws { code: AwsCode::InvalidAttributeName, message }
                if message == "Unknown Attribute FifoQueue."),
            "{err:?}"
        );
    }

    /// Concurrent creates of one name used to leave the loser with a
    /// unique-constraint failure (a 500).
    #[actix_web::test]
    async fn concurrent_creates_of_one_name_both_succeed() {
        let (svc, _dir) = setup().await;

        let (a, b) = futures_util::join!(
            create(&svc, serde_json::json!({})),
            create(&svc, serde_json::json!({}))
        );
        let mut outcomes = [a.unwrap(), b.unwrap()];
        outcomes.sort_by_key(|o| *o == CreateQueueOutcome::AlreadyExists);
        assert_eq!(
            outcomes,
            [CreateQueueOutcome::Created, CreateQueueOutcome::AlreadyExists]
        );
    }
}

#[cfg(test)]
mod access_rule_tests {
    use super::*;
    use actix_identity::Identity;

    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": dir.path().join("test.db").to_string_lossy(),
        }))
        .unwrap();
        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();
        svc.create_namespace("ns", Identity::mock(svc.config().root_email().to_owned()))
            .await
            .unwrap();
        (svc, dir)
    }

    fn who(email: &str) -> Identity {
        Identity::mock(email.to_string())
    }

    #[test]
    fn only_admins_and_owners_manage() {
        for (is_admin, is_owner, manages) in [
            (false, false, false),
            (false, true, true),
            (true, false, true),
            (true, true, true),
        ] {
            let access = NamespaceAccess {
                user_id: 1,
                is_admin,
                is_owner,
            };
            assert_eq!(access.can_manage(), manages, "{access:?}");
        }
    }

    #[tokio::test]
    async fn unknown_and_disabled_callers_get_nothing() {
        let (svc, _dir) = setup().await;
        let ns = svc.get_namespace_id("ns", svc.db()).await.unwrap().unwrap();
        svc.create_user(
            "gone@example.com".try_into().unwrap(),
            "hunter2hunter2".into(),
            Some(Role::Admin),
            vec![],
        )
        .await
        .unwrap();
        svc.set_user_disabled(&"gone@example.com".try_into().unwrap(), true)
            .await
            .unwrap();

        for email in ["ghost@example.com", "gone@example.com"] {
            assert!(matches!(
                svc.require_admin(&who(email)).await,
                Err(Error::Unauthorized)
            ), "{email}: require_admin");
            assert!(svc.check_user_role(who(email), Role::User).await.is_err(), "{email}");
        }
        // Unknown users have no access anywhere.
        assert!(matches!(
            svc.check_user_access(&who("ghost@example.com"), ns, svc.db()).await,
            Err(Error::Unauthorized)
        ));
        assert!(matches!(
            svc.resolve_authorized_queue("ns", "q", &who("ghost@example.com")).await,
            Err(Error::Unauthorized)
        ));
        assert!(svc.list_namespaces(who("ghost@example.com")).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn require_admin_distinguishes_users_from_strangers() {
        let (svc, _dir) = setup().await;
        svc.create_user(
            "bob@example.com".try_into().unwrap(),
            "hunter2hunter2".into(),
            Some(Role::User),
            vec!["ns".into()],
        )
        .await
        .unwrap();

        assert!(svc.require_admin(&who(svc.config().root_email())).await.is_ok());
        assert!(matches!(
            svc.require_admin(&who("bob@example.com")).await,
            Err(Error::Forbidden { .. })
        ));
    }

    /// The last-admin guard is part of each update statement, so racing
    /// requests cannot each see "another admin is left" and both go through.
    #[tokio::test]
    async fn racing_demotions_and_disables_always_leave_an_active_admin() {
        for race in ["demote", "disable", "delete"] {
            let (svc, _dir) = setup().await;
            let root = svc.config().root_email().to_owned();
            svc.create_user(
                "ops@example.com".try_into().unwrap(),
                "hunter2hunter2".into(),
                Some(Role::Admin),
                vec![],
            )
            .await
            .unwrap();

            let act = |email: String| {
                let svc = svc.clone();
                async move {
                    let email: Email = email.as_str().try_into().unwrap();
                    match race {
                        "demote" => svc.set_user_role(&email, Role::User).await,
                        "disable" => svc.set_user_disabled(&email, true).await,
                        _ => svc.delete_user(email).await,
                    }
                }
            };
            let (a, b) = tokio::join!(act(root.clone()), act("ops@example.com".to_string()));

            let refused = [&a, &b]
                .iter()
                .filter(|r| matches!(r, Err(Error::Conflict { .. })))
                .count();
            assert_eq!(refused, 1, "{race}: {a:?} / {b:?}");
            let active_admins: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM users WHERE role = 'admin' AND disabled_at IS NULL",
            )
            .fetch_one(svc.db())
            .await
            .unwrap();
            assert_eq!(active_admins, 1, "{race}");
        }
    }
}

#[cfg(test)]
mod name_rule_tests {
    use super::*;
    use actix_identity::Identity;

    #[tokio::test]
    async fn namespace_names_follow_the_rule() {
        let dir = tempfile::tempdir().unwrap();
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": dir.path().join("test.db").to_string_lossy(),
        }))
        .unwrap();
        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();
        let root = || Identity::mock(svc.config().root_email().to_owned());

        for name in ["team-a_1", &"n".repeat(32)] {
            svc.create_namespace(name, root()).await.unwrap();
        }
        for name in ["", "a.b", "a b", "a/b", &"n".repeat(33)] {
            assert!(
                matches!(
                    svc.create_namespace(name, root()).await,
                    Err(Error::InvalidParameter { .. })
                ),
                "{name:?} was accepted"
            );
        }
    }

    #[test]
    fn queue_names_follow_the_aws_rule() {
        for name in ["q", "order-events_v2", &"q".repeat(80), "jobs.fifo"] {
            assert!(validate_queue_name(name).is_ok(), "{name:?} was refused");
        }
        for name in ["", ".fifo", "a.b", "jobs.fifo.fifo", &"q".repeat(81)] {
            assert!(validate_queue_name(name).is_err(), "{name:?} was accepted");
        }
    }
}

#[cfg(test)]
mod text_affinity_tests {
    use super::*;
    use actix_identity::Identity;

    /// Same throwaway on-disk database setup as `visibility_tests`.
    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path,
        }))
        .unwrap();

        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    /// Numeric-looking names must be stored and read back as text, verbatim.
    /// The name columns were originally declared `string` — NUMERIC affinity
    /// — which coerced "123" to an integer on insert and made every read
    /// that decodes the name as text fail (migration 0005).
    #[tokio::test]
    async fn numeric_names_roundtrip_as_text() {
        let (svc, _dir) = setup().await;

        svc.create_namespace("123", admin()).await.unwrap();
        let namespaces = svc.list_namespaces(admin()).await.unwrap();
        assert!(
            namespaces.iter().any(|ns| ns.name == "123"),
            "namespace named \"123\" should list as text: {namespaces:?}"
        );

        // api_keys.name had the same wart; "007" also checks that leading
        // zeros survive (NUMERIC affinity would have collapsed it to 7).
        svc.create_token("007".to_string(), "123".to_string(), admin())
            .await
            .unwrap();
        let names: Vec<String> = sqlx::query_scalar("SELECT name FROM api_keys")
            .fetch_all(svc.db())
            .await
            .unwrap();
        assert!(names.contains(&"007".to_string()), "token names: {names:?}");
    }
}

#[cfg(test)]
mod migration_upgrade_tests {
    use super::*;

    /// Upgrading a database that already holds data must keep all of it.
    ///
    /// Migration 0005 rebuilds `namespaces` and `queues` (create, copy,
    /// drop, rename). `DROP TABLE` with foreign-key enforcement on runs an
    /// implicit `DELETE`, which fires `ON DELETE CASCADE` — deferring the
    /// checks does not defer the actions — so the upgrade used to delete
    /// every queue, message, permission and API key. Fresh databases never
    /// showed it: there was nothing to cascade into.
    #[tokio::test]
    async fn upgrade_from_0004_keeps_existing_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Bring the database to version 0004 exactly as production did:
        // through sqlx's migrator, on a connection enforcing foreign keys.
        let old_migrations = dir.path().join("migrations");
        std::fs::create_dir(&old_migrations).unwrap();
        for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")).unwrap()
        {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if name.as_str() < "0005" {
                std::fs::copy(&path, old_migrations.join(&name)).unwrap();
            }
        }

        let opts = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();
        sqlx::migrate::Migrator::new(old_migrations.as_path())
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();

        for statement in [
            "INSERT INTO users (id, email, hashed_pass, kms_key_id, role)
             VALUES (1, 'owner@example.com', 'x', 'k', 'admin')",
            "INSERT INTO namespaces (id, name, created_by) VALUES (1, 'prod', 1)",
            "INSERT INTO queues (id, ns, name, created_by) VALUES (1, 1, 'jobs', 1)",
            "INSERT INTO queue_attributes (queue, k, v) VALUES (1, 'DelaySeconds', '0')",
            // Under internal keys: a value that is an integer, and two the
            // attribute-name bypass could store, which 0017 removes: an
            // integer that isn't one, and a redrive policy that isn't a
            // JSON string.
            "INSERT INTO queue_attributes (queue, k, v)
             VALUES (1, 'delay_seconds', '5'), (1, 'visibility_timeout', '\"120\"'),
                    (1, 'redrive_policy', '{\"maxReceiveCount\":3}')",
            "INSERT INTO messages (id, queue, body) VALUES (1, 1, x'00')",
            "INSERT INTO kv_pairs (message, k, v) VALUES (1, 'trace', x'01')",
            "INSERT INTO user_permissions (user, namespace, can_delete_ns) VALUES (1, 1, true)",
            "INSERT INTO api_keys (user, ns, name, key_id, hashed_key, encrypted_key)
             VALUES (1, 1, 'ci', 'AKID', 'h', x'00')",
            // A non-admin owner and a plain member, each with a key: 0012
            // backfills key access from these.
            "INSERT INTO users (id, email, hashed_pass, kms_key_id, role)
             VALUES (2, 'lead@example.com', 'x', 'k2', 'user'),
                    (3, 'worker@example.com', 'x', 'k3', 'user')",
            "INSERT INTO user_permissions (user, namespace, can_delete_ns)
             VALUES (2, 1, true), (3, 1, false)",
            "INSERT INTO api_keys (user, ns, name, key_id, hashed_key, encrypted_key)
             VALUES (2, 1, 'lead', 'AKID2', 'h', x'00'), (3, 1, 'worker', 'AKID3', 'h', x'00')",
        ] {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }
        pool.close().await;

        // Upgrade by starting the service on it, as a deployment would.
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path.to_string_lossy(),
        }))
        .unwrap();
        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();

        for (table, seeded) in [
            ("namespaces", 1),
            ("queues", 1),
            // Four seeded; 0017 removed the two of the wrong type.
            ("queue_attributes", 2),
            ("messages", 1),
            ("kv_pairs", 1),
            ("user_permissions", 3),
            ("api_keys", 3),
        ] {
            let rows: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(svc.db())
                .await
                .unwrap();
            assert_eq!(rows, seeded, "{table} lost rows in the upgrade");
        }

        // 0014 added the trace header and 0015 the millisecond times, all
        // empty for messages sent before them.
        let added: (Option<String>, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT aws_trace_header, sent_at_ms, first_delivered_at_ms FROM messages WHERE id = 1",
        )
        .fetch_one(svc.db())
        .await
        .unwrap();
        assert_eq!(added, (None, None, None));

        // 0017 gave the queue its times, the time of the upgrade, and removed
        // the attributes of the wrong type.
        let (created, modified): (Option<i64>, Option<i64>) =
            sqlx::query_as("SELECT created_at, attributes_modified_at FROM queues WHERE id = 1")
                .fetch_one(svc.db())
                .await
                .unwrap();
        let now = chrono::Utc::now().timestamp();
        for time in [created, modified] {
            assert!(time.is_some_and(|t| (now - t).abs() < 60), "{time:?}");
        }
        let kept: Vec<String> =
            sqlx::query_scalar("SELECT k FROM queue_attributes WHERE queue = 1 ORDER BY k")
                .fetch_all(svc.db())
                .await
                .unwrap();
        assert_eq!(kept, ["DelaySeconds", "delay_seconds"]);

        // 0016 gave the message already stored a MessageId.
        let message_id: String = sqlx::query_scalar("SELECT message_id FROM messages WHERE id = 1")
            .fetch_one(svc.db())
            .await
            .unwrap();
        assert_eq!(
            uuid::Uuid::parse_str(&message_id).ok().map(|u| u.get_version_num()),
            Some(4),
            "{message_id} is not a v4 UUID"
        );
        assert_eq!(message_id, message_id.to_lowercase());

        // 0011 renamed the delete flag to ownership and recorded the
        // creator's email next to their id.
        let owner: bool = sqlx::query_scalar("SELECT is_owner FROM user_permissions WHERE user = 1")
            .fetch_one(svc.db())
            .await
            .unwrap();
        assert!(owner, "can_delete_ns did not carry over to is_owner");
        let creator: Option<String> =
            sqlx::query_scalar("SELECT created_by_email FROM namespaces WHERE name = 'prod'")
                .fetch_one(svc.db())
                .await
                .unwrap();
        assert_eq!(creator.as_deref(), Some("owner@example.com"));

        // 0012 gave existing keys their owner's level in the key's namespace.
        let access: Vec<(String, String)> =
            sqlx::query_as("SELECT name, access FROM api_keys ORDER BY name")
                .fetch_all(svc.db())
                .await
                .unwrap();
        assert_eq!(
            access,
            [
                ("ci".to_string(), "admin".to_string()),
                ("lead".to_string(), "owner".to_string()),
                ("worker".to_string(), "member".to_string()),
            ]
        );
    }

    /// Migration 0011 rebuilds `namespaces`, so it must refuse to run on a
    /// connection that enforces foreign keys — as `sqlx migrate run` would —
    /// rather than cascade-delete everything that references it.
    #[tokio::test]
    async fn namespace_rebuild_refuses_foreign_key_enforcement() {
        let dir = tempfile::tempdir().unwrap();
        let opts = SqliteConnectOptions::new()
            .filename(dir.path().join("test.db"))
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new().connect_with(opts).await.unwrap();

        let err = sqlx::migrate!("./migrations").run(&pool).await.unwrap_err();
        assert!(
            err.to_string().contains("CHECK constraint failed: foreign_keys_off"),
            "{err}"
        );
    }
}

#[cfg(test)]
mod supplied_credential_tests {
    use super::*;
    use crate::auth::crypto::verify_secret;
    use actix_identity::Identity;
    use argon2::password_hash::PasswordHashString;
    use secrecy::SecretString;

    /// Same throwaway on-disk database setup as `text_affinity_tests`.
    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path,
        }))
        .unwrap();

        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    fn supplied(access_key: &str, secret_key: &str) -> Option<SuppliedCredentials> {
        Some(SuppliedCredentials {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        })
    }

    /// A supplied pair is stored exactly as a generated one would be: the access
    /// key lands in `key_id` verbatim and the secret's Argon2 hash verifies.
    #[tokio::test]
    async fn supplied_credentials_are_stored_verbatim() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();

        let response = svc
            .create_token_with(
                "consumer".to_string(),
                "ns".to_string(),
                admin(),
                supplied("MYACCESSKEY", "my-secret-key"),
                None,
            )
            .await
            .unwrap();

        assert_eq!(response.access_key, "MYACCESSKEY");
        assert_eq!(response.secret_key, "my-secret-key");

        let (key_id, hashed): (String, String) =
            sqlx::query_as("SELECT key_id, hashed_key FROM api_keys WHERE name = 'consumer'")
                .fetch_one(svc.db())
                .await
                .unwrap();
        assert_eq!(key_id, "MYACCESSKEY");
        verify_secret(
            SecretString::from("my-secret-key".to_string()),
            PasswordHashString::new(&hashed).unwrap(),
        )
        .expect("the stored hash must verify against the supplied secret");
    }

    /// Omitting the credentials keeps the generating behaviour.
    #[tokio::test]
    async fn omitted_credentials_are_still_generated() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();

        let response = svc
            .create_token("generated".to_string(), "ns".to_string(), admin())
            .await
            .unwrap();

        assert!(!response.access_key.is_empty());
        assert!(!response.secret_key.is_empty());
        assert_ne!(response.access_key, response.secret_key);
    }

    /// `key_id` carries a unique index and sigv4 looks keys up by it, so a
    /// duplicate access key has to be refused — with a message that says so,
    /// not an opaque internal error.
    #[tokio::test]
    async fn a_duplicate_access_key_is_refused() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();

        svc.create_token_with(
            "first".to_string(),
            "ns".to_string(),
            admin(),
            supplied("SHARED", "secret-one"),
            None,
        )
        .await
        .unwrap();

        let error = svc
            .create_token_with(
                "second".to_string(),
                "ns".to_string(),
                admin(),
                supplied("SHARED", "secret-two"),
                None,
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("already in use"),
            "expected a duplicate-access-key error, got: {error}"
        );
    }

    /// sigv4 carries the access key in the slash-separated credential scope, so
    /// neither half may be empty or contain whitespace or a slash.
    #[tokio::test]
    async fn unusable_credentials_are_refused() {
        let (svc, _dir) = setup().await;
        svc.create_namespace("ns", admin()).await.unwrap();

        for (access_key, secret_key) in [
            ("", "secret"),
            ("access", ""),
            ("has space", "secret"),
            ("has/slash", "secret"),
            ("access", "has space"),
        ] {
            let error = svc
                .create_token_with(
                    format!("k-{access_key}-{secret_key}"),
                    "ns".to_string(),
                    admin(),
                    supplied(access_key, secret_key),
                    None,
                )
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("key is empty")
                    || error.to_string().contains("may not contain"),
                "({access_key:?}, {secret_key:?}) should be refused, got: {error}"
            );
        }
    }
}

#[cfg(test)]
mod root_user_tests {
    use super::*;
    use crate::auth::crypto::verify_secret;
    use argon2::password_hash::PasswordHashString;
    use secrecy::SecretString;

    /// A config with the given root password, or none at all.
    fn config(db_path: &str, password: Option<&str>) -> Config {
        // Config fields are private but it derives Deserialize; absent Option
        // fields fall back to their defaults.
        let mut json = serde_json::json!({
            "db_path": db_path,
            "root_email": "admin@example.com",
        });
        if let Some(password) = password {
            json["root_password"] = password.into();
        }
        serde_json::from_value(json).unwrap()
    }

    async fn connect(cfg: Config) -> Service {
        Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap()
    }

    async fn stored_root_hash(svc: &Service) -> PasswordHashString {
        let hash: String = sqlx::query_scalar("SELECT hashed_pass FROM users WHERE email = $1")
            .bind("admin@example.com")
            .fetch_one(svc.db())
            .await
            .unwrap();
        PasswordHashString::new(&hash).unwrap()
    }

    /// A configured root password is applied on first start and re-applied
    /// (overwriting the stored hash) on every subsequent start against an
    /// existing database.
    #[actix_web::test]
    async fn root_password_is_overwritten_from_config_on_each_start() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        // First start seeds the root user with the configured password.
        let svc = connect(config(&db_path, Some("firstpassword"))).await;
        assert!(
            verify_secret(SecretString::new("firstpassword".into()), stored_root_hash(&svc).await)
                .is_ok()
        );
        drop(svc);

        // A later start with a different password overwrites the stored hash:
        // the new password verifies and the old one no longer does. (Each
        // verify consumes the hash, so re-read it for the second check.)
        let svc = connect(config(&db_path, Some("secondpassword"))).await;
        assert!(
            verify_secret(SecretString::new("secondpassword".into()), stored_root_hash(&svc).await)
                .is_ok()
        );
        assert!(
            verify_secret(SecretString::new("firstpassword".into()), stored_root_hash(&svc).await)
                .is_err()
        );
    }

    /// An empty configured password (e.g. `NERVEMQ_ROOT_PASSWORD=`) counts as
    /// not configured: the stored password is kept, and logging in with an
    /// empty password does not work.
    #[actix_web::test]
    async fn root_password_is_kept_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let svc = connect(config(&db_path, Some("firstpassword"))).await;
        drop(svc);

        let svc = connect(config(&db_path, Some(""))).await;
        assert!(
            verify_secret(SecretString::new("firstpassword".into()), stored_root_hash(&svc).await)
                .is_ok()
        );
        assert!(
            verify_secret(SecretString::new("".into()), stored_root_hash(&svc).await).is_err()
        );
    }

    /// When no root password is configured, an existing root user's stored
    /// password is left untouched on startup (rather than reset to the
    /// default), so a password set via the UI/API/CLI survives restarts.
    #[actix_web::test]
    async fn root_password_is_kept_when_unset() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        // The password is changed the way the UI, API and `nervemq user
        // passwd` change it, after a start with no configured password.
        let svc = connect(config(&db_path, None)).await;
        let changed = svc
            .set_user_password(
                Email::from_str("admin@example.com").unwrap(),
                "changed-in-ui".to_string(),
            )
            .await
            .unwrap();
        assert!(changed, "the root user should exist");
        drop(svc);

        // A later start with no configured password must not overwrite it.
        let svc = connect(config(&db_path, None)).await;
        assert!(
            verify_secret(SecretString::new("changed-in-ui".into()), stored_root_hash(&svc).await)
                .is_ok()
        );
        // The built-in default was not applied.
        assert!(
            verify_secret(SecretString::new("password".into()), stored_root_hash(&svc).await)
                .is_err()
        );
    }

    /// Connects with the SQLite key manager, whose keys are rows we can count.
    async fn connect_with_sqlite_kms(cfg: Config) -> Service {
        Service::connect_with()
            .config(cfg)
            .kms_factory(crate::kms::sqlite::SqliteKeyManager::new)
            .call()
            .await
            .unwrap()
    }

    async fn kms_key_count(svc: &Service) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM nervemq_sqlite_kms_keys")
            .fetch_one(svc.db())
            .await
            .unwrap()
    }

    /// Regression test: startup found an existing root user by calling
    /// `create_user` and catching the duplicate, after `create_user` had
    /// already stored a new KMS key, so every start and every CLI command
    /// orphaned one.
    #[actix_web::test]
    async fn restarts_do_not_mint_kms_keys() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        let svc = connect_with_sqlite_kms(config(&db_path, Some("firstpassword"))).await;
        assert_eq!(kms_key_count(&svc).await, 1);
        drop(svc);

        // With and without a configured password (the reset and keep paths).
        let svc = connect_with_sqlite_kms(config(&db_path, Some("secondpassword"))).await;
        drop(svc);
        let svc = connect_with_sqlite_kms(config(&db_path, None)).await;

        assert_eq!(kms_key_count(&svc).await, 1);
        let root_key: String =
            sqlx::query_scalar("SELECT kms_key_id FROM users WHERE email = 'admin@example.com'")
                .fetch_one(svc.db())
                .await
                .unwrap();
        let root_key_stored: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM nervemq_sqlite_kms_keys WHERE key_id = $1)",
        )
        .bind(&root_key)
        .fetch_one(svc.db())
        .await
        .unwrap();
        assert!(root_key_stored, "the one key left must be the root user's");
        // The reset path still applied the configured password.
        assert!(
            verify_secret(SecretString::new("secondpassword".into()), stored_root_hash(&svc).await)
                .is_ok()
        );
    }

    /// A `create_user` that fails (here, the email is taken) deletes the KMS
    /// key it created instead of orphaning it.
    #[actix_web::test]
    async fn a_failed_create_user_deletes_its_kms_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();
        let svc = connect_with_sqlite_kms(config(&db_path, Some("firstpassword"))).await;

        let err = svc
            .create_user(
                Email::from_str("admin@example.com").unwrap(),
                "another".to_string(),
                None,
                vec![],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Conflict { .. }),
            "expected a duplicate-email error, got {err:?}"
        );
        assert_eq!(kms_key_count(&svc).await, 1);
    }
}
