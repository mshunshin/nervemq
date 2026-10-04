//! AWS SQS-compatible API types and data structures.
//!
//! This module defines the request and response types for implementing
//! an AWS SQS-compatible API interface. It includes all the major SQS
//! operations like:
//!
//! - Queue management (create, delete, list, purge)
//! - Message operations (send, receive, delete)
//! - Queue attribute management
//! - Queue tagging
//! - Batch operations
//!
//! Each operation is organized in its own submodule with corresponding
//! request and response types that match the AWS SQS API specification.
//!
//! # Message Attributes
//!
//! The system supports three types of message attributes:
//! - String values
//! - Number values (stored as strings)
//! - Binary values
//!
//! # API Compatibility
//!
//! The types in this module are designed to be wire-compatible with the
//! AWS SQS API, using the same field names and serialization formats.

use bytes::BufMut;
use std::collections::{HashMap, HashSet};
use url::Url;

/// Types for the SendMessage API operation.
///
/// Handles sending a single message to a queue with optional
/// attributes and delivery delay settings.
pub mod send_message {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the SendMessage operation.
    pub struct SendMessageRequest {
        pub queue_url: Url,
        pub message_body: String,
        pub delay_seconds: Option<u64>,
        #[serde(default)]
        pub message_attributes: HashMap<String, SqsMessageAttribute>,
        /// Only `AWSTraceHeader` (see [`super::trace_header`]).
        #[serde(default)]
        pub message_system_attributes: HashMap<String, SqsMessageAttribute>,
        pub message_deduplication_id: Option<String>,
        pub message_group_id: Option<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for the SendMessage operation.
    pub struct SendMessageResponse {
        /// AWS wire format: `MessageId` is a string.
        pub message_id: String,

        #[serde(rename = "MD5OfMessageBody")]
        pub md5_of_message_body: String,

        /// Omitted when the message has no attributes, as AWS does.
        #[serde(
            rename = "MD5OfMessageAttributes",
            skip_serializing_if = "Option::is_none"
        )]
        pub md5_of_message_attributes: Option<String>,

        /// Omitted when the request set no system attributes.
        #[serde(
            rename = "MD5OfMessageSystemAttributes",
            skip_serializing_if = "Option::is_none"
        )]
        pub md5_of_message_system_attributes: Option<String>,
        // pub sequence_number: Option<String>,
    }
}

/// Types for the GetQueueUrl API operation.
///
/// Retrieves the URL of a queue given its name. The URL is required
/// for most other queue operations.
pub mod get_queue_url {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the GetQueueUrl operation.
    pub struct GetQueueUrlRequest {
        pub queue_name: String,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for the GetQueueUrl operation.
    pub struct GetQueueUrlResponse {
        pub queue_url: Url,
    }
}

/// Types for the CreateQueue API operation.
///
/// Handles queue creation with configurable attributes and tags.
/// Creates a new queue or returns the URL of an existing queue with
/// the same name.
pub mod create_queue {
    use super::*;
    use crate::service::QueueAttributeMap;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the CreateQueue operation.
    pub struct CreateQueueRequest {
        pub queue_name: String,
        /// As sent; the service checks the names and values, and stores them
        /// under the same keys `SetQueueAttributes` writes.
        #[serde(default)]
        pub attributes: QueueAttributeMap,
        /// AWS's JSON protocol sends this member as lowercase `tags` (a
        /// documented quirk unique to CreateQueue); accept both spellings.
        #[serde(default, alias = "tags")]
        pub tags: HashMap<String, String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for the CreateQueue operation.
    pub struct CreateQueueResponse {
        pub queue_url: Url,
    }
}

/// Types for the ListQueues API operation.
///
/// Returns a list of queue URLs, optionally filtered by a name prefix.
/// Useful for discovering existing queues in the system.
pub mod list_queues {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the ListQueues operation.
    pub struct ListQueuesRequest {
        pub queue_name_prefix: Option<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for the ListQueues operation.
    pub struct ListQueuesResponse {
        pub queue_urls: Vec<Url>,
    }
}

/// Types for the ChangeMessageVisibility API operation.
///
/// Changes the visibility timeout of an in-flight message. The new timeout
/// is counted from the time of the call, not from when the message was
/// received.
pub mod change_message_visibility {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the ChangeMessageVisibility operation.
    pub struct ChangeMessageVisibilityRequest {
        pub queue_url: Url,
        pub receipt_handle: String,
        /// New visibility timeout in seconds (0 to 43200), starting now.
        pub visibility_timeout: u64,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the ChangeMessageVisibility operation.
    pub struct ChangeMessageVisibilityResponse {}
}

/// Types for the DeleteMessage API operation.
///
/// Deletes a specific message from a queue using its receipt handle.
/// The receipt handle is obtained when receiving the message.
pub mod delete_message {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the DeleteMessage operation.
    pub struct DeleteMessageRequest {
        pub queue_url: Url,
        pub receipt_handle: String,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the DeleteMessage operation.
    pub struct DeleteMessageResponse {}
}

/// Types for the DeleteQueue API operation.
///
/// Permanently deletes a queue and all its messages. This operation
/// cannot be undone.
pub mod delete_queue {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the DeleteQueue operation.
    pub struct DeleteQueueRequest {
        pub queue_url: Url,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the DeleteQueue operation.
    pub struct DeleteQueueResponse {}
}

/// Types for the PurgeQueue API operation.
///
/// Deletes all messages from a queue while retaining the queue itself.
/// Useful for clearing a queue without deleting its configuration.
pub mod purge_queue {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the PurgeQueue operation.
    pub struct PurgeQueueRequest {
        pub queue_url: Url,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the PurgeQueue operation.
    ///
    /// Contains a success flag indicating if the operation was successful.
    pub struct PurgeQueueResponse {
        pub success: bool,
    }
}

/// Types for the GetQueueAttributes API operation.
///
/// Retrieves one or more attributes of a queue. Attributes include
/// settings like delay seconds, message retention period, and
/// visibility timeout.
pub mod get_queue_attributes {
    use std::collections::BTreeMap;

    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the GetQueueAttributes operation.
    ///
    /// Contains the queue URL and a list of attribute names to retrieve.
    pub struct GetQueueAttributesRequest {
        pub queue_url: Url,
        #[serde(default)]
        pub attribute_names: Vec<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for the GetQueueAttributes operation.
    ///
    /// Contains the requested attributes for the queue.
    pub struct GetQueueAttributesResponse {
        /// Left out when no attributes were asked for, as AWS does.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub attributes: Option<BTreeMap<String, String>>,
    }
}

/// Types for the ReceiveMessage API operation.
///
/// Handles retrieving one or more messages from a queue with
/// configurable visibility timeout and wait time settings.
pub mod receive_message {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for the ReceiveMessage operation.
    ///
    /// Contains the queue URL and various options for message retrieval.
    pub struct ReceiveMessageRequest {
        pub queue_url: Url,

        /// System attribute names to return (`SentTimestamp`,
        /// `ApproximateReceiveCount`, ... or `All`). AWS deprecated
        /// `AttributeNames` in favor of `MessageSystemAttributeNames`, and
        /// returns the attributes either one names.
        #[serde(default)]
        pub attribute_names: Vec<String>,
        #[serde(default)]
        pub message_system_attribute_names: Vec<String>,

        #[serde(default)]
        pub message_attribute_names: Vec<String>,

        pub max_number_of_messages: Option<u64>,
        pub visibility_timeout: Option<u64>,
        pub wait_time_seconds: Option<u64>,
        pub receive_request_attempt_id: Option<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for the ReceiveMessage operation.
    ///
    /// Contains a list of messages retrieved from the queue.
    pub struct ReceiveMessageResponse {
        /// Left out when there are none, as AWS does.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub messages: Vec<SqsMessage>,
    }
}

/// Types for the SendMessageBatch API operation.
///
/// Sends multiple messages to a queue in a single request.
/// More efficient than sending messages individually for bulk operations.
/// Supports up to 10 messages per request.
pub mod send_message_batch {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for a batch message send operation.
    ///
    /// Contains the queue URL and a list of message entries to send.
    pub struct SendMessageBatchRequest {
        pub queue_url: Url,
        /// Missing reads as empty, which [`crate::sqs::limits::check_batch`]
        /// refuses as AWS does: an SDK may leave out an empty list.
        #[serde(default)]
        pub entries: Vec<SendMessageBatchRequestEntry>,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Entry for a batch message send request.
    ///
    /// Each entry represents a single message to be sent as part of
    /// a batch operation, with its own ID and attributes.
    pub struct SendMessageBatchRequestEntry {
        pub id: String,
        pub message_body: String,
        pub delay_seconds: Option<u64>,
        #[serde(default)]
        pub message_attributes: HashMap<String, SqsMessageAttribute>,
        /// Only `AWSTraceHeader` (see [`super::trace_header`]).
        #[serde(default)]
        pub message_system_attributes: HashMap<String, SqsMessageAttribute>,
        pub message_deduplication_id: Option<String>,
        pub message_group_id: Option<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Successful result entry for a batch message send operation.
    ///
    /// Contains the ID of the successfully sent message along with
    /// its message ID and MD5 hash for verification.
    pub struct SendMessageBatchResultEntry {
        pub id: String,
        pub message_id: String,
        #[serde(rename = "MD5OfMessageBody")]
        pub md5_of_message_body: String,
        #[serde(
            rename = "MD5OfMessageAttributes",
            skip_serializing_if = "Option::is_none"
        )]
        pub md5_of_message_attributes: Option<String>,
        #[serde(
            rename = "MD5OfMessageSystemAttributes",
            skip_serializing_if = "Option::is_none"
        )]
        pub md5_of_message_system_attributes: Option<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Error result entry for a batch message send operation.
    ///
    /// Contains details about why a particular message in the batch
    /// failed to be sent, including error code and message.
    pub struct SendMessageBatchResultErrorEntry {
        pub id: String,
        pub sender_fault: bool,
        pub code: String,
        pub message: Option<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for a batch message send operation.
    ///
    /// Contains lists of successful and failed messages.
    pub struct SendMessageBatchResponse {
        pub successful: Vec<SendMessageBatchResultEntry>,
        pub failed: Vec<SendMessageBatchResultErrorEntry>,
    }
}

/// Types for the ListQueueTags API operation.
///
/// Lists all tags associated with a queue. Tags are key-value pairs
/// that can be used to categorize and organize queues.
pub mod list_queue_tags {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for listing tags on a queue.
    pub struct ListQueueTagsRequest {
        pub queue_url: Url,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for listing tags on a queue.
    pub struct ListQueueTagsResponse {
        /// Left out when the queue has none, as AWS does.
        #[serde(skip_serializing_if = "HashMap::is_empty")]
        pub tags: HashMap<String, String>,
    }
}

/// Types for the TagQueue API operation.
///
/// Adds or updates tags on a queue. Tags are metadata that can be
/// attached to queues for organization and billing purposes.
pub mod tag_queue {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for adding tags to a queue
    pub struct TagQueueRequest {
        pub queue_url: Url,
        pub tags: HashMap<String, String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the TagQueue operation.
    pub struct TagQueueResponse {}
}

/// Types for the UntagQueue API operation.
///
/// Removes specified tags from a queue. Only the tag keys need to
/// be provided to remove the corresponding tags.
pub mod untag_queue {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for removing tags from a queue.
    pub struct UntagQueueRequest {
        pub queue_url: Url,
        pub tag_keys: Vec<String>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the UntagQueue operation.
    pub struct UntagQueueResponse {}
}

/// Types for the SetQueueAttributes API operation.
///
/// Sets one or more attributes of a queue. Can modify settings like
/// message retention period, visibility timeout, and dead-letter queue
/// configuration.
pub mod set_queue_attributes {
    use crate::service::QueueAttributeMap;

    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for setting queue attributes.
    pub struct SetQueueAttributesRequest {
        pub queue_url: Url,
        pub attributes: QueueAttributeMap,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Empty response for the SetQueueAttributes operation.
    pub struct SetQueueAttributesResponse {}
}

/// Types for the DeleteMessageBatch API operation.
///
/// Deletes multiple messages from a queue in a single request.
/// More efficient than deleting messages individually when processing
/// multiple messages. Supports up to 10 deletions per request.
pub mod delete_message_batch {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Entry for a batch message delete request.
    ///
    /// Each entry identifies a message to be deleted using its
    /// receipt handle and a client-provided ID for tracking.
    pub struct DeleteMessageBatchRequestEntry {
        pub id: String,
        pub receipt_handle: String,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for a batch message delete operation.
    ///
    /// Contains the queue URL and a list of message entries to delete.
    pub struct DeleteMessageBatchRequest {
        pub queue_url: Url,
        /// Missing reads as empty, which [`crate::sqs::limits::check_batch`]
        /// refuses as AWS does: an SDK may leave out an empty list.
        #[serde(default)]
        pub entries: Vec<DeleteMessageBatchRequestEntry>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Successful result entry for a batch message delete operation.
    ///
    /// Contains the ID of the successfully deleted message for correlation
    /// with the original request.
    pub struct DeleteMessageBatchResultSuccess {
        pub id: String,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Error result entry for a batch message delete operation.
    ///
    /// Contains details about why a particular message in the batch
    /// failed to be deleted, including error code and message.
    pub struct DeleteMessageBatchResultError {
        pub code: String,
        pub id: String,
        pub message: String,
        pub sender_fault: bool,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for a batch message delete operation.
    /// Contains lists of successful and failed messages.
    pub struct DeleteMessageBatchResponse {
        pub failed: Vec<DeleteMessageBatchResultError>,
        pub successful: Vec<DeleteMessageBatchResultSuccess>,
    }
}

/// Types for the ChangeMessageVisibilityBatch API operation.
///
/// Changes the visibility timeout of multiple in-flight messages in a
/// single request. Each entry succeeds or fails independently, under the
/// same rules as ChangeMessageVisibility.
pub mod change_message_visibility_batch {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Entry for a batch visibility change request.
    ///
    /// Identifies an in-flight message by its receipt handle, with a
    /// client-provided ID for correlating the per-entry result.
    pub struct ChangeMessageVisibilityBatchRequestEntry {
        pub id: String,
        pub receipt_handle: String,
        /// New visibility timeout in seconds (0 to 43200), starting now.
        pub visibility_timeout: u64,
    }

    #[derive(Debug, serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    /// Request for a batch visibility change operation.
    pub struct ChangeMessageVisibilityBatchRequest {
        pub queue_url: Url,
        /// Missing reads as empty, which [`crate::sqs::limits::check_batch`]
        /// refuses as AWS does: an SDK may leave out an empty list.
        #[serde(default)]
        pub entries: Vec<ChangeMessageVisibilityBatchRequestEntry>,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Successful result entry for a batch visibility change operation.
    pub struct ChangeMessageVisibilityBatchResultSuccess {
        pub id: String,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Error result entry for a batch visibility change operation.
    pub struct ChangeMessageVisibilityBatchResultError {
        pub code: String,
        pub id: String,
        pub message: String,
        pub sender_fault: bool,
    }

    #[derive(Debug, serde::Serialize)]
    #[serde(rename_all = "PascalCase")]
    /// Response for a batch visibility change operation.
    /// Contains lists of successful and failed entries.
    pub struct ChangeMessageVisibilityBatchResponse {
        pub failed: Vec<ChangeMessageVisibilityBatchResultError>,
        pub successful: Vec<ChangeMessageVisibilityBatchResultSuccess>,
    }
}

/// Represents a message attribute in SQS format.
///
/// Message attributes can be one of three types:
/// - String: Text data
/// - Number: Numeric values stored as strings
/// - Binary: Raw binary data
///
/// This matches the AWS SQS message attribute format exactly for compatibility.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase", tag = "DataType")]
pub enum SqsMessageAttribute {
    String {
        #[serde(rename = "StringValue")]
        string_value: String,
    },
    Number {
        #[serde(rename = "StringValue")]
        string_value: String,
    },
    Binary {
        #[serde(rename = "BinaryValue", with = "base64_bytes")]
        binary_value: Vec<u8>,
    },
}

/// (De)serializes binary attribute values in the AWS JSON wire format, where
/// blobs are base64-encoded strings. JSON byte arrays are also accepted on
/// input for compatibility with values stored before this encoding existed.
mod base64_bytes {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Base64(String),
            Bytes(Vec<u8>),
        }

        match Raw::deserialize(d)? {
            Raw::Base64(s) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(serde::de::Error::custom),
            Raw::Bytes(b) => Ok(b),
        }
    }
}

/// Maximum allowed individual message size, and maximum total payload size of
/// a batch (the sum of the individual lengths of all batched messages):
/// 1 MiB, per current AWS SQS policy. A message's size is its body plus, for
/// each message attribute, the name, the data type label and the value.
pub const MAX_MESSAGE_SIZE_BYTES: usize = 1_048_576;

/// The only message system attribute a sender can set: the message's trace
/// context, in X-Ray format (`Root=1-…;Parent=…;Sampled=…`).
pub const AWS_TRACE_HEADER: &str = "AWSTraceHeader";

/// NerveMQ's cap on an `AWSTraceHeader`. System attributes don't count
/// towards a message's size, so without a cap of its own one could carry
/// what the size limit refuses.
pub const MAX_AWS_TRACE_HEADER_BYTES: usize = 4096;

/// The `AWSTraceHeader` a send sets, if any. Any other system attribute, or
/// one that isn't a non-empty `String` within the cap, is refused, as AWS
/// does.
pub fn trace_header(
    system_attributes: &HashMap<String, SqsMessageAttribute>,
) -> Result<Option<&str>, String> {
    let mut header = None;
    for (name, attribute) in system_attributes {
        if name != AWS_TRACE_HEADER {
            return Err(format!(
                "MessageSystemAttributes: {name} cannot be set; the only message system \
                 attribute is {AWS_TRACE_HEADER}"
            ));
        }
        let SqsMessageAttribute::String { string_value } = attribute else {
            return Err(format!(
                "MessageSystemAttributes: {AWS_TRACE_HEADER} must have DataType String"
            ));
        };
        if string_value.is_empty() || string_value.len() > MAX_AWS_TRACE_HEADER_BYTES {
            return Err(format!(
                "MessageSystemAttributes: {AWS_TRACE_HEADER} must be 1 to \
                 {MAX_AWS_TRACE_HEADER_BYTES} bytes, got {}",
                string_value.len()
            ));
        }
        header = Some(string_value.as_str());
    }
    Ok(header)
}

/// A `String` message attribute's value.
pub fn string_attribute<'a>(
    attributes: &'a HashMap<String, SqsMessageAttribute>,
    name: &str,
) -> Option<&'a str> {
    match attributes.get(name)? {
        SqsMessageAttribute::String { string_value } => Some(string_value),
        _ => None,
    }
}

/// Whether a receive's `MessageAttributeNames` asks for the attribute
/// `name`, as AWS reads them: `All`, `.*` and `*` ask for every attribute, a
/// name ending in `.*` for those starting with what precedes it (`Hel.*`
/// asks for `Hello` and `Help.Me`), and any other for itself. A name no
/// attribute could have simply matches nothing.
pub fn message_attribute_wanted(requested: &HashSet<String>, name: &str) -> bool {
    requested.iter().any(|pattern| match pattern.as_str() {
        "All" | ".*" | "*" => true,
        pattern => match pattern.strip_suffix(".*") {
            Some(prefix) => !prefix.is_empty() && name.starts_with(prefix),
            None => pattern == name,
        },
    })
}

/// AWS's `MD5OfMessageAttributes` (and `MD5OfMessageSystemAttributes`): the
/// MD5 of every attribute's encoding ([`SqsMessageAttribute::serialize_into`])
/// in order of name. `None` when there are no attributes, as AWS then omits
/// the field.
///
/// SDKs that check it (the Java SDK does) reject a reply whose digest
/// doesn't match. The order matters: this was once computed in a
/// `HashMap`'s order, which is random.
pub fn attributes_md5<'a>(
    attributes: impl IntoIterator<Item = (&'a String, &'a SqsMessageAttribute)>,
) -> Option<String> {
    let mut sorted: Vec<_> = attributes.into_iter().collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_by_key(|(name, _)| *name);

    let mut encoded = Vec::new();
    for (name, attribute) in sorted {
        attribute.serialize_into(name, &mut encoded);
    }
    Some(hex::encode(md5::compute(&encoded).as_ref()))
}

/// Computes a message's size as AWS counts it: body bytes plus, per
/// attribute, the name, data type label and value bytes.
pub fn message_size(
    body: &str,
    attributes: &HashMap<String, SqsMessageAttribute>,
) -> usize {
    body.len()
        + attributes
            .iter()
            .map(|(name, attr)| name.len() + attr.value_size())
            .sum::<usize>()
}

impl SqsMessageAttribute {
    pub fn data_type(&self) -> &'static str {
        match self {
            SqsMessageAttribute::String { .. } => "String",
            SqsMessageAttribute::Number { .. } => "Number",
            SqsMessageAttribute::Binary { .. } => "Binary",
        }
    }

    /// Bytes this attribute contributes to its message's size (data type
    /// label plus value; the attribute name is counted by the caller).
    pub fn value_size(&self) -> usize {
        self.data_type().len()
            + match self {
                SqsMessageAttribute::String { string_value }
                | SqsMessageAttribute::Number { string_value } => string_value.len(),
                SqsMessageAttribute::Binary { binary_value } => binary_value.len(),
            }
    }

    /// Serializes the attributes in the expected binary format for SQS attributes.
    ///
    /// [key length (4 bytes)][key bytes][type (1 byte)][value length (4 bytes)][value bytes]
    pub fn serialize(&self, key: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize_into(key, &mut buf);
        buf
    }

    /// Serializes the attributes in the expected binary format for SQS attributes, writing the
    /// results to the buffer specified in `buf`.
    ///
    /// [key length (4 bytes)][key bytes][type (1 byte)][value length (4 bytes)][value bytes]
    pub fn serialize_into(&self, key: &str, buf: &mut Vec<u8>) {
        let k_bytes = key.as_bytes();

        buf.put_u32(k_bytes.len() as u32);
        buf.put_slice(k_bytes);

        let t_bytes = self.data_type().as_bytes();
        buf.put_u32(t_bytes.len() as u32);
        buf.put_slice(t_bytes);

        match self {
            SqsMessageAttribute::String { string_value }
            | SqsMessageAttribute::Number { string_value } => {
                let v_bytes = string_value.as_bytes();
                buf.put_u8(1); // Type 1 is string (or number)

                buf.put_u32(v_bytes.len() as u32);
                buf.put_slice(v_bytes);
            }
            SqsMessageAttribute::Binary { binary_value } => {
                let v_bytes = binary_value.as_slice();
                buf.put_u8(2); // Type 2 is binary

                buf.put_u32(v_bytes.len() as u32);
                buf.put_slice(v_bytes);
            }
        };
    }
}

#[test]
fn test_sqs_message_attribute() {
    let attr = SqsMessageAttribute::String {
        string_value: "hello".to_string(),
    };
    let json = serde_json::to_string(&attr).unwrap();
    assert_eq!(json, r#"{"DataType":"String","StringValue":"hello"}"#);
    let attr = SqsMessageAttribute::Number {
        string_value: "123".to_string(),
    };
    let json = serde_json::to_string(&attr).unwrap();
    assert_eq!(json, r#"{"DataType":"Number","StringValue":"123"}"#);
    // Binary values travel base64-encoded, matching the AWS JSON protocol.
    let attr = SqsMessageAttribute::Binary {
        binary_value: b"TEST".to_vec(),
    };
    let json = serde_json::to_string(&attr).unwrap();
    assert_eq!(json, r#"{"DataType":"Binary","BinaryValue":"VEVTVA=="}"#);
    let attr: SqsMessageAttribute = serde_json::from_str(&json).unwrap();
    assert!(matches!(
        &attr,
        SqsMessageAttribute::Binary { binary_value } if binary_value == b"TEST"
    ));
    // Legacy byte-array form (pre-base64 stored values) still deserializes.
    let attr: SqsMessageAttribute =
        serde_json::from_str(r#"{"DataType":"Binary","BinaryValue":[84,69,83,84]}"#).unwrap();
    assert!(matches!(
        &attr,
        SqsMessageAttribute::Binary { binary_value } if binary_value == b"TEST"
    ));

    let attr: SqsMessageAttribute =
        serde_json::from_str(r#"{"DataType":"String","StringValue":"hello"}"#).unwrap();
    assert!(matches!(attr, SqsMessageAttribute::String { .. }),);
}

/// AWS SDKs omit optional map/list fields entirely when empty, so requests must
/// deserialize without them (regression test: a missing `MessageAttributes`
/// used to fail deserialization and surface as a 500).
#[test]
fn test_optional_fields_default_when_omitted() {
    let req: send_message::SendMessageRequest = serde_json::from_str(
        r#"{"QueueUrl":"http://localhost:8080/api/sqs/ns/q","MessageBody":"hello"}"#,
    )
    .unwrap();
    assert!(req.message_attributes.is_empty());

    let req: send_message_batch::SendMessageBatchRequest = serde_json::from_str(
        r#"{"QueueUrl":"http://localhost:8080/api/sqs/ns/q","Entries":[{"Id":"1","MessageBody":"hello"}]}"#,
    )
    .unwrap();
    assert!(req.entries[0].message_attributes.is_empty());

    let req: get_queue_attributes::GetQueueAttributesRequest =
        serde_json::from_str(r#"{"QueueUrl":"http://localhost:8080/api/sqs/ns/q"}"#).unwrap();
    assert!(req.attribute_names.is_empty());
}

/// Represents a message in SQS format.
///
/// Contains all the standard SQS message fields including:
/// - Message ID and receipt handle for tracking
/// - Message body and MD5 hash
/// - Standard attributes
/// - Custom message attributes
///
/// This structure is used when returning messages to clients in the
/// SQS-compatible API format.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SqsMessage {
    pub message_id: String,
    pub receipt_handle: String,
    #[serde(rename = "MD5OfBody")]
    pub md5_of_body: String,
    pub body: String,

    // pub md5_of_system_attributes: String,
    /// System attributes (SentTimestamp, ApproximateReceiveCount, ...),
    /// omitted entirely when none were requested — AWS drops the empty map.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, String>,

    /// Over the returned attributes only; omitted when none are returned.
    #[serde(
        rename = "MD5OfMessageAttributes",
        skip_serializing_if = "Option::is_none"
    )]
    pub md5_of_message_attributes: Option<String>,
    /// Message attributes, filtered to the requested names; like the system
    /// attributes above, AWS omits the map when none were requested.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub message_attributes: HashMap<String, SqsMessageAttribute>,
}

/// Represents all possible SQS API response types.
///
/// This enum encompasses every possible response type that can be
/// returned from an SQS API operation. The serialization is untagged
/// to match the AWS SQS wire format.
///
/// Each variant corresponds to a specific API operation response,
/// maintaining compatibility with the AWS SQS API specification.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "PascalCase", untagged)]
pub enum SqsResponse {
    ChangeMessageVisibility(change_message_visibility::ChangeMessageVisibilityResponse),
    SendMessage(send_message::SendMessageResponse),
    GetQueueUrl(get_queue_url::GetQueueUrlResponse),
    CreateQueue(create_queue::CreateQueueResponse),
    ListQueues(list_queues::ListQueuesResponse),
    DeleteMessage(delete_message::DeleteMessageResponse),
    PurgeQueue(purge_queue::PurgeQueueResponse),
    DeleteQueue(delete_queue::DeleteQueueResponse),
    GetQueueAttributes(get_queue_attributes::GetQueueAttributesResponse),
    ReceiveMessage(receive_message::ReceiveMessageResponse),
    SendMessageBatch(send_message_batch::SendMessageBatchResponse),
    ListQueueTags(list_queue_tags::ListQueueTagsResponse),
    TagQueue(tag_queue::TagQueueResponse),
    UntagQueue(untag_queue::UntagQueueResponse),
    SetQueueAttributes(set_queue_attributes::SetQueueAttributesResponse),
    DeleteMessageBatch(delete_message_batch::DeleteMessageBatchResponse),
    ChangeMessageVisibilityBatch(
        change_message_visibility_batch::ChangeMessageVisibilityBatchResponse,
    ),
}

#[cfg(test)]
mod attribute_digest_tests {
    use super::*;

    #[test]
    fn message_attribute_names_match_as_aws_matches_them() {
        // The names and patterns AWS's own responses were recorded with.
        let wanted = |patterns: &[&str], name: &str| {
            let patterns = patterns.iter().map(|p| p.to_string()).collect();
            message_attribute_wanted(&patterns, name)
        };
        for name in ["General", "Hello", "Help.Me"] {
            for all in ["All", ".*", "*"] {
                assert!(wanted(&[all], name), "{all} {name}");
            }
        }
        assert!(wanted(&["Hel.*"], "Hello"));
        assert!(wanted(&["Hel.*"], "Help.Me"));
        assert!(!wanted(&["Hel.*"], "General"));
        assert!(wanted(&["Foo", "Hello"], "Hello"));
        assert!(!wanted(&["Foo", "Help"], "Hello"));
        assert!(!wanted(&["Hello"], "Help.Me"));
        assert!(!wanted(&[], "Hello"));
        // Names no attribute can have match nothing, rather than failing.
        for illegal in ["AWS.", "..foo"] {
            assert!(!wanted(&[illegal], "Hello"), "{illegal}");
        }
    }

    fn number(value: &str) -> SqsMessageAttribute {
        SqsMessageAttribute::Number {
            string_value: value.to_owned(),
        }
    }

    fn string(value: &str) -> SqsMessageAttribute {
        SqsMessageAttribute::String {
            string_value: value.to_owned(),
        }
    }

    fn md5_of(attributes: Vec<(&str, SqsMessageAttribute)>) -> Option<String> {
        let attributes: Vec<(String, SqsMessageAttribute)> = attributes
            .into_iter()
            .map(|(name, attribute)| (name.to_owned(), attribute))
            .collect();
        attributes_md5(attributes.iter().map(|(name, attribute)| (name, attribute)))
    }

    /// Digests that moto's SQS tests (tests/test_sqs/test_sqs.py) pin as
    /// what AWS returns for these attributes.
    #[test]
    fn single_attributes_digest_as_on_aws() {
        for (name, value, digest) in [
            ("timestamp", "1493147359900", "235c5c510d26fb653d073faed50ae77c"),
            ("timestamp", "1493147359901", "994258b45346a2cc3f9cbb611aa7af30"),
            ("SOME_Valid.attribute-Name", "1493147359900", "36655e7e9d7c0e8479fa3f3f42247ae7"),
        ] {
            assert_eq!(md5_of(vec![(name, number(value))]).as_deref(), Some(digest), "{name}");
        }
    }

    /// Several attributes digest in order of name, whatever order they come
    /// in. The expected value is from moto's implementation of the algorithm
    /// (`Message.attribute_md5` in moto/sqs/models.py).
    #[test]
    fn attributes_digest_in_order_of_name() {
        let traceparent = || string("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        let count = || number("42");
        let blob = || SqsMessageAttribute::Binary {
            binary_value: vec![0, 1, 2, 255],
        };
        for attributes in [
            vec![("traceparent", traceparent()), ("count", count()), ("blob", blob())],
            vec![("blob", blob()), ("count", count()), ("traceparent", traceparent())],
            vec![("count", count()), ("traceparent", traceparent()), ("blob", blob())],
        ] {
            assert_eq!(
                md5_of(attributes).as_deref(),
                Some("ff985ad1603ea377a2934ea42c7253c7")
            );
        }
    }

    #[test]
    fn no_attributes_have_no_digest() {
        assert_eq!(md5_of(vec![]), None);
    }

    #[test]
    fn only_a_string_aws_trace_header_is_accepted() {
        let header = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
        let system = |name: &str, attribute| HashMap::from([(name.to_owned(), attribute)]);

        let set = system(AWS_TRACE_HEADER, string(header));
        assert_eq!(trace_header(&set), Ok(Some(header)));
        assert_eq!(trace_header(&HashMap::new()), Ok(None));
        // MD5OfMessageSystemAttributes uses the same digest (moto's
        // implementation gives this value).
        assert_eq!(
            attributes_md5(&set).as_deref(),
            Some("5ae4d5d7636402d80f4eb6d213245a88")
        );

        for refused in [
            system("SenderId", string("someone")),
            system(AWS_TRACE_HEADER, number("1")),
            system(AWS_TRACE_HEADER, string("")),
            system(AWS_TRACE_HEADER, string(&"x".repeat(MAX_AWS_TRACE_HEADER_BYTES + 1))),
        ] {
            assert!(trace_header(&refused).is_err());
        }
        let at_the_cap = system(AWS_TRACE_HEADER, string(&"x".repeat(MAX_AWS_TRACE_HEADER_BYTES)));
        assert!(trace_header(&at_the_cap).is_ok());
    }
}
