//! Message types and status management for the message queue system.
//!
//! This module defines the core message types and their lifecycle states.
//! Messages flow through the system in different states (pending, delivered, failed)
//! and can carry arbitrary key-value attributes.
//!
//! # Message Lifecycle
//!
//! 1. Messages are created in `Pending` status
//! 2. When successfully processed, they move to `Delivered`
//! 3. If processing fails repeatedly, they move to `Failed`
//!
//! Messages that fail can be moved to a dead-letter queue based on the queue's
//! redrive policy configuration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

/// Represents the current status of a message in the queue system.
///
/// The status transitions typically follow:
/// `Pending` -> `Delivered` (success case)
/// `Pending` -> `Failed`    (error case)
///
/// Messages start as `Pending` and remain in that state until they are
/// successfully processed or fail permanently.
#[derive(Serialize, Deserialize, Debug, sqlx::Type)]
#[sqlx(type_name = "text")]
pub enum MessageStatus {
    /// Message is available to be received (visibility window elapsed or never
    /// received) and has retries remaining.
    #[serde(rename = "pending")]
    #[sqlx(rename = "pending")]
    Pending,
    /// Message is currently in-flight: it has been received and is invisible to
    /// other consumers until its visibility timeout expires.
    #[serde(rename = "delivered")]
    #[sqlx(rename = "delivered")]
    Delivered,
    /// Message has exhausted its delivery attempts (`tries >= max_retries`) and
    /// will no longer be delivered.
    #[serde(rename = "failed")]
    #[sqlx(rename = "failed")]
    Failed,
}

/// Represents a message in the queue system.
///
/// Messages are the fundamental unit of data that flows through the queues.
/// Each message has a unique ID, belongs to a specific queue, and can carry
/// both a message body and key-value attributes.
///
/// Messages are stored in the database and can be tracked through their
/// lifecycle using the `status` field.
#[derive(Serialize, Deserialize, FromRow)]
pub struct Message {
    /// Unique identifier for the message
    pub id: u64,
    /// Name of the queue this message belongs to
    pub queue: String,

    /// Timestamp when the queue received (stored) the message — the
    /// equivalent of AWS SQS's SentTimestamp. `None` only for rows created
    /// before the column existed (migration 0006).
    pub received_at: Option<u64>,
    /// Timestamp when the message was most recently delivered (if ever).
    /// Informational only — availability is governed by `invisible_until`.
    pub delivered_at: Option<u64>,
    /// Timestamp until which the message is invisible to consumers. The message
    /// is available when this is `NULL` or in the past (`<= now`).
    pub invisible_until: Option<u64>,
    /// Receipt handle issued on the most recent receive. A `DeleteMessage` only
    /// succeeds when it presents this handle.
    pub receipt_handle: Option<String>,
    /// ID of the user who sent the message
    pub sent_by: Option<u64>,
    /// The actual message content
    pub body: String,
    /// Number of delivery attempts made
    pub tries: u64,

    /// Current status of the message
    pub status: MessageStatus,

    #[sqlx(skip)]
    /// Arbitrary key-value pairs associated with the message
    pub kv: HashMap<String, String>,
}
