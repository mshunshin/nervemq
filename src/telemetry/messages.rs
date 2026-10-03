//! What the server records about messages as they pass through it.
//!
//! - **Metrics:** sends, deliveries, removals and visibility changes; how
//!   long messages wait and live; per-queue gauges.
//! - **Traces:** the request span links each message it handles to the
//!   context the message was created in. A consumer's receive then shows
//!   which sends its messages came from, and a send which deliveries
//!   followed (docs/architecture/observability.md#message-traces).
//!
//! Each method takes identifiers and timings, never content. A message's
//! state changes only when a request or the retention sweep acts on it:
//! a lapsed visibility window or an exhausted retry count runs no code (see
//! docs/architecture/message-lifecycle.md). So those show up in the gauges,
//! not as events.

use super::Telemetry;

/// The queue an event happened on, as metrics name it.
// Read only by the exporter, which the `otel` feature builds.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
#[derive(Clone, Copy, Debug)]
pub struct Queue<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
}

/// A message at an event, as the statement that changed it returned it.
// Read only by the exporter, which the `otel` feature builds.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
#[derive(Clone, Debug, Default)]
pub struct MessageFacts {
    pub id: u64,
    /// Deliveries so far, the current one included.
    pub tries: u64,
    /// When the queue stored it, in unix milliseconds (AWS's
    /// `SentTimestamp`).
    pub sent_at_ms: Option<u64>,
    /// Its `AWSTraceHeader`: the context it was created in.
    pub trace_header: Option<String>,
    /// Its `traceparent` attribute, where the event sees attributes. It
    /// names the creation context of a message sent while traces weren't
    /// exported, which therefore has no stored header.
    pub traceparent: Option<String>,
}

/// A message stored by a send.
// Read only by the exporter, which the `otel` feature builds.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
#[derive(Clone, Debug, Default)]
pub struct SentMessage {
    pub id: u64,
    pub body_bytes: usize,
    /// The creation context the sender gave the message itself
    /// (`AWSTraceHeader`, or a `traceparent` attribute), if any. Linked
    /// from the request span; a message without one was created in the
    /// request's own trace.
    pub own_trace_header: Option<String>,
    pub own_traceparent: Option<String>,
}

/// Why a message left its queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Removal {
    /// `DeleteMessage`: its consumer is done with it.
    Acknowledged,
    /// Deleted by id from the admin UI or API.
    Admin,
    /// `PurgeQueue`.
    Purged,
    /// Outlived its queue's `MessageRetentionPeriod`.
    Expired,
    /// Cleared with the queue's other exhausted (`failed`) messages.
    FailedCleared,
}

impl Removal {
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    pub fn label(self) -> &'static str {
        match self {
            Removal::Acknowledged => "acknowledged",
            Removal::Admin => "admin",
            Removal::Purged => "purged",
            Removal::Expired => "expired",
            Removal::FailedCleared => "failed_cleared",
        }
    }
}

/// A `ChangeMessageVisibility`: a timeout of 0 hands the message back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VisibilityChange {
    Release,
    Extend,
}

impl VisibilityChange {
    pub fn of(timeout_seconds: u64) -> Self {
        if timeout_seconds == 0 {
            VisibilityChange::Release
        } else {
            VisibilityChange::Extend
        }
    }

    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    pub fn label(self) -> &'static str {
        match self {
            VisibilityChange::Release => "release",
            VisibilityChange::Extend => "extend",
        }
    }
}

/// One queue in the gauge snapshot. Its states partition its messages.
#[derive(Clone, Debug, Default, PartialEq, Eq, sqlx::FromRow)]
pub struct QueueGauge {
    pub namespace: String,
    pub queue: String,
    pub available: u64,
    pub in_flight: u64,
    /// Not yet visible after `DelaySeconds`, and never delivered.
    pub delayed: u64,
    /// Exhausted its delivery attempts.
    pub failed: u64,
    /// When the oldest available message was sent (unix seconds).
    pub oldest_available_at: Option<u64>,
    pub paused: bool,
}

/// Now, in unix milliseconds, as `sent_at_ms` is stored.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

impl Telemetry {
    pub(crate) fn sent(&self, queue: Queue<'_>, messages: &[SentMessage]) {
        #[cfg(feature = "otel")]
        {
            if let Some(instruments) = &self.instruments {
                instruments.sent(queue, messages);
            }
            for message in messages {
                super::otel::messages::link(
                    message.own_trace_header.as_deref(),
                    message.own_traceparent.as_deref(),
                    message.id,
                    None,
                );
            }
        }
        let _ = (queue, messages);
    }

    pub(crate) fn delivered(&self, queue: Queue<'_>, messages: &[MessageFacts]) {
        #[cfg(feature = "otel")]
        {
            if let Some(instruments) = &self.instruments {
                instruments.delivered(queue, messages, now_ms());
            }
            for message in messages {
                super::otel::messages::link(
                    message.trace_header.as_deref(),
                    message.traceparent.as_deref(),
                    message.id,
                    Some(message.tries),
                );
            }
        }
        let _ = (queue, messages);
    }

    pub(crate) fn removed(&self, queue: Queue<'_>, reason: Removal, messages: &[MessageFacts]) {
        #[cfg(feature = "otel")]
        {
            if let Some(instruments) = &self.instruments {
                instruments.removed(queue, reason, messages, now_ms());
            }
            for message in messages {
                super::otel::messages::link(
                    message.trace_header.as_deref(),
                    message.traceparent.as_deref(),
                    message.id,
                    None,
                );
            }
        }
        let _ = (queue, reason, messages);
    }

    /// Removals counted but not looked at one by one: a purge, clearing a
    /// queue's failed messages, the retention sweep.
    pub(crate) fn removed_count(&self, queue: Queue<'_>, reason: Removal, count: u64) {
        #[cfg(feature = "otel")]
        if let Some(instruments) = &self.instruments {
            instruments.removed_count(queue, reason, count);
        }
        let _ = (queue, reason, count);
    }

    pub(crate) fn visibility_changed(
        &self,
        queue: Queue<'_>,
        change: VisibilityChange,
        messages: &[MessageFacts],
    ) {
        #[cfg(feature = "otel")]
        {
            if let Some(instruments) = &self.instruments {
                instruments.visibility_changed(queue, change, messages.len() as u64);
            }
            for message in messages {
                super::otel::messages::link(
                    message.trace_header.as_deref(),
                    message.traceparent.as_deref(),
                    message.id,
                    None,
                );
            }
        }
        let _ = (queue, change, messages);
    }

    /// Replaces the queues the gauges report.
    pub(crate) fn set_queue_gauges(&self, queues: Vec<QueueGauge>) {
        #[cfg(feature = "otel")]
        if let Some(instruments) = &self.instruments {
            instruments.set_queue_gauges(queues);
            return;
        }
        let _ = queues;
    }
}

/// The `AWSTraceHeader` to store for a message that doesn't set one, sent
/// in a request without `X-Amzn-Trace-Id`. It's the message's `traceparent`
/// attribute, else the request span's own context, so that consumers that
/// follow `AWSTraceHeader` continue the producer's trace.
///
/// `None` unless traces are exported, so that without telemetry the stored
/// header is only ever what the sender gave, as on AWS.
pub(crate) fn derived_trace_header(traceparent: Option<&str>) -> Option<String> {
    #[cfg(feature = "otel")]
    let header = super::otel::messages::derived_trace_header(traceparent);
    #[cfg(not(feature = "otel"))]
    let header = {
        let _ = traceparent;
        None
    };
    header
}
