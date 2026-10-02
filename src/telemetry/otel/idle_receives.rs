//! Drops the traces of receives that found nothing.
//!
//! A consumer polling an empty queue sends ReceiveMessage after
//! ReceiveMessage. With short polling that's several a second, and each
//! would be a trace of its own. Such a request's trace is dropped, along
//! with its child spans, when it:
//! - returned no messages;
//! - and isn't part of a caller's trace.
//!
//! Receives that return messages, fail, or belong to a caller's trace are
//! kept. The metrics count every request either way.

use std::{collections::HashMap, sync::Mutex, time::Duration};

use opentelemetry::{
    trace::{SpanId, TraceContextExt},
    Context,
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{Span, SpanData, SpanProcessor},
    Resource,
};

/// Wraps the processor the spans go on to.
#[derive(Debug)]
pub struct DropIdleReceives<P> {
    inner: P,
    /// The receives without a parent that are still running, each with the
    /// child spans that have ended so far. Children end before their
    /// parent, when it isn't yet known whether the receive found anything.
    held: Mutex<HashMap<SpanId, Vec<SpanData>>>,
}

impl<P> DropIdleReceives<P> {
    pub fn new(inner: P) -> Self {
        DropIdleReceives {
            inner,
            held: Mutex::new(HashMap::new()),
        }
    }
}

/// `messaging.batch.message_count` is recorded on every receive's span,
/// zero included, by the ReceiveMessage handler.
fn found_nothing(span: &SpanData) -> bool {
    span.attributes.iter().any(|attribute| {
        attribute.key.as_str() == "messaging.batch.message_count"
            && attribute.value.as_str() == "0"
    })
}

impl<P: SpanProcessor> SpanProcessor for DropIdleReceives<P> {
    fn on_start(&self, span: &mut Span, parent: &Context) {
        // Only requests no caller is tracing, and only their spans: those
        // are named, with all their creation attributes, when they start.
        if !parent.span().span_context().is_valid() {
            if let Some(data) = span.exported_data() {
                if data.name == "SQS.ReceiveMessage" {
                    let id = data.span_context.span_id();
                    self.held.lock().unwrap().insert(id, Vec::new());
                }
            }
        }
        self.inner.on_start(span, parent);
    }

    fn on_end(&self, span: SpanData) {
        let mut held = self.held.lock().unwrap();
        if let Some(children) = held.get_mut(&span.parent_span_id) {
            children.push(span);
            return;
        }
        let Some(children) = held.remove(&span.span_context.span_id()) else {
            drop(held);
            self.inner.on_end(span);
            return;
        };
        drop(held);

        if found_nothing(&span) {
            return;
        }
        for child in children {
            self.inner.on_end(child);
        }
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}
