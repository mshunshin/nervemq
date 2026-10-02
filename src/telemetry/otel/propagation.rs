//! Trace context arriving on a request: W3C `traceparent`, or AWS X-Ray's
//! `X-Amzn-Trace-Id` (the Java SDK's tracing sends it). With both, W3C
//! wins: the composite applies them in order, and the last one with a
//! valid context decides.

use std::sync::LazyLock;

use actix_web::http::header::HeaderMap;
use opentelemetry::{
    propagation::{Extractor, TextMapCompositePropagator, TextMapPropagator},
    trace::{TraceContextExt, TraceId},
};
use opentelemetry_aws::trace::XrayPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub static PROPAGATOR: LazyLock<TextMapCompositePropagator> = LazyLock::new(|| {
    TextMapCompositePropagator::new(vec![
        Box::new(XrayPropagator::default()),
        Box::new(TraceContextPropagator::new()),
    ])
});

/// actix's `HeaderMap` as a carrier. opentelemetry-http's carrier is for
/// the `http` 1.x crate; actix 4 uses 0.2.
struct Headers<'a>(&'a HeaderMap);

impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|name| name.as_str()).collect()
    }
}

/// Continues the caller's trace, if the request carries one, then records
/// the trace id on the span (its `trace_id` field) for stdout logs.
///
/// Must run before the span starts: a started span's parent can't change.
pub fn continue_remote_trace(span: &Span, headers: &HeaderMap) {
    if headers.contains_key("traceparent") || headers.contains_key("x-amzn-trace-id") {
        let parent = PROPAGATOR.extract(&Headers(headers));
        // Fails only when no OpenTelemetry layer is installed, when there
        // is no trace to continue anyway.
        let _ = span.set_parent(parent);
    }
    let trace_id = span.context().span().span_context().trace_id();
    if trace_id != TraceId::INVALID {
        span.record("trace_id", tracing::field::display(trace_id));
    }
}
