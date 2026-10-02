//! A message's creation context: stored as its `AWSTraceHeader` (X-Ray
//! format, as AWS stores it), and linked from each request span that
//! handles the message.

use std::collections::HashMap;

use opentelemetry::{
    propagation::TextMapPropagator,
    trace::{SpanContext, TraceContextExt},
    Context, KeyValue,
};
use opentelemetry_aws::trace::XrayPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const XRAY_HEADER: &str = "x-amzn-trace-id";

/// A span context in X-Ray's header format: `Root=1-…;Parent=…;Sampled=…`.
/// Any W3C trace id converts without loss.
pub fn xray_header(context: &SpanContext) -> Option<String> {
    if !context.is_valid() {
        return None;
    }
    let mut carrier = HashMap::new();
    XrayPropagator::default().inject_context(
        &Context::new().with_remote_span_context(context.clone()),
        &mut carrier,
    );
    carrier.remove(XRAY_HEADER)
}

fn from_xray(header: &str) -> SpanContext {
    let carrier = HashMap::from([(XRAY_HEADER.to_owned(), header.to_owned())]);
    let context = XrayPropagator::default().extract(&carrier);
    context.span().span_context().clone()
}

fn from_traceparent(traceparent: &str) -> SpanContext {
    let carrier = HashMap::from([("traceparent".to_owned(), traceparent.to_owned())]);
    let context = TraceContextPropagator::new().extract(&carrier);
    context.span().span_context().clone()
}

/// The context a message was created in: its stored header, else its
/// `traceparent` attribute.
pub fn creation_context(header: Option<&str>, traceparent: Option<&str>) -> Option<SpanContext> {
    header
        .map(from_xray)
        .filter(SpanContext::is_valid)
        .or_else(|| traceparent.map(from_traceparent).filter(SpanContext::is_valid))
}

/// See `crate::telemetry::messages::derived_trace_header`. Traces are
/// exported when the request span has an OpenTelemetry context.
pub fn derived_trace_header(traceparent: Option<&str>) -> Option<String> {
    let request = tracing::Span::current().context().span().span_context().clone();
    if !request.is_valid() {
        return None;
    }
    let attribute = traceparent.map(from_traceparent).filter(SpanContext::is_valid);
    xray_header(&attribute.unwrap_or(request))
}

/// Links the current (request) span to a message's creation context, with
/// the message's id and, on a delivery, which attempt it is. A link to the
/// request span itself is skipped: that's a message the request created.
pub fn link(header: Option<&str>, traceparent: Option<&str>, id: u64, attempt: Option<u64>) {
    let Some(context) = creation_context(header, traceparent) else {
        return;
    };
    let span = tracing::Span::current();
    if span.context().span().span_context().span_id() == context.span_id() {
        return;
    }
    let mut attributes = vec![KeyValue::new("messaging.message.id", id.to_string())];
    if let Some(attempt) = attempt {
        attributes.push(KeyValue::new("nervemq.message.delivery_attempt", attempt as i64));
    }
    span.add_link_with_attributes(context, attributes);
}
