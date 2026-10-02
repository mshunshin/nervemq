//! What the server records about the requests it handles: the span each
//! request runs in, named and attributed after OpenTelemetry's semantic
//! conventions so it can be exported as the server span.
//!
//! Spans and the logs made inside them never carry message bodies, message
//! attribute values, receipt handles or credentials (`span_tests` in
//! `crate::sqs` pins this).

mod root_span;

pub use root_span::RootSpan;

#[cfg(test)]
pub mod test_support;
