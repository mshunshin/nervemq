//! A tracing layer that keeps every span and event recorded on the test's
//! thread, for tests to assert on what would be logged or exported.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use tracing::{
    field::{Field, Visit},
    span, subscriber::DefaultGuard, Subscriber,
};
use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan, Layer};

/// A span as recorded: its name, its parent's name, and every field value
/// it was given, in order (a field recorded twice appears twice).
#[derive(Clone, Debug)]
pub struct SpanRecord {
    pub name: String,
    pub parent: Option<String>,
    pub fields: Vec<(String, String)>,
}

impl SpanRecord {
    /// The field's last recorded value.
    pub fn field(&self, name: &str) -> Option<String> {
        self.fields
            .iter()
            .rev()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.clone())
    }
}

#[derive(Clone, Default)]
pub struct Captured {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    spans: Vec<SpanRecord>,
    // Span ids are reused once a span closes; this maps to the latest.
    index: HashMap<u64, usize>,
    events: Vec<Vec<(String, String)>>,
}

impl Captured {
    /// Captures everything recorded on this thread until the guard drops.
    /// Actix's test runtime runs a test's requests on its own thread.
    pub fn install() -> (Captured, DefaultGuard) {
        let captured = Captured::default();
        let guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
        (captured, guard)
    }

    pub fn spans(&self) -> Vec<SpanRecord> {
        self.inner.lock().unwrap().spans.clone()
    }

    pub fn last_span(&self, name: &str) -> Option<SpanRecord> {
        self.spans().into_iter().rev().find(|span| span.name == name)
    }

    /// Every value recorded on a span or an event, for checking that
    /// something never appears.
    pub fn all_values(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        let spans = inner.spans.iter().flat_map(|span| &span.fields);
        let events = inner.events.iter().flatten();
        spans.chain(events).map(|(_, value)| value.clone()).collect()
    }
}

impl<S> Layer<S> for Captured
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .map(|parent| parent.name().to_owned());

        let mut inner = self.inner.lock().unwrap();
        let position = inner.spans.len();
        inner.spans.push(SpanRecord {
            name: attrs.metadata().name().to_owned(),
            parent,
            fields: fields.0,
        });
        inner.index.insert(id.into_u64(), position);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        values.record(&mut fields);

        let mut inner = self.inner.lock().unwrap();
        if let Some(&position) = inner.index.get(&id.into_u64()) {
            inner.spans[position].fields.extend(fields.0);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.inner.lock().unwrap().events.push(fields.0);
    }
}

#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}
