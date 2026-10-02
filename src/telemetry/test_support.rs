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

/// Traces, logs and metrics exported into memory through the layers and
/// processors `init` builds, for tests on one thread.
#[cfg(feature = "otel")]
pub mod otel {
    use std::collections::BTreeMap;

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::{
        logs::{InMemoryLogExporter, InMemoryLogExporterBuilder, SdkLoggerProvider},
        metrics::{
            data::{AggregatedMetrics, MetricData},
            InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader,
            SdkMeterProvider,
        },
        trace::{
            InMemorySpanExporter, InMemorySpanExporterBuilder, SdkTracerProvider, SimpleSpanProcessor,
            SpanData,
        },
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::{prelude::*, EnvFilter};

    use super::Captured;
    use crate::telemetry::{
        otel::{layers, DropIdleReceives, Providers},
        Telemetry,
    };

    pub struct Harness {
        pub spans: InMemorySpanExporter,
        pub logs: InMemoryLogExporter,
        pub metrics: InMemoryMetricExporter,
        pub meter: SdkMeterProvider,
        _tracer: SdkTracerProvider,
        _logger: SdkLoggerProvider,
    }

    /// A metric data point: its attributes, and its value (a histogram's
    /// count) and sum (histograms only).
    #[derive(Debug)]
    pub struct Point {
        pub attributes: BTreeMap<String, String>,
        pub value: f64,
        pub sum: f64,
    }

    impl Harness {
        /// With [`Captured`] beside the exporters, to read span fields.
        pub fn install() -> (Harness, Captured, DefaultGuard) {
            let spans = InMemorySpanExporterBuilder::new().build();
            let logs = InMemoryLogExporterBuilder::new().build();
            let metrics = InMemoryMetricExporterBuilder::new().build();
            let tracer = SdkTracerProvider::builder()
                .with_span_processor(DropIdleReceives::new(SimpleSpanProcessor::new(spans.clone())))
                .build();
            let logger = SdkLoggerProvider::builder()
                .with_simple_exporter(logs.clone())
                .build();
            let meter = SdkMeterProvider::builder()
                .with_reader(PeriodicReader::builder(metrics.clone()).build())
                .build();
            let providers = Providers {
                tracer: Some(tracer.clone()),
                meter: None,
                logger: Some(logger.clone()),
            };

            let captured = Captured::default();
            let mut layers = layers(&providers, || Ok(EnvFilter::new("info"))).unwrap();
            layers.push(captured.clone().boxed());
            let guard =
                tracing::subscriber::set_default(tracing_subscriber::registry().with(layers));
            let harness = Harness {
                spans,
                logs,
                metrics,
                meter,
                _tracer: tracer,
                _logger: logger,
            };
            (harness, captured, guard)
        }

        /// A `Telemetry` recording into this harness's metrics.
        pub fn telemetry(&self) -> Telemetry {
            Telemetry::from_meter(self.meter.meter("nervemq"))
        }

        pub fn finished_spans(&self) -> Vec<SpanData> {
            self.spans.get_finished_spans().unwrap()
        }

        /// The last span of that name to end.
        pub fn span(&self, name: &str) -> SpanData {
            self.finished_spans()
                .into_iter()
                .rev()
                .find(|span| span.name == name)
                .unwrap_or_else(|| panic!("no {name} span was exported"))
        }

        pub fn log_bodies(&self) -> Vec<String> {
            let logs = self.logs.get_emitted_logs().unwrap();
            logs.iter()
                .filter_map(|log| log.record.body().map(|body| format!("{body:?}")))
                .collect()
        }

        /// The metric's points as of now (they're cumulative).
        pub fn points(&self, name: &str) -> Vec<Point> {
            self.meter.force_flush().unwrap();
            let exported = self.metrics.get_finished_metrics().unwrap();
            let Some(metric) = exported
                .iter()
                .rev()
                .flat_map(|resource| resource.scope_metrics())
                .flat_map(|scope| scope.metrics())
                .find(|metric| metric.name() == name)
            else {
                return Vec::new();
            };
            let attributes = |attributes: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                attributes
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect::<BTreeMap<_, _>>()
            };
            macro_rules! points {
                ($data:expr) => {
                    match $data {
                        MetricData::Sum(sum) => sum
                            .data_points()
                            .map(|p| Point {
                                attributes: attributes(&mut p.attributes()),
                                value: p.value() as f64,
                                sum: 0.0,
                            })
                            .collect(),
                        MetricData::Gauge(gauge) => gauge
                            .data_points()
                            .map(|p| Point {
                                attributes: attributes(&mut p.attributes()),
                                value: p.value() as f64,
                                sum: 0.0,
                            })
                            .collect(),
                        MetricData::Histogram(histogram) => histogram
                            .data_points()
                            .map(|p| Point {
                                attributes: attributes(&mut p.attributes()),
                                value: p.count() as f64,
                                sum: p.sum() as f64,
                            })
                            .collect(),
                        MetricData::ExponentialHistogram(_) => Vec::new(),
                    }
                };
            }
            match metric.data() {
                AggregatedMetrics::F64(data) => points!(data),
                AggregatedMetrics::U64(data) => points!(data),
                AggregatedMetrics::I64(data) => points!(data),
            }
        }

        /// The total of a counter's points whose attributes include all of
        /// `having`.
        pub fn total(&self, name: &str, having: &[(&str, &str)]) -> f64 {
            self.points(name)
                .iter()
                .filter(|point| {
                    having
                        .iter()
                        .all(|(key, value)| point.attributes.get(*key).map(String::as_str) == Some(*value))
                })
                .map(|point| point.value)
                .sum()
        }
    }
}
