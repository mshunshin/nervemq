//! OpenTelemetry export (the `otel` feature): a provider for each exported
//! signal, the tracing layers that feed them, and the resource that names
//! this server.

mod idle_receives;
mod instruments;
pub(crate) mod messages;
mod propagation;
mod settings;
#[cfg(test)]
mod tests;

pub use idle_receives::DropIdleReceives;
pub use instruments::{Instruments, RequestAttributes};
pub use propagation::continue_remote_trace;
pub use settings::{Settings, Signal};

use opentelemetry::{trace::TracerProvider as _, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    metrics::SdkMeterProvider,
    resource::TelemetryResourceDetector,
    trace::{BatchSpanProcessor, SdkTracerProvider},
    Resource,
};
use tracing::{level_filters::LevelFilter, Level, Metadata};
use tracing_subscriber::{
    filter::{filter_fn, EnvFilter},
    Layer, Registry,
};

pub type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync>;

/// The SDK's providers for the exported signals. Each exports in batches
/// (metrics: periodically) from a thread of its own.
#[derive(Default)]
pub struct Providers {
    pub tracer: Option<SdkTracerProvider>,
    pub meter: Option<SdkMeterProvider>,
    pub logger: Option<SdkLoggerProvider>,
}

impl Providers {
    /// A provider for each signal the settings export. A signal whose
    /// exporter can't be built is turned off with a warning: the server
    /// starts either way.
    pub fn build(settings: &mut Settings) -> Providers {
        let resource = resource(settings);
        let mut providers = Providers::default();
        for signal in Signal::ALL {
            let Some(protocol) = settings.protocol(signal) else {
                continue;
            };
            let resource = resource.clone();
            let built = match signal {
                Signal::Traces => SpanExporter::builder()
                    .with_http()
                    .with_protocol(protocol)
                    .build()
                    .map(|exporter| {
                        let batches = BatchSpanProcessor::builder(exporter).build();
                        providers.tracer = Some(
                            SdkTracerProvider::builder()
                                .with_span_processor(DropIdleReceives::new(batches))
                                .with_resource(resource)
                                .build(),
                        );
                    }),
                Signal::Metrics => MetricExporter::builder()
                    .with_http()
                    .with_protocol(protocol)
                    .build()
                    .map(|exporter| {
                        providers.meter = Some(
                            SdkMeterProvider::builder()
                                .with_periodic_exporter(exporter)
                                .with_resource(resource)
                                .build(),
                        );
                    }),
                Signal::Logs => LogExporter::builder()
                    .with_http()
                    .with_protocol(protocol)
                    .build()
                    .map(|exporter| {
                        providers.logger = Some(
                            SdkLoggerProvider::builder()
                                .with_batch_exporter(exporter)
                                .with_resource(resource)
                                .build(),
                        );
                    }),
            };
            if let Err(error) = built {
                settings
                    .warnings
                    .push(format!("not exporting {}: {error}", signal.label()));
                settings.disable(signal);
            }
        }
        providers
    }

    /// Exports what is queued and stops the exporters. Blocks until done,
    /// each signal for up to its export timeout.
    pub fn shutdown(&mut self) {
        let tracer = self.tracer.take().map(|p| ("traces", p.shutdown()));
        let meter = self.meter.take().map(|p| ("metrics", p.shutdown()));
        // Last, so the others' failures can still be logged.
        let logger = self.logger.take().map(|p| ("logs", p.shutdown()));
        for (signal, result) in [tracer, meter, logger].into_iter().flatten() {
            if let Err(error) = result {
                tracing::warn!(%error, "flushing {signal} at shutdown failed");
            }
        }
    }
}

/// What every exported signal says about where it came from. NerveMQ's
/// defaults come first, so `OTEL_RESOURCE_ATTRIBUTES` and then
/// `OTEL_SERVICE_NAME` override them: each layer of the builder wins over
/// the ones before.
pub fn resource(settings: &Settings) -> Resource {
    let mut resource = Resource::builder_empty()
        .with_service_name("nervemq")
        .with_attributes([
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            // Tells this process's telemetry from other replicas'.
            KeyValue::new(
                "service.instance.id",
                format!("{:032x}", rand::random::<u128>()),
            ),
        ])
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_attributes(
            settings
                .resource_attributes
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        );
    if let Some(name) = &settings.service_name {
        resource = resource.with_service_name(name.clone());
    }
    resource.build()
}

/// The tracing layers that feed the providers, each with its own filter:
/// - **Traces** carry NerveMQ's spans, with warnings and errors as their
///   events, whatever `NERVEMQ_LOG` says.
/// - **Logs** carry the events `NERVEMQ_LOG` lets through to stdout, minus
///   the exporters' own HTTP clients and the SDK. Those would otherwise
///   log each export, which would be exported in turn.
pub fn layers(
    providers: &Providers,
    log_filter: impl Fn() -> eyre::Result<EnvFilter>,
) -> eyre::Result<Vec<BoxedLayer>> {
    let mut layers: Vec<BoxedLayer> = Vec::new();
    if let Some(tracer) = &providers.tracer {
        layers.push(
            tracing_opentelemetry::layer()
                .with_tracer(tracer.tracer("nervemq"))
                // The span's name and timing say what these would.
                .with_threads(false)
                .with_location(false)
                .with_target(false)
                .with_tracked_inactivity(false)
                .with_filter(filter_fn(is_traced).with_max_level_hint(LevelFilter::INFO))
                .boxed(),
        );
    }
    if let Some(logger) = &providers.logger {
        let mut filter = log_filter()?;
        for quiet in [
            "hyper=off",
            "h2=off",
            "reqwest=off",
            "tonic=off",
            "opentelemetry=off",
            "opentelemetry_sdk=off",
            "opentelemetry_otlp=off",
            "opentelemetry_http=off",
        ] {
            filter = filter.add_directive(quiet.parse()?);
        }
        layers.push(OpenTelemetryTracingBridge::new(logger).with_filter(filter).boxed());
    }
    Ok(layers)
}

fn is_traced(metadata: &Metadata<'_>) -> bool {
    if metadata.is_span() {
        metadata.target().starts_with("nervemq")
    } else {
        // Inside a span, these become its events. Outside one, the
        // OpenTelemetry layer ignores them.
        *metadata.level() <= Level::WARN
    }
}
