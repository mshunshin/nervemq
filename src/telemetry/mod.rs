//! What the server records about the requests it handles: the span each
//! request runs in, logs, and metrics. Everything goes to stdout, filtered
//! by `NERVEMQ_LOG`. With the `otel` feature, the standard `OTEL_*`
//! variables also turn on OpenTelemetry export over OTLP
//! (docs/architecture/observability.md).
//!
//! Spans and the logs made inside them never carry message bodies, message
//! attribute values, receipt handles or credentials (`span_tests` in
//! `crate::sqs` pins this).
//!
//! The `cfg(feature = "otel")` code stays in this module: the rest of the
//! server records through `Telemetry`, which does nothing without it.

mod http_metrics;
#[cfg(feature = "otel")]
pub(crate) mod otel;
mod root_span;

pub use http_metrics::http_metrics;
pub use root_span::RootSpan;

#[cfg(test)]
pub mod test_support;

use std::{path::PathBuf, time::Duration};

use sqlx::SqlitePool;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{prelude::*, EnvFilter, Registry};

/// Where the server records its metrics: a cheap handle that `Service`
/// holds and the middleware use. `Telemetry::default()` records nothing;
/// `init` returns one that records when metrics are exported.
#[derive(Clone, Default)]
pub struct Telemetry {
    #[cfg(feature = "otel")]
    instruments: Option<std::sync::Arc<otel::Instruments>>,
}

impl Telemetry {
    /// Records through `meter`.
    #[cfg(feature = "otel")]
    pub(crate) fn from_meter(meter: opentelemetry::metrics::Meter) -> Self {
        Telemetry {
            instruments: Some(std::sync::Arc::new(otel::Instruments::new(meter))),
        }
    }

    pub fn is_recording(&self) -> bool {
        #[cfg(feature = "otel")]
        let recording = self.instruments.is_some();
        #[cfg(not(feature = "otel"))]
        let recording = false;
        recording
    }

    /// Counts a request as in flight until the returned guard drops: when
    /// it's answered, or when the client goes away first and its future is
    /// dropped.
    pub(crate) fn request_started(&self, method: &str) -> InFlight {
        #[cfg(feature = "otel")]
        if let Some(instruments) = &self.instruments {
            instruments.request_started(method);
            return InFlight(Some((instruments.clone(), method.to_owned())));
        }
        let _ = method;
        InFlight::default()
    }

    pub(crate) fn request_answered(
        &self,
        method: &str,
        route: Option<&str>,
        status: u16,
        rpc_method: Option<&'static str>,
        duration: Duration,
    ) {
        #[cfg(feature = "otel")]
        if let Some(instruments) = &self.instruments {
            let request = otel::RequestAttributes {
                method,
                route,
                status,
                rpc_method,
            };
            instruments.request_answered(request, duration);
        }
        let _ = (method, route, status, rpc_method, duration);
    }

    /// Gauges for the named connection pools and database files.
    pub(crate) fn observe_databases(
        &self,
        pools: Vec<(&'static str, SqlitePool)>,
        files: Vec<(&'static str, PathBuf)>,
    ) {
        #[cfg(feature = "otel")]
        if let Some(instruments) = &self.instruments {
            instruments.observe_databases(pools, files);
            return;
        }
        let _ = (pools, files);
    }
}

/// A request counted in `http.server.active_requests` until dropped.
#[derive(Default)]
pub(crate) struct InFlight(
    #[cfg(feature = "otel")] Option<(std::sync::Arc<otel::Instruments>, String)>,
);

impl Drop for InFlight {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some((instruments, method)) = &self.0 {
            instruments.request_ended(method);
        }
    }
}

/// Stops the exporters when the server stops, exporting what's queued
/// first: by `shutdown`, or when dropped.
pub struct TelemetryGuard {
    #[cfg(feature = "otel")]
    providers: otel::Providers,
}

impl TelemetryGuard {
    /// Blocks until the final exports are done (each signal for up to its
    /// export timeout), so call it off the async threads.
    pub fn shutdown(&mut self) {
        #[cfg(feature = "otel")]
        self.providers.shutdown();
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Installs the process's tracing subscriber:
/// - stdout logs filtered by `NERVEMQ_LOG` (default `info`), pretty in
///   debug builds and JSON in release builds;
/// - with the `otel` feature, export of whichever signals the `OTEL_*`
///   variables turn on.
///
/// Problems with those variables are logged as warnings rather than
/// stopping the server.
pub fn init() -> eyre::Result<(TelemetryGuard, Telemetry)> {
    let directives = std::env::var("NERVEMQ_LOG").unwrap_or_default();
    let log_filter = || -> eyre::Result<EnvFilter> {
        Ok(EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .parse(&directives)?)
    };

    #[cfg(debug_assertions)]
    let stdout = tracing_subscriber::fmt::layer().pretty().with_filter(log_filter()?).boxed();
    #[cfg(not(debug_assertions))]
    let stdout = tracing_subscriber::fmt::layer().json().with_filter(log_filter()?).boxed();
    #[allow(unused_mut)]
    let mut layers: Vec<Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>> = vec![stdout];

    #[cfg(feature = "otel")]
    let (settings, providers) = {
        let mut settings = otel::Settings::from_lookup(|name| std::env::var(name).ok());
        let providers = otel::Providers::build(&mut settings);
        layers.extend(otel::layers(&providers, log_filter)?);
        (settings, providers)
    };

    tracing_subscriber::registry().with(layers).try_init()?;

    #[cfg(feature = "otel")]
    let telemetry = {
        for warning in &settings.warnings {
            tracing::warn!("{warning}");
        }
        let exported: Vec<_> = otel::Signal::ALL
            .into_iter()
            .filter(|signal| settings.protocol(*signal).is_some())
            .map(otel::Signal::label)
            .collect();
        if !exported.is_empty() {
            tracing::info!(signals = ?exported, "exporting OpenTelemetry over OTLP");
        }
        use opentelemetry::metrics::MeterProvider as _;
        providers
            .meter
            .as_ref()
            .map(|meter| Telemetry::from_meter(meter.meter("nervemq")))
            .unwrap_or_default()
    };
    #[cfg(not(feature = "otel"))]
    let telemetry = Telemetry::default();

    let guard = TelemetryGuard {
        #[cfg(feature = "otel")]
        providers,
    };
    Ok((guard, telemetry))
}
