//! Which signals to export, from the standard `OTEL_*` variables.
//!
//! The exporters read the endpoint, headers, timeout and compression
//! variables themselves, and the SDK reads the sampler, batching and
//! metric-interval ones. This decides only what they don't:
//! - whether a signal is exported at all;
//! - over which OTLP protocol;
//! - the service name and resource attributes, which are applied over
//!   NerveMQ's defaults.

use opentelemetry_otlp::Protocol;

/// The three OpenTelemetry signals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
}

impl Signal {
    pub const ALL: [Signal; 3] = [Signal::Traces, Signal::Metrics, Signal::Logs];

    /// As in the variable names: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`.
    fn variable(self) -> &'static str {
        match self {
            Signal::Traces => "TRACES",
            Signal::Metrics => "METRICS",
            Signal::Logs => "LOGS",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Metrics => "metrics",
            Signal::Logs => "logs",
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct Settings {
    /// The protocol each exported signal is sent over; `None` for a signal
    /// that isn't exported.
    pub traces: Option<Protocol>,
    pub metrics: Option<Protocol>,
    pub logs: Option<Protocol>,
    /// `OTEL_SERVICE_NAME`, over the default `nervemq`.
    pub service_name: Option<String>,
    /// `OTEL_RESOURCE_ATTRIBUTES`, over NerveMQ's own resource attributes.
    pub resource_attributes: Vec<(String, String)>,
    /// Problems with the variables, logged once logging is up.
    pub warnings: Vec<String>,
}

impl Settings {
    pub fn protocol(&self, signal: Signal) -> Option<Protocol> {
        match signal {
            Signal::Traces => self.traces,
            Signal::Metrics => self.metrics,
            Signal::Logs => self.logs,
        }
    }

    pub fn disable(&mut self, signal: Signal) {
        match signal {
            Signal::Traces => self.traces = None,
            Signal::Metrics => self.metrics = None,
            Signal::Logs => self.logs = None,
        }
    }

    /// Settings from `lookup`, which gives a variable's value (the process
    /// environment, or a table in tests). A signal is exported when:
    /// - an endpoint is set for it (`OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT`
    ///   or `OTEL_EXPORTER_OTLP_ENDPOINT`), or `OTEL_<SIGNAL>_EXPORTER` is
    ///   `otlp`, which uses the default endpoint (`http://localhost:4318`);
    /// - `OTEL_SDK_DISABLED` isn't `true`;
    /// - `OTEL_<SIGNAL>_EXPORTER` isn't `none`;
    /// - its protocol is `http/protobuf` (the default) or `http/json`. gRPC
    ///   isn't built in.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Settings {
        let var = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };

        let mut settings = Settings {
            service_name: var("OTEL_SERVICE_NAME"),
            resource_attributes: var("OTEL_RESOURCE_ATTRIBUTES")
                .map(|value| parse_resource_attributes(&value))
                .unwrap_or_default(),
            ..Settings::default()
        };
        if var("OTEL_SDK_DISABLED").is_some_and(|value| value.eq_ignore_ascii_case("true")) {
            return settings;
        }

        for signal in Signal::ALL {
            let protocol = settings.resolve(signal, &var);
            match signal {
                Signal::Traces => settings.traces = protocol,
                Signal::Metrics => settings.metrics = protocol,
                Signal::Logs => settings.logs = protocol,
            }
        }
        settings
    }

    fn resolve(
        &mut self,
        signal: Signal,
        var: &impl Fn(&str) -> Option<String>,
    ) -> Option<Protocol> {
        let name = signal.variable();
        let label = signal.label();

        let exporter_var = format!("OTEL_{name}_EXPORTER");
        let exporter = var(&exporter_var);
        match exporter.as_deref() {
            None | Some("otlp") => {}
            Some("none") => return None,
            Some(other) => {
                self.warnings.push(format!(
                    "{exporter_var}={other} is not supported (only otlp or none): not exporting {label}"
                ));
                return None;
            }
        }
        let endpoint = var(&format!("OTEL_EXPORTER_OTLP_{name}_ENDPOINT"))
            .or_else(|| var("OTEL_EXPORTER_OTLP_ENDPOINT"));
        if endpoint.is_none() && exporter.is_none() {
            return None;
        }

        let signal_protocol_var = format!("OTEL_EXPORTER_OTLP_{name}_PROTOCOL");
        let (protocol_var, protocol) = match var(&signal_protocol_var) {
            Some(protocol) => (signal_protocol_var, Some(protocol)),
            None => (
                "OTEL_EXPORTER_OTLP_PROTOCOL".to_owned(),
                var("OTEL_EXPORTER_OTLP_PROTOCOL"),
            ),
        };
        match protocol.as_deref() {
            None | Some("http/protobuf") => Some(Protocol::HttpBinary),
            Some("http/json") => Some(Protocol::HttpJson),
            Some(other) => {
                self.warnings.push(format!(
                    "{protocol_var}={other} is not supported (http/protobuf or http/json): \
                     not exporting {label}"
                ));
                None
            }
        }
    }
}

/// `key=value,key=value`, parsed as the SDK's own environment detector does.
fn parse_resource_attributes(value: &str) -> Vec<(String, String)> {
    value
        .split_terminator(',')
        .filter_map(|entry| entry.split_once('='))
        .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
        .filter(|(key, _)| !key.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use opentelemetry_otlp::Protocol;

    use super::Settings;

    fn settings(vars: &[(&str, &str)]) -> Settings {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        Settings::from_lookup(|name| vars.get(name).cloned())
    }

    fn exported(settings: &Settings) -> [Option<Protocol>; 3] {
        [settings.traces, settings.metrics, settings.logs]
    }

    const ENDPOINT: (&str, &str) = ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318");

    #[test]
    fn nothing_is_exported_without_an_endpoint() {
        assert_eq!(exported(&settings(&[])), [None, None, None]);
        assert_eq!(exported(&settings(&[("OTEL_EXPORTER_OTLP_ENDPOINT", " ")])), [None; 3]);
    }

    #[test]
    fn an_endpoint_exports_every_signal_over_protobuf() {
        let all = Some(Protocol::HttpBinary);
        assert_eq!(exported(&settings(&[ENDPOINT])), [all, all, all]);
        // A signal's own endpoint exports just that signal.
        let only_traces = settings(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://c/v1/traces")]);
        assert_eq!(exported(&only_traces), [all, None, None]);
        // As does asking for its exporter by name, at the default endpoint.
        assert_eq!(exported(&settings(&[("OTEL_LOGS_EXPORTER", "otlp")])), [None, None, all]);
    }

    #[test]
    fn signals_can_be_turned_off() {
        let off = settings(&[ENDPOINT, ("OTEL_METRICS_EXPORTER", "none")]);
        assert_eq!(off.metrics, None);
        assert!(off.traces.is_some() && off.logs.is_some());
        assert!(off.warnings.is_empty());

        assert_eq!(exported(&settings(&[ENDPOINT, ("OTEL_SDK_DISABLED", "TRUE")])), [None; 3]);
        assert!(settings(&[ENDPOINT, ("OTEL_SDK_DISABLED", "false")]).traces.is_some());
    }

    #[test]
    fn the_protocol_can_be_set_generally_or_per_signal() {
        let json = settings(&[
            ENDPOINT,
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/json"),
            ("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "http/protobuf"),
        ]);
        assert_eq!(
            exported(&json),
            [Some(Protocol::HttpJson), Some(Protocol::HttpJson), Some(Protocol::HttpBinary)]
        );
    }

    #[test]
    fn unsupported_choices_turn_the_signal_off_with_a_warning() {
        let grpc = settings(&[ENDPOINT, ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")]);
        assert_eq!(grpc.traces, None);
        assert!(grpc.metrics.is_some());
        assert_eq!(grpc.warnings.len(), 1);
        assert!(grpc.warnings[0].contains("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc"), "{grpc:?}");

        let console = settings(&[ENDPOINT, ("OTEL_LOGS_EXPORTER", "console")]);
        assert_eq!(console.logs, None);
        assert!(console.warnings[0].contains("OTEL_LOGS_EXPORTER=console"));
    }

    #[test]
    fn service_name_and_resource_attributes_are_read() {
        let named = settings(&[
            ("OTEL_SERVICE_NAME", "orders-queue"),
            ("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prod, team = payments,bad,=x"),
        ]);
        assert_eq!(named.service_name.as_deref(), Some("orders-queue"));
        assert_eq!(
            named.resource_attributes,
            [
                ("deployment.environment".to_owned(), "prod".to_owned()),
                ("team".to_owned(), "payments".to_owned()),
            ]
        );
    }
}
