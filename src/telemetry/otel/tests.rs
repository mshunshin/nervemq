//! Export through the layers and providers `init` builds, into in-memory
//! exporters. Each test installs its subscriber for its own thread only.

use std::collections::BTreeSet;

use actix_web::{http::StatusCode, test, web, App, HttpResponse};
use opentelemetry::{metrics::MeterProvider as _, Key};
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
};
use tracing_actix_web::TracingLogger;

use super::{resource, Settings};
use crate::telemetry::{test_support::otel::Harness, RootSpan, Telemetry};

const W3C: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const XRAY: &str = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";

#[actix_web::test]
async fn requests_continue_the_callers_trace() {
    let (exported, captured, _guard) = Harness::install();
    let app = test::init_service(
        App::new()
            .wrap(TracingLogger::<RootSpan>::new())
            .route(
                "/work",
                web::get().to(|| async {
                    tracing::warn!("inside the request");
                    HttpResponse::Ok()
                }),
            ),
    )
    .await;

    for (case, headers, trace_id, parent) in [
        ("W3C", vec![("traceparent", W3C)], "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"),
        ("X-Ray", vec![("x-amzn-trace-id", XRAY)], "5759e988bd862e3fe1be46a994272793", "53995c3f42cd8ad8"),
        (
            "both: W3C wins",
            vec![("x-amzn-trace-id", XRAY), ("traceparent", W3C)],
            "0af7651916cd43dd8448eb211c80319c",
            "b7ad6b7169203331",
        ),
    ] {
        let mut request = test::TestRequest::get().uri("/work");
        for header in headers {
            request = request.insert_header(header);
        }
        let response = test::call_service(&app, request.to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);

        let span = exported.span("GET /work");
        assert_eq!(span.span_context.trace_id().to_string(), trace_id, "{case}");
        assert_eq!(span.parent_span_id.to_string(), parent, "{case}");
        assert!(span.parent_span_is_remote, "{case}");

        // The trace id is on the span's fields, for stdout logs.
        let fields = captured.last_span("HTTP request").unwrap();
        assert_eq!(fields.field("trace_id").as_deref(), Some(trace_id), "{case}");

        // A log record made during the request carries its span.
        let logs = exported.logs.get_emitted_logs().unwrap();
        let log = logs.last().unwrap();
        let context = log.record.trace_context().unwrap();
        assert_eq!(context.trace_id, span.span_context.trace_id(), "{case}");
        assert_eq!(context.span_id, span.span_context.span_id(), "{case}");
    }

    // Without either header, a request starts a trace of its own.
    let response = test::call_service(&app, test::TestRequest::get().uri("/work").to_request()).await;
    drop(response);
    let span = exported.span("GET /work");
    assert!(!span.parent_span_is_remote);
    assert_eq!(span.parent_span_id, opentelemetry::trace::SpanId::INVALID);
}

/// Only NerveMQ's spans become traces, and the exporters' own HTTP clients
/// and the SDK aren't exported as logs: each export would log, and that log
/// would be exported in turn.
#[actix_web::test]
async fn only_nervemq_spans_and_logs_are_exported() {
    let (exported, _captured, _guard) = Harness::install();

    tracing::info_span!(target: "sqlx::query", "a query").in_scope(|| {});
    tracing::info_span!("nervemq work").in_scope(|| {});
    tracing::info!(target: "reqwest::connect", "connecting to the collector");
    tracing::info!(target: "hyper::client", "sending");
    tracing::warn!(target: "opentelemetry_sdk", "export retried");
    tracing::debug!("below the log level");
    tracing::info!("served a request");

    let spans = exported.spans.get_finished_spans().unwrap();
    let names: Vec<_> = spans.iter().map(|span| span.name.to_string()).collect();
    assert_eq!(names, ["nervemq work"]);
    let logs = exported.log_bodies();
    assert_eq!(logs.len(), 1, "{logs:?}");
    assert!(logs[0].contains("served a request"), "{logs:?}");
}

#[actix_web::test]
async fn requests_are_measured_by_method_route_status_and_action() {
    let metrics = InMemoryMetricExporterBuilder::new().build();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(metrics.clone()).build())
        .build();
    let (data, _dir) = service(Telemetry::from_meter(provider.meter("nervemq"))).await;
    let app = test::init_service(crate::build_app(
        data,
        crate::auth::session::SqliteSessionStore::in_memory().await,
        actix_web::cookie::Key::generate(),
    ))
    .await;

    for (request, status) in [
        (test::TestRequest::get().uri("/api/health"), StatusCode::OK),
        (test::TestRequest::get().uri("/api/admin/stats/queue"), StatusCode::UNAUTHORIZED),
        (
            test::TestRequest::post()
                .uri("/api/sqs")
                .insert_header(("x-amz-target", "AmazonSQS.ListQueues")),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let status_seen = match test::try_call_service(&app, request.to_request()).await {
            Ok(response) => response.status(),
            Err(error) => error.as_response_error().status_code(),
        };
        assert_eq!(status_seen, status);
    }
    provider.force_flush().unwrap();

    let exported = metrics.get_finished_metrics().unwrap();
    let metric = |name: &str| {
        exported
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .find(|metric| metric.name() == name)
            .unwrap_or_else(|| panic!("{name} wasn't exported"))
            .data()
    };
    let attributes = |attributes: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
        attributes
            .map(|kv| format!("{}={}", kv.key, kv.value))
            .collect::<BTreeSet<_>>()
    };

    let AggregatedMetrics::F64(MetricData::Histogram(durations)) =
        metric("http.server.request.duration")
    else {
        panic!("not a histogram");
    };
    let measured: Vec<(BTreeSet<String>, u64)> = durations
        .data_points()
        .map(|point| (attributes(&mut point.attributes()), point.count()))
        .collect();
    for expected in [
        vec!["http.request.method=GET", "http.response.status_code=200", "http.route=/api/health"],
        vec![
            "http.request.method=GET",
            "http.response.status_code=401",
            "http.route=/api/admin/stats/queue",
        ],
        vec![
            "http.request.method=POST",
            "http.response.status_code=401",
            "http.route=/api/sqs",
            "rpc.method=ListQueues",
        ],
    ] {
        let expected: BTreeSet<String> = expected.into_iter().map(str::to_owned).collect();
        assert!(
            measured.contains(&(expected.clone(), 1)),
            "no point for {expected:?} in {measured:?}"
        );
    }

    // Every request counted in flight was counted out again.
    let AggregatedMetrics::I64(MetricData::Sum(in_flight)) = metric("http.server.active_requests")
    else {
        panic!("not a sum");
    };
    assert!(in_flight.data_points().all(|point| point.value() == 0));
}

#[actix_web::test]
async fn the_resource_names_nervemq_unless_told_otherwise() {
    let get = |resource: &opentelemetry_sdk::Resource, key: &'static str| {
        resource.get(&Key::new(key)).map(|value| value.to_string())
    };

    let default = resource(&Settings::default());
    assert_eq!(get(&default, "service.name").as_deref(), Some("nervemq"));
    assert_eq!(get(&default, "service.version").as_deref(), Some(env!("CARGO_PKG_VERSION")));
    assert_eq!(get(&default, "service.instance.id").map(|id| id.len()), Some(32));
    assert_eq!(get(&default, "telemetry.sdk.language").as_deref(), Some("rust"));

    let attributes = vec![
        ("service.name".to_owned(), "from-attributes".to_owned()),
        ("deployment.environment".to_owned(), "prod".to_owned()),
    ];
    let described = resource(&Settings {
        resource_attributes: attributes.clone(),
        ..Settings::default()
    });
    assert_eq!(get(&described, "service.name").as_deref(), Some("from-attributes"));
    assert_eq!(get(&described, "deployment.environment").as_deref(), Some("prod"));

    let renamed = resource(&Settings {
        service_name: Some("orders-queue".to_owned()),
        resource_attributes: attributes,
        ..Settings::default()
    });
    assert_eq!(get(&renamed, "service.name").as_deref(), Some("orders-queue"));
}

async fn service(telemetry: Telemetry) -> (web::Data<crate::service::Service>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config: crate::config::Config = serde_json::from_value(serde_json::json!({
        "db_path": dir.path().join("test.db").to_string_lossy(),
    }))
    .unwrap();
    let service = crate::service::Service::connect_with()
        .config(config)
        .kms_factory(|_| async move { Ok(crate::kms::memory::InMemoryKeyManager::new()) })
        .telemetry(telemetry)
        .call()
        .await
        .unwrap();
    (web::Data::new(service), dir)
}
