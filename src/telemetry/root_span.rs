//! The request span (`tracing_actix_web::TracingLogger`'s root span).
//!
//! Its name is settled when the request starts: an exported span can't be
//! renamed once started. So an SQS request is named after the action in its
//! `X-Amz-Target` header (`SQS.SendMessage`), like the AWS SDKs' client
//! spans, and anything else after its method and route (`GET
//! /api/admin/stats/queue`). Fields learnt later start empty and are recorded
//! as the request goes: the queue by the SQS handlers (`target_queue`), the
//! caller by the authentication middleware.

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::{header, Version},
    Error, HttpMessage,
};
use tracing::{field::Empty, Span};
use tracing_actix_web::{RequestId, RootSpanBuilder};

use crate::{
    api::health::is_health_path,
    sqs::{error::is_sqs_path, method::Method},
};

/// `TracingLogger::<RootSpan>` names and attributes each request's span.
pub struct RootSpan;

impl RootSpanBuilder for RootSpan {
    fn on_request_start(request: &ServiceRequest) -> Span {
        // Probes call this every few seconds; a span each would bury the
        // traffic that matters.
        if is_health_path(request.path()) {
            return Span::none();
        }

        let method = request.method().as_str();
        let route = request.match_pattern();
        let action = is_sqs_path(request.path())
            .then(|| request.headers().get("x-amz-target"))
            .flatten()
            .and_then(|target| target.to_str().ok())
            .and_then(|target| Method::parse(target).ok())
            .map(<&'static str>::from);
        let name = match (action, &route) {
            (Some(action), _) => format!("SQS.{action}"),
            (None, Some(route)) => format!("{method} {route}"),
            (None, None) => method.to_owned(),
        };
        let request_id = request.extensions().get::<RequestId>().copied();

        tracing::info_span!(
            "HTTP request",
            otel.name = %name,
            otel.kind = "server",
            otel.status_code = Empty,
            http.request.method = method,
            http.route = route.as_deref(),
            url.path = request.path(),
            network.protocol.version = protocol_version(request.version()),
            // The connection's address, not a forwarded header a client
            // could set.
            client.address = request.peer_addr().map(|a| a.ip().to_string()).as_deref(),
            user_agent.original = request
                .headers()
                .get(header::USER_AGENT)
                .and_then(|v| v.to_str().ok()),
            http.response.status_code = Empty,
            error.type = Empty,
            exception.message = Empty,
            rpc.system = action.map(|_| "aws-api"),
            rpc.service = action.map(|_| "AmazonSQS"),
            rpc.method = action,
            messaging.system = action.map(|_| "aws_sqs"),
            messaging.operation.name = action.filter(|a| operation_type(a).is_some()),
            messaging.operation.type = action.and_then(operation_type),
            messaging.destination.name = Empty,
            nervemq.namespace = Empty,
            // The caller's email: recorded by `Authentication` for API keys
            // and SigV4, and by `Protected` for session cookies.
            enduser.id = Empty,
            request_id = request_id.map(tracing::field::display),
        )
    }

    fn on_request_end<B: MessageBody>(span: Span, outcome: &Result<ServiceResponse<B>, Error>) {
        let (status, error) = match outcome {
            Ok(response) => (response.status(), response.response().error()),
            Err(error) => (error.as_response_error().status_code(), Some(error)),
        };
        span.record("http.response.status_code", status.as_u16());
        // A 4xx is the client's mistake, not a failure of the server's: the
        // conventions leave a server span's status unset for it.
        if status.is_server_error() {
            span.record("otel.status_code", "ERROR");
            span.record("error.type", status.as_str());
            if let Some(error) = error {
                span.record("exception.message", tracing::field::display(error));
            }
        }
    }
}

/// The messaging operation an SQS action performs, in the conventions'
/// terms; `None` for the actions that manage queues rather than messages.
fn operation_type(action: &str) -> Option<&'static str> {
    match action {
        "SendMessage" | "SendMessageBatch" => Some("send"),
        "ReceiveMessage" => Some("receive"),
        "DeleteMessage"
        | "DeleteMessageBatch"
        | "ChangeMessageVisibility"
        | "ChangeMessageVisibilityBatch" => Some("settle"),
        _ => None,
    }
}

fn protocol_version(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "0.9",
        Version::HTTP_10 => "1.0",
        Version::HTTP_11 => "1.1",
        Version::HTTP_2 => "2",
        Version::HTTP_3 => "3",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App, HttpResponse};
    use tracing_actix_web::TracingLogger;

    use super::RootSpan;
    use crate::telemetry::test_support::Captured;

    #[actix_web::test]
    async fn only_server_errors_mark_the_span_as_failed() {
        let (captured, _guard) = Captured::install();
        let app = test::init_service(
            App::new()
                .wrap(TracingLogger::<RootSpan>::new())
                .route("/ok", web::get().to(HttpResponse::Ok))
                .route("/missing", web::get().to(HttpResponse::NotFound))
                .route(
                    "/boom",
                    web::get().to(|| async {
                        Err::<HttpResponse, _>(actix_web::error::ErrorInternalServerError(
                            "the database is unreachable",
                        ))
                    }),
                ),
        )
        .await;
        for (uri, status) in [
            ("/ok", StatusCode::OK),
            ("/missing", StatusCode::NOT_FOUND),
            ("/boom", StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            let resp = test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;
            assert_eq!(resp.status(), status);
            drop(resp);

            let span = captured.last_span("HTTP request").unwrap();
            assert_eq!(span.field("otel.name"), Some(format!("GET {uri}")), "{uri}");
            assert_eq!(
                span.field("http.response.status_code"),
                Some(status.as_u16().to_string())
            );
            if status.is_server_error() {
                assert_eq!(span.field("otel.status_code").as_deref(), Some("ERROR"));
                assert_eq!(span.field("error.type").as_deref(), Some("500"));
                assert_eq!(
                    span.field("exception.message").as_deref(),
                    Some("the database is unreachable")
                );
            } else {
                assert_eq!(span.field("otel.status_code"), None, "{uri}");
            }
        }
    }

    #[actix_web::test]
    async fn health_checks_get_no_span() {
        use tracing_actix_web::RootSpanBuilder;

        // Spans are only real while something is listening.
        let (_captured, _guard) = Captured::install();
        let start = |uri| RootSpan::on_request_start(&test::TestRequest::get().uri(uri).to_srv_request());
        assert!(start("/api/health").is_none());
        assert!(start("/api/health/").is_none());
        assert!(!start("/api/admin/stats/queue").is_none());
    }
}
