//! `http.server.request.duration` and `http.server.active_requests`,
//! recorded by the outermost middleware so they cover everything the
//! server does with a request, refusals included.

use std::time::Instant;

use actix_web::{
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
    web::Data,
};

use super::{root_span::sqs_action, Telemetry};
use crate::service::Service;

/// `actix_web::middleware::from_fn` middleware; passes straight through
/// unless metrics are exported.
pub async fn http_metrics<B: MessageBody + 'static>(
    request: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, actix_web::Error> {
    let telemetry = request
        .app_data::<Data<Service>>()
        .map(|service| service.telemetry().clone())
        .filter(Telemetry::is_recording);
    let Some(telemetry) = telemetry else {
        return next.call(request).await;
    };

    let method = request.method().as_str().to_owned();
    // Before routing: the pattern is found from the path, and the request
    // is gone by the time an error comes back.
    let route = request.match_pattern();
    let rpc_method = sqs_action(&request);
    let started = Instant::now();
    let in_flight = telemetry.request_started(&method);

    let outcome = next.call(request).await;
    let status = match &outcome {
        Ok(response) => response.status(),
        Err(error) => error.as_response_error().status_code(),
    };
    telemetry.request_answered(
        &method,
        route.as_deref(),
        status.as_u16(),
        rpc_method,
        started.elapsed(),
    );
    drop(in_flight);
    outcome
}
