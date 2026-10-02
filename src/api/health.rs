//! `GET /api/health`: whether the server can do its job, for load balancers,
//! container orchestrators and uptime monitors.
//!
//! It needs no login, and answers under any host name even when
//! `NERVEMQ_HOST` is set (`crate::auth::middleware::host`), since probes
//! usually address the server by IP. All it tells a caller is that a NerveMQ
//! server is there and whether its database answers.

use std::time::Duration;

use actix_web::{web, HttpResponse, Resource};
use serde_json::json;

use crate::service::Service;

/// How long the database may take to answer before the server counts as
/// unhealthy. Without it a request would wait for the pool's 30-second
/// acquire timeout, past any probe's patience.
const DATABASE_TIMEOUT: Duration = Duration::from_secs(2);

pub fn service() -> Resource {
    web::resource("/health")
        .route(web::get().to(health))
        // Some uptime monitors only send HEAD.
        .route(web::head().to(health))
}

/// Whether `path` is the health check's, before `NormalizePath` has trimmed
/// a trailing slash (the host middleware runs outside it).
pub fn is_health_path(path: &str) -> bool {
    path.trim_end_matches('/') == "/api/health"
}

/// 200 when the database answers a query, 503 when it fails or is too slow.
async fn health(service: web::Data<Service>) -> HttpResponse {
    let ping = sqlx::query("SELECT 1").execute(service.db());
    match tokio::time::timeout(DATABASE_TIMEOUT, ping).await {
        Ok(Ok(_)) => HttpResponse::Ok().json(json!({ "status": "ok" })),
        Ok(Err(err)) => {
            tracing::error!(%err, "health check: the database query failed");
            unavailable()
        }
        Err(_) => {
            tracing::error!("health check: the database took over {DATABASE_TIMEOUT:?} to answer");
            unavailable()
        }
    }
}

/// The cause goes to the log, not to the unauthenticated caller.
fn unavailable() -> HttpResponse {
    HttpResponse::ServiceUnavailable().json(json!({ "status": "unavailable" }))
}
