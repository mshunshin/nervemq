use std::collections::HashMap;

use actix_identity::Identity;
use actix_web::{get, web, Scope};

use crate::{
    error::Error, namespace::NamespaceStatistics, queue::QueueStatistics, service::Service,
};

/// Statistics for every queue the caller can access, keyed by
/// `namespace/queue`.
#[get("/queue")]
async fn queue_stats(
    service: web::Data<Service>,
    identity: Identity,
) -> Result<web::Json<HashMap<String, QueueStatistics>>, Error> {
    Ok(web::Json(service.global_queue_statistics(identity).await?))
}

/// Statistics for every namespace the caller can access, with its owners
/// and whether the caller may manage it.
#[get("/ns")]
async fn namespace_stats(
    service: web::Data<Service>,
    identity: Identity,
) -> Result<web::Json<Vec<NamespaceStatistics>>, Error> {
    Ok(web::Json(service.list_namespace_statistics(identity).await?))
}

pub fn service() -> Scope {
    web::scope("/stats")
        .service(queue_stats)
        .service(namespace_stats)
}
