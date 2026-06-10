use std::collections::HashMap;

use actix_identity::Identity;
use actix_web::{
    delete,
    error::{ErrorInternalServerError, ErrorUnauthorized},
    get, post, web, HttpResponse, Responder, Scope,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    message::MessageStatus,
    queue::Queue,
    service::{MessageDetails, QueueAttributesSer, QueueConfig, Service},
    types::{send_message::SendMessageRequest, SqsMessageAttribute},
};

#[derive(Serialize, Deserialize)]
pub struct ListQueuesResponse {
    queues: Vec<Queue>,
}

#[get("")]
async fn list_all_queues(
    service: web::Data<Service>,
    identity: Identity,
) -> actix_web::Result<impl Responder> {
    let queues = match service.list_all_queues(identity).await {
        Ok(q) => q,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };

    Ok(web::Json(ListQueuesResponse { queues }))
}

#[get("/{ns_name}")]
async fn list_ns_queues(
    service: web::Data<Service>,
    path: web::Path<String>,
) -> actix_web::Result<impl Responder> {
    let queues = match service.list_queues_for_namespace(&*path).await {
        Ok(q) => q,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };

    Ok(web::Json(ListQueuesResponse { queues }))
}

#[delete("/{ns_name}/{queue_name}")]
async fn delete_queue(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> actix_web::Result<impl Responder> {
    let (namespace, name) = &*path;
    if let Err(e) = service.delete_queue(namespace, name, identity).await {
        return Err(actix_web::error::ErrorInternalServerError(e));
    }

    Ok("OK")
}

#[derive(Debug, Deserialize)]
struct CreateQueueRequest {
    attributes: HashMap<String, String>,
    tags: HashMap<String, String>,
}

#[post("/{ns_name}/{queue_name}")]
async fn create_queue(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    data: web::Json<CreateQueueRequest>,
    identity: Identity,
) -> actix_web::Result<impl Responder> {
    let (namespace, name) = &*path;
    let data = data.into_inner();

    // The admin UI sends attributes as a free-form string map; route it
    // through the typed wire representation so known attribute names land
    // under the same storage keys the SQS API uses (unknown keys are kept
    // verbatim via the `other` passthrough).
    let attributes: QueueAttributesSer = serde_json::to_value(data.attributes)
        .and_then(serde_json::from_value)
        .map_err(ErrorInternalServerError)?;

    match service
        .create_queue(namespace, name, attributes, data.tags, identity)
        .await
    {
        Ok(_) => {}
        Err(Error::Unauthorized) => return Err(ErrorUnauthorized("Unauthorized")),
        Err(e) => return Err(ErrorInternalServerError(e)),
    }

    Ok(actix_web::HttpResponse::Ok())
}

#[get("/{ns_name}/{queue_name}")]
async fn queue_stats(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> actix_web::Result<impl Responder> {
    let (namespace, name) = &*path;

    match service.queue_statistics(identity, namespace, name).await {
        Ok(stats) => Ok(web::Json(stats)),
        Err(Error::Unauthorized) => Err(ErrorUnauthorized("Unauthorized")),
        Err(e) => Err(ErrorInternalServerError(e)),
    }
}

#[get("/{ns_name}/{queue_name}/messages")]
async fn list_messages(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> actix_web::Result<web::Json<Vec<MessageDetails>>> {
    let (namespace, name) = &*path;

    let ns_id = match service.get_namespace_id(namespace, service.db()).await {
        Ok(Some(id)) => id,
        Ok(None) => return Err(ErrorInternalServerError("Namespace not found")),
        Err(e) => return Err(ErrorInternalServerError(e)),
    };

    match service
        .check_user_access(&identity, ns_id, service.db())
        .await
    {
        Ok(_) => {}
        Err(e) => return Err(ErrorUnauthorized(e)),
    }

    match service.list_messages(namespace, name).await {
        Ok(messages) => Ok(web::Json(messages)),
        Err(e) => Err(ErrorInternalServerError(e)),
    }
}

#[get("/{ns_name}/{queue_name}/config")]
async fn get_queue_config(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> Result<web::Json<QueueConfig>, Error> {
    let (namespace, name) = &*path;

    let ns_id = match service.get_namespace_id(namespace, service.db()).await {
        Ok(Some(id)) => id,
        Ok(None) => return Err(Error::namespace_not_found(namespace)),
        Err(e) => return Err(e),
    };

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    let queue_id = match service.get_queue_id(namespace, name, service.db()).await? {
        Some(id) => id,
        None => return Err(Error::queue_not_found(name, namespace)),
    };

    let config = service.get_queue_configuration(queue_id).await?;

    Ok(web::Json(config))
}

#[derive(Debug, Deserialize)]
struct UpdateQueueConfigRequest {
    max_retries: u64,
    dead_letter_queue: Option<String>,
}

#[post("/{ns_name}/{queue_name}/config")]
async fn update_queue_config(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    updates: web::Json<UpdateQueueConfigRequest>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name) = &*path;

    let ns_id = match service.get_namespace_id(namespace, service.db()).await {
        Ok(Some(id)) => id,
        Ok(None) => return Err(Error::namespace_not_found(namespace)),
        Err(e) => return Err(e),
    };

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    let queue_id = match service.get_queue_id(namespace, name, service.db()).await? {
        Some(id) => id,
        None => return Err(Error::queue_not_found(name, namespace)),
    };

    let dead_letter_queue = match &updates.dead_letter_queue {
        Some(dlq) => match service.get_queue_id(namespace, dlq, service.db()).await? {
            Some(id) => Some(id),
            None => return Err(Error::queue_not_found(dlq, namespace)),
        },
        None => None,
    };

    let new_config = QueueConfig {
        queue: queue_id,
        max_retries: updates.max_retries,
        dead_letter_queue,
    };

    service
        .update_queue_configuration(queue_id, new_config)
        .await?;

    Ok(HttpResponse::Ok())
}

/// Removes every message from a queue, keeping the queue itself.
#[post("/{ns_name}/{queue_name}/purge")]
async fn purge_queue(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name) = &*path;

    service.purge_queue(namespace, name, identity).await?;

    Ok(HttpResponse::Ok())
}

#[derive(Debug, Deserialize)]
pub struct SendMessageBody {
    pub body: String,
    #[serde(default)]
    pub attributes: HashMap<String, SqsMessageAttribute>,
}

/// Enqueues a message from the management plane. Goes through the same send
/// path as the SQS API, so size limits and queue delay attributes apply.
#[post("/{ns_name}/{queue_name}/messages")]
async fn send_message(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    data: web::Json<SendMessageBody>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name) = &*path;
    let data = data.into_inner();

    let ns_id = service
        .get_namespace_id(namespace, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace))?;
    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;
    let queue_id = service
        .get_queue_id(namespace, name, service.db())
        .await?
        .ok_or_else(|| Error::queue_not_found(name, namespace))?;

    // Record the session user as the sender (SenderId system attribute).
    let sent_by = service.get_user_id(&identity, service.db()).await?;

    let res = service
        .sqs_send(
            queue_id,
            SendMessageRequest {
                // The send path doesn't read the URL (the queue is already
                // resolved above); it's only present to satisfy the shared
                // SQS request shape.
                queue_url: service.config().host(),
                message_body: data.body,
                delay_seconds: None,
                message_attributes: data.attributes,
                message_deduplication_id: None,
                message_group_id: None,
            },
            sent_by,
        )
        .await?;

    Ok(web::Json(res))
}

/// Deletes a single message by ID, regardless of in-flight state.
#[delete("/{ns_name}/{queue_name}/messages/{message_id}")]
async fn delete_message(
    service: web::Data<Service>,
    path: web::Path<(String, String, u64)>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name, message_id) = &*path;

    service
        .admin_delete_message(namespace, name, *message_id, identity)
        .await?;

    Ok(HttpResponse::Ok())
}

#[derive(Debug, Deserialize)]
pub struct UpdateMessageStatusRequest {
    pub status: MessageStatus,
}

/// Forces a message back to `pending` (redeliverable, retries reset) or to
/// `failed` (no further deliveries).
#[post("/{ns_name}/{queue_name}/messages/{message_id}/status")]
async fn update_message_status(
    service: web::Data<Service>,
    path: web::Path<(String, String, u64)>,
    data: web::Json<UpdateMessageStatusRequest>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name, message_id) = &*path;

    service
        .admin_set_message_status(namespace, name, *message_id, data.into_inner().status, identity)
        .await?;

    Ok(HttpResponse::Ok())
}

/// Returns the queue's attributes in the SQS wire shape
/// (`{"VisibilityTimeout": "30", ...}`).
#[get("/{ns_name}/{queue_name}/attributes")]
async fn get_queue_attributes(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name) = &*path;

    let attributes = service
        .get_queue_attributes(namespace, name, &[], identity)
        .await?;

    Ok(web::Json(attributes))
}

/// Upserts queue attributes; same storage path as SQS `SetQueueAttributes`.
#[post("/{ns_name}/{queue_name}/attributes")]
async fn set_queue_attributes(
    service: web::Data<Service>,
    path: web::Path<(String, String)>,
    data: web::Json<QueueAttributesSer>,
    identity: Identity,
) -> Result<impl Responder, Error> {
    let (namespace, name) = &*path;

    service
        .set_queue_attributes(namespace, name, data.into_inner(), identity)
        .await?;

    Ok(HttpResponse::Ok())
}

pub fn service() -> Scope {
    web::scope("/queue")
        .service(list_all_queues)
        .service(list_ns_queues)
        .service(create_queue)
        .service(delete_queue)
        .service(queue_stats)
        .service(list_messages)
        .service(send_message)
        .service(delete_message)
        .service(update_message_status)
        .service(purge_queue)
        .service(get_queue_attributes)
        .service(set_queue_attributes)
        .service(get_queue_config)
        .service(update_queue_config)
}
