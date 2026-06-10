use std::collections::HashSet;

use actix_identity::Identity;
use actix_web::{post, web::Data, Responder, Scope};
use method::Method;
use tokio_stream::StreamExt;
use tracing::instrument;
use types::{
    create_queue::{CreateQueueRequest, CreateQueueResponse},
    delete_message::{DeleteMessageRequest, DeleteMessageResponse},
    delete_message_batch::{
        DeleteMessageBatchRequest, DeleteMessageBatchResponse, DeleteMessageBatchResultError,
        DeleteMessageBatchResultSuccess,
    },
    delete_queue::{DeleteQueueRequest, DeleteQueueResponse},
    get_queue_attributes::{GetQueueAttributesRequest, GetQueueAttributesResponse},
    get_queue_url::{GetQueueUrlRequest, GetQueueUrlResponse},
    list_queues::{ListQueuesRequest, ListQueuesResponse},
    purge_queue::{PurgeQueueRequest, PurgeQueueResponse},
    receive_message::{ReceiveMessageRequest, ReceiveMessageResponse},
    send_message::SendMessageRequest,
    send_message_batch::SendMessageBatchRequest,
    set_queue_attributes::{SetQueueAttributesRequest, SetQueueAttributesResponse},
    SqsResponse,
};
use url::Url;

use crate::{auth::credential::AuthorizedNamespace, error::Error};

pub mod method;
pub mod service;
pub mod types;

#[cfg(test)]
mod endpoint_tests;

#[cfg(test)]
mod sdk_tests;

fn queue_url(mut host: Url, queue_name: &str, namespace_name: &str) -> Result<url::Url, Error> {
    host.path_segments_mut()
        .map_err(|_| Error::InternalServerError { source: None })?
        .push("api")
        .push("sqs")
        .push(namespace_name)
        .push(queue_name);
    Ok(host)
}

#[instrument(skip(service, identity))]
async fn send_message(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: SendMessageRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    let queue_id = service
        .get_queue_id(namespace_name, queue_name, service.db())
        .await?
        .ok_or_else(|| Error::queue_not_found(queue_name, namespace_name))?;

    let res = service.sqs_send(queue_id, request).await?;

    Ok(SqsResponse::SendMessage(res))
}

#[instrument(skip(service, identity))]
async fn send_message_batch(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: SendMessageBatchRequest,
) -> Result<SqsResponse, Error> {
    let queue_url = request.queue_url.clone();

    let mut path = queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    let res = service
        .sqs_send_batch(namespace_name, queue_name, request)
        .await?;

    Ok(SqsResponse::SendMessageBatch(res))
}

#[instrument(skip(service, identity))]
async fn receive_message(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: ReceiveMessageRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    // Message attributes are filtered by `MessageAttributeNames`
    // (`AttributeNames` selects *system* attributes).
    let attribute_names: HashSet<String> =
        HashSet::from_iter(request.message_attribute_names.into_iter());

    /// Maximum long-poll duration accepted by AWS SQS.
    const MAX_WAIT_TIME_SECONDS: u64 = 20;
    /// How often an empty long poll re-checks the queue.
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

    // Long polling: wait up to WaitTimeSeconds (request value, else the
    // queue's `receive_message_wait_time_seconds` attribute, else return
    // immediately) for at least one message, re-checking periodically.
    let wait_time_seconds = match request.wait_time_seconds {
        Some(wait) => wait,
        None => service
            .get_queue_attribute_u64(
                namespace_name,
                queue_name,
                "receive_message_wait_time_seconds",
            )
            .await?
            .unwrap_or(0),
    }
    .min(MAX_WAIT_TIME_SECONDS);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(wait_time_seconds);

    let messages = loop {
        let messages = service
            .sqs_recv_batch(
                namespace_name,
                queue_name,
                request.max_number_of_messages.unwrap_or(1) as u64,
                request.visibility_timeout,
                attribute_names.clone(),
            )
            .await?;

        if !messages.is_empty() || tokio::time::Instant::now() + POLL_INTERVAL > deadline {
            break messages;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    Ok(SqsResponse::ReceiveMessage(ReceiveMessageResponse {
        messages,
    }))
}

#[instrument(skip(service, identity))]
async fn delete_message(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: DeleteMessageRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    service
        .delete_message(namespace_name, queue_name, &request.receipt_handle, identity)
        .await?;

    Ok(SqsResponse::DeleteMessage(DeleteMessageResponse {}))
}

#[instrument(skip(service, identity))]
async fn change_message_visibility(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::change_message_visibility::ChangeMessageVisibilityRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    service
        .change_message_visibility(
            namespace_name,
            queue_name,
            &request.receipt_handle,
            request.visibility_timeout,
            identity,
        )
        .await?;

    Ok(SqsResponse::ChangeMessageVisibility(
        types::change_message_visibility::ChangeMessageVisibilityResponse {},
    ))
}

#[instrument(skip(service, identity))]
async fn delete_message_batch(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: DeleteMessageBatchRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    let entries = request
        .entries
        .into_iter()
        .map(|entry| (entry.id, entry.receipt_handle))
        .collect();

    let (successful, failed) = service
        .delete_message_batch(namespace_name, queue_name, entries, identity)
        .await?;

    Ok(SqsResponse::DeleteMessageBatch(DeleteMessageBatchResponse {
        successful: successful
            .into_iter()
            .map(|id| DeleteMessageBatchResultSuccess { id })
            .collect(),
        failed: failed
            .into_iter()
            .map(|(id, err)| DeleteMessageBatchResultError {
                id,
                // Per-entry failures are stale/unknown receipt handles; the
                // matching AWS error code is the sender's fault.
                code: "ReceiptHandleIsInvalid".to_string(),
                message: err.to_string(),
                sender_fault: true,
            })
            .collect(),
    }))
}

#[instrument(skip(service, identity))]
async fn list_queues(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: ListQueuesRequest,
) -> Result<SqsResponse, Error> {
    let namespace_id = service
        .get_namespace_id(&namespace.0, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(&namespace.0))?;

    service
        .check_user_access(&identity, namespace_id, service.db())
        .await?;

    let queues = service
        .list_queues(Some(&namespace.0), identity)
        .await?
        .into_iter()
        .filter(|queue| {
            if let Some(prefix) = &request.queue_name_prefix {
                queue.name.starts_with(prefix)
            } else {
                true
            }
        });

    let mut urls = Vec::new();

    for queue in queues {
        urls.push(queue_url(
            service.config().host(),
            &queue.name,
            &namespace.0,
        )?);
    }

    Ok(SqsResponse::ListQueues(ListQueuesResponse {
        queue_urls: urls,
    }))
}

#[instrument(skip(service, identity))]
async fn get_queue_url(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: GetQueueUrlRequest,
) -> Result<SqsResponse, Error> {
    let namespace_id = service
        .get_namespace_id(&namespace.0, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(&namespace.0))?;

    service
        .check_user_access(&identity, namespace_id, service.db())
        .await?;

    service
        .get_queue_id(&namespace.0, &request.queue_name, service.db())
        .await?
        .ok_or_else(|| Error::queue_not_found(&request.queue_name, &namespace.0))?;

    let url = queue_url(service.config().host(), &request.queue_name, &namespace.0)?;

    Ok(SqsResponse::GetQueueUrl(GetQueueUrlResponse {
        queue_url: url,
    }))
}

#[instrument(skip(service, identity))]
async fn create_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: CreateQueueRequest,
) -> Result<SqsResponse, Error> {
    let namespace_id = service
        .get_namespace_id(&namespace.0, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(&namespace.0))?;

    service
        .check_user_access(&identity, namespace_id, service.db())
        .await?;

    service
        .create_queue(
            &namespace.0,
            &request.queue_name,
            request.attributes,
            request.tags,
            identity,
        )
        .await?;

    let url = queue_url(service.config().host(), &request.queue_name, &namespace.0)?;

    Ok(SqsResponse::CreateQueue(CreateQueueResponse {
        queue_url: url,
    }))
}

#[instrument(skip(service, identity))]
async fn set_queue_attributes(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: SetQueueAttributesRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    service
        .set_queue_attributes(namespace_name, queue_name, request.attributes, identity)
        .await?;

    Ok(SqsResponse::SetQueueAttributes(
        SetQueueAttributesResponse {},
    ))
}

#[instrument(skip(service, identity))]
async fn get_queue_attributes(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: GetQueueAttributesRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    let attributes = service
        .get_queue_attributes(
            namespace_name,
            queue_name,
            &request.attribute_names,
            identity,
        )
        .await?;

    Ok(SqsResponse::GetQueueAttributes(
        GetQueueAttributesResponse { attributes },
    ))
}

#[instrument(skip(service, identity))]
async fn purge_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    _namespace: AuthorizedNamespace,
    request: PurgeQueueRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    let success = service
        .purge_queue(namespace_name, queue_name, identity)
        .await
        .is_ok();

    Ok(SqsResponse::PurgeQueue(PurgeQueueResponse { success }))
}

#[instrument(skip(service, identity))]
async fn delete_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    _namespace: AuthorizedNamespace,
    request: DeleteQueueRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    service
        .delete_queue(namespace_name, queue_name, identity)
        .await?;

    Ok(SqsResponse::DeleteQueue(DeleteQueueResponse {}))
}

#[instrument(skip(service, identity))]
async fn list_queue_tags(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::list_queue_tags::ListQueueTagsRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    let ns_id = service
        .get_namespace_id(namespace_name, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(namespace_name))?;

    service
        .check_user_access(&identity, ns_id, service.db())
        .await?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    let tags = service
        .get_queue_tags(namespace_name, queue_name, identity)
        .await?;

    Ok(SqsResponse::ListQueueTags(
        types::list_queue_tags::ListQueueTagsResponse { tags },
    ))
}

#[instrument(skip(service, identity))]
async fn tag_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::tag_queue::TagQueueRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    service
        .tag_queue(namespace_name, queue_name, request.tags, identity)
        .await?;

    Ok(SqsResponse::TagQueue(types::tag_queue::TagQueueResponse {}))
}

#[instrument(skip(service, identity))]
async fn untag_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::untag_queue::UntagQueueRequest,
) -> Result<SqsResponse, Error> {
    let mut path = request
        .queue_url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    service
        .untag_queue(namespace_name, queue_name, request.tag_keys, identity)
        .await?;

    Ok(SqsResponse::UntagQueue(
        types::untag_queue::UntagQueueResponse {},
    ))
}

/// Maximum accepted size of an SQS request body.
///
/// AWS caps a message — body plus attributes — at 256 KiB, and a batch
/// request's combined payload at the same limit; 512 KiB leaves room for the
/// JSON envelope (queue URL, attribute structure, escaping) around the
/// largest legal payload. Anything bigger is rejected with 413 before
/// parsing.
const MAX_REQUEST_BODY_SIZE: usize = 512 * 1024;

/// Deserializes a buffered SQS request body.
fn parse_request<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Error> {
    if body.is_empty() {
        return Err(Error::missing_parameter("missing request body"));
    }
    serde_json::from_slice(body)
        .map_err(|e| Error::invalid_parameter(format!("invalid request body: {e}")))
}

#[post("")]
pub async fn sqs_service(
    service: Data<crate::service::Service>,
    method: Method,
    mut payload: actix_web::web::Payload,
    identity: Identity,
    namespace: AuthorizedNamespace,
) -> Result<impl Responder, Error> {
    // Buffer the whole request body (bounded) before deserializing. The body
    // is a single JSON document with no message framing on the wire, so it
    // can only be parsed once complete — network reads chunk it at arbitrary
    // boundaries. (A previous streaming decoder treated the first read —
    // capped at 8 KiB — as a complete JSON frame and returned 500 for any
    // request larger than that.)
    let mut body = actix_web::web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| Error::internal(eyre::eyre!("{e}")))?;
        if body.len() + chunk.len() > MAX_REQUEST_BODY_SIZE {
            return Err(Error::PayloadTooLarge);
        }
        body.extend_from_slice(&chunk);
    }

    let res = match method {
        Method::DeleteMessageBatch => {
            delete_message_batch(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::SetQueueAttributes => {
            set_queue_attributes(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::TagQueue => {
            tag_queue(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::UntagQueue => {
            untag_queue(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::ListQueueTags => {
            list_queue_tags(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::DeleteQueue => {
            delete_queue(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::SendMessage => {
            send_message(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::SendMessageBatch => {
            send_message_batch(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::ReceiveMessage => {
            receive_message(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::DeleteMessage => {
            delete_message(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::ChangeMessageVisibility => {
            change_message_visibility(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::ListQueues => {
            list_queues(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::GetQueueUrl => {
            get_queue_url(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::CreateQueue => {
            create_queue(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::GetQueueAttributes => {
            get_queue_attributes(service, identity, namespace, parse_request(&body)?).await?
        }
        Method::PurgeQueue => {
            purge_queue(service, identity, namespace, parse_request(&body)?).await?
        }
    };

    Ok(actix_web::web::Json(res))
}

pub fn service() -> Scope {
    actix_web::web::scope("/sqs").service(sqs_service)
}
