use std::collections::HashSet;

use actix_identity::Identity;
use actix_web::{post, web::Data, Responder, Scope};
use method::Method;
use tokio_stream::StreamExt;
use types::{
    change_message_visibility_batch::{
        ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResponse,
        ChangeMessageVisibilityBatchResultError, ChangeMessageVisibilityBatchResultSuccess,
    },
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

use crate::{
    auth::credential::{AuthorizedNamespace, Caller, KeyAccess},
    error::{AwsCode, Error},
};
use error::{aws_error_code, is_sender_fault, SqsError};

pub mod error;
pub mod limits;
pub mod method;
pub mod service;
pub mod types;

#[cfg(test)]
mod endpoint_tests;

#[cfg(test)]
mod sdk_tests;

#[cfg(test)]
mod key_tests;

#[cfg(test)]
mod span_tests;

#[cfg(all(test, feature = "otel"))]
mod telemetry_tests;

fn queue_url(mut host: Url, queue_name: &str, namespace_name: &str) -> Result<url::Url, Error> {
    host.path_segments_mut()
        .map_err(|_| Error::InternalServerError { source: None })?
        .push("api")
        .push("sqs")
        .push(namespace_name)
        .push(queue_name);
    Ok(host)
}

/// The namespace and queue a request's `QueueUrl` names, refusing a
/// namespace other than the one the credential is scoped to. Two handlers
/// once skipped that check (PurgeQueue and DeleteQueue), so a key for one
/// namespace could act on another's queues wherever its user had access.
///
/// Names the queue on the request's span too (`crate::telemetry`). The
/// handlers have no spans of their own: their arguments are message bodies,
/// receipt handles and tags, which telemetry must not carry.
fn target_queue<'a>(
    url: &'a Url,
    namespace: &AuthorizedNamespace,
) -> Result<(&'a str, &'a str), Error> {
    let mut path = url
        .path_segments()
        .ok_or_else(|| Error::missing_parameter("queue name"))?;

    let (queue_name, namespace_name) = path
        .next_back()
        .and_then(|queue_name| path.next_back().map(|ns_name| (queue_name, ns_name)))
        .ok_or_else(|| Error::missing_parameter("namespace name"))?;

    if namespace_name != namespace.0 {
        return Err(Error::Unauthorized);
    }

    record_target(namespace_name, Some(queue_name));
    Ok((namespace_name, queue_name))
}

/// Names the namespace, and the queue if there is one, on the request's span.
fn record_target(namespace_name: &str, queue_name: Option<&str>) {
    let span = tracing::Span::current();
    span.record("nervemq.namespace", namespace_name);
    if let Some(queue_name) = queue_name {
        span.record(
            "messaging.destination.name",
            format!("{namespace_name}/{queue_name}"),
        );
    }
}

async fn send_message(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: SendMessageRequest,
    trace_header: Option<&str>,
) -> Result<SqsResponse, Error> {
    // A copy: the request moves into the send while the names are in use.
    let queue_url = request.queue_url.clone();
    let (namespace_name, queue_name) = target_queue(&queue_url, &namespace)?;

    // Namespace, permission, queue and the caller's user id (recorded as
    // sent_by, surfaced as the SenderId system attribute) in one read.
    let authorized = service
        .resolve_authorized_queue(namespace_name, queue_name, &identity)
        .await?;

    let mut sent = sent_message(
        &request.message_body,
        &request.message_attributes,
        &request.message_system_attributes,
    );
    let res = service
        .sqs_send(authorized.queue_id, request, Some(authorized.user_id), trace_header)
        .await?;

    let span = tracing::Span::current();
    span.record("messaging.message.id", res.message_id.as_str());
    span.record("messaging.message.body.size", sent.body_bytes);
    sent.id = res.message_id.clone();
    service.telemetry().sent(
        crate::telemetry::Queue {
            namespace: namespace_name,
            name: queue_name,
        },
        &[sent],
    );
    Ok(SqsResponse::SendMessage(res))
}

/// What telemetry records of a message being sent: its size, and any
/// creation context the sender gave it. Never its content.
pub(crate) fn sent_message(
    body: &str,
    attributes: &std::collections::HashMap<String, types::SqsMessageAttribute>,
    system_attributes: &std::collections::HashMap<String, types::SqsMessageAttribute>,
) -> crate::telemetry::SentMessage {
    crate::telemetry::SentMessage {
        id: String::new(),
        body_bytes: body.len(),
        own_trace_header: types::string_attribute(system_attributes, types::AWS_TRACE_HEADER)
            .map(str::to_owned),
        own_traceparent: types::string_attribute(attributes, "traceparent").map(str::to_owned),
    }
}

async fn send_message_batch(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: SendMessageBatchRequest,
    trace_header: Option<&str>,
) -> Result<SqsResponse, Error> {
    // A copy: the request moves into the send while the names are in use.
    let queue_url = request.queue_url.clone();
    let (namespace_name, queue_name) = target_queue(&queue_url, &namespace)?;
    limits::check_batch(request.entries.iter().map(|entry| entry.id.as_str()))?;

    // Namespace, permission, queue and the caller's user id (recorded as
    // sent_by, surfaced as the SenderId system attribute) in one read.
    let authorized = service
        .resolve_authorized_queue(namespace_name, queue_name, &identity)
        .await?;

    let entries = request.entries.len();
    let mut sent: std::collections::HashMap<String, crate::telemetry::SentMessage> = request
        .entries
        .iter()
        .map(|entry| {
            let message = sent_message(
                &entry.message_body,
                &entry.message_attributes,
                &entry.message_system_attributes,
            );
            (entry.id.clone(), message)
        })
        .collect();
    let res = service
        .sqs_send_batch(
            namespace_name,
            queue_name,
            request,
            Some(authorized.user_id),
            trace_header,
        )
        .await?;

    tracing::Span::current().record("messaging.batch.message_count", entries);
    let stored: Vec<crate::telemetry::SentMessage> = res
        .successful
        .iter()
        .filter_map(|entry| {
            let message = sent.remove(&entry.id)?;
            Some(crate::telemetry::SentMessage {
                id: entry.message_id.clone(),
                ..message
            })
        })
        .collect();
    service.telemetry().sent(
        crate::telemetry::Queue {
            namespace: namespace_name,
            name: queue_name,
        },
        &stored,
    );
    Ok(SqsResponse::SendMessageBatch(res))
}

async fn receive_message(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: ReceiveMessageRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    // Rejected rather than clamped, as on AWS; checked before any database
    // work.
    if let Some(wait) = request.wait_time_seconds {
        if wait > MAX_WAIT_TIME_SECONDS {
            return Err(Error::invalid_parameter(format!(
                "WaitTimeSeconds: must be between 0 and {MAX_WAIT_TIME_SECONDS} seconds, got {wait}"
            )));
        }
    }

    /// Batch-size bounds accepted by AWS SQS (default 1).
    const MIN_NUMBER_OF_MESSAGES: u64 = 1;
    const MAX_NUMBER_OF_MESSAGES: u64 = 10;

    // Validated once, before any database work, rather than inside the
    // long-poll loop. Without this, a value past i64::MAX wraps to a negative
    // LIMIT, which SQLite treats as unbounded: one receive claims the queue.
    let max_number_of_messages = request.max_number_of_messages.unwrap_or(1);
    if !(MIN_NUMBER_OF_MESSAGES..=MAX_NUMBER_OF_MESSAGES).contains(&max_number_of_messages) {
        return Err(Error::invalid_parameter(format!(
            "MaxNumberOfMessages: must be between {MIN_NUMBER_OF_MESSAGES} and \
             {MAX_NUMBER_OF_MESSAGES}, got {max_number_of_messages}"
        )));
    }
    if let Some(visibility_timeout) = request.visibility_timeout {
        limits::check_range(
            "VisibilityTimeout",
            visibility_timeout,
            &limits::VISIBILITY_TIMEOUT,
            "seconds",
        )
        .map_err(Error::invalid_parameter)?;
    }

    // One read for namespace, permission and queue existence (a receive on
    // an unknown queue fails with QueueDoesNotExist, as on AWS, instead of
    // silently returning no messages).
    service
        .resolve_authorized_queue(namespace_name, queue_name, &identity)
        .await?;

    // Message attributes are filtered by `MessageAttributeNames`; system
    // attributes (SentTimestamp, ApproximateReceiveCount, ...) by
    // `AttributeNames` / `MessageSystemAttributeNames`.
    let attribute_names: HashSet<String> =
        HashSet::from_iter(request.message_attribute_names.into_iter());
    let system_attribute_names: HashSet<String> = request
        .attribute_names
        .into_iter()
        .chain(request.message_system_attribute_names)
        .collect();

    /// Maximum long-poll duration accepted by AWS SQS.
    const MAX_WAIT_TIME_SECONDS: u64 = 20;
    /// How often an empty long poll re-checks the queue.
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

    // Long polling: wait up to WaitTimeSeconds (request value, else the
    // queue's `receive_message_wait_time_seconds` attribute, else return
    // immediately) for at least one message, re-checking periodically. The
    // request value was range-checked above; the clamp still bounds a queue
    // attribute stored above 20.
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
                max_number_of_messages,
                request.visibility_timeout,
                attribute_names.clone(),
                system_attribute_names.clone(),
            )
            .await?;

        if !messages.is_empty()
            || tokio::time::Instant::now() + POLL_INTERVAL > deadline
            || service.stopping().is_cancelled()
        {
            break messages;
        }
        tokio::select! {
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
            // The server is stopping: one last look, then answer as an
            // empty queue would, rather than hold up the shutdown for up
            // to 20 s.
            _ = service.stopping().cancelled() => {}
        }
    };
    // Zero included: an empty receive's trace may be dropped
    // (`telemetry::otel::DropIdleReceives`).
    tracing::Span::current().record("messaging.batch.message_count", messages.len());

    Ok(SqsResponse::ReceiveMessage(ReceiveMessageResponse {
        messages,
    }))
}

async fn delete_message(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: DeleteMessageRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    service
        .delete_message(namespace_name, queue_name, &request.receipt_handle, identity)
        .await?;

    Ok(SqsResponse::DeleteMessage(DeleteMessageResponse {}))
}

async fn change_message_visibility(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::change_message_visibility::ChangeMessageVisibilityRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

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

async fn change_message_visibility_batch(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: ChangeMessageVisibilityBatchRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;
    limits::check_batch(request.entries.iter().map(|entry| entry.id.as_str()))?;

    tracing::Span::current().record("messaging.batch.message_count", request.entries.len());
    let entries = request
        .entries
        .into_iter()
        .map(|entry| (entry.id, entry.receipt_handle, entry.visibility_timeout))
        .collect();

    let (successful, failed) = service
        .change_message_visibility_batch(namespace_name, queue_name, entries, identity)
        .await?;

    Ok(SqsResponse::ChangeMessageVisibilityBatch(
        ChangeMessageVisibilityBatchResponse {
            successful: successful
                .into_iter()
                .map(|id| ChangeMessageVisibilityBatchResultSuccess { id })
                .collect(),
            failed: failed
                .into_iter()
                .map(|(id, err)| ChangeMessageVisibilityBatchResultError {
                    id,
                    code: aws_error_code(&err).code.to_string(),
                    message: err.to_string(),
                    sender_fault: is_sender_fault(&err),
                })
                .collect(),
        },
    ))
}

async fn delete_message_batch(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: DeleteMessageBatchRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;
    limits::check_batch(request.entries.iter().map(|entry| entry.id.as_str()))?;

    tracing::Span::current().record("messaging.batch.message_count", request.entries.len());
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
                code: aws_error_code(&err).code.to_string(),
                message: err.to_string(),
                sender_fault: is_sender_fault(&err),
            })
            .collect(),
    }))
}

async fn list_queues(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: ListQueuesRequest,
) -> Result<SqsResponse, Error> {
    record_target(&namespace.0, None);
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

async fn get_queue_url(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: GetQueueUrlRequest,
) -> Result<SqsResponse, Error> {
    record_target(&namespace.0, Some(&request.queue_name));
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

async fn create_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: CreateQueueRequest,
) -> Result<SqsResponse, Error> {
    record_target(&namespace.0, Some(&request.queue_name));
    let namespace_id = service
        .get_namespace_id(&namespace.0, service.db())
        .await?
        .ok_or_else(|| Error::namespace_not_found(&namespace.0))?;

    service
        .check_user_access(&identity, namespace_id, service.db())
        .await?;

    // As on AWS, re-creating an existing queue with matching attributes
    // succeeds and returns its URL.
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

async fn set_queue_attributes(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: SetQueueAttributesRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    service
        .set_queue_attributes(namespace_name, queue_name, request.attributes, identity)
        .await?;

    Ok(SqsResponse::SetQueueAttributes(
        SetQueueAttributesResponse {},
    ))
}

/// The queue-depth attributes SQS computes on request rather than stores
/// (#83), reported as strings like every attribute value on the wire.
pub const APPROXIMATE_NUMBER_OF_MESSAGES: &str = "ApproximateNumberOfMessages";
pub const APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE: &str =
    "ApproximateNumberOfMessagesNotVisible";
pub const APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED: &str =
    "ApproximateNumberOfMessagesDelayed";

const DEPTH_ATTRIBUTES: [&str; 3] = [
    APPROXIMATE_NUMBER_OF_MESSAGES,
    APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
    APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
];

/// Every attribute `GetQueueAttributes` can name, as AWS names them, besides
/// `All`.
const READABLE_ATTRIBUTES: [&str; 21] = [
    "Policy",
    "VisibilityTimeout",
    "MaximumMessageSize",
    "MessageRetentionPeriod",
    APPROXIMATE_NUMBER_OF_MESSAGES,
    APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
    "CreatedTimestamp",
    "LastModifiedTimestamp",
    "QueueArn",
    APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
    "DelaySeconds",
    "ReceiveMessageWaitTimeSeconds",
    "RedrivePolicy",
    "FifoQueue",
    "ContentBasedDeduplication",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
    "DeduplicationScope",
    "FifoThroughputLimit",
    "RedriveAllowPolicy",
    "SqsManagedSseEnabled",
];

/// The attributes AWS reports: the stored ones, with the default NerveMQ
/// applies in place of an unset integer attribute, and the computed ones
/// (depth, ARN, timestamps). With no names it reports nothing, and a name
/// AWS doesn't have is `InvalidAttributeName`, as on AWS.
async fn get_queue_attributes(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: GetQueueAttributesRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    let requested = request
        .attribute_names
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if let Some(unknown) = request
        .attribute_names
        .iter()
        .find(|name| name.as_str() != "All" && !READABLE_ATTRIBUTES.contains(&name.as_str()))
    {
        // AWS's wording.
        return Err(Error::aws(
            AwsCode::InvalidAttributeName,
            format!("Unknown Attribute {unknown}."),
        ));
    }

    // Read even when nothing is asked for: it checks access and that the
    // queue exists.
    let stored = service
        .get_queue_attributes(namespace_name, queue_name, &["All".to_owned()], &identity)
        .await?;
    if requested.is_empty() {
        return Ok(SqsResponse::GetQueueAttributes(GetQueueAttributesResponse {
            attributes: None,
        }));
    }

    let want_all = requested.contains("All");
    let wanted = |name: &str| want_all || requested.contains(name);
    let mut attributes = std::collections::BTreeMap::new();
    let mut report = |name: &str, value: String| {
        if wanted(name) {
            attributes.insert(name.to_owned(), value);
        }
    };

    report("DelaySeconds", stored.delay_seconds.unwrap_or(0).to_string());
    report(
        "MaximumMessageSize",
        stored
            .max_message_size
            .unwrap_or(types::MAX_MESSAGE_SIZE_BYTES as u64)
            .to_string(),
    );
    // NerveMQ keeps an unset queue's messages forever, which 0 says.
    report(
        "MessageRetentionPeriod",
        stored.message_retention_period.unwrap_or(0).to_string(),
    );
    report(
        "ReceiveMessageWaitTimeSeconds",
        stored.receive_message_wait_time_seconds.unwrap_or(0).to_string(),
    );
    report(
        "VisibilityTimeout",
        stored
            .visibility_timeout
            .unwrap_or(crate::config::defaults::VISIBILITY_TIMEOUT)
            .to_string(),
    );
    if let Some(policy) = stored.redrive_policy {
        report("RedrivePolicy", policy);
    }
    for (name, value) in stored.other {
        let value = match value {
            serde_json::Value::String(value) => value,
            value => value.to_string(),
        };
        report(&name, value);
    }
    report(
        "QueueArn",
        format!(
            "arn:aws:sqs:{}:{namespace_name}:{queue_name}",
            service.config().region()
        ),
    );

    if wanted("CreatedTimestamp") || wanted("LastModifiedTimestamp") {
        let (created, modified) = service.queue_times(namespace_name, queue_name).await?;
        if let Some(created) = created {
            report("CreatedTimestamp", created.to_string());
        }
        if let Some(modified) = modified {
            report("LastModifiedTimestamp", modified.to_string());
        }
    }

    // Computed here rather than stored, so the admin API's attribute editor,
    // which shows the stored set, never sees them.
    if DEPTH_ATTRIBUTES.iter().any(|name| wanted(name)) {
        let depth = service
            .queue_depth(namespace_name, queue_name, &identity)
            .await?;
        report(APPROXIMATE_NUMBER_OF_MESSAGES, depth.available.to_string());
        report(
            APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
            depth.not_visible.to_string(),
        );
        report(APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, depth.delayed.to_string());
    }

    Ok(SqsResponse::GetQueueAttributes(GetQueueAttributesResponse {
        attributes: Some(attributes),
    }))
}

async fn purge_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: PurgeQueueRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    // Errors propagate as AWS does: a refused or unknown purge used to come
    // back as a 200 with `"Success": false`, which SDKs read as success.
    service
        .purge_queue(namespace_name, queue_name, identity)
        .await?;

    Ok(SqsResponse::PurgeQueue(PurgeQueueResponse { success: true }))
}

async fn delete_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: DeleteQueueRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    service
        .delete_queue(namespace_name, queue_name, identity)
        .await?;

    Ok(SqsResponse::DeleteQueue(DeleteQueueResponse {}))
}

async fn list_queue_tags(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::list_queue_tags::ListQueueTagsRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    let tags = service
        .get_queue_tags(namespace_name, queue_name, identity)
        .await?;

    Ok(SqsResponse::ListQueueTags(
        types::list_queue_tags::ListQueueTagsResponse { tags },
    ))
}

async fn tag_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::tag_queue::TagQueueRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    service
        .tag_queue(namespace_name, queue_name, request.tags, identity)
        .await?;

    Ok(SqsResponse::TagQueue(types::tag_queue::TagQueueResponse {}))
}

async fn untag_queue(
    service: Data<crate::service::Service>,
    identity: Identity,
    namespace: AuthorizedNamespace,
    request: types::untag_queue::UntagQueueRequest,
) -> Result<SqsResponse, Error> {
    let (namespace_name, queue_name) = target_queue(&request.queue_url, &namespace)?;

    service
        .untag_queue(namespace_name, queue_name, request.tag_keys, identity)
        .await?;

    Ok(SqsResponse::UntagQueue(
        types::untag_queue::UntagQueueResponse {},
    ))
}

/// Maximum accepted size of an SQS request body (the HTTP payload, before
/// parsing).
///
/// This is a transport backstop, not the message-size limit: messages and
/// batch payloads are capped at [`types::MAX_MESSAGE_SIZE_BYTES`] (1 MiB)
/// after parsing. 8 MiB leaves room for the JSON envelope around the largest
/// legal payload even under heavy escaping (a control character costs six
/// bytes on the wire), so no compliant request is ever rejected here.
/// Anything bigger is refused before being buffered in full, as an invalid
/// parameter (400), the error AWS gives an oversized message.
const MAX_REQUEST_BODY_SIZE: usize = 8 * 1024 * 1024;

/// Deserializes a buffered SQS request body. An empty body reads as `{}`,
/// so a request that sends none is refused for the members it lacks.
fn parse_request<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Error> {
    let body = if body.is_empty() { b"{}".as_slice() } else { body };
    serde_json::from_slice(body).map_err(|e| match missing_member(&e) {
        // AWS's wording.
        Some(member) => Error::aws(
            AwsCode::MissingParameter,
            format!("The request must contain the parameter {member}."),
        ),
        None => Error::invalid_parameter(format!("invalid request body: {e}")),
    })
}

/// The member a request lacks, when that is why it failed to parse. serde
/// names it as it appears on the wire (`MessageBody`, `QueueUrl`, …).
fn missing_member(err: &serde_json::Error) -> Option<String> {
    let message = err.to_string();
    let member = message.strip_prefix("missing field `")?.split('`').next()?;
    Some(member.to_owned())
}

#[post("")]
pub async fn sqs_service(
    service: Data<crate::service::Service>,
    method: Method,
    mut payload: actix_web::web::Payload,
    // SQS callers are header-authenticated and sessionless; `Caller` yields
    // a detached `Identity` from the request extensions.
    caller: Caller,
    namespace: AuthorizedNamespace,
    access: KeyAccess,
    http: actix_web::HttpRequest,
) -> Result<impl Responder, SqsError> {
    let identity = caller.0;
    let trace_header = ambient_trace_header(&http);

    // The key's cap. Its owner's own level is checked by the service methods,
    // so a key manages queues only when both allow it.
    if method.manages_queues() && access < KeyAccess::Owner {
        return Err(Error::forbidden(
            "this API key has member access: it can send and receive messages, \
             not manage queues",
        )
        .into());
    }
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
            return Err(Error::PayloadTooLarge.into());
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
            send_message(service, identity, namespace, parse_request(&body)?, trace_header)
                .await?
        }
        Method::SendMessageBatch => {
            let request = parse_request(&body)?;
            send_message_batch(service, identity, namespace, request, trace_header).await?
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
        Method::ChangeMessageVisibilityBatch => {
            change_message_visibility_batch(service, identity, namespace, parse_request(&body)?)
                .await?
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

    // `Json` would label the body `application/json`; SDKs expect AWS's type.
    Ok(actix_web::HttpResponse::Ok()
        .content_type(error::AMZ_JSON)
        .json(res))
}

/// The request's `X-Amzn-Trace-Id`, which a send stores as its messages'
/// `AWSTraceHeader` when they don't set one, as AWS does. A header that isn't
/// text, or is over the cap, is ignored rather than failing the send: the
/// client didn't ask for it to be stored.
fn ambient_trace_header(request: &actix_web::HttpRequest) -> Option<&str> {
    request
        .headers()
        .get("x-amzn-trace-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= types::MAX_AWS_TRACE_HEADER_BYTES)
}

pub fn service() -> Scope {
    actix_web::web::scope("/sqs").service(sqs_service)
}
