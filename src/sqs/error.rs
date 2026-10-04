//! AWS-style error responses for the SQS API.
//!
//! AWS SDKs read an error's code from the JSON body's `__type` and, because
//! SQS is an `awsQueryCompatible` service, from the `x-amzn-query-error`
//! header, which takes precedence. Typed SDK errors match on the header's
//! query-protocol code, e.g. `QueueDoesNotExist` is raised for
//! `AWS.SimpleQueueService.NonExistentQueue`.
//!
//! Each error also takes AWS's HTTP status for its code, which can differ
//! from the status the same [`Error`] has on the admin API: a missing queue
//! is 400 here and 404 there, a refusal 403 here and 401 there. The SQS
//! model gives the statuses of SQS's own errors; AWS's common errors and
//! SigV4 documentation give those of the rest.

use std::fmt;

use actix_web::{http::StatusCode, HttpResponse, ResponseError};

use crate::error::{AwsCode, BatchFault, Error};

/// The content type of every SQS response, as AWS's JSON protocol sends it.
pub const AMZ_JSON: &str = "application/x-amz-json-1.0";

/// An error's identity as AWS SDKs see it.
#[derive(Debug, PartialEq, Eq)]
pub struct AwsErrorCode {
    /// Error shape name, sent namespaced in the JSON body's `__type`.
    pub shape: &'static str,
    /// Query-protocol code, sent in the `x-amzn-query-error` header and in
    /// batch result entries. Differs from `shape` only for errors whose
    /// code predates the JSON protocol.
    pub code: &'static str,
    /// The HTTP status AWS answers with.
    pub status: StatusCode,
}

impl AwsErrorCode {
    const fn new(shape: &'static str, code: &'static str, status: StatusCode) -> Self {
        Self {
            shape,
            code,
            status,
        }
    }

    const fn same(code: &'static str, status: StatusCode) -> Self {
        Self::new(code, code, status)
    }
}

/// A refusal. SDKs show the header's `AccessDenied`, as for AWS's own
/// "Access to the resource ... is denied".
const ACCESS_DENIED: AwsErrorCode =
    AwsErrorCode::new("AccessDeniedException", "AccessDenied", StatusCode::FORBIDDEN);

/// Maps an error to the AWS code an SQS client expects for it. Codes are
/// SQS's modeled errors or AWS's common errors; the match is exhaustive so a
/// new variant has to pick one.
pub fn aws_error_code(err: &Error) -> AwsErrorCode {
    match err {
        Error::QueueNotFound { .. } => AwsErrorCode::new(
            "QueueDoesNotExist",
            "AWS.SimpleQueueService.NonExistentQueue",
            StatusCode::BAD_REQUEST,
        ),
        Error::InvalidReceiptHandle { .. } => {
            AwsErrorCode::same("ReceiptHandleIsInvalid", StatusCode::NOT_FOUND)
        }
        Error::QueueAlreadyExists { .. } => AwsErrorCode::new(
            "QueueNameExists",
            "QueueAlreadyExists",
            StatusCode::BAD_REQUEST,
        ),
        Error::NotFound { .. } => {
            AwsErrorCode::same("ResourceNotFoundException", StatusCode::NOT_FOUND)
        }
        // A request body over the transport cap is one AWS would refuse as
        // an oversized message. `Conflict` only comes from the admin API
        // (e.g. removing the last admin).
        Error::InvalidParameter { .. }
        | Error::InvalidHeader { .. }
        | Error::PayloadTooLarge
        | Error::Conflict { .. } => AwsErrorCode::new(
            "InvalidParameterValueException",
            "InvalidParameterValue",
            StatusCode::BAD_REQUEST,
        ),
        Error::MissingParameter { .. } | Error::MissingHeader { .. } => AwsErrorCode::new(
            "MissingRequiredParameterException",
            "MissingParameter",
            StatusCode::BAD_REQUEST,
        ),
        Error::InvalidAttributeValue { .. } => {
            AwsErrorCode::same("InvalidAttributeValue", StatusCode::BAD_REQUEST)
        }
        Error::InvalidMethod { .. } => AwsErrorCode::same("InvalidAction", StatusCode::BAD_REQUEST),
        Error::InvalidBatch { fault, .. } => batch_error_code(*fault),
        Error::Aws { code, .. } => match code {
            AwsCode::MissingParameter => AwsErrorCode::new(
                "MissingRequiredParameterException",
                "MissingParameter",
                StatusCode::BAD_REQUEST,
            ),
            AwsCode::InvalidParameterValue => AwsErrorCode::new(
                "InvalidParameterValueException",
                "InvalidParameterValue",
                StatusCode::BAD_REQUEST,
            ),
            AwsCode::InvalidAttributeName => {
                AwsErrorCode::same("InvalidAttributeName", StatusCode::BAD_REQUEST)
            }
            AwsCode::InvalidAttributeValue => {
                AwsErrorCode::same("InvalidAttributeValue", StatusCode::BAD_REQUEST)
            }
            AwsCode::InvalidMessageContents => {
                AwsErrorCode::same("InvalidMessageContents", StatusCode::BAD_REQUEST)
            }
            AwsCode::ReceiptHandleIsInvalid => {
                AwsErrorCode::same("ReceiptHandleIsInvalid", StatusCode::NOT_FOUND)
            }
        },
        Error::Unauthorized
        | Error::Forbidden { .. }
        | Error::UserNotFound { .. }
        | Error::IdentityNotFound { .. } => ACCESS_DENIED,
        Error::SignatureExpired { .. } => {
            AwsErrorCode::same("SignatureDoesNotMatch", StatusCode::FORBIDDEN)
        }
        Error::InternalServerError { .. }
        | Error::Sqlx { .. }
        | Error::MigrationError { .. }
        | Error::Whatever { .. } => {
            AwsErrorCode::same("InternalFailure", StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn batch_error_code(fault: BatchFault) -> AwsErrorCode {
    let (shape, code) = match fault {
        BatchFault::Empty => (
            "EmptyBatchRequest",
            "AWS.SimpleQueueService.EmptyBatchRequest",
        ),
        BatchFault::TooManyEntries => (
            "TooManyEntriesInBatchRequest",
            "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
        ),
        BatchFault::IdsNotDistinct => (
            "BatchEntryIdsNotDistinct",
            "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
        ),
        BatchFault::InvalidEntryId => (
            "InvalidBatchEntryId",
            "AWS.SimpleQueueService.InvalidBatchEntryId",
        ),
        BatchFault::TooLong => (
            "BatchRequestTooLong",
            "AWS.SimpleQueueService.BatchRequestTooLong",
        ),
    };
    AwsErrorCode::new(shape, code, StatusCode::BAD_REQUEST)
}

/// Whether the caller, rather than the server, is at fault.
pub fn is_sender_fault(err: &Error) -> bool {
    aws_error_code(err).status.is_client_error()
}

/// An [`Error`] rendered as an AWS JSON-protocol error response, with AWS's
/// HTTP status for its code.
#[derive(Debug)]
pub struct SqsError(pub Error);

impl From<Error> for SqsError {
    fn from(err: Error) -> Self {
        Self(err)
    }
}

impl fmt::Display for SqsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ResponseError for SqsError {
    fn status_code(&self) -> StatusCode {
        aws_error_code(&self.0).status
    }

    fn error_response(&self) -> HttpResponse {
        let AwsErrorCode {
            shape,
            code,
            status,
        } = aws_error_code(&self.0);
        let fault = if status.is_client_error() {
            "Sender"
        } else {
            "Receiver"
        };
        aws_error_response(status, shape, code, fault, &self.0.to_string())
    }
}

/// An AWS JSON-protocol error response: the code in the JSON body's `__type`
/// and in the `x-amzn-query-error` header, which SDKs read first.
fn aws_error_response(
    status: StatusCode,
    shape: &str,
    code: &str,
    fault: &str,
    message: &str,
) -> HttpResponse {
    HttpResponse::build(status)
        .content_type(AMZ_JSON)
        .insert_header(("x-amzn-query-error", format!("{code};{fault}")))
        .body(
            serde_json::json!({
                "__type": format!("com.amazonaws.sqs#{shape}"),
                "message": message,
            })
            .to_string(),
        )
}

/// Why a request failed authentication, as AWS names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFailure {
    /// No credentials at all: no `Authorization` header and no session.
    MissingAuthenticationToken,
    /// An `Authorization` header that cannot be parsed, or one that signs a
    /// header the request does not carry.
    IncompleteSignature,
    /// An access key that does not exist, or whose user is disabled.
    InvalidClientTokenId,
    /// A SigV4 signature that does not match the request.
    SignatureDoesNotMatch,
    /// Any other refusal, e.g. a NerveMQ-scheme key with the wrong secret.
    AccessDenied,
}

impl AuthFailure {
    /// AWS's code and status for the failure: 400 for a header it can't
    /// read, 403 for the rest.
    pub fn aws_error_code(&self) -> AwsErrorCode {
        match self {
            AuthFailure::MissingAuthenticationToken => {
                AwsErrorCode::same("MissingAuthenticationToken", StatusCode::FORBIDDEN)
            }
            AuthFailure::IncompleteSignature => {
                AwsErrorCode::same("IncompleteSignature", StatusCode::BAD_REQUEST)
            }
            AuthFailure::InvalidClientTokenId => {
                AwsErrorCode::same("InvalidClientTokenId", StatusCode::FORBIDDEN)
            }
            AuthFailure::SignatureDoesNotMatch => {
                AwsErrorCode::same("SignatureDoesNotMatch", StatusCode::FORBIDDEN)
            }
            AuthFailure::AccessDenied => ACCESS_DENIED,
        }
    }
}

/// A failed authentication on the SQS API, in the same AWS JSON format and
/// with the same status as AWS. It used to be a plain-text 401, which SDKs
/// could not parse: they reported an unhandled error with no code.
#[derive(Debug)]
pub struct SqsAuthError {
    pub failure: AuthFailure,
    pub message: String,
}

impl fmt::Display for SqsAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl ResponseError for SqsAuthError {
    fn status_code(&self) -> StatusCode {
        self.failure.aws_error_code().status
    }

    fn error_response(&self) -> HttpResponse {
        let AwsErrorCode {
            shape,
            code,
            status,
        } = self.failure.aws_error_code();
        aws_error_response(status, shape, code, "Sender", &self.message)
    }
}

/// Whether a request path belongs to the SQS API.
pub fn is_sqs_path(path: &str) -> bool {
    path == "/api/sqs" || path.starts_with("/api/sqs/")
}

/// A failed authentication for a request to `path`: in AWS's format and
/// status on the SQS API, a plain 401 on the admin API. The authentication middlewares
/// serve both, so they decide by path.
pub fn auth_failure(path: &str, failure: AuthFailure, message: impl fmt::Display) -> actix_web::Error {
    if is_sqs_path(path) {
        SqsAuthError {
            failure,
            message: message.to_string(),
        }
        .into()
    } else {
        actix_web::error::ErrorUnauthorized(message.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Renders `err` and returns its status, `x-amzn-query-error` header,
    /// content type and JSON body.
    async fn render(err: Error) -> (StatusCode, String, String, serde_json::Value) {
        let resp = SqsError(err).error_response();
        let header = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        };
        let (query_error, content_type) = (header("x-amzn-query-error"), header("content-type"));
        let status = resp.status();
        let bytes = actix_web::body::to_bytes(resp.into_body()).await.unwrap();
        (
            status,
            query_error,
            content_type,
            serde_json::from_slice(&bytes).unwrap(),
        )
    }

    /// Refusals over SQS look like AWS's: AccessDenied, the sender's fault,
    /// with AWS's 403 whatever NerveMQ's admin API answers.
    #[actix_web::test]
    async fn refusals_render_as_access_denied() {
        for err in [
            Error::Unauthorized,
            Error::forbidden("member key"),
            Error::IdentityNotFound {
                key_id: "AKID".into(),
            },
        ] {
            let (status, query_error, _, body) = render(err).await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(query_error, "AccessDenied;Sender");
            assert_eq!(body["__type"], "com.amazonaws.sqs#AccessDeniedException");
        }
    }

    #[actix_web::test]
    async fn invalid_parameter_renders_an_aws_error_envelope() {
        let (status, query_error, content_type, body) =
            render(Error::invalid_parameter("VisibilityTimeout: out of range")).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(query_error, "InvalidParameterValue;Sender");
        assert_eq!(content_type, "application/x-amz-json-1.0");
        // AWS's shape name; SDKs show the header's code.
        assert_eq!(
            body["__type"],
            "com.amazonaws.sqs#InvalidParameterValueException"
        );
        assert_eq!(
            body["message"],
            "Invalid parameter: VisibilityTimeout: out of range"
        );
    }

    #[actix_web::test]
    async fn missing_queue_uses_the_legacy_code_typed_sdk_errors_match() {
        let (status, query_error, _, body) = render(Error::queue_not_found("q", "ns")).await;

        // AWS's 400, where the admin API answers 404.
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(query_error, "AWS.SimpleQueueService.NonExistentQueue;Sender");
        assert_eq!(body["__type"], "com.amazonaws.sqs#QueueDoesNotExist");
    }

    #[actix_web::test]
    async fn server_errors_are_the_receivers_fault() {
        let (status, query_error, _, body) = render(Error::opaque()).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(query_error, "InternalFailure;Receiver");
        assert_eq!(body["message"], "Internal server error");
    }

    /// Each error's code and status are AWS's for it: SQS's own errors as
    /// its API model gives them, the rest as AWS's common errors do.
    #[test]
    fn codes_and_statuses_are_aws_s() {
        use StatusCode as S;
        let batch = |fault| Error::invalid_batch(fault, "x");
        for (err, code, status) in [
            (
                Error::queue_not_found("q", "ns"),
                "AWS.SimpleQueueService.NonExistentQueue",
                S::BAD_REQUEST,
            ),
            (
                Error::invalid_receipt_handle("x"),
                "ReceiptHandleIsInvalid",
                S::NOT_FOUND,
            ),
            (
                Error::QueueAlreadyExists {
                    queue: "q".into(),
                    namespace: "ns".into(),
                    attribute: "a".into(),
                },
                "QueueAlreadyExists",
                S::BAD_REQUEST,
            ),
            (Error::namespace_not_found("ns"), "ResourceNotFoundException", S::NOT_FOUND),
            (Error::invalid_parameter("x"), "InvalidParameterValue", S::BAD_REQUEST),
            (Error::PayloadTooLarge, "InvalidParameterValue", S::BAD_REQUEST),
            (Error::conflict("x"), "InvalidParameterValue", S::BAD_REQUEST),
            (Error::missing_parameter("x"), "MissingParameter", S::BAD_REQUEST),
            (
                Error::invalid_attribute_value("x"),
                "InvalidAttributeValue",
                S::BAD_REQUEST,
            ),
            (
                Error::InvalidMethod { message: "x".into() },
                "InvalidAction",
                S::BAD_REQUEST,
            ),
            (
                batch(BatchFault::Empty),
                "AWS.SimpleQueueService.EmptyBatchRequest",
                S::BAD_REQUEST,
            ),
            (
                batch(BatchFault::TooManyEntries),
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
                S::BAD_REQUEST,
            ),
            (
                batch(BatchFault::IdsNotDistinct),
                "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
                S::BAD_REQUEST,
            ),
            (
                batch(BatchFault::InvalidEntryId),
                "AWS.SimpleQueueService.InvalidBatchEntryId",
                S::BAD_REQUEST,
            ),
            (
                batch(BatchFault::TooLong),
                "AWS.SimpleQueueService.BatchRequestTooLong",
                S::BAD_REQUEST,
            ),
            (
                Error::aws(AwsCode::MissingParameter, "x"),
                "MissingParameter",
                S::BAD_REQUEST,
            ),
            (
                Error::aws(AwsCode::InvalidParameterValue, "x"),
                "InvalidParameterValue",
                S::BAD_REQUEST,
            ),
            (
                Error::aws(AwsCode::InvalidAttributeName, "x"),
                "InvalidAttributeName",
                S::BAD_REQUEST,
            ),
            (
                Error::aws(AwsCode::InvalidAttributeValue, "x"),
                "InvalidAttributeValue",
                S::BAD_REQUEST,
            ),
            (
                Error::aws(AwsCode::InvalidMessageContents, "x"),
                "InvalidMessageContents",
                S::BAD_REQUEST,
            ),
            (
                Error::aws(AwsCode::ReceiptHandleIsInvalid, "x"),
                "ReceiptHandleIsInvalid",
                S::NOT_FOUND,
            ),
            (Error::Unauthorized, "AccessDenied", S::FORBIDDEN),
            (Error::forbidden("x"), "AccessDenied", S::FORBIDDEN),
            (
                Error::UserNotFound { email: "x".into() },
                "AccessDenied",
                S::FORBIDDEN,
            ),
            (
                Error::SignatureExpired { message: "x".into() },
                "SignatureDoesNotMatch",
                S::FORBIDDEN,
            ),
            (Error::opaque(), "InternalFailure", S::INTERNAL_SERVER_ERROR),
        ] {
            let aws = aws_error_code(&err);
            assert_eq!((aws.code, aws.status), (code, status), "{err:?}");
            assert_eq!(SqsError(err).status_code(), status);
        }
    }

    /// AWS answers a header it can't read with 400 and every other failed
    /// authentication with 403.
    #[test]
    fn auth_failures_take_aws_s_statuses() {
        for (failure, status) in [
            (AuthFailure::MissingAuthenticationToken, StatusCode::FORBIDDEN),
            (AuthFailure::IncompleteSignature, StatusCode::BAD_REQUEST),
            (AuthFailure::InvalidClientTokenId, StatusCode::FORBIDDEN),
            (AuthFailure::SignatureDoesNotMatch, StatusCode::FORBIDDEN),
            (AuthFailure::AccessDenied, StatusCode::FORBIDDEN),
        ] {
            let err = SqsAuthError {
                failure,
                message: "x".into(),
            };
            assert_eq!(err.status_code(), status, "{failure:?}");
            assert_eq!(err.error_response().status(), status, "{failure:?}");
        }
    }

    /// The admin API keeps its own statuses: the UI branches on 401 (log in
    /// again) and 404.
    #[test]
    fn the_admin_api_keeps_its_statuses() {
        assert_eq!(Error::Unauthorized.status_code(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            Error::queue_not_found("q", "ns").status_code(),
            StatusCode::NOT_FOUND
        );
    }
}
