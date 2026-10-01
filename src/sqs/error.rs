//! AWS-style error responses for the SQS API.
//!
//! AWS SDKs read an error's code from the JSON body's `__type` and, because
//! SQS is an `awsQueryCompatible` service, from the `x-amzn-query-error`
//! header, which takes precedence. Typed SDK errors match on the header's
//! query-protocol code, e.g. `QueueDoesNotExist` is raised for
//! `AWS.SimpleQueueService.NonExistentQueue`.

use std::fmt;

use actix_web::{http::StatusCode, HttpResponse, ResponseError};

use crate::error::Error;

/// An error's identity as AWS SDKs see it.
#[derive(Debug, PartialEq, Eq)]
pub struct AwsErrorCode {
    /// Error shape name, sent namespaced in the JSON body's `__type`.
    pub shape: &'static str,
    /// Query-protocol code, sent in the `x-amzn-query-error` header and in
    /// batch result entries. Differs from `shape` only for errors whose
    /// code predates the JSON protocol.
    pub code: &'static str,
}

impl AwsErrorCode {
    const fn same(code: &'static str) -> Self {
        Self { shape: code, code }
    }
}

/// Maps an error to the AWS code an SQS client expects for it. Codes are
/// SQS's modeled errors or AWS's common errors; the match is exhaustive so a
/// new variant has to pick one.
pub fn aws_error_code(err: &Error) -> AwsErrorCode {
    match err {
        Error::QueueNotFound { .. } => AwsErrorCode {
            shape: "QueueDoesNotExist",
            code: "AWS.SimpleQueueService.NonExistentQueue",
        },
        Error::InvalidReceiptHandle { .. } => AwsErrorCode::same("ReceiptHandleIsInvalid"),
        Error::QueueAlreadyExists { .. } => AwsErrorCode {
            shape: "QueueNameExists",
            code: "QueueAlreadyExists",
        },
        Error::NotFound { .. } => AwsErrorCode::same("ResourceNotFoundException"),
        Error::InvalidParameter { .. } | Error::InvalidHeader { .. } | Error::PayloadTooLarge => {
            AwsErrorCode::same("InvalidParameterValue")
        }
        Error::MissingParameter { .. } | Error::MissingHeader { .. } => {
            AwsErrorCode::same("MissingParameter")
        }
        Error::InvalidAttributeValue { .. } => AwsErrorCode::same("InvalidAttributeValue"),
        Error::InvalidMethod { .. } => AwsErrorCode::same("InvalidAction"),
        Error::Unauthorized
        | Error::Forbidden { .. }
        | Error::UserNotFound { .. }
        | Error::IdentityNotFound { .. } => AwsErrorCode::same("AccessDeniedException"),
        Error::SignatureExpired { .. } => AwsErrorCode::same("SignatureDoesNotMatch"),
        // Only the admin API raises it (e.g. removing the last admin).
        Error::Conflict { .. } => AwsErrorCode::same("InvalidParameterValue"),
        Error::InternalServerError { .. }
        | Error::Sqlx { .. }
        | Error::MigrationError { .. }
        | Error::Whatever { .. } => AwsErrorCode::same("InternalFailure"),
    }
}

/// Whether the caller, rather than the server, is at fault.
pub fn is_sender_fault(err: &Error) -> bool {
    err.status_code().is_client_error()
}

/// An [`Error`] rendered as an AWS JSON-protocol error response. HTTP status
/// codes are unchanged from [`Error`]'s.
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
        self.0.status_code()
    }

    fn error_response(&self) -> HttpResponse {
        let AwsErrorCode { shape, code } = aws_error_code(&self.0);
        let fault = if is_sender_fault(&self.0) {
            "Sender"
        } else {
            "Receiver"
        };
        aws_error_response(self.status_code(), shape, code, fault, &self.0.to_string())
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
        .content_type("application/x-amz-json-1.0")
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
    pub fn code(&self) -> &'static str {
        match self {
            AuthFailure::MissingAuthenticationToken => "MissingAuthenticationToken",
            AuthFailure::IncompleteSignature => "IncompleteSignature",
            AuthFailure::InvalidClientTokenId => "InvalidClientTokenId",
            AuthFailure::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            AuthFailure::AccessDenied => "AccessDeniedException",
        }
    }
}

/// A failed authentication on the SQS API, in the same AWS JSON format as
/// every other SQS error. It used to be a plain-text 401, which SDKs could
/// not parse: they reported an unhandled error with no code. The status stays
/// NerveMQ's 401 (AWS sends 400 or 403), as other errors keep theirs.
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
        StatusCode::UNAUTHORIZED
    }

    fn error_response(&self) -> HttpResponse {
        let code = self.failure.code();
        aws_error_response(self.status_code(), code, code, "Sender", &self.message)
    }
}

/// Whether a request path belongs to the SQS API.
pub fn is_sqs_path(path: &str) -> bool {
    path == "/api/sqs" || path.starts_with("/api/sqs/")
}

/// A failed authentication for a request to `path`: in AWS's format on the
/// SQS API, a plain 401 on the admin API. The authentication middlewares
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

    /// Refusals over SQS look like AWS's: AccessDeniedException, the sender's
    /// fault, with the status NerveMQ uses on its own API.
    #[actix_web::test]
    async fn refusals_render_as_access_denied() {
        for (err, status) in [
            (Error::Unauthorized, StatusCode::UNAUTHORIZED),
            (Error::forbidden("member key"), StatusCode::FORBIDDEN),
            (
                Error::IdentityNotFound {
                    key_id: "AKID".into(),
                },
                StatusCode::UNAUTHORIZED,
            ),
        ] {
            let (got, query_error, _, body) = render(err).await;
            assert_eq!(got, status);
            assert_eq!(query_error, "AccessDeniedException;Sender");
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
        assert_eq!(body["__type"], "com.amazonaws.sqs#InvalidParameterValue");
        assert_eq!(
            body["message"],
            "Invalid parameter: VisibilityTimeout: out of range"
        );
    }

    #[actix_web::test]
    async fn missing_queue_uses_the_legacy_code_typed_sdk_errors_match() {
        let (status, query_error, _, body) = render(Error::queue_not_found("q", "ns")).await;

        // The status is NerveMQ's 404; AWS itself sends 400 here.
        assert_eq!(status, StatusCode::NOT_FOUND);
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
}
