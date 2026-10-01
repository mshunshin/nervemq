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
        Error::NotFound { .. } => AwsErrorCode::same("ResourceNotFoundException"),
        Error::InvalidParameter { .. } | Error::InvalidHeader { .. } | Error::PayloadTooLarge => {
            AwsErrorCode::same("InvalidParameterValue")
        }
        Error::MissingParameter { .. } | Error::MissingHeader { .. } => {
            AwsErrorCode::same("MissingParameter")
        }
        Error::InvalidMethod { .. } => AwsErrorCode::same("InvalidAction"),
        Error::Unauthorized | Error::UserNotFound { .. } | Error::IdentityNotFound { .. } => {
            AwsErrorCode::same("AccessDeniedException")
        }
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
        HttpResponse::build(self.status_code())
            .content_type("application/x-amz-json-1.0")
            .insert_header(("x-amzn-query-error", format!("{code};{fault}")))
            .body(
                serde_json::json!({
                    "__type": format!("com.amazonaws.sqs#{shape}"),
                    "message": self.0.to_string(),
                })
                .to_string(),
            )
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
