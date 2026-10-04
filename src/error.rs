//! Error handling for the application.
//!
//! This module provides a centralized error type that encompasses all possible
//! error cases in the application, from API validation to database operations.
//! It uses the `snafu` crate for error handling patterns.

use snafu::Snafu;

/// The main error enum that represents all possible errors in the application.
/// Each variant includes context-specific information and appropriate error messages.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unauthorized"))]
    Unauthorized,

    /// Authenticated, but not allowed to do this — e.g. a namespace member
    /// (not an owner or admin) managing a queue.
    #[snafu(display("Forbidden: {message}"))]
    Forbidden { message: String },

    /// The request conflicts with the current state — e.g. removing the
    /// last active admin.
    #[snafu(display("Conflict: {message}"))]
    Conflict { message: String },

    #[snafu(display("Resource not found: {resource}"))]
    NotFound { resource: String },

    #[snafu(display("Resource not found: queue {queue} in namespace {namespace}"))]
    QueueNotFound { queue: String, namespace: String },

    #[snafu(display("Resource not found: {message}"))]
    InvalidReceiptHandle { message: String },

    #[snafu(display(
        "Queue {queue} already exists in namespace {namespace} with a different {attribute}"
    ))]
    QueueAlreadyExists {
        queue: String,
        namespace: String,
        attribute: String,
    },

    #[snafu(display("Internal server error"))]
    InternalServerError {
        #[snafu(source(false))]
        source: Option<eyre::Report>,
    },

    #[snafu(display("Error returned from database"))]
    Sqlx {
        #[snafu(source)]
        source: sqlx::Error,
    },

    #[snafu(display("Error running migrations"))]
    MigrationError {
        #[snafu(source)]
        source: sqlx::migrate::MigrateError,
    },

    #[snafu(display("Identity {key_id} not found"))]
    IdentityNotFound { key_id: String },

    #[snafu(display("User not found for email: {email}"))]
    UserNotFound { email: String },

    #[snafu(display("Payload too large"))]
    PayloadTooLarge,

    #[snafu(display("Missing header: {header}"))]
    MissingHeader { header: String },

    #[snafu(display("Invalid header: {header}"))]
    InvalidHeader { header: String },

    /// A SigV4 request whose `X-Amz-Date` is outside the clock-drift window.
    #[snafu(display("{message}"))]
    SignatureExpired { message: String },

    #[snafu(whatever, display("{message}"))]
    Whatever {
        message: String,
        #[snafu(source(from(eyre::Report, Some)))]
        source: Option<eyre::Report>,
    },

    #[snafu(display("Invalid parameter: {message}"))]
    InvalidParameter { message: String },

    #[snafu(display("Invalid attribute value: {message}"))]
    InvalidAttributeValue { message: String },

    #[snafu(display("Invalid request method: {message}"))]
    InvalidMethod { message: String },

    #[snafu(display("Missing parameter: {message}"))]
    MissingParameter { message: String },

    /// A batch request refused as a whole, before any entry is tried.
    #[snafu(display("{message}"))]
    InvalidBatch { fault: BatchFault, message: String },
}

/// Why a batch request (`SendMessageBatch`, `DeleteMessageBatch`,
/// `ChangeMessageVisibilityBatch`) was refused. Each is its own AWS error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFault {
    /// No entries.
    Empty,
    /// More entries than AWS allows in one request.
    TooManyEntries,
    /// Two entries with the same `Id`.
    IdsNotDistinct,
    /// An `Id` that is empty, too long, or has characters AWS doesn't allow.
    InvalidEntryId,
    /// `SendMessageBatch` messages larger together than one message may be.
    TooLong,
}

impl From<sqlx::Error> for Error {
    fn from(source: sqlx::Error) -> Self {
        Self::Sqlx { source }
    }
}

impl From<eyre::Report> for Error {
    fn from(e: eyre::Report) -> Self {
        Self::InternalServerError { source: Some(e) }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::InternalServerError {
            source: Some(e.into()),
        }
    }
}

impl From<actix_web::Error> for Error {
    fn from(source: actix_web::Error) -> Self {
        Self::InternalServerError {
            source: Some(eyre::eyre!("{source}")),
        }
    }
}

impl From<actix_identity::error::GetIdentityError> for Error {
    fn from(_: actix_identity::error::GetIdentityError) -> Self {
        Self::Unauthorized
    }
}

impl From<sqlx::migrate::MigrateError> for Error {
    fn from(source: sqlx::migrate::MigrateError) -> Self {
        Self::MigrationError { source }
    }
}

/// Convenience methods for creating common error types
impl Error {
    /// Creates a new internal server error with a source error
    pub fn internal(e: impl Into<eyre::Report>) -> Self {
        Self::InternalServerError {
            source: Some(e.into()),
        }
    }

    /// Creates an internal server error without exposing the underlying error
    pub fn opaque() -> Self {
        Self::InternalServerError { source: None }
    }

    /// Creates a not found error for a generic resource
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::NotFound {
            resource: resource.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden {
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::InvalidParameter {
            message: message.into(),
        }
    }

    /// Creates an error for a queue attribute whose value is out of range
    pub fn invalid_attribute_value(message: impl Into<String>) -> Self {
        Self::InvalidAttributeValue {
            message: message.into(),
        }
    }

    pub fn missing_parameter(message: impl Into<String>) -> Self {
        Self::MissingParameter {
            message: message.into(),
        }
    }

    pub fn invalid_batch(fault: BatchFault, message: impl Into<String>) -> Self {
        Self::InvalidBatch {
            fault,
            message: message.into(),
        }
    }

    /// Creates a not found error specifically for queues within a namespace
    pub fn queue_not_found(queue: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self::QueueNotFound {
            queue: queue.into(),
            namespace: namespace.into(),
        }
    }

    /// Creates an error for a receipt handle that is unknown, expired, or
    /// whose message is no longer in flight
    pub fn invalid_receipt_handle(message: impl Into<String>) -> Self {
        Self::InvalidReceiptHandle {
            message: message.into(),
        }
    }

    /// Creates a not found error specifically for namespaces
    pub fn namespace_not_found(namespace: impl Into<String>) -> Self {
        Self::NotFound {
            resource: format!("namespace {}", namespace.into()),
        }
    }
}

/// Maps internal errors to HTTP status codes for API responses.
/// This implementation ensures consistent error handling across the API.
impl actix_web::ResponseError for Error {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match self {
            Self::Unauthorized
            | Self::UserNotFound { .. }
            | Self::IdentityNotFound { .. }
            | Self::SignatureExpired { .. } => actix_web::http::StatusCode::UNAUTHORIZED,
            Self::Forbidden { .. } => actix_web::http::StatusCode::FORBIDDEN,
            Self::Conflict { .. } => actix_web::http::StatusCode::CONFLICT,
            Self::NotFound { .. }
            | Self::QueueNotFound { .. }
            | Self::InvalidReceiptHandle { .. } => actix_web::http::StatusCode::NOT_FOUND,

            Self::MissingHeader { .. }
            | Self::MissingParameter { .. }
            | Self::InvalidHeader { .. }
            | Self::InvalidMethod { .. }
            | Self::InvalidParameter { .. }
            | Self::InvalidAttributeValue { .. }
            | Self::InvalidBatch { .. }
            | Self::QueueAlreadyExists { .. } => actix_web::http::StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge => actix_web::http::StatusCode::PAYLOAD_TOO_LARGE,

            Self::MigrationError { .. }
            | Self::InternalServerError { .. }
            | Self::Sqlx { .. }
            | Self::Whatever { .. } => actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, ResponseError};

    /// The HTTP status of each error is part of the API: the UI and clients
    /// branch on 401 (log in again), 403 (not allowed), 404 and 409.
    #[test]
    fn errors_map_to_their_http_status() {
        for (err, status) in [
            (Error::Unauthorized, StatusCode::UNAUTHORIZED),
            (Error::UserNotFound { email: "x".into() }, StatusCode::UNAUTHORIZED),
            (Error::IdentityNotFound { key_id: "x".into() }, StatusCode::UNAUTHORIZED),
            (Error::SignatureExpired { message: "x".into() }, StatusCode::UNAUTHORIZED),
            (Error::forbidden("x"), StatusCode::FORBIDDEN),
            (Error::conflict("x"), StatusCode::CONFLICT),
            (Error::not_found("x"), StatusCode::NOT_FOUND),
            (Error::namespace_not_found("x"), StatusCode::NOT_FOUND),
            (Error::queue_not_found("q", "ns"), StatusCode::NOT_FOUND),
            (Error::invalid_receipt_handle("x"), StatusCode::NOT_FOUND),
            (Error::invalid_parameter("x"), StatusCode::BAD_REQUEST),
            (Error::invalid_attribute_value("x"), StatusCode::BAD_REQUEST),
            (Error::missing_parameter("x"), StatusCode::BAD_REQUEST),
            (Error::MissingHeader { header: "x".into() }, StatusCode::BAD_REQUEST),
            (Error::InvalidHeader { header: "x".into() }, StatusCode::BAD_REQUEST),
            (Error::InvalidMethod { message: "x".into() }, StatusCode::BAD_REQUEST),
            (Error::invalid_batch(BatchFault::Empty, "x"), StatusCode::BAD_REQUEST),
            (
                Error::QueueAlreadyExists {
                    queue: "q".into(),
                    namespace: "ns".into(),
                    attribute: "a".into(),
                },
                StatusCode::BAD_REQUEST,
            ),
            (Error::PayloadTooLarge, StatusCode::PAYLOAD_TOO_LARGE),
            (Error::opaque(), StatusCode::INTERNAL_SERVER_ERROR),
            (Error::internal(eyre::eyre!("x")), StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            assert_eq!(err.status_code(), status, "{err:?}");
        }
    }

    /// Internal errors keep their cause out of the response body.
    #[test]
    fn internal_errors_do_not_leak_their_cause() {
        let err = Error::internal(eyre::eyre!("secret database detail"));
        assert_eq!(err.to_string(), "Internal server error");
    }
}
