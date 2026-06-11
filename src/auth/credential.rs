use actix_identity::{Identity, IdentityExt};
use actix_web::{FromRequest, HttpMessage};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Namespace authorized for the request.
///
/// Included in request-local extension data once authorized.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::Type)]
pub struct AuthorizedNamespace(pub String);

impl FromRequest for AuthorizedNamespace {
    type Error = Error;

    type Future = std::future::Ready<Result<AuthorizedNamespace, Self::Error>>;

    fn from_request(req: &actix_web::HttpRequest, _: &mut actix_web::dev::Payload) -> Self::Future {
        std::future::ready(
            req.extensions()
                .get::<AuthorizedNamespace>()
                .cloned()
                .ok_or(Error::Unauthorized),
        )
    }
}

/// The principal authenticated by an `Authorization` header (NerveMQ API
/// key or AWS SigV4), recorded on the request by the `Authentication`
/// middleware *without* creating a session.
///
/// Header-authenticated clients prove themselves cryptographically on every
/// request and never replay cookies, so the previous `Identity::login` here
/// persisted a brand-new session row per SQS request — two to three writes
/// of pure overhead per call, serialized through SQLite's single writer,
/// and an unbounded pile of orphaned sessions (27k+ on a dev database).
#[derive(Debug, Clone)]
pub struct HeaderAuthedUser(pub String);

/// Extracts the caller's [`Identity`] from whichever authentication source
/// the request used: the header-authenticated principal recorded by the
/// `Authentication` middleware (API key / SigV4 — sessionless), or the
/// session cookie for browser/admin callers.
///
/// Handlers reachable by both kinds of caller take this instead of
/// [`Identity`].
pub struct Caller(pub Identity);

impl FromRequest for Caller {
    type Error = actix_web::Error;

    type Future = std::future::Ready<Result<Caller, Self::Error>>;

    fn from_request(req: &actix_web::HttpRequest, _: &mut actix_web::dev::Payload) -> Self::Future {
        let identity = match req.extensions().get::<HeaderAuthedUser>() {
            // A detached identity over an unchanged in-memory session:
            // `.id()` resolves to the email, nothing is ever persisted.
            Some(user) => Ok(Identity::mock(user.0.clone())),
            None => req
                .get_identity()
                .map_err(actix_web::error::ErrorUnauthorized),
        };

        std::future::ready(identity.map(Caller))
    }
}

/// Prefix for API keys for identification.
pub const API_KEY_PREFIX: &str = "nervemq";

/// Request to delete an API key.
#[derive(Debug)]
pub struct ApiKey {
    /// For AWS Sigv4, this is the access key ID
    pub short_token: String,
    /// For AWS Sigv4, this is the secret access key
    pub long_token: SecretString,
}

impl ApiKey {
    /// Creates a new API key with the specified short and long tokens.
    pub fn new(short_token: String, long_token: SecretString) -> Self {
        Self {
            short_token,
            long_token,
        }
    }
}
