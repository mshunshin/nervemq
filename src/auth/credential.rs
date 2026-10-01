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

/// The most an API key may do (`api_keys.access`), recorded on the request
/// alongside [`AuthorizedNamespace`] by the `Authentication` middleware.
///
/// It caps the key's owner's own level and never raises it: an `Admin` key
/// owned by a plain member still only sends and receives. Ordered from least
/// to most access.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, sqlx::Type,
)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum KeyAccess {
    /// Send, receive and inspect messages in the key's namespace.
    Member,
    /// Also manage that namespace's queues.
    Owner,
    /// Everything the owner can do, including the admin API if they are an
    /// admin.
    Admin,
}

impl KeyAccess {
    pub fn as_str(&self) -> &'static str {
        match self {
            KeyAccess::Member => "member",
            KeyAccess::Owner => "owner",
            KeyAccess::Admin => "admin",
        }
    }
}

impl std::str::FromStr for KeyAccess {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "member" => Ok(KeyAccess::Member),
            "owner" => Ok(KeyAccess::Owner),
            "admin" => Ok(KeyAccess::Admin),
            other => Err(format!(
                "invalid access '{other}': must be 'member', 'owner' or 'admin'"
            )),
        }
    }
}

impl FromRequest for KeyAccess {
    type Error = Error;

    type Future = std::future::Ready<Result<KeyAccess, Self::Error>>;

    fn from_request(req: &actix_web::HttpRequest, _: &mut actix_web::dev::Payload) -> Self::Future {
        std::future::ready(
            req.extensions()
                .get::<KeyAccess>()
                .copied()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_access_is_ordered_from_least_to_most() {
        assert!(KeyAccess::Member < KeyAccess::Owner);
        assert!(KeyAccess::Owner < KeyAccess::Admin);
    }

    #[test]
    fn key_access_parses_case_insensitively_and_round_trips() {
        for access in [KeyAccess::Member, KeyAccess::Owner, KeyAccess::Admin] {
            assert_eq!(access.as_str().parse::<KeyAccess>(), Ok(access));
            assert_eq!(access.as_str().to_uppercase().parse::<KeyAccess>(), Ok(access));
            let json = serde_json::to_value(access).unwrap();
            assert_eq!(json, serde_json::Value::String(access.as_str().into()));
            assert_eq!(serde_json::from_value::<KeyAccess>(json).unwrap(), access);
        }
        assert!("root".parse::<KeyAccess>().is_err());
        assert!(serde_json::from_str::<KeyAccess>("\"root\"").is_err());
    }

    #[actix_web::test]
    async fn extractors_refuse_requests_the_middleware_did_not_authorize() {
        let req = actix_web::test::TestRequest::default().to_http_request();
        let mut payload = actix_web::dev::Payload::None;
        assert!(matches!(
            KeyAccess::from_request(&req, &mut payload).await,
            Err(Error::Unauthorized)
        ));
        assert!(matches!(
            AuthorizedNamespace::from_request(&req, &mut payload).await,
            Err(Error::Unauthorized)
        ));
    }
}
