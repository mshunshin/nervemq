//! Refuses writes that ride the session cookie from another origin
//! (cross-site request forgery).
//!
//! CORS stops another origin from *reading* a response, but a browser still
//! *sends* a "simple" request — a POST of a form, of plain text or of no
//! body at all, which needs no preflight — and with it the session cookie
//! whenever the page counts as the same site (`SameSite=Lax` only stops
//! other sites). The admin API's JSON bodies must be labelled
//! `application/json`, which a simple request cannot be, but many writes
//! take no body (pausing a queue, disabling a user, logging out). Browsers
//! send `Origin` with every such request, so a write whose `Origin` is not
//! the server's own is refused here.
//!
//! Only cookie-authenticated requests are at risk. A request with an
//! `Authorization` header (an API key or a SigV4 signature) is authenticated
//! by that header alone, which another origin cannot forge, and clients that
//! send no `Origin` (curl, SDKs, scripts) are not a browser being tricked.

use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::{header, Method};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, HttpResponse};
use url::Url;

use super::host::{scheme_default_port, split_host_port};
use crate::service::Service;

/// `actix_web::middleware::from_fn` middleware; see the module docs.
pub async fn refuse_cross_origin_cookie_writes<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<EitherBody<B>>, Error> {
    if is_cross_origin_cookie_write(&req) {
        let origin = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        tracing::warn!(%origin, path = req.path(), "refused a cross-origin write");
        let response = HttpResponse::Forbidden().body(format!(
            "refused: a request from {origin} cannot act with this server's session cookie"
        ));
        return Ok(req.into_response(response).map_into_right_body());
    }

    Ok(next.call(req).await?.map_into_left_body())
}

fn is_cross_origin_cookie_write(req: &ServiceRequest) -> bool {
    // A forged read gains nothing: CORS keeps the response from the page.
    if matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS) {
        return false;
    }
    let headers = req.headers();
    if headers.contains_key(header::AUTHORIZATION) {
        return false;
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return false;
    };

    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let configured = req
        .app_data::<Data<Service>>()
        .map(|service| service.config().host());

    !origin
        .to_str()
        .is_ok_and(|origin| is_own_origin(origin, host, configured.as_ref()))
}

/// Whether `origin` (an `Origin` header) is this server: it names the host
/// the request was sent to (the `Host` header), or the configured
/// `NERVEMQ_HOST`, which covers a reverse proxy that rewrites `Host`. An
/// opaque origin (`null`, from a sandboxed frame or a file) is never ours.
fn is_own_origin(origin: &str, host: Option<&str>, configured: Option<&Url>) -> bool {
    let Ok(origin) = Url::parse(origin) else {
        return false;
    };
    if configured.is_some_and(|configured| configured.origin() == origin.origin()) {
        return true;
    }
    let (Some(host), Some(origin_host)) = (host, origin.host_str()) else {
        return false;
    };

    let (name, port) = split_host_port(host);
    name.eq_ignore_ascii_case(origin_host)
        && port.or(scheme_default_port(&origin)) == origin.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::is_own_origin;
    use url::Url;

    #[test]
    fn the_request_host_is_our_origin() {
        for (origin, host) in [
            ("http://localhost:8080", "localhost:8080"),
            ("http://LOCALHOST:8080", "localhost:8080"),
            ("https://mq.example.com", "mq.example.com"),
            ("https://mq.example.com", "mq.example.com:443"),
            ("http://[::1]:8080", "[::1]:8080"),
        ] {
            assert!(is_own_origin(origin, Some(host), None), "{origin} vs {host}");
        }
    }

    #[test]
    fn other_ports_hosts_and_opaque_origins_are_not() {
        for (origin, host) in [
            ("http://localhost:9999", "localhost:8080"),
            ("http://evil.example.com", "mq.example.com"),
            ("http://mq.example.com.evil.com", "mq.example.com"),
            ("https://mq.example.com:8443", "mq.example.com"),
            ("http://[::1]:9999", "[::1]:8080"),
            ("null", "localhost:8080"),
            ("not a url", "localhost:8080"),
        ] {
            assert!(!is_own_origin(origin, Some(host), None), "{origin} vs {host}");
        }
        assert!(!is_own_origin("http://localhost:8080", None, None));
    }

    /// A reverse proxy may rewrite `Host`; the configured `NERVEMQ_HOST`
    /// still identifies the server.
    #[test]
    fn the_configured_host_is_our_origin() {
        let configured = Url::parse("https://mq.example.com").unwrap();
        assert!(is_own_origin("https://mq.example.com", Some("10.0.0.5:8080"), Some(&configured)));
        assert!(!is_own_origin("http://mq.example.com", Some("10.0.0.5:8080"), Some(&configured)));
    }
}
