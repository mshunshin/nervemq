//! Answers browser-facing requests only for the configured host name, when
//! there is one (DNS rebinding).
//!
//! A browser decides what an origin is from the name in the URL, never the
//! IP address it resolves to. An attacker who controls a domain can serve a
//! page from it, then point the name at this server (say 127.0.0.1): to the
//! browser the page's requests stay same-origin, so neither CORS nor the
//! `Origin` check (`super::same_origin`) applies, and they reach the server
//! with `Host` and `Origin` both naming the attacker's domain. The attacker
//! gets no session that way — the victim's cookie belongs to the real name,
//! and a `Secure` cookie is not kept over plain HTTP — but can use the
//! victim's browser to reach anything that needs none, such as trying
//! passwords at the login endpoint of a server only the victim can reach.
//!
//! The `Host` header is the one thing the attacker cannot make ours, so when
//! `NERVEMQ_HOST` is set, a request for any other name is refused, apart
//! from loopback names (a rebinding page's `Host` is always its own domain).
//! When it is unset, every name is answered: being reachable under whatever
//! name points at the server is often more useful, and the gap is documented
//! in docs/architecture/web-security.md.
//!
//! Two paths are exempt. The SQS API: its requests carry their credentials
//! in signed headers, so a rebinding page gains nothing from it, and SQS
//! clients may well use another name for the server (an internal service
//! name, say). And the health check (`crate::api::health`), which load
//! balancers and orchestrators address by IP, and which reveals nothing more
//! than that the server is up.

use actix_web::body::{EitherBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::{header, StatusCode};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, HttpResponse};
use url::Url;

use crate::api::health::is_health_path;
use crate::service::Service;
use crate::sqs::error::is_sqs_path;

/// `actix_web::middleware::from_fn` middleware; see the module docs.
pub async fn refuse_unknown_hosts<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<EitherBody<B>>, Error> {
    let configured = req
        .app_data::<Data<Service>>()
        .and_then(|service| service.config().configured_host().cloned());

    if let Some(configured) = configured {
        let host = req.headers().get(header::HOST).and_then(|v| v.to_str().ok());
        if !is_exempt(req.path()) && !is_allowed_host(host, &configured) {
            let host = host.unwrap_or_default().to_owned();
            tracing::warn!(%host, path = req.path(), "refused a request for an unknown host");
            let response = HttpResponse::build(StatusCode::MISDIRECTED_REQUEST).body(format!(
                "this server answers to {}, not {host}",
                configured.origin().ascii_serialization()
            ));
            return Ok(req.into_response(response).map_into_right_body());
        }
    }

    Ok(next.call(req).await?.map_into_left_body())
}

/// Paths answered under any name; see the module docs.
fn is_exempt(path: &str) -> bool {
    is_sqs_path(path) || is_health_path(path)
}

/// Whether a `Host` header names the configured server, or a loopback name.
fn is_allowed_host(host: Option<&str>, configured: &Url) -> bool {
    let Some(host) = host else {
        return false;
    };
    let (name, port) = split_host_port(host);
    if ["localhost", "127.0.0.1", "[::1]"]
        .iter()
        .any(|loopback| name.eq_ignore_ascii_case(loopback))
    {
        return true;
    }
    configured
        .host_str()
        .is_some_and(|configured_name| name.eq_ignore_ascii_case(configured_name))
        && port.or(scheme_default_port(configured)) == configured.port_or_known_default()
}

/// Splits a `Host` header into its name and port, if it gives one. IPv6
/// names keep their brackets (`[::1]`), as `Url::host_str` does.
pub(super) fn split_host_port(host: &str) -> (&str, Option<u16>) {
    match host.rsplit_once(':') {
        Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
            (name, port.parse().ok())
        }
        _ => (host, None),
    }
}

/// The port a `Host` header without one means, for a URL's scheme. `Host`
/// never names the scheme (TLS may end at a proxy), so it is taken from the
/// URL being compared with.
pub(super) fn scheme_default_port(url: &Url) -> Option<u16> {
    match url.scheme() {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::is_allowed_host;
    use url::Url;

    #[test]
    fn the_configured_name_and_loopback_are_allowed() {
        let configured = Url::parse("https://mq.example.com").unwrap();
        for host in [
            "mq.example.com",
            "MQ.example.com",
            "mq.example.com:443",
            "localhost",
            "localhost:3000",
            "127.0.0.1:8080",
            "[::1]:8080",
        ] {
            assert!(is_allowed_host(Some(host), &configured), "{host}");
        }
    }

    #[test]
    fn other_names_and_ports_are_not() {
        let configured = Url::parse("https://mq.example.com").unwrap();
        for host in [
            "evil.test",
            "evil.test:8080",
            "mq.example.com:8443",
            "mq.example.com.evil.test",
            "10.0.0.5:8080",
        ] {
            assert!(!is_allowed_host(Some(host), &configured), "{host}");
        }
        assert!(!is_allowed_host(None, &configured));

        let local = Url::parse("http://mq.lan:8080").unwrap();
        assert!(is_allowed_host(Some("mq.lan:8080"), &local));
        assert!(!is_allowed_host(Some("mq.lan"), &local), "port 80 is not 8080");
    }
}
