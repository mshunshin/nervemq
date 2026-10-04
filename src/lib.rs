use std::future::Future;

use actix_cors::Cors;
use actix_identity::IdentityMiddleware;
use actix_session::{
    config::{CookieContentSecurity, PersistentSession},
    SessionMiddleware,
};
use actix_web::{
    body::MessageBody,
    cookie::Key,
    dev::{ServiceFactory, ServiceRequest, ServiceResponse},
    middleware::{NormalizePath, TrailingSlash},
    web::{Data, FormConfig, JsonConfig},
    App, HttpServer,
};
use auth::{
    middleware::{authentication::Authentication, protected_route::Protected},
    session::SqliteSessionStore,
};
use chrono::TimeDelta;
use config::ConfigBuilder;
use error::Error;
use kms::KeyManager;
use sqlx::SqlitePool;
use sqs::service::SqsApi;
use tracing_actix_web::TracingLogger;

mod api;
mod auth;
pub mod cli;
pub mod config;
pub mod error;
pub mod kms;
mod message;
mod namespace;
mod queue;
pub mod service;
mod sqs;
mod telemetry;
mod utils;

pub use sqs::method::*;
pub use sqs::types;

/// Serving of the embedded UI build (`out/`, made by `bun run build`). Only
/// compiled when the `embed-ui` feature is enabled; otherwise the server is
/// API-only.
#[cfg(feature = "embed-ui")]
mod ui {
    use actix_web::{http::header, HttpRequest, HttpResponse};
    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "out/"]
    struct Frontend;

    fn respond(file: rust_embed::EmbeddedFile) -> HttpResponse {
        HttpResponse::Ok()
            .insert_header((header::CONTENT_TYPE, file.metadata.mimetype()))
            .body(file.data.into_owned())
    }

    /// App-level default service: answers every request the API routes did
    /// not match. A file in the build is served as it is. Any other path is a
    /// page of the single-page app, so it gets `index.html` and the client's
    /// router renders it (or its own not-found page) — including deep links
    /// such as `/queues/<ns>/<name>`. Unknown API paths and missing assets
    /// stay 404s, so a client never parses the app's HTML as JSON or a
    /// script. (Extensions are no guide: a queue may be named `jobs.fifo`.)
    pub async fn serve(req: HttpRequest) -> HttpResponse {
        // The request path arrives percent-encoded, while rust-embed keys are
        // literal file paths, and a browser may encode any character.
        let path = urlencoding::decode(req.path()).unwrap_or_else(|_| req.path().into());
        let path = path.trim_start_matches('/');

        if let Some(file) = Frontend::get(path) {
            return respond(file);
        }

        let not_a_page = path == "api" || path.starts_with("api/") || path.starts_with("assets/");
        if !not_a_page {
            if let Some(file) = Frontend::get("index.html") {
                return respond(file);
            }
        }

        HttpResponse::NotFound().finish()
    }

    #[cfg(test)]
    mod tests {
        use actix_web::{http::StatusCode, test, web, App};

        /// A browser may send any path percent-encoded, so the handler must
        /// decode the request path before the embed lookup. Exercised by
        /// encoding an ordinary character of a real embedded asset's path.
        #[actix_web::test]
        async fn serves_percent_encoded_asset_paths() {
            let chunk = super::Frontend::iter()
                .find(|path| path.ends_with(".js"))
                .expect("a JS bundle in the UI build");
            let ch = chunk
                .chars()
                .find(|c| c.is_ascii_alphanumeric())
                .expect("an encodable character in the asset path");
            let encoded = chunk.replacen(ch, &format!("%{:02X}", ch as u32), 1);
            assert_ne!(*chunk, encoded);

            let app =
                test::init_service(App::new().default_service(web::to(super::serve))).await;

            let req = test::TestRequest::get()
                .uri(&format!("/{encoded}"))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        /// Pages, deep links included, get the app; unknown API paths and
        /// missing assets do not.
        #[actix_web::test]
        async fn pages_get_the_app_and_other_misses_are_404s() {
            let app =
                test::init_service(App::new().default_service(web::to(super::serve))).await;

            for page in ["/", "/login", "/queues", "/queues/ns/jobs.fifo", "/no/such/page"] {
                let req = test::TestRequest::get().uri(page).to_request();
                let resp = test::call_service(&app, req).await;
                assert_eq!(resp.status(), StatusCode::OK, "{page}");
                let body = test::read_body(resp).await;
                assert!(
                    String::from_utf8_lossy(&body).contains(r#"<div id="root">"#),
                    "{page} did not get the app"
                );
            }

            for miss in ["/api", "/api/admin/no-such-route", "/assets/missing.js"] {
                let req = test::TestRequest::get().uri(miss).to_request();
                let resp = test::call_service(&app, req).await;
                assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{miss}");
            }
        }
    }
}

/// Cross-origin access to the API: any origin, but never with the browser's
/// cookies. A session is only usable from the UI's own origin (this server,
/// or the dev server's proxy); requests carrying an API key or a SigV4
/// signature hold their credentials in headers, so they work from anywhere.
///
/// This used to allow credentials for every origin, echoing the caller's
/// `Origin` back: any page that counts as the same site (another port on the
/// same host, a sibling subdomain), whose requests the `SameSite=Lax` session
/// cookie does not stop, could read the admin API with a logged-in user's
/// session. `send_wildcard` answers `*` instead, and `actix-cors` refuses to
/// start if it is ever combined with `supports_credentials`.
fn cors() -> Cors {
    Cors::default()
        .allow_any_origin()
        .send_wildcard()
        .allow_any_header()
        .allow_any_method()
}

/// The Content-Security-Policy on every response. Scripts only from this
/// server's own files: no inline `<script>`, no `onerror=` attributes, no
/// `eval`, so markup an attacker manages to inject into the UI cannot run.
/// Everything else also comes only from here; no plugins, no `<base>` tag,
/// forms submit only here, and no other page may frame the UI
/// (clickjacking). Inline styles stay allowed: the dialogs' scroll lock and
/// the toasts insert `<style>` elements, and injected CSS cannot run code.
const CONTENT_SECURITY_POLICY: &str = concat!(
    "default-src 'self'; ",
    "script-src 'self'; ",
    "style-src 'self' 'unsafe-inline'; ",
    "img-src 'self' data:; ",
    "object-src 'none'; ",
    "base-uri 'none'; ",
    "form-action 'self'; ",
    "frame-ancestors 'none'",
);

/// Adds, to every response, the Content-Security-Policy and headers that
/// stop the browser guessing a content type (`nosniff`), framing the UI
/// (for browsers predating `frame-ancestors`) and sending URLs as referrers.
/// Errors get them too, on the response they will be sent as: their
/// messages echo request input, such as a queue's name.
async fn security_headers<B: actix_web::body::MessageBody + 'static>(
    req: actix_web::dev::ServiceRequest,
    next: actix_web::middleware::Next<B>,
) -> Result<actix_web::dev::ServiceResponse<B>, actix_web::Error> {
    use actix_web::http::header::{self, HeaderMap, HeaderValue};

    fn add(headers: &mut HeaderMap) {
        for (name, value) in [
            (header::CONTENT_SECURITY_POLICY, CONTENT_SECURITY_POLICY),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::X_FRAME_OPTIONS, "DENY"),
            (header::REFERRER_POLICY, "no-referrer"),
        ] {
            headers.insert(name, HeaderValue::from_static(value));
        }
    }

    match next.call(req).await {
        Ok(mut res) => {
            add(res.headers_mut());
            Ok(res)
        }
        Err(err) => {
            let mut response = err.error_response();
            add(response.headers_mut());
            Err(actix_web::error::InternalError::from_response(err, response).into())
        }
    }
}

#[cfg(test)]
mod security_header_tests {
    use actix_web::{
        error::ErrorNotFound,
        http::{header, StatusCode},
        middleware::from_fn,
        test, web, App, HttpResponse,
    };

    /// Pages, API answers and errors all carry the headers.
    #[actix_web::test]
    async fn every_response_carries_the_security_headers() {
        let app = test::init_service(
            App::new()
                .wrap(from_fn(super::security_headers))
                .route("/ok", web::get().to(HttpResponse::Ok))
                .route(
                    "/missing",
                    web::get().to(|| async { Err::<HttpResponse, _>(ErrorNotFound("queue <b>x</b>")) }),
                ),
        )
        .await;

        for (uri, status) in [("/ok", StatusCode::OK), ("/missing", StatusCode::NOT_FOUND)] {
            let resp = test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;
            assert_eq!(resp.status(), status, "{uri}");
            let headers = resp.headers();
            let csp = headers.get(header::CONTENT_SECURITY_POLICY).unwrap().to_str().unwrap();
            assert!(csp.contains("script-src 'self';"), "{uri}: {csp}");
            assert!(csp.contains("frame-ancestors 'none'"), "{uri}: {csp}");
            assert_eq!(headers.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff", "{uri}");
            assert_eq!(headers.get(header::X_FRAME_OPTIONS).unwrap(), "DENY", "{uri}");
            assert_eq!(headers.get(header::REFERRER_POLICY).unwrap(), "no-referrer", "{uri}");
        }
    }
}

#[cfg(test)]
mod cors_tests {
    use actix_web::{
        http::{header, Method, StatusCode},
        test, web, App, HttpResponse,
    };

    const ORIGIN: &str = "http://elsewhere.localhost:9999";

    /// Another origin may call the API, but the browser is never told it may
    /// do so with cookies, so it neither sends a session in a preflighted
    /// request nor lets the page read a response to one that rode a cookie.
    #[actix_web::test]
    async fn other_origins_are_allowed_without_credentials() {
        let app = test::init_service(
            App::new()
                .wrap(super::cors())
                .route("/api/admin/users", web::get().to(HttpResponse::Ok)),
        )
        .await;

        let preflight = test::TestRequest::default()
            .method(Method::OPTIONS)
            .uri("/api/admin/users")
            .insert_header((header::ORIGIN, ORIGIN))
            .insert_header((header::ACCESS_CONTROL_REQUEST_METHOD, "GET"))
            .to_request();
        let get = test::TestRequest::get()
            .uri("/api/admin/users")
            .insert_header((header::ORIGIN, ORIGIN))
            .to_request();

        for req in [preflight, get] {
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);
            let headers = resp.headers();
            assert_eq!(headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(), "*");
            assert!(
                headers.get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS).is_none(),
                "credentials must never be allowed cross-origin"
            );
        }
    }
}

/// Returns a builder for the main application.
#[bon::builder(finish_fn = start)]
pub async fn run<K, F, R>(
    kms_factory: K,
    /// Directory to store the SQLite database files in. When set, overrides
    /// `NERVEMQ_DB_PATH` and places `nervemq.db` (and, by derivation,
    /// `sessions.db`) inside it. The directory must already exist.
    data_dir: Option<std::path::PathBuf>,
) -> eyre::Result<()>
where
    K: FnOnce(SqlitePool) -> F,
    F: Future<Output = Result<R, Error>>,
    R: KeyManager,
{
    // Logs to stdout, and OpenTelemetry export when OTEL_* turns it on.
    // Dropping the guard on an early return stops the exporters too.
    let (mut telemetry_guard, telemetry) = telemetry::init()?;

    let mut builder = ConfigBuilder::new()
        .with_layer(config::DefaultsLayer)
        .with_layer(config::EnvironmentLayer);
    if let Some(dir) = data_dir {
        builder = builder.with_layer(config::DataDirLayer::new(dir));
    }
    let config = builder.load().await?;

    let service = service::Service::connect_with()
        .config(config)
        .kms_factory(kms_factory)
        .telemetry(telemetry.clone())
        .call()
        .await?;

    // Sessions live in their own database file so the per-request session
    // TTL writes never compete with message traffic for the main database's
    // write lock (see docs/architecture/sessions.md).
    let sessions_db = auth::session::connect(&service.config().sessions_db_path()).await?;
    let session_store = SqliteSessionStore::new(sessions_db.clone());

    let main_db = std::path::PathBuf::from(service.config().db_path());
    let sessions_file = std::path::PathBuf::from(service.config().sessions_db_path());
    let wal = |path: &std::path::Path| {
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        std::path::PathBuf::from(wal)
    };
    telemetry.observe_databases(
        vec![("main", service.db().clone()), ("sessions", sessions_db.clone())],
        vec![
            ("main-wal", wal(&main_db)),
            ("main", main_db),
            ("sessions-wal", wal(&sessions_file)),
            ("sessions", sessions_file),
        ],
    );

    // Periodically collect expired session rows (the first sweep runs
    // immediately, clearing anything left over from previous runs).
    auth::session::spawn_session_gc(sessions_db);

    // Periodically reclaim pages freed by deletes (incremental auto-vacuum).
    service.spawn_db_maintenance();

    // The per-queue gauges read a snapshot this keeps fresh.
    if telemetry.is_recording() {
        service.spawn_queue_gauges(telemetry::queue_gauge_interval());
    }

    // Session cookie signing key: generated on first run and persisted in the
    // database so restarts don't invalidate existing session cookies.
    let secret_key = auth::session::load_or_generate_session_key(service.db()).await?;

    // Resolve the listen address before the service is moved into app data.
    // Defaults to loopback; set NERVEMQ_BIND_ADDRESS=0.0.0.0:8080 to listen on
    // all interfaces (as the Docker image does).
    let bind_address = service.config().bind_address().to_owned();
    tracing::info!("binding HTTP server to {bind_address}");

    let stopping = service.stopping().clone();
    let data = Data::new(service);

    let server = HttpServer::new(move || {
        build_app(data.clone(), session_store.clone(), secret_key.clone())
    })
    // .bind_openssl(&bind_address, ssl_acceptor)?
    .bind(bind_address.as_str())?
    // Stopping is handled here (`stop_on_signal`), to end long polls first.
    .disable_signals()
    .shutdown_timeout(SHUTDOWN_TIMEOUT_SECS)
    .run();
    tokio::spawn(stop_on_signal(server.handle(), stopping));
    let served = server.await;

    // Export what's queued before exiting, whether or not the server
    // stopped cleanly. It waits on the network, so off the async threads.
    tokio::task::spawn_blocking(move || telemetry_guard.shutdown()).await?;

    served?;
    Ok(())
}

/// How long requests in flight get to finish once the server is stopping.
/// With long polls answered at once, requests take milliseconds; the cap
/// leaves the final telemetry export most of the 10 s `docker stop` allows
/// before it kills the process.
const SHUTDOWN_TIMEOUT_SECS: u64 = 5;

/// Stops the server on SIGTERM (`docker stop`, Kubernetes) or SIGINT
/// (Ctrl-C):
/// 1. long polls answer at once, with whatever the queue holds;
/// 2. the server stops accepting connections;
/// 3. requests in flight get up to [`SHUTDOWN_TIMEOUT_SECS`] to finish.
///
/// SIGQUIT stops at once. actix's own handling waited up to 30 s, and
/// a long poll (up to 20 s) used most of it.
async fn stop_on_signal(
    server: actix_web::dev::ServerHandle,
    stopping: tokio_util::sync::CancellationToken,
) {
    #[cfg(unix)]
    let graceful = {
        use tokio::signal::unix::{signal, SignalKind};
        let (Ok(mut term), Ok(mut int), Ok(mut quit)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
            signal(SignalKind::quit()),
        ) else {
            tracing::error!("can't listen for signals: the server won't stop gracefully");
            return;
        };
        tokio::select! {
            _ = term.recv() => true,
            _ = int.recv() => true,
            _ = quit.recv() => false,
        }
    };
    #[cfg(not(unix))]
    let graceful = {
        let _ = tokio::signal::ctrl_c().await;
        true
    };

    tracing::info!(graceful, "stopping");
    stopping.cancel();
    server.stop(graceful).await;
}

/// How long a session lasts without a visit.
const SESSION_EXPIRATION: TimeDelta = chrono::Duration::hours(1);

/// The app each server worker runs: the middleware, the API and the embedded
/// UI. Tests build it too, so they run the production middleware in its
/// production order.
pub(crate) fn build_app(
    data: Data<service::Service>,
    session_store: SqliteSessionStore,
    secret_key: Key,
) -> App<
    impl ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    let deadline = SESSION_EXPIRATION.to_std().expect("valid duration");
    let session_ttl = actix_web::cookie::time::Duration::new(SESSION_EXPIRATION.num_seconds(), 0);

    let session_middleware = SessionMiddleware::builder(session_store, secret_key)
        .cookie_secure(true)
        .cookie_content_security(CookieContentSecurity::Signed)
        .session_lifecycle(PersistentSession::default().session_ttl(session_ttl))
        .cookie_http_only(true)
        .cookie_name("nervemq_session".to_owned())
        .build();

    let identity_middleware = IdentityMiddleware::builder()
        .visit_deadline(Some(deadline))
        .logout_behaviour(actix_identity::config::LogoutBehaviour::PurgeSession)
        .id_key("nervemq_id")
        .build();

    let json_cfg = JsonConfig::default().content_type_required(false);
    let form_cfg = FormConfig::default();

    #[allow(unused_mut)]
    let mut app = App::new()
        .wrap(
            // IMPORTANT: This must be first in the middleware stack (executed last) because
            // it mutated the request path, which breaks AWS SigV4 authentication because the
            // request path is used in the hash/signature. We do need this however, since the
            // Actix router doesn't seem to work without it.
            NormalizePath::new(TrailingSlash::Trim),
        )
        .wrap(Authentication)
        .wrap(identity_middleware)
        .wrap(session_middleware)
        // Inside CORS, so a refusal still carries its headers.
        .wrap(actix_web::middleware::from_fn(
            auth::middleware::same_origin::refuse_cross_origin_cookie_writes,
        ))
        .wrap(actix_web::middleware::from_fn(
            auth::middleware::host::refuse_unknown_hosts,
        ))
        .wrap(cors())
        // Outside everything else that can answer, so every response gets
        // the headers.
        .wrap(actix_web::middleware::from_fn(security_headers))
        // Inside the span, which mints the request id it sends.
        .wrap(actix_web::middleware::from_fn(sqs::service::request_id_header))
        // The whole request runs in its span, so the signature check's span
        // is its child and refused requests are traced too.
        .wrap(TracingLogger::<telemetry::RootSpan>::new())
        // Outermost: request durations cover everything above.
        .wrap(actix_web::middleware::from_fn(telemetry::http_metrics))
        .app_data(data)
        .app_data(json_cfg)
        .app_data(form_cfg);

    // All API routes live under `/api`: the SQS-compatible endpoint at
    // `/api/sqs` and the management API at `/api/admin/*`. Keeping the API
    // namespaced under `/api` means UI routes (e.g. `/admin`, `/queues`)
    // never collide with API scopes.
    app = app.service(
        actix_web::web::scope("/api")
            .service(api::health::service())
            .service(sqs::service().wrap(Protected::authenticated()).wrap(SqsApi))
            .service(
                actix_web::web::scope("/admin")
                    // JSON bodies must say so (`application/json`). The
                    // app-wide config accepts any content type, for SQS
                    // clients' `application/x-amz-json-1.0`; here that
                    // would let a page on another origin post JSON as
                    // `text/plain`, which needs no CORS preflight.
                    .app_data(JsonConfig::default())
                    .service(api::queue::service().wrap(Protected::authenticated()))
                    .service(api::data::service().wrap(Protected::authenticated()))
                    .service(api::tokens::service().wrap(Protected::authenticated()))
                    // Any logged-in user: members list namespaces and
                    // owners delete them; each route checks its rule.
                    .service(api::namespace::service().wrap(Protected::authenticated()))
                    .service(api::admin::service().wrap(Protected::admin_only()))
                    .service(api::auth::service()),
            ),
    );

    // Serve the embedded UI for any other route not matched by the API above.
    #[cfg(feature = "embed-ui")]
    {
        app = app.default_service(actix_web::web::to(ui::serve));
    }

    app
}
