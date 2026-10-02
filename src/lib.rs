use std::future::Future;

use actix_cors::Cors;
use actix_identity::IdentityMiddleware;
use actix_session::{
    config::{CookieContentSecurity, PersistentSession},
    SessionMiddleware,
};
use actix_web::{
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
use tracing::level_filters::LevelFilter;
use tracing_actix_web::TracingLogger;
use tracing_subscriber::{util::SubscriberInitExt, EnvFilter, FmtSubscriber};

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
    #[cfg(debug_assertions)]
    FmtSubscriber::builder()
        .pretty()
        .with_env_filter(
            EnvFilter::builder()
                .with_env_var("NERVEMQ_LOG")
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()?,
        )
        .finish()
        .try_init()?;

    #[cfg(not(debug_assertions))]
    FmtSubscriber::builder()
        .json()
        .with_env_filter(
            EnvFilter::builder()
                .with_env_var("NERVEMQ_LOG")
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()?,
        )
        .finish()
        .try_init()?;

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
        .call()
        .await?;

    // Sessions live in their own database file so the per-request session
    // TTL writes never compete with message traffic for the main database's
    // write lock (see docs/architecture/sessions.md).
    let sessions_db = auth::session::connect(&service.config().sessions_db_path()).await?;
    let session_store = SqliteSessionStore::new(sessions_db.clone());

    // Periodically collect expired session rows (the first sweep runs
    // immediately, clearing anything left over from previous runs).
    auth::session::spawn_session_gc(sessions_db);

    // Periodically reclaim pages freed by deletes (incremental auto-vacuum).
    service.spawn_db_maintenance();

    // Session cookie signing key: generated on first run and persisted in the
    // database so restarts don't invalidate existing session cookies.
    let secret_key = auth::session::load_or_generate_session_key(service.db()).await?;

    // Resolve the listen address before the service is moved into app data.
    // Defaults to loopback; set NERVEMQ_BIND_ADDRESS=0.0.0.0:8080 to listen on
    // all interfaces (as the Docker image does).
    let bind_address = service.config().bind_address().to_owned();
    tracing::info!("binding HTTP server to {bind_address}");

    let data = Data::new(service);

    const SESSION_EXPIRATION: TimeDelta = chrono::Duration::hours(1);

    let deadline = SESSION_EXPIRATION.to_std().expect("valid duration");
    let session_ttl = actix_web::cookie::time::Duration::new(SESSION_EXPIRATION.num_seconds(), 0);

    HttpServer::new(move || {
        let session_middleware =
            SessionMiddleware::builder(session_store.clone(), secret_key.clone())
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
            .wrap(TracingLogger::default())
            .wrap(Authentication)
            .wrap(identity_middleware)
            .wrap(session_middleware)
            .wrap(cors())
            .app_data(data.clone())
            .app_data(json_cfg)
            .app_data(form_cfg);

        // All API routes live under `/api`: the SQS-compatible endpoint at
        // `/api/sqs` and the management API at `/api/admin/*`. Keeping the API
        // namespaced under `/api` means UI routes (e.g. `/admin`, `/queues`)
        // never collide with API scopes.
        app = app.service(
            actix_web::web::scope("/api")
                .service(sqs::service().wrap(Protected::authenticated()).wrap(SqsApi))
                .service(
                    actix_web::web::scope("/admin")
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
    })
    // .bind_openssl(&bind_address, ssl_acceptor)?
    .bind(bind_address.as_str())?
    .run()
    .await?;

    Ok(())
}
