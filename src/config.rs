//! Configuration management for NerveMQ.
//!
//! Handles loading and accessing configuration values from environment
//! variables with fallback to default values.

use std::path::PathBuf;
use std::pin::Pin;

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use url::Url;

/// Default configuration values used when not specified in environment.
pub mod defaults {
    pub const DB_PATH: &str = "nervemq.db";
    /// File name of the sessions database; placed next to the main
    /// database file unless `NERVEMQ_SESSIONS_DB_PATH` overrides it.
    pub const SESSIONS_DB_FILE: &str = "sessions.db";
    /// Default per-queue delivery-attempt cap. Counts every receive,
    /// including the first delivery: 2 means one initial delivery plus one
    /// redelivery before the message parks as `failed`.
    pub const MAX_RETRIES: usize = 2;

    /// Default visibility timeout (in seconds) applied when neither the
    /// ReceiveMessage request nor the queue specifies one. Mirrors AWS SQS.
    pub const VISIBILITY_TIMEOUT: u64 = 30;

    pub const HOST: &str = "http://localhost:8080";

    /// Socket address the HTTP server binds to. Loopback by default so a
    /// locally run server isn't exposed on the network; override with
    /// `NERVEMQ_BIND_ADDRESS` (e.g. `0.0.0.0:8080`) to listen on all
    /// interfaces, as the Docker image does.
    pub const BIND_ADDRESS: &str = "127.0.0.1:8080";

    pub const ROOT_EMAIL: &str = "admin@example.com";
    pub const ROOT_PASSWORD: &str = "password";
}

#[derive(Debug, snafu::Snafu)]
pub enum ConfigError {
    FatalConflict {
        conflicts: Vec<Conflict>,
    },
    Environment {
        #[snafu(source)]
        source: envy::Error,
    },
}

impl From<envy::Error> for ConfigError {
    fn from(err: envy::Error) -> Self {
        ConfigError::Environment { source: err }
    }
}

#[derive(Debug)]
pub enum ConflictSeverity {
    Fatal,
    Warning,
}

#[derive(Debug)]
#[allow(unused)]
pub struct Conflict {
    severity: ConflictSeverity,
    field: String,
    message: String,
}

pub trait Configuration: for<'de> Deserialize<'de> + 'static {
    fn apply(
        self,
        other: Self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self, ConfigError>>>>;

    fn validate(self) -> Pin<Box<dyn std::future::Future<Output = Result<Self, ConfigError>>>>
    where
        Self: Sized,
    {
        Box::pin(async move { Ok(self) })
    }
}

pub trait Layer {
    type Config: Configuration;

    fn resolve(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self::Config, ConfigError>>>>;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

pub struct ConfigBuilder<C> {
    base: C,
    layers: Vec<Box<dyn Layer<Config = C>>>,
}

impl<C> ConfigBuilder<C>
where
    C: Configuration + Default,
{
    pub fn new() -> Self {
        Self {
            base: Default::default(),
            layers: Vec::new(),
        }
    }

    pub fn with_layer<L: Layer<Config = C> + 'static>(mut self, layer: L) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    pub async fn load(self) -> Result<C, ConfigError> {
        let mut config = self.base;
        for layer in self.layers {
            let layer_config = layer.resolve().await?;
            config = config.apply(layer_config).await?;
        }

        config.validate().await
    }
}

pub struct ValueLayer {
    value: Config,
}

impl Layer for ValueLayer {
    type Config = Config;

    fn resolve(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self::Config, ConfigError>>>> {
        let value = self.value.clone();
        Box::pin(async { Ok(value) })
    }
}

pub struct EnvironmentLayer;

impl Layer for EnvironmentLayer {
    type Config = Config;

    fn resolve(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self::Config, ConfigError>>>> {
        Box::pin(async { Ok(envy::prefixed("NERVEMQ_").from_env::<Config>()?) })
    }
}

pub struct DefaultsLayer;

impl Layer for DefaultsLayer {
    type Config = Config;

    fn resolve(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self::Config, ConfigError>>>> {
        Box::pin(async {
            Ok(Config {
                db_path: Some(defaults::DB_PATH.to_string()),
                // Left unset on purpose: the accessor derives the default
                // from db_path's directory, which a later layer may change.
                sessions_db_path: None,
                default_max_retries: Some(defaults::MAX_RETRIES),
                host: Some(defaults::HOST.try_into().expect("valid default url")),
                bind_address: Some(defaults::BIND_ADDRESS.to_string()),
                root_email: Some(defaults::ROOT_EMAIL.to_string()),
                // Left unset on purpose: the accessor falls back to the default
                // for first-time setup, but leaving it `None` lets startup tell
                // "no password configured" from "configured to the default
                // value" and avoid overwriting an existing root password.
                root_password: None,
            })
        })
    }
}

/// Places the SQLite database files under an explicit directory.
///
/// Sets `db_path` to `<dir>/nervemq.db`. The sessions database is left unset
/// so [`Config::sessions_db_path`] derives it as a sibling — i.e. it lands in
/// the same directory — unless `NERVEMQ_SESSIONS_DB_PATH` overrides it.
///
/// Apply this *after* [`EnvironmentLayer`] so an explicit `--data-dir` wins
/// over `NERVEMQ_DB_PATH`.
pub struct DataDirLayer {
    dir: PathBuf,
}

impl DataDirLayer {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

impl Layer for DataDirLayer {
    type Config = Config;

    fn resolve(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self::Config, ConfigError>>>> {
        let db_path = self
            .dir
            .join(defaults::DB_PATH)
            .to_string_lossy()
            .into_owned();
        Box::pin(async move {
            Ok(Config {
                db_path: Some(db_path),
                ..Default::default()
            })
        })
    }
}

#[derive(Clone, Deserialize)]
/// Application configuration loaded from environment variables.
///
/// All fields are optional and fall back to values in `defaults` module.
/// Environment variables are prefixed with `NERVEMQ_` when loading.
///
/// # Fields
/// * `db_path` - Path to the SQLite database file
/// * `sessions_db_path` - Path to the admin-sessions SQLite database file
/// * `default_max_retries` - Maximum number of retry attempts for failed messages
/// * `host` - Base URL for the server
/// * `bind_address` - Socket address the HTTP server listens on
/// * `root_email` - Email address for the root admin user
/// * `root_password` - Password for the root admin user (stored securely)
///
/// # Environment Variables
/// * `NERVEMQ_DB_PATH`             - Database file path
/// * `NERVEMQ_SESSIONS_DB_PATH`    - Sessions database file path
/// * `NERVEMQ_DEFAULT_MAX_RETRIES` - Default retry limit
/// * `NERVEMQ_HOST`                - Server host URL (for UI access)
/// * `NERVEMQ_BIND_ADDRESS`        - Socket address to listen on (e.g. `0.0.0.0:8080`)
/// * `NERVEMQ_ROOT_EMAIL`          - Root admin email
/// * `NERVEMQ_ROOT_PASSWORD`       - Root admin password
pub struct Config {
    db_path: Option<String>,
    sessions_db_path: Option<String>,
    default_max_retries: Option<usize>,

    host: Option<Url>,
    bind_address: Option<String>,

    root_email: Option<String>,
    root_password: Option<SecretString>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_path: None,
            sessions_db_path: None,
            default_max_retries: None,
            host: None,
            bind_address: None,
            root_email: None,
            root_password: None,
        }
    }
}

impl Configuration for Config {
    fn apply(
        mut self,
        other: Self,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Self, ConfigError>>>> {
        Box::pin(async move {
            if let Some(other_db_path) = other.db_path {
                self.db_path = Some(other_db_path);
            }

            if let Some(other_sessions_db_path) = other.sessions_db_path {
                self.sessions_db_path = Some(other_sessions_db_path);
            }

            if let Some(other_max_retries) = other.default_max_retries {
                self.default_max_retries = Some(other_max_retries);
            }

            if let Some(other_host) = other.host {
                self.host = Some(other_host);
            }

            if let Some(other_bind_address) = other.bind_address {
                self.bind_address = Some(other_bind_address);
            }

            if let Some(other_root_email) = other.root_email {
                self.root_email = Some(other_root_email);
            }

            if let Some(other_root_password) = other.root_password {
                self.root_password = Some(other_root_password);
            }
            Ok(self)
        })
    }

    fn validate(self) -> Pin<Box<dyn std::future::Future<Output = Result<Self, ConfigError>>>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            if self.root_email.is_none() {
                tracing::warn!(
                    "No root email provided, using default - don't do this in production!"
                );
            }

            if self.root_password.is_none() {
                tracing::warn!(
                    "No root password provided, using default - don't do this in production!"
                );
            }

            Ok(self)
        })
    }
}

impl Config {
    /// Gets the configured server host URL.
    ///
    /// # Returns
    /// The configured host URL or the default if not specified
    pub fn host(&self) -> Url {
        self.host
            .clone()
            .unwrap_or(defaults::HOST.try_into().expect("valid default url"))
    }

    /// Gets the socket address the HTTP server should listen on.
    ///
    /// # Returns
    /// The configured bind address (`NERVEMQ_BIND_ADDRESS`) or the default
    /// loopback address if not specified.
    pub fn bind_address(&self) -> &str {
        self.bind_address
            .as_deref()
            .unwrap_or(defaults::BIND_ADDRESS)
    }

    /// Gets the database file path.
    ///
    /// # Returns
    /// The configured database path or the default if not specified
    pub fn db_path(&self) -> &str {
        self.db_path
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or(defaults::DB_PATH)
    }

    /// Gets the sessions database file path.
    ///
    /// # Returns
    /// The configured path, or `sessions.db` in the same directory as the
    /// main database file — so relocating the main database via
    /// `NERVEMQ_DB_PATH` carries the sessions file along by default.
    pub fn sessions_db_path(&self) -> String {
        self.sessions_db_path.clone().unwrap_or_else(|| {
            std::path::Path::new(self.db_path())
                .parent()
                .map(|dir| dir.join(defaults::SESSIONS_DB_FILE))
                .unwrap_or_else(|| defaults::SESSIONS_DB_FILE.into())
                .to_string_lossy()
                .into_owned()
        })
    }

    /// Gets the maximum number of retry attempts for failed messages.
    ///
    /// # Returns
    /// The configured retry limit or the default if not specified
    pub fn default_max_retries(&self) -> usize {
        self.default_max_retries.unwrap_or(defaults::MAX_RETRIES)
    }

    /// Gets the root administrator email address.
    ///
    /// # Returns
    /// The configured root email or the default if not specified
    pub fn root_email(&self) -> &str {
        self.root_email
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or(defaults::ROOT_EMAIL)
    }

    /// Gets the root administrator password.
    ///
    /// # Returns
    /// The configured root password or the default if not specified
    ///
    /// # Security
    /// The password is stored as a SecretString but must be exposed
    /// for authentication. Care should be taken when using this value.
    pub fn root_password(&self) -> &str {
        self.root_password
            .as_ref()
            .map(|s| s.expose_secret())
            .unwrap_or(defaults::ROOT_PASSWORD)
    }

    /// Whether a root password was explicitly configured (via
    /// `NERVEMQ_ROOT_PASSWORD` or a config layer), as opposed to falling back
    /// to the built-in default.
    ///
    /// Startup uses this to decide whether to overwrite an existing root
    /// user's stored password: it does so only when a password was actually
    /// provided, so an unset variable never resets a password set elsewhere.
    pub fn root_password_provided(&self) -> bool {
        self.root_password.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `EnvironmentLayer` reads real process environment, which is shared
    /// across the parallel test threads — serialize every test that touches
    /// `NERVEMQ_*` variables.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn accessors_fall_back_to_defaults() {
        let config = Config::default();

        assert_eq!(config.db_path(), defaults::DB_PATH);
        assert_eq!(config.default_max_retries(), defaults::MAX_RETRIES);
        assert_eq!(config.host(), Url::parse(defaults::HOST).unwrap());
        assert_eq!(config.bind_address(), defaults::BIND_ADDRESS);
        assert_eq!(config.root_email(), defaults::ROOT_EMAIL);
        assert_eq!(config.root_password(), defaults::ROOT_PASSWORD);
    }

    #[test]
    fn root_password_provided_reflects_explicit_configuration() {
        // Default config: nothing explicitly provided.
        assert!(!Config::default().root_password_provided());

        // Explicitly configured (as `NERVEMQ_ROOT_PASSWORD` or a config layer
        // would set it).
        let config = Config {
            root_password: Some(SecretString::new("hunter2".into())),
            ..Default::default()
        };
        assert!(config.root_password_provided());
        assert_eq!(config.root_password(), "hunter2");
    }

    #[tokio::test]
    async fn defaults_layer_supplies_every_field() {
        let config = ConfigBuilder::new().with_layer(DefaultsLayer).load().await.unwrap();

        assert_eq!(config.db_path, Some(defaults::DB_PATH.to_string()));
        assert_eq!(config.default_max_retries, Some(defaults::MAX_RETRIES));
        assert_eq!(config.host, Some(Url::parse(defaults::HOST).unwrap()));
        assert_eq!(config.bind_address, Some(defaults::BIND_ADDRESS.to_string()));
        assert_eq!(config.root_email, Some(defaults::ROOT_EMAIL.to_string()));
        // root_password is left unset (like sessions_db_path); the accessor
        // still falls back to the default, but `root_password_provided` reports
        // that nothing was explicitly configured.
        assert!(config.root_password.is_none());
        assert!(!config.root_password_provided());
        assert_eq!(config.root_password(), defaults::ROOT_PASSWORD);
    }

    #[tokio::test]
    async fn an_empty_builder_validates_to_the_base_config() {
        // No layers: validate() warns about the missing root credentials but
        // must still succeed, leaving every field unset.
        let config = ConfigBuilder::<Config>::new().load().await.unwrap();
        assert!(config.db_path.is_none());
        assert!(config.root_email.is_none());
    }

    #[tokio::test]
    async fn later_layers_override_earlier_ones_field_by_field() {
        let overrides = Config {
            db_path: Some("custom.db".to_string()),
            default_max_retries: Some(7),
            ..Default::default()
        };

        let config = ConfigBuilder::new()
            .with_layer(DefaultsLayer)
            .with_layer(ValueLayer { value: overrides })
            .load()
            .await
            .unwrap();

        // Fields the later layer sets win; fields it leaves None keep the
        // earlier layer's values rather than being cleared.
        assert_eq!(config.db_path(), "custom.db");
        assert_eq!(config.default_max_retries(), 7);
        assert_eq!(config.host(), Url::parse(defaults::HOST).unwrap());
        assert_eq!(config.root_email(), defaults::ROOT_EMAIL);
    }

    #[tokio::test]
    async fn environment_layer_reads_prefixed_variables() {
        let _guard = ENV_LOCK.lock().unwrap();

        std::env::set_var("NERVEMQ_DB_PATH", "/tmp/env-test.db");
        std::env::set_var("NERVEMQ_DEFAULT_MAX_RETRIES", "9");
        std::env::set_var("NERVEMQ_BIND_ADDRESS", "0.0.0.0:9000");
        std::env::set_var("NERVEMQ_ROOT_EMAIL", "env-root@example.com");

        let result = ConfigBuilder::new()
            .with_layer(DefaultsLayer)
            .with_layer(EnvironmentLayer)
            .load()
            .await;

        std::env::remove_var("NERVEMQ_DB_PATH");
        std::env::remove_var("NERVEMQ_DEFAULT_MAX_RETRIES");
        std::env::remove_var("NERVEMQ_BIND_ADDRESS");
        std::env::remove_var("NERVEMQ_ROOT_EMAIL");

        let config = result.unwrap();
        assert_eq!(config.db_path(), "/tmp/env-test.db");
        assert_eq!(config.default_max_retries(), 9);
        assert_eq!(config.bind_address(), "0.0.0.0:9000");
        assert_eq!(config.root_email(), "env-root@example.com");
        // Untouched by the environment: still the defaults layer's value.
        assert_eq!(config.host(), Url::parse(defaults::HOST).unwrap());
    }

    #[tokio::test]
    async fn malformed_environment_values_are_environment_errors() {
        let _guard = ENV_LOCK.lock().unwrap();

        std::env::set_var("NERVEMQ_DEFAULT_MAX_RETRIES", "not-a-number");

        let result = ConfigBuilder::new().with_layer(EnvironmentLayer).load().await;

        std::env::remove_var("NERVEMQ_DEFAULT_MAX_RETRIES");

        assert!(matches!(result, Err(ConfigError::Environment { .. })));
    }

    #[test]
    fn sessions_db_path_defaults_to_a_sibling_of_the_main_database() {
        // Unset: derived from db_path's directory.
        let config = Config::default();
        assert_eq!(config.sessions_db_path(), "sessions.db");

        let config = Config {
            db_path: Some("/data/nervemq.db".to_string()),
            ..Default::default()
        };
        assert_eq!(config.sessions_db_path(), "/data/sessions.db");

        // Explicit configuration wins over derivation.
        let config = Config {
            db_path: Some("/data/nervemq.db".to_string()),
            sessions_db_path: Some("/elsewhere/s.db".to_string()),
            ..Default::default()
        };
        assert_eq!(config.sessions_db_path(), "/elsewhere/s.db");
    }

    #[tokio::test]
    async fn data_dir_layer_places_databases_in_the_directory() {
        let config = ConfigBuilder::new()
            .with_layer(DefaultsLayer)
            .with_layer(DataDirLayer::new("/data/nervemq"))
            .load()
            .await
            .unwrap();

        assert_eq!(config.db_path(), "/data/nervemq/nervemq.db");
        // Sessions follow the main database into the same directory.
        assert_eq!(config.sessions_db_path(), "/data/nervemq/sessions.db");
    }

    #[tokio::test]
    async fn data_dir_layer_overrides_environment_db_path() {
        let _guard = ENV_LOCK.lock().unwrap();

        std::env::set_var("NERVEMQ_DB_PATH", "/tmp/env-test.db");

        let result = ConfigBuilder::new()
            .with_layer(DefaultsLayer)
            .with_layer(EnvironmentLayer)
            .with_layer(DataDirLayer::new("/data"))
            .load()
            .await;

        std::env::remove_var("NERVEMQ_DB_PATH");

        let config = result.unwrap();
        assert_eq!(config.db_path(), "/data/nervemq.db");
    }

    #[test]
    fn layer_names_identify_the_layer_type() {
        assert!(Layer::name(&DefaultsLayer).contains("DefaultsLayer"));
        assert!(Layer::name(&EnvironmentLayer).contains("EnvironmentLayer"));
    }
}
