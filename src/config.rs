//! Configuration management for NerveMQ.
//!
//! Handles loading and accessing configuration values from environment
//! variables with fallback to default values.

use std::pin::Pin;

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use url::Url;

/// Default configuration values used when not specified in environment.
pub mod defaults {
    pub const DB_PATH: &str = "nervemq.db";
    /// Default per-queue delivery-attempt cap. Counts every receive,
    /// including the first delivery: 2 means one initial delivery plus one
    /// redelivery before the message parks as `failed`.
    pub const MAX_RETRIES: usize = 2;

    /// Default visibility timeout (in seconds) applied when neither the
    /// ReceiveMessage request nor the queue specifies one. Mirrors AWS SQS.
    pub const VISIBILITY_TIMEOUT: u64 = 30;

    pub const HOST: &str = "http://localhost:8080";

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
                default_max_retries: Some(defaults::MAX_RETRIES),
                host: Some(defaults::HOST.try_into().expect("valid default url")),
                root_email: Some(defaults::ROOT_EMAIL.to_string()),
                root_password: Some(SecretString::new(defaults::ROOT_PASSWORD.into())),
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
/// * `default_max_retries` - Maximum number of retry attempts for failed messages
/// * `host` - Base URL for the server
/// * `root_email` - Email address for the root admin user
/// * `root_password` - Password for the root admin user (stored securely)
///
/// # Environment Variables
/// * `NERVEMQ_DB_PATH`             - Database file path
/// * `NERVEMQ_DEFAULT_MAX_RETRIES` - Default retry limit
/// * `NERVEMQ_HOST`                - Server host URL (for UI access)
/// * `NERVEMQ_ROOT_EMAIL`          - Root admin email
/// * `NERVEMQ_ROOT_PASSWORD`       - Root admin password
pub struct Config {
    db_path: Option<String>,
    default_max_retries: Option<usize>,

    host: Option<Url>,

    root_email: Option<String>,
    root_password: Option<SecretString>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_path: None,
            default_max_retries: None,
            host: None,
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

            if let Some(other_max_retries) = other.default_max_retries {
                self.default_max_retries = Some(other_max_retries);
            }

            if let Some(other_host) = other.host {
                self.host = Some(other_host);
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
        assert_eq!(config.root_email(), defaults::ROOT_EMAIL);
        assert_eq!(config.root_password(), defaults::ROOT_PASSWORD);
    }

    #[tokio::test]
    async fn defaults_layer_supplies_every_field() {
        let config = ConfigBuilder::new().with_layer(DefaultsLayer).load().await.unwrap();

        assert_eq!(config.db_path, Some(defaults::DB_PATH.to_string()));
        assert_eq!(config.default_max_retries, Some(defaults::MAX_RETRIES));
        assert_eq!(config.host, Some(Url::parse(defaults::HOST).unwrap()));
        assert_eq!(config.root_email, Some(defaults::ROOT_EMAIL.to_string()));
        assert!(config.root_password.is_some());
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
        std::env::set_var("NERVEMQ_ROOT_EMAIL", "env-root@example.com");

        let result = ConfigBuilder::new()
            .with_layer(DefaultsLayer)
            .with_layer(EnvironmentLayer)
            .load()
            .await;

        std::env::remove_var("NERVEMQ_DB_PATH");
        std::env::remove_var("NERVEMQ_DEFAULT_MAX_RETRIES");
        std::env::remove_var("NERVEMQ_ROOT_EMAIL");

        let config = result.unwrap();
        assert_eq!(config.db_path(), "/tmp/env-test.db");
        assert_eq!(config.default_max_retries(), 9);
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
    fn layer_names_identify_the_layer_type() {
        assert!(Layer::name(&DefaultsLayer).contains("DefaultsLayer"));
        assert!(Layer::name(&EnvironmentLayer).contains("EnvironmentLayer"));
    }
}
