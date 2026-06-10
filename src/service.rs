//! Core service implementation for NerveMQ message queue system.
//!
//! This module implements the main service layer that provides:
//!
//! - Queue management (create, delete, list, purge)
//! - Message operations (send, receive, delete)
//! - Namespace management (create, delete, list)
//! - User management and authentication
//! - Statistics and monitoring
//!
//! # Key Types
//!
//! - [`Service`] - The main service struct that handles all operations
//! - [`QueueAttributes`] - Configuration options for queues
//! - [`QueueConfig`] - Internal queue configuration
//! - [`MessageDetails`] - Detailed message information
//!
//! # Examples
//!
//! ```no_run
//! use std::collections::HashMap;
//!
//! use actix_identity::Identity;
//! use nervemq::service::Service;
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to the service (configuration is read from the environment)
//!     let service = Service::connect().await?;
//!
//!     // The identity of the acting user; in a request handler this is
//!     // extracted from the session rather than mocked.
//!     let identity = || Identity::mock("admin@example.com".to_string());
//!
//!     // Create a namespace
//!     service.create_namespace("my-namespace", identity()).await?;
//!
//!     // Create a queue
//!     service.create_queue(
//!         "my-namespace",
//!         "my-queue",
//!         HashMap::new(),
//!         HashMap::new(),
//!         identity()
//!     ).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Database Schema
//!
//! The service uses SQLite with the following main tables:
//!
//! - `namespaces` - Namespace definitions
//! - `queues` - Queue definitions
//! - `messages` - Message storage
//! - `users` - User accounts
//! - `user_permissions` - Access control
//! - `queue_configurations` - Queue settings
//! - `queue_attributes` - Queue attributes
//! - `queue_tags` - Queue metadata
//! - `kv_pairs` - Message attributes
//!
//! # Architecture
//!
//! The service implements an AWS SQS-compatible message queue with:
//!
//! - Multi-tenant support via namespaces
//! - Role-based access control
//! - Dead letter queues
//! - Message attributes
//! - Configurable retry policies
//! - Queue tags and attributes
//!
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    sync::Arc,
};

use actix_identity::Identity;
use actix_web::{error::ErrorUnauthorized, web, ResponseError};
use base64::Engine;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_email::Email;
use sqlx::{
    sqlite::{
        SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode,
        SqlitePoolOptions,
    },
    Acquire, FromRow, Sqlite, SqlitePool,
};
use tokio::task::JoinSet;
use tokio_stream::StreamExt as _;

use crate::{
    api::{
        auth::{Permission, Role, User},
        tokens::CreateTokenResponse,
    },
    auth::crypto::{generate_api_key, hash_secret, GeneratedKey},
    config::Config,
    error::Error,
    kms::{memory::InMemoryKeyManager, KeyManager},
    message::{Message, MessageStatus},
    namespace::{Namespace, NamespaceStatistics},
    queue::{Queue, QueueStatistics},
    sqs::types::{SqsMessage, SqsMessageAttribute},
    types::{
        send_message::{SendMessageRequest, SendMessageResponse},
        send_message_batch::{
            SendMessageBatchRequest, SendMessageBatchResponse, SendMessageBatchResultEntry,
            SendMessageBatchResultErrorEntry,
        },
    },
};

/// Configuration for dead-letter queue redrive policy.
///
/// This defines how failed messages should be moved to a dead-letter queue
/// after exceeding the maximum number of receive attempts.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RedrivePolicy {
    /// The field is named ARN, but for NerveMQ we use the format `namespace:queue`
    dead_letter_target_arn: String,
    max_receive_count: u64,
}

/// Configurable attributes for a queue.
///
/// These attributes control the queue's behavior including:
/// - Message delay
/// - Message size limits
/// - Message retention
/// - Visibility timeout
/// - Dead letter queue configuration
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueueAttributes {
    pub delay_seconds: Option<u64>,
    pub max_message_size: Option<u64>,
    pub message_retention_period: Option<u64>,
    pub receive_message_wait_time_seconds: Option<u64>,
    pub visibility_timeout: Option<u64>,

    // TODO: RedrivePolicy, RedriveAllowPolicy
    pub redrive_policy: Option<RedrivePolicy /* Must be JSON serialized to a string */>,

    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct QueueAttributesSer {
    pub delay_seconds: Option<u64>,
    pub max_message_size: Option<u64>,
    pub message_retention_period: Option<u64>,
    pub receive_message_wait_time_seconds: Option<u64>,
    pub visibility_timeout: Option<u64>,

    // TODO: RedrivePolicy, RedriveAllowPolicy
    pub redrive_policy: Option<String /* Must be JSON serialized to a string */>,

    #[serde(flatten)]
    pub other: HashMap<String, serde_json::Value>,
}

impl QueueAttributesSer {
    pub fn deser(self) -> Result<QueueAttributes, Error> {
        Ok(QueueAttributes {
            delay_seconds: self.delay_seconds,
            max_message_size: self.max_message_size,
            message_retention_period: self.message_retention_period,
            receive_message_wait_time_seconds: self.receive_message_wait_time_seconds,
            visibility_timeout: self.visibility_timeout,
            redrive_policy: self
                .redrive_policy
                .map(|rp| serde_json::from_str(&rp))
                .transpose()?,
            other: self.other,
        })
    }
}

impl QueueAttributes {
    pub fn ser(self) -> Result<QueueAttributesSer, Error> {
        Ok(QueueAttributesSer {
            delay_seconds: self.delay_seconds,
            max_message_size: self.max_message_size,
            message_retention_period: self.message_retention_period,
            receive_message_wait_time_seconds: self.receive_message_wait_time_seconds,
            visibility_timeout: self.visibility_timeout,
            redrive_policy: self
                .redrive_policy
                .map(|rp| serde_json::to_string(&rp))
                .transpose()?,
            other: self.other,
        })
    }
}

/// Trait for type-safe queue attributes.
///
/// Used to define queue attribute names and types for extraction from
/// the database.
#[allow(unused)]
pub trait QueueAttribute {
    type Value;

    /// Returns the name of the queue attribute's column in the database.
    fn name(&self) -> &str;
}

#[allow(unused)]
pub(crate) mod queue_attributes {
    use super::QueueAttribute;

    /// Represents the delay_seconds queue attribute.
    pub struct DelaySeconds;

    impl QueueAttribute for DelaySeconds {
        type Value = u64;

        fn name(&self) -> &str {
            "delay_seconds"
        }
    }

    /// Represents the max_message_size queue
    pub struct MaxMessageSize;

    impl QueueAttribute for MaxMessageSize {
        type Value = u64;

        fn name(&self) -> &str {
            "max_message_size"
        }
    }

    /// Represents the message_retention_period queue attribute.
    pub struct MessageRetentionPeriod;

    impl QueueAttribute for MessageRetentionPeriod {
        type Value = u64;
        fn name(&self) -> &str {
            "message_retention_period"
        }
    }

    /// Represents the receive_message_wait_time_seconds queue attribute.
    pub struct ReceiveMessageWaitTimeSeconds;

    impl QueueAttribute for ReceiveMessageWaitTimeSeconds {
        type Value = u64;
        fn name(&self) -> &str {
            "receive_message_wait_time_seconds"
        }
    }

    /// Represents the visibility_timeout queue.
    pub struct VisibilityTimeout;

    impl QueueAttribute for VisibilityTimeout {
        type Value = u64;
        fn name(&self) -> &str {
            "visibility_timeout"
        }
    }

    /// Represents the redrive_policy queue attribute.
    pub struct RedrivePolicy;

    impl QueueAttribute for RedrivePolicy {
        type Value = String;

        fn name(&self) -> &str {
            "redrive_policy"
        }
    }

    /// Represents an arbitrary stringly-typed queue attribute.
    pub struct Other(String);

    impl QueueAttribute for Other {
        type Value = String;

        fn name(&self) -> &str {
            &self.0
        }
    }
}

/// Internal configuration for a queue stored in the database.
///
/// Contains:
/// - Queue ID
/// - Maximum retry attempts
/// - Optional dead letter queue ID
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct QueueConfig {
    pub queue: u64,
    pub max_retries: u64,
    pub dead_letter_queue: Option<u64>,
}

/// Represents the details of a message for display in the UI.
/// Detailed information about a message for display in the UI.
///
/// Includes:
/// - Message ID and queue
/// - Delivery status and attempts
/// - Message body and attributes
/// - Timestamps
#[derive(Debug, Serialize)]
pub struct MessageDetails {
    pub id: u64,
    pub queue: String,

    pub delivered_at: Option<u64>,
    pub sent_by: Option<u64>,
    pub body: String,
    pub tries: u64,

    pub status: MessageStatus,

    pub message_attributes: HashMap<String, serde_json::Value>,
}

/// Main service struct that handles all queue operations.
///
/// The service manages:
/// - Queue and message operations
/// - User authentication and authorization
/// - Database connections
/// - Key management for encryption
#[derive(Clone)]
pub struct Service {
    kms: Arc<dyn KeyManager>,
    db: SqlitePool,
    config: Arc<crate::config::Config>,
}

#[bon::bon]
impl Service {
    /// Returns a reference to the underlying SQLite connection pool.
    pub fn db(&self) -> &SqlitePool {
        &self.db
    }

    /// Creates a new Service instance with default configuration and in-memory key management.
    ///
    /// Mostly useful for tests and debugging.
    #[allow(unused)]
    pub async fn connect() -> Result<Self, Error> {
        Self::connect_with()
            .config(Config::default())
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
    }

    /// Returns a reference to the service configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Creates a new Service instance with custom configuration and key management.
    ///
    /// # Arguments
    /// * `config` - Custom service configuration
    /// * `kms_factory` - Factory function to create a key management service
    #[builder]
    pub async fn connect_with<K, F, R>(config: Config, kms_factory: F) -> Result<Self, Error>
    where
        F: FnOnce(SqlitePool) -> R,
        R: Future<Output = Result<K, Error>>,
        K: KeyManager,
    {
        let opts = SqliteConnectOptions::new()
            .filename(config.db_path())
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .locking_mode(SqliteLockingMode::Normal)
            .optimize_on_close(true, None)
            .auto_vacuum(SqliteAutoVacuum::Full);

        let pool = SqlitePoolOptions::new().connect_with(opts).await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        let kms = kms_factory(pool.clone()).await?;

        let svc = Self {
            kms: Arc::new(kms),
            db: pool,
            config: Arc::new(config),
        };

        match svc
            .create_user(
                Email::from_str(svc.config.root_email()).map_err(Error::internal)?,
                svc.config().root_password().to_owned().into(),
                Some(Role::Admin),
                vec![],
            )
            .await
        {
            Ok(_) => {
                tracing::info!("Root user created");
            }
            Err(e) => match e {
                Error::Sqlx { source } => match source {
                    sqlx::Error::Database(db_err) => match db_err.kind() {
                        sqlx::error::ErrorKind::UniqueViolation => {
                            tracing::info!("Root user already exists");
                        }
                        _ => tracing::warn!("{db_err}"),
                    },
                    other => tracing::warn!("{other}"),
                },
                other => tracing::warn!("{other}"),
            },
        };

        Ok(svc)
    }

    /// Deletes a user account and their associated encryption key.
    ///
    /// # Arguments
    /// * `email` - Email address of the user to delete
    pub async fn delete_user(&self, email: Email) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        let key_id = sqlx::query_scalar(
            "
            DELETE FROM users
            WHERE email = $1
            RETURNING kms_key_id
            ",
        )
        .bind(email.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

        self.kms.delete_key(&key_id).await?;

        tx.commit().await?;

        Ok(())
    }

    /// Gets the internal ID for a queue given its namespace and name.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `name` - Name of the queue
    /// * `exec` - Database executor to use
    pub async fn get_queue_id(
        &self,
        namespace: &str,
        name: &str,
        exec: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<Option<u64>, Error> {
        Ok(sqlx::query_scalar(
            "
            SELECT q.id FROM queues q
            JOIN namespaces n ON q.ns = n.id
            WHERE n.name = $1 AND q.name = $2
            ",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&mut *exec.acquire().await?)
        .await?)
    }

    /// Gets the internal ID for a namespace given its name.
    ///
    /// # Arguments
    /// * `name` - Name of the namespace
    /// * `ex` - Database executor to use
    pub async fn get_namespace_id<'a>(
        &self,
        name: &str,
        ex: impl Acquire<'a, Database = Sqlite>,
    ) -> Result<Option<u64>, Error> {
        Ok(sqlx::query_scalar(
            "
            SELECT id FROM namespaces WHERE name = $1
            ",
        )
        .bind(name)
        .fetch_optional(&mut *ex.acquire().await?)
        .await?)
    }

    /// Lists all namespaces accessible to the authenticated user.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn list_namespaces(&self, identity: Identity) -> Result<Vec<Namespace>, Error> {
        let email = identity.id()?;

        Ok(sqlx::query_as(
            "
            SELECT ns.id, ns.name, nu.email as created_by FROM namespaces ns
            JOIN user_permissions p ON p.namespace = ns.id
            JOIN users u ON p.user = u.id
            JOIN users nu ON ns.created_by = nu.id
            WHERE u.email = $1
        ",
        )
        .bind(email)
        .fetch_all(&mut *self.db.acquire().await?)
        .await?)
    }

    /// Verifies that a user has at least the specified role level.
    ///
    /// # Arguments
    /// * `identity` - Identity of the user to check
    /// * `role` - Minimum required role level
    pub async fn check_user_role(&self, identity: Identity, role: Role) -> Result<(), Error> {
        let email = identity.id()?;
        let user: User = sqlx::query_as("SELECT * FROM users WHERE email = $1")
            .bind(email)
            .fetch_one(&mut *self.db.acquire().await?)
            .await?;
        if user.role < role {
            return Err(Error::Unauthorized);
        }

        return Ok(());
    }

    /// Creates a new namespace. Only admin users can create namespaces.
    ///
    /// # Arguments
    /// * `name` - Name of the namespace to create
    /// * `identity` - Identity of the authenticated admin user
    pub async fn create_namespace(&self, name: &str, identity: Identity) -> Result<u64, Error> {
        let mut tx = self.db().begin().await?;

        let user_email = identity.id()?;

        let user: User = sqlx::query_as("SELECT * FROM users WHERE email = $1")
            .bind(&user_email)
            .fetch_optional(&mut *tx.acquire().await?)
            .await?
            .ok_or_else(|| Error::Unauthorized)?;

        if user.role != Role::Admin {
            return Err(Error::Unauthorized);
        }

        let ns_id: u64 = sqlx::query_scalar(
            "INSERT INTO namespaces(name, created_by) VALUES ($1, $2) RETURNING id",
        )
        .bind(name)
        .bind(user.id as i64)
        .fetch_one(&mut *tx.as_mut().acquire().await?)
        .await?;

        sqlx::query(
            "
            INSERT INTO user_permissions (user, namespace, can_delete_ns)
            VALUES ($1, $2, true)
        ",
        )
        .bind(user.id as i64)
        .bind(ns_id as i64)
        .execute(&mut *tx.as_mut().acquire().await?)
        .await?;

        tx.commit().await?;

        Ok(user.id)
    }

    /// Deletes a namespace and all its queues. User must have delete permission.
    ///
    /// # Arguments
    /// * `name` - Name of the namespace to delete
    /// * `identity` - Identity of the authenticated user
    pub async fn delete_namespace(&self, name: &str, identity: Identity) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        let namespace = self
            .get_namespace_id(name, &mut tx)
            .await?
            .ok_or_else(|| eyre::eyre!("Namespace {name} does not exist"))?;

        let (_user_id, can_delete) = self
            .check_user_access(&identity, namespace, &mut tx)
            .await?;

        if !can_delete {
            return Err(Error::Unauthorized);
        }

        sqlx::query(
            "
            DELETE FROM namespaces WHERE name = $1
        ",
        )
        .bind(name)
        .execute(&mut *tx)
        .await
        .map(|_| ())?;

        tx.commit().await?;

        Ok(())
    }

    /// Checks if a user has access to a namespace and returns their permissions.
    ///
    /// # Arguments
    /// * `identity` - Identity of the user to check
    /// * `ns` - ID of the namespace
    /// * `exec` - Database executor to use
    ///
    /// # Returns
    /// Tuple of (user_id, can_delete_ns)
    pub async fn check_user_access<'a>(
        &self,
        identity: &Identity,
        ns: u64,
        exec: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<(u64, bool), Error> {
        let email = identity.id()?;
        let mut db = exec.acquire().await?;

        let res: Option<Permission> = sqlx::query_as(
            "
            SELECT p.* FROM user_permissions p
            JOIN users u ON p.user = u.id
            WHERE u.email = $1 AND p.namespace = $2
        ",
        )
        .bind(email)
        .bind(ns as i64)
        .fetch_optional(&mut *db)
        .await?;

        match res {
            Some(permission) => Ok((permission.user, permission.can_delete_ns)),
            None => Err(Error::Unauthorized),
        }
    }

    /// Creates a new queue in a namespace.
    ///
    /// # Arguments
    /// * `namespace` - Namespace to create the queue in
    /// * `name` - Name of the queue
    /// * `attributes` - Queue configuration attributes
    /// * `tags` - Metadata tags for the queue
    /// * `identity` - Identity of the authenticated user
    pub async fn create_queue(
        &self,
        namespace: &str,
        name: &str,
        attributes: HashMap<String, String>,
        tags: HashMap<String, String>,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        let namespace = self
            .get_namespace_id(namespace, &mut tx)
            .await?
            .ok_or_else(|| eyre::eyre!("Namespace {namespace} does not exist"))?;

        let (user_id, _) = self
            .check_user_access(&identity, namespace, &mut tx)
            .await?;

        let queue_id: u64 = sqlx::query_scalar(
            "
            INSERT INTO queues (ns, name, created_by)
            VALUES ($1, $2, $3)
            RETURNING id
        ",
        )
        .bind(namespace as i64)
        .bind(name)
        .bind(user_id as i64)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            "
            INSERT INTO queue_configurations (queue, max_retries)
            VALUES ($1, $2)
        ",
        )
        .bind(queue_id as i64)
        .bind(self.config.default_max_retries() as i64)
        .execute(&mut *tx)
        .await?;

        for (k, v) in attributes.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, $2, $3)
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await?;
        }

        for (k, v) in tags.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_tags (queue, k, v)
                VALUES ($1, $2, $3)
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub fn kms(&self) -> &dyn KeyManager {
        self.kms.as_ref()
    }

    /// Updates the attributes of an existing queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `attributes` - New queue attributes
    /// * `identity` - Identity of the authenticated user
    pub async fn set_queue_attributes(
        &self,
        ns: &str,
        queue: &str,
        attributes: QueueAttributesSer,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        let ns_id = self
            .get_namespace_id(ns, &mut *tx)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(&identity, ns_id, &mut *tx).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *tx)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        if let Some(delay_seconds) = attributes.delay_seconds {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'delay_seconds', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(delay_seconds as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(max_message_size) = attributes.max_message_size {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'max_message_size', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(max_message_size as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(message_retention_period) = attributes.message_retention_period {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'message_retention_period', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(message_retention_period as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(receive_message_wait_time_seconds) =
            attributes.receive_message_wait_time_seconds
        {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'receive_message_wait_time_seconds', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(receive_message_wait_time_seconds as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(visibility_timeout) = attributes.visibility_timeout {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'visibility_timeout', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(visibility_timeout as i64)
            .execute(&mut *tx)
            .await?;
        }

        if let Some(redrive_policy) = attributes.redrive_policy {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, 'redrive_policy', $2)
                ON CONFLICT (queue, k) DO UPDATE SET v = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(redrive_policy)
            .execute(&mut *tx)
            .await?;
        }

        for (k, v) in attributes.other.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_attributes (queue, k, v)
                VALUES ($1, $2, $3)
                ON CONFLICT (queue, k) DO UPDATE SET v = $3
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Gets the current attributes of a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `names` - Names of attributes to retrieve
    /// * `identity` - Identity of the authenticated user
    pub async fn get_queue_attributes(
        &self,
        ns: &str,
        queue: &str,
        names: &[String],
        identity: Identity,
    ) -> Result<QueueAttributesSer, Error> {
        let mut db = self.db().acquire().await?;

        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(&identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        let set = names.iter().collect::<HashSet<_>>();

        let mut res = sqlx::query_as::<_, (String, serde_json::Value)>(
            "
            SELECT k, v FROM queue_attributes WHERE queue = $1
            ",
        )
        .bind(queue_id as i64)
        .fetch(&mut *db);

        let mut attributes = QueueAttributesSer {
            delay_seconds: None,
            max_message_size: None,
            message_retention_period: None,
            receive_message_wait_time_seconds: None,
            visibility_timeout: None,
            redrive_policy: None,
            other: Default::default(),
        };
        while let Some((k, v)) = res.next().await.transpose()? {
            match &*k {
                "delay_seconds" => attributes.delay_seconds = Some(serde_json::from_value(v)?),
                "max_message_size" => {
                    attributes.max_message_size = Some(serde_json::from_value(v)?)
                }
                "message_retention_period" => {
                    attributes.message_retention_period = Some(serde_json::from_value(v)?)
                }
                "receive_message_wait_time_seconds" => {
                    attributes.receive_message_wait_time_seconds = Some(serde_json::from_value(v)?)
                }
                "visibility_timeout" => {
                    attributes.visibility_timeout = Some(serde_json::from_value(v)?)
                }
                "redrive_policy" => attributes.redrive_policy = Some(serde_json::from_value(v)?),
                _ => {
                    if set.contains(&k) {
                        attributes.other.insert(k, v);
                    }
                }
            }
        }

        Ok(attributes)
    }

    /// Adds or updates tags on a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `tags` - Tags to set
    /// * `identity` - Identity of the authenticated user
    pub async fn tag_queue(
        &self,
        ns: &str,
        queue: &str,
        tags: HashMap<String, String>,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut db = self.db().acquire().await?;
        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(&identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        for (k, v) in tags.into_iter() {
            sqlx::query(
                "
                INSERT INTO queue_tags (queue, k, v)
                VALUES ($1, $2, $3)
                ON CONFLICT (queue, k) DO UPDATE SET v
                ",
            )
            .bind(queue_id as i64)
            .bind(k)
            .bind(v)
            .execute(&mut *db)
            .await?;
        }

        Ok(())
    }

    /// Removes tags from a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `tags` - Tags to remove
    /// * `identity` - Identity of the authenticated user
    pub async fn untag_queue(
        &self,
        ns: &str,
        queue: &str,
        tags: Vec<String>,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut db = self.db().acquire().await?;
        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(&identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        for tag in tags {
            sqlx::query(
                "
                DELETE FROM queue_tags WHERE queue = $1 AND k = $2
                ",
            )
            .bind(queue_id as i64)
            .bind(tag)
            .execute(&mut *db)
            .await?;
        }

        Ok(())
    }

    /// Gets all tags for a queue.
    ///
    /// # Arguments
    /// * `ns` - Namespace containing the queue
    /// * `queue` - Name of the queue
    /// * `identity` - Identity of the authenticated user
    pub async fn get_queue_tags(
        &self,
        ns: &str,
        queue: &str,
        identity: Identity,
    ) -> Result<HashMap<String, String>, Error> {
        let mut db = self.db().acquire().await?;

        let ns_id = self
            .get_namespace_id(ns, &mut *db)
            .await?
            .ok_or(Error::namespace_not_found(ns))?;

        self.check_user_access(&identity, ns_id, &mut *db).await?;

        let queue_id = self
            .get_queue_id(ns, queue, &mut *db)
            .await?
            .ok_or(Error::queue_not_found(queue, ns))?;

        let res = sqlx::query_as(
            "
            SELECT k, v FROM queue_tags WHERE queue = $1
            ",
        )
        .bind(queue_id as i64)
        .fetch_all(&mut *db)
        .await?;

        Ok(res.into_iter().collect())
    }

    /// Deletes a queue and all its messages.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `name` - Name of the queue
    /// * `identity` - Identity of the authenticated user
    pub async fn delete_queue(
        &self,
        namespace: &str,
        name: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        let namespace_id = self
            .get_namespace_id(namespace, &mut tx)
            .await?
            .ok_or_else(|| eyre::eyre!("Namespace {namespace} does not exist"))?;

        self.check_user_access(&identity, namespace_id, &mut tx)
            .await?;

        let id = self
            .get_queue_id(namespace, name, &mut tx)
            .await?
            .ok_or_else(|| eyre::eyre!("Queue {name} does not exist"))?;

        sqlx::query("DELETE FROM queues WHERE id = $1")
            .bind(id as i64)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Lists queues, optionally filtered by namespace.
    ///
    /// # Arguments
    /// * `namespace` - Optional namespace to filter by
    /// * `identity` - Identity of the authenticated user
    pub async fn list_queues(
        &self,
        namespace: Option<&str>,
        identity: Identity,
    ) -> Result<Vec<Queue>, Error> {
        let mut conn = self.db().acquire().await?;

        if let Some(namespace) = namespace {
            let namespace_id = self
                .get_namespace_id(namespace, &mut *conn)
                .await?
                .ok_or_else(|| eyre::eyre!("Namespace {namespace} does not exist"))?;

            self.check_user_access(&identity, namespace_id, &mut *conn)
                .await?;
        }

        // Queue::list(conn.acquire().await?, namespace, identity).await

        match namespace {
            Some(ns) => self.list_queues_for_namespace(ns).await,
            None => self.list_all_queues(identity).await,
        }
    }

    /// Lists all queues in a specific namespace.
    ///
    /// # Arguments
    /// * `namespace` - Namespace to list queues from
    pub async fn list_queues_for_namespace(&self, namespace: &str) -> Result<Vec<Queue>, Error> {
        let mut db = self.db().acquire().await?;
        let mut stream = sqlx::query_as(
            "
            SELECT q.id, q.name, n.name as ns, u.email as created_by FROM queues q
            JOIN namespaces n ON q.ns = n.id
            JOIN users u on q.created_by = u.id
            WHERE n.name = $1",
        )
        .bind(namespace)
        .fetch(&mut *db);

        let mut queues = Vec::new();

        while let Some(res) = stream.next().await.transpose()? {
            queues.push(res);
        }

        Ok(queues)
    }

    /// Lists all queues accessible to the authenticated user.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn list_all_queues(&self, identity: Identity) -> Result<Vec<Queue>, Error> {
        let email = identity.id()?;

        let queues = sqlx::query_as(
            "
            SELECT q.id, q.name, qu.email as created_by, n.name as ns FROM queues q
            JOIN user_permissions p ON p.namespace = q.ns
            JOIN namespaces n ON n.id = q.ns
            JOIN users u ON u.id = p.user
            JOIN users qu ON q.id = q.created_by
            WHERE u.email = $1
            ",
        )
        .bind(email)
        .fetch_all(&mut *self.db().acquire().await?)
        .await?;

        Ok(queues)
    }

    /// Gets the KMS key ID associated with a user.
    ///
    /// # Arguments
    /// * `user_email` - Email of the user
    pub async fn get_key_id(&self, user_email: &str) -> Result<String, Error> {
        let key_id = sqlx::query_scalar(
            "
            SELECT kms_key_id FROM users
            WHERE email = $1
            ",
        )
        .bind(user_email)
        .fetch_one(self.db())
        .await?;

        Ok(key_id)
    }

    /// Creates an API token for accessing a namespace.
    ///
    /// # Arguments
    /// * `name` - Name of the token
    /// * `namespace` - Namespace to grant access to
    /// * `identity` - Identity of the authenticated user
    pub async fn create_token(
        &self,
        name: String,
        namespace: String,
        identity: Identity,
    ) -> Result<CreateTokenResponse, Error> {
        let GeneratedKey {
            short_token,
            long_token,
            long_token_hash,
        } = web::block(|| generate_api_key())
            .await
            .map_err(Error::internal)?
            .map_err(Error::internal)?;

        let mut tx = self.db().begin().await?;

        let namespace_id = self
            .get_namespace_id(&namespace, &mut *tx)
            .await
            .map_err(Error::internal)?
            .ok_or_else(|| Error::namespace_not_found(&namespace))?;

        self.check_user_access(&identity, namespace_id, &mut *tx)
            .await?;

        let key_id = self.get_key_id(&identity.id()?).await?;

        let encrypted_key = self
            .kms
            .encrypt(&key_id, long_token.as_bytes().to_vec())
            .await?;

        sqlx::query(
            "
            INSERT INTO api_keys (name, user, key_id, hashed_key, encrypted_key, ns)
            VALUES ($1, (SELECT id FROM users WHERE email = $2), $3, $4, $5, $6)
            ",
        )
        .bind(&name)
        .bind(identity.id().map_err(ErrorUnauthorized)?)
        .bind(&short_token)
        .bind(long_token_hash.to_string())
        .bind(encrypted_key)
        .bind(namespace_id as i64)
        .execute(&mut *tx)
        .await
        .map_err(Error::internal)?;

        tx.commit().await?;

        // Return the plain API key (should be securely sent/stored by the user).
        Ok(CreateTokenResponse {
            name,
            namespace,
            access_key: short_token,
            secret_key: long_token,
        })
    }

    /// Creates a new user account.
    ///
    /// # Arguments
    /// * `email` - User's email address
    /// * `password` - User's password
    /// * `role` - Optional role to assign
    /// * `namespaces` - Namespaces to grant access to
    pub async fn create_user(
        &self,
        email: Email,
        password: String,
        role: Option<Role>,
        namespaces: Vec<String>,
    ) -> Result<(), Error> {
        let hashed_password = web::block(move || hash_secret(password))
            .await
            .map_err(|e| Error::internal(e))??;

        let mut tx = self.db().begin().await?;

        let key_id = self.kms.create_key().await?;

        let user_id: u64 = sqlx::query_scalar(
            "
            INSERT INTO users (email, hashed_pass, role, kms_key_id)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ",
        )
        .bind(email.as_str())
        .bind(hashed_password.to_string())
        .bind(role.unwrap_or(Role::User))
        .bind(key_id)
        .fetch_one(&mut *tx.acquire().await?)
        .await?;

        for namespace in namespaces {
            sqlx::query(
                "
                INSERT INTO user_permissions (user, namespace, can_delete_ns)
                VALUES ($1, (SELECT id FROM namespaces WHERE name = $2), false)
            ",
            )
            .bind(user_id as i64)
            .bind(namespace)
            .execute(tx.acquire().await?)
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    /// Sends a single message to a queue.
    pub async fn sqs_send(
        &self,
        queue: u64,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, Error> {
        let mut tx = self.db().begin().await?;

        let res = self.sqs_send_internal(queue, req, &mut tx).await?;

        tx.commit().await?;

        Ok(res)
    }

    async fn sqs_send_internal(
        &self,
        queue: u64,
        req: SendMessageRequest,
        exec: impl Acquire<'_, Database = Sqlite>,
    ) -> Result<SendMessageResponse, Error> {
        let mut tx = exec.acquire().await?;

        let msg_id: u64 =
            sqlx::query_scalar("INSERT INTO messages (queue, body) VALUES ($1, $2) RETURNING id")
                .bind(queue as i64)
                .bind(&req.message_body)
                .fetch_one(&mut *tx)
                .await?;

        let mut attr_bytes_to_digest = Vec::new();
        for (k, v) in req.message_attributes.into_iter() {
            v.serialize_into(&k, &mut attr_bytes_to_digest);

            sqlx::query("INSERT INTO kv_pairs (message, k, v) VALUES ($1, $2, $3)")
                .bind(msg_id as i64)
                .bind(k)
                .bind(serde_json::to_vec(&v).map_err(Error::internal)?)
                .execute(&mut *tx)
                .await?;
        }

        let body_digest = hex::encode(md5::compute(&req.message_body).as_ref());
        let attr_digest = hex::encode(md5::compute(&attr_bytes_to_digest).as_ref());

        Ok(SendMessageResponse {
            message_id: msg_id,
            md5_of_message_body: body_digest,
            md5_of_message_attributes: attr_digest,
            // md5_of_message_system_attributes: hex::encode(md5::compute(b"").as_ref()),
        })
    }

    /// Sends multiple messages to a queue in one operation.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `messages` - Vector of (message body, attributes) pairs
    #[allow(unused)]
    pub async fn sqs_send_batch(
        &self,
        queue_name: &str,
        namespace_name: &str,
        req: SendMessageBatchRequest,
    ) -> Result<SendMessageBatchResponse, Error> {
        let mut tx = self.db().begin().await?;

        let queue_id = self
            .get_queue_id(namespace_name, queue_name, &mut *tx)
            .await?
            .ok_or_else(|| Error::queue_not_found(queue_name, namespace_name))?;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in req.entries {
            let message_attributes = entry.message_attributes;
            let message_body = entry.message_body;

            match self
                .sqs_send_internal(
                    queue_id,
                    SendMessageRequest {
                        queue_url: req.queue_url.clone(),
                        message_body,
                        delay_seconds: entry.delay_seconds,
                        message_attributes,
                        message_deduplication_id: entry.message_deduplication_id,
                        message_group_id: entry.message_group_id,
                    },
                    &mut *tx,
                )
                .await
            {
                Ok(res) => {
                    successful.push(SendMessageBatchResultEntry {
                        id: entry.id,
                        message_id: res.message_id.to_string(),
                        md5_of_message_body: res.md5_of_message_body,
                        // md5_of_message_attributes: res.md5_of_message_attributes,
                        // md5_of_message_system_attributes: res.md5_of_message_system_attributes,
                    });
                }
                Err(e) => {
                    failed.push(SendMessageBatchResultErrorEntry {
                        id: entry.id,
                        sender_fault: false,
                        code: e.status_code().to_string(),
                        message: Some(e.to_string()),
                    });
                }
            }
        }

        tx.commit().await?;

        Ok(SendMessageBatchResponse { successful, failed })
    }

    /// Receives a single message from a queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    #[allow(unused)]
    pub async fn sqs_recv(
        &self,
        namespace: impl AsRef<str>,
        queue: impl AsRef<str>,
        attribute_names: HashSet<String>,
    ) -> Result<Option<SqsMessage>, Error> {
        let mut tx = self.db().begin().await?;

        // Claim the first available message: visibility window elapsed (or never
        // received) and retries remaining. Stamps invisibility, bumps the
        // delivery counter, and mints a fresh receipt handle.
        let message: Option<Message> = sqlx::query_as(
            "
            WITH next_message AS (
                SELECT
                    m.id
                FROM messages m
                JOIN queues q ON m.queue = q.id
                JOIN queue_configurations conf ON q.id = conf.queue
                JOIN namespaces n ON q.ns = n.id
                WHERE n.name = $1
                AND q.name = $2
                AND (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                AND m.tries < conf.max_retries
                ORDER BY m.id ASC
                LIMIT 1
            )
            UPDATE messages
            SET delivered_at = unixepoch('now'),
                tries = tries + 1,
                invisible_until = unixepoch('now') + COALESCE(
                    (SELECT CAST(v AS INTEGER) FROM queue_attributes qa
                     WHERE qa.queue = messages.queue AND qa.k = 'visibility_timeout'),
                    $3
                ),
                receipt_handle = messages.id || ':' || lower(hex(randomblob(16)))
            WHERE id IN (SELECT id FROM next_message)
            RETURNING
                *,
                (SELECT q.name FROM queues q WHERE q.id = messages.queue) as queue,
                (CASE
                    WHEN messages.invisible_until IS NOT NULL AND messages.invisible_until > unixepoch('now') THEN 'delivered'
                    WHEN messages.tries >= (SELECT max_retries FROM queue_configurations WHERE queue = messages.queue) THEN 'failed'
                    ELSE 'pending'
                END) as status
            ",
        )
        .bind(namespace.as_ref())
        .bind(queue.as_ref())
        .bind(crate::config::defaults::VISIBILITY_TIMEOUT as i64)
        .fetch_optional(&mut *tx)
        .await?;

        let message = if let Some(message) = message {
            let mut kv = sqlx::query_as::<_, (String, Vec<u8>)>(
                "
                SELECT k, v FROM kv_pairs WHERE message = $1
                ",
            )
            .bind(message.id as i64)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();

            let mut message_attributes = HashMap::new();
            let mut attr_bytes_to_digest = Vec::new();
            for (k, v) in kv.into_iter().filter(|(k, _)| attribute_names.contains(k)) {
                let v: SqsMessageAttribute = serde_json::from_slice(&v).map_err(Error::internal)?;

                v.serialize_into(&k, &mut attr_bytes_to_digest);

                message_attributes.insert(k, v);
            }

            let sqs_message = SqsMessage {
                message_id: message.id.to_string(),

                receipt_handle: message.receipt_handle.clone().unwrap_or_default(),

                md5_of_body: hex::encode(md5::compute(&message.body).as_slice()),
                body: message.body,

                md5_of_message_attributes: hex::encode(
                    md5::compute(&attr_bytes_to_digest).as_ref(),
                ),
                message_attributes,
                // md5_of_system_attributes: hex::encode(md5::compute([]).as_ref()), // TODO
                attributes: HashMap::new(),
            };

            Some(sqs_message)
        } else {
            None
        };

        tx.commit().await?;

        Ok(message)
    }

    /// Receives multiple messages from a queue in one operation.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `max_messages` - Maximum number of messages to receive
    pub async fn sqs_recv_batch(
        &self,
        namespace: &str,
        queue: &str,
        max_messages: u64,
        visibility_timeout: Option<u64>,
        attribute_names: HashSet<String>,
    ) -> Result<Vec<SqsMessage>, Error> {
        let mut tx = self.db().begin().await?;

        // Atomically claim up to `max_messages` available messages: those whose
        // visibility window has elapsed (or were never received) and which still
        // have retries remaining. Claiming stamps `invisible_until`, bumps the
        // delivery counter, and mints a fresh receipt handle.
        //
        // The effective visibility timeout is the request override, else the
        // queue's `visibility_timeout` attribute, else the global default.
        let mut stream = sqlx::query_as::<_, Message>(
            "
            WITH next_messages AS (
                SELECT
                    m.id
                FROM messages m
                JOIN queues q ON m.queue = q.id
                JOIN queue_configurations conf ON q.id = conf.queue
                JOIN namespaces n ON q.ns = n.id
                WHERE n.name = $1
                AND q.name = $2
                AND (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now'))
                AND m.tries < conf.max_retries
                ORDER BY m.id ASC
                LIMIT $3
            )
            UPDATE messages
            SET delivered_at = unixepoch('now'),
                tries = tries + 1,
                invisible_until = unixepoch('now') + COALESCE(
                    $4,
                    (SELECT CAST(v AS INTEGER) FROM queue_attributes qa
                     WHERE qa.queue = messages.queue AND qa.k = 'visibility_timeout'),
                    $5
                ),
                receipt_handle = messages.id || ':' || lower(hex(randomblob(16)))
            WHERE id IN (SELECT id FROM next_messages)
            RETURNING
                *,
                (SELECT q.name FROM queues q WHERE q.id = messages.queue) as queue,
                (CASE
                    WHEN messages.invisible_until IS NOT NULL AND messages.invisible_until > unixepoch('now') THEN 'delivered'
                    WHEN messages.tries >= (SELECT max_retries FROM queue_configurations WHERE queue = messages.queue) THEN 'failed'
                    ELSE 'pending'
                END) as status
            ",
        )
        .bind(namespace)
        .bind(queue)
        .bind(max_messages as i64)
        .bind(visibility_timeout.map(|v| v as i64))
        .bind(crate::config::defaults::VISIBILITY_TIMEOUT as i64)
        .fetch(&mut *tx);
        // .await
        //     .map_err(|e| {
        //         tracing::error!("Failed to fetch messages {e}");
        //         e
        //     })
        //     ?
        // .into_iter()
        // .map(|message: Message| SqsMessage {
        //     message_id: message.id.to_string(),
        //     md5_of_body: hex::encode(md5::compute(&message.body).as_slice()),
        //     body: message.body,
        // })
        // .collect();

        let mut messages = vec![];
        while let Some(message) = stream.next().await.transpose()? {
            let kv = sqlx::query_as::<_, (String, Vec<u8>)>(
                "
                SELECT k, v FROM kv_pairs WHERE message = $1
                ",
            )
            .bind(message.id as i64)
            .fetch_all(self.db())
            .await?
            .into_iter()
            .collect::<BTreeMap<_, _>>();

            let mut message_attributes = HashMap::new();
            let mut attr_bytes_to_digest = Vec::new();
            for (k, v) in kv
                .into_iter()
                .filter(|(k, _)| attribute_names.contains(k))
                .sorted_by_key(|(k, _)| k.clone())
            {
                tracing::info!("Attribute {k}");
                let v: SqsMessageAttribute = serde_json::from_slice(&v).map_err(Error::internal)?;

                v.serialize_into(&k, &mut attr_bytes_to_digest);

                message_attributes.insert(k, v);
            }

            let sqs_message = SqsMessage {
                message_id: message.id.to_string(),

                receipt_handle: message.receipt_handle.clone().unwrap_or_default(),

                md5_of_body: hex::encode(md5::compute(&message.body.as_bytes()).as_slice()),
                body: message.body,

                md5_of_message_attributes: hex::encode(
                    md5::compute(&attr_bytes_to_digest).as_ref(),
                ),
                message_attributes,
                // md5_of_system_attributes: hex::encode(md5::compute([]).as_ref()), // TODO
                attributes: HashMap::new(),
            };
            messages.push(sqs_message);
        }

        drop(stream);

        tx.commit().await?;

        Ok(messages)
    }

    /// Lists all messages in a queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    pub async fn list_messages(
        &self,
        namespace: &str,
        queue: &str,
    ) -> Result<Vec<MessageDetails>, Error> {
        let mut db = self.db().acquire().await?;

        let mut messages = sqlx::query_as::<_, Message>(
            "
            SELECT
                m.*,
                q.name as queue,
                (CASE
                    WHEN m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 'delivered'
                    WHEN m.tries >= conf.max_retries THEN 'failed'
                    ELSE 'pending'
                END) as status
            FROM messages m
            JOIN queues q ON m.queue = q.id
            JOIN queue_configurations conf ON q.id = conf.queue
            WHERE q.ns = (SELECT id FROM namespaces WHERE name = $1) AND q.name = $2
        ",
        )
        .bind(namespace)
        .bind(queue)
        .fetch(&mut *db);

        let mut join_set = JoinSet::new();
        while let Some(message) = messages.next().await.transpose()? {
            let db = self.db().clone();
            join_set.spawn_local(async move {
                let mut conn = db.acquire().await?;
                // let mut kv_pairs = sqlx::query_as::<_, (String, Vec<u8>)>(
                //     "
                //     SELECT k, v FROM kv_pairs WHERE message = $1
                // ",
                // )
                // .bind(message.id as i64)
                // .fetch(&mut *conn);
                //
                // while let Some((k, v)) = kv_pairs.next().await.transpose()? {
                //     message
                //         .kv
                //         .insert(k, bincode::deserialize(&v).map_err(Error::internal)?);
                // }

                let mut message_attributes = HashMap::new();
                let mut kv = sqlx::query_as::<_, (String, Vec<u8>)>(
                    "
                    SELECT k, v FROM kv_pairs WHERE message = $1
                    ",
                )
                .bind(message.id as i64)
                .fetch(&mut *conn);

                while let Some((k, v)) = kv.next().await.transpose()? {
                    let attr = match serde_json::from_slice(&v) {
                        Ok(attr) => attr,
                        Err(e) => {
                            tracing::warn!(
                                attribute = k,
                                message = message.id,
                                "Failed to deserialize message attribute: {e}",
                            );

                            continue;
                        }
                    };
                    let value = match attr {
                        SqsMessageAttribute::String { string_value: s } => {
                            serde_json::Value::String(s)
                        }
                        SqsMessageAttribute::Number { string_value: s } => {
                            serde_json::Value::Number(s.parse().map_err(Error::internal)?)
                        }
                        SqsMessageAttribute::Binary { binary_value: b } => {
                            serde_json::Value::String(base64::prelude::BASE64_STANDARD.encode(b))
                        }
                    };
                    message_attributes.insert(k, value);
                }

                let sqs_message = MessageDetails {
                    id: message.id,
                    queue: message.queue,
                    status: message.status,
                    sent_by: message.sent_by,
                    delivered_at: message.delivered_at,
                    tries: message.tries,
                    body: message.body,

                    message_attributes,
                };

                Result::<_, Error>::Ok(sqs_message)
            });
        }

        let mut messages = Vec::new();

        while let Some(result) = join_set
            .join_next()
            .await
            .transpose()
            .map_err(Error::internal)?
            .transpose()?
        {
            messages.push(result);
        }

        Ok(messages)
    }

    /// Gets the configuration for a queue.
    ///
    /// # Arguments
    /// * `queue` - Queue ID
    pub async fn get_queue_configuration(&self, queue: u64) -> Result<QueueConfig, Error> {
        let mut db = self.db().acquire().await?;
        Ok(sqlx::query_as(
            "
            SELECT * FROM queue_configurations WHERE queue = $1
            ",
        )
        .bind(queue as i64)
        .fetch_one(&mut *db)
        .await?)
    }

    /// Updates the configuration for a queue.
    ///
    /// # Arguments
    /// * `queue` - Queue ID
    /// * `new_config` - New configuration settings
    pub async fn update_queue_configuration(
        &self,
        queue: u64,
        new_config: QueueConfig,
    ) -> Result<(), Error> {
        let mut db = self.db().acquire().await?;

        sqlx::query(
            "
            UPDATE queue_configurations
            SET max_retries = $1, dead_letter_queue = $2
            WHERE queue = $3
            ",
        )
        .bind(new_config.max_retries as i64)
        .bind(new_config.dead_letter_queue.map(|id| id as i64))
        .bind(queue as i64)
        .execute(&mut *db)
        .await?;

        Ok(())
    }

    /// Gets statistics for a specific queue.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    pub async fn queue_statistics(
        &self,
        identity: Identity,
        namespace: &str,
        queue: &str,
    ) -> Result<QueueStatistics, Error> {
        let mut db = self.db().acquire().await?;
        let email = identity.id()?;

        Ok(sqlx::query_as(
            "
            SELECT
                q.id,
                q.name,
                qu.email as created_by,
                n.name as ns,
                COUNT(m.id) AS message_count,
                IFNULL(AVG(LENGTH(m.body)), 0.0) as avg_size_bytes,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries < conf.max_retries THEN 1 END) as pending,
                COUNT(CASE WHEN m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 1 END) as delivered,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries >= conf.max_retries THEN 1 END) as failed
            FROM queues q
            JOIN queue_configurations conf ON q.id = conf.queue
            LEFT JOIN messages m ON q.id = m.queue
            JOIN user_permissions p ON p.namespace = q.ns
            JOIN namespaces n ON n.id = q.ns
            JOIN users u ON u.id = p.user
            JOIN users qu ON q.created_by = qu.id
            WHERE u.email = $1 AND n.name = $2 AND q.name = $3
        ",
        )
        .bind(email)
        .bind(namespace)
        .bind(queue)
        .fetch_one(&mut *db)
        .await?)
    }

    /// Gets statistics for all queues accessible to the user.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn global_queue_statistics(
        &self,
        identity: Identity,
    ) -> Result<HashMap<String, QueueStatistics>, Error> {
        let mut db = self.db().acquire().await?;
        let email = identity.id()?;

        let res = sqlx::query_as(
            "
            SELECT
                q.id,
                q.name,
                qu.email as created_by,
                n.name as ns,
                COUNT(m.id) AS message_count,
                IFNULL(AVG(LENGTH(m.body)), 0.0) as avg_size_bytes,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries < conf.max_retries THEN 1 END) as pending,
                COUNT(CASE WHEN m.invisible_until IS NOT NULL AND m.invisible_until > unixepoch('now') THEN 1 END) as delivered,
                COUNT(CASE WHEN (m.invisible_until IS NULL OR m.invisible_until <= unixepoch('now')) AND m.tries >= conf.max_retries THEN 1 END) as failed
            FROM queues q
            JOIN queue_configurations conf ON q.id = conf.queue
            LEFT JOIN messages m ON q.id = m.queue
            JOIN user_permissions p ON p.namespace = q.ns
            JOIN namespaces n ON n.id = q.ns
            JOIN users u ON u.id = p.user
            JOIN users qu ON q.created_by = qu.id
            WHERE u.email = $1
            GROUP BY q.id, q.name
        ",
        )
        .bind(email)
        .fetch_all(&mut *db)
        .await?
        .into_iter()
        .map(|row: QueueStatistics| (row.queue.name.clone(), row))
        .collect::<HashMap<_, _>>();

        Ok(res)
    }

    /// Deletes multiple messages from a queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `message_ids` - IDs of messages to delete
    /// * `identity` - Identity of the authenticated user
    ///
    /// # Returns
    /// Tuple of (successfully deleted IDs, failed deletions with errors)
    #[allow(unused)]
    pub async fn delete_message_batch(
        &self,
        namespace: &str,
        queue: &str,
        message_ids: Vec<u64>,
        identity: Identity,
    ) -> Result<
        (
            Vec<u64>,          // Successfully deleted message IDs
            Vec<(u64, Error)>, // Failed message IDs
        ),
        Error,
    > {
        let mut tx = self.db().begin().await?;
        // Verify namespace exists and user has access
        let namespace_id = self
            .get_namespace_id(namespace, &mut tx)
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;
        self.check_user_access(&identity, namespace_id, &mut tx)
            .await?;
        // Verify queue exists
        let queue_id = self
            .get_queue_id(namespace, queue, &mut tx)
            .await?
            .ok_or_else(|| Error::queue_not_found(queue, namespace))?;

        let mut success = Vec::new();
        let mut failure = Vec::new();

        for message_id in message_ids {
            match sqlx::query(
                "
                DELETE FROM messages
                WHERE id = $1 AND queue = $2
                ",
            )
            .bind(message_id as i64)
            .bind(queue_id as i64)
            .execute(&mut *tx)
            .await
            {
                Ok(res) => {
                    if res.rows_affected() == 0 {
                        failure.push((
                            message_id,
                            Error::not_found(format!("{message_id} in queue {queue}")),
                        ));
                    } else {
                        success.push(message_id);
                    }
                }
                Err(err) => failure.push((message_id, err.into())),
            };
        }

        Ok((success, failure))
    }

    /// Deletes a single message from a queue, acknowledging its receipt.
    ///
    /// The delete only succeeds if `receipt_handle` matches the handle issued on
    /// the message's most recent receive. A stale handle — e.g. from a message
    /// whose visibility timeout expired and which was redelivered to another
    /// consumer — matches nothing and is reported as not found.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `receipt_handle` - Receipt handle returned by the latest ReceiveMessage
    /// * `identity` - Identity of the authenticated user
    pub async fn delete_message(
        &self,
        namespace: &str,
        queue: &str,
        receipt_handle: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        // Verify namespace exists and user has access
        let namespace_id = self
            .get_namespace_id(namespace, &mut tx)
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;

        self.check_user_access(&identity, namespace_id, &mut tx)
            .await?;

        // Verify queue exists
        let queue_id = self
            .get_queue_id(namespace, queue, &mut tx)
            .await?
            .ok_or_else(|| Error::queue_not_found(queue, namespace))?;

        // Delete the in-flight message identified by this receipt handle.
        let result = sqlx::query(
            "
            DELETE FROM messages
            WHERE queue = $1 AND receipt_handle = $2
            ",
        )
        .bind(queue_id as i64)
        .bind(receipt_handle)
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::not_found(format!(
                "receipt handle invalid or expired in queue {queue}"
            )));
        }

        tx.commit().await?;

        Ok(())
    }

    /// Deletes all messages from a queue.
    ///
    /// # Arguments
    /// * `namespace` - Namespace containing the queue
    /// * `queue` - Queue name
    /// * `identity` - Identity of the authenticated user
    pub async fn purge_queue(
        &self,
        namespace: &str,
        queue: &str,
        identity: Identity,
    ) -> Result<(), Error> {
        let mut tx = self.db().begin().await?;

        // Verify namespace exists and user has access
        let namespace_id = self
            .get_namespace_id(namespace, &mut tx)
            .await?
            .ok_or_else(|| Error::namespace_not_found(namespace))?;

        self.check_user_access(&identity, namespace_id, &mut tx)
            .await?;

        // Verify queue exists
        let queue_id = self
            .get_queue_id(namespace, queue, &mut tx)
            .await?
            .ok_or_else(|| Error::queue_not_found(queue, namespace))?;

        // Delete all messages from the queue
        sqlx::query(
            "
            DELETE FROM messages
            WHERE queue = $1
            ",
        )
        .bind(queue_id as i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Gets statistics for all namespaces accessible to the user.
    ///
    /// # Arguments
    /// * `identity` - Identity of the authenticated user
    pub async fn list_namespace_statistics(
        &self,
        identity: Identity,
    ) -> Result<Vec<NamespaceStatistics>, Error> {
        let email = identity.id()?;

        Ok(sqlx::query_as(
            "
            SELECT
                ns.*,
                nu.email as created_by,
                COUNT(q.id) as queue_count
            FROM namespaces ns
            JOIN user_permissions p ON p.namespace = ns.id
            JOIN users u ON p.user = u.id
            JOIN users nu ON ns.created_by = nu.id
            LEFT JOIN queues q ON q.ns = ns.id
            WHERE u.email = $1
            GROUP BY ns.id, nu.email
        ",
        )
        .bind(email)
        .fetch_all(&mut *self.db().acquire().await?)
        .await?)
    }
}

#[cfg(test)]
mod visibility_tests {
    use super::*;
    use actix_identity::Identity;
    use std::collections::{HashMap, HashSet};

    /// Spins up a Service backed by a throwaway on-disk SQLite database (a real
    /// file is required so every pooled connection sees the same schema). The
    /// returned `TempDir` must be kept alive for the duration of the test.
    async fn setup() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();

        // Config has private fields but derives Deserialize; `Option` fields
        // absent from the JSON fall back to their `defaults` (root user becomes
        // admin@example.com / "password").
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "db_path": db_path,
            "default_max_retries": 5,
        }))
        .unwrap();

        let svc = Service::connect_with()
            .config(cfg)
            .kms_factory(|_| async move { Ok(InMemoryKeyManager::new()) })
            .call()
            .await
            .unwrap();

        (svc, dir)
    }

    fn admin() -> Identity {
        Identity::mock("admin@example.com".to_string())
    }

    fn send_req(body: &str) -> SendMessageRequest {
        SendMessageRequest {
            queue_url: "http://localhost:8080/api/sqs/ns/q".parse().unwrap(),
            message_body: body.to_string(),
            delay_seconds: None,
            message_attributes: HashMap::new(),
            message_deduplication_id: None,
            message_group_id: None,
        }
    }

    /// Pulls every in-flight message's visibility deadline into the past so the
    /// next receive treats them as expired — lets us assert re-availability
    /// without sleeping through a real timeout.
    async fn expire_inflight(svc: &Service) {
        sqlx::query("UPDATE messages SET invisible_until = unixepoch('now') - 1 WHERE invisible_until IS NOT NULL")
            .execute(svc.db())
            .await
            .unwrap();
    }

    async fn seed_queue_with_one_message(svc: &Service) -> u64 {
        svc.create_namespace("ns", admin()).await.unwrap();
        svc.create_queue("ns", "q", HashMap::new(), HashMap::new(), admin())
            .await
            .unwrap();
        let qid = svc.get_queue_id("ns", "q", svc.db()).await.unwrap().unwrap();
        svc.sqs_send(qid, send_req("hello")).await.unwrap();
        qid
    }

    #[tokio::test]
    async fn received_message_is_invisible_until_timeout() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(!first[0].receipt_handle.is_empty());

        // Still within the visibility window: must not be handed out again.
        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        assert!(second.is_empty(), "in-flight message should be invisible");
    }

    #[tokio::test]
    async fn message_becomes_available_again_after_timeout() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        let handle1 = first[0].receipt_handle.clone();

        expire_inflight(&svc).await;

        // Timeout elapsed without a delete: the message is available again and
        // gets a fresh receipt handle.
        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_ne!(
            second[0].receipt_handle, handle1,
            "redelivery should mint a new receipt handle"
        );
    }

    #[tokio::test]
    async fn delete_requires_current_receipt_handle() {
        let (svc, _dir) = setup().await;
        seed_queue_with_one_message(&svc).await;

        let first = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        let stale_handle = first[0].receipt_handle.clone();

        // Timeout expires and the message is redelivered to a new consumer.
        expire_inflight(&svc).await;
        let second = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        let current_handle = second[0].receipt_handle.clone();

        // The stale handle from the first receive must no longer delete anything.
        assert!(
            svc.delete_message("ns", "q", &stale_handle, admin())
                .await
                .is_err(),
            "stale receipt handle should not delete a redelivered message"
        );

        // The handle from the latest receive succeeds and removes the message.
        svc.delete_message("ns", "q", &current_handle, admin())
            .await
            .unwrap();

        expire_inflight(&svc).await;
        let after = svc
            .sqs_recv_batch("ns", "q", 10, Some(300), HashSet::new())
            .await
            .unwrap();
        assert!(after.is_empty(), "deleted message should be gone for good");
    }
}
