//! SQLite-based session storage implementation for Actix-web.
//!
//! This module provides a persistent session storage backend using SQLite. It implements
//! the `SessionStore` trait from actix-session and stores session data in two tables:
//! - sessions: Stores session metadata (id, key, expiry)
//! - session_state: Stores key-value pairs for each session
//!
//! Sessions live in their **own database file** (`sessions.db` by default,
//! see `Config::sessions_db_path`), separate from the message database:
//! SQLite's write lock, WAL and snapshot semantics are per file, so the
//! per-request session TTL writes never compete with message traffic for
//! the main database's write lock or pool slots, and crash-losing a few
//! sessions only costs a re-login. The schema is owned by this module and
//! bootstrapped on connect — it is deliberately not part of the main
//! database's migrations.
//!
//! The implementation supports all standard session operations including:
//! - Creating new sessions
//! - Loading existing sessions
//! - Updating session data
//! - Managing session TTL
//! - Deleting sessions

use std::str::FromStr;

use actix_session::storage::{LoadError, SaveError, SessionKey, UpdateError};
use rand::distributions::{Alphanumeric, DistString};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};
use tokio_stream::StreamExt;

pub use actix_session::storage::SessionStore;

pub type SessionState = serde_json::Map<String, serde_json::Value>;

/// Opens (creating if missing) the dedicated sessions database and
/// bootstraps its schema. Sessions are throwaway state, so the file runs
/// with the same relaxed-durability settings as the main database (WAL,
/// `synchronous=NORMAL`): losing the tail of the WAL on a power failure
/// logs those sessions out, nothing more.
pub async fn connect(path: &str) -> eyre::Result<SqlitePool> {
    let opts = SqliteConnectOptions::from_str(path)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal);

    let pool = SqlitePoolOptions::new().connect_with(opts).await?;
    ensure_schema(&pool).await?;

    Ok(pool)
}

/// Creates the session tables if they don't exist. Mirrors what migrations
/// 0001 + 0009 used to build in the main database, minus the index that
/// duplicated `session_state`'s primary key.
async fn ensure_schema(db: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        create table if not exists sessions (
            id integer not null,
            session_key text not null,
            expires_at integer not null default 0,

            primary key (id)
        )
        ",
    )
    .execute(db)
    .await?;
    sqlx::query("create unique index if not exists sessions_key_idx on sessions(session_key)")
        .execute(db)
        .await?;
    sqlx::query("create index if not exists sessions_expires_at_idx on sessions(expires_at)")
        .execute(db)
        .await?;
    sqlx::query(
        "
        create table if not exists session_state (
            session integer not null,
            k text not null,
            v text not null,

            primary key (session, k),
            foreign key (session) references sessions(id) on delete cascade
        )
        ",
    )
    .execute(db)
    .await?;

    Ok(())
}

/// SQLite-based implementation of the session store.
///
/// Provides persistent storage of session data using SQLite as the backend.
/// Each instance maintains a connection pool to the database.
#[derive(Clone)]
pub struct SqliteSessionStore {
    db: SqlitePool,
}

impl SqliteSessionStore {
    /// Creates a new SQLite session store over a pool opened with
    /// [`connect`] (which bootstraps the schema).
    pub fn new(db: SqlitePool) -> Self {
        Self { db }
    }

    /// A store over a throwaway in-memory database, for tests. Capped at
    /// one connection: each plain `sqlite::memory:` connection would
    /// otherwise get its own private database.
    #[cfg(test)]
    pub async fn in_memory() -> Self {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sessions database");
        ensure_schema(&db).await.expect("sessions schema");
        Self { db }
    }
}

/// Represents a session in the database.
///
/// Contains the session's unique identifier, key for lookup,
/// and the associated state data.
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Session {
    id: u64,
    session_key: String,

    #[sqlx(skip)]
    state: SessionState,
}

/// Represents a single key-value entry in a session's state.
///
/// Maps to the session_state table in SQLite, where each row
/// contains a reference to its parent session and a key-value pair.
#[derive(Serialize, Deserialize, sqlx::FromRow)]
struct SessionStateEntry {
    session: u64,
    k: String,
    v: serde_json::Value,
}

/// Implementation of the `SessionStore` trait for SQLite-based storage.
///
/// Provides all required session management operations including:
/// - Loading sessions by key
/// - Saving new sessions
/// - Updating existing sessions
/// - Managing session TTL
/// - Deleting sessions
impl SessionStore for SqliteSessionStore {
    /// Loads a session from the database by its key.
    ///
    /// Returns the session state if found, or None if the session doesn't exist.
    /// Also loads all associated key-value pairs from the session_state table.
    fn load(
        &self,
        session_key: &actix_session::storage::SessionKey,
    ) -> impl ::core::future::Future<Output = Result<Option<SessionState>, LoadError>> {
        let db = self.db.clone();
        Box::pin(async move {
            let session: Option<Session> =
                sqlx::query_as(
                    // Expired rows are dead even if the sweeper hasn't
                    // collected them yet.
                    "SELECT * from sessions WHERE session_key = $1 AND expires_at > unixepoch('now')",
                )
                    .bind(session_key.as_ref())
                    .fetch_optional(&db)
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to load session: {e}");
                        LoadError::Other(anyhow::Error::new(e))
                    })?;

            let session = match session {
                Some(mut session) => {
                    let mut kv = sqlx::query_as::<_, SessionStateEntry>(
                        "SELECT * FROM session_state WHERE session = $1",
                    )
                    .bind(session.id as i64)
                    .fetch(&db);

                    while let Some(pair) = kv.next().await.transpose().map_err(|e| {
                        tracing::warn!("Load error: {e}");
                        LoadError::Other(anyhow::Error::new(e))
                    })? {
                        session.state.insert(pair.k, pair.v);
                    }

                    session
                }
                None => {
                    return Ok(None);
                }
            };

            tracing::debug!("Loaded session: {}", session.id);

            Ok(Some(session.state))
        })
    }

    /// Saves a new session to the database.
    ///
    /// Creates a new session with a randomly generated key and stores all
    /// provided state data. Returns the new session key on success.
    fn save(
        &self,
        session_state: SessionState,
        ttl: &actix_web::cookie::time::Duration,
    ) -> impl ::core::future::Future<Output = Result<actix_session::storage::SessionKey, SaveError>>
    {
        let db = self.db.clone();
        Box::pin(async move {
            let mut tx = db
                .begin()
                .await
                .map_err(|e| SaveError::Other(anyhow::Error::new(e)))?;

            let key: SessionKey = Alphanumeric
                .sample_string(&mut rand::thread_rng(), 64)
                .try_into()
                .expect("generated string should be within the size range for a session key");

            let id: u64 = sqlx::query_scalar(
                "
                INSERT INTO sessions (session_key, expires_at)
                VALUES ($1, unixepoch('now') + $2)
                RETURNING id
                ",
            )
            .bind(key.as_ref())
            .bind(ttl.whole_seconds())
            .fetch_one(tx.as_mut())
            .await
            .map_err(|e| SaveError::Other(anyhow::Error::new(e)))?;

            for (k, v) in session_state.into_iter() {
                sqlx::query(
                    "
                    INSERT INTO session_state (session, k, v)
                    VALUES ($1, $2, $3)
                ",
                )
                .bind(id as i64)
                .bind(k)
                .bind(v)
                .execute(tx.as_mut())
                .await
                .map_err(|e| SaveError::Other(anyhow::Error::new(e)))?;
            }

            tx.commit()
                .await
                .map_err(|e| SaveError::Other(anyhow::Error::new(e)))?;

            Ok(key)
        })
    }

    /// Updates an existing session with new state data.
    ///
    /// Modifies both the session's TTL and its state data. Removes any
    /// key-value pairs that are no longer present in the new state.
    fn update(
        &self,
        session_key: actix_session::storage::SessionKey,
        session_state: SessionState,
        ttl: &actix_web::cookie::time::Duration,
    ) -> impl ::core::future::Future<Output = Result<actix_session::storage::SessionKey, UpdateError>>
    {
        let db = self.db.clone();
        Box::pin(async move {
            let mut tx = db
                .begin()
                .await
                .map_err(|e| UpdateError::Other(anyhow::Error::new(e)))?;

            let ttl_query = "
                UPDATE sessions
                SET expires_at = unixepoch('now') + $1
                WHERE session_key = $2
                RETURNING id
            ";

            let session_id: u64 = sqlx::query_scalar(ttl_query)
                .bind(ttl.whole_seconds())
                .bind(session_key.as_ref())
                .fetch_one(tx.as_mut())
                .await
                .map_err(|e| UpdateError::Other(anyhow::Error::new(e)))?;

            let keys =
                session_state
                    .keys()
                    .map(|k| format!("'{k}'"))
                    .fold(String::new(), |s, k| {
                        if s.len() == 0 {
                            return k;
                        }
                        format!("{s}, {k}")
                    });

            sqlx::query(&format!(
                "
                DELETE FROM session_state
                WHERE session = $1 AND k NOT IN ({keys})
            ",
            ))
            .bind(session_id as i64)
            .execute(tx.as_mut())
            .await
            .map_err(|e| UpdateError::Other(anyhow::Error::new(e)))?;

            for (k, v) in session_state.iter() {
                sqlx::query(
                    "
                        INSERT OR REPLACE INTO session_state (session, k, v)
                        VALUES ($1, $2, $3)
                    ",
                )
                .bind(session_id as i64)
                .bind(k)
                .bind(v)
                .execute(tx.as_mut())
                .await
                .map_err(|e| UpdateError::Other(anyhow::Error::new(e)))?;
            }

            tx.commit()
                .await
                .map_err(|e| UpdateError::Other(anyhow::Error::new(e)))?;

            Ok(session_key)
        })
    }

    /// Updates only the TTL (time-to-live) of an existing session.
    ///
    /// This is used to extend or shorten a session's lifetime without
    /// modifying its state data.
    fn update_ttl(
        &self,
        session_key: &actix_session::storage::SessionKey,
        ttl: &actix_web::cookie::time::Duration,
    ) -> impl ::core::future::Future<Output = Result<(), anyhow::Error>> {
        let db = self.db.clone();

        Box::pin(async move {
            let query = "
                UPDATE sessions
                SET expires_at = unixepoch('now') + $1
                WHERE session_key = $2
            ";
            let mut db = db.acquire().await.map_err(|e| anyhow::Error::new(e))?;

            sqlx::query(query)
                .bind(ttl.whole_seconds())
                .bind(session_key.as_ref())
                .execute(db.as_mut())
                .await
                .map_err(|e| anyhow::Error::new(e))?;

            Ok(())
        })
    }

    /// Deletes a session and all its associated state data.
    ///
    /// Removes the session and relies on foreign key cascading to
    /// clean up related session_state entries.
    fn delete(
        &self,
        session_key: &actix_session::storage::SessionKey,
    ) -> impl ::core::future::Future<Output = Result<(), anyhow::Error>> {
        let db = self.db.clone();
        Box::pin(async move {
            let mut db = db
                .acquire()
                .await
                .map_err(|e| LoadError::Other(anyhow::Error::new(e)))?;

            sqlx::query("DELETE FROM sessions WHERE session_key = $1")
                .bind(session_key.as_ref())
                .execute(db.as_mut())
                .await
                .map_err(|e| LoadError::Other(anyhow::Error::new(e)))?;

            Ok(())
        })
    }
}

/// Loads the session cookie signing key from the database, generating and
/// persisting one on first run.
///
/// Reusing the same key across restarts keeps existing session cookies valid;
/// a freshly generated key would fail the cookie's cryptographic checks and
/// silently log out every user on each restart.
pub async fn load_or_generate_session_key(
    db: &SqlitePool,
) -> eyre::Result<actix_web::cookie::Key> {
    let candidate = actix_web::cookie::Key::generate();

    // Insert-if-absent, then read back, so concurrent first runs agree on one key.
    sqlx::query(
        "INSERT INTO server_secrets (name, value) VALUES ('session_key', $1)
         ON CONFLICT (name) DO NOTHING",
    )
    .bind(candidate.master())
    .execute(db)
    .await?;

    let master: Vec<u8> =
        sqlx::query_scalar("SELECT value FROM server_secrets WHERE name = 'session_key'")
            .fetch_one(db)
            .await?;

    Ok(actix_web::cookie::Key::from(&master))
}

/// How often the session sweeper collects expired rows.
const SESSION_GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// Spawns a background task that periodically deletes expired sessions
/// (their `session_state` rows cascade). Expired sessions are already
/// rejected at load time; this bounds table growth. The first tick runs
/// immediately, cleaning anything left over from before the process
/// started.
pub fn spawn_session_gc(db: SqlitePool) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SESSION_GC_INTERVAL);
        loop {
            interval.tick().await;
            match sqlx::query("DELETE FROM sessions WHERE expires_at <= unixepoch('now')")
                .execute(&db)
                .await
            {
                Ok(res) if res.rows_affected() > 0 => {
                    tracing::info!(swept = res.rows_affected(), "Expired sessions collected");
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("Session GC sweep failed: {e}"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::cookie::time::Duration;

    /// The store over the real schema, exactly as `connect` bootstraps it.
    async fn setup_store() -> SqliteSessionStore {
        SqliteSessionStore::in_memory().await
    }

    fn create_test_state() -> SessionState {
        let mut state = SessionState::new();
        state.insert(
            "user_id".to_string(),
            serde_json::Value::String("123".to_string()),
        );
        state.insert(
            "username".to_string(),
            serde_json::Value::String("test_user".to_string()),
        );
        state
    }

    #[tokio::test]
    async fn test_save_and_load_session() {
        let store = setup_store().await;
        let state = create_test_state();
        let ttl = Duration::minutes(30);

        // Save session
        let session_key = store.save(state.clone(), &ttl).await.unwrap();

        // Load and verify session
        let loaded_state = store.load(&session_key).await.unwrap().unwrap();
        assert_eq!(
            loaded_state.get("user_id").unwrap().as_str().unwrap(),
            "123"
        );
        assert_eq!(
            loaded_state.get("username").unwrap().as_str().unwrap(),
            "test_user"
        );
    }

    #[tokio::test]
    async fn test_update_session() {
        let store = setup_store().await;
        let initial_state = create_test_state();
        let ttl = Duration::minutes(30);

        // Create initial session
        let session_key = store.save(initial_state, &ttl).await.unwrap();

        // Update session with new state
        let mut new_state = SessionState::new();
        new_state.insert(
            "user_id".to_string(),
            serde_json::Value::String("456".to_string()),
        );

        store
            .update(session_key.clone(), new_state, &ttl)
            .await
            .unwrap();

        // Verify updated state
        let loaded_state = store.load(&session_key).await.unwrap().unwrap();
        assert_eq!(
            loaded_state.get("user_id").unwrap().as_str().unwrap(),
            "456"
        );
        assert!(loaded_state.get("username").is_none());
    }

    #[tokio::test]
    async fn test_delete_session() {
        let store = setup_store().await;
        let state = create_test_state();
        let ttl = Duration::minutes(30);

        // Create session
        let session_key = store.save(state, &ttl).await.unwrap();

        // Delete session
        store.delete(&session_key).await.unwrap();

        // Verify session is deleted
        let loaded_state = store.load(&session_key).await.unwrap();
        assert!(loaded_state.is_none());
    }

    #[tokio::test]
    async fn test_update_ttl() {
        let store = setup_store().await;
        let db = store.db.clone();
        let state = create_test_state();
        let initial_ttl = Duration::minutes(30);

        // Create session
        let session_key = store.save(state, &initial_ttl).await.unwrap();

        // Update TTL
        let new_ttl = Duration::minutes(60);
        store.update_ttl(&session_key, &new_ttl).await.unwrap();

        // Verify the expiry moved out to about now + new_ttl.
        let expires_at: i64 =
            sqlx::query_scalar("SELECT expires_at FROM sessions WHERE session_key = ?")
                .bind(session_key.as_ref())
                .fetch_one(&db)
                .await
                .unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let remaining = expires_at - now;
        assert!(
            (remaining - new_ttl.whole_seconds()).abs() <= 2,
            "expiry should be ~{}s out, got {remaining}s",
            new_ttl.whole_seconds()
        );
    }

    #[tokio::test]
    async fn expired_session_is_not_loaded_and_gets_swept() {
        let store = setup_store().await;
        let db = store.db.clone();
        let state = create_test_state();

        let live = store
            .save(state.clone(), &Duration::minutes(30))
            .await
            .unwrap();
        let expired = store.save(state, &Duration::seconds(0)).await.unwrap();

        assert!(store.load(&live).await.unwrap().is_some());
        assert!(
            store.load(&expired).await.unwrap().is_none(),
            "expired session must not load even before the sweeper runs"
        );

        // The sweeper's DELETE collects only the expired row.
        sqlx::query("DELETE FROM sessions WHERE expires_at <= unixepoch('now')")
            .execute(&db)
            .await
            .unwrap();
        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sessions")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(remaining, 1, "the live session must survive the sweep");
    }

    /// The production path: `connect` creates the database file, bootstraps
    /// the schema (idempotently) and the store works over it.
    #[tokio::test]
    async fn connect_creates_and_bootstraps_the_sessions_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.db");
        let path = path.to_str().unwrap();

        let pool = connect(path).await.unwrap();
        let store = SqliteSessionStore::new(pool);

        let key = store
            .save(create_test_state(), &Duration::minutes(30))
            .await
            .unwrap();
        assert!(store.load(&key).await.unwrap().is_some());

        // Reconnecting to the existing file must not fail or lose sessions.
        let store = SqliteSessionStore::new(connect(path).await.unwrap());
        assert!(store.load(&key).await.unwrap().is_some());
    }
}
