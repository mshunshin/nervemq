-- Sessions moved to their own database file (`sessions.db`, see
-- `Config::sessions_db_path`): SQLite's write lock, WAL and snapshot
-- semantics are per file, so per-request session TTL writes no longer
-- compete with message traffic for the main database's write lock or pool
-- slots. The schema is now owned and bootstrapped by the session store
-- (`auth::session::connect`), not these migrations. Dropping the old
-- tables logs out all admin sessions once; SQS clients are unaffected.
drop table if exists session_state;
drop index if exists sessions_expires_at_idx;
drop index if exists sessions_key_idx;
drop table if exists sessions;
