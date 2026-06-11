# Admin sessions

How the admin API's cookie sessions work, why they write to the database on
every request, and the trade-offs around changing that. SQS callers are
unaffected — they authenticate per-request with SigV4 and carry no session.

## Storage

Sessions live in the main SQLite database ([`src/auth/session.rs`](../../src/auth/session.rs)):

- `sessions` — one row per session (`session_key`, `ttl`), created by
  migration 0001; `session_state` rows hang off it with `ON DELETE CASCADE`.
- The cookie (`nervemq_session`) is **signed**, not encrypted; the signing
  key is generated on first run and persisted in `server_secrets`
  (`load_or_generate_session_key`), so restarts don't invalidate cookies.
- Neither table references `users` — deleting a user does **not** revoke
  their live sessions (they die at their TTL).

The session/identity machinery itself comes from two NerveMQ-specific
forks of the actix-extras crates (`nervemq-actix-session` /
`nervemq-actix-identity`) — see [actix-forks.md](actix-forks.md) for what
they change and why.

## The time limit

`SESSION_EXPIRATION` is **1 hour**, hard-coded in [`src/lib.rs`](../../src/lib.rs),
and feeds two separate mechanisms that happen to share the value:

1. **`PersistentSession::session_ttl`** — how long the `sessions` row and
   the cookie's `Max-Age` live. The default TTL-extension policy is
   `TtlExtensionPolicy::OnEveryRequest`, so this is a **sliding window**:
   every request pushes expiry out another hour.
2. **`IdentityMiddleware::visit_deadline`** — actix-identity logs the user
   out if more than an hour passed since their *last visit*.

Net behavior: an active dashboard stays logged in indefinitely; an idle one
is logged out after an hour.

## One database write per request

With `OnEveryRequest`, every authenticated response:

1. refreshes the session row's TTL — `SqliteSessionStore::update_ttl` runs
   `UPDATE sessions SET ttl = … WHERE session_key = …`, a real write; and
2. re-issues the session cookie with the new deadline. The cookie carries
   the `Secure` flag, which browsers honor on localhost but `requests`/curl
   do not — which is why every non-browser client of the admin API
   (`admin_request()` in the Python examples, cookie-jar `sed` in shell
   smoke tests) has to unflag it **after every response**, not just at
   login.

### Why not `TtlExtensionPolicy::OnStateChanges`?

On its own it changes almost nothing, because of a non-obvious interaction:
when `visit_deadline` is set, actix-identity **writes a fresh last-visited
timestamp into the session state on every request**. That is a state
change, so `OnStateChanges` still persists the session every request — via
a full state save, if anything heavier than `update_ttl`.

Actually reducing the writes means changing both — `OnStateChanges` *and*
dropping `visit_deadline` — which trades behavior, not just I/O:

| | Today (sliding) | `OnStateChanges`, no `visit_deadline` |
| --- | --- | --- |
| Active user | Never logged out | **Hard logout 1 h after login**, even mid-action |
| Idle user | Logged out after 1 h idle | Logged out 1 h after login |
| DB writes | ~1 per request | ~1 per login |
| Cookie | Re-issued every response | Issued once at login |
| Stolen-cookie exposure | Lives as long as it keeps being used | Absolute ≤ 1 h cap |

If this area is ever revisited, the more useful change is making
`SESSION_EXPIRATION` configurable, and the common production shape is
*both* deadlines: the existing sliding idle timeout plus a longer absolute
cap (`IdentityMiddleware::login_deadline`) — "idle logout after 1 h, forced
re-auth after 24 h" — without touching the TTL policy.

## Would a separate session database help?

Asked after the per-request session writes kept poisoning read-then-write
transactions in the batch send path (see the concurrency notes in
[message-lifecycle.md](message-lifecycle.md)). The answer: **it would have
masked that incident, but not fixed the bug.**

SQLite's write lock, WAL and snapshot semantics are all per database
*file*. The `SQLITE_BUSY_SNAPSHOT` failure needed a transaction that read
before its first write *and* any concurrent writer on the same database in
that window. During the incident the only other writer was the session TTL
update (one per dashboard poll) — in a separate file, those commits land
under a different write lock and the bulk send would have completed clean.

But the read-then-write transactions were still wrong, and any
*message-database* writer would have triggered the identical failure: two
producers batch-sending concurrently, a consumer deleting during a send, an
admin purging during traffic. Splitting the databases would have downgraded
a bug reproducible with one open browser tab into one that appears
intermittently under real load — strictly worse for diagnosis. The real
fix (write-first transactions) was needed regardless, and with it in place
the session writes are harmless.

On its own merits a split is defensible but low-value:

- **Pros**: isolates high-churn throwaway writes from data (smaller WAL,
  no checkpoint pressure from TTL updates, no competition for the main
  pool's slots or write lock); sessions could run with relaxed durability
  (`PRAGMA synchronous=NORMAL`/`OFF` — losing them on a crash just means
  re-login); data backups stop carrying session rows.
- **Cons**: a second pool, file and migration path; the signing key lives
  in the main database's `server_secrets` and would need a home; cross-
  database atomicity is lost (though the schema is already split-clean —
  no FK between sessions and users).

Current position: leave it. The remaining cost is one small `UPDATE` per
request in WAL mode; if write pressure ever becomes real, revisit the
TTL-policy/deadline configuration first.
