# The `nervemq-actix-*` forks

NerveMQ's cookie-session machinery is built on **two coupled forks** of the
[actix-extras](https://github.com/actix/actix-extras) auth crates, declared
in [`Cargo.toml`](../../Cargo.toml) via `package =` renames so the code
still imports them as `actix_session` / `actix_identity`:

| Crates.io name | Version | Forked from | Depends on |
| --- | --- | --- | --- |
| `nervemq-actix-session` | 0.10.2 | `actix-session` 0.10.1 | — |
| `nervemq-actix-identity` | 0.8.1 | `actix-identity` 0.8.0 | the session fork |

Both were published on **2024-12-09 by `willothy`** (the original NerveMQ
author at fortress-build, this repository's upstream). There is no public
fork repository: the published crates' metadata still points at
`actix/actix-extras`, the changelogs carry no fork entries, and the listed
authors are upstream's. Everything below comes from diffing the published
crates against their upstream versions (~75 changed lines in session, ~40
in identity).

## What the forks change

### 1. Typed, public session state (the substantive change)

Upstream `actix-session` stores session state as `HashMap<String, String>`
— every value is a *stringified* JSON blob — and keeps the `SessionState`
alias `pub(crate)`. The fork changes it to:

```rust
pub type SessionState = serde_json::Map<String, Value>;
```

Consequences:

- **Custom stores become first-class.** NerveMQ implements its own
  [`SqliteSessionStore`](../../src/auth/session.rs) against the
  `SessionStore` trait; with the fork, the `sessions`/`session_state`
  tables hold real JSON values instead of double-encoded strings.
- `Session::get_value(key) -> Option<serde_json::Value>` — a raw-value
  accessor alongside the typed `get::<T>()`.
- The identity fork gains `InvalidIdTypeError`: now that a session value
  can be any JSON type, a non-string user id is a detectable error rather
  than an opaque parse failure.

### 2. Mock constructors (what NerveMQ leans on)

- `Session::mock(state, status)` — a detached, in-memory session with a
  chosen status, never touched by `SessionMiddleware`.
- `Identity::mock(id)` — a detached `Identity` over an `Unchanged` mock
  session: `.id()` resolves to the given string and **nothing is ever
  persisted**.

These were added "useful for testing" — every service-level test in this
repository constructs callers with `Identity::mock` — but they are also a
*production* primitive: the sessionless authentication path for API-key /
SigV4 callers (PR #45) uses `Identity::mock` to hand handlers a working
`Identity` without creating a session row per request. See
[sessions.md](sessions.md) for why that matters.

## Caveats

- **Provenance is opaque.** The fork diff is only discoverable by
  downloading both crates and diffing them (as this document was written).
  If the forks ever change, re-derive this document the same way.
- **Version pinning.** NerveMQ is pinned to the actix-session 0.10 /
  actix-identity 0.8 lineage, and only the original publisher can push new
  versions of these crate names. When upstream moves (actix-session 0.11+,
  or an actix-web major), someone must re-apply the fork's patches by hand
  — or take one of the exits below.
- **Production coupling.** Since PR #45, `Identity::mock` is load-bearing
  in the request path, not just in tests. Any migration off the forks needs
  an equivalent "detached identity" constructor.

## Exit strategies, if maintenance ever becomes a problem

1. **Vendor the two crates** into the repository (they are small) — full
   control, no crates.io dependency on a third-party publisher.
2. **Republish under project-owned names** to regain publishing rights.
3. **Upstream the patches.** Both changes are reasonable upstream PRs: the
   public typed `SessionState` solves real custom-store ergonomics, and
   actix-extras has long-standing requests for testability hooks like the
   mock constructors. If upstream accepted them, the forks could be
   dropped entirely.
