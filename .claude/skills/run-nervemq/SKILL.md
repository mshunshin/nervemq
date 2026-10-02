---
name: run-nervemq
description: Build, run and drive NerveMQ (Rust SQS-compatible queue server with an embedded React Router admin UI). Use when asked to start or run the server, build it, screenshot or click through the admin UI, send/receive messages over SQS against a live server, check a change in the real app, compare against origin/main, or run its tests.
---

NerveMQ is one binary: the Rust server embeds the admin UI (React Router,
built by Vite into `out/`) at compile time. Drive it with `.claude/skills/run-nervemq/driver.mjs`:
`up` starts a seeded server on a throwaway data directory, `run` executes a
line-based script mixing browser (admin UI), admin-API and SQS steps in one
session, and `down` stops it.

Paths are relative to the repo root. Verified on macOS arm64 (cargo 1.98,
bun 1.3.9, node 26, just 1.43); not yet run on Linux.

## Setup (once)

```bash
npm install --prefix .claude/skills/run-nervemq
(cd .claude/skills/run-nervemq && npx playwright install chromium-headless-shell)
```

The second line downloads the headless Chromium matching the installed
Playwright (~95 MB). Without it the driver falls back to the newest cached
`chromium_headless_shell-*` and says so.

## Build

```bash
just build
```

Runs `bun install --frozen-lockfile`, `bun run build` (type-check, then Vite
builds the UI into `out/`), then `cargo build` (embeds `out/`) →
`target/debug/nervemq`. About 5 s incremental; a fresh checkout takes about
1.5 min, nearly all of it Rust.

## Run (agent path)

```bash
PORT=8090 node .claude/skills/run-nervemq/driver.mjs up
```

The default port is 8080; set `PORT` when it's taken. `up` creates
namespace `demo`, queue `jobs` and an API key `DRIVERKEY` / `driver-secret`,
and logs in as `admin@example.com` / `driver-password`. Everything goes to
`$TMPDIR/nervemq-run/` (`state.json`, `server.log`, `data/`, `shots/`), and
`run` and `down` find the server from `state.json`.

Smoke test:

```bash
printf 'login\nnav /queues\nwait Running\nshot queues\n' | node .claude/skills/run-nervemq/driver.mjs run
```

A full flow: a consumer holds messages, the UI pauses the queue, the API
resumes it.

```bash
node .claude/skills/run-nervemq/driver.mjs run <<'EOF'
send job-1
send job-2
send job-3
receive 2
login
nav /queues
wait Running
shot queues
nav /queues/demo/jobs
wait Message Size (avg)
click Pause
wait Pause Queue
shot pause-dialog
click Pause Queue
wait 2 messages are still in flight
shot paused
receive 10
assert-received 0
delete-held
wait No messages are in flight
api GET /queue/demo/jobs
api POST /queue/demo/jobs/resume
receive 10
assert-received 1
errors
EOF
```

Then stop it:

```bash
node .claude/skills/run-nervemq/driver.mjs down
```

**Look at the screenshots** (Read the PNGs printed by `shot`). On a failing
step the driver prints `FAILED: …`, saves `shots/failure.png` and exits 1.

| command | what it does |
|---|---|
| `login` | Log in as the root admin via the login form. |
| `nav <path>` | Hard-load `<base><path>`, wait for network idle. |
| `wait <text>` / `gone <text>` | Wait (15 s) for text to appear / disappear. |
| `click <name>` | Click the button with exactly that accessible name, else exact text. |
| `pick <combobox> <option>` | Choose from a dropdown: `pick Namespace other` on the queue list. |
| `fill <selector> <value>` / `press <key>` | Type into an input / press a key. |
| `shot <name>` | Screenshot to `shots/<name>.png` and print the path. |
| `eval <js>` | Evaluate in the page and print JSON. |
| `api <METHOD> <path> [json]` | Admin API call (`/api/admin<path>`) with the browser's session; `login` first. |
| `queue <ns> <name>` | Switch the SQS queue (default `demo/jobs`). |
| `send <body>` | SQS SendMessage. |
| `receive [max] [visibility] [wait]` | SQS ReceiveMessage (defaults 10, 600 s, 0 s); holds what it gets. |
| `assert-received <n>` | Fail unless the last receive returned n messages. |
| `delete-held` / `release-held` | Delete held messages / set their visibility to 0. |
| `errors` / `clear-errors` | Print / clear console errors **and** uncaught page errors. |
| `sleep <ms>` | Pause. |

Env: `PORT` (8080), `VIEWPORT` (`1280x900`), `NERVEMQ_BIN`
(`target/debug/nervemq`), `NERVEMQ_RUN_DIR` (`$TMPDIR/nervemq-run`).

### Compare against origin/main

When the page shows an error or a layout problem, check whether main has it
too before blaming a change:

```bash
git fetch -q origin && git worktree add --detach "$TMPDIR/nervemq-main" origin/main && cd "$TMPDIR/nervemq-main" && just build
```

```bash
export NERVEMQ_BIN="$TMPDIR/nervemq-main/target/debug/nervemq" PORT=8091 NERVEMQ_RUN_DIR="$TMPDIR/nervemq-run-main" && node .claude/skills/run-nervemq/driver.mjs up && printf 'login\nnav /queues/demo/jobs\nwait Message Size (avg)\nshot main-queue\nerrors\n' | node .claude/skills/run-nervemq/driver.mjs run && node .claude/skills/run-nervemq/driver.mjs down && git worktree remove --force "$TMPDIR/nervemq-main"
```

## Run (human path)

```bash
bun run dev
```

Vite's dev server on :3000 with hot reload. It forwards `/api/admin` to a
server on :8080 (`just run`, or `driver.mjs up`), so log in at
http://localhost:3000/login. SQS clients still talk to :8080 directly.

## Test

```bash
cargo test                                        # 1–2 min: 282 unit/endpoint tests + smoke test of the real binary
cargo test --lib -- paused_queue long_poll_on_a_paused   # filters are substrings of test names
bun run test                                      # UI unit tests (lib/)
npx eslint . && npx tsc --noEmit -p .            # one warning already on main (data-table.tsx)
cargo clippy --tests                              # 42 warnings already on main
```

Tests that call code directly, without starting the app, each build a
`Service` on a temporary SQLite file: signed SQS requests in
`src/sqs/endpoint_tests.rs`, the admin API with a session cookie in
`src/api/endpoint_tests.rs`, and service-level visibility tests in
`src/service.rs`.

## Gotchas

- **:8080 may be the user's own `just run`.** Don't kill it; use
  `PORT=8090`. `up` refuses to start when something already answers.
- **The UI is compiled into the binary.** A UI edit shows up only after
  `bun run build` *and* `cargo build`. `just build` does both, in that
  order.
- **`errors` reports uncaught page errors as well as console messages.**
  React reports render failures as page errors, which a console-only listener
  misses. A signed-out visit logs a few `401 (Unauthorized)` console messages
  (the session check and the page's queries) before the redirect to
  `/login`: expected. Signed in, expect none.
- **The queue page is wider than the window** (1470 px of content at a
  1280 px window). Clicking a button on the right scrolls the page sideways,
  so screenshots show the content under the sidebar. `shot` resets the
  horizontal scroll; use `VIEWPORT=1600x1000` to fit the whole page.
- **Dialogs fade in.** A screenshot taken straight after `click` shows a
  half-transparent dialog. `shot` waits 400 ms first.
- **Wait for text, not the load event.** Pages render after the app's
  script loads and fetch their data afterwards, so `wait` for something on
  the page (`Message Size (avg)` on a queue page). The queue page refreshes
  its numbers every 30 s (every 5 s while paused): `nav` again instead of
  waiting for a number to change.
- **The driver's server only answers to localhost names.** `up` sets
  `NERVEMQ_HOST` to its URL, which turns on the host restriction: a request
  for another name (a different `Host` header, a `/etc/hosts` alias) gets
  `421`. Use `localhost` or `127.0.0.1`.
- **`eval` bypasses the Content-Security-Policy.** It runs through DevTools,
  which CSP deliberately doesn't restrict, so code it runs directly
  (`eval("…")` included) proves nothing about the CSP. To test it, inject
  markup (`<img src=x onerror=…>`, a `<script>` element) and check whether the
  page ran it: blocked attempts appear in `errors`.
- **`click` is exact.** `click Pause` and `click Pause Queue` are different
  buttons.
- **Dropdowns aren't buttons.** The queue list's namespace picker (and any
  other Radix `Select`) is a combobox, so `click` can't open it, and arrow
  keys pressed while it opens are dropped. Use `pick`.
- **Worktrees need their own `node_modules`.** `just build` in the worktree
  installs them (`bun install --frozen-lockfile`, ~2 s from cache); don't
  symlink this checkout's (it broke the Next.js build that predates the move
  to React Router).
- **macOS has no `timeout`.** For your own wait-for-port loops, use
  `for i in $(seq 1 60); do curl -sf … && break; sleep 0.5; done`. The
  driver polls by itself.

## Troubleshooting

- **`error: something already answers on http://localhost:8080`**: another
  server holds the port. Set `PORT`, or `driver.mjs down` if it's yours.
- **`(using cached chromium_headless_shell-NNNN; …)`**: Playwright's own
  browser build isn't installed. Harmless; the install line under Setup
  removes the message.
- **`FAILED: TimeoutError: locator.waitFor: Timeout 15000ms exceeded`**: the
  text never appeared. Read `shots/failure.png` and check `server.log`.
- **`command not found: timeout`**: see the macOS gotcha above.
