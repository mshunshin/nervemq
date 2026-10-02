# Web security

How the admin UI and its API resist attacks that run through a victim's
browser. The SQS API is mostly outside this: its requests are signed with an
API key's secret, which a web page doesn't have.

## The session cookie

Logging in (`POST /api/admin/auth/login`) creates a row in `sessions.db` and
sets `nervemq_session`, a signed session key (see [sessions.md](sessions.md)):

| Attribute | Effect |
| --- | --- |
| `HttpOnly` | Page scripts can't read it |
| `Secure` | Sent and kept only over HTTPS, except on `localhost`; beyond localhost the server needs TLS in front of it |
| `SameSite=Lax` | Sent on every same-site request; from other sites only on a top-level `GET` navigation |
| 1 hour, sliding | Idle sessions end; active ones don't |

A *site* is the scheme plus the registered domain: **ports and subdomains
don't count**. `localhost:9999` and `localhost:8080` are the same site, and so
are `wiki.example.com` and `mq.example.com`. So `SameSite=Lax` stops other
sites, but not other apps on the same host or domain. The layers below close
that gap.

## The layers

| Layer | What it stops | Where |
| --- | --- | --- |
| **CORS without credentials.** Any origin may call the API, answered with `Access-Control-Allow-Origin: *`, never `Allow-Credentials`. | Another origin reading responses made with the cookie, or sending JSON, `DELETE` and other preflighted requests with it | `cors()` in [`src/lib.rs`](../../src/lib.rs) |
| **`Origin` check.** A `POST`/`PUT`/`PATCH`/`DELETE` with no `Authorization` header (so it can only be using the cookie) whose `Origin` isn't this server gets a 403. | A same-site page *sending* a "simple" request, which needs no preflight: a form, plain text or no body (pausing a queue, disabling a user) | [`same_origin.rs`](../../src/auth/middleware/same_origin.rs) |
| **JSON must say so.** Admin-API JSON bodies must be `application/json` (400 otherwise). | Smuggling a JSON body in as `text/plain`, which a simple request may carry | `JsonConfig` on the `/admin` scope in `src/lib.rs` |
| **Content-Security-Policy** and companions on every response, errors included. | Injected markup running script: no inline `<script>`, no `onerror=`, no `eval`. Also framing (clickjacking), content-type guessing, and URLs leaking as referrers | `security_headers()` in `src/lib.rs` |
| **Host restriction,** only when `NERVEMQ_HOST` is set. | DNS rebinding (below) | [`host.rs`](../../src/auth/middleware/host.rs) |

Not affected, by design:
- **API-key and SigV4 callers,** from any origin. Their credentials are in a
  header, which authenticates the request on its own.
- **Clients that send no `Origin`** (curl, SDKs, scripts). They aren't a
  browser being tricked.

The CSP allows inline *styles* (`'unsafe-inline'` in `style-src`): the
dialogs' scroll lock and the toasts insert `<style>` elements, and injected
CSS can't run code. Scripts must be files from this server, which is why the
first-paint theme script lives in `public/theme.js`, not inline in
`index.html`.

## Who can do what

| Attacker | Gets |
| --- | --- |
| A page on another site | Nothing: `SameSite=Lax` keeps the cookie off its requests |
| A page on the same site (another port, a sibling subdomain) | Nothing: reads hidden by CORS, preflighted writes blocked, simple writes refused by the `Origin` check |
| Markup injected into the UI (an XSS bug) | No script runs (CSP) |
| A holder of an API key | What the key allows, from anywhere: keys are passwords |
| Someone on the network | The cookie only if there's no TLS (`Secure` keeps it off plain HTTP, except on localhost) |
| A DNS-rebinding page | See below |

## DNS rebinding

A browser decides what an origin is from the **name in the URL**, never from
the IP address that name resolves to.

1. The victim visits `http://evil.test:8080/`. The attacker's DNS points
   `evil.test` at the attacker's server, which sends the page.
2. The attacker repoints `evil.test` at the victim's NerveMQ: `127.0.0.1`, or
   an address on the LAN.
3. The page calls `fetch("/api/admin/auth/login", …)`. To the browser that's
   the page's own origin, so CORS doesn't apply, and the request reaches
   NerveMQ with `Host: evil.test:8080` and `Origin: http://evil.test:8080`.
   The `Origin` check passes: both headers name the attacker's domain, and the
   check only compares them with each other.

**What the attacker gets:** no session, but a way to reach unauthenticated
routes from inside the victim's network.
- The victim's real session is out of reach: cookies belong to a host name,
  and theirs belongs to the real one.
- The page can't keep a session of its own: a `Secure` cookie isn't kept over
  plain HTTP. Over HTTPS the attack fails sooner, because the certificate is
  for the real name.
- What's left is the routes that need no session, chiefly login. The page can
  try passwords against a server only the victim can reach, and read whether
  they worked. A fresh install without `NERVEMQ_ROOT_PASSWORD` has the login
  `admin@example.com` / `password`.

**`NERVEMQ_HOST` decides the response:**
- **Set:** the UI and admin API answer only requests for that name, plus the
  loopback names `localhost`, `127.0.0.1` and `[::1]`. A rebinding page's
  `Host` is always its own domain, never a loopback name, so it gets
  `421 Misdirected Request` for everything, login included. A reverse proxy
  in front must pass the original `Host` on (nginx:
  `proxy_set_header Host $host;`).
- **Unset** (the default): every name is answered. Reaching the server under
  whatever name points at it (an IP address, a LAN hostname, a tunnel) is
  often more useful than the protection, so the restriction is opt-in. The
  login routes stay reachable by rebinding, as described above.

The SQS API is exempt either way. A rebinding page has no key to sign with,
and SQS clients may well use another name for the server, such as an internal
service name.

**Recommendations:**
- Set `NERVEMQ_HOST` whenever the server has a fixed name.
- Always set `NERVEMQ_ROOT_PASSWORD`.
- Keep the default loopback `NERVEMQ_BIND_ADDRESS` unless the server must be
  reachable from elsewhere.

## Rules for new code

- **A `GET` route must not change anything.** `SameSite=Lax` sends the cookie
  on a cross-site top-level `GET`, and the CORS and `Origin` layers only guard
  writes. All 15 admin-API `GET` routes only read (checked October 2026).
- **Admin-API bodies are read with `web::Json`,** so the `application/json`
  rule applies. The UI's `adminFetch` labels every body it sends.
- **No inline scripts or event handlers in the UI.** The CSP blocks them; put
  code in modules or files under `public/`.
- **Browser-facing routes get the host restriction automatically.** Only paths
  under `/api/sqs` are exempt.

## Tests and how to check by hand

- **`Origin` and host matching:** unit tests in `same_origin.rs` and
  `host.rs` (ports, default ports, case, IPv6, `null`).
- **Through the endpoint test app:** in `src/api/endpoint_tests.rs`,
  `cookie_writes_from_another_origin_are_refused`,
  `admin_json_must_be_labelled_as_json`,
  `a_configured_host_refuses_requests_for_other_names` and
  `without_a_configured_host_any_name_is_answered`. That app mirrors the
  production middleware.
- **Headers:** `cors_tests` and `security_header_tests` in `src/lib.rs`.
- **DNS rebinding:** start Chromium with
  `--host-resolver-rules="MAP evil.test 127.0.0.1"` and open
  `http://evil.test:8080/`. With `NERVEMQ_HOST` set, the page and its login
  get 421; unset, 200.
- **CSP:** inject `<img src=x onerror=…>` or a `<script>` element and check it
  doesn't run. Playwright's `page.evaluate` (the run-nervemq driver's `eval`)
  goes through DevTools, which CSP deliberately doesn't restrict, so code it
  runs directly, `eval` included, proves nothing; test what the *page* does
  with the markup.
