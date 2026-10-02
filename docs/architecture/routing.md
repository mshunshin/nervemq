# Routing

The admin UI is a single-page app: React Router in the browser, built by Vite
into `out/`, which the Rust server embeds at compile time (the `embed-ui`
feature). Routing happens in two places: the route table, in the browser, and
the server's fallback, which hands every page path to that table.

## The route table

[`app/router.tsx`](../../app/router.tsx) lists every page, using React
Router's `createBrowserRouter`:

| Path | Component | Notes |
| --- | --- | --- |
| `/` | `<Navigate to="/queues">` | |
| `/login` | [`app/routes/login.tsx`](../../app/routes/login.tsx) | Outside the dashboard: no sidebar or header |
| — | [`app/layouts/dashboard.tsx`](../../app/layouts/dashboard.tsx) | Layout route: sidebar, header and `<AuthVerifier>` around the pages below, through `<Outlet>` |
| `/queues` | [`app/routes/queues.tsx`](../../app/routes/queues.tsx) | Every queue the user can access |
| `/queues/:namespace` | [`app/routes/queues.tsx`](../../app/routes/queues.tsx) | One namespace's queues: the queue page's breadcrumb and the namespace picker lead here. An unknown namespace, or one the user can't access, gets the not-found card |
| `/queues/:namespace/:queue` | [`app/routes/queue-detail.tsx`](../../app/routes/queue-detail.tsx) | Reads both with `useParams()` |
| `/namespaces`, `/api-keys`, `/admin` | `app/routes/*.tsx` | |
| `*` | [`app/routes/not-found.tsx`](../../app/routes/not-found.tsx) | |

A pathless route around all of them has
[`app/routes/error.tsx`](../../app/routes/error.tsx) as its `errorElement`, so
an uncaught render error shows an error page instead of a blank screen.

Before the move to React Router (October 2026) the UI was a Next.js static
export. That needed every dynamic route enumerable at build time, so the
queue page was prerendered once as a `queues/_/_` placeholder and re-read its
real segments from `window.location`. A single-page app has no such step.

## The server

API routes (`/api/...`) are matched first by actix. Everything else falls to
the `default_service` in [`src/lib.rs`](../../src/lib.rs) (`mod ui`):

1. a file in the build (`index.html`, `favicon.ico`, `assets/*`) is served as
   it is;
2. a miss under `/api/` or `/assets/` is a 404, so a client never parses the
   app's HTML as JSON or as a script;
3. anything else is a page: it gets `index.html`, and the router renders the
   matching route, or its not-found page.

File extensions are deliberately ignored: a queue may be named `jobs.fifo`.
The request path is percent-decoded before the lookup. Both rules are pinned
by the tests in that module, and `tests/smoke.rs` loads `/`, `/login` and a
queue deep link from the real binary.

## Navigation and guards

- **`<Link>`** for the sidebar and the breadcrumbs, with `useLocation()`
  driving the sidebar's active item
  ([`components/sidebar.tsx`](../../components/sidebar.tsx),
  [`components/header.tsx`](../../components/header.tsx)).
- **`useNavigate()`** for the moves code makes: after logging in or out, a
  queue or namespace row click (segments encoded with `encodeURIComponent`),
  the queue list's namespace picker, the access-denied and not-found buttons,
  and the admin page sending non-admins away.
- **Authentication is checked in the browser.**
  [`AuthVerifier`](../../components/auth-verifier.tsx), in the dashboard
  layout, calls `POST /api/admin/auth/verify` on load and every 5 minutes, and
  sends the user to `/login` when it fails. That is for the user's benefit,
  not security: the server checks the session cookie on every API call.

## Development

`bun run dev` (or `just dev`) starts Vite's dev server on port 3000, with hot
reload. It forwards `/api/admin` to a server on port 8080
([`vite.config.ts`](../../vite.config.ts)), so in development the UI is on one
origin, as it is when embedded: the session cookie and the relative API URLs
in [`lib/actions/api.ts`](../../lib/actions/api.ts) work unchanged.

## Rough edges

- **Queue and namespace rows navigate through `onRowClick`**
  ([`app/routes/queues.tsx`](../../app/routes/queues.tsx),
  [`app/routes/namespaces.tsx`](../../app/routes/namespaces.tsx)) rather than
  being links, so cmd-click, middle-click and "open in new tab" don't work on them.
