# Routing

Routing in nervemq happens in **three layers**: Next.js's file-system router at
build time, the Rust server at serve time, and the Next client router in the
browser. The unusual part is that there is **no Next.js server at runtime** —
the UI is a static export (`output: "export"` in
[`next.config.ts`](../../next.config.ts)) baked into the Rust binary via the
`embed-ui` feature, so anything Next would normally do on a server (redirects,
middleware, dynamic params) has to be handled by either the Rust side or the
client.

## Layer 1: File-system routes (Next.js App Router)

Pure convention-based App Router — no routing library, no `pages/` directory:

| File | URL | Notes |
|---|---|---|
| [`app/page.tsx`](../../app/page.tsx) | `/` | Client redirect → `/queues` |
| [`app/login/page.tsx`](../../app/login/page.tsx) | `/login` | Outside the dashboard group — no sidebar/header |
| [`app/(dashboard)/layout.tsx`](../../app/(dashboard)/layout.tsx) | — | **Route group**: wraps the four pages below in sidebar + header + `<AuthVerifier>` without affecting URLs |
| [`app/(dashboard)/queues/page.tsx`](../../app/(dashboard)/queues/page.tsx) | `/queues` | |
| [`app/(dashboard)/queues/[...queueId]/page.tsx`](../../app/(dashboard)/queues/%5B...queueId%5D/page.tsx) | `/queues/<ns>/<name>` | **Catch-all dynamic route** — see below |
| `app/(dashboard)/{namespaces,api-keys,admin}/page.tsx` | `/namespaces` etc. | |
| [`app/not-found.tsx`](../../app/not-found.tsx) / [`app/error.tsx`](../../app/error.tsx) | — | 404 page + error boundary conventions |

`next build` flattens this into static HTML: one file per route
(`queues.html`, `admin.html`, …) plus shared JS chunks, all under `out/`.

## Layer 2: The dynamic-route problem and its workaround

Static export requires every dynamic route to be enumerable at build time, but
queue names only exist at runtime. The solution
([`app/(dashboard)/queues/[...queueId]/page.tsx`](../../app/(dashboard)/queues/%5B...queueId%5D/page.tsx)):

1. `generateStaticParams()` returns a **single placeholder**
   `{ queueId: ["_", "_"] }`, so the build emits exactly one shell:
   `out/queues/_/_.html`.
2. On a hard load of `/queues/mss/test`, `useParams()` returns the baked-in
   `"_"` segments — so the `useQueueId()` hook in
   [`queue-detail.tsx`](../../app/(dashboard)/queues/%5B...queueId%5D/queue-detail.tsx)
   detects the placeholder and **re-derives the real segments from
   `window.location.pathname`** (with `decodeURIComponent`).
3. On client-side navigation (clicking a row), `useParams()` has the real
   values and the fallback never runs.

This is the standard escape hatch for runtime-dynamic routes under
`output: "export"`.

## Layer 3: The Rust server

API routes (`/api/...`) are matched first by actix; everything else falls to a
`default_service` ([`src/lib.rs`](../../src/lib.rs), `mod ui`) that resolves
against the `rust-embed`-bundled `out/` directory, trying in order:

1. exact file
2. `<path>.html`
3. `<path>/index.html`
4. **SPA fallback**: any unmatched `/queues/...` path serves the
   `queues/_/_.html` shell
5. `404.html` (Next's not-found page) with a real 404 status

It also percent-decodes request paths before lookup (asset URLs can arrive
encoded), and that behaviour is covered by a unit test in the same module.

## Client-side navigation & guards

- **`<Link>`** (with automatic prefetch) for the sidebar nav, with
  `usePathname()` driving the active-item highlight
  ([`components/sidebar.tsx`](../../components/sidebar.tsx)).
- **`useRouter().push/replace`** from `next/navigation` (the App Router API,
  not the legacy `next/router`) for programmatic moves: login success, logout,
  the `/` → `/queues` redirect, queue-row clicks, and the
  access-denied/not-found "go back" buttons.
- **Auth is entirely client-side**:
  [`AuthVerifier`](../../components/auth-verifier.tsx) in the dashboard layout
  polls `POST /api/admin/auth/verify` every 5 minutes and redirects to
  `/login` on failure; the admin page redirects confirmed non-admins via an
  effect. There is no route protection at the routing layer itself — which is
  correct here, because middleware doesn't exist in a static export and the
  real enforcement is the server's cookie checks on every API call. The client
  redirects are UX, not security.

## Assessment

Routing uses current idioms throughout, with zero extra libraries:

- App Router conventions (route groups, catch-all segments,
  `generateStaticParams`, `not-found.tsx`, `error.tsx`), `next/navigation`
  APIs, no deprecated `pages/`, `getStaticProps`, or `next/router`.
- Routing is 100% Next.js built-in — no react-router or similar bolted on.
- The placeholder-shell + `window.location` fallback is a hack, but it's the
  *idiomatic* hack for this architecture, contained and documented at both
  ends (Next page comment + Rust fallback comment + test).

Known minor non-idiomatic spots, both cosmetic:

1. **Queue rows navigate via `onRowClick` + `router.push`**
   ([`app/(dashboard)/queues/page.tsx`](../../app/(dashboard)/queues/page.tsx))
   instead of rendering links — so no prefetch, and
   cmd+click/middle-click/open-in-new-tab don't work on rows.
2. **`/` redirects in a `useEffect`** ([`app/page.tsx`](../../app/page.tsx)) —
   a frame of blank render before landing on `/queues`. Unavoidable without a
   server; redirecting in the Rust layer would shave the flash.

No `loading.tsx`/Suspense route transitions exist, but with all data fetched
client-side through TanStack Query (every page renders instantly with its own
spinners), they would add little here.
