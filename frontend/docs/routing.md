# Frontend route topology

Status of every route under `app/` — rendering mode (CSR / RSC / hybrid), data-fetch boundary, prefetch policy, and the URL state model. Encodes the parts of ADR-05 + Phase 9a (cleanup_plan) that have actually shipped and calls out the remaining work.

## Route table

| Route | Render mode | Cold-load data path | URL state model | Notes |
|---|---|---|---|---|
| `/` | CSR redirect | n/a | n/a | Redirects to `/dashboard` |
| `/dashboard` | CSR client page | SSR'd `/api/bootstrap` (root layout) → React Query cache → client `useDashboardBundle` for the chart/top-bots | `?service=` (nuqs), `?start_time=`, `?end_time=`, `?range=`, `?metric=`, `?interval=` (legacy `useViewMetricUrlSync`) | Highest-traffic route. SSR'd bootstrap puts share-banner, sync-status, log-extents, log-fields-catalog in first paint. Bundle is async-aware (Web Worker for n>2000 rows) |
| `/network` | CSR client page | SSR'd bootstrap → client `useReportConfig` for network aggregates | `?service=`, `?start_time=`, `?end_time=` | NetworkMap reads `world.geojson` via fetch — gzipped 102 KB, `max-age=86400, immutable` on Fastly |
| `/origin` | CSR client page | SSR'd bootstrap → client per-section queries | `?service=`, `?start_time=`, `?end_time=` | |
| `/performance` | CSR client page | SSR'd bootstrap → client per-section queries | `?service=`, `?start_time=`, `?end_time=` | |
| `/security` | CSR client page | SSR'd bootstrap → client per-section queries | `?service=`, `?start_time=`, `?end_time=`, `?filters=` | |
| `/sessions` | CSR client page | SSR'd bootstrap → client `useServiceQuery('sessions')` | `?service=`, `?start_time=`, `?end_time=` | |
| `/insights` | CSR client page | SSR'd bootstrap → client `useInsights` | `?service=` | |
| `/query` | CSR client page | SSR'd bootstrap → client SQL editor + per-run query | `?service=`, `?mode=raw\|structured`, `?start_time=`, `?end_time=`, `?range=`, `?filters=`, `?q=` (raw mode) | Raw mode owns `?mode`/`?q` via direct `router.replace` (not nuqs yet); structured-mode filter/range params now mirror the other ReportLayout pages via `useFilterUrlWriteback` so URLs are shareable |
| `/charts` | CSR client page | SSR'd bootstrap → client `useReportConfig` | `?service=`, `?start_time=`, `?end_time=`, `?metric=`, `?compare=` | |
| `/alerts` | CSR client page | SSR'd bootstrap → client `useAlerts` | `?service=` | |
| `/logs` | CSR client page (admin) | SSR'd bootstrap → client `useLogsPageState` (tabs + file browser) | `?service=`, `?tab=` | Admin-only — blocked at `/api/services/{id}/lake-info` etc. via `_ANALYST_BLOCKED_SUBPATH_REGEX` |
| `/usage` | CSR client page (admin) | SSR'd bootstrap → client `usePrefill` | n/a (no service scope) | Admin-only |
| `/admin` | CSR client page (admin) | SSR'd bootstrap → admin endpoints | n/a | Admin-only — `proxy.ts` blocks remote visitors via `X-Proxied-By-Caddy` |
| `/admin/share` | CSR client page (admin) | SSR'd bootstrap → admin share endpoints | n/a | |
| `/admin/usage-log` | CSR client page (admin) | SSR'd bootstrap → client `useUsageLog` | n/a | |
| `/admin/session-scoring` | CSR client page (admin) | SSR'd bootstrap → admin scoring endpoints | n/a | |
| `/share-login` | CSR client page (analyst) | n/a (auth screen) | n/a | TOS-gated analyst entry point |
| `/share-login/acknowledge` | CSR client page (analyst) | n/a | n/a | |

**Rendering mode summary:** Nine routes (`/admin`, `/admin/rum`, `/admin/trends`, `/alerts`, `/assets-shield`, `/insights`, `/logs`, `/security`, `/streaming`) use an async Server Component `page.tsx` that SSR-fetches primary query data via `apiSsrFetch` and dehydrates it into `<HydrationBoundary>` before rendering a `'use client'` client component; `/dashboard` uses a Server Component `<Suspense>` wrapper passing `nowServerStr` to `DashboardClient`; and the remaining routes are `'use client'` pages directly (or thin barrel re-exports of `'use client'` `_sections/*Client.tsx`). In addition, the root layout (`app/layout.tsx`) is a Server Component that SSR-fetches `/api/bootstrap` and dehydrates it into React Query — the dependent caches it seeds (`['views', sid]`, `['log-fields-catalog', sid]`, `['sync-status', sid]`, `['log-extents', sid]`) land on every page's first paint.

**Filter URL codec:** all `ReportLayout` pages plus `/query` and `/sessions` emit the modern `?filters=<json>` form on store mutations (write path owned by `useFilterUrlWriteback`). On INGRESS every page also accepts the legacy `?filter_<col>=<val>` short form (read path owned by `hydrateFilterStoreFromUrl`) — drill-down emitters (`FilterValueCell`, `session-urls`, `AlertsList`) still produce those URLs, and any external bookmarks pinned to that shape keep working. The legacy form gets normalized to `?filters=` on the first store touch.

## Zustand stores audit (Phase 9a.5)

| Store | Consumer count | Decision | Migration target |
|---|---|---|---|
| `serviceStore` | 34 files | **Stays client-side**, URL-synced via `nuqs` (proof-of-concept) | `useUrlServiceSync` re-implemented on `useQueryState('service')` — first nuqs adoption |
| `filterStore` | 16 files | Stays client-side, URL-synced via `useViewMetricUrlSync` (legacy) | **DEFERRED** — nuqs migration is the next chunk. Touches the filter URL codec + every analytics page. |
| `timezoneStore` | 9 files | Stays client-side, persisted to localStorage | No URL sync needed — user preference, not shareable state |
| `debugStore` | 3 files | Stays client-side, localStorage only | No URL sync needed |

The serviceStore migration is intentionally scoped tight: `useUrlServiceSync` is the ONE URL touch-point for the store. Refactoring just that hook proves out the nuqs pattern (NuqsAdapter wiring in QueryProvider, `useQueryState` binding, write-back-to-store sync) without touching the 34 consumers that READ `activeServiceId`. Same model applies when `filterStore` migrates.

## Code-split policy

- **`PlotlyChart`** ([`components/PlotlyChart/index.tsx`](../components/PlotlyChart/index.tsx)): dynamic-import via `next/dynamic` of `plotly.js-cartesian-dist-min` (~1.4 MB, 3.4× smaller than full plotly.js). The chart's render gated on `IntersectionObserver` with `rootMargin: '600px'` so chunk fetch only fires when the chart is near the viewport.
- **MapLibre**: dynamic-import via `next/dynamic({ ssr: false })`. World geojson (`/geo/world.geojson`) is `Cache-Control: public, max-age=86400, immutable`, gzipped to 102 KB.

## Prefetch policy

- **Sidebar nav links** ([`components/AppLayout.tsx`](../components/AppLayout.tsx) `NavLink`): hover-prefetch via Next.js default `<Link prefetch>`. Click-to-render feels instant on warm cache.
- **`world.geojson`** prefetched via `<link rel="prefetch">` ONLY on `/dashboard` + `/network` (the routes that mount maps). Other pages don't waste the 251 KB raw / 102 KB gzip.
- **`/api/bootstrap`** SSR'd in root layout (every page). The dehydrated state inflates HTML by ~15 KB gzip but eliminates the cold-load client fetch RTT (~300-600 ms on prod-tunnel) for every page navigation.

## SSR Bootstrap Pre-Fetch & Hydration Invariants

Implemented across [`app/layout.tsx`](../app/layout.tsx), [`lib/apiSsrProxy.ts`](../lib/apiSsrProxy.ts), and [`lib/ssr/upstream.ts`](../lib/ssr/upstream.ts):

1. **Use `node:http.request`, not `fetch()`.** Node's `fetch()` always rewrites `Host` from the URL; the backend's `_remote_host_allowed` gate rejects remote-classified requests whose `Host` isn't the public endpoint, so the SSR fetch would silently fall through to the client (or misclassify a remote request as admin-from-loopback). Copy the `rawRequest` pattern in [`lib/apiSsrProxy.ts`](../lib/apiSsrProxy.ts).
2. **Trust topology is `X-Remote-Analyst: 1`, not `X-Proxied-By-Caddy`.** SSR reaches the backend over loopback, and `is_request_remote` classifies on `request.client.host` first, so a forwarded Caddy marker is ignored and the backend would return the **admin** payload into public HTML. Forward `X-Remote-Analyst: 1` (honored on loopback, gated on `tunnel_manager.is_sharing_active()`) only when the inbound request carries `X-Proxied-By-Caddy`; otherwise the admin SSH-tunnel path is mis-classified as analyst.
3. **Always wrap in try/catch + a bounded timeout (5 s) and return `null` on failure.** SSR errors must never break a page; the layout falls back to client fetch.
4. **`export const dynamic = "force-dynamic"` is required** in any layout/page doing a per-request SSR fetch through an imported helper that calls `cookies()`/`headers()`. Next's static analysis detects only direct calls in the component file; otherwise the route is built statically when the backend isn't reachable and the dehydrated state is permanently empty.
5. **Dehydrated state boundary:** anonymous public HTML contains only the `needs_login` stub (no `sharing_active`, `ngwaf_workspace_id`, `sync_status`); admin HTML contains the full payload.
6. **The SSR seed key must byte-match the first-paint client key.** Key the page query on a server-reproducible `(rangeToken, anchor)` relative-range pair, not a client-`now()`-anchored `start/end`, and make the identical snap-to-stale-extents decision on both sides (extents > 15 min stale snap the window to real data), otherwise the seed lands under a different key and the client refetches.
7. **Honor the `?service=` URL param over `bootstrap.active_service_id`**, or a cold load of a non-default service seeds and paints the wrong service.
8. **Don't read from a Zustand store if the answer matters on first paint and the store hydrates post-mount** — the `serviceStore` slice hydrates from the SSR-cached bootstrap in `useBootstrap`'s post-mount effect. For the one-render window before it fires, use [`useEffectiveServiceId`](../hooks/useBootstrap.ts), which falls back to `bootstrap.active_service_id` from the React Query cache; reading `useServiceStore(s => s.activeServiceId)` directly flashes "No service selected".
9. **Server route wrappers:** replacing a server `page.tsx` with its client component removes prefetch and dehydration even if the fetch helper remains, regressing populated initial HTML and causing layout shift. `PlotlyChart` and `LazyMount` default `visible=false` on both server and client and promote to `true` post-mount to avoid hydration mismatch.

## Next.js Navigation & Loading Conventions

1. **Every navigable route has a `loading.tsx`** using `RouteSkeleton` ([`components/ui/skeleton.tsx`](../components/ui/skeleton.tsx)) variants.
2. **Layouts do not block on uncached data.** Layout-level data uses `useQuery` with `staleTime` without gating children on a loading flag.
3. **In-flight queries cancel on route change** (`queryClient.cancelQueries({ type: 'active' })` in `AppLayout`'s `useEffect([pathname])`) and `signal` threads through `queryFn`s (`queryFn: async ({ signal }) => client.GET(..., { signal })`).
4. **Poll intervals:** default ≥ 10 s (never faster than 5 s), with `refetchIntervalInBackground: false`.
5. **Shared 1 Hz clock:** components needing visible-tick "now" state use the `useNowMs` hook rather than per-instance `setInterval`.
6. **Async buttons:** `<Loader2 className="h-3 w-3 mr-1 animate-spin" />` plus a pending label (`Saving…`).
7. **Prefetch:** `<Link prefetch>` is the default; `prefetch={false}` is used in dense lists. Links whose target needs an API call use `onMouseEnter` → `queryClient.prefetchQuery(...)` (see `AdminPrefetchLinks`).
8. **`router.replace()` inside effects** is wrapped in `startTransition`.
9. **React Query defaults** (`QueryProvider`): `staleTime: 30s`, `gcTime: 5min`, `refetchOnWindowFocus: false`.

## Deferred (NOT in this Phase 9a chunk)

- **Drop `PlotlyPrewarm` + `MapPrewarm`** (cleanup_plan Phase 9a.2). These render a 1×1px hidden chart on app mount to force the ~500-1000 ms plotly/maplibre init to happen during page load instead of when real chart data arrives. They look like hacks but they're load-bearing — saves a visible cold-load gap. Dropping requires a paired modulepreload-execution mechanism (preload + on-load handler that calls into the factory). Tracked as a separate workstream.
- **`useViewMetricUrlSync` → nuqs migration** (Phase 9a.6 full scope). Touches the filter URL codec + 16 `useFilterStore` consumers + every analytics page that uses URL filters. The `useUrlServiceSync` migration in THIS chunk demonstrates the pattern; the filter migration is its own session.
- **A11Y screen-reader tables for maps** (NetworkMap, ChoroplethMap, ShieldingMap). Geographic data needs alt-text summary + region list, not the table shape the `PlotlyChart` companion uses. Separate design pass.
- **Web Worker for non-dashboard charts**. `chartDataWorker` infrastructure shipped, currently only routed by the dashboard's `trafficData` path. Extending to origin / performance / security / sessions is a one-liner per page once their per-page data-build helpers go through `buildTrafficDataAsync`-style wrappers.
