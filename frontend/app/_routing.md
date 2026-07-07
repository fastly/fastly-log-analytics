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

**Rendering mode summary:** every analytics page is `'use client'` today. The root layout (`app/layout.tsx`) is a Server Component that SSR-fetches `/api/bootstrap` and dehydrates it into React Query — the dependent caches it seeds (`['views', sid]`, `['log-fields-catalog', sid]`, `['sync-status', sid]`, `['log-extents', sid]`) land on every page's first paint without per-route SSR work.

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

- **`PlotlyChart`** ([`components/PlotlyChart/PlotlyChart.tsx`](../components/PlotlyChart/PlotlyChart.tsx)): dynamic-import via `next/dynamic` of `plotly.js-cartesian-dist-min` (~1.4 MB, 3.4× smaller than full plotly.js). The chart's render gated on `IntersectionObserver` with `rootMargin: '600px'` so chunk fetch only fires when the chart is near the viewport.
- **MapLibre**: dynamic-import via `next/dynamic({ ssr: false })`. World geojson (`/geo/world.geojson`) is `Cache-Control: public, max-age=86400, immutable`, gzipped to 102 KB.

## Prefetch policy

- **Sidebar nav links** ([`components/AppLayout.tsx`](../components/AppLayout.tsx) `NavLink`): hover-prefetch via Next.js default `<Link prefetch>`. Click-to-render feels instant on warm cache.
- **`world.geojson`** prefetched via `<link rel="prefetch">` ONLY on `/dashboard` + `/network` (the routes that mount maps). Other pages don't waste the 251 KB raw / 102 KB gzip.
- **`/api/bootstrap`** SSR'd in root layout (every page). The dehydrated state inflates HTML by ~15 KB gzip but eliminates the cold-load client fetch RTT (~300-600 ms on prod-tunnel) for every page navigation.

## Hydration rules

Distilled from the SSR pattern shipped 2026-06-11 and reproduced from the parent "Frontend Patterns" section of [AGENTS.md](../../AGENTS.md):

1. **Don't read from a Zustand store IF the answer matters on first paint AND the store hydrates from localStorage** — use [`useEffectiveServiceId`](../hooks/useIsDataReady.ts) (or an equivalent) that falls back to the SSR'd React Query cache. Otherwise the page flashes "no service selected" / empty filters for one render before Zustand catches up.
2. **`PlotlyChart`'s `visible` flag starts `false` on both server and client** to avoid hydration mismatch (see PlotlyChart.tsx comment). IntersectionObserver promotes to true post-mount.
3. **`LazyMount` defaults to `visible=false`** for the same reason — match SSR and hydrate shapes exactly, lift `visible` after mount.
4. New SSR fetches in `layout.tsx` MUST use the `node:http` pattern from [`lib/ssr/bootstrap.ts`](../lib/ssr/bootstrap.ts) — Node's `fetch()` rewrites the `Host` header, which the backend rejects for remote-classified requests (cause of the 2026-06-11 SSR-leak incident).

## Deferred (NOT in this Phase 9a chunk)

- **Drop `PlotlyPrewarm` + `MapPrewarm`** (cleanup_plan Phase 9a.2). These render a 1×1px hidden chart on app mount to force the ~500-1000 ms plotly/maplibre init to happen during page load instead of when real chart data arrives. They look like hacks but they're load-bearing — saves a visible cold-load gap. Dropping requires a paired modulepreload-execution mechanism (preload + on-load handler that calls into the factory). Tracked as a separate workstream.
- **`useViewMetricUrlSync` → nuqs migration** (Phase 9a.6 full scope). Touches the filter URL codec + 16 `useFilterStore` consumers + every analytics page that uses URL filters. The `useUrlServiceSync` migration in THIS chunk demonstrates the pattern; the filter migration is its own session.
- **A11Y screen-reader tables for maps** (NetworkMap, ChoroplethMap, ShieldingMap). Geographic data needs alt-text summary + region list, not the table shape the `PlotlyChart` companion uses. Separate design pass.
- **Web Worker for non-dashboard charts**. `chartDataWorker` infrastructure shipped, currently only routed by the dashboard's `trafficData` path. Extending to origin / performance / security / sessions is a one-liner per page once their per-page data-build helpers go through `buildTrafficDataAsync`-style wrappers.
