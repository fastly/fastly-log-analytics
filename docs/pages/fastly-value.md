# Fastly Value (`/fastly-value`)

Summary of what the CDN delivers for a service: offload, cache efficiency, bandwidth, threats blocked, bot share, performance, protocol adoption and Image Optimizer savings, shown in tabs.

Source: [`frontend/app/fastly-value/`](../../frontend/app/fastly-value/) (`page.tsx` re-exports `_sections/FastlyValueClient.tsx`, `ValueDomainTabs.tsx`, `IOValueTab.tsx`). Built on `ReportLayout`; not SSR-seeded.

## Tabs

`summary`, `caching` (CDN & Caching), `assets` (Assets & Shield), `security`, `bots` (Bot Management), `performance`, `network`, `io` (Image Optimizer). Each tab requests only its own section(s) from `POST /api/value/summary` (the Summary tab asks for `overview`, `caching`, `network`). The Assets & Shield tab renders the same content as [assets-shield.md](assets-shield.md) and disables the value query. Without Image Optimizer traffic the `io` tab shows an upsell view with optimization opportunities.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only; `/api/value/` is in the analyst POST allowlist; window clamped to the invite; the Image Optimizer tab still depends on the service's stored Fastly credentials server-side. |

## Backend

`POST /api/value/summary` in [`backend/routers/performance.py`](../../backend/routers/performance.py); logic in [`backend/repositories/value.py`](../../backend/repositories/value.py); models in [`backend/models/analytics/dashboard.py`](../../backend/models/analytics/dashboard.py).

- Request `ValueSummaryRequest`: range fields (`start_time`, `end_time`, `range_token`, `anchor`), `filters`, `chart_interval` (default `1 day`), `sections` from `overview`, `caching`, `security`, `bots`, `performance`, `network`, `io`.
- Response `ValueSummaryResponse`: `overview` (totals, bandwidth, edge hit and origin requests, origin offload %, threats blocked, cache acceleration factor), `caching` (hit/miss/pass/synth requests, bandwidth saved, shield effectiveness, offload and cache-state series), `security` (WAF blocked/logged/passed, threat series, top WAF signals), `bots` (bot and verified-bot requests, top bots), `performance` (hit and miss latency, p99, series), `network` (HTTP/3, TLS, IPv6, HTTP/2 shares and series), `io` (transforms, bandwidth, shield bandwidth, format distribution, estimated cost at Fastly list pricing for Image Requests: 100k free per month, then tiered per 10,000 requests, optimization opportunities).

Unfiltered overview, caching and network requests use the hourly rollup bundles (`_try_overview_from_rollup`) with a raw-scan fallback. The `io` section reads the Fastly Historical Stats API (`/stats/service/{id}?by=day`) with the service's stored token, cached for 60 seconds (`_IO_STATS_CACHE`); it returns nothing if no token or service ID is configured. Optional log fields that are absent degrade to null or zero rather than failing the query.

## Tests

- Frontend: `fastly-value page mounts and renders title and tab navigation` in [`frontend/__tests__/app/dashboard.test.tsx`](../../frontend/__tests__/app/dashboard.test.tsx)
- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/repositories/test_base_helpers.py`](../../tests/repositories/test_base_helpers.py)
