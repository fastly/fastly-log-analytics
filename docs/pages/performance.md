# Performance (`/performance`)

End-to-end delivery latency for one service: an average latency waterfall, slowest URLs and networks (ASNs), cache TTL distribution, and an origin-vs-edge scatter.

Source: [`frontend/app/performance/`](../../frontend/app/performance/) (`page.tsx` re-exports `_sections/PerformanceClient.tsx`; help dialogs in `PerformanceHelp.tsx`). Built on `ReportLayout`; not SSR-seeded.

## URL parameters

Shared range and filter parameters (see [dashboard.md](dashboard.md)) plus `sort_by` (`avg`, `p50`, `p95`, `p99`; default `p99`) which orders the URL and ASN tables.

## Panels

- End-to-End Latency Waterfall (Average): edge processing, origin wait, origin download, client download.
- Slowest URLs and Slowest Networks tables (columns: requests, avg, p50, p95, p99) with column-visibility dropdowns.
- Cache TTL Distribution histogram.
- Origin vs Edge Processing scatter (sampled points, coloured by cache state).
- An `Approximate` badge shows when the response has `_approx: true`; its tooltip says percentiles on that window are request-weighted averages of per-hour percentiles while request counts are exact.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only; `/api/performance/` is in the analyst POST allowlist, the window is clamped to the invite ceiling, IPs are masked per invite. |

## Backend

`POST /api/performance/aggregates` in [`backend/routers/performance.py`](../../backend/routers/performance.py) (that file also hosts the assets, CMCD and value routes). Models: [`backend/models/analytics/performance.py`](../../backend/models/analytics/performance.py).

- Request `PerformanceRequest`: `RangeKeyedRequest` fields (`filters`, `start_time`, `end_time`, `range_token`, `anchor`), `sort_by`, `sections` (`waterfall`, `scatter`, `top_urls`, `top_asns`, `ttl_dist`). `top_urls`/`top_asns` are always computed together, as are `waterfall`/`scatter`.
- Response `PerformanceAggregatesResponse`: `waterfall.avg` (`edge_processing`, `origin_wait`, `origin_download`, `client_download`), `top_urls[]` (`url`, `requests`, `avg`, `p50`, `p95`, `p99`), `top_asns[]` (`asn`, `label`, `requests`, plus the same percentiles), `ttl_dist[]` (`bucket`, `count`), `scatter[]` (`origin`, `edge`, `cache`), and `_approx`.

Standard mode: unfiltered long windows use the `perf_latency` rollup for top URLs and ASNs (`try_perf_latency_from_rollup`, sets `_approx`); otherwise scoped SQL over one narrowed temp table, with the waterfall and scatter sharing one materialized components CTE.

High-scale mode ([`backend/high_scale/performance.py`](../../backend/high_scale/performance.py)): unfiltered requests read the minute projections (`performance_minute_dimensions` for URLs/ASNs, `performance_minute_summary` for the waterfall, `performance_scatter_samples` for the scatter); any active filter reroutes every section to `request_facts`. Projection percentiles are request-weighted averages (`_approx: true`); filtered percentiles are exact. Stored latency columns hold microseconds and the reader divides by 1000. See [AGENTS.md](../../AGENTS.md) for the projection-table checklist.

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py)
- E2E: `Performance Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
