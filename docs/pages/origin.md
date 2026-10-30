# Origin Performance (`/origin`)

Backend-side view of a service: origin TTFB/TTLB, error rate, fetch volume, status codes, slowest URLs, latency by POP and per-origin-IP health.

Source: [`frontend/app/origin/`](../../frontend/app/origin/) (`page.tsx` re-exports `_sections/OriginClient.tsx`, `Aggregates.tsx`, `Timeseries.tsx`, `LatencyHeatmap.tsx`). Built on `ReportLayout`; not SSR-seeded.

## URL parameters

Shared range and filter parameters (`range`, `start_time`, `end_time`, `filters`, see [dashboard.md](dashboard.md)) plus:

| Parameter | Values | Default |
|---|---|---|
| `metric` | `ttfb`, `ttlb` | `ttfb` |
| `percentile` | `p50`, `p95`, `p99` | `p95` |

## Panels

- KPI cards: Origin TTFB (P50), Origin TTFB (P95), Origin Error Rate, Fetch Volume (`Aggregates.tsx`). An `Approximate` badge appears when a section carries `_approx`.
- Origin Latency time series (metric/percentile toggles, trend smoothing) and Status Code Distribution (`Timeseries.tsx`).
- Tables: slowest URLs, latency by POP, origin IP health, plus the Origin Payload Size card; each table has a column-visibility dropdown (`LatencyHeatmap.tsx`).
- When no request reached origin the page shows "No Origin Metrics Yet" / "No Origin Metrics Found" instead of empty panels.
- Each card reserves fixed height and shows its own loading message; refetches dim the content instead of clearing it.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only; window clamped to the invite ceiling by `RequestContext.clamp`. `/api/origin/` is in the analyst POST allowlist (`_ANALYST_ALLOWED_WRITE_PREFIXES`), so analysts can run these read queries. |

## Backend

Routes in [`backend/routers/origin.py`](../../backend/routers/origin.py), models in [`backend/models/analytics/origin.py`](../../backend/models/analytics/origin.py).

- `POST /api/origin/aggregates` (`OriginAggregatesResponse`): the page's single call. Request extends `RangeKeyedRequest` with `bucket_minutes`, `split_by_leg`, `timeseries_metric`, `timeseries_percentile`, `slow_urls_limit`, `slow_urls_min_requests`, `ip_health_limit`, `pop_latency_limit` and `sections` (any of `summary`, `timeseries`, `slow_urls`, `status_codes`, `path_breakdown`, `pop_latency`, `ip_health`). Requesting one of `timeseries` / `status_codes` / `path_breakdown`, or one of `pop_latency` / `ip_health`, pulls in the rest of its group (`_TS_STATUS_PATH_TRIPLE`, `_POP_IP_PAIR`).
- Per-section endpoints (not called by the page): `/summary`, `/timeseries`, `/slow-urls`, `/status-codes`, `/path-breakdown`, `/pop-latency`, `/ip-health`, `/shielding-analysis`, `/drilldown`.

Standard mode reads origin rollups (`origin_summary`, `origin_dims`, `origin_latency_ts`) on eligible unfiltered windows, otherwise scoped raw SQL; rollup-served percentile sections are weighted averages and set `_approx`.

High-scale mode ([`backend/high_scale/origin.py`](../../backend/high_scale/origin.py)): unfiltered requests read the per-minute origin projections, which carry a mergeable log-histogram sketch per percentile metric; sketches are merged across the window with `quantilesExactWeightedArray`, so percentiles are computed over the union of requests (relative error bounded by `RELATIVE_ERROR`, about 1%), counts stay exact, and percentile sections set `_approx` with an `_approx_note`. Minutes written before the sketch columns existed report `null` percentiles until rebuilt ([reader-parity backfill runbook](../runbooks/high-scale-reader-parity-backfill.md)). Any active filter reads `request_facts` with exact quantiles (`_approx: false`).

## Tests

- Backend: [`tests/repositories/test_origin.py`](../../tests/repositories/test_origin.py), [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/core/test_rollups_origin.py`](../../tests/core/test_rollups_origin.py)
- E2E: `Origin Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
