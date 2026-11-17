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

The ClickHouse reader ([`backend/high_scale/origin.py`](../../backend/high_scale/origin.py)) serves unfiltered requests from the per-minute origin projections, which carry a mergeable log-histogram sketch per percentile metric; sketches are merged across the window with `quantilesExactWeightedArray`, so percentiles are computed over the union of requests (relative error bounded by `RELATIVE_ERROR`, about 1%), counts stay exact, and percentile sections set `_approx` with an `_approx_note`. Minutes written before the sketch columns existed report `null` percentiles until rebuilt ([reader-parity backfill runbook](../runbooks/high-scale-reader-parity-backfill.md)). Any active filter reads `request_facts` with exact quantiles (`_approx: false`).

## Cloud

Unfiltered, the page sends the sections v3 summaries can answer as their own request ([`rollup-sections.ts`](../../frontend/lib/cloud-engine/rollup-sections.ts), [`origin-rollup.ts`](../../frontend/lib/cloud-engine/origin-rollup.ts)); the rest scan raw Parquet with exact percentiles. Any section the summaries can't match sends that whole request to the raw scan.

- `status_codes`: exact, from the per-field origin status counts. Raw when those counts were cut to a top K.
- `ip_health`: counts and error shares exact, p50/p95 ≈ from the IP's OTTFB or client TTFB histogram. Raw when an IP mixes the two or has rows without an origin status. Requests are shown as "≥ N" when the IP list was cut to a top K.
- `timeseries`: only for OTTLB on whole 5-minute buckets that tile a day, without a leg split. Miss counts exact, percentiles ≈.
- Raw only. Each needs summary data the v3 format lacks:
  - `summary`: an `elapsed − ottlb` histogram, an `obytes` histogram, and MISS/PASS counts and OTTFB/OTTLB histograms restricted to rows with an origin latency.
  - `slow_urls`: a complete per-URL `COALESCE(ottfb, ttfb)` histogram.
  - `path_breakdown`, `pop_latency` and the TTFB series: a `COALESCE(ottfb, ttfb)` histogram per edge flag, per POP and per 5-minute bucket. Summaries keep OTTFB and client TTFB apart.

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/high_scale/test_query_service.py`](../../tests/high_scale/test_query_service.py)
- E2E: `Origin Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
