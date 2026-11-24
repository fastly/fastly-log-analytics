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

The ClickHouse reader ([`backend/high_scale/performance.py`](../../backend/high_scale/performance.py)) serves unfiltered requests from the minute projections (`performance_minute_dimensions` for URLs/ASNs, `performance_minute_summary` for the waterfall, `performance_scatter_samples` for the scatter); any active filter reroutes every section to `request_facts`. `performance_minute_dimensions` stores mergeable weighted latency sketches (`latency_sk_v`, `latency_sk_w`) evaluated with `quantilesExactWeightedArray(0.5, 0.95, 0.99)` (falling back to legacy request-weighted averages only for rows without sketches; `_approx: true`); filtered percentiles are exact. Stored latency columns hold microseconds and the reader divides by 1000. See [AGENTS.md](../../AGENTS.md) for the projection-table checklist.

## Cloud

Unfiltered, `ttl_dist` (exact counts from v3 `urls/ttl_bucket` summary rows, with fallback to `all_fields` TTL counts), `waterfall` (exact averages), `top_urls`, `top_asns` and `scatter` come from v3 summaries ([`performance-rollup.ts`](../../frontend/lib/cloud-engine/performance-rollup.ts)), with a raw fallback. The page reads all five in one summary call and loads no raw Parquet; any filter keeps one raw read for every section.

- `top_urls` and `top_asns` come from the summaries' `urls` rows, one per URL and per ASN with an elapsed histogram and an exact elapsed sum and count. Request counts and averages are exact, so sorting by average ranks like raw. p50, p95 and p99, and a ranking by them, come from the histogram, so the cards show Approximate. Each summary keeps its 2,000 busiest URLs and ASNs; when it cut some, the card title shows "≥ counts" and a rarely requested slow URL can be missing from the ranking. Summaries written before the `urls` rows existed can't answer the rankings.
- `scatter` comes from the summaries' `sample` rows: each summary keeps the 300 rows with a TTFB that have the smallest sample key, so the 300 smallest across the range are a uniform sample of it, and the points drop rows without an elapsed or with a negative edge time like raw. Raw samples by `hash(timestamp|url)` instead, so the points differ from a raw read's (each is still a real request), and a range can show fewer than 300 points where raw shows 300. The card title says "≈ sample" when the scatter came from summaries.

The raw sections run against normalized Parquet views in local DuckDB. Cached compressed files stay reusable; loading
does not copy every decoded column into a resident table. DuckDB can project the
columns needed by each query and apply eligible Parquet pruning.
Range loading reconciles the current manifest with resident
files: unchanged files stay loaded, fresh tails are added, and retired or replaced
files are removed before querying. File identities include the service and file
version, so changing services or replacing a file cannot reuse its old rows.
Discovered custom filters retain their native string, numeric or boolean types.
The existing 256-file and 64 MiB compressed scan limits still apply.

The 256 MB DuckDB query budget is separate from the compressed-file budget.
An out-of-memory or fatal database error fails the current request without replay.
A new request recreates the backend only after its old worker cleanup is confirmed
and can reuse the cached compressed bytes. High-cardinality queries can still
exceed the budget; Cloud does not thin the selected data to make them pass.

Analytical windows use half-open `[start, end)` bounds. File discovery rounds
outward to native second precision without widening the SQL window, so an
exact-endpoint record cannot change the count merely because files are regrouped.

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py)
- E2E: `Performance Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
