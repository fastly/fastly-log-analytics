# Assets & Shield (`/assets-shield`)

Caching diagnostics by asset type: request volume, egress, cache hit ratio, compression and TTL hotspots.

Source: [`frontend/app/assets-shield/`](../../frontend/app/assets-shield/): `page.tsx` is a server wrapper that prefetches and dehydrates the default aggregates; `_sections/AssetsClient.tsx`, `AssetsSummaryAndHelp.tsx`, `AssetsTables.tsx`. Built on `ReportLayout`.

## Panels

- Overview grid: one card per asset type (requests, egress, hit ratio, compression rate); each links to the Dashboard filtered to that category.
- Cache Efficiency by Asset Type: stacked cache states (HIT, MISS, PASS, ...) per type.
- Payload Compression Efficiency per type (`br`, `gzip`, uncompressed).
- Large Uncompressed Assets and Low TTL Assets tables.
- If the service's log field settings (`active_log_field_ids` from bootstrap) leave off `ttl` or `resp_header_content_encoding`, a banner names them. The response's `missing_fields` is not used. With no data the page shows "No Asset Metrics Yet"; a failed query shows "Error Loading Assets Data".
- Each card has an info button explaining how its numbers are computed.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only; `/api/assets/` is in the analyst POST allowlist; window clamped to the invite. |

## Backend

`POST /api/assets/aggregates` (and a query-string `GET` mirror) in [`backend/routers/performance.py`](../../backend/routers/performance.py); models in [`backend/models/analytics/performance.py`](../../backend/models/analytics/performance.py). Request is `AssetsRequest` (range fields plus `filters`). Response `AssetsAggregatesResponse`: `asset_type_breakdown`, `cache_performance`, `compression_performance`, `large_uncompressed_assets`, `low_ttl_assets`, `missing_fields`. The ClickHouse reader ([`backend/high_scale/performance.py`](../../backend/high_scale/performance.py)) classifies URLs into asset types over visible `request_facts`.

## Classification (both editions)

Both editions classify and count by the same rules (High-Scale's `_ASSET_TYPE_CASE` in [`backend/high_scale/ingest_controller/service.py`](../../backend/high_scale/ingest_controller/service.py); Cloud's `ASSET_TYPE_SQL` in [`frontend/lib/cloud-engine/assets.ts`](../../frontend/lib/cloud-engine/assets.ts) and `url_asset_type` in [`compute/cloud/src/ingest/summary.rs`](../../compute/cloud/src/ingest/summary.rs)):

- Asset type from the URL's extension: Images, Documents, JavaScript/CSS, Fonts, Video, else API/Dynamic. No content type is logged, so it is never used.
- A request with no cache value counts as MISS. Cache hit ratio is HIT and HIT-SYNTHETIC over HIT, HIT-SYNTHETIC, MISS, PASS and BYPASS.
- Encoding: `br` or `gzip` when the value contains them, else the logged value, else `uncompressed`. Compression rate is requests with an encoding of exactly gzip, br or deflate over compressible URLs (`.js .mjs .css .svg .html .json`).
- Large Uncompressed Assets: compressible URLs served uncompressed, status not 204 or 304, top 20 by bytes. Low Cache-TTL Hotspots: non-API URLs answered with MISS, top 20 by requests, with their average TTL.

## Cloud

Cloud answers `POST /api/assets/aggregates` in the browser and accepts a Cloud-only `sections` field (`summary`, `large_uncompressed_assets`, `low_ttl_assets`; omitted means all). When the view is unfiltered, or its only filter is an exact-value `cache` filter, the page makes two reads: `summary` (the totals, the overview grid, Cache Efficiency and Payload Compression) and the two URL tables. The first is served from v3 summaries (the `assets` rows' `[asset_type, cache, encoding, compressed, compressible]` sums; summaries written before those keys fall back to the raw scan) and shows "Data through" above the grid. The tables read raw rows only once their panel is within 600 px of the viewport, and that read loads only the URL, asset type, encoding, body size, status, TTL and cache columns (no TTFB or origin timing). Any other filter keeps one raw read for every section.

The summary read falls back to the raw scan when the summaries cut the asset keys to their top K, a key holds non-ASCII text, the asset request total differs from the overview's, or the range isn't covered by bundles. With a cache filter it also falls back when a `cache_status` column is logged, the `cache` values are cut to their top K, or any logged `cache` value is `OTHER` or not upper case. The URL tables always scan raw: summaries hold no per-URL TTL or body bytes.

## Tests

- Cloud: [`frontend/lib/cloud-engine/assets-rollup.test.ts`](../../frontend/lib/cloud-engine/assets-rollup.test.ts), [`frontend/__tests__/components/AssetsValueRollup.test.tsx`](../../frontend/__tests__/components/AssetsValueRollup.test.tsx)
- E2E: `Assets & Shield Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
