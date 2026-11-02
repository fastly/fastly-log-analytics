# Assets & Shield (`/assets-shield`)

Caching diagnostics by asset type: request volume, egress, cache hit ratio, compression and TTL hotspots.

Source: [`frontend/app/assets-shield/`](../../frontend/app/assets-shield/): `page.tsx` is a server wrapper that prefetches and dehydrates the default aggregates; `_sections/AssetsClient.tsx`, `AssetsSummaryAndHelp.tsx`, `AssetsTables.tsx`. Built on `ReportLayout`.

## Panels

- Overview grid: one card per asset type (requests, egress, hit ratio, compression rate); each links to the Dashboard filtered to that category.
- Cache Efficiency by Asset Type: stacked cache states (HIT, MISS, PASS, ...) per type.
- Payload Compression Efficiency per type (`br`, `gzip`, uncompressed).
- Large Uncompressed Assets and Low TTL Assets tables.
- If the response lists `missing_fields` (for example the content-encoding or TTL log fields), a banner names them. With no data the page shows "No Asset Metrics Yet"; a failed query shows "Error Loading Assets Data".
- Each card has an info button explaining how its numbers are computed.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only; `/api/assets/` is in the analyst POST allowlist; window clamped to the invite. |

## Backend

`POST /api/assets/aggregates` (and a query-string `GET` mirror) in [`backend/routers/performance.py`](../../backend/routers/performance.py); models in [`backend/models/analytics/performance.py`](../../backend/models/analytics/performance.py). Request is `AssetsRequest` (range fields plus `filters`). Response `AssetsAggregatesResponse`: `asset_type_breakdown`, `cache_performance`, `compression_performance`, `large_uncompressed_assets`, `low_ttl_assets`, `missing_fields`. The repository ([`backend/repositories/assets.py`](../../backend/repositories/assets.py)) classifies URLs into asset types in a single pass.

## Tests

- E2E: `Assets & Shield Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
