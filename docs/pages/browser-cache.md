# Browser Cache (`/cache`)

Cloud-only page for inspecting and clearing local browser storage used by the browser-side DuckDB analytical engine.

Source: [`frontend/app/cache/`](../../frontend/app/cache/) (`page.tsx`, `CacheClient.tsx`) and [`frontend/lib/cloud-engine/dashboard/browser-cache-stats.ts`](../../frontend/lib/cloud-engine/dashboard/browser-cache-stats.ts).

## Panels

- **Cached Data:** total bytes and item count across the Origin Private File System (OPFS) Parquet store and Cache Storage buckets.
- **Origin Storage Quota:** browser-reported origin usage and quota from `navigator.storage.estimate()`, percentage used, and persistence badge (`Persistent` / `Best-effort` from `navigator.storage.persisted()`).
- **OPFS Parquet Store:** file count and total bytes inside `flp-parquet-opfs-v1` (used by DuckDB-WASM).
- **Cache Storage:** entry count and total bytes across non-empty browser Cache Storage buckets (`flp-parquet-v1`, `flp-parquet-meta-v1`, `flp-manifest-v1`, `flp-filtered-v1`, `flp-parquet-rollup-*`, `flp-ops-*`).
- **Last Cache Write Warning:** shown when `readStoreError()` (`file-store.ts`) has recorded a recent write or quota error, displaying the store name, error message, object key, size, and timestamp.
- **Storage Breakdown:** per-store table listing each local store's human-readable label, raw identifier, item count, and size.

## Actions

- **Refresh:** re-scans OPFS, Cache Storage, and `navigator.storage` estimates on demand.
- **Clear Browser Cache:** clears in-memory manifest/source caches (`clearManifestCache`, `clearDashboardBrowserCaches`), deletes OPFS (`flp-parquet-opfs-v1`) and Cache Storage buckets (including `clearOpsCaches`), and immediately refreshes the displayed totals. Server-side data in Fastly Object Storage is never modified.

## Roles & Editions

- **Cloud:** linked under `SYSTEM_NAVIGATION` as **Browser Cache** for all signed-in roles (admins and analysts each manage their own browser's local cache) and allowlisted in `compute/cloud/src/shell.rs` (`APP_PAGE_ROUTES`).
- **High-Scale:** hidden from navigation; visiting `/cache` directly displays an informational notice that browser Parquet caching is only used in Cloud deployments.

## Tests

- [`frontend/lib/cloud-engine/dashboard/browser-cache-stats.test.ts`](../../frontend/lib/cloud-engine/dashboard/browser-cache-stats.test.ts)
- [`frontend/app/cache/CacheClient.test.tsx`](../../frontend/app/cache/CacheClient.test.tsx)
- [`compute/cloud/tests/route_auth/auth_routes.rs`](../../compute/cloud/tests/route_auth/auth_routes.rs) (`analytics_site_routes_allowlist_covers_all_exported_pages`)
