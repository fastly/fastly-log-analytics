---
title: "Assets & Shield single-pass read-only CTE, hotspot URL collapsing, and zero-CLS reservations"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-965f3539
scope: repo
status: active
src: agent
touches:
  - "backend/repositories/assets.py#get_assets_aggregates"
  - "backend/routers/performance.py#_assets_aggregates_impl"
  - "frontend/app/assets-shield/_sections/AssetsSummaryAndHelp.tsx#AssetsOverviewGrid"
  - "frontend/components/SyncStatusBadge/SyncStatusBadge.tsx#SyncStatusBadge"
---

On /assets-shield, get_assets_aggregates previously created a DuckDB temp table grouping by (url, cache, content_encoding, status) across all rows (8.3M rows on 30d remote-standard -> 14.7s) and ran 37 LIKE '%.ext' wildcard patterns per URL, while check_assets_cache always returned None in production because sources.py dicts omit duckdb_path/cache_dir. Extracting split_part(url, '.', -1) once, nulling out url/status for non-hotspot rows (collapsing API/Dynamic and compressed HIT rows), combining dims + large_top + low_ttl_top into one read-only CTE query, and wiring run_cached_repo_endpoint + single-flight coalesce eliminates temp-table lock contention and brings 30d cold scans within budget. On the frontend, reserving h-[140px] on AssetsOverviewGrid cards and a 409px placeholder in SyncStatusBadge when !ready eliminates the 0.0372 CLS.
