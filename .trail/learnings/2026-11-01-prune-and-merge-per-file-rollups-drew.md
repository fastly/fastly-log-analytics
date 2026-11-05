---
title: Prune and merge per-file rollups on filtered and edge-only dashboard queries
author: Drew
date: 2026-11-01
branch: main
id: l-3351ccc0
scope: repo
status: active
src: agent
touches:
  - app/web/components/dashboard/Dashboard.tsx
  - app/web/lib/dashboard/duckdb.ts
  - app/web/lib/dashboard/engine.test.ts
  - app/web/lib/dashboard/engine.ts
  - app/web/lib/dashboard/filters.ts
  - app/web/lib/dashboard/sources.test.ts
  - app/web/lib/dashboard/sources.ts
---

In app/web/lib/dashboard/engine.ts, runDashboard previously skipped source.readRollup whenever hasFilters was true and recomputed all Parquet files when edgeOnly was true on services with shield rows. In addition, DuckDBAnalyticalEngine.ingestFile in app/web/lib/dashboard/duckdb.ts ran SELECT COUNT(*), MIN(ts_ms), MAX(ts_ms) FROM requests WHERE scoped_key = ... after every file insert, causing O(N^2) full-table scans across 400+ files, and Dashboard.tsx omitted placeholderData: keepPreviousData so every filter/range transition wiped result and opened LoadingDialog. Fix by: (1) storing edgeRollup inside FileRollup and classifying cached rollups against filters ('none' / 'all' / 'partial') to skip 0%-match files and merge 100%-match interior rollups without reading Parquet bytes or touching DuckDB; (2) passing knownStats from cachedRollup to ingestFile and querying read_parquet(virtualName) instead of scanning requests; (3) raising MAX_MEMORY_ROLLUP_ROWS to 5,000,000 and adding an in-memory file LRU cache in browserSource; and (4) setting placeholderData: keepPreviousData on dataQuery and compareQuery in Dashboard.tsx.
