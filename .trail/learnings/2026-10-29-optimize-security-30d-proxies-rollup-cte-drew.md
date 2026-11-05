---
title: Optimize /security 30d proxies rollup CTE and full-retention wellknown_bots version sweep
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-38d7b142
scope: repo
status: active
src: agent
touches:
  - backend/repositories/rollup_executor/scalars.py
  - backend/cron/jobs/compaction.py
  - backend/core/rollups/security_bots.py
  - frontend/app/security/page.tsx
  - frontend/app/security/_sections/BotsSection.tsx
---

On /security, try_security_proxies_from_rollup previously scanned 1.97M 30d security_proxies rollup rows into a materialized CTE with GROUP BY ALL before joining pops, taking 590ms-1.08s on GCE. Joining pops (<=50 rows) directly to the parquet scan with inline Haversine and a single-pass GROUP BY ip cut the 30d rollup query from 590ms to 156ms (3.7x speedup). Also, run_rollup_compaction passed lookback_days=2 to backfill_wellknown_bots_rollup, leaving hours older than 48h on stale pattern_set_version; sweeping stale hours across full retention keeps 7d/30d wellknown_bots 100% rollup-covered.
