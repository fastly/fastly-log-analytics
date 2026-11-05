---
title: "Usage prefill/rates fast path, current-storage metadata rum_bytes, and concurrent log-activity"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-aa80e6a7
scope: repo
status: active
src: agent
touches:
  - backend/routers/usage.py
  - backend/repositories/assets.py
  - frontend/app/usage/_sections/UsageStatCards.tsx
  - frontend/components/Insights/index.tsx
---

GET /api/usage/prefill/rates previously called _compute_prefill_rates with full metadata averages (get_node_count_avg and get_rum_beacons_avg_per_day), adding ~3s of unnecessary metadata scans on every /usage mount even though prefill/rates is only used for rate constants on stat cards. Adding include_metadata_averages=False for prefill/rates and caching metadata averages in _PREFILL_META_CACHE for prefill, reading rum_bytes from ingested_files when storage_info_success is True instead of listing S3 raw/rum/, and running _fastly_api concurrently with repo.get_log_activity in /api/usage/log-activity keeps 30d log-activity under 10s on High-Scale.
