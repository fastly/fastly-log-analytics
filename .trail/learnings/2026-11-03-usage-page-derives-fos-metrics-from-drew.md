---
title: Usage page derives FOS metrics from cron_runs when dev_telemetry is disabled
author: Drew
date: 2026-11-03
branch: main
id: l-e209219b
scope: repo
status: active
src: agent
touches:
  - app/src/telemetry/mod.rs
  - app/src/telemetry/flush.rs
  - app/web/lib/usage/summary.ts
  - app/web/lib/usage/series.ts
---

On wasm32 production builds, dev_telemetry_available defaults to false so fos_usage and api_telemetry streams are not written; /usage must synthesize FOS ops, storage dataset deltas, and timeline series from cron_runs via deriveFosUsageFromCronRuns, and read_live/log_flush_reserving must skip scanning Core Cache keys for disabled dev-telemetry kinds.
