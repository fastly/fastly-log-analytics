---
title: "FOS Class A/B operation reductions across log_flush, summary_bundle, and inline_summary_files"
author: Drew
date: 2026-11-03
branch: main
id: l-540c0732
scope: repo
status: active
src: agent
touches:
  - app/src/telemetry/flush.rs
  - app/src/ingest/bundle.rs
  - app/src/ingest/api.rs
  - app/web/lib/usage/estimate.ts
---

FOS Class A/B operation reductions: (1) Partition svc:_audit_outbox by hour in telemetry::flush::collect and allow check_due when cursor.behind is true even if rows == 0 so multi-hour (>MAX_GROUPS=6) svc audit backlogs drain across flushes instead of livelocking cursor.behind=true and writing a 3-row Parquet file every 2 minutes. (2) Skip redundant present(day) delimiter LIST in summary_bundle open_hour unless the closed-hour loop already populated self.present for that day (saves 720 Class A LISTs/day). (3) Resolve bundled and lookback hours in inline_summary_files (GET /api/files?summaries=1) directly from raw_objects + bundles so closed bundled hours require 0 LISTs on processed/summary/request/.
