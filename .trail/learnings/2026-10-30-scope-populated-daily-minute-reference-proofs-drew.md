---
title: Scope populated daily minute-reference proofs to in-window hours on 30d retention boundaries
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-aa4a1ec7
scope: repo
status: active
src: agent
touches:
  - "backend/repositories/rollup_executor/exact_window.py#minute_reference_proofs"
  - tests/repositories/test_top_n_rollups_golden.py
---

When a 30-day rolling retention window starts mid-day on a day older than 14 days (where hour_bundled has been retired and day_bundled/day=YYYY-MM-DD/time_series.parquet is the minute reference), any in-window hour with >=1 active minute (bool(hour_minutes)) or a day with all 24 hours populated in time_series.parquet (full_day_hours) proves its own [start, end) hour or requested minute directly against raw logs without demanding pre-retention [00:00, st_dt) raw logs from earlier on that day.
