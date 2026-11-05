---
title: Daily boundary proof clips to st_floor and Dashboard reuses inclusive t_active_direct temp
author: Drew
date: 2026-11-02
branch: v3.0.0-rc1
id: l-f052411c
scope: repo
status: active
src: agent
touches:
  - CHANGELOG.md
  - backend/repositories/rollup_executor/exact_window.py
  - backend/repositories/_base.py
  - backend/repositories/rollup_executor/top_n.py
  - backend/repositories/dashboard.py
---

## Decided
- In `backend/repositories/rollup_executor/exact_window.py` (`minute_reference_proofs`), clip the daily `time_series.parquet` completeness window to `start = max(st_dt, day)` and `in_window_floor = max(day, st_floor)` so 30-day rolling retention pruning `[00:00, st_dt)` on the leading boundary day still allows completely idle in-window hours (`>= st_floor`) without demanding pruned pre-window hours (`< st_floor`).
- In `backend/repositories/_base.py` (`QueryRunner.begin_shared_active_hour_temps`, `shared_active_temp_for_point`), `backend/repositories/rollup_executor/top_n.py` (`_create_active_hour_temp_direct`, `_top_n_live_active_hour`, `_top_n_exact_window_rows`), and `backend/repositories/dashboard.py` (`get_aggregates`, `_compute_count`), pass `include_end=True` so `t_active_direct_<uuid>` materializes `timestamp <= end_time`. `_compute_count` queries `t_active_direct_<uuid>` for the exact endpoint count instead of scanning the full `logs_<svc>` view, and `_top_n_exact_window_rows` skips the duplicate `timestamp >= end_time AND timestamp <= end_time` `UNION ALL` branch in `verified_boundary_table`.

## Tried and ruled out
- Requiring all 24 hours in `daily_hours` whenever any in-window hour on a daily-reference boundary day is completely idle: fails on 30-day rolling windows (`start_time = now - 30d`) because retention legitimately prunes pre-window hours `[00:00, st_floor)` while in-window hours `[st_floor, 24:00)` can have quiet/idle hours.
- Setting `_last_active_end_covered = True` when `_create_active_hour_temp_direct` returns `"__empty__"` (no `buffer/`, `data/`, or `lake` table): synthetic unit tests that populate only an in-memory `CREATE TABLE logs_<svc>` without `buffer/` or `lake` rely on `verified_boundary_table` to read `timestamp == end_time`.
