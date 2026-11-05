---
title: "_captured_sleeps must filter out non-caller threads and call real time.sleep for background threads"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-0222e9dc
scope: repo
status: active
src: agent
touches:
  - backend/core/rollups/recompute.py
  - tests/utils/test_router_utils.py
---

## Decided
- In `_captured_sleeps` (`tests/utils/test_router_utils.py`), record `caller_tid = threading.get_ident()` and `real_sleep = time.sleep` before patching `time.sleep`, and have `_global_sleep` delegate to `real_sleep(seconds)` without recording whenever `threading.get_ident() != caller_tid`.
- In `_daily_covered_hours` (`backend/core/rollups/recompute.py`), use `parse_day_key_strict(day)` from `backend.utils.date_utils` instead of `datetime.fromisoformat(day).replace(tzinfo=UTC)` so `scripts/check_no_raw_dateparse.sh` stays within its 1-site ratchet baseline.

## Tried and ruled out
- Leaving `_captured_sleeps` with `only_from=None` patching `time.sleep` as a process-wide no-op across all threads: under `pytest -n auto` (`pytest-xdist`), background daemon threads left running in the worker process call `time.sleep(1.0)` in a loop, busy-spin without sleeping, and append hundreds of `1.0` entries to `delays`, failing `test_fos_secret_retries_write_write_conflict_with_rollback_each_attempt`.
