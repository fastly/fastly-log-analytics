---
title: Router SQL boundary and repository metric coalesce ratchets
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-5cbf460f
scope: repo
status: active
src: agent
touches:
  - backend/core/metadata/__init__.py
  - backend/core/metadata/ingest_log.py
  - backend/repositories/assets.py
  - backend/routers/usage.py
  - tests/architecture/test_import_boundaries.py
  - scripts/check_no_inline_metric_coalesce.sh
---

## Decided
- Keep RUM `ingested_files` window byte queries in `backend/core/metadata/ingest_log.py:get_rum_storage_stats_window` and call it from `backend/routers/usage.py` instead of calling `con.execute(...)` in the router, because `tests/architecture/test_import_boundaries.py:test_no_inline_sql_in_routers` forbids `.execute(` / `SELECT` in `backend/routers/*.py` outside the 4 exempted routers.
- Use `sum_or_zero(...)` from `backend.core.metric_expressions` in `backend/repositories/assets.py:_ALERT_ROLLUP_METRICS` instead of literal `COALESCE(SUM(...), 0)`, because `scripts/check_no_inline_metric_coalesce.sh` ratchets literal `COALESCE(` occurrences in `backend/repositories/*.py`.

## Tried and ruled out
- Running `con.execute("SELECT coalesce(sum(file_size_bytes), 0) ...")` directly inside `backend/routers/usage.py`: failed `test_no_inline_sql_in_routers`.
- Writing literal `COALESCE(SUM(...), 0)` in `backend/repositories/assets.py:_ALERT_ROLLUP_METRICS`: exceeded the baseline of 2 in `scripts/check_no_inline_metric_coalesce.sh`.
