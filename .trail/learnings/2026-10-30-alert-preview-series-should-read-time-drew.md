---
title: Alert preview series should read time_series.parquet rollups and skip DuckDB checkout on 0 enabled alerts
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-474f8161
scope: repo
status: active
src: agent
touches:
  - backend/repositories/assets.py
  - backend/routers/alerts.py
  - frontend/app/alerts/_sections/AlertsClient.tsx
  - frontend/app/alerts/loading.tsx
  - tests/routers/test_analytics_routers.py
---

## Decided
- In `backend/routers/alerts.py`, use `_acquire_alert_con(request, src)` so `evaluate_alerts_endpoint` skips DuckDB connection checkout when 0 alerts are enabled and `preview_alert` checks `repo.get_cached_alert_preview(...)` before opening a connection, while still honoring `request.app.dependency_overrides.get(get_con)` in unit tests.
- In `backend/repositories/assets.py`, serve `preview_alert_series` (`PREVIEW_SERIES`, `PREVIEW_BASELINE`, `PREVIEW_HISTORY`) from `time_series.parquet` rollups via `collect_day_prefer_paths` when `eval_scope == "all"` and `metric` is in `_ALERT_ROLLUP_METRICS`, and cache preview responses in `_ALERT_PREVIEW_CACHE` (30s TTL).
- In `frontend/app/alerts/_sections/AlertsClient.tsx`, fix the `Last Checked` badge width (`w-[290px]`) and page container min-height (`min-h-[820px]`) so hydration does not shift `PageHeader` actions or `FOOTER`.
