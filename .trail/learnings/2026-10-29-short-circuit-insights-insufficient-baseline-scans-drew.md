---
title: Short-circuit /insights insufficient-baseline scans and pre-aggregate URLs before regex flags
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-c57873c2
scope: repo
status: active
src: agent
touches:
  - backend/repositories/insights/definitions/sql_coalesced.py
  - backend/repositories/insights/repository.py
  - backend/repositories/rollup_executor/top_n.py
  - backend/repositories/top_n_rollups.py
  - backend/routers/insights.py
  - frontend/components/Insights/index.tsx
  - tests/repositories/test_insights_golden.py
  - tests/repositories/test_top_n_rollups_golden.py
---

In get_insights, precheck min(timestamp) on unfiltered views and gate _coalesced_specs on available_history_hours >= baseline_hours so insufficient-baseline requests (e.g. 24h vs 30d with <30d retention or Analyst Path B clamped to 24h) do not materialize 30d or run 8 coalesced queries whose results check_baseline discards. In COALESCED_URL_EXTENDED_AGGREGATES, group by url first (HAVING COUNT(*) FILTER (WHERE is_w) > 0) and evaluate regexp_matches once per distinct window URL instead of scanning SELECT DISTINCT url and joining 2.2M base rows. Move the unavailable-insights Alert below InsightTabs in frontend/components/Insights/index.tsx so /api/insight-availability resolution does not shift the tab bar down (CLS 0.049 -> <0.01).
