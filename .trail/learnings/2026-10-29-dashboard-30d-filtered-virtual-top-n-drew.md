---
title: "Dashboard 30d filtered virtual Top-N pre-grouping, DuckDB UNION ALL LIMIT wrapping, and high-scale top_bots memoization"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-44adf902
scope: repo
status: active
src: agent
touches:
  - backend/high_scale/dashboard.py
  - backend/high_scale/security.py
  - backend/repositories/dashboard.py
  - backend/repositories/rollup_executor/top_n.py
  - backend/repositories/security/watchdog.py
  - tests/high_scale/test_query_service.py
  - tests/repositories/test_security.py
---

In dashboard 30d filtered scans, VIRTUAL_FIELD_EXPLODED_TOP_N should pre-group by backing_col with count(*) AS row_cnt before unnest(string_split(backing_col, ',')) (8.4x speedup), and execute_top_n_batch should use parenthesized (SELECT ... ORDER BY c DESC LIMIT N) branches wrapped in SELECT * FROM (... UNION ALL ...) ORDER BY field, c DESC instead of a second QUALIFY ROW_NUMBER() window function (which raises DuckDB Parser Error: exprLocation NOT IMPLEMENTED if not wrapped in an outer SELECT * FROM (...)). Also memoize HighScaleSecuritySubService.top_bots in _security_memo and normalize ClickHouse space-separated header_metrics extents via parse_iso_utc.
