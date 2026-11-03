---
title: clickhouse-cte-single-scan-and-bucketed-maps
author: Drew
date: 2026-11-03
branch: v3.0.0-rc2
cost: { minutes: 30, tokens: 29830 }
id: l-f60d7902
scope: repo
status: active
src: agent
touches:
  - backend/high_scale/insights/sql_queries.py
  - backend/high_scale/network.py
  - backend/high_scale/sql/request_schema.sql
  - backend/high_scale/postgres_control/control_plane.py
---

In ClickHouse, CTEs (`WITH x AS (...)`) are macro-expanded rather than materialized, so referencing `base` or an intermediate CTE in multiple `FROM`/`JOIN` clauses re-scans `request_facts` N times; in `backend/high_scale/insights/sql_queries.py`, keep each insight to a single scan of `base` (using `arrayReduce` over `arrayDifference(arraySort(groupUniqArray(sec)))` in `_beaconing` and conditional aggregates in `_per_ip_outlier` / `_share_spike`). Also keep `map_serialization_version = 'with_buckets'` (`max_buckets_in_map = 64`) on `request_facts` in `backend/high_scale/sql/request_schema.sql`, use `any(toFloat64OrNull(custom_fields['lat']))` instead of grouping by per-IP `lat, lon` in `backend/high_scale/network.py`, and use `statement_timestamp()` (STABLE) instead of `clock_timestamp()` (VOLATILE) in `backend/high_scale/postgres_control/control_plane.py` so PostgreSQL uses `high_scale_archive_manifests_domain_expiry_idx` instead of seq-scanning on every worker tick.
