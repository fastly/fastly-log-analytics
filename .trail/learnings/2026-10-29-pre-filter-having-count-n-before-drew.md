---
title: "Pre-filter HAVING count(*) > N before DuckDB approx_quantile on high-cardinality groups"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-4d9a29cc
scope: repo
status: active
src: agent
touches:
  - "backend/repositories/performance.py#_top_latency_sql"
  - "backend/high_scale/performance.py#_ttl_dist"
  - frontend/app/performance/_sections/PerformanceClient.tsx
---

In DuckDB, GROUP BY queries with approx_quantile(...) and HAVING count(*) > N allocate and populate per-group t-digest sketches for every distinct group BEFORE HAVING discards low-count/singleton groups. On high-cardinality columns like url (~96% singleton rows), pre-filtering qualified groups via WITH qualified AS (SELECT col FROM tbl GROUP BY col HAVING count(*) > N) + SEMI JOIN + a single list sketch approx_quantile(col, [0.5, 0.95, 0.99]) in backend/repositories/performance.py#_top_latency_sql cuts 500k-row execution from 37.7s to 80ms (468x speedup). Similarly, in backend/high_scale/performance.py#_ttl_dist, pre-grouping by distinct TTL string before toFloat64OrNull and _ttl_bucket_expr avoids evaluating the 11-way CASE over 432,000 minute aggregate rows.
