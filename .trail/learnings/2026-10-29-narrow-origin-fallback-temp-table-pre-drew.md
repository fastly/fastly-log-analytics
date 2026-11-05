---
title: "Narrow /origin fallback temp table, pre-filter slow URLs before t-digest, and reserve ApproxBadge/DataTable height"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-46ed565f
scope: repo
status: active
src: agent
touches:
  - "backend/repositories/origin.py#get_aggregates"
  - "backend/routers/origin.py#origin_aggregates"
  - frontend/app/origin/_sections/Aggregates.tsx
  - frontend/app/origin/_sections/LatencyHeatmap.tsx
---

On /origin, 30d unfiltered and 7d/30d filtered requests were hitting 16-24s on remote-standard because (1) slow_urls fell back on >7d windows and get_aggregates materialized all 12 origin columns and all cache-HIT rows into t_origin_<uuid> even when missed == {'slow_urls'}, (2) SLOW_URLS computed MEDIAN and APPROX_QUANTILE(0.95/0.99) t-digest sketches for every unique URL before filtering by HAVING COUNT(*) >= min_requests, and (3) get_aggregates bypassed ResponseMemo. Pre-filtering URLs via a qualified CTE + SEMI JOIN (SLOW_URLS_QUALIFIED), narrowing t_origin_<uuid> columns/rows to missed sections (and filtering url IS NOT NULL AND lat_us IS NOT NULL when missed == {'slow_urls'}), adding ResponseMemo to get_aggregates, and reserving min-h-[24px] for ApproxBadge + min-h-[480px] on Origin DataTable cards fixes both latency and CLS.
