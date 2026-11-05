---
title: "Sessions Phase 4b ClickHouse Code 184, Rollup Total/Streaming & Stream CLS Fixes"
author: Drew
date: 2026-10-29
branch: v3.0.0-rc1
id: l-ff337374
scope: repo
status: active
src: agent
touches:
  - backend/high_scale/sessions.py
  - backend/repositories/sessions/rollup_reader.py
  - backend/repositories/sessions/queries.py
  - frontend/app/sessions/stream/page.tsx
---

Fixed ClickHouse Code 184 ILLEGAL_AGGREGATION in backend/high_scale/sessions.py (_SESSIONS_CTE nested aggregate alias in sessions_agg GROUP BY moved to flagged_sessions; _enrich_paginated_sessions SELECT aliases renamed to en_* so WHERE country/asn filters do not bind to argMin aggregates), removed 3-hour suffix pruning in backend/repositories/sessions/rollup_reader.py that truncated COUNT(*) OVER () total_sessions on 24h/7d/30d to 2 hours, enabled streaming_only=True (cmcd_count > 0) on the hourly sessions rollup reader, removed dead src.get('cache_dir') guard in backend/repositories/sessions/queries.py so _memo caches get_sessions and get_session_detail, and stabilized frontend/app/sessions/stream/page.tsx skeleton/alert ordering for zero CLS.
