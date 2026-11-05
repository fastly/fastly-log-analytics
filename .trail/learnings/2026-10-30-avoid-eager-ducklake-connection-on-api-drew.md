---
title: Avoid eager DuckLake connection on /api/schema and prevent Export toolbar shift on /query
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-bc61f133
scope: repo
status: active
src: agent
touches:
  - backend/routers/bootstrap.py
  - backend/core/duckdb_status.py
  - backend/repositories/query.py
  - backend/high_scale/query.py
  - frontend/app/query/_sections/QueryToolbarAndResults.tsx
  - frontend/app/query/_sections/QueryClient.tsx
---

In backend/routers/bootstrap.py, schema_endpoint and insight_availability declared Depends(get_con) in their FastAPI route signatures, which eagerly opened a DuckDB connection (and on durable/high-scale attached Postgres DuckLake + S3 secrets for ~450ms) BEFORE checking cached_status or _schema_cache, and refresh_config_status(include_top_values=False) dropped status['schema'] on fast ticks. Resolve schema lazily via _resolve_schema_for_source (falling back to log_fields_catalog in durable mode), cache recent query executions for 15s, and move the conditional Export button in QueryToolbar to the left-aligned group so Run Query results do not shift Plan and Row limit.
