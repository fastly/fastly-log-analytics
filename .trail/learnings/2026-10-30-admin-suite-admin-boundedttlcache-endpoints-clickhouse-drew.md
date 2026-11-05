---
title: "Admin suite (/admin/*) BoundedTTLCache endpoints, ClickHouse server component, and zero-CLS reservations"
author: Drew
date: 2026-10-30
branch: v3.0.0-rc1
id: l-63be263f
scope: repo
status: active
src: agent
touches:
  - backend/cron/jobs/metric_snapshots.py
  - backend/routers/admin/health.py
  - backend/routers/admin/admin_queries.py
  - frontend/app/admin/clickhouse/page.tsx
  - frontend/app/admin/session-scoring/page.tsx
  - frontend/app/admin/queries/_sections/QueryMonitorClient.tsx
---

Because admin dashboards poll or composite-fetch metric_snapshots.get_batch, health_snapshot, api_celery_status, and count/list_persisted_slow_queries on every load, we cache them with short BoundedTTLCache instances (_BATCH_CACHE, _HEALTH_SNAPSHOT_CACHE, _CELERY_STATUS_CACHE, _SLOW_QUERIES_CACHE) registered in CacheRegistry and bypassed when PYTEST_CURRENT_TEST is set. On the frontend, ClickHouseAdminPage must be a Server Component in frontend/app/admin/clickhouse/page.tsx with dynamic='force-dynamic' because process.env.HIGH_SCALE_ENABLED is not inlined in client bundles, and /admin/rum + /admin/session-scoring must filter out activeServiceId === 'all' before querying service-scoped endpoints.
