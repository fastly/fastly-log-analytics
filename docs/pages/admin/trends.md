# System Trends (`/admin/trends`)

Sparklines of sampled system vitals: host load, memory and disk, active queries, and ingest lag.

Source: [`frontend/app/admin/trends/page.tsx`](../../../frontend/app/admin/trends/page.tsx) (a `force-dynamic` server wrapper that prefetches the default `1h` batch with `fetchMetricHistoryBatchServerSide` and dehydrates it) and `AdminTrendsSection.tsx` in [`frontend/app/admin/_sections/`](../../../frontend/app/admin/_sections/) (exported as `AdminTrendsClient`).

## UI

- Window selector: Last hour (`1h`), Last 6h, Last 24h, Last 7 days, plus Refresh. Switching the window keeps the sparklines mounted and dims them while refetching.
- Cards, each with a latest value: CPU load (1m avg, `cpu_load_1m`), Memory used (`mem_used_pct`), Data disk used (`disk_used_pct`), Boot disk used (`disk_used_pct_root`), Active queries (`active_query_count`), Ingest lag (`ingest_lag_s`, maximum across services per timestamp).

## Roles

Admin only (`require_admin`; `/api/admin/` is analyst-blocked). The endpoint works before any service exists.

## Backend

`GET /api/admin/metric-history/batch?since=<N>m|h|d` in [`backend/routers/admin/health.py`](../../../backend/routers/admin/health.py) (default `1h`, capped at 30 days; an invalid window is `400`). It returns `MetricHistoryBatchResponse`: `series`, a map from series key to ascending `[{ts, value}]` points. Keys are `<metric>` for global series, `<metric>|<service_id>` for per-service series (`ingest_lag_s`) and `<metric>|<service_id>|<task>` for per-task series such as `cron_duration_ms`.

## Tests

- E2E: `/admin/trends — Operational Trends & Health Sparklines Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/cron/test_metadata_jobs.py`](../../../tests/cron/test_metadata_jobs.py), [`tests/routers/test_admin_system_and_streams.py`](../../../tests/routers/test_admin_system_and_streams.py)
