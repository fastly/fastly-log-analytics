# System Trends (`/admin/trends`)

Sparklines of sampled system vitals: host load, memory and disk, active queries, DuckDB pool wait and ingest lag.

Source: [`frontend/app/admin/trends/page.tsx`](../../../frontend/app/admin/trends/page.tsx) (a `force-dynamic` server wrapper that prefetches the default `1h` batch with `fetchMetricHistoryBatchServerSide` and dehydrates it) and `AdminTrendsSection.tsx` in [`frontend/app/admin/_sections/`](../../../frontend/app/admin/_sections/) (exported as `AdminTrendsClient`).

## UI

- Window selector: Last hour (`1h`), Last 6h, Last 24h, Last 7 days, plus Refresh. Switching the window keeps the sparklines mounted and dims them while refetching.
- Seven cards, each with a latest value: CPU load (1m avg, `cpu_load_1m`), Memory used (`mem_used_pct`), Data disk used (`disk_used_pct`), Boot disk used (`disk_used_pct_root`), Active DuckDB queries (`active_query_count`), Pool wait p95 (`pool_wait_p95_ms`, maximum across services per timestamp), Ingest lag (`ingest_lag_s`, maximum across services per timestamp).
- Header text: "Sampled every 60 s. Retained 30 days."

## Roles

Admin only (`require_admin`; `/api/admin/` is analyst-blocked). The endpoint works before any service exists.

## Backend

`GET /api/admin/metric-history/batch?since=<N>m|h|d` in [`backend/routers/admin/health.py`](../../../backend/routers/admin/health.py) (default `1h`, capped at 30 days; an invalid window is `400`). It returns `MetricHistoryBatchResponse`: `series`, a map from series key to ascending `[{ts, value}]` points. Keys are `<metric>` for global series, `<metric>|<service_id>` for per-service series (`pool_wait_p95_ms`, `ingest_lag_s`) and `<metric>|<service_id>|<task>` for per-task series such as `cron_duration_ms`. High-scale deployments additionally record Celery queue depth and active tasks (used by [queue.md](queue.md)).

Points come from the PostgreSQL `metric_snapshots` table ([ADR-22](../../adr/22-postgres-only-metadata.md)), written every 60 seconds by the process-global `metric_snapshot` cron ([`backend/cron/jobs/metric_snapshots.py`](../../../backend/cron/jobs/metric_snapshots.py), [spec](../../cron/jobs/metric-snapshot.md)) and pruned by `metadata_cleanup`.

## Tests

- E2E: `/admin/trends — Operational Trends & Health Sparklines Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/cron/test_metadata_jobs.py`](../../../tests/cron/test_metadata_jobs.py), [`tests/routers/test_admin_system_and_streams.py`](../../../tests/routers/test_admin_system_and_streams.py)
