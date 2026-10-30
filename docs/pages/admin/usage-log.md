# FOS Usage Log (`/admin/usage-log`)

Per-operation ledger of every Fastly Object Storage (Class A and Class B) and CDN download call the system makes, attributed to the API route, cron job or worker that made it, with estimated cost and an ingest-accounting panel. It answers where storage and API spend comes from.

Source: [`frontend/app/admin/usage-log/`](../../../frontend/app/admin/usage-log/) (`page.tsx`, `loading.tsx`, `_sections/UsageLogClient.tsx`, `UsageLogTableAndCards.tsx`, `LogAccountingPanel.tsx`).

## UI

- Toolbar: Refresh, Purge Logs (confirmation dialog) and Export CSV.
- KPI cards: FOS Class A Ops, FOS Class B Ops, CDN Egress, and estimated cost.
- Ingest accounting panel (`GET /api/admin/log-accounting`): Fastly's per-bucket log-emission counter against ingested rows, with a sustained-loss indicator.
- Filters: time presets (Last 1h, 24h, 7d, 30d), usage type (all, FOS Class A, FOS Class B, CDN), operation type, and process context (for example `cron:sync`).
- Ledger table. The page loads a small head slice with aggregates first and then a 500-row slice in the background; refetches keep the previous data on screen and dim it.

## Roles

Admin only (`require_admin` on every route; `/api/admin/` is analyst-blocked).

## Backend

Routes in [`backend/routers/admin/admin_queries.py`](../../../backend/routers/admin/admin_queries.py) (usage log, usage-logging settings and system jobs) and [`backend/routers/admin/log_accounting.py`](../../../backend/routers/admin/log_accounting.py); models in [`backend/models/admin/health_and_sync.py`](../../../backend/models/admin/health_and_sync.py) and [`operations.py`](../../../backend/models/admin/operations.py).

- `GET /api/admin/usage-log` (`UsageLogResponse`): query `service_id`, `start`, `end`, `usage_type`, `process_context`, `operation_type`, `page`, `page_size` (1 to 1000, default 100). Returns `entries[]` (`id`, `timestamp`, `operation_class`, `operation_type`, `url`, `bytes`, `duration_ms`, `function_name`, `process_context`, `status`, estimated cost), `total`, and `aggregate` (`total_class_a`, `total_class_b`, `total_cdn_downloads`, `total_cdn_bytes`, `total_fos_bytes`, estimated costs per class and in total, class A/B breakdowns by operation).
- `GET /api/admin/usage-log/export`: CSV stream with the same filters and `max_rows` (default 100,000, up to 1,000,000).
- `DELETE /api/admin/usage-log`: deletes all usage-log rows for the service (the durable hourly summary table is separate).
- `GET`/`PATCH /api/admin/usage-logging`: global config (`enabled`, `retention_days`, `class_a_rate_per_1k`, `class_b_rate_per_10k`, `cdn_egress_rate_per_gb`, `storage_rate_per_gb_month`, `min_billed_days`, `track_duckdb_httpfs`, `billing_zone`); PATCH validates numeric ranges and the zone.
- `GET /api/admin/system-jobs`: schedules and latest status of the process-global jobs (bot data refresh, rDNS enrichment, share audit purge).
- `GET /api/admin/log-accounting[/{service_id}]`: accounting buckets (see [overview.md](overview.md) for the sustained-loss rule).

Every FOS and CDN call is recorded with a process context set by the caller (for example `cron:sync:<service>` or `api:GET /api/...`); entries are buffered in memory and flushed to PostgreSQL `usage_log`, with compact rollups kept in `usage_log_hourly_summary` ([`backend/core/metadata/usage_log.py`](../../../backend/core/metadata/usage_log.py), [`fos_usage.py`](../../../backend/core/metadata/fos_usage.py)). Costs are computed at query time from the configured rates, so changing a rate re-prices history. Detailed rows are pruned by the `metadata_cleanup` cron after `retention_days` while the hourly summary is kept ([spec](../../cron/jobs/metadata-cleanup.md)).

## Tests

- E2E: `/admin/usage-log — Admin FOS Usage & Class A/B Ledger Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/core/test_metadata_db.py`](../../../tests/core/test_metadata_db.py), [`tests/routers/test_admin_ops_and_storage.py`](../../../tests/routers/test_admin_ops_and_storage.py)
