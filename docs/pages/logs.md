# Data Management (`/logs`)

Admin hub for one service's ingestion pipeline: cron schedule and run history, configuration audit history, ingested files, DuckLake storage, PostgreSQL metadata footprint, raw FOS browser and the logs-table schema. Raw log exploration lives on [`/query`](query.md) and [`/high-scale/request-facts`](high-scale-request-facts.md).

Source: [`frontend/app/logs/`](../../frontend/app/logs/): `page.tsx` is a server wrapper (`force-dynamic`) that prefetches the 500-row cron-runs slice and the cron schedule and dehydrates them into the keys the client uses (`LogsClient` in `_sections/`); `loading.tsx` reserves the header, quick-actions bar, tab list and table. Page state and polling are in `_sections/useLogsPageState.ts`.

## URL parameters

`service` (target service for the SSR seed) and `tab` (`cron` default, `service_history`, `ingestion`, `storage`, `metadata_storage`, `raw`, `schema`).

## Roles

Admin only. The sidebar entry is hidden for share analysts, a direct visit is redirected by `AppLayout`, and the API surface is blocked for analysts by `_ANALYST_BLOCKED_PREFIXES` / `_ANALYST_BLOCKED_SUBPATHS` in [`backend/utils/remote_access.py`](../../backend/utils/remote_access.py) (`/api/admin/`, `/api/cron-runs`, `/api/audit-logs`, `/api/cron-schedule`, `/api/download*` return `403 admin_only`).

## Quick actions

Import Logs (manual `log_discovery`), Backfill Logs (opens a date-range dialog), Commit Buffer, NGWAF Bot Sync (only when the service has an NGWAF workspace) and View Recent Logs. Long operations stream into an SSE console and a floating operations dock that survives tab switches.

## Tabs

1. **Cron Runs**: collapsible schedule overview plus the `cron_runs` table filterable by task and status, a live elapsed counter for running jobs, and Purge History.
2. **Service History**: configuration and lifecycle audit events (`audit_logs`).
3. **Ingestion History**: filterable list of `ingested_files` (read-only).
4. **DuckLake Storage**: storage status, snapshot calendar and a Parquet file browser (lazy-mounted).
5. **Metadata Storage**: PostgreSQL table sizes and row counts, retention windows and a Run Cleanup Now action (lazy-mounted).
6. **Available Logs**: hierarchical browser over the FOS `raw/` prefix with ZIP and single-file download (lazy-mounted).
7. **DuckDB Schema**: columns and types of the active `logs` view with search (lazy-mounted).

The Cron Runs tab delta-polls `GET /api/cron-runs?since_id=` (15 s, faster while a job is running) and merges rows in place.

## Backend

| Endpoint | Purpose |
|---|---|
| `GET /api/cron-runs`, `DELETE /api/cron-runs`, `DELETE /api/cron-runs/{log_id}` | Run history (`task`, `status`, `page`, `per_page`, `sort`, `dir`, `since_id`, `skip_total`); purge. In [`backend/routers/services/views.py`](../../backend/routers/services/views.py). |
| `GET /api/cron-runs/{run_id}/stream` | SSE for a manual run ([`backend/routers/services/crud.py`](../../backend/routers/services/crud.py)). |
| `GET /api/cron-schedule` | Configured intervals and next runs per service (same file). |
| `GET /api/audit-logs` | Paged audit events (`event_type`, `page`, `per_page`, `sort`, `dir`), `views.py`. |
| `GET /api/admin/ingested-files` | Ingested file list ([`backend/routers/admin/sync_status.py`](../../backend/routers/admin/sync_status.py)). |
| `GET /api/schema` | Columns and types of the logs table ([`backend/routers/bootstrap.py`](../../backend/routers/bootstrap.py)). |
| `GET /api/admin/storage-info`, `/storage-calendar`, `/compaction-status`, `/metadata-storage`; `POST /api/admin/metadata-cleanup`, `/commit-storage` | Storage inspection and actions ([`backend/routers/admin/compaction.py`](../../backend/routers/admin/compaction.py)). |
| `GET /api/admin/storage-tree`, `/raw-tree` | File browsers ([`backend/routers/admin/downloads.py`](../../backend/routers/admin/downloads.py)). |
| `POST /api/admin/ingest-logs` | Manual import or ranged backfill ([`backend/routers/admin/ingest.py`](../../backend/routers/admin/ingest.py)). |
| `POST /api/services/{service_id}/ngwaf-sync` | NGWAF bot sync (SSE). |

All operational metadata (`cron_runs`, `audit_logs`, `ingested_files`, `usage_log`, ...) is in PostgreSQL ([ADR-22](../adr/22-postgres-only-metadata.md)). Job definitions and schedules are documented in [docs/cron](../cron/README.md).

## Tests

- Backend: [`tests/routers/test_admin_ops_and_storage.py`](../../tests/routers/test_admin_ops_and_storage.py), [`tests/routers/test_admin_system_and_streams.py`](../../tests/routers/test_admin_system_and_streams.py), [`tests/cron/test_sync_job.py`](../../tests/cron/test_sync_job.py)
- E2E: `Data Management Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts)
