# Admin Overview (`/admin`)

Admin landing page: operational status cards, service management, system health, ingest quarantine and global settings.

Source: [`frontend/app/admin/page.tsx`](../../../frontend/app/admin/page.tsx) (`force-dynamic`) and [`frontend/app/admin/_sections/`](../../../frontend/app/admin/_sections/) (`AdminOverviewCards.tsx`, `AdminServicesTable.tsx`, `AdminGlobalSettings.tsx`, `OperationsOverview.tsx`). The System Health card is [`frontend/components/SystemHealthCard.tsx`](../../../frontend/components/SystemHealthCard.tsx); the quarantine section is `QuarantineSection` in `frontend/app/control-room/_sections/ControlRoomClient.tsx`.

## Roles

Admin only. Analysts are blocked at the API (`/api/admin/`, `/api/provision/` are in `_ANALYST_BLOCKED_PREFIXES`) and the UI redirects them away from `/admin`. The same applies to every page under [`docs/pages/admin/`](.).

## UI

- Header links to the other admin pages: Share Dashboard, Session Scoring, Live Queries, Trends, Queue, RUM, and ClickHouse (shown only when a service is high-scale). Hovering a link prefetches that page's data.
- Operations cards: Live Queries, Ingest Gap (log accounting), Notable Slow Queries, Workers & Queues. Each links to its detail page and polls about every 10 seconds; the first paint is seeded from the bootstrap payload.
- Service Management table: name, ID, FOS bucket, local cache size (with ZIP download), cron sync status and settings, and per-service actions (credentials rotation, NGWAF workspace, delete data, teardown). Add Service opens the provisioning wizard (see [`frontend/components/ProvisionWizard/`](../../../frontend/components/ProvisionWizard/)); Refresh Status refetches services and health.
- System Health card (process, host and pool vitals).
- Quarantine section for unparseable log lines.
- Global settings: debug-response visibility, PoP location refresh, and Pricing & Retention Defaults (the rates the [usage page](../usage-and-cost.md) uses).
- An empty install (no service yet) still renders; the global endpoints work without a service.

## Backend

Admin routes are in [`backend/routers/admin/`](../../../backend/routers/admin/) (prefix `/api`); service routes in [`backend/routers/services/`](../../../backend/routers/services/); provisioning in [`backend/routers/provision.py`](../../../backend/routers/provision.py).

| Area | Endpoints |
|---|---|
| Sync and storage state | `GET /api/admin/sync-status` and `GET /api/sync-status` (`sync_status.py`); `GET /api/admin/compaction-status[/{service_id}]` |
| Health | `GET /api/admin/health-snapshot` (aliases `/system-metrics`, `/health`), `GET /api/admin/metric-history/batch`, `GET /api/admin/duckdb/status`, `POST /api/admin/duckdb/recycle` (`health.py`) |
| Log accounting | `GET /api/admin/log-accounting[/{service_id}]`: compares Fastly's per-bucket log-emission counter with ingested rows and reports a sustained-loss alert after 2 or more consecutive completed buckets at or above 5% deficit |
| Operations cards | `GET /api/admin/queries/summary`, `GET /api/admin/slow-queries/count`, `GET /api/admin/celery/status` |
| Manual jobs (202 + SSE progress) | `POST /api/admin/sync/{service_id}`, `/commit/{service_id}`, `/compact/{service_id}`, `/optimize/{service_id}`, `/full-sweep/{service_id}`, `/gap-heal/{service_id}`, `/ledger/sweep/{service_id}`, `/rum/sync/{service_id}`, `/expire-snapshots/{service_id}`, `/metadata-cleanup[/{service_id}]` |
| Quarantine | `GET /api/admin/quarantine`, `/quarantine/summary`, `/quarantine/download/{item_id}`, `POST /api/admin/quarantine/purge` (`quarantine.py`) |
| Services | `GET /api/services`, `PATCH /api/services/{service_id}/credentials`, `POST /api/services/{service_id}/cron-settings`, `/api/provision/*` (validate, execute, teardown, ngwaf-workspaces) |
| Global settings | `GET`/`PATCH /api/admin/debug-settings`, `GET`/`PATCH /api/admin/usage-logging`, `GET /api/admin/pop-locations`, `POST /api/admin/pop-locations/refresh` |

Operational metadata is PostgreSQL only ([ADR-22](../../adr/22-postgres-only-metadata.md)). Job definitions and schedules are documented in [docs/cron](../../cron/README.md).

## Tests

- Frontend: [`frontend/__tests__/app/admin-overview.test.tsx`](../../../frontend/__tests__/app/admin-overview.test.tsx)
- E2E: `Admin Overview & Ingest Operations Page Contract` and the provision/teardown tests in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/routers/test_admin_ops_and_storage.py`](../../../tests/routers/test_admin_ops_and_storage.py), [`tests/routers/test_admin_system_and_streams.py`](../../../tests/routers/test_admin_system_and_streams.py), [`tests/routers/test_provision.py`](../../../tests/routers/test_provision.py)
