# Task Queue (`/admin/queue`)

Operational view of background work: ingest ledger state, Celery queues and workers (high-scale), the in-process scheduler (standard), and one-hour trend sparklines.

Source: [`frontend/app/admin/queue/`](../../../frontend/app/admin/queue/) (`page.tsx`, `_sections/QueueOverviewPanels.tsx`, `QueueWorkerAndSchedulePanels.tsx`, `QueueLedgerInspector.tsx`).

## UI

- Header: a broker badge (`Broker Reachable`, or `Standard Mode · In-Process Scheduler` when no Celery broker is configured), Back to Admin, and a Refresh action that refetches status and metric history. Live updates also arrive on the `celery-status` channel of the multiplexed admin stream (`GET /api/admin/events/stream`).
- Standard mode: Standard Ingest Summary, In-Process Scheduler Overview, Active In-Process Jobs, and the quarantined files and corrupt lines count.
- High-scale mode: Total Queue Depth (1h) and Active Tasks (1h) sparklines, a Queue Depths table over the known queues (`celery`, `q.ingest`, `q.control`) plus any other `q.*` queue found, and Active Workers with their in-flight tasks.
- Ingest Ledger Status: counts per ledger state (`discovered`, `claimed`, `committed`, `quarantined`, `dead_letter`) summed across services, with an inspector that lists problem rows and can retry or purge a selection or all rows.
- Quarantine section (same component as the admin overview).

## Roles

Admin only (`/api/admin/` is analyst-blocked and every route here uses `require_admin`). The routes also work before any service exists.

## Backend

Routes in [`backend/routers/admin/health.py`](../../../backend/routers/admin/health.py); models in [`backend/models/admin/high_scale.py`](../../../backend/models/admin/high_scale.py).

- `GET /api/admin/celery/status` (`CeleryStatusResponse`): `deployment_mode`, `broker_reachable`, `workers`, `queues` (name to depth), `ledger` (status to count), `inprocess_schedules`, `inprocess_active_runs`, `standard_ingest`. Without `CELERY_BROKER_URL` it returns immediately with `broker_reachable: false`.
- `GET /api/admin/celery/ledger` (`status`: `problem` default, `all`, `dead_letter`, `quarantined`, `discovered`, `claimed`, `committed`; optional `service_id`; `limit` 1 to 500; `offset`).
- `POST /api/admin/celery/ledger/retry` and `POST /api/admin/celery/ledger/purge` (`IngestLedgerActionRequest`).
- `GET /api/admin/metric-history/batch?since=1h`: sparkline series sampled into PostgreSQL `metric_snapshots` by the `metric_snapshot` cron.

Ledger counts come from the PostgreSQL `ingest_ledger` table ([ADR-16](../../adr/16-ingest-ledger.md)), scoped per service so a multi-service deployment never multiplies counts. See [docs/cron](../../cron/README.md) for the ledger sweep jobs.

## Tests

- E2E: `Admin Task Queue & Ingest Ledger Page Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/routers/test_admin_system_and_streams.py`](../../../tests/routers/test_admin_system_and_streams.py)
