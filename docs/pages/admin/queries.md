# Live Query Monitor (`/admin/queries`)

Real-time view of executing and recently completed queries across DuckDB, PostgreSQL and ClickHouse, with caller attribution (analyst, admin, cron, system), a persisted slow-query history, and a cancel action.

Source: [`frontend/app/admin/queries/`](../../../frontend/app/admin/queries/) (`_sections/QueryMonitorClient.tsx`, `QueryMonitorComponents.tsx`, `QueryMonitorSections.tsx`, `QueryMonitorDialogs.tsx`, `useQueryMonitorState.ts`).

## URL parameters

All are optional and cleared when at their default: `q` (text filter), `kind` (`all`, `analyst`, `admin`, `cron`, `system`), `view` (`all`, `live`, `past`), `slow` (slow-query threshold in ms), `db` (`all`, `DuckDB`, `Postgres`, `ClickHouse`), `nogroup` (disable grouping). The Admin overview's slow-queries card links here with `?view=past&slow=...`.

## UI

- Active queries with live durations, attribution and caller; a Cancel action ("Cancelling…" while pending).
- Recently completed queries (briefly kept as faded rows) and a Notable Slow Queries panel with a selectable history window and threshold, plus a text report export.
- Row click opens a SQL detail dialog (full SQL via `GET /api/admin/queries/{qid}`, copy to clipboard).
- Polling can be paused and resumed (keyboard shortcut `.`); it also pauses while the tab is hidden. A keyboard-shortcuts dialog is available.

## Roles

Admin only (`require_admin` on every route; `/api/admin/` is analyst-blocked).

## Backend

Routes in [`backend/routers/admin/admin_queries.py`](../../../backend/routers/admin/admin_queries.py); the registry is [`backend/core/request_telemetry/query_registry.py`](../../../backend/core/request_telemetry/query_registry.py).

- `GET /api/admin/queries` (`SnapshotResponse`): active queries plus completed entries (`since_seq`, `include_completed`, `cold_limit`).
- `GET /api/admin/queries/summary`: counts for badges (active by engine, slow count, latest id).
- `GET /api/admin/queries/{qid}`: one query with its full SQL.
- `POST /api/admin/queries/{qid}/cancel` (`CancelResponse`): interrupts a running DuckDB statement; rate limited per admin (429 `cancel_rate_limit_exceeded`) and recorded as a `query_cancelled` audit event for the query's service. ClickHouse entries carry no connection and cannot be interrupted from here.
- `GET /api/admin/slow-queries` (`since_hours`, `threshold_ms`, `kind`, `db_type`, `sort`, `limit`), `GET /api/admin/slow-queries/count`, `DELETE /api/admin/slow-queries`: persisted history from the PostgreSQL `slow_queries` table.
- `GET /api/admin/app-config/query-monitor`: slow-query threshold and poll interval.

`QUERY_MONITOR_ENABLED=0` makes these endpoints return 404; `QUERY_REGISTRY_DISABLED=1` bypasses the registry hot path entirely. The registry hooks the PostgreSQL cursor wrapper, the DuckDB connection wrapper and the ClickHouse client; DuckDB queries deregister when their result is fully fetched, not when `execute()` returns.

## Tests

- E2E: `Admin Live Query Monitor Page Contract` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../../frontend/e2e/pages/admin-overview.spec.ts)
- Backend: [`tests/routers/test_admin_ops_and_storage.py`](../../../tests/routers/test_admin_ops_and_storage.py), [`tests/core/test_metrics_and_telemetry.py`](../../../tests/core/test_metrics_and_telemetry.py)
