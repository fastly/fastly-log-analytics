# SQL Query Pad (`/query`)

Read-only log explorer with two modes: a structured mode driven by the global filter bar with server-side column sorting, and a raw SQL mode with a schema sidebar.

Source: [`frontend/app/query/`](../../frontend/app/query/) (`page.tsx`, `_sections/QueryClient.tsx`, `QueryEditorSection.tsx`, `QueryToolbarAndResults.tsx`). The editor is CodeMirror with SQL highlighting.

## URL parameters

| Parameter | Meaning |
|---|---|
| `q` | SQL text; its presence switches the page to raw SQL mode. |
| `mode` | `raw` opens raw SQL mode; the mode toggle writes this back. |
| `start_time`, `end_time`, `filters` | Window and filters handed off from other pages (the Dashboard's "See Raw Logs" button). |

Raw mode starts with `SELECT * FROM logs LIMIT 100`. Query history is kept in browser storage.

## UI

- Toolbar: dataset selector (`logs`, `client_vitals`, `client_errors`), row-limit selector (up to 10,000), Explain toggle, preset queries, history, Run, CSV export (client-side from the loaded rows).
- Schema sidebar (raw mode) from `GET /api/query/schema/{service_id}`; clicking a column inserts it.
- Results table is virtualised. Errors render in a destructive alert. While the engine or a query is running the page shows reserved skeleton states.

## Roles

| Role | Access |
|---|---|
| Admin | Full read-only SQL. |
| Analyst (remote share) | `POST /api/query` is allowed. Time bounds are clamped to the invite and IP columns are masked server-side (`mask_ips`). In high-scale mode analysts only get the console through a per-service, least-privilege ClickHouse identity; without `CLICKHOUSE_CONSOLE_SECRET` the API returns 403 `console_unavailable_for_analysts` ([runbook](../runbooks/high-scale-analyst-console.md)). |

## Backend

Routes in [`backend/routers/query.py`](../../backend/routers/query.py); models in [`backend/models/analytics/dashboard.py`](../../backend/models/analytics/dashboard.py).

- `POST /api/query` (`QueryRequest`: `sql`, `dataset`, `max_rows` 1 to 10,000 default 500, `explain`) returns `QueryResponse` (`columns`, `data`, `row_count`, `total_rows`, `truncated`, `elapsed_ms`, `explain_plan`). Empty SQL is 400 `empty_sql`.
- `GET /api/query/schema/{service_id}`: table and column definitions for the service (falls back to a small default column list when no schema is known). `GET /api/schema` and `GET /api/presets` (curated `PresetQuery` list) support the page.

Validation ([`backend/core/sql_validator.py`](../../backend/core/sql_validator.py), `validate_user_sql`): the statement is parsed with DuckDB's own parser and only `SELECT` and set-operation nodes are accepted, with input size limits, a NUL-byte check and table/function allow-listing; a rejection is 403 `sql_not_permitted`. Standard mode runs on a pooled DuckDB connection with the stale-view retry; high-scale mode runs against ClickHouse, where the user SQL is never spliced with scope: service, publication and window scope are sent as server-enforced `additional_table_filters` ([AGENTS.md](../../AGENTS.md), High-scale readers section).

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/security/test_no_raw_exception_in_responses.py`](../../tests/security/test_no_raw_exception_in_responses.py), [`tests/high_scale/test_query_service.py`](../../tests/high_scale/test_query_service.py)
- E2E: `SQL Query Pad Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts) (schema reflection, valid query, rejected `DROP TABLE`, CSV export, `?mode=raw` round trip)
