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
| Analyst (remote share) | `POST /api/query` is allowed. Time bounds are clamped to the invite and IP columns are masked server-side (`mask_ips`). Analysts only access the console through a per-service, least-privilege ClickHouse identity; without `CLICKHOUSE_CONSOLE_SECRET` the API returns 403 `console_unavailable_for_analysts` ([runbook](../runbooks/high-scale-analyst-console.md)). |

## Backend

Routes in [`backend/routers/query.py`](../../backend/routers/query.py); models in [`backend/models/analytics/dashboard.py`](../../backend/models/analytics/dashboard.py).

- `POST /api/query` (`QueryRequest`: `sql`, `dataset`, `max_rows` 1 to 10,000 default 500, `explain`) returns `QueryResponse` (`columns`, `data`, `row_count`, `total_rows`, `truncated`, `elapsed_ms`, `explain_plan`). Empty SQL is 400 `empty_sql`.
- `GET /api/query/schema/{service_id}`: table and column definitions for the service (falls back to a small default column list when no schema is known). `GET /api/schema` and `GET /api/presets` (curated `PresetQuery` list) support the page.

Validation ([`backend/core/sql_validator.py`](../../backend/core/sql_validator.py), `validate_user_sql`): the statement is parsed into an AST and only `SELECT` and set-operation nodes are accepted, with input size limits, a NUL-byte check and table/function allow-listing; a rejection is 403 `sql_not_permitted`. Validated queries execute against ClickHouse, where the user SQL is never spliced with scope: service, publication and window scope are sent as server-enforced `additional_table_filters` ([AGENTS.md](../../AGENTS.md), ClickHouse readers section).

## Cloud

The selected service and time window travel alongside SQL as file-loading scope;
explicit request bounds take precedence. Structured queries use half-open
`[start, end)` bounds. Raw SQL keeps its own predicates: Cloud does not rewrite
the user's comparison operators or inject a time condition, apart from the
newest-window scope described below. Discovery
conservatively includes the upper-endpoint file and rounds outward to native
second precision; SQL still decides which loaded rows match.

SQL reads complete normalized views over cached Parquet bytes, rather than
full-width decoded copies. The selected service, file versions and loaded range
stay bound to that read. Range replacement or backend recovery invalidates an old
read instead of letting it query a newer or empty dataset. A failed query is not
automatically replayed by the engine.

Request queries load only the columns the SQL reads. Cloud finds them from the
statement's names and confirms the set with DuckDB's binder against empty tables
of the same schema, custom fields included. `SELECT *`, whole-row references and
anything the check can't confirm load every column. Projected bytes are measured
from Parquet footers and count against the same 256-file and 64 MiB limits.

Row-limited queries load the newest hour of the range first and widen it four
times per step, up to the full range, only while the result has fewer rows than
the limit. This applies to a plain single-table `SELECT` with no ordering,
grouping, aggregate, `DISTINCT`, window or join, and to the same shape ordered by
`timestamp DESC`. For the ordered form Cloud reads every file overlapping the
window and restricts the table to rows at or after the window start, so the
newest rows match a full-range query. Other queries load the full range with
projection. Client vitals and errors still load whole files.

When the needed bytes or files still exceed the limit, the 413 response names the
needed and allowed size and asks for a shorter range or fewer columns.

## Tests

- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/security/test_no_raw_exception_in_responses.py`](../../tests/security/test_no_raw_exception_in_responses.py), [`tests/high_scale/test_query_service.py`](../../tests/high_scale/test_query_service.py)
- E2E: `SQL Query Pad Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts) (schema reflection, valid query, rejected `DROP TABLE`, CSV export, `?mode=raw` round trip)
