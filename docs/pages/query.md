# Page Specification: SQL Query Pad (`/query`)

> [!IMPORTANT] **Page Specification Status: Live-Certified (`release/v3.0.0-beta3`)**
> This specification defines the verified contract, architecture behaviors, role permissions, and test cases for `/query`. Audited and live-certified across Standard (Local & GCE) and High-Scale (Elevation) deployments.

---

## 1. Overview & Objectives

The **SQL Query Pad** provides an interactive SQL analytical development interface allowing engineers and analysts to execute arbitrary read-only SQL queries against DuckDB, DuckLake tables, and ClickHouse facts (`request_facts`, `rum_vitals_facts`, `rum_error_facts`). It features a visual query builder, an interactive Monaco SQL editor, schema explorer, query timing profiler, saved query presets, and client-side CSV export.

### Key Tenets:
- **Interactive SQL Power:** Direct read-only execution against the unified `logs` view, `client_vitals`, `client_errors`, DuckLake tables, and High-Scale ClickHouse facts.
- **Strict Tenancy & Safety:** Enforces read-only AST validation via DuckDB `extract_statements` (`validate_user_sql`, rejecting `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `ATTACH`, `COPY` with HTTP 403 `sql_not_permitted`), prohibited function/table blocking (`read_parquet`, `s3://`, `duckdb_secrets()`), and strict tenancy enforcement (`service_id` isolation via `RequestContext`).
- **Shared Primitives:** Conforms strictly to global navigation, `ReportLayout` primitives, URL filter synchronization (`useFilterUrlWriteback` + `?q=` hydration), and theme tokens.

---

## 2. Routes & URL Schema

- **Primary Route:** `/query`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `q`: Pre-populated SQL query string (URL encoded, automatically switches to Raw SQL mode).
  - `filters`: URL-encoded filter pills synchronized with `useFilterStore`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Arbitrary read-only SQL execution, schema exploration, CSV download. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Read-only SQL queries allowed (`POST /api/query` in `_ANALYST_ALLOWED_WRITE_PREFIXES`, `GET /api/query/schema/{service_id}` in `_READ_ALLOWLIST`); queries automatically sanitize/mask client IP (`mask_ips_for` / `mask_ip_values`) and redact sensitive keys (`redact_keys`); CSV export allowed. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckDB engine. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **SQL Engine** | DuckDB in-process connection from pool with self-healing retry (`execute_with_stale_view_retry`). | ClickHouse fact engine (`request_facts`, `rum_vitals_facts`, `rum_error_facts`) with automatic identifier translation and `service_id` tenant scoping. |
| **AST Validator** | DuckDB native `extract_statements` AST validator (`validate_user_sql`) + prohibited token scanner. | DuckDB native `extract_statements` AST validator (`validate_user_sql`) prior to ClickHouse translation. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `POST /api/query` (`response_model=QueryResponse`):
   - Payload: `QueryRequest` (`{"sql": "SELECT ...", "dataset": "logs", "max_rows": 1000}`).
   - Returns: `QueryResponse` (`columns`, `data`, `total_rows`, `elapsed_ms`, `debug_queries`, `_debug_queries`).
2. `GET /api/query/schema/{service_id}` and `GET /api/schema` (`response_model=QuerySchemaResponse`):
   - Returns table names and normalized column definitions (`SchemaColumn` with `column`, `name`, `type`).
3. `GET /api/presets`:
   - Returns curated `PresetQuery` definitions (`top_urls`, `error_breakdown`, `slow_requests`, `traffic_by_country`, `cache_performance`, `bot_requests`).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID` and executes inside `process_context_scope("query:execute")` / `ctx.telemetry.section("query")`.
- Database statements recorded in `telemetry_queries`, `_debug_queries`, `slow_queries`, and `usage_log`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Root container (`[data-testid="query-page-root"]`), Editor Container (`[data-testid="query-editor-container"]`), Schema Sidebar (`[data-testid="query-schema-sidebar"]`), and Results Container (`[data-testid="query-results-container"]`) enforce CSS intrinsic size reservations (`[contain-intrinsic-size:500px]`, `[contain-intrinsic-size:320px]`, `[content-visibility:auto]`).
- **In-Place Skeletons:** Renders engine initialization skeleton (`Initializing DuckDB analytical engine...` at `[data-testid="query-engine-initializing"]`) and query execution skeleton (`Executing SQL query...` at `[data-testid="query-results-loading"]`).
- **Virtual Results Table:** Uses `@tanstack/react-virtual` (`DataTable` inside `[data-testid="query-results-table"]`) to display up to 10,000 result rows without DOM memory exhaustion or layout shifts, with non-destructive refetch dimming (`opacity-60 transition-opacity duration-150`).

---

## 7. Interactive Workflows & Edge Cases

- **Schema Auto-Completion & Click-to-Insert:** Monaco editor autocompletes table and column names; clicking any column in `[data-testid="query-schema-sidebar"]` inserts it into the SQL query.
- **Query Error Highlighting:** AST rejections and syntax errors surface in a structured destructive alert banner (`[data-testid="query-error-alert"]`).
- **CSV Download:** One-click CSV export (`[data-testid="query-csv-export-btn"]`) streams `query_results.csv` directly from the browser without server re-execution.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 | <= 0.05 | PerformanceObserver |
| **Query Engine Overhead** | < 20 ms backend dispatch | < 20 ms backend dispatch | Backend section timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_queries` / `telemetry_queries` audit |
| **Safety Validation** | 100% non-SELECT rejected | 100% non-SELECT rejected | Security test suite |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /query` returns HTTP 200 and renders `ReportLayout` with footer indicator.
- [x] **2. Zero Layout Shift:** Page loads with pre-allocated panel skeletons and CLS <= 0.05.
- [x] **3. SQL Execution Contract:** Execute `SELECT count(*) AS total_count FROM logs` and verify valid JSON result and rendered table.
- [x] **4. Safety Enforcement:** Execute `DROP TABLE logs`, `DELETE`, `INSERT`, `ALTER` and verify HTTP 403 `sql_not_permitted` error and UI alert banner.
- [x] **5. Schema Explorer:** Verify `/api/query/schema/{service_id}` and schema sidebar display column names and types.
- [x] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs (`xxx.xxx.xxx.xxx`) when querying `client_ip`.
- [x] **7. Telemetry & Query Audit:** Verify executed SQL query is recorded in `_debug_queries` / `debug_queries`.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright E2E Test:** `frontend/e2e/pages/query.spec.ts` (8 contract tests).
- **Frontend Unit Tests:** `frontend/__tests__/app/query.test.tsx` (7 unit tests).
- **Backend Router & Security Tests:** `tests/routers/test_query_router.py` (22 unit & contract tests) and `tests/remote_access/test_route_table_tripwire.py`.
