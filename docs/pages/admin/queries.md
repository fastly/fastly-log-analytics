# Page Specification: Admin Live Query Monitor (`/admin/queries`)

> [!IMPORTANT] **Page Specification Status: Live-Certified (2026-10-17)**
> This specification has been audited, hardened, and live-certified across Local Standard, Remote Standard GCE, and High-Scale Elevation environments.

---

## 1. Overview & Objectives

The **Live Query Monitor** provides real-time observability into all running and recently executed analytical and operational SQL queries across **DuckDB**, **ClickHouse** (`ClickHouseClient` internal queries and inserts registered with `query_registry.register("ClickHouse", ...)`), and **PostgreSQL** (`METADATA_DSN` operational metadata, `DUCKLAKE_CATALOG` catalog, and `ingest_ledger` per ADR-22; legacy `SQLite` profiler labels are also supported for backwards compatibility). It surfaces active query durations, lock contention, thread wait times (`app.thread_wait_ms`), caller attribution (API route vs cron job), persisted slow query logs, and supports live query cancellation.

### Key Tenets:
- **Zero Dark Queries:** Every database statement across DuckDB, ClickHouse, and PostgreSQL (`sqlite_profiler` instrumentation) is surfaced in real time with duration, row counts, and caller tags.
- **Lock Contention Forensics:** Visualizes metadata/catalog thread locks and DuckDB connection pool saturation.
- **Strict RBAC:** Accessible **exclusively** to Admin users. Analysts (Remote Share) receive HTTP `403 Forbidden`.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/queries`
- **Client-Side Filter Controls:**
  - `dbTypeFilter`: Filter by `all`, `DuckDB`, `ClickHouse`, `Postgres`, `SQLite`.
  - `serviceFilter`: Filter queries by service ID or `__global_share__`.
  - `callerFilter`: Substring filter on `caller` (`endpoint:GET /api/...`, `cron:...`).
  - `sourceFilter` (Slow Queries): Filter between `all`, `in_memory` ring buffer, and `persisted` PostgreSQL (`METADATA_DSN`) `slow_queries` records.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full query texts, caller attribution, thread IDs, query cancellation (`POST /api/admin/queries/{qid}/cancel`), slow query log clearing (`DELETE /api/admin/slow-queries`). |
| **Analyst (Remote Share)** | Blocked (403) | All `/api/admin/queries*` and `/api/admin/slow-queries*` routes rejected with HTTP `403 Forbidden` via `_ANALYST_BLOCKED_PREFIXES` (`admin_only`). |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Live Profiler (`query_registry`)** | Reads in-memory `QueryRegistry` tracking DuckDB (`duckdb_pool`) and PostgreSQL (`sqlite_profiler` / `pg_connection`) active and recent ring-buffer queries. | Reads `QueryRegistry` tracking DuckDB, ClickHouse (`ClickHouseClient`), and PostgreSQL (`METADATA_DSN` / `DUCKLAKE_CATALOG`) active and completed queries. |
| **Persisted Slow Query Log** | PostgreSQL `slow_queries` table (`METADATA_DSN`, ADR-22) via `backend/core/metadata/slow_queries.py` (`record_slow_query`, `get_slow_queries`, `get_slow_query_count`, `clear_slow_queries`). | PostgreSQL `slow_queries` table (`METADATA_DSN`, ADR-22) shared across serving and worker pods. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/queries` (`SnapshotResponse`):
   - Returns `active` running queries, incremental `completed` ring-buffer queries (`since_id`, `limit`), `slow` in-memory queries, `latest_id`, and `server_time`.
2. `GET /api/admin/queries/summary` (`SummaryResponse`):
   - Lightweight count snapshot (`active_count`, `duckdb_active`, `sqlite_active`, `clickhouse_active`, `postgres_active`, `slow_count_1h`, `completed_buffer_size`, `latest_id`) for nav badges and header KPIs.
3. `GET /api/admin/queries/{qid}` (`QueryDetailResponse` / `ActiveQueryRow`):
   - Fetches untruncated SQL text (up to 64 KB) for an active, completed, or slow query by ID in the in-memory `QueryRegistry`.
4. `POST /api/admin/queries/{qid}/cancel` (`CancelResponse`):
   - Interrupts a running DuckDB query via `conn.interrupt()` and records an `admin_cancel_query` audit event in PostgreSQL `audit_logs`.
5. `GET /api/admin/slow-queries` (`SlowQueriesListResponse` / `SlowQueriesResponse`):
   - Queries persisted slow queries from PostgreSQL `slow_queries` (`METADATA_DSN`) with optional `service_id`, `db_type` (`DuckDB|Postgres|ClickHouse|SQLite`), `min_duration_ms`, `since`, and `limit`.
6. `GET /api/admin/slow-queries/count` (`SlowQueriesCountResponse`):
   - Returns `{ count, window_seconds }` of persisted slow queries within the lookback window (default 3600s).
7. `DELETE /api/admin/slow-queries` (`SlowQueriesClearResponse`):
   - Clears persisted slow query rows from PostgreSQL `slow_queries` (scoped to `service_id` or all services) and records an `admin_clear_slow_queries` audit event.
8. `GET /api/admin/app-config/query-monitor` (`QueryMonitorConfigResponse`):
   - Returns `slow_query_threshold_ms` and `poll_interval_ms` from `app_config.json`.

### Telemetry Attribution:
- Every frontend fetch carries `X-Page-Load-ID` via `apiFetch`.
- Profiler queries and `slow_queries` persistence statements execute inside `sqlite_profiler.exclude_from_profiling()` to prevent recursive self-logging.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Summary cards (`[contain-intrinsic-size:auto_76px]`), Active Queries (`[contain-intrinsic-size:auto_220px]`), Slow Queries (`[contain-intrinsic-size:auto_200px]`), and Completed Queries (`[contain-intrinsic-size:auto_320px]`) enforce CSS layout containment (`[content-visibility:auto]`) with `CLS <= 0.05`.
- **In-Place Skeletons:** Route-level `loading.tsx` (`data-testid="admin-queries-loading"`) and in-place panel skeletons render contextual copy (`Inspecting DuckDB connection pool...`, `Loading query telemetry...`).
- **Non-Destructive Refresh & Dimming:** Manual Refresh (`data-testid="admin-queries-refresh-btn"`) and background polling dim active panels (`opacity-40 pointer-events-none`) without collapsing layout containers.

---

## 7. Interactive Workflows & Edge Cases

- **Kill Running Query:** Administrator can click "Cancel" on a running DuckDB query (`cancelable: true`) to invoke `conn.interrupt()` and free blocked connection pool workers.
- **Query Detail Modal:** Clicking a query row opens `SqlDetailModal`, fetching untruncated SQL from `GET /api/admin/queries/{qid}` (with inline fallback for persisted slow queries) and copy-to-clipboard support.
- **Pause / Resume & Manual Refresh:** Administrator can pause live 1.5s polling to inspect tables without row movement, or click `Refresh` (`data-testid="admin-queries-refresh-btn"`) for an immediate snapshot + persisted slow query sync.
- **Zero Active Queries State:** Displays `"No queries running right now"` inside the reserved Active Queries table container.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.00 actual) | <= 0.05 (0.00 actual) | PerformanceObserver |
| **API Latency (Active Queries)** | < 100 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | Query profiler audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright & Pytest auth audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin/queries` returns HTTP 200 for Admin; returns HTTP 403 for Analyst across all `/api/admin/queries*` and `/api/admin/slow-queries*` endpoints.
- [x] **2. Zero Layout Shift:** Verified page loads with pre-allocated panel reservations (`[contain-intrinsic-size:...]`) and `CLS <= 0.05`.
- [x] **3. Active Query & Snapshot Telemetry:** Verified `/api/admin/queries` and `/api/admin/queries/summary` render live active, slow, and completed ring-buffer telemetry.
- [x] **4. Slow Query History:** Verified persisted slow queries (`GET /api/admin/slow-queries`, `GET /api/admin/slow-queries/count`, `DELETE /api/admin/slow-queries`) against PostgreSQL `slow_queries` (`METADATA_DSN`).
- [x] **5. Role Verification:** Confirmed Analyst receives HTTP 403 across all 8 endpoints in `tests/routers/test_admin_queries.py` and `frontend/e2e/pages/admin-queries.spec.ts`.
- [x] **6. Telemetry & Query Audit:** Verified `X-Page-Load-ID` propagation and profiler self-exclusion (`exclude_from_profiling`).

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit Tests:** `tests/routers/test_admin_queries.py` (35 tests covering registry, snapshot, summary, detail, cancel + audit, persisted slow query list/count/clear, config endpoint, and Analyst 403 rejection).
- **Frontend Unit Tests:** `frontend/__tests__/app/admin-queries.test.tsx` (3 tests covering `loading.tsx`, initial skeleton reservations, and populated query tables + interactive controls).
- **Playwright E2E Suite:** `frontend/e2e/pages/admin-queries.spec.ts` (5 E2E contract tests certified across Local Standard, Remote Standard GCE, and High-Scale Elevation).
