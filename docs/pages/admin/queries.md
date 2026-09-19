# Page Specification: Admin Live Query Monitor (`/admin/queries`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin/queries`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Live Query Monitor** provides real-time observability into all running and recently executed analytical SQL queries across **DuckDB**, **ClickHouse** (`ClickHouseClient` internal queries and inserts registered with `query_registry.register("ClickHouse", ...)`), **Postgres** (multi-writer catalog and `ingest_ledger`), and **SQLite** metadata/usage pools. It surfaces active query durations, lock contention, thread wait times (`app.thread_wait_ms`), caller attribution (API route vs cron job), slow query logs, and supports query cancellation.

### Key Tenets:
- **Zero Dark Queries:** Every database statement across DuckDB, ClickHouse, Postgres, and SQLite is surfaced in real time with duration, memory, and caller tags.
- **Lock Contention Forensics:** Visualizes SQLite thread locks and DuckDB connection pool saturation.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/queries`
- **Supported Query Parameters:**
  - `service`: Filter queries by service ID or `__global_share__`.
  - `engine`: Filter by `DuckDB`, `ClickHouse`, `SQLite`, `Postgres`.
  - `status`: Filter by `running`, `completed`, `slow`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full query texts, caller attribution, thread IDs, query cancellation. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Live Profiler** | Reads in-memory query tracker in `duckdb_pool` and `sqlite_pool`. | Reads `query_registry` tracking DuckDB, ClickHouse, Postgres, and SQLite. |
| **Slow Query Log** | SQLite `slow_queries` table. | Postgres or SQLite `slow_queries` table. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/queries/active`:
   - Returns currently executing queries across DuckDB, ClickHouse, Postgres, and SQLite, durations, and connection pool states.
2. `GET /api/admin/queries/slow`:
   - Returns historical slow queries exceeding threshold (e.g. > 1000ms).
3. `POST /api/admin/queries/{query_id}/cancel`:
   - Interrupts running DuckDB query execution. (ClickHouse HTTP queries use strict `max_execution_time` timeouts).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.

- Profiler queries themselves are excluded from recursive self-logging.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Pool Health Gauge, Active Queries Table, and Slow Queries Feed enforce container intrinsic sizes (`contain-intrinsic-size: 400px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Inspecting DuckDB connection pool...`).
- **Live Updating Rows:** Polling or SSE updates update active timers in place with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Kill Running Query:** Administrator can click "Cancel Query" to terminate an expensive query that is blocking connection pool workers.
- **Query Detail Modal:** Clicking a query row displays the full formatted SQL string, EXPLAIN plan, and thread wait latency.
- **Zero Active Queries State:** Displays a green "All connection pools idle" indicator.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Active Queries)** | < 100 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | Query profiler audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin/queries` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Active Query Detection:** Trigger an analytical query in another tab and verify it appears in the Active Queries table.
- [ ] **4. Slow Query History:** Verify historically slow queries render with duration and caller attribution.
- [ ] **5. Role Verification:** Confirm Analyst Path B receives 403.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-queries.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Injected long-running analytical query to verify live capture and cancellation.
