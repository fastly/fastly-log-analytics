# Page Specification: SQL Query Pad (`/query`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/query`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **SQL Query Pad** provides an interactive SQL analytical development interface allowing engineers and analysts to execute arbitrary read-only SQL queries against DuckDB and DuckLake tables. It features an interactive Monaco SQL editor, schema explorer, query timing profiler, saved query presets, and CSV export.

### Key Tenets:
- **Interactive SQL Power:** Direct read-only execution against the unified `logs` view, DuckLake tables, and rollups.
- **Strict Tenancy & Safety:** Enforces read-only AST validation (rejects `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `ATTACH`) and tenancy enforcement (`service_id` isolation).
- **Shared Primitives:** Conforms strictly to global navigation, layout primitives, and theme tokens.

---

## 2. Routes & URL Schema

- **Primary Route:** `/query`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `q`: Pre-populated SQL query string (URL encoded).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Arbitrary read-only SQL execution, schema exploration, CSV download. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Read-only SQL queries allowed; queries automatically sanitize/mask client IP if invite policy requires; export allowed. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckDB engine. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **SQL Engine** | DuckDB in-process connection from pool with self-healing retry (`execute_with_stale_view_retry`). | Ephemeral read-only DuckDB connection over Postgres DuckLake catalog. |
| **AST Validator** | Python SQLGlot / DuckDB EXPLAIN validation. | AST validator blocking non-SELECT queries. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `POST /api/query`:
   - Payload: `{"sql": "SELECT ...", "service_id": "..."}`.
   - Returns: Columns metadata, rows array, row_count, execution_time_ms.
2. `GET /api/query/schema/{service_id}`:
   - Returns table names and column definitions.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries`, `slow_queries`, and `usage_log.db`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Monaco Editor Box, Schema Tree Drawer, and Results Grid enforce container intrinsic sizes (`contain-intrinsic-size: 500px`).
- **In-Place Skeletons:** Renders editor skeleton (`Initializing DuckDB analytical engine...`).
- **Virtual Results Table:** Uses `@tanstack/react-virtual` to display up to 10,000 result rows without DOM memory exhaustion or layout shifts.

---

## 7. Interactive Workflows & Edge Cases

- **Schema Auto-Completion:** Monaco editor autocomplete autocompletes table names (`logs`, `client_vitals`, `rollups`) and column names.
- **Query Error Highlighting:** Syntax errors return line/column numbers highlighted directly in the editor.
- **CSV Download:** One-click CSV export streams results directly from the browser without server re-execution.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **Query Engine Overhead** | < 20 ms backend dispatch | < 20 ms backend dispatch | Backend section timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Safety Validation** | 100% non-SELECT rejected | 100% non-SELECT rejected | Security test suite |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /query` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. SQL Execution Contract:** Execute `SELECT count(*) FROM logs` and verify valid JSON result.
- [ ] **4. Safety Enforcement:** Execute `DROP TABLE logs` or `CREATE TABLE foo` and verify HTTP 400 error.
- [ ] **5. Schema Explorer:** Verify table schema expands and displays column types.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs when querying `client_ip`.
- [ ] **7. Telemetry & Query Audit:** Verify executed SQL query is recorded in `telemetry_queries`.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/query.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Standard ingested log records available for aggregation testing.
