# Page Specification: Raw Logs Explorer (`/logs`)

> [!NOTE] **Live-certified on `release/v3.0.0-beta3`**
> This specification defines the verified contract, architecture behaviors, role permissions, and test suites for `/logs`. Validated across Local Standard, Remote Standard GCE, and High-Scale Elevation deployments with full query attribution and zero layout shifts.

---

## 1. Overview & Objectives

The **Raw Logs Explorer** provides high-throughput, paginated, searchable access to individual Fastly CDN access log records. It supports dynamic column selection, complex multi-field filtering, JSON row expansion, and deep incident forensics.

### Key Tenets:
- **High-Throughput Pagination:** Cursor or limit/offset pagination optimized to fetch pages of 50-100 rows in sub-second times.
- **Deep Row Inspection:** Expanding any log row reveals all parsed Fastly VCL variables, headers, and geo data with formatted table and raw JSON viewing modes and quick-copy actions.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.
- **Analyst Masking:** Honors remote share IP masking (`mask_ips_for`) and session ID redaction (`[redacted]`).

---

## 2. Routes & URL Schema

- **Primary Route:** `/logs`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `tab`: Active tab selector (`explorer` for Raw Logs Explorer, `cron`, `service_history`, `ingestion`, `iceberg`, `metadata_storage`, `raw`, `schema`).
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-1h`).
  - `limit`, `offset`: Pagination controls.
  - Standard drill-down filters: `status`, `pop`, `client_ip`, `url`, `origin`, `asn`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full log records, unmasked client IPs, full request URLs and headers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Log records viewable; client IPs masked (if enabled by invite setting). `POST /api/logs` and `POST /api/logs/query` allowed under `_ANALYST_ALLOWED_WRITE_PREFIXES` and `_WRITE_VERB_GATE_ROUTES`; `GET /api/logs` and `GET /api/logs/{service_id}` allowed in `_READ_ALLOWLIST`. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session `logs` view with pushdown predicates and stale-view retry protection. | Reads ClickHouse `request_facts` table with partition filtering. |
| **Filter Optimization** | Uses partition pruning (`timestamp >= ...`) to minimize parquet reads. | Uses ClickHouse primary keys and partition date bounds. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `POST /api/logs/query` & `POST /api/logs`:
   - Body: `LogsQueryRequest` (`service_id`, `from`, `to`, `range_token`, `limit`, `offset`, `columns`, `search`, `sort_dir`, `filters`).
   - Returns: `LogsQueryResponse` (`rows` array, `total_count`, `has_more`, `columns`, `schema_columns`).
2. `GET /api/logs` & `GET /api/logs/{service_id}`:
   - Query params: `service_id`, `from`, `to`, `limit`, `offset`, `search`, `sort_dir`.
   - Returns: `LogsQueryResponse`.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Log queries execute under `process_context_scope("logs:query")`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Raw Logs Explorer container and table container enforce container intrinsic sizes (`[contain-intrinsic-size:600px]` and `[content-visibility:auto]`).
- **In-Place Skeletons:** Renders tabular skeleton rows (`Querying edge log records...`).
- **Dimmed Updates:** In-flight refetches dim the table (`opacity-60 pointer-events-none`) with zero layout shift (CLS <= 0.05).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Inherits time range from `FilterBar` (`useTimeRange` and `useDebouncedFilterPayload`).
- **Row Expansion:** Clicking any table row opens a dialog drawer with formatted field/value pairs and raw JSON viewer with quick copy.
- **Column Customization:** Dropdown menu allows toggling visible columns dynamically while maintaining minimum column constraints.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **LCP** | < 1500 ms | < 1500 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (Target 0.00) | <= 0.05 (Target 0.00) | PerformanceObserver |
| **API Latency (Logs Query)** | < 300 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Partition-pruned select; 0 redundant queries | Partition-pruned ClickHouse query; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /logs` returns HTTP 200 with shell and footer environment indicator.
- [x] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS <= 0.05.
- [x] **3. Logs Table Contract:** Verify rows populate with timestamp, status, URL, and Pop.
- [x] **4. Time Range Extent:** Confirms time range integration and clean search/filter debouncing.
- [x] **5. Row Expansion:** Click a row to expand and verify JSON details display.
- [x] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs; verb gates pass.
- [x] **7. Telemetry & Query Audit:** Verify all DuckDB and ClickHouse queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit Tests:** `tests/routers/test_logs_router.py` (6 tests covering empty table, paginated logs, filter and search, GET endpoints, column projection, analyst IP masking).
- **Route Gate Tests:** `tests/remote_access/test_route_table_tripwire.py` (`POST /api/logs`, `POST /api/logs/query` in allowed write prefixes; `GET /api/logs`, `GET /api/logs/{service_id}` in read allowlist).
- **Frontend Unit Tests:** `frontend/__tests__/app/logs.test.tsx` (tests verifying mounting in both cron and raw logs explorer modes).
- **Playwright E2E Test:** `frontend/e2e/pages/logs.spec.ts` (8 contract tests validating accessibility, zero-CLS layout, schema, controls, search debouncing, row detail inspection modal, perf budget, and telemetry).
- **Synthetic Traffic Profile:** Continuous diverse log records covering all HTTP methods, status codes, and edge POPs.
