# Page Specification: Raw Logs Explorer (`/logs`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/logs`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Raw Logs Explorer** provides high-throughput, paginated, searchable access to individual Fastly CDN access log records. It supports dynamic column selection, complex multi-field filtering, JSON row expansion, and deep incident forensics.

### Key Tenets:
- **High-Throughput Pagination:** Cursor or limit/offset pagination optimized to fetch pages of 50-100 rows in sub-second times.
- **Deep Row Inspection:** Expanding any log row reveals all parsed Fastly VCL variables, headers, and geo data.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/logs`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-1h`).
  - `page`, `limit`: Pagination controls.
  - Standard drill-down filters: `status`, `pop`, `client_ip`, `url`, `origin`, `asn`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full log records, unmasked client IPs, full request URLs and headers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Log records viewable; client IPs masked (if enabled by invite setting). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session `logs` view with pushdown predicates. | Reads Postgres DuckLake or ClickHouse raw logs table. |
| **Filter Optimization** | Uses partition pruning (`timestamp >= ...`) to minimize parquet reads. | Uses ClickHouse primary keys / Postgres index scans. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/logs/{service_id}`:
   - Parameters: `service_id`, `from`, `to`, `limit`, `offset`, plus active filters.
   - Returns: `rows` array, `total_count`, `has_more`.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Logs Filter Toolbar, Column Selector Drawer, and Paginated Logs Table enforce container intrinsic sizes (`contain-intrinsic-size: 600px`).
- **In-Place Skeletons:** Renders tabular skeleton rows (`Querying edge log records...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Row Expansion:** Clicking any table row expands an accordion drawer with formatted JSON.
- **Column Customization:** Checkbox dropdown allows toggling visible columns (Status, IP, URL, Pop, Time, TLS, WAF).

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Logs Page)** | < 300 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Partition-pruned select; 0 redundant queries | Indexed select; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /logs` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Logs Table Contract:** Verify rows populate with timestamp, status, URL, and Pop.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Row Expansion:** Click a row to expand and verify JSON details display.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/logs.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Continuous diverse log records covering all HTTP methods and response codes.
