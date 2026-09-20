# Page Specification: Origin Health (`/origin`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/origin`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Origin Health** page focuses on backend origin infrastructure, shielding efficiency, origin connect latency, first-byte delays, backend error rates (502, 503, 504), origin retries, and origin host distributions.

### Key Tenets:
- **Backend Infrastructure Monitoring:** Surfaces origin health and latency across multiple configured backend hosts.
- **Shielding Topology:** Displays shielding PoP performance vs direct-to-origin fetches.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/origin`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `origin`, `pop`, `status`, `content_type`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full origin host names, backend IP diagnostics, retry logs. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Origin health metrics viewable; backend hostnames preserved unless masked by invite policy. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `origin_summary` / `origin_dims` rollups. | Reads Postgres DuckLake / ClickHouse origin tables. |
| **Rollup Optimization** | Leverages `origin_latency_ts` rollup parquet files for instant 30d views. | Leverages server-side pre-aggregates. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/origin/bundle` (or composite bundle):
   - Returns origin status breakdown, backend connect latency percentiles, origin retry counts, shielding ratio, and origin host list.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Origin KPI Cards, Backend Latency Time Series, and Origin Host Table enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Auditing backend health...`, `Measuring origin connect time...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Backend Host Filter:** Clicking an origin backend filters all latency and error graphs by that specific host.
- **Zero-Origin Edge Case:** When all requests are edge cache hits, origin panels display "No origin requests recorded" cleanly without errors.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `origin_summary` rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /origin` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Data Bundle Contract:** Verify backend latency and origin retry counts populate correctly.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Backend Filtering:** Clicking an origin host in the table applies the filter chip.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/origin.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Multi-origin backend traffic with mixed connect times and simulated backend retries.
