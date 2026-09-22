# Page Specification: Admin System Trends & Health Sparklines (`/admin/trends`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin/trends`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Admin System Trends** page displays historical time-series graphs of operational health vitals, system resource utilization, memory consumption, CPU load, DuckDB connection pool saturation, SQLite thread lock duration, ingestion lag, and Celery task throughput.

### Key Tenets:
- **Operational Health History:** Powered by the continuous `metric_snapshot` background job running every 60s.
- **Resource Saturation Forensics:** Identifies memory leaks, connection pool exhaustion, or I/O bottlenecks.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/trends`
- **Supported Query Parameters:**
  - `range`: Time range (`1h`, `6h`, `24h`, `7d`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full host vitals, pool queue lengths, disk I/O metrics. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Snapshot Source** | Populated by `metric_snapshot` cron into SQLite `metadata.db`. | Populated by `metric_snapshot` into Postgres or SQLite `metadata.db`. |
| **Metrics Sampled** | Host CPU/RAM (`psutil`), DuckDB pool, SQLite pool, thread wait ms. | Host CPU/RAM, Celery worker depths, pool stats. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/system/trends`:
   - Returns time series for CPU %, Memory MB, Active DuckDB Conns, Queue Depth, and Ingestion Lag.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** CPU/RAM Time Series Box, DuckDB Pool Saturation Chart, and Ingest Lag Sparklines enforce container intrinsic sizes (`contain-intrinsic-size: 350px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Loading system vital trends...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Range Selector:** Switch between 1h, 6h, 24h, 7d history.
- **Anomaly Highlight:** Spikes in thread wait time or pool saturation are highlighted with warning indicators.
- **Fresh Boot State:** Services recently booted display points since boot without rendering broken trend lines.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Trends)** | < 200 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin/trends` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Trends Data Contract:** Verify CPU, memory, and pool time-series populate from `system_metrics`.
- [ ] **4. Range Toggle:** Switch time range from 1h to 24h and verify chart updates.
- [ ] **5. Role Verification:** Confirm Analyst Path B receives 403.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-trends.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Continuous `metric_snapshot` ticks populating the `system_metrics` table.
