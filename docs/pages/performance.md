# Page Specification: Edge Performance (`/performance`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/performance`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Edge Performance** page analyzes end-to-end delivery latency across Fastly's edge cloud. It inspects Time to First Byte (TTFB), Time to Last Byte (TTLB), edge compute response times, TLS handshake durations, client-side RTT, and regional delivery percentiles (p50, p75, p90, p95, p99).

### Key Tenets:
- **Latency Distribution:** Deep percentiles across edge hits vs cache misses.
- **Regional & PoP Latency:** Granular breakdown by client country, continent, and edge PoP.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/performance`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `pop`, `country`, `asn`, `status`, `content_type`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full percentile charts, slow URL drill-down, unmasked IPs. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full latency percentiles and geo charts; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `perf_dims` / `perf_latency` rollups. | Reads Postgres DuckLake / ClickHouse percentile aggregates. |
| **Percentile Engine** | `approx_quantile(time_elapsed, [0.5, 0.9, 0.95, 0.99])`. | ClickHouse/Postgres t-digest / quantile approximations. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/performance/bundle` (or composite bundle):
   - Returns TTFB/TTLB percentile time series, PoP latency table, Slowest URLs table, and TLS handshake metrics.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Percentile Time Series Box, PoP Latency Grid, and Slowest URLs Table enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Panels display contextual loading copy (`Calculating latency percentiles...`, `Profiling PoP TTFB...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Percentile Toggle:** Switch between p50, p90, p95, p99 on the multi-trace latency chart.
- **Hit vs Miss Latency Split:** Compare edge-hit response time directly against origin-fetch response time.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `perf_latency` rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /performance` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Data Bundle Contract:** Verify TTFB and TTLB percentiles populate correctly.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Metric Switching:** Toggle between p50, p90, and p99 percentiles and verify traces update.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked IPs and cannot access admin triggers.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/performance.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Varied latency distribution (fast edge hits at <20ms, origin misses at 150-400ms).
