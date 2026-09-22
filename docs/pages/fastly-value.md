# Page Specification: Service Summary & Fastly Value (`/fastly-value`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/fastly-value`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Fastly Value** page provides executive and financial visibility into CDN ROI, offload efficiency, egress bandwidth savings, origin request reduction, compute cost avoidance, and cache hit ratio (CHR) trends.

### Key Tenets:
- **Financial & Operational Value:** Computes total bytes served vs. origin bytes fetched to quantify origin egress cost savings.
- **Edge Efficiency:** Demonstrates compression ratio savings, edge shielding benefits, and Compute execution metrics.
- **Shared Primitives:** Uses `ReportLayout`, `FilterBar`, and global time range stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/fastly-value`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`, `now-30d`).
  - Standard drill-down filters: `pop`, `origin`, `content_type`, `country`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full value KPIs, cost parameters configuration, export capabilities. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full executive charts and offload KPIs viewable; cost configuration settings read-only. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + daily/hourly rollups. | Reads Postgres DuckLake or ClickHouse aggregate tables. |
| **Rollup Optimization** | Uses precomputed `origin_summary` and `day_bundles` for 30d/90d historical ROI calculation. | Uses pre-aggregated rollup tables. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/fastly-value/summary` (or composite bundle):
   - Returns total requests, offload percentage, origin requests saved, edge bytes served, origin egress avoided, estimated dollar savings.
2. `GET /api/fastly-value/time-series`:
   - Returns hourly or daily offload ratio and egress bandwidth trends.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Executive KPI banner, Offload Donut/Gauge, and Value Trend Chart enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Calculating bandwidth offload...`, `Estimating cost savings...`).
- **Dimmed Updates:** Filter or time range updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Cost Parameter Editing:** Operators can toggle estimated origin egress cost per GB ($/GB) to dynamically recalculate dollar savings.
- **Zero-Origin Edge Case:** If 100% of traffic is cached (0 origin requests), offload correctly displays 100.0% without division-by-zero errors.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Summary)** | < 300 ms (p95) | < 200 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Leverages rollups; 0 redundant queries | Leverages rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /fastly-value` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Value Contract:** Verify offload percentage and bandwidth savings are accurately computed.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Cost Parameter Calculation:** Dynamic update of $/GB parameter immediately recalculates savings.
- [ ] **6. Role Verification:** Analyst Path B has full read-only visibility into ROI metrics.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/fastly-value.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Standard web traffic with high cache hit ratio (85%+) to demonstrate CDN offload.
