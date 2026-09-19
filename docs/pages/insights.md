# Page Specification: Automated Insights & Anomaly Detection (`/insights`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/insights`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Insights** page runs 45 automated anomaly and pattern detectors comparing current traffic against baseline historical distributions (e.g. current 1h vs 168h baseline). It identifies abnormal error rate surges, sudden traffic drop-offs, regional routing deviations, cache hit degradations, and emerging attack signatures.

### Key Tenets:
- **Baseline Comparison Engine:** Employs statistical deviation checks (z-score, ratio shifts) across 5 categories: Traffic, Performance, Origin, Security, and Routing.
- **Prewarmed Speed:** Relies on the background `insights_prewarmer_{id}` cron job to ensure instant sub-second rendering.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/insights`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `tab`: Category tab (`all`, `traffic`, `performance`, `origin`, `security`, `routing`).
  - `window`: Active evaluation window (e.g. `1h`, `24h`).
  - `baseline`: Historical baseline window (e.g. `168h` / 7 days).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full anomaly cards, sensitivity controls, trigger immediate re-scan. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full anomaly cards and baseline comparisons viewable; re-scan trigger disabled. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Anomaly Compute** | DuckDB window functions & ratio comparisons over local data. | Postgres DuckLake / ClickHouse analytical queries. |
| **Prewarming Cache** | Pod-local memory and disk cache populated by `insights_prewarmer`. | Shared Redis or pod-local cache. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/insights` (or composite category endpoints):
   - Parameters: `service_id`, `window`, `baseline`.
   - Returns array of scored insight cards (category, title, severity, metric_name, current_val, baseline_val, pct_change).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Category Tab Bar, Insight Grid Container, and Card Placeholders enforce container intrinsic sizes (`contain-intrinsic-size: 400px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Evaluating 45 baseline detectors...`, `Analyzing traffic anomalies...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 1h active window vs 168h baseline for mature services; adapts baseline if history < 168h.
- **Category Filtering:** Switching tabs (`Security`, `Performance`, etc.) filters cards client-side without re-fetching entire payloads.
- **No Anomalies State:** When all detectors pass cleanly, displays an "All systems operating within normal parameters" banner.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Prewarmed)** | < 100 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **API Latency (Cold)** | < 600 ms (p95) | < 400 ms (p95) | Cold cache run |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Prewarmed hit; 0 redundant queries | Cached hit; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /insights` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Anomaly Evaluation Contract:** Verify all 5 categories return valid insight cards or clean passes.
- [ ] **4. Prewarmer Effectiveness:** Confirm response time for default window is < 150ms due to `insights_prewarmer`.
- [ ] **5. Category Tab Switching:** Click category tabs and verify instantaneous client-side filter.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/insights.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Injected 200% error surge on specific URL to trigger high-severity anomaly card.
