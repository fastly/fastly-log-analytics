# Page Specification: Real User Monitoring & Core Web Vitals (`/rum`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/rum`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Real User Monitoring (RUM)** page captures browser-side telemetry delivered via Fastly beacon streaming. It provides user-centric visibility into Google Core Web Vitals (Largest Contentful Paint [LCP], Interaction to Next Paint [INP], Cumulative Layout Shift [CLS], First Contentful Paint [FCP], Time to First Byte [TTFB]) and browser JavaScript error stacks.

### Key Tenets:
- **Core Web Vitals Compliance:** Evaluates Google 75th percentile thresholds (Good / Needs Improvement / Poor).
- **CDN Correlation:** Correlates RUM beacon sessions with CDN access logs via `rum_cid`.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/rum`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `page_path`, `browser`, `device_type`, `country`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full CWV histograms, client JS error stack traces, unmasked client IPs. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full CWV dashboards viewable; JS error details viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake `client_vitals` / `client_errors`. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckLake tables `lake.client_vitals` and `lake.client_errors`. | Reads Postgres DuckLake or ClickHouse RUM tables. |
| **Ingest Pipeline** | Driven by `rum_sync_{id}` and `rum_commit_{id}`. | Driven by `rum_discovery_{id}` and Celery workers. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/rum/bundle` (or composite bundle):
   - Returns 75th percentile ratings for LCP, INP, CLS, FCP, TTFB, browser distributions, and recent JS error clusters.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** CWV Scorecard Grid, CWV Distribution Histograms, and JS Error Stack Table enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Aggregating Core Web Vitals...`, `Analyzing browser JS errors...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **RUM Not Configured State:** If RUM beacon logging is not enabled in service config, displays clear guidance on how to enable RUM collection without throwing UI crashes.
- **CWV Rating Breakdown:** Interactive toggles show the percentage of users experiencing Good / Needs Improvement / Poor scores.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Queries `client_vitals` partition; 0 redundant queries | Optimized partition scans; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /rum` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. CWV Metric Contract:** Verify LCP, INP, and CLS percentiles calculate accurately against Google thresholds.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Unconfigured State:** Disabling RUM renders the onboarding helper gracefully.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/rum.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Simulated browser beacons with varied LCP (1.2s to 4.5s) and simulated JS errors.
