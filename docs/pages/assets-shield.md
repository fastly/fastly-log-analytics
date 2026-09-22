# Page Specification: Assets & Origin Shielding (`/assets-shield`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/assets-shield`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Assets & Origin Shielding** page provides detailed caching diagnostics broken down by static asset types (images, scripts, stylesheets, fonts, video, documents) and analyzes Fastly origin shielding performance across edge PoP to shield PoP transitions.

### Key Tenets:
- **Asset Categorization:** Automatically groups file paths into MIME/content categories to evaluate cache hit ratios per asset class.
- **Shielding Topology Analysis:** Tracks edge PoP -> shield PoP -> origin fetch paths to quantify shield hit efficiency.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/assets-shield`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `content_type`, `pop`, `shield_pop`, `status`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full asset URLs, shield topology maps, cache headers diagnostics. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Asset breakdowns and shielding metrics viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `origin_summary` / `perf_dims` rollups. | Reads Postgres DuckLake or ClickHouse asset tables. |
| **Shield Evaluation** | Filter queries comparing `pop` vs `shield` headers. | Pre-aggregated shield dimension tables. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/assets-shield/bundle` (or composite bundle):
   - Returns asset type distribution (images, js, css, video), cache hit ratio per type, shield hit percentage, and shield PoP traffic share.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Shielding KPI Cards, Asset Class Donut, Shielding Flow Sankey/Map, and Top Cached Assets Table enforce container intrinsic sizes (`contain-intrinsic-size: 350px`).
- **In-Place Skeletons:** Panels display contextual loading copy (`Analyzing asset cache hit ratios...`, `Evaluating shield PoP efficiency...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Asset Type Filter:** Clicking "Images" filters all dashboard graphs to image assets.
- **No Shielding Configured:** If origin shielding is disabled on the Fastly service, displays an educational banner explaining how to configure origin shield.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Utilizes asset rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /assets-shield` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Asset Bundle Contract:** Verify asset classes and shield hit ratios calculate accurately.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Asset Filtering:** Clicking an asset class filters the data view.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/assets-shield.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Mixed web traffic with static JS/CSS, images, and origin-shielded requests.
