# Page Specification: Client Session Scoring (`/sessions`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/sessions`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Client Sessions** page analyzes behavioral session clusters across client IP, TLS JA4, and user-agent fingerprints. It runs machine learning / statistical session scoring models to detect account takeover, credential stuffing, scraping bots, and API abuse.

### Key Tenets:
- **Session Clustering:** Groups high-frequency request sequences into logical client sessions.
- **Threat Scoring Matrix:** Evaluates 12 behavioral dimensions (burstiness, error frequency, path entropy, JA4 rarity, 401/403 rates).
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/sessions`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `score_tier` (`high`, `medium`, `low`), `ja4`, `country`, `asn`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full session details, unmasked client IPs, access to retrain models and score weighting. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Session distributions and threat clusters viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `sessions` rollups. | Reads Postgres DuckLake / ClickHouse session aggregates. |
| **Scoring Engine** | Python ML scoring pipeline running over DuckDB aggregates. | Worker-evaluated scoring models / ClickHouse feature queries. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/sessions/bundle` (or composite bundle):
   - Returns threat score distribution (Low / Medium / High / Critical), top suspicious sessions table, and JA4 fingerprint clusters.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Threat Tier Scorecard, Score Distribution Histogram, and Suspicious Sessions Table enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Clustering client sessions...`, `Evaluating threat matrices...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Session Detail Drawer:** Clicking a suspicious session opens the session inspector displaying all requests in the sequence.
- **Low Threat State:** Services with zero elevated threat scores show clear green indicator banners.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes session rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /sessions` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Threat Scoring Contract:** Verify session score tiers (Low, Medium, High, Critical) calculate properly.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Session Inspector:** Clicking a session opens the drawer and shows request history.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked IPs and cannot access admin scoring configs.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/sessions.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Injected aggressive scraper session making 50 requests/sec with high 404/403 rates.
