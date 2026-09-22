# Page Specification: Admin Session Scoring & Scorer Weights (`/admin/session-scoring`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin/session-scoring`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Admin Session Scoring** page manages machine learning scoring matrix weights, threshold boundaries (Low, Medium, High, Critical), exclusion regex rules, feature contributions, and model retraining for client behavioral session threat scoring.

### Key Tenets:
- **Scoring Matrix Customization:** Fine-tunes weights across 12 behavioral dimensions (burstiness, error frequency, path entropy, JA4 rarity).
- **Model Retraining:** Triggers re-computation of scoring matrices over historical log data.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/session-scoring`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Edit weights, configure threshold cutoffs, trigger retraining, manage exclusion rules. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Scorer Config** | Stored in per-service config JSON and SQLite metadata. | Stored in shared Postgres or per-service config. |
| **Retraining Job** | Runs inline or in background thread over DuckDB data. | Dispatches Celery retraining task over DuckLake / ClickHouse data. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/session-scoring/config/{service_id}`:
   - Returns active feature weights, threshold cutoffs, and exclusion regexes.
2. `PUT /api/session-scoring/config/{service_id}`:
   - Updates weights and thresholds.
3. `POST /api/session-scoring/retrain/{service_id}`:
   - Triggers model retraining.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Feature Weight Sliders Grid, Threshold Cutoff Visualizer, and Exclusion Rules Drawer enforce container intrinsic sizes (`contain-intrinsic-size: 450px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Loading scoring matrix parameters...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Interactive Weight Sliders:** Adjusting weights re-normalizes the sum to 1.0 and previews score distribution changes.
- **Exclusion Regex Tester:** Operators can input a user-agent or path to verify if it will be excluded from threat scoring.
- **Retrain Progress:** Triggering retraining opens a progress modal tracking batch completion.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Config)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin/session-scoring` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Weights Contract:** Verify current feature weights and threshold boundaries load accurately.
- [ ] **4. Weight Adjustment:** Adjust a slider, click save, and verify config updates.
- [ ] **5. Role Verification:** Confirm Analyst Path B receives 403.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-session-scoring.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Pre-computed session records to test weight re-evaluations.
