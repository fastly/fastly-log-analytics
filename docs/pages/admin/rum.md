# Page Specification: Admin RUM Ingestion & Configuration (`/admin/rum`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin/rum`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Admin RUM Ingestion** page configures and monitors Real User Monitoring (RUM) beacon ingestion pipelines. It displays incoming beacon volumes, sampling rates, Fastly VCL snippet generation for RUM JavaScript injection, DuckLake `client_vitals` and `client_errors` table health, and beacon retention settings.

### Key Tenets:
- **Telemetry Pipeline Configuration:** Controls RUM beacon sampling and Fastly logging destination configuration.
- **Client Script Generation:** Generates copy-paste JavaScript snippet for site integration.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/rum`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Configure sampling rates, regenerate client JS snippets, trigger RUM sync/compaction. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Ingest Jobs** | Managed via APScheduler (`rum_sync_{id}`, `rum_commit_{id}`). | Managed via RedBeat & Celery (`rum_discovery_{id}`, `ledger_rum_sweep_{id}`). |
| **Storage Tables** | Local DuckLake `lake.client_vitals` & `lake.client_errors`. | Postgres DuckLake or ClickHouse RUM tables. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/rum/status`:
   - Returns beacon ingest rates, total vitals rows, error beacon counts, and active configuration.
2. `POST /api/admin/rum/config`:
   - Updates RUM sampling rates and retention windows.
3. `POST /api/admin/rum/sync/{service_id}` (Trigger manual RUM sync).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** RUM Pipeline Status Cards, Client Script Snippet Box, and Ingestion Rate Chart enforce container intrinsic sizes (`contain-intrinsic-size: 350px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Inspecting RUM beacon pipeline...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Copy JavaScript Snippet:** One-click copy button copies the browser telemetry tag with embedded Fastly service ID.
- **Enable / Disable Toggle:** Toggling RUM collection updates service config and reloads the background scheduler dynamically.
- **Zero Beacons Received State:** Helpful diagnostic banner checking Fastly VCL snippet installation.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (RUM Status)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin/rum` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. RUM Status Contract:** Verify beacon volume and table row counts reflect current state.
- [ ] **4. Config Update:** (Admin) Adjust sampling percentage and verify saved config.
- [ ] **5. Role Verification:** Confirm Analyst Path B receives 403.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-rum.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Injected synthetic RUM beacons into raw FOS prefix to test pipeline.
