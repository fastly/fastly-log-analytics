# Page Specification: Admin Overview & Ingest Operations (`/admin`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Admin Overview** page is the administrative mission control for Fastly Log Analytics. It displays ingestion sync health, Celery/APScheduler worker status, Parquet compaction and table optimization metrics, DuckDB connection pool stats, disk and memory utilization, service CRUD management, and manual operation triggers (Sync Now, Commit Now, Compact Now, Optimize Now).

### Key Tenets:
- **Operational Control:** Allows administrators to monitor and trigger all background lifecycle pipelines.
- **Service Management:** Full CRUD management for configured Fastly services, FOS credentials, and custom VCL field mappings.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403 Forbidden.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin`
- **Supported Query Parameters:**
  - `tab`: Selected admin view (`services`, `sync`, `compaction`, `health`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full view of credentials, service configurations, manual cron triggers, and system vitals. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. UI hides all admin navigation links. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Sync Status** | Reads local SQLite `cron_runs` and APScheduler job state. | Reads RedBeat Celery status and Postgres `ingest_ledger`. |
| **Manual Trigger** | Invokes local job function directly in background thread. | Dispatches Celery task to worker queue. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/sync-status`:
   - Returns last sync, last commit, files queued, buffer size, and active cron errors.
2. `POST /api/admin/sync/{service_id}` (Trigger manual sync).
3. `POST /api/admin/commit/{service_id}` (Trigger manual commit).
4. `POST /api/admin/compact/{service_id}` (Trigger manual local compact).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** System Health Bar, Services Grid, Ingestion Status Cards, and Action Drawer enforce container intrinsic sizes (`contain-intrinsic-size: 400px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Inspecting background scheduler state...`, `Loading service configs...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Manual Trigger Progress:** Clicking "Sync Now" opens a progress indicator listening to Server-Sent Events from `cron_progress`.
- **Service Configuration Modal:** Allows editing bucket name, prefix, log period, and custom log fields with Falco VCL validation.
- **Scheduler Error Banners:** If any cron job failed in its last run, an amber warning banner highlights the failure details.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Sync Status)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Ingest Status Contract:** Verify last sync, last commit, and queued files reflect current state.
- [ ] **4. Manual Trigger:** Click "Sync Now" and verify job runs to completion with progress updates.
- [ ] **5. Services CRUD:** Verify configured services render with correct status badges.
- [ ] **6. Role Verification:** Confirm Analyst Path B receives 403 and cannot access `/admin`.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-overview.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Simulated ingestion state with active service configs and historical cron run records.
