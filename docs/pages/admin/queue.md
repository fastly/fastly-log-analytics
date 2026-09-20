# Page Specification: Admin Task Queue & Ingest Ledger (`/admin/queue`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/admin/queue`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Admin Task Queue & Ingest Ledger** page provides deep operational visibility into background worker queues, Celery task throughput, RedBeat schedule entries, Valkey queue depth, and the Postgres `ingest_ledger` distributed state machine (`discovered` -> `claimed` -> `committed` / `quarantined` / `dead_letter`).

### Key Tenets:
- **Ledger State Transparency:** Inspects file claims, worker assignments, conversion durations, and retry counts.
- **Queue Health & Backpressure:** Monitors message lag across Celery conversion and commit queues.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analysts receive HTTP 403.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/queue`
- **Supported Query Parameters:**
  - `status`: Filter ledger rows by state (`all`, `discovered`, `claimed`, `committed`, `quarantined`, `dead_letter`).
  - `service`: Fastly Service ID.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full ledger inspection, retry dead-letter batches, purge stuck claims. |
| **Analyst Path B (Remote Share)** | Blocked (403) | Route access rejected. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Route access rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Queue Backend** | Shows simulated in-process APScheduler task queues. | Queries live Celery broker (Valkey/Redis) & Postgres `ingest_ledger`. |
| **Ledger Operations** | Reads SQLite `ingested_files`. | Full distributed `ingest_ledger` table with recovery actions. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/queue/status`:
   - Returns queue lengths, active workers, RedBeat tasks, and ledger counts by status.
2. `POST /api/admin/queue/retry/{file_id}` (Retry quarantined file).
3. `POST /api/admin/queue/sweep/{service_id}` (Manually trigger ledger crash sweep).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Queue Depth KPI Cards, Ledger State Flow Graph, and Quarantined Files Table enforce container intrinsic sizes (`contain-intrinsic-size: 400px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Inspecting Celery task broker...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Retry Quarantined File:** Administrator can click "Retry" on a failed log file to re-dispatch conversion.
- **Manual Sweep Trigger:** Click "Run Ledger Sweep" to trigger immediate crash-net recovery.
- **Zero Queue Lag State:** Displays green "All queues clear, 0 pending messages" indicator.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Queue Status)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright auth audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /admin/queue` returns HTTP 200 for Admin; returns HTTP 403 for Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Queue Status Contract:** Verify Celery queue depths and ledger state counts populate accurately.
- [ ] **4. Retry Action:** (Admin) Click retry on a quarantined item and verify status transitions to `discovered`.
- [ ] **5. Role Verification:** Confirm Analyst Path B receives 403.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/admin-queue.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Injected simulated ledger entries across all lifecycle states.
