# Page Specification: Admin Task Queue & Ingest Ledger (`/admin/queue`)

> [!IMPORTANT] **Page Specification Status: Live-Certified (2026-10-17)**
> This specification has been audited, hardened, and live-certified across Local Standard, Remote Standard GCE, and High-Scale Elevation environments.

---

## 1. Overview & Objectives

The **Admin Task Queue & Ingest Ledger** page provides deep operational visibility into background worker queues, Celery task throughput, RedBeat schedule entries, Valkey queue depth, 1-hour metric trend sparklines (`celery_queue_depth` and `celery_active_tasks`), and the PostgreSQL `ingest_ledger` (`METADATA_DSN` per ADR-22) distributed state machine (`discovered` -> `claimed` -> `committed` / `quarantined` / `dead_letter`).

### Key Tenets:
- **Ledger State Transparency:** Aggregates file claim states (`discovered`, `claimed`, `committed`, `quarantined`, `dead_letter`) across all configured services in PostgreSQL `ingest_ledger` (`METADATA_DSN`), explicitly scoped per `service_id` (`WHERE service_id = ?`) so shared PostgreSQL catalog tables never double-count rows across services.
- **Queue Health & Backpressure:** Monitors message depth across `KNOWN_QUEUES` (`celery`, `q.ingest`, `q.control`) plus any dynamically routed `q.*` queues in Valkey, active worker task assignments, RedBeat schedule entries (`redbeat:*` hashes in Valkey), and 1-hour historical queue depth / active task sparklines.
- **Strict RBAC:** Accessible **exclusively** to Admin (`read_write`) users. Analyst Path B (Remote Share) and Analyst Path A receive HTTP `403 Forbidden` (`{"error": "admin_only"}`).

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/queue`
- **Navigation & Controls:**
  - `<BackToAdminLink />` returning to `/admin`.
  - Broker connectivity status badge (`data-testid="admin-queue-broker-badge"`: `Broker Reachable` vs `Broker Unreachable / Standard Mode`).
  - Manual `Refresh` action button (`data-testid="admin-queue-refresh-btn"`) triggering concurrent refetches of `/api/admin/celery/status` and `/api/admin/metric-history/batch?since=1h`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full queue depth telemetry, active worker task inspection, RedBeat schedules, PostgreSQL `ingest_ledger` state counts, and 1h metric trend sparklines. |
| **Analyst Path B (Remote Share)** | Blocked (403) | All `/api/admin/celery/status` and `/api/admin/metric-history/batch` requests rejected with HTTP `403 Forbidden` (`{"error": "admin_only"}`) via `_ANALYST_BLOCKED_PREFIXES` and route-level `Depends(require_admin)`. |
| **Analyst Path A (JSON Join)** | Blocked (403) | Administrative queue & broker telemetry rejected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Celery Broker & Workers (`get_celery_status`)** | Gracefully handles Valkey/Celery absence (`broker_reachable: false`, `workers.count: 0`, `queues: {}`, `schedules: []`), displaying `Broker Unreachable / Standard Mode` and informative fallback states (`No queues found or Celery mode is inactive.`, `No active workers found.`, `No schedules found.`). | Connects to live Valkey/Redis broker (`CELERY_BROKER_URL`), inspects active/registered/scheduled worker tasks via `celery_app.control.inspect(timeout=1.0)`, reads LLEN across `KNOWN_QUEUES` (`celery`, `q.ingest`, `q.control`) and `q.*`, and scans `redbeat:*` HASH schedule definitions. |
| **Ingest Ledger (`ingest_ledger_summary`)** | Queries PostgreSQL `ingest_ledger` (`METADATA_DSN`, ADR-22) scoped by `WHERE service_id = ?` across configured services (`Ledger is empty.` when no distributed claims exist). | Queries shared PostgreSQL `ingest_ledger` (`METADATA_DSN`, ADR-22) scoped per configured `service_id`, aggregating distributed worker claim and commit states (`discovered`, `claimed`, `committed`, `quarantined`, `dead_letter`). |
| **Metric History Sparklines (`metric_snapshots`)** | Queries PostgreSQL `metric_snapshots` (`METADATA_DSN`, ADR-22) for `celery_queue_depth` and `celery_active_tasks` over the last 1h (`since=1h`). | Queries PostgreSQL `metric_snapshots` (`METADATA_DSN`, ADR-22) populated every minute by `metric_snapshot`. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/admin/celery/status` (`CeleryStatusResponse` in `backend/routers/admin/celery_status.py` / `backend/celery_status.py`):
   - Returns `broker_reachable` (`bool`), `workers` (`WorkersSummary`: `count`, `active_tasks`, `stats`, `registered`, `scheduled`, `error`), `queues` (`dict[str, int]`), `schedules` (`list[ScheduleItem]`), and `ledger` (`dict[str, int]` across `ingest_ledger` states).
   - Protected by both router-level and route-level `Depends(require_admin)`.
2. `GET /api/admin/metric-history/batch?since=1h` (`MetricHistoryBatchResponse` in `backend/routers/admin/metric_history.py`):
   - Returns `{ series: { celery_queue_depth: [{ ts, value }], celery_active_tasks: [{ ts, value }], ... } }` from PostgreSQL `metric_snapshots` (`METADATA_DSN`).
   - Protected by both router-level and route-level `Depends(require_admin)`.
3. `GET /api/admin/events/stream?channels=celery-status` (SSE via `useAdminStream('celery-status')`):
   - Pushes real-time `CeleryStatus` snapshots every ~3s and populates the React Query cache (`['admin', 'celery-status']`).

### Telemetry Attribution:
- Every frontend fetch carries `X-Page-Load-ID` via `apiFetch`.
- All `/api/admin/celery/status` and `/api/admin/metric-history/batch` paths are registered in `SERVICELESS_PATHS` in `frontend/lib/api.ts` so `/admin/queue` functions even before services are provisioned.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** All 5 sections enforce CSS layout containment (`[content-visibility:auto]`) and intrinsic sizing (`[contain-intrinsic-size:auto_*]`) with `CLS <= 0.05`:
  - `data-testid="admin-queue-trends-reservation"`: `[contain-intrinsic-size:auto_200px]` (1h Queue Depth & Active Tasks sparklines).
  - `data-testid="admin-queue-depths-reservation"`: `[contain-intrinsic-size:auto_200px]` (Queue Depths table).
  - `data-testid="admin-queue-workers-reservation"`: `[contain-intrinsic-size:auto_200px]` (Active Workers list).
  - `data-testid="admin-queue-ledger-reservation"`: `[contain-intrinsic-size:auto_180px]` (Ingest Ledger Status state cards).
  - `data-testid="admin-queue-schedules-reservation"`: `[contain-intrinsic-size:auto_200px]` (RedBeat Schedules table).
- **In-Place Skeletons:** Route-level `loading.tsx` (`data-testid="admin-queue-loading"`) and in-place `AnalyticsCard` skeletons render contextual loading copy (`Inspecting Celery task broker...`, `Loading queue telemetry...`) instead of collapsing into a raw `<div>Loading...</div>`.
- **Non-Destructive Refresh & Dimming:** Manual `Refresh` (`data-testid="admin-queue-refresh-btn"`) and background polling dim active card contents (`opacity-40 pointer-events-none transition-opacity`) with zero layout shift (`CLS = 0.00`).

---

## 7. Interactive Workflows & Edge Cases

- **Manual Refresh (`data-testid="admin-queue-refresh-btn"`):** Administrator can click `Refresh` at any time to trigger concurrent refetches of `/api/admin/celery/status` and `/api/admin/metric-history/batch?since=1h` with `X-Page-Load-ID` attribution while keeping existing data visible and dimmed.
- **Standard Mode (APScheduler / Unreachable Broker):** Displays `Broker Unreachable / Standard Mode` badge, surfaces any inspect warning, and renders clean empty-state messaging across Queue Depths (`No queues found or Celery mode is inactive.`), Active Workers (`No active workers found.`), Ingest Ledger Status (`Ledger is empty.`), and RedBeat Schedules (`No schedules found.`).
- **High-Scale Mode (Active Workers & Ledger State Cards):** Displays `Broker Reachable` badge, renders `data-testid="admin-queue-depths-table"`, lists active workers and in-flight task arguments (`data-testid="admin-queue-workers-list"`), renders `data-testid="admin-queue-ledger-cards"` per status, and lists RedBeat schedule definitions (`data-testid="admin-queue-schedules-table"`).

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 1200 ms | < 1200 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.00 actual) | <= 0.05 (0.00 actual) | PerformanceObserver |
| **API Latency (Queue Status)** | < 150 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `X-Page-Load-ID` & query profiler |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright & Pytest auth audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin/queue` returns HTTP 200 for Admin; returns HTTP 403 (`{"error": "admin_only"}`) for Analyst Path B on `/api/admin/celery/status` and `/api/admin/metric-history/batch`.
- [x] **2. Zero Layout Shift:** Verified page loads with pre-allocated panel reservations (`[contain-intrinsic-size:auto_*]`) and `CLS <= 0.05`.
- [x] **3. Queue Status & Metric History Contracts:** Verified `/api/admin/celery/status` and `/api/admin/metric-history/batch?since=1h` populate queue depths, worker states, RedBeat schedules, `ingest_ledger` state counts, and 1h sparkline series accurately.
- [x] **4. Per-Service Ledger Scoping (ADR-22):** Verified `ingest_ledger_summary()` scopes queries by `WHERE service_id = ?` on the shared PostgreSQL `ingest_ledger` table so multi-service deployments never multiply counts.
- [x] **5. Role Verification:** Confirmed Analyst Path B receives HTTP 403 in `tests/routers/test_admin_celery.py` and `frontend/e2e/pages/admin-queue.spec.ts`.
- [x] **6. Interactive Refresh & Telemetry Audit:** Verified manual `Refresh` (`data-testid="admin-queue-refresh-btn"`) triggers refetches with `X-Page-Load-ID` header propagation.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit Tests:** `tests/routers/test_admin_celery.py` & `tests/routers/test_admin_celery_status.py` (13 tests covering queue lengths, worker status, RedBeat schedules, per-service `ingest_ledger` state counts, Standard vs High-Scale fallbacks, metric history batch sparkline series, and Analyst Path B 403 rejection).
- **Frontend Unit Tests:** `frontend/__tests__/app/admin-queue.test.tsx` (4 tests covering `loading.tsx`, initial skeleton reservations without raw `Loading...` text, populated High-Scale queue/worker/ledger/schedule cards + manual refresh, and Standard-mode offline broker fallback).
- **Playwright E2E Suite:** `frontend/e2e/pages/admin-queue.spec.ts` (5 E2E contract tests certified across Local Standard, Remote Standard GCE, and High-Scale Elevation).
