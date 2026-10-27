# Page Specification: Admin Overview & Ingest Operations (`/admin`)

> [!IMPORTANT] **Live-Certified (`release/v3.0.0-beta3` — 2026-10-17)**
> This specification has been audited, hardened, and live-certified across Local Standard, Remote Standard GCE, and High-Scale Elevation environments. All ADR-22 PostgreSQL metadata contracts, DuckLake maintenance operations, RBAC restrictions, zero-CLS layout reservations, and Playwright E2E checks are verified.

---

## 1. Overview & Objectives

The **Admin Overview** page (`/admin`) is the administrative mission control for Fastly Log Analytics. It surfaces real-time operational health (`OperationsOverview` 4-card status row: Live Queries, Ingest Gap, Notable Slow Queries, Workers & Queues), Fastly service lifecycle CRUD (`ServicesTable` with provisioning wizard, credentials editor, cron settings, log field settings, NGWAF bot telemetry config, RUM settings, data deletion, and teardown), host/process/DuckDB/PostgreSQL vitals (`SystemHealthCard`), quarantined ingest batches (`QuarantineSection`), global debug/usage settings (`GlobalSettings`), and FOS cost estimator pricing (`PricingSettings`).

### Key Tenets:
- **Operational Control:** Allows administrators to monitor ingestion health, detect sustained log-accounting gaps, inspect live query activity, and trigger background lifecycle pipelines (`sync`, `commit`, `compact`, `optimize`).
- **Service Management:** Full CRUD management for configured Fastly services, FOS credentials, custom VCL field mappings, and DuckLake storage maintenance.
- **Unified PostgreSQL Metadata (ADR-22):** Operational state (`cron_runs`, `ingested_files`, `slow_queries`, `usage_log`, `sources`) is backed exclusively by PostgreSQL 16 (`METADATA_DSN`), while analytical tables are managed via DuckLake (`DUCKLAKE_CATALOG`) in Standard mode and ClickHouse MergeTree in High-Scale mode.
- **Strict RBAC:** Accessible **exclusively** to Admin sessions. Analysts (Remote Share) receive HTTP `403 Forbidden`.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin`
- **Sub-Navigation Links (`AdminPrefetchLinks`):** `/admin/queries`, `/admin/queue`, `/admin/trends`, `/admin/usage-log`, `/admin/session-scoring`, `/admin/rum`, `/admin/share`, `/admin/clickhouse`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full access to `/admin`, service configurations, credentials rotation, manual cron triggers (`sync`, `commit`, `compact`, `optimize`), quarantine inspection, and system vitals. |
| **Analyst (Remote Share)** | Blocked (`403 Forbidden`) | Blocked by `RemoteAccessGuardMiddleware` (`403 admin_only`) on all `/api/admin/*` and `/api/services/*` mutation routes. UI hides all admin navigation links. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Sync Status (`/api/admin/sync-status`, `/api/sync-status`)** | Reads PostgreSQL `cron_runs` (`METADATA_DSN`), local Parquet buffer state, and DuckDB/DuckLake snapshot extents. | Reads PostgreSQL `cron_runs` / `ingest_ledger`, Celery worker state, and ClickHouse watermark extents. |
| **System Health (`/api/admin/system-metrics`, `/api/admin/health`, `/api/admin/health-snapshot`)** | Reports process RSS/CPU, host disk/memory, DuckDB pool stats, and PostgreSQL metadata DB sizes. | Reports serving pod vitals, Celery worker queue depths, ClickHouse/PostgreSQL health, and storage metrics. |
| **Manual Triggers (`/api/admin/{sync,commit,compact,optimize}/{service_id}`)** | Invokes `sync_from_fos`, `commit_to_iceberg` (DuckLake), `compact_local_buffers`, or `optimize_table` in a background thread with SSE progress events. | Dispatches discovery/maintenance tasks or runs DuckLake/ClickHouse maintenance with PostgreSQL `cron_runs` audit tracking. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Consumed / Exposed:
1. `GET /api/admin/sync-status` & `GET /api/sync-status` (`SyncStatusResponse`):
   - Returns service readiness (`has_service`, `has_duckdb`, `has_logs`), `log_period`, `commit_interval_mins`, `last_ingested`, `last_ingested_request_at`, `total_events`, and active cron status.
2. `GET /api/admin/system-metrics`, `GET /api/admin/health`, & `GET /api/admin/health-snapshot` (`HealthSnapshotResponse`):
   - Returns `captured_at`, `process` (RSS, CPU, threads, open FDs), `system` (memory, disk), `duckdb` pool stats, `metadata_db` table counts, and per-service storage breakdown.
3. `GET /api/admin/compaction-status` & `GET /api/admin/compaction-status/{service_id}` (`CompactionStatusResponse`):
   - Returns `total_parquet_files`, `small_files_count`, `compacted_files_count`, and `needs_compaction`.
4. `GET /api/admin/log-accounting/{service_id}` & `GET /api/admin/log-accounting` (`LogAccountingResponse`):
   - Compares Fastly Stats API edge request/log counts against ingested rows over the last 24h and surfaces any `sustained_loss` alert.
5. `GET /api/admin/queries/summary` (`SummaryResponse`), `GET /api/admin/slow-queries/count`, & `GET /api/admin/celery/status`:
   - Powers the `OperationsOverview` top status cards.
6. `GET /api/services`, `POST /api/services`, `PATCH /api/services/{service_id}/settings`, `DELETE /api/services/{service_id}`:
   - Full service CRUD and lifecycle management.
7. Manual Operation Triggers (`SyncStartResponse`, HTTP `202 Accepted`):
   - `POST /api/admin/sync/{service_id}` & `POST /api/admin/sync-now`
   - `POST /api/admin/commit/{service_id}` & `POST /api/admin/commit-storage`
   - `POST /api/admin/compact/{service_id}` & `POST /api/admin/compact-local`
   - `POST /api/admin/optimize/{service_id}`

### Telemetry Attribution:
- Every request from `/admin` propagates `X-Page-Load-ID` via `client` / `adminFetch`.
- Database statements against PostgreSQL (`METADATA_DSN`) and DuckDB/ClickHouse are recorded with full `process_context` attribution.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** `frontend/app/admin/page.tsx` wraps each section (`OperationsOverview`, `ServicesTable`, `SystemHealthCard`, `QuarantineSection`, `GlobalSettings`, `PricingSettings`) in pre-allocated containers with `[contain-intrinsic-size:auto_...px] [content-visibility:auto]` (`data-testid="admin-operations-reservation"`, `data-testid="admin-services-reservation"`, `data-testid="admin-health-reservation"`, etc.).
- **In-Place Skeletons:** `frontend/app/admin/loading.tsx`, `OperationsOverview.tsx`, and `ServicesTable.tsx` display contextual loading copy (`Inspecting background scheduler state...`, `Loading service configs...`).
- **Dimmed Updates:** Background refetches and manual status refreshes (`data-testid="admin-overview-refresh-btn"`) dim active panels (`opacity-40 pointer-events-none`) with zero layout shift (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Edge Cases

- **Manual Status Refresh (`Refresh Status`):** Invalidates `['services']`, `['admin', 'overview']`, `['admin', 'health-snapshot']`, and `syncStatus` queries while propagating `X-Page-Load-ID`.
- **Manual Trigger Progress:** Triggering Sync, Commit, Compact, or Optimize returns HTTP `202 Accepted` (`status: "started"`) and streams real-time progress via `cron_progress` SSE.
- **Service Configuration & Credentials Modals:** Supports editing bucket/prefix/retention settings, custom VCL log fields (validated via `falco`), NGWAF credentials, and RUM settings.
- **Serviceless Fresh-Install Safety:** Global endpoints (`/api/admin/health-snapshot`, `/api/admin/system-metrics`, `/api/admin/health`, `/api/admin/system-jobs`, `/api/admin/queries/summary`, `/api/admin/celery/status`) are registered in `SERVICELESS_PATH_PREFIXES` so `/admin` renders cleanly even when zero services are configured.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.00 actual) | <= 0.05 (0.00 actual) | PerformanceObserver |
| **API Latency (Sync Status)** | < 150 ms (p95) | < 100 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% requests carry `X-Page-Load-ID` | 100% requests carry `X-Page-Load-ID` | Playwright request audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Playwright & pytest RBAC audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin` returns HTTP 200 for Admin; Analyst receives HTTP 403 on `/api/admin/sync-status`.
- [x] **2. Zero Layout Shift:** Page loads with pre-allocated `[contain-intrinsic-size:...]` containers and `CLS <= 0.05`.
- [x] **3. Ingest & System Health Contracts:** `/api/admin/sync-status`, `/api/admin/system-metrics`, `/api/admin/health`, `/api/admin/compaction-status/{service_id}`, and `/api/admin/log-accounting/{service_id}` return typed Pydantic models.
- [x] **4. Manual Triggers & Refresh:** Manual cron triggers (`/api/admin/{sync,commit,compact,optimize}/{service_id}`) return `202 Accepted` and the UI `Refresh Status` workflow re-fetches with `X-Page-Load-ID`.
- [x] **5. Services CRUD:** Configured services render in `ServicesTable` with status badges and management actions.
- [x] **6. Role Verification:** Analyst mutations receive HTTP `403 Forbidden`.
- [x] **7. Telemetry & Environment Indicator:** `ServerFooter` renders active environment indicator and all API fetches carry `X-Page-Load-ID`.

---

## 10. Automated Test Suite

- **Backend Unit & RBAC Tests:** `tests/routers/test_admin_overview.py` (7 tests) & `tests/remote_access/test_route_table_tripwire.py`.
- **Frontend Unit Tests:** `frontend/__tests__/app/admin-overview.test.tsx` (4 tests) & `frontend/__tests__/app/admin.test.tsx` (2 tests).
- **Playwright E2E Suite:** `frontend/e2e/pages/admin-overview.spec.ts` (5 end-to-end contract tests verified across Local Standard `3000`, Remote Standard GCE `4000`, and High-Scale Elevation `5000`).
