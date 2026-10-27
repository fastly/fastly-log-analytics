# Page Specification: Data Management (`/logs`)

> [!IMPORTANT] **Live-Certified (`release/v3.0.0-beta4` — 2026-10-21)**
> This specification has been audited, hardened, and live-certified across Local Standard, Remote Standard GCE, and High-Scale Elevation environments. All ADR-22 PostgreSQL metadata contracts, DuckLake maintenance operations, RBAC restrictions, zero-CLS layout reservations, and Playwright E2E checks are verified.

---

## 1. Overview & Objectives

The **Data Management** page (`/logs`) is the operational hub for inspecting and managing the per-service ingestion pipeline, scheduled background crons, configuration audit trail, ingested FOS files, DuckLake storage snapshots, PostgreSQL metadata table footprint, raw FOS objects, and active DuckDB analytical schema.

> [!NOTE]
> The raw log explorer and live tail stream that temporarily shared `/logs` were retired in `v3.0.0` (commit `856febe6`); ad-hoc raw log inspection lives on [`/query`](query.md) (`SQL Query Pad`) and [`/high-scale/request-facts`](high-scale-request-facts.md), while live request tailing lives on [`/control-room`](control-room.md) and [`/sessions/stream`](sessions-stream.md).

### Key Tenets:
- **End-to-End Pipeline Visibility:** Unifies scheduled job schedules, execution histories (`cron_runs`), configuration audit trails (`audit_logs`), ingested `.gz` file ledgers (`ingested_files`), DuckLake catalog/file trees, PostgreSQL metadata table sizes, raw FOS object trees, and DuckDB table/view schemas into a single 7-tab workspace.
- **Interactive Pipeline Controls (`QuickActionsBar` & `FloatingOperationsDock`):** Provides one-click administrative triggers for **Import Logs**, **Backfill Logs**, **Commit Buffer**, **NGWAF Bot Sync** (when NGWAF workspace is configured), and **View Recent Logs**, with live Server-Sent Events (SSE) progress modals and background dock status tracking.
- **Unified PostgreSQL Metadata (ADR-22):** All operational tables (`cron_runs`, `audit_logs`, `ingested_files`, `sources`, `usage_log`) reside in PostgreSQL 16 (`METADATA_DSN`), while analytical storage is backed by DuckLake (`DUCKLAKE_CATALOG`) in Standard mode and ClickHouse MergeTree + DuckLake archive in High-Scale mode.
- **Strict RBAC:** `/logs` is an **Admin** data-management surface; **Analyst (Remote Share)** sessions are blocked at both the UI shell (`AppLayout` redirect to `/dashboard`) and the API boundary (`RemoteAccessGuardMiddleware` returning `403 Forbidden`).

---

## 2. Routes, URLs & Query Parameters

- **Primary Route:** `/logs`
- **Supported Query Parameters:**
  - `service` (optional string): Target `service_id` for SSR hydration (`fetchCronRunsServerSide(serviceId)` in [`frontend/lib/ssr/logs.ts`](../../frontend/lib/ssr/logs.ts)) and initial client selection.
  - `tab` (optional string, default `'cron'`): Initial active tab (`cron`, `service_history`, `ingestion`, `storage`, `metadata_storage`, `raw`, or `schema`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | Full access to all 7 tabs (`cron`, `service_history`, `ingestion`, `storage`, `metadata_storage`, `raw`, `schema`), `QuickActionsBar` triggers (`Import Logs`, `Sync from Cloud`, `Commit Buffer`, `NGWAF Bot Sync`, `View Recent Logs`), cron history purge (`DELETE /api/cron-runs`), ingested file deletion (`DELETE /api/admin/ingested-files`), and DuckLake maintenance triggers (`compact`, `optimize`, `expire-snapshots`, `metadata-cleanup`). |
| **Analyst (Remote Share)** | Blocked (`403 Forbidden` / Redirect) | Hidden from sidebar navigation (`shareAnalystVisible: false` in [`AppLayout.tsx`](../../frontend/components/AppLayout.tsx)). Navigating directly to `/logs` renders an accessible polite status banner (`"This page is not available for your account. Redirecting to the dashboard."`) and redirects to `/dashboard`. All backing endpoints (`/api/cron-runs`, `/api/cron-schedule`, `/api/audit-logs`, `/api/admin/*`) are blocked by `RemoteAccessGuardMiddleware` (`403 admin_only`). |

---

## 4. Architecture Execution Matrix

| Subsystem / Tab | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Cron Runs (`cron`)** | Reads `cron_runs` from PostgreSQL (`METADATA_DSN`) and live APScheduler job metadata (`log_discovery`, `commit`, `local_compact`, `rollup_heal`, `rollup_compact`, `optimize`, `expire`, `full_sync`, `gap_heal`, `rum_sync`, `rum_commit`, `metadata_sync`, `metadata_cleanup`, `alerts_evaluation`). | Reads `cron_runs` from PostgreSQL (`METADATA_DSN`) and High-Scale scheduler/Celery jobs (`discover_new_files`, `ledger_sweep`, `expire_snapshots`, `metadata_cleanup`, `alerts_evaluation`, etc.). |
| **Service History (`service_history`)** | Reads per-service `audit_logs` from PostgreSQL (`METADATA_DSN`), synced to FOS `admin_state.json`. | Reads per-service `audit_logs` from shared PostgreSQL (`METADATA_DSN`). |
| **Ingestion History (`ingestion`)** | Lists and filters `ingested_files` from PostgreSQL (`METADATA_DSN`); supports deleting rows so `.gz` files are re-ingested on the next discovery sweep. | Lists `ingested_files` / `ingest_ledger` state from PostgreSQL (`METADATA_DSN`). |
| **DuckLake Storage (`storage`)** | Inspects DuckLake snapshot metadata, local compaction status, and Parquet file tree via `DUCKLAKE_CATALOG` and local/FOS storage. | Inspects shared PostgreSQL DuckLake catalog (`DUCKLAKE_CATALOG`) and durable cloud storage Parquet tree. |
| **Metadata Storage (`metadata_storage`)** | Queries PostgreSQL (`pg_total_relation_size`) and per-service row counts across operational tables (`ingested_files`, `usage_log`, `cron_runs`, `slow_queries`, `audit_logs`, etc.); triggers `metadata_cleanup`. | Queries shared PostgreSQL (`METADATA_DSN`) table sizes and row counts; triggers `metadata_cleanup`. |
| **Available Logs (`raw`)** | Streams hierarchical S3/FOS prefix tree (`raw/request/`, `raw/rum/`) with date-range filtering and ZIP/single-file download. | Streams hierarchical S3/FOS prefix tree from cloud object storage. |
| **DuckDB Schema (`schema`)** | Executes `DESCRIBE logs` against the per-service DuckDB connection (stitching DuckLake + local Parquet buffer) and caches schema metadata. | Executes `DESCRIBE logs` against the serving engine / DuckLake view. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Consumed / Exposed:
1. `GET /api/cron-runs` & `DELETE /api/cron-runs` ([`backend/routers/services/cron.py`](../../backend/routers/services/cron.py)):
   - Supports `service_id`, `task`, `status`, `limit` (default `500`), and `since_id` (delta polling for newly completed or running jobs).
   - `DELETE /api/cron-runs?service_id={id}` purges historical cron execution rows for the service (`require_write_access`).
2. `GET /api/cron-schedule` ([`backend/routers/services/cron.py`](../../backend/routers/services/cron.py)):
   - Returns configured APScheduler intervals, next-run timestamps, and enabled flags per service (`CronScheduleSection`).
3. `GET /api/audit-logs` ([`backend/routers/services/audit.py`](../../backend/routers/services/audit.py)):
   - Returns up to `500` configuration and operational audit events (`action`, `details`, `created_at`) for the selected service.
4. `GET /api/admin/ingested-files` & `DELETE /api/admin/ingested-files` ([`backend/routers/admin/ingest.py`](../../backend/routers/admin/ingest.py)):
   - Paginated/filtered list of ingested `.gz` log objects (`search`, `limit`) and batch deletion by file path to force re-ingestion.
5. `GET /api/schema` ([`backend/routers/services/core.py`](../../backend/routers/services/core.py)):
   - Returns column names, DuckDB types, and nullability for the `logs` analytical table/view (`SchemaTab`).
6. DuckLake & Storage Inspection Endpoints ([`backend/routers/admin/`](../../backend/routers/admin/)):
   - `GET /api/admin/storage-info` (`StorageStatus` card)
   - `GET /api/admin/storage-calendar` (`StorageCalendar` daily heatmap)
   - `GET /api/admin/compaction-status` (local Parquet compaction metrics)
   - `GET /api/admin/storage-tree` & `GET /api/admin/tree/raw` (`FileBrowser` lazy directory trees)
   - `GET /api/admin/metadata-storage` & `POST /api/admin/metadata-cleanup` (`MetadataStorageCard`)
7. Interactive Action & SSE Streaming Endpoints:
   - `POST /api/admin/ingest-logs` & `GET /api/cron-runs/{run_id}/stream` (manual log discovery & SSE stream)
   - `POST /api/admin/commit-storage` & `GET /api/cron-runs/{run_id}/stream` (manual commit of local Parquet buffer to DuckLake & SSE stream)
   - `POST /api/services/{service_id}/ngwaf-sync` (SSE NGWAF verified bot sync)
   - `POST /api/admin/ingest-logs?start_time=&end_time=` (`BackfillRangeModal` historical backfill)

### Telemetry Attribution:
- Every client fetch propagates `X-Page-Load-ID` via `client` / `apiFetch`.
- All PostgreSQL (`METADATA_DSN`), DuckDB (`DUCKLAKE_CATALOG`), and FOS S3 calls are instrumented with `RequestTelemetry` and attributed in `usage_log`.

---

## 6. UI Components, Panels & Tabs

- **Server Component Entry ([`frontend/app/logs/page.tsx`](../../frontend/app/logs/page.tsx)):**
  - Pre-fetches initial `cron-runs` (`limit=500`) server-side via `fetchCronRunsServerSide(serviceId)` and hydrates `queryKeys.admin.cronLogs(serviceId ?? null, 'all', 'all')` so the default `Cron Runs` tab renders on first frame with zero loading flicker.
- **Zero-CLS Skeleton ([`frontend/app/logs/loading.tsx`](../../frontend/app/logs/loading.tsx)):**
  - Reserves exact dimensions for `PageHeader` ("Data Management"), `QuickActionsBar` (`h-12`), `TabsList` (`h-10`), and a 10-row table skeleton (`h-[440px]`).
- **`QuickActionsBar` ([`frontend/app/logs/_sections/QuickActionsBar.tsx`](../../frontend/app/logs/_sections/QuickActionsBar.tsx)):**
  - Provides **Import Logs**, **Backfill Logs**, **Commit Buffer**, **NGWAF Bot Sync** (conditional on `ngwaf_workspace_id`), and **View Recent Logs** buttons.
- **7-Tab Workspace ([`frontend/app/logs/_sections/LogsClient.tsx`](../../frontend/app/logs/_sections/LogsClient.tsx)):**
  1. **Cron Runs (`cron`):** `CronTab` (`CronScheduleSection` collapsible schedule overview + filterable `cron_runs` table by task & status, live elapsed duration counter for `running` jobs, and `Purge History` action).
  2. **Service History (`service_history`):** `ServiceHistoryTab` displaying chronological configuration and lifecycle audit events with action badges (`provision`, `update_settings`, `commit`, `sync`, etc.).
  3. **Ingestion History (`ingestion`, Admin-only):** `IngestionTab` with substring search, multi-select checkboxes, and `Delete Selected` (removes entries from `ingested_files` so they re-ingest on next sync).
  4. **DuckLake Storage (`storage`, lazy-mounted):** `StorageStatus` + `StorageCalendar` + `FileBrowser(type="storage")` for browsing committed Parquet partitions and snapshots.
  5. **Metadata Storage (`metadata_storage`, Admin-only, lazy-mounted):** `MetadataStorageCard` showing PostgreSQL table row counts, disk bytes, retention windows, and a `Run Cleanup Now` trigger.
  6. **Available Logs (`raw`, Admin-only, lazy-mounted):** `FileBrowser(type="raw")` with date-range picker and bulk ZIP / single-file download from FOS `raw/`.
  7. **DuckDB Schema (`schema`, lazy-mounted):** `SchemaTab` listing all columns and data types in the active `logs` view with instant column search.
- **`FloatingOperationsDock` & `SSEModal`:**
  - Tracks running background/manual operations across tab switches and streams live terminal-style SSE logs when opened.

---

## 7. Interactive Workflows & Edge Cases

- **Delta Polling for Cron Runs:** `useLogsPageState` ([`frontend/app/logs/_state.ts`](../../frontend/app/logs/_state.ts)) polls `/api/cron-runs?since_id={latestCronIdRef}` every 15s (or 3s while a job is in `running` state) and merges updates in-place without resetting scroll position or collapsing the table into a skeleton.
- **Lazy Tab Mounting:** Heavy storage and S3 tree components (`StorageStatus`, `StorageCalendar`, `MetadataStorageCard`, `FileBrowser`, `SchemaTab`) only mount and issue network requests when their tab becomes active (`activeTab === '...'`), keeping initial `/logs` page load fast and free of unnecessary S3 LIST operations.
- **Analyst Redirect:** Share analysts (`isShareAnalyst` / `isAnalyst`) are immediately blocked by `AppLayout` with an ARIA-live status message and redirected to `/dashboard`.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / Shell Paint** | < 400 ms | < 400 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.00 actual) | <= 0.05 (0.00 actual) | PerformanceObserver |
| **API Latency (`GET /api/cron-runs`)** | < 250 ms (p95) | < 250 ms (p95) | HAR log / API benchmark |
| **API Latency (`GET /api/cron-schedule`)** | < 100 ms (p95) | < 100 ms (p95) | HAR log / API benchmark |
| **API Latency (`GET /api/audit-logs`)** | < 150 ms (p95) | < 150 ms (p95) | HAR log / API benchmark |
| **API Latency (`GET /api/schema`)** | < 200 ms (p95) | < 200 ms (p95) | HAR log / API benchmark |
| **Zero Unsolicited S3 Calls on Initial Load** | 0 FOS Class A/B ops on `cron` tab | 0 FOS Class A/B ops on `cron` tab | `usage_log` attribution audit |
| **RBAC Enforcement** | 100% Analyst requests 403 / redirected | 100% Analyst requests 403 / redirected | Playwright & pytest RBAC audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility & SSR Hydration:** `GET /logs` returns HTTP 200 for Admin with `"Data Management"` heading and pre-hydrated `Cron Runs` table; Analyst is redirected to `/dashboard` and receives HTTP `403 Forbidden` on `/api/cron-runs`, `/api/cron-schedule`, `/api/audit-logs`, and `/api/admin/ingested-files`.
- [x] **2. Zero Layout Shift:** Page loads with `CLS == 0.000` and zero console or uncaught page errors.
- [x] **3. Quick Actions Bar:** Renders `Import Logs`, `Sync from Cloud`, `Commit Buffer`, and `View Recent Logs` buttons for Admin.
- [x] **4. 7-Tab Navigation:** Switching across `Cron Runs`, `Service History`, `Ingestion History`, `DuckLake Storage`, `Metadata Storage`, `Available Logs`, and `DuckDB Schema` renders each tab panel cleanly without collapsing previously loaded tabs or throwing runtime errors.
- [x] **5. Cron Schedule & Filters:** `CronScheduleSection` toggles open/closed and `CronRunsTab` filters by task and status (`all`, `success`, `error`, `running`).
- [x] **6. Backend Endpoint Contracts:** `/api/cron-runs`, `/api/cron-schedule`, `/api/audit-logs`, `/api/admin/ingested-files`, and `/api/schema` return valid JSON payloads within p95 latency budgets across all 3 environments.

---

## 10. Automated Test Suite

- **Backend Unit & RBAC Tests:** `tests/routers/services/test_cron_router.py`, `tests/routers/test_metadata_storage.py`, and `tests/remote_access/test_route_table_tripwire.py`.
- **Frontend Unit Tests:** `frontend/__tests__/app/logs.test.tsx`, `frontend/__tests__/app/logs/CronExplanations.test.ts`, `frontend/__tests__/app/logs/QuickActionsBar.test.tsx`, `frontend/__tests__/ssr/logs_usage_log.test.ts`, and `frontend/__tests__/components/SchemaAndBotTables.test.tsx`.
- **Playwright E2E Suite:** `frontend/e2e/pages/logs.spec.ts` (end-to-end contract and RBAC verification across Local Standard, Remote Standard GCE, and High-Scale Elevation).
