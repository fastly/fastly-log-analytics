# Page Specification: Admin FOS Usage & Class A/B Ledger (`/admin/usage-log`)

**Status:** **Live-certified** (2026-10-19 — Audited, hardened, and live-verified across Local Standard, Remote Standard GCE, and High-Scale Elevation on `release/v3.0.0-beta4`)

---

## 1. Overview & Objectives

The **Admin FOS Usage & Class A/B Ledger** page (`/admin/usage-log`) provides granular auditability into Fastly Object Storage (FOS) Class A (`PUT`, `POST`, `LIST`, `COPY`) and Class B (`GET`, `HEAD`) operations alongside Fastly CDN egress cost tracking and Fastly Stats API vs. ingested-row accounting (`LogAccountingPanel`). It attributes every cloud storage and CDN API operation to its exact caller (`process_context` and `function_name`), tracks response bytes and status codes, computes estimated dollar costs from configurable rate cards, and supports CSV export and manual log purging.

### Key Tenets:
- **Zero Dark Cloud Ops:** Every S3/FOS and Fastly CDN API call executed by any API route, background scheduler job, or Celery worker is logged with `process_context`, `function_name`, `operation_class`, and `operation_type` attribution.
- **Two-Stage Query Pattern (Fast First Paint):** Pre-fetches the initial 50-row `head` slice + aggregate KPI totals server-side (`fetchUsageLogServerSide` in [`frontend/lib/ssr/usage_log.ts`](../../../frontend/lib/ssr/usage_log.ts)) and hydrates via `HydrationBoundary`, then loads the full 500-row table slice in the background with `include_aggregate=false` to skip redundant SQL `SUM()` scans.
- **Durable Hourly Rollup Summaries:** Backed by PostgreSQL 16 `usage_log` (detailed call records) and `usage_log_hourly_summary` (compact hourly rollups preserved across detailed-row purges) per [ADR-22](../../adr/22-postgres-only-metadata.md).
- **Strict Admin-Only RBAC:** Accessible **exclusively** to Admin (`read_write`) operators. Analyst remote-share sessions receive `403 Forbidden` with `{"error": "admin_only"}`.

---

## 2. Routes, Entrypoints & URL Schema

- **Primary Route:** `/admin/usage-log`
- **Supported URL Query Parameters:**
  - `service`: Optional Fastly Service ID (falls back to `useServiceStore.activeServiceId`).
- **Frontend Entrypoints:**
  - [`frontend/app/admin/usage-log/page.tsx`](../../../frontend/app/admin/usage-log/page.tsx) — Force-dynamic RSC shell that computes the minute-floored 24h window (`[now - 24h, now]`), pre-fetches the 50-row head slice via `fetchUsageLogServerSide()`, and seeds `['usage-log', 'head', serviceId ?? null, start, end, '', '', '']` into `HydrationBoundary`.
  - [`frontend/app/admin/usage-log/loading.tsx`](../../../frontend/app/admin/usage-log/loading.tsx) — Geometry-matched route-level loading skeleton with pre-allocated zero-CLS reservation containers.
  - [`frontend/app/admin/usage-log/_sections/UsageLogClient.tsx`](../../../frontend/app/admin/usage-log/_sections/UsageLogClient.tsx) — Interactive client view managing two-stage `headQuery` (50 rows + aggregate) and `fullQuery` (500 rows, `include_aggregate=false`), 4-card KPI grid, `LogAccountingPanel`, `UsageLogFilters`, virtualized `DataTable`, CSV export link, and purge confirmation dialog.
  - [`frontend/app/admin/usage-log/_sections/Filters.tsx`](../../../frontend/app/admin/usage-log/_sections/Filters.tsx) — Time preset selector (`1h`, `24h`, `7d`, `30d`), usage type selector (`All`, `FOS Only`, `CDN Only`), `operation_type` filter input, and `process_context` filter input.
  - [`frontend/app/admin/usage-log/_sections/UsageChart.tsx`](../../../frontend/app/admin/usage-log/_sections/UsageChart.tsx) — `LogAccountingPanel` comparing Fastly Stats API authoritative request counts against locally ingested rows (`GET /api/admin/log-accounting`).
  - [`frontend/app/admin/usage-log/_sections/UsageTable.tsx`](../../../frontend/app/admin/usage-log/_sections/UsageTable.tsx) — Column definitions for timestamp, service ID, operation class (`A`/`B`/`CDN` badges), operation type, URL/path, bytes, function name, process context, HTTP status, and estimated cost.
  - [`frontend/lib/ssr/usage_log.ts`](../../../frontend/lib/ssr/usage_log.ts) — Server-side SSR helper for `/api/admin/usage-log`.

---

## 3. Role & Permission Matrix (RBAC)

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write` / mTLS Admin Gateway)** | Full Control | `200 OK` across `/admin/usage-log`, `GET /api/admin/usage-log`, `GET /api/admin/usage-log/export`, `DELETE /api/admin/usage-log`, `GET/PATCH /api/admin/usage-logging`, `GET /api/admin/system-jobs`, and `GET /api/admin/log-accounting`. |
| **Analyst (Remote Share)** | **Blocked (`403 Forbidden`)** | Enforced at both `RemoteAccessMiddleware` (`/api/admin/*` prefix gate) and route-level `dependencies=[Depends(require_admin)]` across all 6 endpoints in [`backend/routers/admin_usage.py`](../../../backend/routers/admin_usage.py). Any request carrying `X-Remote-Analyst: 1` returns `403 Forbidden` with `{"error": "admin_only"}`. `fetchUsageLogServerSide()` refuses to inject the admin bearer token when `x-fla-role: analyst` is present. |

---

## 4. Architecture Execution Matrix (`DEPLOYMENT_MODE`)

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Ledger Storage ([ADR-22](../../adr/22-postgres-only-metadata.md))** | Unified PostgreSQL 16 (`METADATA_DSN`) tables: `usage_log` (detailed call records) and `usage_log_hourly_summary` (hourly rollups by `(hour_bucket, service_id, usage_type, operation_class, process_context)`). | Unified PostgreSQL 16 (`METADATA_DSN`) tables: `usage_log` and `usage_log_hourly_summary`, shared across the serving pod and Celery ingest/maintenance workers. |
| **Write Buffering & Flush** | Thread-safe in-memory buffer in [`backend/core/metadata/usage_log.py`](../../../backend/core/metadata/usage_log.py) flushed in batches via [`backend/core/metadata/usage_log_db.py`](../../../backend/core/metadata/usage_log_db.py), updating both `usage_log` and `usage_log_hourly_summary` atomically (`ON CONFLICT DO UPDATE`). | Identical PostgreSQL upsert pipeline from both serving pod and Celery workers, with MVCC isolation ensuring writers never block dashboard or admin readers. |
| **Retention & Purge** | Detailed rows in `usage_log` are pruned in 5,000-row chunks by `metadata_cleanup` after `retention_days` (default 30d) or manually via `DELETE /api/admin/usage-log`; `usage_log_hourly_summary` is preserved for historical trend continuity. | Identical. |
| **Ingest Accounting (`LogAccountingPanel`)** | Compares Fastly Historical Stats API against `ingested_files` / DuckLake row counts via `GET /api/admin/log-accounting`. | Compares Fastly Historical Stats API against High-Scale ClickHouse / Postgres `ingest_ledger` row counts via `GET /api/admin/log-accounting`. |

---

## 5. Backend APIs, Contracts & Telemetry Attribution

### Endpoints Implemented in [`backend/routers/admin_usage.py`](../../../backend/routers/admin_usage.py) & [`backend/routers/admin/log_accounting.py`](../../../backend/routers/admin/log_accounting.py):

1. **`GET /api/admin/usage-log`** (`response_model=UsageLogResponse`, `Depends(require_admin)`):
   - **Query Parameters:**
     - `service_id` (`str | None`): Service scope (resolves via `get_source(service_id)`).
     - `start`, `end` (`str | None`): ISO-8601 UTC window bounds.
     - `usage_type` (`str | None`): `"fos"` or `"cdn"`.
     - `process_context` (`str | None`): Caller context filter (`cron:sync`, `cron.optimize`, `api`, etc.).
     - `operation_type` (`str | None`): Operation filter (`GET_OBJECT`, `LIST_OBJECTS_V2`, `PUT_OBJECT`, etc.).
     - `page` (`int`, default `1`, `ge=1`), `page_size` (`int`, default `100`, `ge=1`, `le=500`).
     - `include_aggregate` (`bool`, default `True`): When `False` (used by the client's 500-row background `fullQuery`), skips the `SUM()` aggregate scan and returns a zeroed `UsageLogAggregate` placeholder.
   - **Response Schema (`UsageLogResponse` in [`backend/models/admin.py`](../../../backend/models/admin.py)):**
     - `entries`: `list[UsageLogEntry]` (`id`, `timestamp`, `service_id`, `usage_type`, `operation_class`, `operation_type`, `url`, `bytes`, `function_name`, `process_context`, `status`, `estimated_cost`).
     - `total`: `int`, `page`: `int`, `page_size`: `int`.
     - `aggregate`: `UsageLogAggregate` (`total_class_a`, `total_class_b`, `total_fos_bytes`, `total_cdn_requests`, `total_cdn_bytes`, `estimated_cost_class_a`, `estimated_cost_class_b`, `estimated_cost_cdn_bandwidth`, `estimated_cost_cdn_requests`, `estimated_cost_total`, `by_process`, `rates`).

2. **`GET /api/admin/usage-log/export`** (`StreamingResponse` `text/csv`, `Depends(require_admin)`):
   - Streams up to 10,000 matching rows as CSV (`usage_log_export.csv`) with columns `id,timestamp,service_id,usage_type,operation_class,operation_type,url,bytes,function_name,process_context,status,estimated_cost`.

3. **`DELETE /api/admin/usage-log`** (`response_model=StatusDeletedResponse`, `Depends(require_admin)`):
   - Optional query parameters `before` (`ISO-8601`) and `service_id` (`str`). Purges matching rows from `usage_log` and returns `{"status": "ok", "deleted": <count>}`.

4. **`GET /api/admin/usage-logging` & `PATCH /api/admin/usage-logging`** (`Depends(require_admin)`):
   - Reads and updates global usage-logging configuration (`enabled`, `retention_days`, `class_a_rate_per_1k`, `class_b_rate_per_10k`, `storage_rate_per_gb_month`, `billing_zone`, `cdn_bw_rate_per_gb`, `cdn_req_rate_per_10k`).
   - Validates `retention_days >= 1`, non-negative pricing rates, and valid `billing_zone` in `CDN_ZONE_RATES` (`400 Bad Request` on invalid input).

5. **`GET /api/admin/system-jobs`** (`response_model=SystemJobsResponse`, `Depends(require_admin)`):
   - Returns process-global background job schedules (`bot_data_refresh`, `rdns_enrichment`, `share_audit_purge`) with next-run timestamps and latest `cron_runs` status.

6. **`GET /api/admin/log-accounting`** (`Depends(require_admin)`):
   - Powers `LogAccountingPanel` with hourly/daily comparison buckets between Fastly Stats API request totals and locally ingested row counts.

### Telemetry Attribution:
- Every client request propagates `X-Page-Load-ID`.
- All PostgreSQL queries against `usage_log` and `usage_log_hourly_summary` execute through `backend/core/metadata/pg_connection.py` and are recorded in `sqlite_profiler` / `telemetry_queries`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

1. **Pre-Allocated Zero-CLS Reservation Containers:**
   - **Toolbar (`data-testid="admin-usage-log-toolbar"`):** `min-h-[36px] [contain-intrinsic-size:auto_36px] [content-visibility:auto]` holding `Admin`, `Refresh`, `Purge Logs`, and `Export CSV` buttons.
   - **4-Card KPI Grid (`data-testid="admin-usage-log-kpi-reservation"`):** `grid grid-cols-2 md:grid-cols-4 gap-3 min-h-[164px] [contain-intrinsic-size:auto_164px] [content-visibility:auto]` for `FOS Class A Ops`, `FOS Class B Ops`, `CDN Egress`, and `Est. Total Cost`.
   - **Ingest Accounting Panel (`data-testid="admin-usage-log-chart-reservation"`):** `min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]` wrapping `LogAccountingPanel` (`data-testid="admin-usage-log-accounting-panel"`).
   - **Filters & Ledger Table Card (`data-testid="admin-usage-log-table-reservation"`):** `min-h-[520px] [contain-intrinsic-size:auto_520px] [content-visibility:auto]` wrapping `UsageLogFilters` (`data-testid="admin-usage-log-filters"`) and the virtualized `DataTable`.
2. **In-Place Skeletons & Non-Destructive Dimming:**
   - [`frontend/app/admin/usage-log/loading.tsx`](../../../frontend/app/admin/usage-log/loading.tsx) and unseeded initial loads display geometry-matched skeletons (`Loading FOS usage ledger...`).
   - Both `headQuery` and `fullQuery` use `placeholderData: keepPreviousData`; background refetches dim the active KPI grid and table (`opacity-40 pointer-events-none`) without collapsing layout (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Stable `data-testid` Contract

- `data-testid="admin-usage-log-root"`: Outer page container.
- `data-testid="admin-usage-log-toolbar"`: Action button toolbar.
- `data-testid="admin-usage-log-refresh-btn"`: Manual refresh button (triggers `headQuery.refetch()` + `fullQuery.refetch()`).
- `data-testid="admin-usage-log-purge-btn"`: Opens the purge confirmation modal.
- `data-testid="admin-usage-log-export-link"`: Direct CSV download anchor (`/api/admin/usage-log/export?...`).
- `data-testid="admin-usage-log-kpi-reservation"`: 4-card aggregate KPI grid.
- `data-testid="admin-usage-log-chart-reservation"` & `data-testid="admin-usage-log-accounting-panel"`: Ingest Accounting section.
- `data-testid="admin-usage-log-table-reservation"`: Filter bar + DataTable reservation card.
- `data-testid="admin-usage-log-filters"`: Filter bar container.
- `data-testid="admin-usage-log-preset-1h"`, `admin-usage-log-preset-24h`, `admin-usage-log-preset-168h`, `admin-usage-log-preset-720h`: Time-window preset buttons with `aria-pressed`.
- `data-testid="admin-usage-log-type-select"`: Usage type dropdown (`all`, `fos`, `cdn`).
- `data-testid="admin-usage-log-operation-input"`: Operation type substring filter (`e.g. GET_OBJECT`).
- `data-testid="admin-usage-log-process-input"`: Caller process context filter (`e.g. cron:sync`).

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / Warm Load** | < 1200 ms | < 1200 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.00 typical) | <= 0.05 (0.00 typical) | PerformanceObserver (`clsInitScript`) |
| **API Latency (`GET /api/admin/usage-log`)** | < 200 ms (p95) | < 150 ms (p95) | Playwright response timing |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Pytest + Route Tripwire |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin/usage-log` returns HTTP 200 for Admin; Analyst requests to `/api/admin/usage-log*`, `/api/admin/usage-logging`, and `/api/admin/system-jobs` return HTTP 403 (`{"error": "admin_only"}`).
- [x] **2. Zero Layout Shift:** Page loads with pre-allocated reservation containers (`admin-usage-log-toolbar`, `admin-usage-log-kpi-reservation`, `admin-usage-log-chart-reservation`, `admin-usage-log-table-reservation`) and achieves `CLS <= 0.05` (`0.00` measured).
- [x] **3. Usage Log & Config Contracts:** `GET /api/admin/usage-log`, `GET /api/admin/usage-logging`, `PATCH /api/admin/usage-logging`, `GET /api/admin/usage-log/export`, `DELETE /api/admin/usage-log`, and `GET /api/admin/system-jobs` verified against Pydantic schemas and OpenAPI snapshot.
- [x] **4. Filter & Preset Workflows:** Time-window preset switching (`24h` -> `1h` -> `7d` -> `30d`), `process_context` filtering, and `operation_type` filtering verified in both Vitest and Playwright E2E.
- [x] **5. Role Verification:** Analyst `403` rejection verified across all 6 endpoints in `tests/routers/test_admin_usage_log.py` and `tests/remote_access/test_route_table_tripwire.py`.
- [x] **6. Telemetry & Export Audit:** Manual refresh (`admin-usage-log-refresh-btn`) and CSV export (`admin-usage-log-export-link`) verified with `X-Page-Load-ID` header attribution across Local Standard, Remote Standard GCE, and High-Scale Elevation.

---

## 10. Automated Test Suite

- **Backend Unit Tests:** [`tests/routers/test_admin_usage_log.py`](../../../tests/routers/test_admin_usage_log.py) (19 tests covering raw entries, aggregates, `include_aggregate=False`, filtering by `service_id`/`usage_type`/`process_context`/`operation_type`, CSV export, purge, `GET/PATCH /api/admin/usage-logging` validation, `GET /api/admin/system-jobs`, and Analyst `403` rejection across all 6 endpoints).
- **Route Tripwire & OpenAPI Snapshot Tests:** [`tests/remote_access/test_route_table_tripwire.py`](../../../tests/remote_access/test_route_table_tripwire.py) and [`tests/test_openapi_snapshot.py`](../../../tests/test_openapi_snapshot.py).
- **Frontend Unit Tests:** [`frontend/__tests__/app/admin/usage-log.test.tsx`](../../../frontend/__tests__/app/admin/usage-log.test.tsx) (4 tests covering `loading.tsx` geometry, pending skeleton state, KPI/accounting/table rendering, and preset/filter/refresh/purge interactions).
- **Playwright E2E Suite:** [`frontend/e2e/pages/admin-usage-log.spec.ts`](../../../frontend/e2e/pages/admin-usage-log.spec.ts) (5 end-to-end contract tests verified locally and across all 3 live deployment tiers).
