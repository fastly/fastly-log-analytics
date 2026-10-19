# Page Specification: Admin RUM Ingestion & Configuration (`/admin/rum`)

> [!NOTE] **Live-Certified across Standard & High-Scale Topologies**
> This specification documents the live-certified contract, ADR-22 PostgreSQL + DuckLake/ClickHouse storage architecture, strict Admin-only RBAC boundaries, zero-CLS layout reservation behavior, and automated verification suite for `/admin/rum`.

---

## 1. Overview & Objectives

The **Admin RUM Ingestion & Configuration** page (`/admin/rum`) inspects and controls Real User Monitoring (RUM) beacon ingestion pipelines for the active Fastly service. It surfaces real-time RUM status (`enabled`, `enabled_at`, `vcl_drift`), self-hosted Grafana Faro Web SDK version and collector settings (`RumFaroVersionCard` + `RumSettingsDialog`), collector modules (`capture_vitals`, `capture_errors`, `capture_sessions`, `capture_console`), VCL injection conditions (`inject_vcl_condition`), and streaming SSE enable/disable provisioning flows (`EnableRumDialog` / `DisableRumDialog`).

### Key Tenets:
- **Telemetry Pipeline Configuration:** Controls RUM collector configuration, Faro SDK version upgrades, and VCL condition guards (`inject_vcl_condition`), persisting settings to `configs/{service_id}.json`, uploading self-hosted SDK bundles to Fastly Object Storage (`{prefix}/rum/sdk/`), and reconciling VCL snippets when conditions change.
- **VCL Drift Detection:** Recomputes `expected_sha` over the active service's RUM config, Faro SDK version, and custom VCL snippet (`fla_rum_custom`) to detect and remediate out-of-band VCL changes.
- **Strict Admin RBAC:** Accessible **exclusively** to Admin (`read_write`) users (`/admin/*` prefix blocked by `_ANALYST_BLOCKED_PREFIXES`). All RUM mutation and admin routes (`/enable`, `/disable`, `/settings`, `/versions`, `/upgrade`, `/api/admin/faro-sdk-version`) enforce `Depends(require_admin)` + `_ANALYST_BLOCKED_RUM_SUFFIXES`, returning HTTP `403 Forbidden` with `{"error": "admin_only"}` for Analyst Path B sessions.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/rum`
- **Supported Context:**
  - Active service resolved from `useService()` store (`GET /api/services`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | View full RUM status (`enabled`, `enabled_at`, `vcl_drift`, `capture_*`, `faro_sdk_version`, `inject_vcl_condition`), enable/disable RUM via SSE, configure collectors/VCL conditions, inspect/upgrade Faro SDK versions. |
| **Analyst Path B (Remote Share)** | Blocked (`403 Forbidden`) | `/admin/rum` route blocked; `POST /api/services/{id}/rum/{enable,disable,settings,upgrade}`, `GET /api/services/{id}/rum/versions`, and `GET /api/admin/faro-sdk-version` return `403 {"error": "admin_only"}`. (`GET /api/services/{id}/rum/status` returns minimal `{enabled, enabled_at}` projection for main navigation badge visibility only.) |
| **Analyst Path A (JSON Join)** | Read-Only | View-only status inspection; Fastly API provisioning and FOS SDK writes require `read_write` service credentials. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Ingest Jobs** | APScheduler `rum_sync_{id}` (every `log_period` sec) + immediate/1m `rum_commit_{id}` draining local Parquet buffers. | Distributed `high-scale-worker` micro-batching from `raw/rum/**/*.gz` via PostgreSQL `ingest_ledger` (`stream_type = 'rum'`). |
| **Storage Tables** | DuckLake `lake.client_vitals` & `lake.client_errors` backed by ADR-22 PostgreSQL catalog (`DUCKLAKE_CATALOG`). | ClickHouse `rum_vitals_facts`, `rum_error_facts`, and `rum_vitals_aggregates` + durable FOS archive Parquet. |
| **SDK Hosting & VCL** | Self-hosted Faro bundles uploaded to `s3://{bucket}/{prefix}/rum/sdk/` + Fastly VCL snippets (`fla_rum_custom`). | Identical Fastly VCL + FOS self-hosted SDK bundle pipeline. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/services`:
   - Resolves configured services and active `service_id`.
2. `GET /api/services/{service_id}/rum/status`:
   - Typed `RumStatusResponse` returning `enabled`, `enabled_at`, `capture_vitals`, `capture_errors`, `capture_sessions`, `capture_console`, `faro_sdk_version`, `inject_vcl_condition`, and live-computed `vcl_drift` (`expected_sha != rum_vcl_version`).
3. `GET /api/admin/faro-sdk-version`:
   - Admin-only endpoint (`Depends(require_admin)`) returning bundled vs latest npm `@grafana/faro-web-sdk` versions (`current`, `latest`, `update_available`).
4. `POST /api/services/{service_id}/rum/settings`:
   - Admin-only endpoint (`Depends(require_admin)`) accepting `RumSettingsRequest` (`capture_vitals`, `capture_errors`, `capture_sessions`, `capture_console`, `inject_vcl_condition`). Uploads updated SDK config to FOS, reconciles Fastly VCL only if `inject_vcl_condition` changed, and rolls back `configs/{service_id}.json` atomically on FOS or VCL failure.
5. `POST /api/services/{service_id}/rum/enable` & `POST /api/services/{service_id}/rum/disable`:
   - Admin-only SSE endpoints (`Depends(require_admin)`) streaming step-by-step Fastly VCL and FOS SDK provisioning/teardown progress.
6. `GET /api/services/{service_id}/rum/versions` & `POST /api/services/{service_id}/rum/upgrade`:
   - Admin-only endpoints (`Depends(require_admin)`) for inspecting and upgrading the self-hosted Faro SDK version in FOS.

### Telemetry Attribution:
- Every client request propagates `X-Page-Load-ID` via `apiFetch`.
- ServerFooter renders active cloud/VM/container identity dynamically.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Route & Component Layout Reservations:**
  - `frontend/app/admin/rum/loading.tsx` pre-allocates `data-testid="admin-rum-loading"`, `data-testid="admin-rum-toolbar-skeleton"` (`min-h-[44px] [contain-intrinsic-size:auto_44px] [content-visibility:auto]`), and `data-testid="admin-rum-status-skeleton-card"` (`min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]`).
  - `RumStatusPanel` (`frontend/components/Rum/RumStatusPanel.tsx`) renders `data-testid="admin-rum-toolbar"` (housing `data-testid="admin-rum-status-badge"`, `RumFaroVersionCard` `data-testid="admin-rum-faro-version-card"`, and manual refresh button `data-testid="admin-rum-refresh-btn"`) and `data-testid="admin-rum-status-reservation"` (`min-h-[240px] [contain-intrinsic-size:auto_240px] [content-visibility:auto]`).
- **In-Place Skeletons:** Displays contextual loading copy (`Inspecting RUM beacon pipeline...`) inside `data-testid="admin-rum-status-skeleton"` while initial status queries resolve.
- **Dimmed Updates:** Manual or background refetches dim the active status card (`opacity-40 pointer-events-none transition-opacity`) while keeping layout height locked (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Edge Cases

- **Manual Refresh (`data-testid="admin-rum-refresh-btn"`):** Triggers `refetch()` on `useRumStatus(selectedService)`, spinning the refresh icon and dimming the status card in place without layout shift.
- **Collector & VCL Condition Configuration (`data-testid="admin-rum-settings-btn"`):** Opens `RumSettingsDialog` to toggle `capture_vitals`, `capture_errors`, `capture_sessions`, `capture_console`, and edit `inject_vcl_condition`.
- **Enable / Disable / Re-apply VCL (`data-testid="admin-rum-enable-btn"` / `data-testid="admin-rum-disable-btn"`):** Opens `EnableRumDialog` (or `DisableRumDialog`) streaming SSE progress steps and refreshing `/rum/status` on completion.
- **VCL Drift Warning Banner (`data-testid="admin-rum-drift-alert"`):** Displayed whenever `status.vcl_drift === true`, offering one-click `Re-apply VCL` reconciliation.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / Warm Load** | < 1200 ms | < 1200 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 | <= 0.05 | PerformanceObserver (`layout-shift`) |
| **API Latency (`/rum/status`)** | < 150 ms (p95) | < 150 ms (p95) | Playwright response audit |
| **Telemetry Capture** | `X-Page-Load-ID` on API requests | `X-Page-Load-ID` on API requests | Playwright request header audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Pytest & Playwright RBAC audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin/rum` returns HTTP 200 for Admin; Analyst Path B receives HTTP 403 on all RUM admin/mutation endpoints.
- [x] **2. Zero Layout Shift:** Verified pre-allocated reservation containers (`[contain-intrinsic-size:auto_*]`) and warm-load `CLS <= 0.05`.
- [x] **3. RUM Status Contract:** Verified `/api/services/{service_id}/rum/status` payload schema (`enabled`, `enabled_at`, `vcl_drift`, `capture_*`, `faro_sdk_version`) and `/api/admin/faro-sdk-version`.
- [x] **4. Config Update & Rollback Safety:** Verified `POST /api/services/{service_id}/rum/settings` updates config, skips redundant VCL deploys when condition is unchanged, and rolls back on FOS or VCL failure.
- [x] **5. Role Verification:** Confirmed Analyst Path B receives `403 {"error": "admin_only"}` across `/enable`, `/disable`, `/settings`, `/versions`, `/upgrade`, and `/api/admin/faro-sdk-version`.
- [x] **6. Telemetry & Manual Refresh Audit:** Verified `X-Page-Load-ID` header propagation and interactive manual refresh (`admin-rum-refresh-btn`).

---

## 10. Automated Test Suite

- **Backend Unit Tests:** `tests/routers/test_rum_admin.py` (17 tests covering status, drift, legacy SHA migration, settings updates/rollbacks, SSE enable/disable, and Analyst Path B `403` enforcement).
- **Route Table Tripwire:** `tests/remote_access/test_route_table_tripwire.py`.
- **Frontend Unit Tests:** `frontend/__tests__/app/admin-rum.test.tsx` & `frontend/components/Rum/__tests__/RumStatusPanel.test.tsx`.
- **Playwright E2E Suite:** `frontend/e2e/pages/admin-rum.spec.ts` (5 end-to-end contract tests verified across Local Standard, Remote Standard GCE, and High-Scale Elevation).
