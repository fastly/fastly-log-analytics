# Page Specification: Admin Session Scoring & Scorer Weights (`/admin/session-scoring`)

> [!NOTE] **Live-Certified across Standard & High-Scale Topologies**
> This specification documents the live-certified contract, ADR-22 PostgreSQL + DuckLake/ClickHouse storage architecture, strict Admin-only RBAC boundaries, zero-CLS layout reservation behavior, and automated verification suite for `/admin/session-scoring`.

---

## 1. Overview & Objectives

The **Admin Session Scoring & Scorer Weights** page (`/admin/session-scoring`) manages real-time edge behavioral session threat scoring (`StatusPanel`, `ThresholdSlider`, `L2EnforcementCard`, `ExcludeRegexCard`, `MatrixVersionsCard`, `RetrainButton`, `RotateKeyButton`), live telemetry and evaluation cards (`ScoringHealthCard`, `TopFlaggedTable`, `RocPrCurves`, `ScoreDistChart`, `ScorerLatencyChart`, `ScorerErrorsChart`, `ScorerFailOpenBreakdownCard`, `PerReasonAucCard`), ground-truth IP labeling (`LabelsTab`), and configuration/retraining audit history (`AuditLogTab`).

### Key Tenets:
- **Composite Single-Round-Trip Hydraulics (`GET /scoring/config` & `GET /scoring/analytics`):** Eliminates 11-way HTTP request waterfalls on page mount by fetching two composite bundles (`ConfigCompositeResponse` and `AnalyticsCompositeResponse` / `/dashboard`) and synchronously seeding individual TanStack Query cache keys (`['scoring-status']`, `['scoring-health']`, `['scoring-score-dist']`, `['scoring-evaluation']`, `['scoring-per-reason']`, `['scoring-top-flagged']`, `['scoring-matrix-versions']`, `['scoring-audit']`) inside `queryFn` before child panels render.
- **Canonical & Alias Route Support:** Supports both `/api/services/{service_id}/scoring/*` and `/api/services/{service_id}/session-scoring/*` route aliases across all status, composite, configuration, retraining, matrix restore, key rotation, label, and audit endpoints.
- **Strict Admin RBAC:** Accessible **exclusively** to Admin (`read_write`) users (`/admin/*` prefix blocked by `_ANALYST_BLOCKED_PREFIXES`). All 22 session-scoring admin/mutation endpoints enforce `Depends(require_admin)` + `_ANALYST_BLOCKED_SCORING_SUFFIXES`, returning HTTP `403 Forbidden` with `{"error": "admin_only"}` for Analyst Path B sessions.

---

## 2. Routes & URL Schema

- **Primary Route:** `/admin/session-scoring`
- **Supported Context:**
  - Active service resolved from `useService()` store with fallback to `GET /api/services` when `activeServiceId` is initially unset.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Control | View status & drift, enable/disable/redeploy edge scorer via SSE, adjust alert/enforcement thresholds (`threshold`, `enforce-threshold`), configure L2 NGWAF enforcement (`l2-enforce`, `enforce-status-code`), edit exclusion regex (`exclude-regex`), trigger model retraining (`retrain`), restore matrix versions (`restore`), rotate signing keys (`rotate-key`), manage ground-truth labels (`labels`), and inspect audit logs (`audit`). |
| **Analyst Path B (Remote Share)** | Blocked (`403 Forbidden`) | `/admin/session-scoring` route blocked; all `/api/services/{id}/scoring/*` and `/api/services/{id}/session-scoring/*` admin/config/mutation routes return `403 {"error": "admin_only"}`. |
| **Analyst Path A (JSON Join)** | Read-Only | View-only status/analytics inspection; Fastly Compute/VCL deployment and FOS/KV mutations require `read_write` service credentials. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Scorer Config & State** | Stored in `configs/{service_id}.json` and unified ADR-22 PostgreSQL metadata (`METADATA_DSN`) + FOS admin state sync. | Stored in `configs/{service_id}.json` and unified ADR-22 PostgreSQL metadata (`METADATA_DSN`). |
| **Analytics & Evaluation** | Queries DuckDB + DuckLake (`lake.logs`) via `RequestContext` and `execute_with_stale_view_retry` self-healing. | Queries ClickHouse `request_facts` / durable DuckLake tables via `RequestContext`. |
| **Edge Deployment & KV** | Compute scorer package + VCL integration (`fla_scoring_*`) + Fastly KV store matrix versions (`scoring_matrix_versions`). | Identical Fastly Compute + VCL + KV store matrix pipeline. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit (under `/api/services/{service_id}/scoring/*` and `/api/services/{service_id}/session-scoring/*`):
1. `GET /api/services`:
   - Resolves configured services when `activeServiceId` is initially empty.
2. `GET .../config`:
   - `ConfigCompositeResponse` returning `{status, matrix_versions, audit}` in one round-trip.
3. `GET .../analytics` & `GET .../dashboard`:
   - `AnalyticsCompositeResponse` returning `{health, score_distribution, evaluation, per_reason, top_flagged}` over the requested `window` (`1h`, `6h`, `24h`, `7d`).
4. `GET .../status`:
   - `ScoringStatusResponse` returning `enabled`, `compute_service_id`, `kv_store_id`, `secret_store_id`, `matrix_version`, `last_retrained_at`, `threshold`, `enforce_threshold`, `exclude_regex`, `enforce_status_code`, `l2_enforce`, `vcl_version`, and `vcl_drift`.
5. `GET .../threshold` & `PUT .../threshold`:
   - Inspects or updates the primary flagging threshold (`0 < threshold < 1`).
6. `PUT .../enforce-threshold`:
   - Updates or clears (`null`) the active edge blocking threshold (`enforce_threshold >= threshold`).
7. `GET .../l2-enforce` & `PUT .../l2-enforce`:
   - Inspects or toggles L2 NGWAF enforcement (`enabled: bool`).
8. `PUT .../exclude-regex` & `POST .../exclude-regex/validate`:
   - Validates and updates the path exclusion regex (`max_length=500`, linear-time regex validation).
9. `PUT .../enforce-status-code`:
   - Configures the HTTP status code (`403` or `429`) returned when enforcing.
10. `GET .../matrix-versions` & `POST .../restore`:
    - Lists historical scoring matrix versions (`MatrixVersionsResponse`) and restores a selected version to the Fastly KV store.
11. `POST .../retrain`, `POST .../rotate-key`, `POST .../enable`, `POST .../disable`:
    - Admin-only SSE / action endpoints for model retraining, HMAC key rotation, and Compute/VCL provisioning.
12. `GET .../labels`, `POST .../labels`, `DELETE .../labels/{ip}` & `GET .../audit`:
    - Ground-truth IP label CRUD (`benign` / `malicious`) and audit trail (`AuditResponse`).

### Telemetry Attribution:
- Every client request propagates `X-Page-Load-ID` via `apiFetch`.
- ServerFooter renders active cloud/VM/container identity dynamically.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Route & Component Layout Reservations:**
  - `frontend/app/admin/session-scoring/loading.tsx` and `<React.Suspense fallback={<Loading />}>` render `data-testid="admin-session-scoring-root"`, `PageHeader`, `data-testid="admin-session-scoring-status-reservation"` (`min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]`), `data-testid="admin-session-scoring-tabs"`, and `data-testid="admin-session-scoring-overview-reservation"` (`min-h-[900px] [contain-intrinsic-size:auto_900px] [content-visibility:auto]`).
  - `StatusPanel` (`frontend/components/SessionScoring/StatusPanel.tsx`) renders `data-testid="admin-session-scoring-status-card"` (`min-h-[220px]`), `data-testid="admin-session-scoring-status-line"`, `data-testid="admin-session-scoring-status-badge"` (`Enabled` / `Disabled`), and a 3-column metadata grid (`grid-cols-1 md:grid-cols-2 lg:grid-cols-3`) so enabled and disabled states maintain stable vertical geometry.
- **Synchronous Cache Seeding (Zero Child Waterfall Reflow):**
  - `configComposite` and `analyticsComposite` seed all child query keys inside `queryFn` prior to resolving, ensuring child cards mount immediately with populated cache data and zero intermediate skeleton flash.
- **Dimmed Updates:** Manual or background refetches dim active containers (`opacity-40 pointer-events-none transition-opacity`) while keeping layout height locked (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Edge Cases

- **Manual Refresh (`data-testid="admin-session-scoring-refresh-btn"`):** Invalidates `['scoring-config-composite']`, `['scoring-analytics-composite']`, and `['scoring-status']`, spinning the refresh icon and dimming active panels in place with `X-Page-Load-ID` telemetry correlation.
- **Tab Navigation (`Overview`, `Labels`, `Matrix history`, `Audit`):** Switches cleanly via `data-testid="admin-session-scoring-tab-overview"`, `data-testid="admin-session-scoring-tab-labels"`, `data-testid="admin-session-scoring-tab-matrix"`, and `data-testid="admin-session-scoring-tab-audit"`.
- **Threshold, L2 Enforcement & Exclusion Updates:** Inline sliders and forms validate bounds (`enforce_threshold >= threshold`, regex syntax) and persist atomically to service config and Fastly KV.
- **Enable / Redeploy / Disable (`data-testid="admin-session-scoring-enable-btn"`, `data-testid="admin-session-scoring-redeploy-btn"`, `data-testid="admin-session-scoring-disable-btn"`):** Streams step-by-step SSE provisioning/teardown progress.
- **Preflight prerequisite dialog:** When prerequisites are missing, Enable opens instructions rather than starting provisioning. A successful readiness recheck clears the dialog's open state during render, without an effect-driven update. If readiness later becomes missing again, the dialog stays closed until Enable is clicked again.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / Warm Load** | < 1200 ms | < 1200 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 | <= 0.05 | PerformanceObserver (`layout-shift`) |
| **Composite API Calls on Mount** | 2 (`/config`, `/analytics`) | 2 (`/config`, `/analytics`) | Playwright network audit |
| **Telemetry Capture** | `X-Page-Load-ID` on API requests | `X-Page-Load-ID` on API requests | Playwright request header audit |
| **RBAC Enforcement** | 100% Analyst requests 403 | 100% Analyst requests 403 | Pytest & Playwright RBAC audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /admin/session-scoring` returns HTTP 200 for Admin; Analyst Path B receives HTTP 403 on all session-scoring admin/mutation endpoints.
- [x] **2. Zero Layout Shift:** Verified pre-allocated reservation containers (`[contain-intrinsic-size:auto_220px]`, `[contain-intrinsic-size:auto_900px]`), synchronous composite cache seeding, and warm-load `CLS <= 0.05`.
- [x] **3. Composite & Status Contracts:** Verified `/api/services/{service_id}/scoring/config`, `/api/services/{service_id}/scoring/analytics`, and `/api/services/{service_id}/session-scoring/status` schemas.
- [x] **4. Config & Threshold Validation:** Verified threshold, enforce-threshold, l2-enforce, exclude-regex, enforce-status-code, matrix-versions, restore, rotate-key, and audit endpoints.
- [x] **5. Role Verification:** Confirmed Analyst Path B receives `403 {"error": "admin_only"}` across both `/scoring/*` and `/session-scoring/*` routes.
- [x] **6. Telemetry & Manual Refresh Audit:** Verified `X-Page-Load-ID` header propagation and interactive manual refresh (`admin-session-scoring-refresh-btn`).

---

## 10. Automated Test Suite

- **Backend Unit Tests:** `tests/routers/test_session_scoring_admin.py` (18 tests covering status, config composite, dashboard composite, threshold, enforce-threshold, l2-enforce, exclude-regex, enforce-status-code, matrix-versions, restore, rotate-key, audit, `/session-scoring/*` aliases, and Analyst Path B `403` enforcement).
- **Route Table Tripwire & OpenAPI Snapshot:** `tests/remote_access/test_route_table_tripwire.py` & `tests/test_openapi_snapshot.py`.
- **Frontend Unit Tests:** `frontend/__tests__/app/admin/session-scoring.test.tsx` (3 tests covering composite rendering + zero-CLS containers/tabs, manual refresh invalidation, and `/api/services` fallback).
- **Prerequisite Dialog Unit Tests:** `frontend/__tests__/components/SessionScoring/StatusPanel.test.tsx` covers the prerequisite gate, readiness recheck and keeping a resolved dialog closed across later readiness regression.
- **Playwright E2E Suite:** `frontend/e2e/pages/admin-session-scoring.spec.ts` retains the five existing page-contract cases and adds a fixture-controlled prerequisite-dialog recovery/regression case. The new case is verified against the local mock test app; it is not deployed-environment evidence.
