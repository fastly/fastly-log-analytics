> [!NOTE]
> **Specification Status: LIVE-CERTIFIED (2026-10-20)**
> This specification defines the verified functional, architectural, role, and telemetry contract for the **Admin ClickHouse Replay & Cluster Operations** page (`/admin/clickhouse`), certified across Local Standard, Remote Standard GCE, and High-Scale Elevation.

# Page Specification: Admin ClickHouse Replay & Cluster Operations (`/admin/clickhouse`)

## 1. Overview & Objectives
The ClickHouse Replay & Cluster Operations page provides administrative visibility and control over the High-Scale ClickHouse analytical engine and its bounded artifact replay machinery (`backend/routers/admin/clickhouse.py`, `backend/models/admin_clickhouse.py`, `backend/core/clickhouse_manifest.py`, `backend/core/clickhouse_publication.py`, `backend/core/clickhouse_schema.py`). ClickHouse serves as the primary high-throughput event and minute-rollup data plane in `DEPLOYMENT_MODE=high_scale` topologies, ingesting request and RUM telemetry into partitioned MergeTree tables (`log_facts`, `request_facts`, `high_scale_batch_publications`, `origin_minute_summary`, `network_minute_dimensions`, etc.).

### Key Tenets:
- **Cluster & Manifest Health Monitoring:** Surface authenticated connection health (`ok` | `disabled` | `unavailable`), schema version alignment (`CLICKHOUSE_SCHEMA_VERSION = 2`), publication batch counts (`published`, `pending`, `claimed`, `failed`), oldest pending batch age, active generation coverage metadata, and expired dataset count via `PgManifest(bounded=True).admin_status(service_id, target_identity(client))`.
- **Bounded Diagnostic Replay:** Provide operators with a safe, bounded HTTP replay mechanism (`limit` 1..100 artifacts per request) to rebuild (`dataset_id`) or resume (`generation`) ClickHouse analytical state from sealed Fastly Object Storage (FOS) artifacts and PostgreSQL replay manifests (`PgManifest`).
- **Dry-Run Safety:** Replay workflows default to `dry_run: true`, validating manifest references (`store.replay_preview(...)`) and returning `planned_artifacts` and `artifact_count` without mutating ClickHouse or reading FOS.
- **Strict Role & Tenancy Isolation:** Administrative operations are restricted to `read_write` Admin sessions via both `RemoteAccessMiddleware` defense-in-depth and route-level `Depends(require_admin)`. Analyst sessions receive HTTP `403 Forbidden` with `{"detail": {"error": "admin_only", ...}}`.

---

## 2. Routes & URL Schema
- **Primary Route:** `/admin/clickhouse`
- **Query Parameters:**
  - `service` / `service_id` (*optional*, falls back to `useServiceStore.activeServiceId` or first configured service): Fastly logging service identifier (e.g. `?service=<service-id>`).

---

## 3. Role & Permission Matrix
| Role | Page Access | Status Inspection (`GET /api/admin/clickhouse/status`) | Dry-Run Preview (`POST /api/admin/clickhouse/replay`) | Live Replay (`dry_run: false`) |
|---|---|---|---|---|
| **Admin** | Full Access (`200 OK`) | Allowed (`200 OK` / `503 Unavailable`) | Allowed (cap: `1..100` artifacts) | Allowed (cap: `1..100` artifacts; audited in `audit_logs`) |
| **Analyst** (Remote Share) | Blocked by `RemoteAccessMiddleware` | HTTP `403 Forbidden` (`{"error": "admin_only"}`) | HTTP `403 Forbidden` (`{"error": "admin_only"}`) | HTTP `403 Forbidden` (`{"error": "admin_only"}`) |

*Both `RemoteAccessMiddleware` and route-level `dependencies=[Depends(require_admin)]` guard `/api/admin/clickhouse/status` and `/api/admin/clickhouse/replay`.*

---

## 4. Architecture Execution Matrix
| Architecture / Mode | Engine State | Behavior |
|---|---|---|
| **Standard** (`DEPLOYMENT_MODE=standard`) | Disabled | `GET /api/admin/clickhouse/status?service_id={id}` returns HTTP `200` with `enabled: false, health: "disabled"`, and `null` for `schema_version`, `publication_counts`, `oldest_pending_age_seconds`, `last_published_at`, `active_generation`, and `expired_dataset_count`. UI renders an informational banner (`data-testid="admin-clickhouse-disabled-banner"`) explaining that DuckDB + DuckLake serves all queries, and disables replay controls (`POST /api/admin/clickhouse/replay` returns `409 clickhouse_disabled`). |
| **High-Scale** (`DEPLOYMENT_MODE=high_scale`) | Active | `GET /api/admin/clickhouse/status?service_id={id}` verifies `client.health()`, queries `PgManifest(bounded=True).admin_status(service_id, target_identity(client))`, checks `schema_version == CLICKHOUSE_SCHEMA_VERSION`, and returns HTTP `200` (`health: "ok"`) or HTTP `503` (`health: "unavailable"`). Full dry-run preview and bounded replay execution (`full_rebuild` / `replay_unpublished`) are enabled. |

---

## 5. Backend APIs & Telemetry Attribution
### Endpoints
1. `GET /api/admin/clickhouse/status?service_id={id}`
   - **Dependencies:** `Depends(require_admin)`
   - **Response Model:** `ClickHouseStatusResponse` (`backend/models/admin_clickhouse.py`)
   - **Fields:**
     - `service_id: str`
     - `enabled: bool`
     - `health: Literal["ok", "unavailable", "disabled"]`
     - `schema_version: int | None`
     - `publication_counts: dict[str, int] | None` (`pending`, `claimed`, `failed`, `published`)
     - `oldest_pending_age_seconds: float | None`
     - `last_published_at: str | None`
     - `active_generation: ClickHouseGenerationSummary | None` (`generation`, `dataset_id`, `coverage_start`, `coverage_end`, `expires_at`, `expired`, `target_matches`, `coverage_age_seconds`)
     - `expired_dataset_count: int | None`
   - **Status Codes:** `200 OK` (`health: "ok"` or `"disabled"`), `400 Bad Request` (missing `service_id`), `403 Forbidden` (`admin_only`), `404 Not Found` (`service_not_found`), `503 Service Unavailable` (`health: "unavailable"` with `ClickHouseStatusResponse` body, or `service_unavailable` if config load fails).

2. `POST /api/admin/clickhouse/replay`
   - **Dependencies:** `Depends(require_admin)`
   - **Request Model:** `ClickHouseReplayRequest` (`extra="forbid"`):
     - `service_id: str` (`min_length=1, max_length=128`, validated against `SERVICE_ID_PATTERN`)
     - `dataset_id: str | None` XOR `generation: str | None` (exactly one required; passing both or neither raises HTTP `422`)
     - `dry_run: bool = True`
     - `limit: StrictInt = 100` (`ge=1`, HTTP cap `<= 100`; values `< 1` or `> 100` raise HTTP `422`)
   - **Response Model:** `ClickHouseReplayResponse`:
     - `service_id: str`
     - `dry_run: bool`
     - `operation: Literal["rebuild", "resume"]`
     - `dataset_id: str`
     - `generation: str | None`
     - `artifact_count: int`
     - `planned_artifacts: int`
     - `attempted: int` (`0` when `dry_run=True`)
     - `published: int` (`0` when `dry_run=True`)
     - `activated: bool` (`False` when `dry_run=True`)
     - `limit: int`
   - **Error Responses:**
     - `403 Forbidden`: `admin_only` (analyst)
     - `404 Not Found`: `service_not_found` or `replay_not_found` (`LookupError` from `PgManifest.replay_preview`)
     - `409 Conflict`: `clickhouse_disabled` (Standard mode) or `replay_ineligible` (`ValueError` when reference is expired, target mismatches, or exceeds HTTP cap)
     - `422 Unprocessable Entity`: Invalid `limit` (`< 1` or `> 100`), missing/both `dataset_id` and `generation`, or extra fields
     - `503 Service Unavailable`: `ClickHouseReplayErrorResponse` (`detail.error = "replay_unavailable"`, sanitized with empty `_debug_calls` so FOS URLs never leak on failure)

### Telemetry & Audit Attribution
- **Page Load Correlation:** All frontend requests via `adminFetch` propagate `X-Page-Load-ID`; clicking manual `Refresh` (`admin-clickhouse-refresh-btn`) calls `resetPageLoadId('manual-refresh')` before refetching.
- **Audit Logging:** Non-dry-run replay requests record `clickhouse_replay` events in PostgreSQL `audit_logs` via `metadata.record_audit` across phases `requested`, `completed` (with `attempted`, `published`, `activated`), and `failed`.
- **Query Registry & Metrics:** ClickHouse operations emit structured logs (`clickhouse.operation`) and register with `clickhouse_observer` / Prometheus rules (`observability/clickhouse.rules.yml`).

---

## 6. UI Components, Skeletons & Zero-CLS Layout Reservations
Implemented in `frontend/app/admin/clickhouse/page.tsx`, `frontend/app/admin/clickhouse/loading.tsx`, and `frontend/app/admin/clickhouse/_sections/ClickHouseAdminClient.tsx`, and wired into `frontend/app/admin/AdminPrefetchLinks.tsx` and `frontend/lib/query-keys.ts` (`queryKeys.admin.clickhouseStatus`).

- **Page Root (`data-testid="admin-clickhouse-root"` / `data-testid="admin-clickhouse-loading"`):**
  - Renders `PageHeader` (`ClickHouse Replay & Cluster Operations`) with `BackToAdminLink`.
- **Pre-Allocated Zero-CLS Containers:**
  1. **Toolbar (`data-testid="admin-clickhouse-toolbar"`):** `min-h-[56px] [contain-intrinsic-size:auto_56px] [content-visibility:auto]`. Displays service badge (`admin-clickhouse-service-badge`), health badge (`admin-clickhouse-health-badge`: `ok` / `disabled` / `unavailable`), and manual `Refresh` button (`admin-clickhouse-refresh-btn`).
  2. **Cluster & Publication Vitals (`data-testid="admin-clickhouse-vitals-reservation"`):** `min-h-[220px] [contain-intrinsic-size:auto_220px] [content-visibility:auto]`. Displays engine status (`admin-clickhouse-vital-enabled`), `schema_version`, publication counts (`admin-clickhouse-vital-counts`), `oldest_pending_age_seconds` (`admin-clickhouse-vital-pending-age`), `last_published_at`, `expired_dataset_count` (`admin-clickhouse-vital-expired-count`), and contextual banners for Standard mode (`admin-clickhouse-disabled-banner`) or HTTP 503 unavailability (`admin-clickhouse-unavailable-banner`).
  3. **Active Generation Card (`data-testid="admin-clickhouse-generation-reservation"`):** `min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]`. Displays `active_generation` (`generation`, `dataset_id`, `coverage_start`, `coverage_end`, `expires_at`, `expired`, `target_matches`, `coverage_age_seconds`) with a quick-fill `Use in Replay` button (`admin-clickhouse-prefill-btn`), or a dashed empty state (`admin-clickhouse-no-active-generation`) when `active_generation` is `null`.
  4. **Bounded Artifact Replay Control Panel (`data-testid="admin-clickhouse-replay-reservation"`):** `min-h-[300px] [contain-intrinsic-size:auto_300px] [content-visibility:auto]`. Renders operation mode toggle (`admin-clickhouse-mode-rebuild` / `admin-clickhouse-mode-resume`), reference input (`admin-clickhouse-reference-input`), batch limit input `1..100` (`admin-clickhouse-limit-input`), `dry_run` checkbox (`admin-clickhouse-dry-run-checkbox`, default `true`), submit button (`admin-clickhouse-replay-submit-btn`), and replay outcome / error banners (`admin-clickhouse-replay-outcome`, `admin-clickhouse-replay-error`).
- **Non-Destructive Background Refresh:** Uses `placeholderData: keepPreviousData` and dims cards (`opacity-40 pointer-events-none`) during background refetches.
- **Status Notice Reservation:** The loading shell and client keep both contextual notices in one shared grid cell. Inactive notices are invisible and hidden from assistive technology but retain their wrapped-text height, so the status response cannot insert a banner above the counters and push subsequent cards down.

---

## 7. Interactive Workflows & Edge Cases
1. **Cluster Status Inspection & Graceful 503 Handling:** `fetchClickHouseStatus` parses the `ClickHouseStatusResponse` JSON body on both HTTP `200` (`ok` / `disabled`) and HTTP `503` (`unavailable`), rendering the `unavailable` badge and diagnostic banner without collapsing the page.
2. **Dry-Run Replay Preview:** Operator selects `Dataset Rebuild` (`dataset_id`) or `Generation Resume` (`generation`), sets `limit` (`1..100`), leaves `Dry Run Preview` checked (`dry_run: true`), and submits. The backend validates the reference in `PgManifest.replay_preview` and returns `planned_artifacts` and `artifact_count` with `attempted: 0, published: 0, activated: false`.
3. **Live Bounded Replay Execution:** Operator unchecks `Dry Run Preview` (`dry_run: false`) and submits. The backend records `clickhouse_replay` audit entries, streams sealed artifacts from FOS via `FosArtifacts.load`, publishes rows into ClickHouse (`full_rebuild` or `replay_unpublished`), and returns the activated generation summary while triggering a status refetch.
4. **Standard Mode Fallback:** When `enabled === false`, all replay form inputs and the submit button are disabled with explanatory copy (`data-testid="admin-clickhouse-replay-disabled-hint"`).

---

## 8. Performance, Cost & Telemetry Budgets
- **First Contentful Paint (FCP):** `< 1200ms` (warm-load verified `< 300ms`).
- **Cumulative Layout Shift (CLS):** `<= 0.05` (verified `0.000` across all 3 tiers).
- **FOS Overhead:** Status checks and dry-run previews execute against PostgreSQL (`PgManifest`) and ClickHouse health checks only, incurring zero Class A/B FOS operations.

---

## 9. AI Session Automated Verification Checklist
- [x] 1. Navigate to `/admin/clickhouse` as Admin; verify page loads with HTTP 200, renders `ServerFooter`, and mounts `admin-clickhouse-root`.
- [x] 2. Measure layout shifts and FCP during page load; confirm `CLS <= 0.05` and `FCP < 1200ms` with all 4 reservation containers (`admin-clickhouse-toolbar`, `admin-clickhouse-vitals-reservation`, `admin-clickhouse-generation-reservation`, `admin-clickhouse-replay-reservation`) pre-allocated.
- [x] 3. Verify `GET /api/admin/clickhouse/status?service_id=<id>` returns HTTP 200 with `enabled: false, health: "disabled"` in Standard mode and `enabled: true, health: "ok", schema_version: 2` in High-Scale mode.
- [x] 4. Verify `POST /api/admin/clickhouse/replay` rejects `limit: 101` with HTTP 422, rejects missing `dataset_id`/`generation` with HTTP 422, and returns HTTP 409 `clickhouse_disabled` in Standard mode or HTTP 200/404 in High-Scale mode.
- [x] 5. Verify Analyst sessions receive HTTP 403 Forbidden (`{"error": "admin_only"}`) on both `/api/admin/clickhouse/status` and `/api/admin/clickhouse/replay`.
- [x] 6. Confirm manual refresh (`admin-clickhouse-refresh-btn`) rotates and propagates `X-Page-Load-ID` header on `/api/admin/clickhouse/status`.

---

## 10. Automated Test Suite
- **Playwright E2E Spec:** `frontend/e2e/pages/admin-clickhouse.spec.ts` (verified across Local Standard, Remote Standard GCE, and High-Scale Elevation).
- **Frontend Unit Tests:**
  - `frontend/__tests__/app/admin/clickhouse.test.tsx`
  - `frontend/__tests__/clickhouse-msw.test.ts`
- **Backend Unit & Integration Tests:**
  - `tests/routers/test_admin_clickhouse.py`
  - `tests/remote_access/test_route_table_tripwire.py`
  - `tests/core/test_clickhouse_client.py`
  - `tests/core/test_clickhouse_publication.py`
  - `tests/core/test_clickhouse_schema.py`
  - `tests/core/test_clickhouse_metrics.py`
