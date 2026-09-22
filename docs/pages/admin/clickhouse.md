> [!TODO]
> **Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target functional, architectural, role, and telemetry contract for the **Admin ClickHouse Replay & Cluster Operations** page (`/admin/clickhouse`).
> An AI testing session has not yet verified this page against a running cluster. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Page Specification: Admin ClickHouse Replay & Cluster Operations (`/admin/clickhouse`)

## 1. Overview & Objectives
The ClickHouse Replay & Cluster Operations page provides administrative visibility and control over the High-Scale ClickHouse analytical engine and its bounded replay machinery. ClickHouse serves as the primary high-throughput event and minute-rollup data plane in `DEPLOYMENT_MODE=high_throughput` topologies, ingesting millions of RPS into partitioned MergeTree tables (`request_facts`, `high_scale_batch_publications`, `origin_minute_summary`, `network_minute_dimensions`, etc.).

### Key Tenets:
- **Cluster & Schema Health Monitoring:** Surface authenticated connection health, schema version alignment (`CLICKHOUSE_SCHEMA_VERSION`), and disk capacity metrics directly from ClickHouse system tables (`system.disks`).
- **Bounded Diagnostic Replay:** Provide operators with a safe, bounded replay mechanism to rebuild or resume ClickHouse analytical state from immutable Fastly Object Storage (FOS) raw artifacts and PostgreSQL replay manifests (`PgManifest`).
- **Dry-Run Safety:** Replay workflows mandate an explicit dry-run preview before executing data mutations or activating new dataset generations.
- **Strict Role & Tenancy Isolation:** Administrative operations are restricted to `read_write` Admin sessions. Analysts receive HTTP 403 Forbidden.

---

## 2. Routes & URL Schema
- **Primary Route:** `/admin/clickhouse`
- **Query Parameters:**
  - `service_id` (*required*): Fastly logging service identifier (e.g. `?service_id=<service-id>`).
  - `tab` (*optional*): Sub-view selection (`status` | `replay` | `tables` | `partitions`). Defaults to `status`.
  - `dataset_id` (*optional*): Pre-selected dataset generation identifier for targeted replay inspection.

---

## 3. Role & Permission Matrix
| Role | Page Access | Replay Execution | Dry-Run Preview | Schema Inspection |
|---|---|---|---|---|
| **Admin** (`read_write`) | Full Access | Allowed (cap: 100 artifacts) | Allowed | Full visibility |
| **Analyst Path B** (Remote Share) | HTTP 403 Forbidden | Blocked | Blocked | Blocked |
| **Analyst Path A** (Standalone) | HTTP 403 Forbidden | Blocked | Blocked | Blocked |

*RemoteAccessMiddleware blocks all `/api/admin/*` routes for analysts.*

---

## 4. Architecture Execution Matrix
| Architecture / Mode | Engine State | Behavior |
|---|---|---|
| **Standard** (`DEPLOYMENT_MODE=standard`) | Disabled | Page displays informational banner: ClickHouse is not active in Standard architecture (DuckDB/DuckLake serves all queries). `/api/admin/clickhouse/status` returns `enabled: false, health: "disabled"`. |
| **High-Scale** (`DEPLOYMENT_MODE=high_throughput`) | Active | Full cluster monitoring, schema validation, publication lag tracking, and bounded replay operations via `ClickHouseClient` and `PgManifest`. |

---

## 5. Backend APIs & Telemetry Attribution
### Endpoints
1. `GET /api/admin/clickhouse/status?service_id={id}`
   - **Response Model:** `ClickHouseStatusResponse`
   - **Fields:** `service_id`, `enabled`, `health` (`ok` | `unavailable` | `disabled`), `schema_version`, `target_identity`, `active_dataset`, `active_generation`, `disk_free_bytes`, `disk_total_bytes`, `publication_lag_seconds`.
2. `POST /api/admin/clickhouse/replay`
   - **Request Model:** `ClickHouseReplayRequest` (`service_id`, `dry_run`: bool, `dataset_id`?: str, `generation`?: int, `limit`: int [1..100])
   - **Response Model:** `ClickHouseReplayResponse` (`service_id`, `dry_run`, `operation`, `generation`, `limit`, `planned_artifacts`, `published_artifacts`, `acknowledged_rows`, `duration_ms`)
   - **Error Model:** `ClickHouseReplayErrorResponse` (HTTP 409 if disabled or ineligible, 422 if limit > 100).

### Telemetry Attribution
- **Tracing:** Spans tagged with `component: admin_clickhouse`, `service_id`, and `operation`.
- **Query Registry:** All internal ClickHouse queries (`execute`, `insert`) register into `query_registry.register("ClickHouse", ...)` and appear in the Live Query Monitor (`/admin/queries`).
- **Prometheus Metrics:** Exported via OpenTelemetry and scraped by Prometheus (`clickhouse.rules.yml`):
  - `app_clickhouse_up`
  - `app_clickhouse_query_duration_ms`
  - `app_clickhouse_insert_duration_ms`
  - `app_clickhouse_rows_inserted_total`
  - `app_clickhouse_bytes_read_bytes_total`
  - `app_clickhouse_publication_lag_seconds`
  - `app_clickhouse_disk_free_bytes` / `app_clickhouse_disk_total_bytes`

---

## 6. UI Components, Skeletons & Layout Reservation
- **Layout Shell:** Admin navigation shell with persistent top status bar and breadcrumbs.
- **Zero-CLS Skeletons:**
  - Cluster Vitals Card: Fixed height (`min-h-[160px]`) containing health pill, schema badge, disk progress bar.
  - Active Dataset Banner: Fixed height (`min-h-[100px]`) displaying active generation ID, publication lag, and last rebuild timestamp.
  - Replay Control Panel: Form controls with intrinsic reservation (`min-h-[240px]`), limit slider, dry-run toggle, and action buttons.
  - Artifact Table / Audit Stream: Paginated table with fixed row heights (`h-12`) and loading skeletons.

---

## 7. Interactive Workflows & Edge Cases
1. **Cluster Status Polling:** Page polls `/api/admin/clickhouse/status` every 10s. If status transitions to `unavailable`, a prominent warning alert renders with retry guidance.
2. **Dry-Run Replay Simulation:**
   - Operator selects target generation or full rebuild, sets artifact limit (default 20, max 100), and checks "Dry Run".
   - Submitting fires `POST /api/admin/clickhouse/replay` with `dry_run: true`.
   - UI renders a summary modal showing planned artifacts, projected row count, and estimated duration without executing any inserts or mutations.
3. **Live Replay Execution:**
   - Operator unchecks "Dry Run" and confirms via confirmation dialog.
   - UI locks replay controls, renders an active progress spinner, and streams publication outcome upon completion.
   - New generation becomes active in `PgManifest` and is acknowledged by ClickHouse.
4. **Standard Mode Fallback:** When running against a Standard service, all action buttons are disabled with tooltip explanation: "ClickHouse is disabled in Standard deployment mode."

---

## 8. Performance, Cost & Telemetry Budgets
- **First Contentful Paint (FCP):** < 400ms.
- **Time to Interactive (TTI):** < 800ms.
- **Cumulative Layout Shift (CLS):** 0.00.
- **Status API Latency (p95):** < 150ms.
- **Replay Preview Latency (p95):** < 300ms.
- **FOS Overhead:** Replay reads immutable `.gz` artifacts from FOS; previews perform metadata-only index lookups in PostgreSQL, incurring zero Class A/B FOS charges.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Navigate to `/admin/clickhouse?service_id=<id>` as Admin; verify page loads with HTTP 200 and zero console errors.
- [ ] 2. Measure layout shifts during status fetch; confirm CLS = 0.00.
- [ ] 3. Verify Cluster Vitals card renders correct connection health (`ok` or `disabled`), schema version, and disk capacity.
- [ ] 4. In High-Scale mode, verify `app_clickhouse_up=1` metric aligns with UI status.
- [ ] 5. Trigger a Dry-Run replay preview with `limit=10`; confirm `planned_artifacts` is returned and `published_artifacts=0`.
- [ ] 6. Verify that requesting replay limit > 100 returns HTTP 422 with canonical error payload.
- [ ] 7. In Standard mode (`DEPLOYMENT_MODE=standard`), verify page gracefully indicates ClickHouse is disabled and status endpoint returns `enabled: false`.
- [ ] 8. Verify all ClickHouse queries during status check appear in Live Query Monitor (`/admin/queries`) attributed to `"ClickHouse"`.
- [ ] 9. Attempt to access `/admin/clickhouse` using an Analyst session; verify strict HTTP 403 Forbidden redirect.
- [ ] 10. Confirm all network calls pass `X-Page-Load-ID` header and generate attributed trace spans.

---

## 10. Automated Test Suite & Traffic Generation
- **Playwright Test Specs:**
  - `frontend/__tests__/clickhouse-msw.test.ts` (API mock contract verification).
  - `e2e/admin/clickhouse.spec.ts` (Cluster status, dry-run replay modal, RBAC enforcement).
- **Backend Tests:**
  - `tests/core/test_clickhouse_client.py`
  - `tests/core/test_clickhouse_publication.py`
  - `tests/core/test_clickhouse_schema.py`
  - `tests/core/test_clickhouse_metrics.py`
  - `tests/high_scale/test_clickhouse_backup.py`
- **Replay Verification Harness:**
  - Run bounded test replay: `python -m scripts.clickhouse_replay --service-id=<id> --limit=10 --dry-run`.
