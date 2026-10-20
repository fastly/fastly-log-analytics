# Page Specification: Live Log Streaming (`/streaming`)

> [!NOTE] **Live-certified (`release/v3.0.0-beta3`)**
> Audited and verified across Local Standard, Remote Standard GCE, and Elevation High-Scale.
> Verified real-time SSE stream contract (`/api/stream/{service_id}` and `/api/services/{service_id}/stream`), zero-CLS layout reservations (`[contain-intrinsic-size:650px]` and `[content-visibility:auto]`), terminal interface with pause/resume buffering, live regex/string search, status filtering, and deep JSON row inspector modal.

---

## 1. Overview & Objectives

The **Live Log Streaming** page delivers a high-speed, interactive terminal-style live tail of incoming CDN edge log lines via Server-Sent Events (SSE). It supports pause/resume, client-side regex search, JSON inspect modal, and column formatting.

### Key Tenets:
- **Low-Latency Streaming:** Pushes raw edge request events as they land in ingest buffers.
- **Interactive Inspection:** Clicking any streamed line opens the deep JSON inspector showing all captured Fastly VCL variables.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/streaming`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `filter`: Live regex/string search pattern.
  - Standard drill-down filters: `status`, `pop`, `client_ip`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full raw JSON log records, unmasked client IPs, full request headers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Live stream viewable; client IPs masked (enforced via `mask_ips_for` and invite policy). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Streams from local buffer/lake. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Stream Transport** | FastAPI SSE streaming endpoint (`/api/stream/{service_id}` & `/api/services/{service_id}/stream`). | FastAPI SSE backed by Redis / ClickHouse logs query bridge. |
| **Buffer Source** | Reads newest rows from local memory ring buffer and DuckDB view. | Reads newest events from ClickHouse / Redis stream. |

High-scale `POST /api/cmcd/aggregates` computes every section from
`cmcd_projection_facts` (`backend/high_scale/cmcd.py`): overview, the five time series
(padded to every bucket of the window), top content, rebuffer by country and ASN, object
type and streaming-format distributions, and the session-duration histogram. The
projection stores the CMCD key/value map plus the request's `country` and `asn`
(`backend/high_scale/projections.py`). `filters` on `country`/`asn` are applied; any
other filter is a `400`. Sections whose CMCD key never appears in the window are omitted,
as on the standard path. Rows written before the geography columns existed need the
rebuild in [the reader-parity backfill runbook](../runbooks/high-scale-reader-parity-backfill.md)
before the country/ASN sections populate.

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/stream/{service_id}` and `GET /api/services/{service_id}/stream` (Server-Sent Events):
   - Streams JSON chunks with typed `StreamLogEvent` fields (timestamp, client_ip, method, url, status, pop, time_elapsed, geo_country, asn, response_body_size).
   - Enforces tenancy (`RequestContext` / `require_service_access`), IP masking (`mask_ips_for`), client disconnect detection (`request.is_disconnected()`), keep-alive heartbeats (`ping=5`), and `SSE_PASSTHROUGH_HEADERS`.

### Telemetry Attribution:
- Connection establishment and stream duration recorded with `process_context_scope("stream:tail")`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Terminal Stream Box, Stream Controls Toolbar, and Inspector Drawer enforce container intrinsic sizes (`[contain-intrinsic-size:650px] [content-visibility:auto]`).
- **In-Place Skeletons:** Renders a connected stream skeleton (`Connecting to live Fastly log stream...`).
- **Zero CLS Log Append:** Appending log rows utilizes bounded circular buffer (max 1,000 items) to prevent DOM memory leaks and eliminate scroll jumps.

---

## 7. Interactive Workflows & Edge Cases

- **Pause / Resume:** Spacebar or UI button pauses stream without dropping the SSE connection; incoming items are safely queued in a pause buffer; resuming flushes the buffer seamlessly.
- **JSON Inspector Modal:** Clicking any log line opens the dialog modal with formatted table and raw JSON tabs plus a Copy JSON button.
- **Stream Disconnect Recovery:** Automatically reconnects with exponential backoff if SSE stream breaks.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **Stream Event Latency** | < 1000 ms from edge ingest | < 1000 ms from edge ingest | Timestamp diff |
| **Telemetry Capture** | 100% streams captured | 100% streams captured | `usage_log.db` audit |
| **Memory Leak Guard** | Max 1000 DOM lines capped | Max 1000 DOM lines capped | Bounded circular buffer |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /streaming` returns HTTP 200 with footer environment indicator.
- [x] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS <= 0.05.
- [x] **3. SSE Connection:** Verify `/api/stream/{service_id}` establishes connection and streams typed JSON events.
- [x] **4. Pause/Resume:** Toggle pause; verify stream freezes in place without UI errors and resumes cleanly.
- [x] **5. Line Inspector:** Click a streamed row; verify inspector dialog opens with parsed JSON attributes and Copy JSON button.
- [x] **6. Role Verification:** Confirm Analyst Path B observes masked client IPs in streamed lines via `mask_ips_for`.
- [x] **7. Telemetry & Query Audit:** Verify SSE stream connection is handled with disconnect tracking and route tripwire protection.

---

## 10. Automated Test Suite & Verification

- **Backend Router Tests:** `tests/routers/test_stream_router.py` (7 tests covering event formatting, IP masking with `pii_policy`, fallback fields, Standard mode streaming, event deduplication, High-Scale mode streaming, and disconnect handling).
- **Route Table Tripwire:** `tests/remote_access/test_route_table_tripwire.py` (verifies permissions and allowlist entry for `/api/stream/{service_id}` and `/api/services/{service_id}/stream`).
- **Frontend Unit Tests:** `frontend/__tests__/app/streaming.test.tsx` (7 tests covering mounting, connecting skeleton, SSE event reception, search & status filtering, row inspector modal, pause/resume buffering, and log clearing).
- **Playwright E2E Integration Suite:** `frontend/e2e/pages/streaming.spec.ts` (contract tests covering route accessibility, zero-CLS layout reservations, controls toolbar, pause/resume toggle, search input, row inspector, and performance budgets).
