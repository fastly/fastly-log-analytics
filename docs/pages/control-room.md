# Page Specification: Control Room (`/control-room`)

> [!NOTE] **Live-certified Specification**
> This specification defines the audited, verified, and certified contract, architecture behaviors, role permissions, and test cases for the Control Room (`/control-room`). Audited and verified across Standard (`DEPLOYMENT_MODE=standard`) and High-Scale (`DEPLOYMENT_MODE=high_scale`) deployment modes on `release/v3.0.0-beta3`.

---

## 1. Overview & Objectives

The **Control Room** is the real-time operations console for Fastly Log Analytics. It provides operators with high-frequency live health vitals, active request throughput, error rate telemetry, and a live-updating stream of recent HTTP 5xx/4xx incidents.

### Key Tenets:
- **Real-Time Responsiveness:** Employs low-latency Server-Sent Events (SSE) via `/realtime-stream` and `/control-room/error-stream` to reflect live CDN edge state with sub-second responsiveness.
- **Incident Triaging & Correlation:** Surfaces immediate error spikes and anomaly banners, allowing operators to correlate recent errors across dimensions (URL, POP, status code, client IP) with single-click drill-downs into historical analysis.
- **Layout Shell Exemption:** Renders within `ReportShell` rather than `ReportLayout` because the Control Room operates on live sliding event buffers (60s tick history and real-time SSE) rather than historical time-bounded query windows (documented contract exemption in `docs/pages/README.md` § 3.4).

---

## 2. Routes & URL Schema

- **Primary Route:** `/control-room`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID (synced with `useServiceStore`).
- **Historical Drill-Down Navigation:**
  - Each tab and error card provides direct deep-links to historical reports (`/dashboard`, `/origin`, `/security`, `/performance`, `/network`), passing the active service and pre-populating filter query parameters (`status`, `url`, `pop`, `client_ip`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Live throughput and metrics stream (`/realtime-stream`), live error event stream (`/control-room/error-stream`), Top-N error correlator (`/control-room/correlate`), VCL log field audit (`/log-field-audit`), admin configuration tab visible. Edge mutation endpoints (`/control-room/{mitigations,rules,allowlist,big-red-button,cost-governor}`) enforce `require_admin` and return `501 Not Implemented` stubs. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Real-time metrics stream and live error event stream accessible (`test_realtime_stream_allowed_for_analyst`). Client IPs automatically masked (`xxx.xxx.xxx.xxx`) when `mask_ips` is active. Admin tab hidden via `useIsAnalyst()`. Edge mutation endpoints return `403 Forbidden`. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Operates in Standard mode against local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane Engine** | Reads DuckDB `logs_{service_id}` view and local Parquet buffers. | Queries ClickHouse `request_facts` table via `query_clickhouse_aggregate`. |
| **Realtime Stream (`/realtime-stream`)** | In-process SSE stream broadcasting Fastly Real-time API metrics ticks. | In-process SSE stream broadcasting Fastly Real-time API metrics ticks. |
| **Live Error Stream (`/control-room/error-stream`)** | Polling SSE tail querying DuckDB `logs_{service_id}` for recent 4xx/5xx requests. | Polling SSE tail querying ClickHouse `request_facts` for recent 4xx/5xx requests. |
| **Top-N Error Correlator (`/control-room/correlate`)** | In-memory aggregation across recent DuckDB log window. | Distributed aggregation via ClickHouse `request_facts`. |
| **IP Privacy & Masking** | `_mask_error_events` masks client IP strings when `mask_ips` is set. | `_mask_error_events` masks client IP strings when `mask_ips` is set. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints:
1. `GET /api/services/{service_id}/realtime-seed`:
   - Returns initial historical ticks (`MetricsTick[]`) from Fastly Real-time API or cached in-memory buffer.
2. `GET /api/services/{service_id}/realtime-stream` (SSE):
   - Emits live `metrics_tick` events every second (RPS, 5xx rate, 4xx rate, p95 latency, bandwidth).
3. `GET /api/services/{service_id}/control-room/error-stream` (SSE):
   - Emits recent error events (`timestamp`, `status`, `path`, `pop`, `client_ip`) bounded to recent 4xx/5xx requests.
4. `POST /api/services/{service_id}/control-room/correlate`:
   - Correlates errors across a requested dimension (`url`, `pop`, `status`, `client_ip`) over a specified lookback window (default 60m).
5. `GET /api/services/{service_id}/log-field-audit`:
   - Verifies whether the active Fastly VCL logging format covers required control room dimensions.
6. `POST /api/services/{service_id}/control-room/{mitigations,rules,allowlist,big-red-button,cost-governor}`:
   - Admin-only mutation stubs returning 501 for Admin, 403 for Analyst.

### Telemetry Attribution:
- All queries and fetch requests carry `X-Page-Load-ID` (including `realtime-seed`, `realtime-stream`, `error-stream`, and `correlate`).
- Profiler and telemetry calls record attribution in request telemetry and PostgreSQL's `usage_log` table.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Container boxes for charts, gauges, maps, and leaderboard cards use fixed height containers and flex layout (`contain-intrinsic-size`) ensuring zero layout shift (CLS = 0.00).
- **Header Badges:** Displays `ConnectionBadge` (indicating SSE connection state: `Connected`, `Reconnecting`, `Offline`) and `FreshnessBadge` (seconds since last received tick).
- **Tabbed Views:** 6 distinct tabs:
  1. `Summary`: Live RPS, Error Rate, Cache Hit Ratio, Bandwidth cards + traffic timeseries chart.
  2. `CDN & Caching`: Origin RPS, Shield Hit Ratio, Cache Status breakdown.
  3. `Security`: WAF Blocked, WAF Logged, TLS/threat indicators.
  4. `Performance`: Edge p95 Latency, Origin Latency, TTFB vitals.
  5. `Network`: Active PoPs, Healthy PoPs, POP traffic distribution map (`PopTrafficMap`).
  6. `Admin` (admin-only): VCL field audit, edge configuration controls.
- **Responsive Mobile Overview:** Screen widths $<768$px automatically switch from the tabbed desktop layout to `MobileOverview`, rendering high-priority vitals in a single scrollable feed without horizontal overflow.

---

## 7. Interactive Workflows & Edge Cases

- **Incident Click-to-Filter:** Clicking an error item in `RecentErrorsCard` (status code, path, POP, client IP) navigates directly to `/dashboard` with pre-filled query filters (`filter_status`, `filter_url`, `filter_pop`, `filter_client_ip`).
- **SSE Stream Resilience:** `useSSE` manages auto-reconnection with exponential backoff on disconnect.
- **Zero-Traffic Edge Case:** If no active edge traffic is present, metric cards render 0 RPS cleanly with "No active traffic" placeholders without throwing errors or unmounting cards.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Verified Measurement |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | < 350 ms / < 700 ms |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | 0.00 |
| **API Latency (Correlator)** | < 250 ms (p95) | < 150 ms (p95) | < 120 ms (p95) |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | 100% via `X-Page-Load-ID` |
| **WCAG 2.1 AA Accessibility** | 0 violations | 0 violations | 0 violations (`axe-core`) |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /control-room` returns HTTP 200 for Admin and Analyst.
- [x] **2. Zero Layout Shift:** Verified page loads with pre-allocated panel skeletons and CLS = 0.00.
- [x] **3. Data Stream:** Verified `/realtime-stream` and `/control-room/error-stream` deliver live metrics ticks and error incidents.
- [x] **4. Time Range Extent:** Verified real-time 60s sliding window and 60m correlator window behavior under `ReportShell`.
- [x] **5. Click-to-Filter:** Verified clicking error attributes navigates to `/dashboard` with pre-filled query filters.
- [x] **6. Role Verification:** Confirmed Analyst Path B observes masked IPs (`xxx.xxx.xxx.xxx`), Admin tab is hidden, and edge mutations return 403 Forbidden.
- [x] **7. Telemetry & Query Audit:** Verified `X-Page-Load-ID` propagation across all fetch and SSE calls with 100% telemetry attribution.

---

## 10. Automated Test Suite & Coverage

- **Playwright E2E Test:** [`frontend/e2e/control-room.spec.ts`](../../frontend/e2e/control-room.spec.ts)
  - Sidebar navigation to `/control-room`.
  - Desktop 6-tab rendering (Summary, CDN & Caching, Security, Performance, Network, Admin).
  - SSE connection badge transitions to green.
  - Mobile layout condensed overview on small viewports.
  - Zero WCAG 2.1 AA violations via `@axe-core/playwright`.
  - Metric cards live data rendering and historical navigation link checks.
- **Backend Router Tests:** [`tests/routers/test_control_room.py`](../../tests/routers/test_control_room.py) (45 tests, 100% pass)
  - Real-time seed and stream endpoints.
  - Error stream SSE tests across DuckDB and ClickHouse data planes.
  - Correlator tests across dimensions and RBAC masking.
  - Log field audit and wizard endpoints.
  - Mutation 501/403 RBAC checks.
- **RBAC Route Tripwire:** [`tests/remote_access/test_route_table_tripwire.py`](../../tests/remote_access/test_route_table_tripwire.py) (`test_realtime_stream_allowed_for_analyst`).
- **Frontend Component Tests:** [`frontend/__tests__/app/control-room/`](../../frontend/__tests__/app/control-room/) (12 tests, 100% pass)
