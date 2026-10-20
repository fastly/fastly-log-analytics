# Page Specification: User Sessions Explorer (`/sessions`)

> [!NOTE] **Live-certified on `release/v3.0.0-beta3`**
> This specification defines the verified contract, architecture behaviors, role permissions, and test suites for `/sessions`. Validated across Local Standard, Remote Standard GCE, and High-Scale Elevation deployments with full query attribution and zero layout shifts.

---

## 1. Overview & Objectives

The **User Sessions Explorer** aggregates client requests into behavioral sessions grouped by client IP, TLS JA4 fingerprint, or Edge Session ID (`edge_sid`). It empowers operators and security analysts to inspect session duration, request volumes, 4xx error ratios, round-trip times, Autonomous System Numbers (ASNs), and identify flagged or video-streaming client activities.

### Key Tenets:
- **Session Clustering:** Groups high-frequency request sequences into logical client sessions bounded by a 7-day safety ceiling.
- **Flagging & Labeling:** Surfaces operator-flagged sessions and integration with session scoring labels (`useScoringLabels`).
- **Opaque Token Drill-Down:** Generates cryptographically sealed, tamper-proof `session_token`s that allow Analyst Path B users with client IP masking to inspect deep request-level session logs without deanonymizing real client IPs.
- **CMCD Video Streaming Integration:** Detects Common Media Client Data (CMCD) beacons and provides direct deep-link navigation to `/sessions/stream?token={session_token}`.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/sessions`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`, bounded by 7-day max window).
  - Standard drill-down filters: `flagged_only`, `streaming_only`, `min_reqs`, `min_4xx_pct`, `country`, `asn`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full session details, unmasked client IPs, full access to flag/label sessions and drill down into raw request records. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Session distributions and metrics viewable; client IPs masked (if enabled). Allowed under `_ANALYST_ALLOWED_WRITE_PREFIXES` and `_WRITE_VERB_GATE_ROUTES` for `POST /api/sessions` and `POST /api/sessions/detail`. Opaque `session_token` unsealing permits drill-down without violating IP redaction policies. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `sessions` rollups with `execute_with_stale_view_retry` protection against Trap #35. | Reads Postgres DuckLake / ClickHouse session aggregates via `backend/high_scale/sessions.py`. |
| **Session Drill-Down** | Reads DuckDB raw log records for the unsealed session time extent with stale-view retry. | Queries ClickHouse `request_facts` partitioned table for the unsealed session time extent. |

High-scale sessions apply `filters` (shared `_filters.py`), `sort_by`/`sort_dir` (same
column allow-list as the standard path), real pagination, and report a real `total`
(window `count()` over the filtered sessions, not a page-size guess). `has_rtt`,
`has_ja4`, `has_edge`, `has_edge_sid` and `has_cmcd` reflect the data; `is_streaming` and
`streaming_only` read the `cmcd` map (CMCD fields are stored there, not in
`custom_fields`); edge/shield counts understand the stored boolean spellings (`1`/`true`);
`asn_names` is hoisted like the standard response.

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `POST /api/sessions`:
   - Body: `SessionsRequest` (`start_time`, `end_time`, `range_token`, `anchor`, `filters`, `page`, `limit`, `sort_by`, `sort_dir`, `flagged_only`, `streaming_only`, `min_reqs_flag`, `min_4xx_pct_flag`).
   - Returns: `SessionsResponse` (`sessions` array with opaque `session_token`, `total_count`, `page`, `limit`, `min_reqs_flag`, `min_4xx_pct_flag`).
   - Tagged with structured telemetry span `process_context_scope("sessions:list")`.
2. `POST /api/sessions/detail`:
   - Body: `SessionDetailRequest` (`session_token`, or optional `ip`, `ja4`, `start_time`, `end_time`).
   - Returns: `SessionDetailResponse` (`columns`, `data` rows of individual request logs).
   - Tagged with structured telemetry span `process_context_scope("sessions:detail")`.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Sessions table container (`data-testid="sessions-table-container"`) enforces container intrinsic sizes (`min-h-[500px] [contain-intrinsic-size:500px]`).
- **In-Place Skeletons:** Cards display contextual loading skeleton rows.
- **Dimmed Updates:** In-flight refetches dim the controls and table (`opacity-40 pointer-events-none`) with zero layout shift (CLS <= 0.05).
- **Lazy-Loaded Inspector:** `SessionDetail` dialog is dynamically imported to keep initial bundle size lean.

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, bounded to a 7-day maximum window (with an informative alert displayed if exceeded).
- **Session Detail Modal:** Clicking a table row or the "Details" action button opens `SessionDetail` displaying start/end times, requests count, unique URLs, edge/shield breakdown, median RTT, and individual log records.
- **Streaming Deep Link:** Sessions with CMCD telemetry display a streaming badge and a deep link button to `/sessions/stream?token=...`.
- **Zero-Width Window Handling:** Sessions with a single request (`session_start == session_end`) automatically widen by 1s on each side to prevent empty range clamping errors.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **LCP** | < 1500 ms | < 1500 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (Target 0.00) | <= 0.05 (Target 0.00) | PerformanceObserver |
| **API Latency (List Sessions)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Utilizes session rollups / partitioned views; 0 redundant queries | Utilizes ClickHouse partitioned queries; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /sessions` returns HTTP 200 and renders environment indicator.
- [x] **2. Zero Layout Shift:** Page loads with pre-allocated zero-CLS containers (`contain-intrinsic-size:500px`) and CLS <= 0.05.
- [x] **3. Sessions Table Contract:** `POST /api/sessions` returns valid typed Pydantic schema with opaque `session_token` on every row.
- [x] **4. Session Detail Contract:** `POST /api/sessions/detail` unseals `session_token` and returns request logs with column definitions.
- [x] **5. Interactive Controls:** Flagged toggle, streaming toggle, min-requests, and min-4xx% filters function smoothly without console errors.
- [x] **6. Session Detail Modal:** Clicking a session row opens the inspector dialog with parsed fields.
- [x] **7. Role Verification:** Confirm Analyst Path B observes masked IPs and can drill down via `session_token` without PII violation.
- [x] **8. Telemetry & Query Audit:** Verify all DuckDB and ClickHouse queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit Tests:** `tests/routers/test_sessions_token.py` (17 tests covering token sealing, unsealing, expiration, tampering, zero-width window expansion, range tokens, telemetry attribution, and stale view retry).
- **Route Gate Tests:** `tests/remote_access/test_route_table_tripwire.py` (`POST /api/sessions`, `POST /api/sessions/detail` in `_WRITE_VERB_GATE_ROUTES`).
- **Frontend Unit Tests:** `frontend/__tests__/app/sessions.test.tsx`, `frontend/__tests__/components/sessions/SessionsTable.test.tsx`, `frontend/__tests__/components/sessions/ScoringControls.test.tsx`, `frontend/__tests__/components/sessions/SessionDetail.test.tsx`.
- **Playwright E2E Test:** `frontend/e2e/pages/sessions.spec.ts` (8 contract tests asserting route accessibility, layout stability, schema validation, filter controls, detail dialog, stream detail integration, perf budget, and telemetry).
- **Synthetic Traffic Profile:** Injected user traffic with mixed HTTP methods, status codes, and CMCD streaming sessions.
