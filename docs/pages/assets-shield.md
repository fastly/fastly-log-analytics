# Page Specification: Assets & Origin Shielding (`/assets-shield`)

> [!NOTE] **Live-certified on `release/v3.0.0-beta3`**
> This specification defines the verified contract, architecture behaviors, role permissions, and test suites for `/assets-shield`. Validated across Local Standard, Remote Standard GCE, and High-Scale Elevation deployments with full query attribution and zero layout shifts.

---

## 1. Overview & Objectives

The **Assets & Origin Shielding** page provides detailed caching diagnostics broken down by static asset types (images, scripts, stylesheets, fonts, video, documents) and analyzes Fastly origin shielding and edge caching performance.

### Key Tenets:
- **Asset Categorization:** Automatically groups URLs into MIME/content categories (`Images`, `Documents`, `JavaScript/CSS`, `Fonts`, `Video`, and `API/Dynamic` fallback) to evaluate request volume, egress bytes, cache hit ratio, and payload compression rate per category.
- **Cache Absorption Diagnostics:** Stacks Fastly cache delivery states (`HIT`, `HIT-SYNTHETIC`, `HIT-CLUSTER`, `HIT-STALE`, `MISS`, `MISS-CLUSTER`, `PASS`, `BYPASS`, `ERROR`) per asset type to identify unnecessary origin traffic.
- **Payload Compression Efficiency:** Compares returned Content-Encoding headers (`br`, `gzip`, `uncompressed`) per asset class to pinpoint bandwidth waste.
- **Hotspot Detection:** Surfaces large uncompressed static assets (>50KB) and low cache-TTL hotspots (recurring Cache MISSes) for rapid remediation.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.
- **Graceful Field Degradation:** If optional static asset headers (`resp_header_content_encoding`, `ttl`) are absent from the active VCL schema, surfaces an in-place actionable banner linking to `/service-config` while continuing to compute all core asset breakdown metrics.

---

## 2. Routes & URL Schema

- **Primary Route:** `/assets-shield`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - `range`: Relative window preset token (`15m`, `1h`, `24h`, `7d`, `30d`).
  - Standard drill-down filters: `content_type`, `pop`, `status`, `url`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full asset URLs, compression diagnostics, cache state distributions, and service log configuration link. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Asset breakdowns and caching metrics viewable; client IPs masked (if enabled). `POST /api/assets/aggregates` allowed under `_ANALYST_ALLOWED_WRITE_PREFIXES` and `_WRITE_VERB_GATE_ROUTES`; `GET /api/assets/aggregates` allowed in `_READ_ALLOWLIST`. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view with `execute_with_stale_view_retry`. | Ephemeral in-memory DuckDB reading DuckLake catalog. |
| **Aggregation Strategy** | Single-pass regex/pattern classification CTE over request logs with optional field detection. | Single-pass aggregation CTE. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints:
1. `POST /api/assets/aggregates`:
   - Primary data endpoint accepting `range_token`, `anchor`, `start_time`, `end_time`, and `filters`.
   - Returns `asset_type_breakdown`, `cache_performance`, `compression_performance`, `large_uncompressed_assets`, `low_ttl_assets`, and `missing_fields`.
   - Instrumented with `process_context_scope("assets:aggregates")` and protected by `execute_with_stale_view_retry`.
2. `GET /api/assets/aggregates`:
   - Query-parameter mirror for SSR / external callers.

### Telemetry Attribution:
- Every query executes under `assets:aggregates` telemetry span.
- Telemetry queries recorded and auditable via `x-debug-responses: 1`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** All sections enforce container intrinsic sizes and content-visibility:
  - `[data-testid="assets-overview-grid"]` (`min-h-[160px] [contain-intrinsic-size:160px] [content-visibility:auto]`)
  - `[data-testid="assets-cache-efficiency-card"]` (`min-h-[380px] [contain-intrinsic-size:420px] [content-visibility:auto]`)
  - `[data-testid="assets-compression-card"]` (`min-h-[380px] [contain-intrinsic-size:420px] [content-visibility:auto]`)
  - `[data-testid="assets-uncompressed-card"]` (`min-h-[320px] [contain-intrinsic-size:360px] [content-visibility:auto]`)
  - `[data-testid="assets-low-ttl-card"]` (`min-h-[320px] [contain-intrinsic-size:360px] [content-visibility:auto]`)
- **In-Place Skeletons:** Panels display contextual loading copy (`Analyzing static asset cache hit ratios...`, `Evaluating cache absorption efficiency...`, `Evaluating payload compression efficiency...`, `Scanning uncompressed static assets...`, `Detecting low cache-TTL hotspots...`).
- **Dimmed Updates:** Background updates dim panels (`opacity-60 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Missing Optional Fields Alert:** If `resp_header_content_encoding` or `ttl` are omitted from the active VCL schema, an informative alert (`[data-testid="missing-asset-fields-banner"]`) renders with a direct link to `/service-config`.
- **Drill-down to Dashboard:** Asset URLs and category cards link directly to `/dashboard` filtered by URL or asset category.
- **Detailed Help Dialogs:** Every card provides an interactive info button opening a comprehensive modal explaining calculation methods, meaning, and recommended operational actions.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **LCP** | < 1500 ms | < 1500 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (Target 0.00) | <= 0.05 (Target 0.00) | PerformanceObserver |
| **API Latency (Aggregates)** | < 400 ms (p95) | < 300 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Single composite query per window; 0 redundant queries | Single composite query; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /assets-shield` returns HTTP 200 with shell and footer environment indicator.
- [x] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS <= 0.05.
- [x] **3. Asset Aggregates Contract:** `POST /api/assets/aggregates` and `GET /api/assets/aggregates` return complete schema.
- [x] **4. Missing Fields Handling:** Gracefully handles missing `resp_header_content_encoding` and `ttl`, rendering banner linking to `/service-config`.
- [x] **5. Time Range Extent:** Confirms 24h default and clean preset switching without 500s.
- [x] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative configuration buttons; verb gate passes `/api/assets/aggregates`.
- [x] **7. Telemetry & Query Audit:** Verify all DuckDB queries execute under `assets:aggregates` with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit Tests:** `tests/routers/test_assets.py` (5 tests covering absolute ranges, relative tokens, missing fields fallback, GET params, and populated data).
- **Route Gate Tests:** `tests/remote_access/test_route_table_tripwire.py` (`POST /api/assets/aggregates` in allowed write prefixes; `GET /api/assets/aggregates` in read allowlist).
- **Frontend Unit Tests:** `frontend/__tests__/app/assets-shield.test.tsx` (5 tests verifying mounting, skeletons, data render, missing fields banner, and empty state).
- **Playwright E2E Test:** `frontend/e2e/pages/assets-shield.spec.ts` (8 contract tests validating accessibility, zero-CLS layout, schema, sections, time range switching, error count, perf budget, and telemetry).
- **Synthetic Traffic Profile:** Mixed web traffic with static JS/CSS, images, documents, and dynamic responses.
