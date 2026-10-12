# Page Specification: Service Summary & Fastly Value (`/fastly-value`)

> [!NOTE] **Live-certified Specification**
> This specification defines the verified production contract, architecture behaviors, role permissions, and test suites for `/fastly-value` (Service Summary & Fastly Value). Certified across Standard and High-Scale modes and verified with unit, Playwright accessibility, and backend route tests.

---

## 1. Overview & Objectives

The **Fastly Value** (Service Summary) page provides executive and financial visibility into CDN ROI, offload efficiency, egress bandwidth savings, origin request reduction, compute cost avoidance, cache hit ratio (CHR) trends, and image optimization.

### Key Tenets:
- **Financial & Operational Value:** Computes total requests, edge hit requests, origin requests, total bandwidth served, and origin egress bytes saved to quantify CDN value and operational offload.
- **8-Tab Navigation:** Organizes granular product value into 8 dedicated views:
  1. `Summary` (`overview`, `caching`, `network` sections)
  2. `CDN & Caching` (`caching` section)
  3. `Assets & Shield` (embedded `AssetsReportContent` view)
  4. `Security` (`security` section)
  5. `Bot Management` (`bots` section)
  6. `Performance` (`performance` section)
  7. `Network` (`network` section)
  8. `Image Optimizer` (`io` section & Fastly Historical Stats API)
- **Dynamic Section Expansion:** Queries only the sections needed for the active tab via `POST /api/value/summary`, minimizing DuckDB scan overhead and connection hold time.
- **Rollup Fast Paths:** Leverages precomputed `_hour_bundled_root` rollups for overview, caching, and network aggregates when unfiltered, falling back to raw table scans when filters are applied.
- **Image Optimizer (IO) & Upsell:** Fetches IO transform and bandwidth metrics from Fastly's Historical Stats API (cached via `BoundedTTLCache(maxsize=64, ttl=60.0)`). If unconfigured or 0 transforms, displays `IOUpsellView` highlighting unoptimized image bandwidth and 40% compression savings potential.
- **Shared Primitives:** Wrapped in `ReportLayout`, `FilterBar`, and global time range stores (`useFilterStore`, `useTimeRangeStore`).

---

## 2. Routes & URL Schema

- **Primary Route:** `/fastly-value`
- **Supported Query Parameters & Store State:**
  - `service`: Fastly Service ID (`activeServiceId`).
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`, `now-30d`).
  - `filters`: Custom drill-down filters (`pop`, `origin`, `content_type`, `country`, `status`, etc.).
  - `anchor`: Time quantization anchor computed via `quantizeAnchor(storeEndTime)`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full value KPIs, all 8 tabs, export capabilities, raw IP inspection. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full executive charts and offload KPIs viewable; registered in `_READ_ALLOWLIST` (`("POST", "/api/value/summary")`). Time range strictly clamped via `ctx.clamp` to session bounds. IP masking enforced if configured. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + local Parquet buffer + DuckLake table. | Ephemeral in-memory DuckDB reads cloud DuckLake Parquet directly from cloud storage. |
| **Rollup Optimization** | Uses precomputed hourly bundles (`_hour_bundled_root`) via `_try_overview_from_rollup` and `_try_network_from_rollup`. | Leverages precomputed rollup parquet files in cloud storage. |
| **Image Optimizer** | Calls Fastly Historical Stats API via `fastly("GET", ...)` with in-memory TTL cache (60s). | Same Fastly Historical Stats API integration with TTL cache. |

---

## 5. Backend APIs & Telemetry Attribution

### Primary Endpoint: `POST /api/value/summary`

- **Request Schema (`ValueSummaryRequest`):**
  ```json
  {
    "filters": {},
    "chart_interval": "1 day",
    "sections": ["overview", "caching", "network"],
    "range_token": "now-24h",
    "anchor": "2026-10-12T16:00:00Z",
    "start_time": "2026-10-11T16:00:00Z",
    "end_time": "2026-10-12T16:00:00Z"
  }
  ```
- **Response Schema (`ValueSummaryResponse`):**
  - `overview`: Total requests, bandwidth bytes, edge hit requests, origin requests, origin offload %, threats blocked, cache acceleration factor.
  - `caching`: Hit/miss/pass/synth requests, bandwidth saved bytes, shield effectiveness %, offload time-series, cache state time-series.
  - `security`: WAF blocked/logged/passed counts, threat time-series, top WAF signals.
  - `bots`: Total requests, bot requests, verified bots, top bot user agents/names.
  - `performance`: Hit latency avg, miss latency avg, cache acceleration factor, latency time-series.
  - `network`: HTTP/3 %, TLS %, IPv6 %, HTTP/2 %, protocol adoption time-series.
  - `io`: Transforms, bandwidth bytes, shield bandwidth, format distribution, modern format %, estimated cost ($0.0025/transform), optimization opportunities.

### External & Background APIs:
- `fastly("GET", "/stats/service/{id}?by=day&from={from}&to={to}")`: Fetches historical image optimization counts and bandwidth, cached in `_IO_STATS_CACHE` (`ttl=60s`).

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID` injected by frontend API middleware.
- All database queries executed by `QueryRunner` are attributed to execution sections (`overview_rollup`, `overview_agg_query`, `caching_offload_ts`, `security_threat_ts`, `network_protocol_ts`, `io_stats_fetch`, etc.).

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:**
  - Summary KPI cards use `Skeleton className="h-28"` inside reserved grid containers (`grid-cols-2 lg:grid-cols-4`).
  - Time-series and distribution charts use `Skeleton className="h-72"` (`grid-cols-1 lg:grid-cols-2`).
- **Tab Layout:**
  - Standardized tabs bar (`@/components/ui/tabs`) with icons for all 8 categories.
  - Switching to `Assets & Shield` renders `AssetsReportContent` directly while cleanly disabling the `/api/value/summary` query (`enabled: activeTab !== 'assets'`).
- **Dimmed Background Refresh:** Existing data remains visible with reduced opacity during refetches instead of collapsing into blank skeletons.

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Respects 24-hour default for mature datasets, dynamically adapting to max available extent when total service history is under 24 hours.
- **Analyst Session Window Clamping:** Backend automatically clamps requested timestamps to the analyst session bounds (`ctx.clamp(req.start_time, req.end_time)`).
- **Missing Field Graceful Degradation:** Optional fields (`protocol`, `ipv6`, `is_ssl`, `tls`, `waf`, `bot_score`, `io_input_bytes`) degrade cleanly if absent from the active VCL schema, falling back to 0 or null without query errors.
- **Zero-Origin / All-Cache Edge Cases:** Offload percentage safely handles 100% cache hit ratios or 0 origin requests without division-by-zero exceptions.
- **IO Absence & Upsell:** If Image Optimizer is not configured or returns zero transforms, `IOUpsellView` renders estimated bandwidth savings (40% compression) and unoptimized image asset analysis.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver / AxeBuilder |
| **API Latency (Summary)** | < 300 ms (p95) | < 200 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Unfiltered overview uses rollups (< 30ms) | Rollup parquet files (< 30ms) | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /fastly-value` returns HTTP 200 and mounts successfully (`frontend/__tests__/app/fastly-value.test.tsx`).
- [x] **2. Zero Layout Shift & A11y:** Verified with Playwright axe-core audit (`frontend/e2e/a11y-routes.spec.ts`, 0 WCAG 2.1 AA violations).
- [x] **3. Value Contract:** Verified offload percentage, bandwidth savings, and section expansion in `backend/repositories/value.py`.
- [x] **4. Time Range Extent:** Confirms 24h default and range token resolution via `resolve_window` and `quantizeAnchor`.
- [x] **5. Cost & IO Calculation:** Confirms `$0.0025` per transform cost estimation and 40% compression calculation.
- [x] **6. Role Verification:** Analyst Path B allowed via `_READ_ALLOWLIST` with time-window clamping verified (`tests/routers/test_value_router.py::test_value_summary_analyst_session_clamps_window`).
- [x] **7. Telemetry & Query Audit:** Verified `X-Page-Load-ID` injection and SectionTimer query attribution.

---

## 10. Automated Test Suites & Traffic Generation

- **Accessibility Suite:** `frontend/e2e/a11y-routes.spec.ts` (includes `/fastly-value` in `CLEAN_ROUTES`).
- **Frontend Smoke Test:** `frontend/__tests__/app/fastly-value.test.tsx` (tests mount, title, and all 8 tab triggers).
- **Backend Router Suite:** `tests/routers/test_value_router.py` (tests default request, missing columns, range tokens, and analyst session window clamping).
- **Route Table RBAC Tripwire:** `tests/remote_access/test_route_table_tripwire.py` (verifies `("POST", "/api/value/summary")` in read allowlist).
