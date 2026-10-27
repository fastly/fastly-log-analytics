# Page Specification: Edge Performance (`/performance`)

> [!NOTE] **Live-certified Specification**
> This specification defines the verified production contract, architecture behaviors, role permissions, and test suites for `/performance` (Edge Performance). Certified across Standard and High-Scale modes and verified with backend router tests, Vitest unit tests, and Playwright contract tests.

---

## 1. Overview & Objectives

The **Edge Performance** page analyzes end-to-end delivery latency across Fastly's edge cloud. It inspects Time to First Byte (TTFB), Time to Last Byte (TTLB), edge compute response times, origin wait time, origin download duration, client download duration, cache TTL distribution, and regional delivery percentiles (avg, p50, p95, p99).

### Key Tenets:
- **Latency Distribution & Waterfall:** Clear decomposition of end-to-end latency into edge processing, origin wait (TTFB), origin download (TTLB - TTFB), and client download.
- **Origin vs. Edge Processing:** Scatter plot comparison profiling edge execution against origin response times across cache hits and misses.
- **Endpoint & Network Profiling:** Top slowest URLs and ASNs ranked by configurable percentile metrics (`avg`, `p50`, `p95`, `p99`).
- **Cache TTL Distribution:** Granular histogram of cache lifetimes across standard interval buckets (`0s`, `<10s`, `<1m`, `<1h`, `<1d`, etc.).
- **Rollup Acceleration:** Leverages precomputed `perf_latency` and `perf_dims` rollups on unfiltered windows >= 48h, surfacing an `Approximate` badge while returning sub-second query times.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores (`useFilterStore`, `useServiceStore`).

---

## 2. Routes & URL Schema

- **Primary Route:** `/performance`
- **Supported Query Parameters & Store State:**
  - `service`: Fastly Service ID (`activeServiceId`).
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`, `now-30d`).
  - `range_token`, `anchor`: Server-quantized time window tokens ensuring SSR cache key parity.
  - Standard drill-down filters: `pop`, `origin`, `country`, `asn`, `status`, `content_type`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full percentile charts, slow URL drill-down, unmasked IPs, sort-by customization. |
| **Analyst (Remote Share)** | Read-Only Access | Full latency percentiles and charts viewable; registered in `_READ_ALLOWLIST` (`("POST", "/api/performance/aggregates")`). Time range strictly clamped via `ctx.clamp` to session bounds. Client IPs masked if configured. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `perf_dims` / `perf_latency` rollups. | Reads PostgreSQL metadata + ClickHouse `performance_minute_dimensions` (top URLs/ASNs), `performance_minute_summary` (waterfall), `performance_scatter_samples` (scatter) and `request_aggregates` (TTL). |
| **Percentile Engine** | `percentile_ms_expr("CAST(elapsed AS DOUBLE)", quantile, approx=True)`. | Unfiltered: request-weighted average of per-minute percentiles (`_approx: true`). Filtered: exact `quantile(p)(elapsed_ms)` over matching `request_facts` (`_approx: false`). |
| **Rollup Fast Path** | `runner.try_perf_latency_from_rollup` for top URLs and ASNs (unfiltered >= 48h). Returns `approx: true`. | Every unfiltered section is served from a minute projection. Any active filter switches all requested sections to `request_facts`; a filter field with no facts mapping is rejected with HTTP 400, never ignored. |
| **Materialized Spool** | Single `WITH components AS MATERIALIZED (...)` CTE serving both 300-row scatter sample and waterfall averages. | Waterfall averages are `sum(component sums) / sum(component_count)`. Scatter is the 300 lowest `sample_key` rows across the window; each ingest batch keeps its own 300 lowest, so the merged result stays a uniform sample. |
| **Sections / Sort** | `sections` expanded by `_expand_sections`; `sort_by` orders URLs/ASNs. | Same: the router passes the expanded section set and only those queries run; `sort_by` orders URLs/ASNs. ASN rows carry the same `label`. |

---

## 5. Backend APIs & Telemetry Attribution

### Canonical Route:
`POST /api/performance/aggregates`

### Request Payload (`PerformanceRequest`):
```json
{
  "filters": {},
  "sort_by": "p99",
  "sections": ["waterfall", "scatter", "top_urls", "top_asns", "ttl_dist"],
  "range_token": "24h",
  "anchor": "2026-10-12T20:00:00Z"
}
```

### Section Expansion & Coupling:
- `_TOP_N_PAIR`: `top_urls` and `top_asns` are auto-coupled. Requesting one includes both to share the two-pass CTE against the narrowed temp table.
- `_WATERFALL_SCATTER_PAIR`: `waterfall` and `scatter` are auto-coupled to amortize the single materialized components CTE spool.
- `ttl_dist`: Independent histogram section.

### Response Schema (`PerformanceAggregatesResponse`):
```json
{
  "waterfall": {
    "avg": {
      "edge_processing": 12.4,
      "origin_wait": 45.1,
      "origin_download": 18.2,
      "client_download": 9.5
    }
  },
  "top_urls": [
    { "url": "/api/v1/checkout", "requests": 1420, "avg": 62.1, "p50": 45.0, "p95": 115.2, "p99": 180.4 }
  ],
  "top_asns": [
    { "asn": 15169, "label": "AS15169 Google", "requests": 8420, "avg": 35.2, "p50": 28.0, "p95": 72.1, "p99": 95.0 }
  ],
  "ttl_dist": [
    { "bucket": "<1m", "count": 1200 },
    { "bucket": "<1h", "count": 8500 }
  ],
  "scatter": [
    { "origin": 45.1, "edge": 12.4, "cache": "HIT" }
  ],
  "_approx": false,
  "_query_time_ms": 42.1,
  "_query_source": "duckdb"
}
```

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID` for telemetry attribution.
- Database statements recorded in `telemetry_queries` and audited for efficiency.
- Wall-clock execution phases profiled under `_section_timings`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation (Zero CLS):**
  - Waterfall Card: `h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]`
  - Slowest URLs & Slowest Networks Tables: `min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]`
  - Cache TTL Distribution: `h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]`
  - Origin vs Edge Processing Scatter: `h-[360px] [contain-intrinsic-size:360px] [content-visibility:auto]`
- **Contextual In-Place Skeletons:**
  - Waterfall: `"Calculating latency percentiles..."`
  - Slowest URLs: `"Profiling endpoint latency..."`
  - Slowest Networks: `"Profiling network latency..."`
  - Cache TTL: `"Profiling cache TTL distribution..."`
  - Origin vs Edge: `"Profiling origin vs edge latency..."`
- **Dimmed Updates:** Query refetches maintain layout and dim background content (`opacity-40 pointer-events-none transition-opacity`) with zero cumulative layout shift (CLS = 0.00).
- **Approximate Badge:** Renders an unobtrusive `Approximate` badge when `_approx === true` on rollup-served windows (>= 48h unfiltered).

---

## 7. Interactive Workflows & Controls

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Sort By Customization:** Tables support sorting by `avg`, `p50`, `p95`, or `p99` latency.
- **Column Visibility Toggles:** `ColumnVisibilityDropdown` allows toggling visibility of `requests`, `avg`, `p50`, `p95`, and `p99` columns.
- **Hit vs Miss Latency Split:** Origin vs Edge scatter plot visually differentiates edge cache hits (`#10b981`) and misses (`#f59e0b`).

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 (<= 0.05 budget) | 0.00 (<= 0.05 budget) | PerformanceObserver |
| **API Latency (Aggregates)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_queries` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `perf_latency` rollups on >=48h; 0 redundant queries | Utilizes ClickHouse rollups; 0 redundant queries | Query profiler audit |

---

## 9. Automated Test Suite & Verification Checklist

- [x] **1. Route Accessibility:** `GET /performance` returns HTTP 200.
- [x] **2. Zero Layout Shift:** Pre-allocated intrinsic size containers (`[contain-intrinsic-size:360px]`, `[contain-intrinsic-size:300px]`, `[content-visibility:auto]`) ensure CLS <= 0.05 (measured 0.00).
- [x] **3. Data Aggregates Contract:** `POST /api/performance/aggregates` returns complete waterfall, top_urls, top_asns, ttl_dist, and scatter data.
- [x] **4. Section Pruning & Coupling:** Backend correctly handles `_TOP_N_PAIR` and `_WATERFALL_SCATTER_PAIR` couplings.
- [x] **5. Time Range Extents & SSR Parity:** Range tokens (`24h`, `7d`, `30d`) and anchors resolve identically across client and server.
- [x] **6. Role Verification:** Analyst adheres to `_READ_ALLOWLIST`, session time bounds clamping, and IP masking.
- [x] **7. Telemetry & Query Audit:** All DuckDB and ClickHouse queries are attributed with `X-Page-Load-ID` and profiled.
- [x] **8. Backend Router Tests:** `tests/routers/test_performance_router.py` (10 tests passing).
- [x] **9. Frontend Unit Tests:** `frontend/__tests__/app/performance.test.tsx` (3 tests passing).
- [x] **10. Playwright Contract Tests:** `frontend/e2e/pages/performance.spec.ts` (9 tests passing).
