# Page Specification: Origin Health & Shielding (`/origin`)

> [!NOTE] **Live-certified Specification**
> This specification defines the verified production contract, architecture behaviors, role permissions, and test suites for `/origin` (Origin Health & Performance). Certified across Standard and High-Scale modes and verified with backend router tests, Vitest unit tests, and Playwright contract tests.

---

## 1. Overview & Objectives

The **Origin Health & Shielding** page focuses on backend origin infrastructure, shielding efficiency, origin connect latency, first-byte delays, backend error rates (502, 503, 504), origin retries, and origin host distributions.

### Key Tenets:
- **Backend Infrastructure Monitoring:** Surfaces origin health, latency percentiles (P50, P75, P95, P99), and payload sizes across backend hosts.
- **Shielding Topology & Path Breakdown:** Tracks shielding POP performance vs direct-to-origin fetches, distinguishing edge and shield hops.
- **Endpoint & Network Drill-Down:** Profiles slowest origin URLs, POP-level latency, and per-origin-IP error and latency rates.
- **Rollup Acceleration:** Leverages precomputed `origin_summary`, `origin_dims`, and `origin_latency_ts` rollups on wide time windows (>= 48h), surfacing an `Approximate` badge while returning sub-second query times.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores (`useFilterStore`, `useServiceStore`).

---

## 2. Routes & URL Schema

- **Primary Route:** `/origin`
- **Supported Query Parameters & Store State:**
  - `service`: Fastly Service ID (`activeServiceId`).
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`, `now-30d`).
  - `range_token`, `anchor`: Server-quantized time window tokens ensuring SSR cache key parity.
  - Standard drill-down filters: `pop`, `origin`, `status`, `content_type`, `url`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full origin host names, backend IP diagnostics, retry logs, and drill-down controls. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Origin health metrics viewable; invite session bounds clamped via `ctx.clamp`; write/admin endpoints blocked. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `origin_summary`, `origin_dims`, and `origin_latency_ts` rollups. | Reads ClickHouse / DuckLake origin tables. |
| **Rollup Optimization** | Leverages local NVMe rollup parquet files for instant multi-day views. | Leverages server-side pre-aggregates and materialized views. |

---

## 5. Backend APIs & Telemetry Attribution

### Primary Composite Endpoint:
- `POST /api/origin/aggregates`:
  - Composite single-round-trip endpoint populating summary KPIs, latency timeseries, slowest URLs, status code distribution, path breakdown, POP latency, and IP health.
  - Supports section pruning (`sections: [...]`) with dependency coupling (`_TS_STATUS_PATH_TRIPLE`, `_POP_IP_PAIR`).
  - Accepts `range_token` and `anchor` for deterministic SSR cache hydration.

### Granular & Drill-Down Endpoints:
- `POST /api/origin/summary`: High-level TTFB/TTLB, error rates, and fetch volume.
- `POST /api/origin/timeseries`: TTFB/TTLB latency timeseries bucketed by interval.
- `POST /api/origin/slow-urls`: Ranked slowest URLs by P95/P99 latency.
- `POST /api/origin/status-codes`: Status code distribution (2xx, 3xx, 4xx, 5xx, and synthetic fallback).
- `POST /api/origin/path-breakdown`: Shielding analysis and path hops.
- `POST /api/origin/pop-latency`: Origin latency grouped by Fastly edge/shield POP.
- `POST /api/origin/ip-health`: Backend IP health, error percentages, and latency.
- `POST /api/origin/drilldown`: Multi-dimensional drill-down by `host`, `shield_pop`, `datacenter`, `protocol`, `pop`, or `oip`.
- `POST /api/origin/shielding-analysis`: Detailed origin shielding efficiency analysis.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency via `_debug_queries`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:**
  - KPI Cards: `min-h-[110px] [contain-intrinsic-size:110px] [content-visibility:auto]`
  - Timeseries & Status Distribution: `h-[400px] [contain-intrinsic-size:400px] [content-visibility:auto]`
  - Data Tables (Slow URLs, POP Latency, IP Health): `min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]`
  - Payload Size Card: `min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]`
- **In-Place Skeletons & Loading Messages:**
  - Origin TTFB (P50): `"Loading Origin TTFB (P50)..."`
  - Origin TTFB (P95): `"Loading Origin TTFB (P95)..."`
  - Origin Error Rate: `"Loading Origin Error Rate..."`
  - Fetch Volume: `"Loading Fetch Volume..."`
  - Origin Latency: `"Loading origin latency timeseries..."`
  - Status Code Distribution: `"Loading origin status code breakdown..."`
  - Slow URLs: `"Loading slowest URLs at origin..."`
  - POP Latency: `"Loading origin performance by POP..."`
  - IP Health: `"Loading origin IP health..."`
  - Payload Size: `"Loading origin payload size..."`
- **Dimmed Updates:** Query refetches maintain layout and dim background content (`opacity-40 pointer-events-none transition-opacity`) with zero cumulative layout shift (CLS = 0.00).
- **Approximate Badge:** Renders an unobtrusive `Approximate` badge when `_approx === true` on rollup-served windows (>= 48h unfiltered).

---

## 7. Interactive Workflows & Controls

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Metric & Percentile Toggles:** Switch timeseries metric between `TTFB` and `TTLB`, and percentiles between `P50`, `P95`, and `P99`.
- **Moving Average Trends:** Configurable trend smoothing (`off`, `auto`, `1m`, `5m`, `1h`, `1d`).
- **Column Visibility Toggles:** `ColumnVisibilityDropdown` allows toggling columns across Slow URLs, POP Latency, and IP Health tables.
- **Zero-Origin Edge Case:** When all requests are edge cache hits, origin panels display "No Origin Metrics Yet" cleanly without errors.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 (<= 0.05 budget) | 0.00 (<= 0.05 budget) | PerformanceObserver |
| **API Latency (Aggregates)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_queries` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `origin_summary` rollups on >=48h; 0 redundant queries | Utilizes ClickHouse rollups; 0 redundant queries | Query profiler audit |

---

## 9. Automated Test Suite & Verification Checklist

- [x] **1. Route Accessibility:** `GET /origin` returns HTTP 200.
- [x] **2. Zero Layout Shift:** Pre-allocated intrinsic size containers (`[contain-intrinsic-size:110px]`, `[contain-intrinsic-size:400px]`, `[contain-intrinsic-size:300px]`, `[contain-intrinsic-size:220px]`, `[content-visibility:auto]`) ensure CLS <= 0.05 (measured 0.00).
- [x] **3. Data Aggregates Contract:** `POST /api/origin/aggregates` returns complete summary, timeseries, slow_urls, status_codes, path_breakdown, pop_latency, and ip_health.
- [x] **4. Section Pruning & Coupling:** Backend correctly handles `_TS_STATUS_PATH_TRIPLE` and `_POP_IP_PAIR` couplings.
- [x] **5. Time Range Extents & SSR Parity:** Range tokens (`15m`, `24h`, `30d`) and anchors resolve identically across client and server.
- [x] **6. Role Verification:** Analyst Path B adheres to `_ANALYST_ALLOWED_WRITE_PREFIXES`, session time bounds clamping, and IP masking.
- [x] **7. Telemetry & Query Audit:** All DuckDB and ClickHouse queries are attributed with `X-Page-Load-ID` and profiled.
- [x] **8. Backend Router Tests:** `tests/routers/test_origin_router.py` (23 tests passing).
- [x] **9. Frontend Unit Tests:** `frontend/__tests__/app/origin.test.tsx` (3 tests passing).
- [x] **10. Playwright Contract Tests:** `frontend/e2e/pages/origin.spec.ts` (9 tests passing).
