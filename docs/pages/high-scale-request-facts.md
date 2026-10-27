# Page Specification: High-Scale Request Facts (`/high-scale/request-facts`)

> [!IMPORTANT] **Page Specification Status: Live-Certified (2026-10-16)**
> Audited, hardened, and live-certified across Local Standard, Remote Standard GCE, and Remote High-Scale Elevation environments across Admin and Analyst roles.

---

## 1. Overview & Objectives

The **High-Scale Request Facts** page provides specialized deep-dive analytical querying, filter drill-downs, and partition watermark telemetry into high-scale request fact MergeTree tables in ClickHouse (`request_facts`, `high_scale_batch_publications FINAL`, `cmcd_projection_facts`, and minute-level dimensions), with transparent DuckDB Standard-mode emulation fallback when running under `DEPLOYMENT_MODE=standard`. It is designed for multi-million requests/sec production environments requiring sub-second analytical inspection across billions of rows.

### Key Tenets:
- **Massive Scale Diagnostics:** Inspects raw request facts across ClickHouse cluster partitions (`request_facts` joined against `high_scale_batch_publications FINAL` keyed by `service_id`, `batch_id`, `timestamp`, `event_id`) with HMAC-signed opaque keyset cursor pagination.
- **Standard Mode Emulation Parity:** When accessed on a Standard-mode deployment (`DEPLOYMENT_MODE=standard` or `emulate_standard=true`), seamlessly emulates the request facts response against the DuckDB/DuckLake `logs` view with `mode: "duckdb_emulation"` and displays the `DuckDB Standard Mode Emulation` badge.
- **Shared Primitives & Zero CLS:** Conforms strictly to global navigation, zero-CLS layout reservations (`[contain-intrinsic-size:...]` and `[content-visibility:auto]`), and theme tokens.

---

## 2. Routes & URL Schema

- **Primary Route:** `/high-scale/request-facts`
- **Supported Query / Filter Controls:**
  - `service`: Fastly Service ID.
  - `start_time`, `end_time` (`from`, `to`): ISO timestamps or preset tokens (`now-1h`, `now-24h`).
  - Standard drill-down filters: `status` (HTTP status code), `domain` (`host`), `pop`, `country` (`geo_country_code`), `batch_id`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full distributed query execution, watermark/freshness metrics, cluster mode indicator, unmasked client IPs. |
| **Analyst (Remote Share)** | Read-Only Access | Enforced via `RequestContext` (`require_service_access`) and clamped to session `TimeBounds`; client IPs masked (`mask_ips_for` → `x.x.x.xxx` / `xxxx:...`) when privacy masking is active. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Engine Availability** | Emulates facts view via DuckDB partitioned `logs` view scan (`mode: "duckdb_emulation"`) with `execute_with_stale_view_retry` self-healing. | Direct execution against ClickHouse MergeTree tables (`request_facts`, `high_scale_batch_publications FINAL`, `mode: "clickhouse"`). |
| **Scalability** | Up to tens of millions of rows. | Hundreds of millions to billions of rows at sustained 2M–5M RPS. |
| **Data Plane** | Local Parquet buffer + DuckLake table. | Distributed High-Scale workers batch-inserting into ClickHouse and FOS archive Parquet, indexed by PostgreSQL `high_scale_manifests` and watermarks. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `POST /api/high-scale/services/{service_id}/request-facts`:
   - Request model: `HighScaleRequestFactRequest` (`start_time`, `end_time`, `limit`, `cursor`, `status`, `domain`, `pop`, `country`, `batch_id`, `emulate_standard`).
   - Response model: `HighScaleFactQueryResponse` (`rows`, `metadata` with `status`, `exact`, `coverage`, `freshness_lag_seconds`, `mode`, `watermark`, `next_cursor`, `_debug_queries`, `_is_cached`).
2. `GET /api/high-scale/request-facts` & `POST /api/high-scale/request-facts`:
   - Query-param (`GET`) and JSON-body (`POST`) aliases using `RequestContext` active service resolution and automatic Standard-mode DuckDB emulation fallback when ClickHouse is not configured for the service.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID` and executes inside `process_context_scope("high_scale:request_facts")`.
- ClickHouse statements are recorded via `_record_clickhouse_telemetry` into `_QUERIES` (`_debug_queries`) and `usage_log.log_telemetry_query`; DuckDB emulation queries are tracked via `track_query`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Header wrapper (`min-h-[60px] [contain-intrinsic-size:auto_60px]`), Metadata Status Bar (`[contain-intrinsic-size:auto_88px]`), Filter Bar (`[contain-intrinsic-size:auto_48px]`), and Request Facts Card (`min-h-[600px] [contain-intrinsic-size:auto_600px]`) enforce container intrinsic sizes and `[content-visibility:auto]`.
- **In-Place Skeletons:** Route-level `loading.tsx` (`TablePageSkeleton`) and in-place card loading skeleton display contextual copy (`Loading request facts…` / `Querying distributed cluster shards...`).
- **Dimmed Updates:** Background filter and pagination refetches dim the table container (`opacity-40 pointer-events-none transition-opacity`) with zero layout shift (`CLS <= 0.05`).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to the active `useTimeRange` window (24h for mature services, or adaptive data extent).
- **Standard Mode Fallback:** Displays the `DuckDB Standard Mode Emulation` badge when `metadata.mode === "duckdb_emulation"`, or `ClickHouse Distributed Cluster` when `metadata.mode === "clickhouse"`.
- **Interactive Filters:** Supports live filtering by `status`, `domain` (`host`), `pop`, `country`, and `batch_id` with a one-click `Clear filters` reset.
- **Zero Records State:** Renders `"No request facts in this time range."` inside the table body when no facts match the active time window or filter predicates.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / LCP** | < 400 ms / < 1500 ms | < 400 ms / < 1500 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (0.000 measured) | <= 0.05 (0.000 measured) | PerformanceObserver |
| **API Latency (Query)** | < 400 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_queries` / `telemetry_queries` audit |
| **Query Efficiency** | Partition-pruned select; 0 redundant queries | Distributed select; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /high-scale/request-facts` returns HTTP 200 and renders `ServerFooter` with environment indicator.
- [x] **2. Zero Layout Shift:** Page loads with pre-allocated `[contain-intrinsic-size:...]` skeletons and `CLS <= 0.05`.
- [x] **3. Facts Contract:** `GET /api/high-scale/request-facts` and `POST /api/high-scale/services/{service_id}/request-facts` return valid rows, watermark metadata, and execution `mode`.
- [x] **4. Interactive Filters & Time Range:** Filter inputs (`status`, `domain`, `pop`, `country`, `batch_id`) and clear button update queries cleanly without console errors or HTTP 500s.
- [x] **5. Role Verification:** Analyst is permitted in route tripwire allowlists and observes masked client IPs (`x.x.x.xxx`).
- [x] **6. Telemetry & Query Audit:** All ClickHouse and DuckDB queries are recorded in `_debug_queries` and `telemetry_queries` under `X-Page-Load-ID`.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend Unit Tests:** `tests/routers/test_high_scale.py` and `tests/remote_access/test_route_table_tripwire.py`.
- **Frontend Unit Tests:** `frontend/__tests__/app/high-scale-request-facts.test.tsx`, `frontend/__tests__/high-scale-request-facts.test.tsx`, and `frontend/__tests__/hooks/useHighScaleRequestFacts.test.tsx`.
- **Playwright E2E Test:** `frontend/e2e/pages/high-scale-request-facts.spec.ts` (7 contract tests).
