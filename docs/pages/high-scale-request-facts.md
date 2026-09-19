# Page Specification: High-Scale Request Facts (`/high-scale/request-facts`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/high-scale/request-facts`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **High-Scale Request Facts** page provides specialized deep-dive analytical querying, column inspection, and partition telemetry into high-scale request fact MergeTree tables in ClickHouse (`request_facts`, `high_scale_batch_publications`, `cmcd_projection_facts`, and minute-level dimensions). It is designed for multi-million requests/sec production environments requiring sub-second analytical aggregations across billions of rows.

### Key Tenets:
- **Massive Scale Diagnostics:** Inspects raw request facts across ClickHouse cluster partitions (`request_facts` keyed by `service_id`, `batch_id`, `timestamp`).
- **Partition & Column Pruning:** Displays partition boundaries, primary sort keys (`(service_id, toStartOfHour(timestamp), domain)`), compression ratios, and ClickHouse scan byte stats (`app_clickhouse_bytes_read`).
- **Shared Primitives:** Conforms strictly to global navigation, layout primitives, and theme tokens.

---

## 2. Routes & URL Schema

- **Primary Route:** `/high-scale/request-facts`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-1h`, `now-24h`).
  - Standard drill-down filters: `status`, `pop`, `domain`, `country`, `batch_id`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full distributed query execution, shard metrics, cluster topology. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Query facts viewable; client IPs masked (if enabled by invite setting). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local high-scale engine replica. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Engine Availability** | Emulates facts view via DuckDB partitioned table scan. | Direct execution against ClickHouse MergeTree tables (`request_facts`, `high_scale_batch_publications`). |
| **Scalability** | Up to tens of millions of rows. | Hundreds of millions to billions of rows at sustained 2M-5M RPS. |
| **Data Plane** | Local Parquet buffer + DuckLake table. | Distributed Celery workers batch-inserting into ClickHouse via `ClickHouseClient.insert_rows()`, indexed by `PgManifest`. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/high-scale/request-facts`:
   - Parameters: `service_id`, `from`, `to`, `filters`, `limit`.
   - Returns: Partition summary, facts rows, query execution duration, scanned bytes, ClickHouse query ID.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements registered via `query_registry.register("ClickHouse", ...)` and recorded in `telemetry_queries`.
- Metrics exported to Prometheus via `app_clickhouse_query_duration_ms` and `app_clickhouse_bytes_read_bytes_total`.


---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Shard Status Bar, Partition Metrics Card, and Fact Records Grid enforce container intrinsic sizes (`contain-intrinsic-size: 500px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Querying distributed cluster shards...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Standard Mode Fallback:** If accessed in Standard Mode, displays an informational badge indicating emulation mode.
- **Zero Records State:** Renders clean empty state when no facts match partition filters.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Query)** | < 400 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `telemetry_queries` audit |
| **Query Efficiency** | Partition-pruned select; 0 redundant queries | Distributed select; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /high-scale/request-facts` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Facts Contract:** Verify request facts return valid partitioned rows.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Role Verification:** Confirm Analyst Path B observes masked client IPs.
- [ ] **6. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/high-scale-request-facts.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** High-throughput batch records partitioned across timestamps.
