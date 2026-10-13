# Page Specification: Network Path & ASN Heatmap (`/network`)

> [!NOTE] **Live-certified on `release/v3.0.0-beta3`**
> This specification defines the verified contract, architecture behaviors, role permissions, and test suites for `/network`. Validated across Local Standard, Remote Standard GCE, and High-Scale Elevation deployments with full query attribution and zero layout shifts.

---

## 1. Overview & Objectives

The **Network Path** page provides deep visibility into client network conditions, TCP round-trip times (RTT), TCP retransmissions, packet loss, throughput speeds (Mbps), client connection types (IPv4 vs IPv6, HTTP/1.1 vs HTTP/2 vs HTTP/3), and an interactive ASN Health Heatmap.

### Key Tenets:
- **Transport Layer Telemetry:** Surfaces TCP connect RTT and packet retransmits recorded at the Fastly edge (`tcp_rtt`, `retrans`, `ploss`, `delivery_rate`).
- **Autonomous System (ASN) Intelligence:** Ranks and classifies top ISPs/ASNs by delivery quality and latency degradation.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.
- **Graceful Field Degradation:** If optional transport fields are not enabled in the active VCL schema, renders an informative alert banner guiding the operator to `/service-config` while continuing to display available ASN/network metrics.

---

## 2. Routes & URL Schema

- **Primary Route:** `/network`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `asn`, `country`, `pop`, `client_protocol`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full TCP metrics, ASN labels, unmasked client IP network paths. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full ASN health heatmap and TCP charts viewable; client IPs masked (if enabled). Allowed under `_ANALYST_ALLOWED_WRITE_PREFIXES` for `/api/network/bundle`. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `network_rtt` / `network_speed` rollups. | Reads Postgres DuckLake / ClickHouse network tables with fallback support in `backend/high_scale/network.py`. |
| **ASN Enrichment** | Local PostgreSQL `asn_names` table (ADR-22) enriched via `enrich_asn_labels`. | Shared Postgres / ClickHouse ASN dictionaries. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints:
1. `POST /api/network/bundle`:
   - Composite endpoint returning TCP transport metrics (`tcp_rtt_p50_ms`, `tcp_rtt_p90_ms`, `tcp_rtt_p95_ms`, `tcp_rtt_p99_ms`, `retrans_rate_pct`, `packet_loss_pct`, `delivery_rate_mbps`), protocol distribution, and list of `missing_fields`.
   - Instrumented with `process_context_scope("network:bundle")` and protected by `execute_with_stale_view_retry`.
2. `POST /api/network-health`:
   - Returns ASN health summary, leaderboard, metro leaderboard, and heatmap matrix.
   - Gated by section parameter (`summary`, `leaderboard`, `metro_leaderboard`, `map`).
3. `POST /api/network-quality`:
   - Returns RTT vs TTFB correlation scatter plot and latency breakdown rows by Country, ASN, Region, and POP.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Network KPI Cards, Edge Transport Cards, Protocol Distribution Donut, and ASN Health Heatmap enforce container intrinsic sizes (`[contain-intrinsic-size:...]` and `[content-visibility:auto]`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Analyzing TCP round-trip times...`, `Compiling ASN health matrix...`, `Scanning ASN degradation...`, `Evaluating regional latency...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS <= 0.05).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **ASN Heatmap Drill-Down:** Clicking an ASN block or table row filters all network graphs to that autonomous system.
- **Zero Retransmits State:** When packet loss is zero, indicators show optimal green network health.
- **Missing Fields Guidance:** If `tcp_rtt`, `retrans`, or `ploss` are absent in log schema, a clear banner is displayed with a link to `/service-config`.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 | <= 0.05 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `network_rtt` rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /network` returns HTTP 200 and renders environment indicator.
- [x] **2. Zero Layout Shift:** Page loads with pre-allocated zero-CLS containers (`contain-intrinsic-size`) and CLS <= 0.05.
- [x] **3. Network Bundle Contract:** `POST /api/network/bundle` returns valid typed Pydantic schema with transport metrics and protocol breakdown.
- [x] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [x] **5. ASN Filtering:** Clicking an ASN row applies the filter chip and re-evaluates TCP metrics.
- [x] **6. Role Verification:** Analyst Path B allowed to access `/api/network/bundle` via `_ANALYST_ALLOWED_WRITE_PREFIXES`.
- [x] **7. Telemetry & Query Audit:** Telemetry queries captured with `process_context="network:bundle"`.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/network.spec.ts`.
- **Vitest Unit Test:** `frontend/__tests__/app/network.test.tsx`.
- **Backend Router Test:** `tests/routers/test_network_router.py`.
