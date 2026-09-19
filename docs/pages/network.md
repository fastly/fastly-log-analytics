# Page Specification: Network Path & ASN Heatmap (`/network`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/network`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Network Path** page provides deep visibility into client network conditions, TCP round-trip times (RTT), TCP retransmissions, packet loss, throughput speeds (Mbps), client connection types (IPv4 vs IPv6, HTTP/1.1 vs HTTP/2 vs HTTP/3), and an interactive ASN Health Heatmap.

### Key Tenets:
- **Transport Layer Telemetry:** Surfaces TCP connect RTT and packet retransmits recorded at the Fastly edge.
- **Autonomous System (ASN) Intelligence:** Ranks and classifies top ISPs/ASNs by delivery quality and latency degradation.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

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
| **Analyst Path B (Remote Share)** | Read-Only Access | Full ASN health heatmap and TCP charts viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `network_rtt` / `network_speed` rollups. | Reads Postgres DuckLake / ClickHouse network tables. |
| **ASN Enrichment** | Local SQLite `asn_names` table populated by `asn_enrichment`. | Shared Postgres / ClickHouse ASN dictionaries. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/network/bundle` (or composite bundle):
   - Returns TCP RTT percentiles, Retransmit Rate time series, Protocol breakdown, and ASN Health Heatmap matrix.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Network KPI Cards, TCP RTT Percentiles Chart, Protocol Distribution Donut, and ASN Health Heatmap enforce container intrinsic sizes (`contain-intrinsic-size: 350px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Analyzing TCP round-trip times...`, `Compiling ASN health matrix...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **ASN Heatmap Drill-Down:** Clicking an ASN block in the heatmap filters all network graphs to that autonomous system.
- **Zero Retransmits State:** When packet loss is zero, indicators show optimal green network health.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `network_rtt` rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /network` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Network Bundle Contract:** Verify TCP RTT percentiles and ASN heatmap render accurately.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. ASN Filtering:** Clicking an ASN row applies the filter chip and re-evaluates TCP metrics.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/network.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Mixed mobile/cellular traffic with elevated RTT and varied ASN distributions.
