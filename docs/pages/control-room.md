# Page Specification: Control Room (`/control-room`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/control-room`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Control Room** is the real-time operations console for Fastly Log Analytics. It provides operators with high-frequency live health vitals, active request throughput, error rate telemetry, and a live-updating stream of recent HTTP 5xx/4xx incidents.

### Key Tenets:
- **Real-Time Responsiveness:** Employs low-latency polling / Server-Sent Events (SSE) to reflect live CDN edge state with minimal lag.
- **Incident Triaging:** Surfaces immediate error spikes and anomaly banners allowing operators to drill down into specific client IPs, URLs, or backends.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/control-room`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID (defaults to active service from `useServiceStore`).
  - `from`, `to`: ISO timestamps or preset tokens (e.g. `now-1h`, `now`).
  - Standard drill-down filters: `status`, `country`, `pop`, `origin`, `asn`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full real-time stream, unmasked client IPs, full backend diagnostics, manual sync triggers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Live throughput and error stream accessible; client IPs masked (if enabled by invite setting); administrative actions blocked. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads local DuckDB `logs` view and local Parquet buffers. | Queries Postgres DuckLake / ClickHouse read-only engine. |
| **Telemetry Stream** | In-process SSE stream from recent ingest buffer. | Redis pub/sub or Celery streaming bridge. |
| **Throughput Gauge** | Calculated from rolling 1-minute buffer counts. | Calculated from distributed ledger metrics. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/control-room/summary` (or composite bundle):
   - Returns live RPS, 5xx rate, 4xx rate, p95 latency, and active error count.
2. `GET /api/control-room/stream` (SSE or polling):
   - Emits recent error events (timestamp, status, path, pop, client_ip).

### Telemetry Attribution:
- All queries must carry `X-Page-Load-ID` and record DuckDB / SQLite profiler rows in `telemetry_queries` and `usage_log.db`.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Container boxes for the live throughput chart, error gauge, and live error log table must enforce `contain-intrinsic-size` to ensure zero layout shift (CLS = 0.00).
- **In-Place Skeletons:** Each card renders contextual skeletons (`Monitoring edge throughput...`, `Listening for errors...`).
- **Dimmed Updates:** Background SSE updates or filter changes dim active cards (`opacity-40 pointer-events-none`) without destroying the DOM tree.

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to rolling 24h for mature services, or max available data extent if history < 24h.
- **Incident Click-to-Filter:** Clicking an error row immediately injects the corresponding filter chip into `ReportLayout`.
- **Zero-Traffic Edge Case:** If the CDN has zero requests, gauges render 0 RPS cleanly with "No active traffic" placeholder without throwing errors.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Summary)** | < 250 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Zero redundant queries | Zero redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /control-room` returns HTTP 200 for Admin and Analyst.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Data Stream:** Verify summary API returns valid live throughput and error statistics.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Click-to-Filter:** Clicking an error item adds the corresponding filter chip to `FilterBar`.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes masked IPs and cannot access admin triggers.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries and execution time within budget.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/control-room.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** High-frequency burst traffic with 2% 5xx error rate and 5% 4xx error rate.
