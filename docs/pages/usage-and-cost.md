# Page Specification: Usage & Cost Estimator (`/usage`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/usage`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Usage & Cost** page provides complete operational visibility into Fastly Object Storage (FOS) usage, raw log volume, Parquet storage footprint, Class A (writes/LIST) and Class B (reads/GET) API operations, billing reconciliation against Fastly `/stats/service`, and an interactive cost simulator.

### Key Tenets:
- **Financial Transparency:** Tracks every penny of FOS storage and API operations driven by log streaming and background jobs.
- **Fastly Billing Reconciliation:** Reconciles edge log volume recorded by Fastly against ingested row counts to verify 100% capture.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/usage`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-30d`, `now-90d`).

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full usage metrics, cost rates configuration, manual billing reconciliation trigger. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full usage and cost charts viewable; cost configuration rates read-only. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local `usage_log.db`. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Usage Ledger** | Local SQLite `usage_log.db` (`usage_log` & `usage_log_hourly_summary`). | Postgres / SQLite distributed usage ledger. |
| **Reconciliation Data** | Fastly `/stats/service` aggregated via `reconcile_fastly_stats`. | Fastly API stats matched with Postgres ledger. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/usage/summary` (or composite bundle):
   - Returns total raw logs received, Parquet bytes stored, Class A ops, Class B ops, estimated monthly cost, and billing reconciliation match percentage.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Cost KPI Cards, Storage & Operations Time Series, Operations Breakdown Donut, and Cost Calculator enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Panels display contextual loading copy (`Calculating FOS storage costs...`, `Reconciling billing stats...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 30d for mature services, or max available data extent if history < 30d.
- **Cost Simulator:** Sliders for FOS $/GB/month, Class A $/10k ops, and Class B $/10k ops dynamically recalculate total projected bill.
- **Zero Log Loss Status:** Displays green "100.0% Reconciled" badge when edge stats match ingested log volume within 0.1%.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Summary)** | < 250 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Queries `usage_log_hourly_summary`; 0 redundant queries | Optimized ledger queries; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /usage` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. Usage Contract:** Verify Class A/B operations and storage bytes match recorded ledger entries.
- [ ] **4. Time Range Extent:** Confirms 30d default or adaptive extent for services < 30d history.
- [ ] **5. Cost Calculator:** Adjusting rate sliders updates projected cost immediately without network calls.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/usage-and-cost.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Ingestion ledger records spanning multiple days with simulated Class A/B operation counts.
