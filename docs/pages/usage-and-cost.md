# Page Specification: Usage & Cost Estimator (`/usage`)

> [!NOTE] **Live-certified on `release/v3.0.0-beta3`**
> Audited, hardened, and verified across Local Standard, Remote Standard GCE, and High-Scale Elevation topologies. Integrates Fastly Delivery Billing Zones (`MBZ100`, `MBZ200`, `Global`), zero-CLS layout reservations, full typed contracts, and strict H-1 tenancy/admin role protections.

---

## 1. Overview & Objectives

The **Usage & Cost** page provides complete operational visibility into Fastly Object Storage (FOS) usage, raw log volume, Parquet storage footprint, Class A (writes/LIST) and Class B (reads/GET) API operations, billing reconciliation against Fastly `/stats/service`, and an interactive cost simulator.

### Key Tenets:
- **Financial Transparency:** Tracks every penny of FOS storage and API operations driven by log streaming and background jobs.
- **Fastly Delivery Billing Zones:** Models real-world Fastly CDN bandwidth egress pricing across zones:
  - `MBZ100` ($0.08/GB): North America, Hawaii, and European POPs (lowest egress charges).
  - `MBZ200` ($0.12/GB): All Fastly POPs except Africa, India, and South Korea. Default baseline zone.
  - `Global` ($0.18/GB): Global blended egress encompassing all Fastly POPs worldwide.
- **Media Shield / Shield-to-Origin Modeling:** Accounts for origin shielding offload and edge delivery savings.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/usage`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-30d`, `now-90d`).

---

## 3. Role & Permission Matrix

Per H-1 security hardening, `/usage` and `/api/usage/*` expose financial cost rates, billing data, and sensitive Fastly API token interactions. Consequently, remote analyst sessions are restricted:

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full usage metrics, Fastly billing zone selection, cost rates configuration, manual billing reconciliation trigger, FOS ops inspection. |
| **Analyst (Remote Share)** | Blocked (403 Forbidden) | Blocked at middleware via `_ANALYST_BLOCKED_PREFIXES`. Financial and billing data protected. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Usage Ledger** | PostgreSQL 16 (`usage_log` & `usage_log_hourly_summary` via `METADATA_DSN`). | PostgreSQL 16 distributed usage ledger (`usage_log` & `usage_log_hourly_summary`). |
| **Reconciliation Data** | Fastly `/stats/service` aggregated via `reconcile_fastly_stats`. | Fastly API stats matched with PostgreSQL metadata ledger. |
| **Prefill & Billing Rates** | Instant read via `/api/usage/prefill/rates` and historical RUM/log calculation in `/api/usage/prefill`. | Instant PostgreSQL read via `/api/usage/prefill/rates`. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints:
1. `GET /api/usage/prefill/rates`:
   - Instant prefill rates for stat cards without DuckDB hops (`class_a_rate_per_1k`, `class_b_rate_per_10k`, `cdn_egress_rate_per_gb`, `storage_rate_per_gb_month`, `billing_zone`).
2. `GET /api/usage/prefill`:
   - Comprehensive cost estimator prefill: historical log file sizes, data days, empirical flush nodes, and average daily RUM beacons (`client_vitals`, `client_errors`).
3. `GET /api/usage/current-storage`:
   - Live FOS bucket object count, total bytes stored, and cumulative 30-day minimum billing GB-hours.
4. `GET /api/usage/operations`:
   - Fastly `/stats` Class A and Class B billing operations time series with automatic granularity clamping (`hour`, `day`).
5. `GET /api/usage/log-activity`:
   - Breakdown of ingested vs unbackfilled edge log files across time slices.
6. `GET / PATCH /api/admin/usage-logging`:
   - Admin settings management for default rates and active `billing_zone`.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` with query attribution.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Stat cards grid (`min-h-[140px] [contain-intrinsic-size:140px] [content-visibility:auto]`), charts grid (`min-h-[740px] [contain-intrinsic-size:740px]`), and Cost Estimator container (`min-h-[420px] [contain-intrinsic-size:420px]`).
- **In-Place Skeletons:** Cards display contextual loading placeholders while preserving layout dimensions.
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Fastly Billing Zone Selector:** 3-way toggle between `MBZ100` ($0.08/GB), `MBZ200` ($0.12/GB), and `Global` ($0.18/GB) updates delivery rate and projected costs in real time.
- **Cost Simulator:** Interactive inputs for FOS storage $/GB/mo, Class A $/10k ops, Class B $/10k ops, and Media Shield origin offload dynamically recalculate projected monthly cost.
- **Missing Token Fallback:** Displays actionable configuration banner if `fastly_api_key` is not configured for Fastly stats operations.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (measured 0.00) | <= 0.05 (measured 0.00) | PerformanceObserver / Playwright |
| **API Latency (Summary)** | < 250 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Queries `usage_log_hourly_summary`; 0 redundant queries | Optimized ledger queries; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /usage` returns HTTP 200 with footer environment indicator.
- [x] **2. Zero Layout Shift:** Pre-allocated layout reservations enforce CLS <= 0.05.
- [x] **3. Usage Contract:** Verify `/api/usage/prefill/rates`, `/api/usage/prefill`, and `/api/usage/current-storage` schema contracts.
- [x] **4. Fastly Delivery Billing Zones:** `MBZ100`, `MBZ200`, `Global` rates correctly configure calculator state and compute costs.
- [x] **5. Cost Calculator:** Interactive sliders and inputs update projected costs instantaneously with zero network hops.
- [x] **6. Role Verification:** H-1 security boundary blocks Analyst from accessing billing/usage endpoints.
- [x] **7. Telemetry & Query Audit:** All PostgreSQL and DuckDB statements recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/usage.spec.ts` (Comprehensive 7-step suite).
- **Backend Router Tests:** `tests/routers/test_usage_router.py` (39 tests covering all endpoints, rates, RUM beacon averages, and billing zones).
- **Frontend Unit Tests:** `frontend/__tests__/components/CostCalculator/calc.test.ts` (5 tests verifying billing zone calculations and prefill mappings) and `frontend/__tests__/app/usage.test.tsx`.
