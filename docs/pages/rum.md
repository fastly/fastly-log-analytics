# Page Specification: Real User Monitoring & Core Web Vitals (`/rum`)

> [!TODO] **Page Specification Pending Dedicated AI Verification Session**
> This specification defines the target contract, architecture behaviors, role permissions, and test cases for `/rum`. A dedicated AI session will execute the verification sequence against live running instances, capture telemetry, and certify end-to-end functionality across roles and architectures.

---

## 1. Overview & Objectives

The **Real User Monitoring (RUM)** page captures browser-side telemetry delivered via Fastly beacon streaming. It provides user-centric visibility into Google Core Web Vitals (Largest Contentful Paint [LCP], Interaction to Next Paint [INP], Cumulative Layout Shift [CLS], First Contentful Paint [FCP], Time to First Byte [TTFB]) and browser JavaScript error stacks.

### Key Tenets:
- **Core Web Vitals Compliance:** Evaluates Google 75th percentile thresholds (Good / Needs Improvement / Poor).
- **CDN Correlation:** Correlates RUM beacon sessions with CDN access logs via `rum_cid`.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/rum`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `page_path`, `browser`, `device_type`, `country`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full CWV histograms, client JS error stack traces, unmasked client IPs. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Full CWV dashboards viewable; JS error details viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake `client_vitals` / `client_errors`. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`) |
|---|---|---|
| **Data Plane** | Reads DuckLake tables `lake.client_vitals` and `lake.client_errors`. | Reads Postgres DuckLake or ClickHouse RUM tables. |
| **Ingest Pipeline** | Driven by `rum_sync_{id}` and `rum_commit_{id}`. | Driven by `rum_discovery_{id}` and Celery workers. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/rum/analytics`:
   - Returns 75th percentile ratings for LCP, INP, CLS, FCP, TTFB, browser distributions, and recent JS error clusters.

### Native high-scale analytics contract

`backend/high_scale/rum.py::rum_analytics` must read the service's **visible**
`rum_vitals_facts` and `rum_error_facts` through the instrumented
`service.client.execute`. Aggregate counts/sums cannot supply p75 percentiles.
Query failures propagate to the existing API error handling; they must not
be converted into successful zero-count or empty-chart responses.

- Bounds are inclusive event timestamps, normalized to UTC. Missing bounds
  use the standard producer's defaults (now minus 24 hours / now).
- `trends` contains aligned, ascending ISO UTC `timestamps`, `lcp`, `cls`,
  `error_rate`, `pageviews`, `interactions`, and `errors` arrays. Windows of
  at most 48 hours use hour boundaries; longer windows use day boundaries,
  including both endpoint buckets. Daily measurements aggregate the whole
  selected day's facts, not the last hourly row written into a daily slot.
- Empty buckets have null measurements/rates and zero counts; populated
  errors-only buckets have null LCP/CLS and a 100% error rate. Missing metric
  types remain null, never synthetic zero measurements.
- Beacon identity follows the standard raw producer: request event ID when
  present, otherwise client ID plus event timestamp rounded to seconds.
  Multiple metrics from one beacon count once. Pageviews exclude `event_%`
  metric names; interactions include them. `beacon_count` is the distinct
  union across vitals and errors, not the sum of overlapping categories.
- Bucket error rate is distinct errors / (distinct vitals + distinct errors)
  × 100. Percentiles use continuous/interpolated p75. LCP/FCP/TTFB follow the
  standard wire conversion (values >20 divided by 1000, rounded to 2 decimal
  places), CLS rounds to 3 decimals, and INP is integer milliseconds.
- `no_data` means neither visible fact table has any data for this service.
  An empty selected range with older data still returns the full empty axis
  and zero summary counts. Errors-only services are not treated as no-data.
- Worst pages (maximum 5) and exceptions (maximum 3) use available facts.
  The current high-scale schema has no browser/OS/device or exception
  line/column columns: environment maps stay empty; exception line/column
  use the standard unknown defaults (0), without inventing measurements.

This contract does not change ingestion, schemas, filters, admin migration,
or deployment configuration. Live chart certification is a separate rollout
gate and is recorded below when it passes.

### Current verification status

The native high-scale producer now returns fact-backed trend arrays. Do not
accept axes, trace count, timestamp-array length, or a remote-chart warning as
evidence of populated measurements. Require finite plotted values from the
selected real-data window.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** CWV Scorecard Grid, CWV Distribution Histograms, and JS Error Stack Table enforce container intrinsic sizes (`contain-intrinsic-size: 320px`).
- **In-Place Skeletons:** Cards display contextual loading copy (`Aggregating Core Web Vitals...`, `Analyzing browser JS errors...`).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **RUM Not Configured State:** If RUM beacon logging is not enabled in service config, displays clear guidance on how to enable RUM collection without throwing UI crashes.
- **CWV Rating Breakdown:** Interactive toggles show the percentage of users experiencing Good / Needs Improvement / Poor scores.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Queries `client_vitals` partition; 0 redundant queries | Optimized partition scans; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [ ] **1. Route Accessibility:** `GET /rum` returns HTTP 200.
- [ ] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [ ] **3. CWV Metric Contract:** Verify LCP, INP, and CLS percentiles calculate accurately against Google thresholds.
- [ ] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [ ] **5. Unconfigured State:** Disabling RUM renders the onboarding helper gracefully.
- [ ] **6. Role Verification:** Confirm Analyst Path B observes expected view without administrative buttons.
- [ ] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries.
- [ ] **8. High-scale positive chart gate:** On a service with real visible RUM
  facts in the selected 30-day range, verify both trend charts have finite
  plotted points (not merely traces or a nonempty zero-filled axis), the
  summary/bucket counts reconcile with facts, and ClickHouse query telemetry
  is attributed to the page load. No synthetic traffic or fabricated values
  may be used to claim this production gate.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/rum.spec.ts` (Pending implementation).
- **Synthetic Traffic Profile:** Simulated browser beacons with varied LCP (1.2s to 4.5s) and simulated JS errors.

### Native producer regression gate

- `uv run pytest tests/high_scale/test_rum.py` exercises populated chart arrays,
  aligned empty buckets, absent data versus an empty selected range, UTC/default
  bounds, null measurements, and propagation of each query-stage failure.
- `RUN_RUM_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_rum_integration.py`
  executes the producer's SQL on real ClickHouse through the instrumented
  `ClickHouseClient`. It uses an already-installed
  `clickhouse/clickhouse-server:25.8.4.13` Docker image in disposable
  `clickhouse-local` processes; `RUM_CLICKHOUSE_TEST_IMAGE` can select another
  installed compatible image. No listening ports, deployed services, production
  writes, or new Python dependencies are required.
- The real-engine fixture preserves canonical fact columns/types, replacing
  only the replicated engine with local MergeTree (no Keeper). It covers
  distinct union counts, visible/service fencing, inclusive sub-hour bounds,
  hourly/daily/30-day axes, whole-day continuous p75, errors-only and
  interactions-only buckets, zero versus missing metrics, units, and available
  page/exception fields.
- These regressions validate the native producer, not replication, ingestion
  freshness, HTTP routing/mTLS, or browser rendering. The live positive chart
  gate above must still pass after the operator's approved rollout.

**Local validation (2026-10-05):** The high-scale suite plus standard RUM
analytics tests passed with the real-engine gate enabled (284 tests). Targeted
Ruff checks/format, mypy, import contracts, and the scan-performance gate passed.
The full backend, frontend, contract, and E2E gates passed before rollout.

**Live certification (2026-10-05, commit `197b57d04569`):** The canonical
rollout report `reports/deploys/2026-10-05-14-51-38/` verified Local Standard,
Remote Standard, and Remote High-Scale. Each environment produced finite
30-day Plotly measurements, active 24-hour and 15-minute RUM data, and
reconciled the page distinct-beacon total with the header total within the
verifier's documented tolerance. Native administrator mTLS and anonymous public
analyst/SSR separation also passed. The verifier's finite-measurement predicate
is the acceptance criterion; timestamp/null-only arrays do not qualify.
