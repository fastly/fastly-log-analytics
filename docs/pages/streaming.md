# Page Specification: Streaming — CMCD Quality Analytics (`/streaming`)

> [!NOTE] **Status: spec written 2026-10-21, not yet re-certified**
> `/streaming` is the CMCD streaming-analytics page. For a short stretch of `v3.0.0-beta4` it was replaced by a live log tail; that tail is gone and this page is back. The earlier certification covered the tail, not this page, so the checklist in section 9 starts from what has actually been verified and leaves the rest open.

---

## 1. Overview & Objectives

The **Streaming** page turns Common Media Client Data (CMCD v1/v2) that players attach to their requests into video Quality-of-Experience analytics for a service. It answers the questions a streaming operator asks first: how many viewers are watching, are they rebuffering, what bitrate are they getting, and which content, countries and networks are hurting.

### Key Tenets:
- **Aggregate view across all viewers.** Per-session drill-down lives on `/sessions/stream`; this page is the fleet-level companion.
- **CMCD-driven.** Every metric comes from the `cmcd_*` log fields the edge VCL extracts (`cmcd_sid`, `cmcd_cid`, `cmcd_br`, `cmcd_bl`, `cmcd_bs`, `cmcd_mtp`, `cmcd_ot`, `cmcd_sf`, `cmcd_su`, `cmcd_tb`, and friends). No CMCD, no data, and the page says so plainly.
- **Same window and filter model as the other analytics pages.** Global time range and FilterBar filters apply; chart zoom writes back to the range.
- **Standard and High-Scale parity.** Same request and response contract on both architectures.

---

## 2. Routes & URL Schema

- **Primary Route:** `/streaming`
- **Navigation:** the sidebar entry "Streaming" is shown only when the active service's schema contains the `cmcd_sid` column (`requiresCmcd` in `AppLayout`).
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `range` and the FilterBar parameters, handled by `ReportLayout` like every other analytics page.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | All sections. |
| **Analyst Path B (Remote Share)** | Read-Only Access | All sections. `/api/cmcd/` is in the analyst-allowed POST prefixes (`_ANALYST_ALLOWED_WRITE_PREFIXES` in `backend/utils/remote_access.py`) because the aggregates call is a read-only query sent as POST. No section returns a client IP, so the IP-masking flag (`mask_ips_for`) is accepted for contract parity and has nothing to mask. |
| **Analyst Path A (JSON Join)** | Local Read-Only | Reads the local DuckLake view. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | DuckDB request view columns `cmcd_*` via `backend/repositories/cmcd.py`. The page is "available" only if the view has `cmcd_sid`. | ClickHouse `cmcd_projection_facts`, queried by `backend/high_scale/cmcd.py`. |
| **Availability** | `available: false` when `cmcd_sid` is missing from the schema. | Same response shape. |
| **Aggregation** | One query runner computes every requested section for the window. | Same sections, ClickHouse quantiles (`quantileTDigestIf`). |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `POST /api/cmcd/aggregates`
   - Body (`CmcdRequest`): `filters`, `bucket_seconds` (page sends `300`), `top_n` (page sends `30`), and the range fields from `resolveRangeWire` (relative token plus anchor, or absolute start/end).
   - Optional `sections` limits the work. Valid names: `overview`, `sessions_ts`, `buffer_health_ts`, `bitrate_ts`, `throughput_ts`, `top_content`, `rebuffer_by_country`, `rebuffer_by_asn`, `object_type_dist`, `streaming_format_dist`, `startup_ts`, `session_duration_dist`. The page requests all of them.
   - Returns `CmcdAggregatesResponse` with `available`, each section, and the standard telemetry block.

### Telemetry Attribution:
- The page uses `useServiceQuery`, so every request carries the service context and `X-Page-Load-ID`.
- The response includes the query telemetry produced by the section runner (`with_telemetry`).
- Refetches every 30 seconds and cancels in-flight `cmcd` queries on unmount.

---

## 6. UI Components, Skeletons & Layout Reservation

- **Loading shell:** `app/streaming/loading.tsx` renders `DashboardSkeleton` with the page title and description.
- **Layout:** `ReportLayout` plus `AnalyticsCard` (per-card loading and error states). Layout-shift numbers for this page have not been re-measured since the reversion.
- **Seven headline cards:** Total Sessions, Peak Viewers, Avg Duration, Rebuffer Rate, Avg Bitrate, Avg Buffer, Median Throughput.
- **Charts and tables:** Active Viewers, Session Starts, Buffer Health (p50/p95 buffer plus starvation rate), Bitrate & Quality, Measured Throughput, Startup Requests, Top Content, Session Duration, Object Type Distribution, Streaming Formats, Rebuffer Rate by Country, Rebuffer Rate by ASN.
- **Info popovers:** each card has help text from `streamingInfo.tsx`.

---

## 7. Interactive Workflows & Edge Cases

- **CMCD not enabled:** when the response says `available: false`, the page shows "CMCD data not available" with a pointer to Admin settings. The sidebar link is hidden for the same reason.
- **Per-card errors:** a failed query puts the error in each card (`AnalyticsCard`) instead of blanking the page.
- **Chart zoom:** zooming a time-series chart updates the global range; double-click (autorange) is ignored on purpose.
- **Booleans and CMCD versions:** v1 encodes true as a bare key (`su`, `bs`), v2 as `su=?1`. Which one the edge parses depends on the service's CMCD version setting; the other form leaves those flags false.
- **Transport mode:** the edge reads either the four `CMCD-*` headers or `?CMCD=...`, per the service's CMCD mode.

---

## 8. Performance, Cost & Telemetry Budgets

Measured by the deploy verifier (`scripts/verify_dashboard.js`, `[PERF Streaming]` lines) on commit `f9fc9ebe`, first load of `/streaming?range=24h` under continuous load-test traffic, from the operator machine (high-scale goes through the admin gateway). Local numbers swing with host load; the remote ones are steadier.

| Metric | Target | Local Standard | Remote Standard | Remote High-Scale |
|---|---|---|---|---|
| **FCP / LCP** | LCP < 1500 ms | 1,104 ms | 824 ms | 892 ms |
| **Cumulative Layout Shift (CLS)** | <= 0.05 (aim 0.00) | 0.013 | 0.013 | 0.013 |
| **`/api/cmcd/aggregates` total** | < 1000 ms (proposed) | 14,090 ms | 3,302 ms | 2,776 ms |
| of which: waiting for a DuckDB connection (`connection_acquire`) | - | 7,925 ms | 2,035 ms | n/a |
| of which: query sections (`_section_timings`, excl. connection) | - | ~4,500 ms | ~1,030 ms | not reported |
| **`/api/cmcd/aggregates` payload** | - | 10 KB | 11 KB | 10 KB |
| **Duplicate data requests** | 0 | 0 | 0 | 0 |

What the breakdown says:
- The page itself is fast and stable: FCP under 1.2 s everywhere and CLS 0.013 on all three.
- **Most of the endpoint's time is waiting for a connection**, not querying. `connection_acquire` is about 60% of the total on both Standard environments. The 12 sections together cost roughly 1 s on Remote Standard.
- **One section was wasting time and is fixed.** `rebuffer_by_country` used `APPROX_QUANTILE` per (country, session) group; with many countries it took up to 4.4 s locally. It now uses `MEDIAN` (64 ms locally and 25 ms on Remote Standard in the same environments after the change; about 9x faster on a 3M-row benchmark).
- High-Scale reports no section timings (the ClickHouse reader doesn't emit them), so its 2.8 s is not yet broken down.

**Pool bulkhead (`f47cec84`, deployed 2026-10-22).** Aggregate routes now take a heavy slot (default `max_size - 1` of the pool) before checking out a connection, so cheap calls such as `/api/cron-runs` always have a connection. In that deploy the cheap calls took 55 to 413 ms while Security aggregates ran 13 s. The Streaming aggregates call itself is a heavy route, so it still queues behind other heavy queries: 1.9 s on High-Scale, 5.5 s on Local Standard and 11.3 s on Remote Standard (it ran right after the Security and Network pages' heavy queries). The bulkhead protects light requests; it doesn't make heavy ones faster.

---

## 9. Verification Checklist

- [x] **1. Real data on all three environments:** `scripts/verify_dashboard.js` loads `/streaming?range=24h` and requires the Total Sessions and Peak Viewers cards, a populated Plotly chart, and no empty-state panels. Passed on Local Standard, Remote Standard and Remote High-Scale for commit `f70b048e` (2026-10-21).
- [x] **2. CMCD traffic exists:** `scripts/dev/scale_harness.py --cmcd-ratio` sends CMCD playback traffic (headers and `?CMCD=`, v1/v2, 300-session pool, rebuffer and startup flags). The deploy script seeds with `--cmcd-ratio 0.35`.
- [x] **3. Layout stability:** CLS 0.013 on all three environments (budget 0.05); the e2e test also holds CLS <= 0.05 on a warm reload.
- [x] **4. Accessibility:** `/streaming` is in `e2e/a11y-routes.spec.ts` and the populated page is axe-checked in `e2e/pages/streaming.spec.ts`; both pass on chromium, firefox and webkit.
- [ ] **5. Role verification:** confirm Analyst Path B can load the page and that no section exposes a client IP.
- [x] **6. Duplicate-request audit:** one `/api/cmcd/aggregates` call per load on every environment. Latency is recorded in section 8 and is above a sensible target (open: connection wait).
- [x] **7. Not-enabled state:** covered by a unit test and an e2e test with `available: false`; the nav link hides when the schema has no `cmcd_sid`.

---

## 10. Automated Test Suite & Traffic Generation

- **Backend tests:** `tests/routers/test_cmcd.py` (absolute range and range-token requests), `tests/high_scale/test_aggregate_query.py`, `tests/high_scale/test_clickhouse_readers_integration.py`. Provisioning side: `tests/provision/test_cmcd_enable_call_order.py`, `test_cmcd_disable_call_order.py`, `test_system_fields.py`.
- **Frontend unit tests:** `frontend/__tests__/app/streaming.test.tsx` (request contract, populated cards, not-available state, failed query).
- **Playwright E2E:** `frontend/e2e/pages/streaming.spec.ts` (5 tests, aggregates endpoint stubbed because the e2e backend has no CMCD columns) plus the axe route entry in `frontend/e2e/a11y-routes.spec.ts`. Real data is checked live by the deploy-time check in `scripts/verify_dashboard.js`, which also logs the `[PERF Streaming]` numbers above.
- **Synthetic Traffic Profile:** `scale_harness.py` with `--cmcd-ratio` and `--diversify` (see section 9); sessions get a stable `sid`/`cid`, a mix of video, audio, manifest and image objects, and about 4% rebuffer events.
