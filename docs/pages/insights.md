# Page Specification: Automated Insights & Anomaly Detection (`/insights`)

> [!NOTE] **Status: Live-certified (`release/v3.0.0-beta3`)**
> This specification defines the verified contract, architecture behaviors, role permissions, layout stability standards, and automated test suites for `/insights`. Live certified across Standard, Remote Standard GCE, and High-Scale Elevation environments.

---

## 1. Overview & Objectives

The **Insights** page runs 51 automated anomaly and pattern detectors comparing current traffic against baseline historical distributions (e.g. current 1h vs 168h baseline). It identifies abnormal error rate surges, sudden traffic drop-offs, regional routing deviations, cache hit degradations, and emerging attack signatures.

### Key Tenets:
- **Baseline Comparison Engine:** Employs statistical deviation checks (z-score, ratio shifts) across 5 categories: Traffic (6), Performance/Edge (10), Origin (8), Security (18), and Network/Routing (9).
- **Prewarmed Speed:** Relies on the background `insights_prewarmer_{id}` (Cron 13) job to guarantee instant sub-150ms p95 rendering.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.
- **Zero-CLS Layout Reservations:** Enforces strict CSS layout containment (`min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]` on grid and empty-pass state, `min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]` on cards) to prevent layout shifts during hydration or tab switching.
- **Missing Fields Guidance:** Detects when optional VCL logging fields are missing for specific detectors and guides the operator directly to `/service-config?service={id}` to enable them.

---

## 2. Routes & URL Schema

- **Primary Route:** `/insights`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `tab`: Category tab (`all`, `traffic`, `performance` / `edge`, `origin`, `security`, `routing` / `network`).
  - `window`: Active evaluation window (e.g. `1h`, `24h`).
  - `baseline`: Historical baseline window (e.g. `168h` / 7 days).
- **Tab Aliases:**
  - `performance` aliases directly to `edge`.
  - `routing` aliases directly to `network`.
  - Frontend tabs render `all`, `traffic`, `performance`, `origin`, `security`, `routing` with `data-tab`, `data-category`, and `data-alias` attributes.

---

## 3. Anomaly Detectors Catalog (51 Detectors)

### Traffic Category (6 Detectors)
1. `city_surges`: City Traffic Surges (required fields: `city`, `country`)
2. `new_city_traffic`: New City Traffic (required fields: `city`)
3. `new_country_traffic`: New Country Traffic (required fields: `country`)
4. `ua_monoculture`: User-Agent Monoculture (required fields: `ua`)
5. `referer_monoculture`: Referer Monoculture (required fields: `referer`)
6. `method_drift`: HTTP Method Drift (required fields: `method`)

### Edge & Performance Category (10 Detectors)
1. `city_latency_regressions`: City Latency Regressions (required fields: `city`, `elapsed`)
2. `latency_regression`: URL Latency Regressions (required fields: `url`, `elapsed`)
3. `cache_ttl_mismatch`: Cache TTL Mismatches (required fields: `cache`, `ttl`, `age`, `hits`)
4. `cache_collapse`: Cache Efficiency Collapse (required fields: `url`, `cache`)
5. `cacheability_regression`: Cacheability Regression (required fields: `url`, `cache`)
6. `tail_latency`: Tail Latency Anomaly (required fields: `url`, `elapsed`)
7. `image_optimization_opportunities`: Image Optimization Opportunities (required fields: `url`, `ua`, `resp_bytes`)
8. `pop_latency_regression`: PoP Latency Regression (required fields: `pop`, `elapsed`)
9. `cache_hit_cliff`: Cache HIT-Ratio Cliff (required fields: `cache`)
10. `payload_compression_regression`: Payload Compression Regression (required fields: `url`, `resp_header_content_encoding`, `resp_bytes`, `status`)

### Origin Health Category (8 Detectors)
1. `error_spikes`: Error Spikes (required fields: `status`, `url`)
2. `city_error_spikes`: City Error Spikes (required fields: `city`, `status`)
3. `origin_latency_spike`: Origin Latency Spike (required fields: `ottfb`)
4. `origin_error_rate`: Origin Error Rate (required fields: `ost`)
5. `origin_retries`: Origin Retries Elevated (required fields: `oretries`)
6. `origin_ip_failure`: Specific Origin IP Failing (required fields: `oip`, `ost`)
7. `shield_path_degradation`: Shield Path Degradation (required fields: `rid`, `prid`, `edge`, `pop`, `ottfb`)
8. `timeout_split`: Origin Connect vs Read Timeout Split (required fields: `oconnect_ms`, `ottfb`)

### Security Category (18 Detectors)
1. `botnet_grouping`: Botnet Grouping (required fields: `ja3`, `ja4`)
2. `low_and_slow`: Low and Slow Scans (required fields: `ip`, `url`)
3. `credential_enumeration`: Credential Enumeration / Brute Force (required fields: `ip`, `url`, `status`)
4. `content_discovery`: Content-Discovery Scanning (required fields: `ip`, `url`, `status`)
5. `proxy_surge`: Proxy Traffic Surge (required fields: `p_type`)
6. `request_size_anomaly`: Request Size Anomalies (required fields: `req_bytes`, `req_header_bytes`)
7. `waf_signal_spikes`: WAF Signal Spikes (required fields: `waf`, `waf_sig`, `waf_resp`, `status`)
8. `repeated_patterns`: Scripted Traffic Patterns (required fields: `ip`)
9. `repeated_patterns_fp`: Scripted Traffic Patterns by TLS Fingerprint (required fields: `ip`)
10. `new_probe_urls`: New Probe URLs (required fields: `url`, `status`)
11. `cipher_spread`: Cipher Fingerprint Clustering (required fields: `tls_ciphers_sha`)
12. `impossible_distance`: Impossible Distance / Spoofing (required fields: `pop`, `lat`, `lon`, `tcp_rtt`)
13. `connection_abuse`: Connection Reuse Anomaly (required fields: `conn_requests`)
14. `session_harvesting`: Session-ID Harvesting (required fields: `ip`, `cookie_session`)
15. `crawler_disparity`: Bot & Crawler Referral & Bandwidth Disparity (required fields: `ua`, `referer`, `ip`, `timestamp`; optional bandwidth tracking via `resp_bytes`, dynamic `bot_sources` + NGWAF + HTTP client matching, and interactive `CrawlerDisparityModal` breakdown)
16. `stale_browser_version`: Stale Headless Browser (required fields: `ua`, `ip`, `timestamp`)
17. `orphaned_deep_crawl`: Orphaned Deep Crawl (required fields: `ip`, `url`, `referer`, `timestamp`)
18. `residential_fingerprint_dispersion`: Residential Fingerprint Dispersion (required fields: `ja4`, `ip`, `asn`, `p_type`, `timestamp`)

### Network & Routing Category (9 Detectors)
1. `asn_concentration`: ASN Concentration (required fields: `asn`)
2. `network_asn_health`: Network Path (ASN) Health (required fields: `asn`, `ploss`, `rtt_var`, `retrans`)
3. `region_latency`: Billing Region Latency (required fields: `server_region`, `elapsed`, `ttfb`, `asn`, `ottfb`)
4. `asn_metro_performance`: ASN/Metro Performance Regressions (required fields: `asn`, `metro`, `tcp_rtt`, `country`)
5. `new_asn_traffic`: New ASN Traffic (required fields: `asn`)
6. `asn_hosting_shift`: ASN Hosting Shift (required fields: `asn`, `p_type`)
7. `metro_delivery_degradation`: Metro Delivery-Rate Degradation (required fields: `metro`, `delivery_rate`)
8. `connection_type_mix`: Connection-Type Mix Shift (required fields: `c_type`, `c_speed`)
9. `http3_fallback`: HTTP/3 → TCP Fallback Spike (required fields: `transport`)

---

## 4. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full anomaly cards, sensitivity controls, trigger immediate re-scan (`force_refresh=True`). |
| **Analyst (Remote Share)** | Read-Only Access | Full anomaly cards and baseline comparisons viewable; re-scan trigger hidden from DOM (`!isRemoteAnalyst`); server-side `force_refresh` clamped to `False`; IP addresses masked if enabled by admin. |

---

## 5. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Anomaly Compute** | DuckDB window functions & ratio comparisons over local data. Zero FOS egress. Wrapped in `execute_with_stale_view_retry`. | Ephemeral DuckDB / ClickHouse analytical queries reading durable DuckLake. |
| **Prewarming Cache** | Pod-local memory `_insights_cache` (TTL 300s) populated by `insights_prewarmer_{id}` (Cron 13). | Shared Redis or pod-local cache populated by prewarmer. |
| **Missing Fields Detection** | Dynamic catalog comparison against `service.json` log fields. Renders clear feedback and enablement guidance. | Same dynamic catalog comparison against active service schema. |

---

### High-scale detections

`backend/high_scale/insights.py` computes every registered detection natively in
ClickHouse over visible `request_facts`
(`backend/high_scale/insight_queries.py`, one query per detection returning the exact
row layout of the standard processors). Cards, thresholds, severities, `meta` and
summaries therefore come from the same registry code on both data planes. The
window/baseline/analyst-clamp parameters are honoured, `mask_ips` is applied to
IP-keyed detections, and:

- A detection whose required log field is absent from the service's recent rows is
  omitted, as on the standard path.
- A service with less history than `baseline_hours` gets the standard `info` card
  ("Requires Nh of historical data") instead of a computed result. `impossible_distance`
  is the one detection that runs without a baseline.
- A failing query is an `error` card ("Query failed: ..."), never a `clean` one.
- Percentiles use t-digest sketches (bounded memory at high-scale volumes); the
  detections compare ratios and large absolute deltas, so the sketch error is far
  below every threshold.
- `POST /api/insights/cache-collapse-detail` returns the real timeline, disposition
  breakdown, hit/pass rates and recent misses for the URL.
- The legacy `high_scale_5xx` card is no longer emitted; `error_spikes` supersedes it.

`RUN_HS_READERS_CLICKHOUSE_INTEGRATION=1 uv run pytest
tests/high_scale/test_clickhouse_readers_integration.py` runs every detection on a real
ClickHouse engine (disposable `clickhouse-local` processes) over planted anomalies.

## 6. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/insights`:
   - Parameters: `service_id` (str), `window` (int, default 1), `baseline` (int, default 168), `force_refresh` (bool, default False).
   - Response: `InsightsResponse` typed Pydantic model (`insights: list[InsightItem]`, `total: int`, `by_category: dict[str, int]`, `evaluation_time_ms: float`, `evaluated_at: str`, `cached: bool`, `window_hours: int`, `baseline_hours: int`, `service_id: str`).
   - Telemetry attribution: Section spans `insights.evaluate`, `process_context="insights:eval"`, query attribution via `RequestContext`.
2. `GET /api/insights/cache-collapse-detail`:
   - Parameters: `service_id`, `url`, `window_hours`, `baseline_hours`.
   - Response: `CacheCollapseDetailResponse` typed model.
3. `GET /api/bootstrap/insight-availability`:
   - Parameters: `service_id`.
   - Response: `InsightsAvailabilityResponse` (`available: list[InsightAvailability]`, `unavailable: list[InsightAvailability]`).

---

## 7. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:**
  - Grid container: `min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]` (`data-testid="insights-grid"`).
  - Empty category pass card: `min-h-[300px] [contain-intrinsic-size:300px] [content-visibility:auto]` (`data-testid="insights-empty-pass"`).
  - Individual cards: `min-h-[220px] [contain-intrinsic-size:220px] [content-visibility:auto]` (`data-testid="insight-card"`).
- **In-Place Skeletons:** Cards render contextual loading copy via `SECTION_LOADING_MESSAGES`:
  - `all`: `"Evaluating all 51 baseline detectors..."`
  - `security`: `"Analyzing security and threat indicators..."`
  - `origin`: `"Evaluating origin health and backend latency..."`
  - `edge`: `"Analyzing cache efficiency and edge latency..."`
  - `network`: `"Evaluating network path and routing performance..."`
  - `traffic`: `"Analyzing traffic volume and distribution anomalies..."`
- **Dimmed Updates:** Auto-refresh or filter changes dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 8. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 1h active window vs 168h baseline for mature services; adapts baseline if history < 168h.
- **Category Filtering:** Switching tabs (`all`, `traffic`, `performance`, `origin`, `security`, `routing`) filters cards client-side instantaneously without refetching.
- **Normal Parameters State:** When all detectors pass cleanly in a category, renders `data-testid="insights-empty-pass"` with checkmark and `"No anomalies detected in {section.label}"`.
- **Missing Optional Fields Alert:** When detectors are unavailable due to unconfigured log fields, renders a dismissible alert with a warning and direct link to `/service-config?service={activeServiceId}`.

---

## 9. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **LCP / FCP** | < 1500 ms / < 400 ms | < 1500 ms / < 400 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | $\le$ 0.05 (0.00) | $\le$ 0.05 (0.00) | PerformanceObserver / Playwright |
| **API Latency (Prewarmed)** | < 150 ms (p95) | < 150 ms (p95) | HAR log / API timing |
| **API Latency (Cold)** | < 600 ms (p95) | < 400 ms (p95) | Cold cache run |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Prewarmed hit; 0 redundant queries | Cached hit; 0 redundant queries | Query profiler audit |

---

## 10. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /insights` returns HTTP 200 with shell and ServerFooter indicator.
- [x] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS $\le$ 0.05 (0.00).
- [x] **3. Anomaly Evaluation Contract:** Verify all 5 categories return valid insight cards or clean passes. Unavailable insights render guidance linking to `/service-config`.
- [x] **4. Prewarmer Effectiveness:** Confirm response time for default window is < 150ms due to `insights_prewarmer_{id}`.
- [x] **5. Category Tab Switching:** Click category tabs and verify instantaneous client-side filtering without refetching.
- [x] **6. Role Verification:** Confirm Analyst observes expected view with Re-scan button hidden from DOM and server-side `force_refresh` clamped.
- [x] **7. Telemetry & Query Audit:** Verify all DuckDB and SQLite queries are recorded with zero duplicate queries and proper attribution (`process_context="insights:eval"`).

---

## 11. Automated Test Suite & Coverage

- **Playwright E2E Suite:** `frontend/e2e/pages/insights.spec.ts` (8 comprehensive tests asserting route accessibility, layout stability, data contracts, tab switching, clean pass states, preset transitions, performance budgets, and telemetry attribution).
- **Frontend Unit Tests:** `frontend/__tests__/app/insights.test.tsx` (7 tests verifying tab switching, zero-CLS layout reservations, Re-scan button role gating, missing fields warning banner, and empty state pass indicators).
- **Section Helper Unit Tests:** `frontend/__tests__/lib/insight-sections.test.ts` (verifying tab normalization, all section grouping, loading messages).
- **Backend Router Unit Tests:** `tests/routers/test_insights_router.py` (5 tests verifying Pydantic models, query validation, analyst force-refresh clamping, stale view retry wrapping, and cache collapse detail endpoint).
