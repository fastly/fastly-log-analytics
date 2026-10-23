# Page Specification: Edge Security & Bot Intelligence (`/security`)

> [!NOTE] **Live-certified Specification**
> This specification defines the verified contract, architecture behaviors, role permissions, and test cases for `/security`. Verified and live-certified across Standard and High-Scale deployment modes with full telemetry attribution, zero layout shift (CLS = 0.00), and end-to-end Playwright validation.

---

## 1. Overview & Objectives

The **Security & Bot Intelligence** page delivers threat analytics across Fastly WAF rules, Next-Gen WAF (Signal Sciences) signals, Verified Bots (Googlebot, Bingbot, etc.), TLS fingerprinting (JA3 / JA4), DDoS rate limiting blocks, and malicious client behavior.

### Key Tenets:
- **Comprehensive Threat Surface:** Integrates WAF blocks, NGWAF agent tags, and rate limiting actions.
- **Bot Verification:** Distinguishes verified search engine crawlers from spoofed user agents using rDNS and CIDR intelligence.
- **Shared Primitives:** Conforms strictly to `ReportLayout`, `FilterBar`, and global Zustand stores.

---

## 2. Routes & URL Schema

- **Primary Route:** `/security`
- **Supported Query Parameters:**
  - `service`: Fastly Service ID.
  - `from`, `to`: ISO timestamps or preset tokens (`now-24h`, `now-7d`).
  - Standard drill-down filters: `waf_action`, `bot_category`, `ja4`, `country`, `asn`.

---

## 3. Role & Permission Matrix

| Role | Access Level | Data Redaction & Capabilities |
|---|---|---|
| **Admin (`read_write`)** | Full Access | Full client IPs, threat payloads, NGWAF signal drill-down, IP blocking triggers. |
| **Analyst Path B (Remote Share)** | Read-Only Access | Aggregated attack charts and threat distributions viewable; client IPs masked (if enabled). |
| **Analyst Path A (JSON Join)** | Local Read-Only | Queries local DuckLake / cached buffer. |

---

## 4. Architecture Execution Matrix

| Component / Subsystem | Standard Mode (`DEPLOYMENT_MODE=standard`) | High-Scale Mode (`DEPLOYMENT_MODE=high_scale`) |
|---|---|---|
| **Data Plane** | Reads DuckDB session view + `security_dims` / `security_proxies` / `verified_bots_ts` rollups. | Reads ClickHouse `request_facts` (every card honors the request filters and `sections` selector) and, for unfiltered NGWAF bot cards, the `ngwaf_bot` rows of `security_minute_dimensions`. A ClickHouse failure returns an error envelope, never an empty card. |
| **Bot Enrichment** | PostgreSQL `ngwaf_bots` and `rdns` tables (`METADATA_DSN`). | NGWAF bot names are resolved at ingest from the Postgres `ngwaf_bots` cache and persisted on the fact row (`ngwaf_bot`, `ngwaf_bot_category`, `ngwaf_wellknown_bot`) plus the per-minute `ngwaf_bot` projection; well-known-bot verification uses the shared `rdns` cache. |
| **Analyst masking** | `mask_ips` masks `active_clients[].ip` and `top_ips_header[].ip`. | Same fields masked by the same helper, on the aggregates and proxies-export paths. |

---

## 5. Backend APIs & Telemetry Attribution

### Endpoints Hit:
1. `GET /api/security/bundle` and `POST /api/security/bundle` (or composite bundle):
   - Returns WAF block rate, attack type distribution, Top Attacking IPs/ASNs, Verified vs Spoofed Bot counts, Bot & Crawler Value vs. Cost (`bot_disparity`: requests, bandwidth `bytes`, bandwidth share `%`, referred human IPs, `requests_per_referral`, and `bytes_per_referral` dynamically joined against `bot_sources`, NGWAF bot tags, and HTTP client User-Agent families), JA4 fingerprints, header anomalies, and VPN/proxy indicators.
2. `GET /api/security/threat-intel`:
   - Returns active client threat intel, JA3/JA4 fingerprints, and NGWAF detections.
3. `GET /api/security/proxies/export`:
   - Streams CSV or plain text suspicious proxy client data with tenancy and IP masking support.

### Telemetry Attribution:
- Every query carries `X-Page-Load-ID`.
- Database statements recorded in `telemetry_queries` and audited for efficiency.

---

## 6. UI Components, Skeletons & Layout Reservation (Zero CLS)

- **Layout Reservation:** Threat KPI Summary, WAF Attack Timeline, Bot Classification Donut, and Top Threat Vectors Table enforce container intrinsic sizes (`[contain-intrinsic-size:360px]` / `[contain-intrinsic-size:300px]` / `[contain-intrinsic-size:120px]`) and `[content-visibility:auto]`.
- **In-Place Skeletons:** Panels display contextual loading copy (`Scanning WAF events...`, `Classifying bot traffic...`, `Auditing proxy & VPN traffic...`, etc.).
- **Dimmed Updates:** Updates dim panels (`opacity-40 pointer-events-none`) with zero layout shift (CLS = 0.00).

---

## 7. Interactive Workflows & Edge Cases

- **Universal Time Extent Standard:** Defaults to 24h for mature services, or max available data extent if history < 24h.
- **Threat Vector Drill-Down:** Clicking a WAF rule or JA4 fingerprint injects the filter chip to isolate attack waves.
- **Zero-Attack State:** Services with zero security blocks display "No security incidents detected" calmly with green health indicators.

---

## 8. Performance, Cost & Telemetry Budgets

| Metric | Target (Standard) | Target (High-Scale) | Measurement Method |
|---|---|---|---|
| **FCP / TTI** | < 400 ms / < 800 ms | < 400 ms / < 800 ms | Web Vitals / Playwright |
| **Cumulative Layout Shift (CLS)** | 0.00 | 0.00 | PerformanceObserver |
| **API Latency (Bundle)** | < 350 ms (p95) | < 250 ms (p95) | HAR log / API timing |
| **Telemetry Capture** | 100% queries captured | 100% queries captured | `_debug_*` / `/api/debug/page-telemetry` |
| **Query Efficiency** | Utilizes `security_dims` rollups; 0 redundant queries | Utilizes rollups; 0 redundant queries | Query profiler audit |

---

## 9. AI Session Automated Verification Checklist

- [x] **1. Route Accessibility:** `GET /security` returns HTTP 200.
- [x] **2. Zero Layout Shift:** Verify page loads with pre-allocated panel skeletons and CLS = 0.00.
- [x] **3. Threat Bundle Contract:** Verify WAF actions and bot classifications populate accurately.
- [x] **4. Time Range Extent:** Confirms 24h default or adaptive extent for services < 24h history.
- [x] **5. Bot Verification Filter:** Filter by "Verified Bots" vs "Spoofed Bots" and verify graph transitions.
- [x] **6. Role Verification:** Confirm Analyst Path B observes masked IPs and cannot access admin triggers.
- [x] **7. Telemetry & Query Audit:** Verify all DuckDB and PostgreSQL queries are recorded with zero duplicate queries.

---

## 10. Automated Test Suite & Traffic Generation

- **Playwright Test:** `frontend/e2e/pages/security.spec.ts` (Comprehensive E2E suite covering zero CLS, bundle API verification, core card rendering, tab navigation, 15m/24h/30d presets, and performance budgets).
- **Synthetic Traffic Profile:** Mixed legitimate traffic with injected SQLi/XSS attack patterns and simulated bot crawlers.
