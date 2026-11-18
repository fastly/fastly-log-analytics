# Fastly Value (`/fastly-value`)

Summary of what the CDN delivers for a service: offload, cache efficiency, bandwidth, threats blocked, bot share, performance, protocol adoption and Image Optimizer savings, shown in tabs.

Source: [`frontend/app/fastly-value/`](../../frontend/app/fastly-value/) (`page.tsx` re-exports `_sections/FastlyValueClient.tsx`, `ValueDomainTabs.tsx`, `IOValueTab.tsx`). Built on `ReportLayout`; not SSR-seeded.

## Tabs

`summary`, `caching` (CDN & Caching), `assets` (Assets & Shield), `security`, `bots` (Bot Management), `performance`, `network`, `io` (Image Optimizer). Each tab requests only its own section(s) from `POST /api/value/summary` (the Summary tab asks for `overview`, `caching`, `network`). The Assets & Shield tab renders the same content as [assets-shield.md](assets-shield.md) and disables the value query. Without Image Optimizer traffic the `io` tab shows an upsell view with optimization opportunities.

## Roles

| Role | Access |
|---|---|
| Admin | Full. |
| Analyst (remote share) | Read-only; `/api/value/` is in the analyst POST allowlist; window clamped to the invite; the Image Optimizer tab still depends on the service's stored Fastly credentials server-side. |

## Backend

`POST /api/value/summary` in [`backend/routers/performance.py`](../../backend/routers/performance.py); ClickHouse reader logic in [`backend/high_scale/performance.py`](../../backend/high_scale/performance.py); models in [`backend/models/analytics/dashboard.py`](../../backend/models/analytics/dashboard.py).

- Request `ValueSummaryRequest`: range fields (`start_time`, `end_time`, `range_token`, `anchor`), `filters`, `chart_interval` (default `1 day`), `sections` from `overview`, `caching`, `security`, `bots`, `performance`, `network`, `io`.
- Response `ValueSummaryResponse`: `overview` (totals, bandwidth, edge hit and origin requests, origin offload %, threats blocked, cache acceleration factor), `caching` (hit/miss/pass/synth requests, bandwidth saved, shield effectiveness, offload and cache-state series), `security` (WAF blocked/logged/passed, threat series, top WAF signals), `bots` (bot and verified-bot requests, top bots), `performance` (hit and miss latency, p99, series), `network` (HTTP/3, TLS, IPv6, HTTP/2 shares and series), `io` (transforms, bandwidth, shield bandwidth, format distribution, estimated cost at Fastly list pricing for Image Requests: 100k free per month, then tiered per 10,000 requests, optimization opportunities).

Request sections query ClickHouse `request_facts` and minute projections for the active service. The `io` section reads daily entity metrics from Fastly Platform Metrics with the service's stored token, cached for 60 seconds (`_IO_STATS_CACHE`); it returns nothing if no token or service ID is configured. Optional log fields that are absent degrade to null or zero rather than failing the query.

## Fastly-reported comparison

With no filter, the Summary tab's Total Requests, Bandwidth Delivered and Origin Offload cards add a line such as `Fastly reported 1.2M · logs −0.4%` from Fastly [Platform Metrics](https://www.fastly.com/documentation/reference/api/metrics-stats/metrics-v2/platform/), so dropped or missing logs show up. The logged figures stay the primary values: filters and breakdowns depend on them. The line is left out, without failing the page, when a filter is active, metrics cannot be read (no token or service ID, an error, an analyst on Cloud) or the range spans more than 31 days. The line's tooltip names the comparison window.

The window is the whole UTC hours from the range start through the last hour Platform Metrics has reported, clipped to the logs' coverage (the High-Scale watermark, the Cloud "data through"). Hourly metrics are available about 15 minutes after the hour ends, so the current partial hour is never compared. The response's `fastly_reported` (`FastlyReportedMetrics`) carries the reported totals over that window and the logged requests and bytes over the same hours (`logged_requests`, `logged_bandwidth_bytes`). The page compares the logs with `reported × sample_rate / 100`. Offload is compared in percentage points against the card's logged value over the whole range.

| Logged figure | Stats fields (all nodes) | Stats fields (`edge_only`) |
|---|---|---|
| Requests (one log line per request on each node) | `requests` | `edge_requests` |
| Bytes (`resp.bytes_written`, headers included) | `resp_body_bytes` + `resp_header_bytes` | `edge_resp_body_bytes` + `edge_resp_header_bytes` |
| Cache hits (`HIT*` cache states) | `hits` | `edge_hit_requests` |

Field definitions: [Platform Metrics field reference](https://www.fastly.com/documentation/reference/api/metrics-stats/metrics-v2/platform/). Fields not used, and why:
- `bandwidth` also counts backend request, Compute and WebSocket bytes.
- `hit_ratio` is hits / (hits + misses), not hits / requests.
- `origin_offload` is byte-based.
- `origin_fetches` doesn't match either install's origin count, so origin counts aren't compared.

Known differences that are not lost logs:
- `requests` counts a shielded request at the edge and again at the shield. The logs do the same (`vcl_log` runs on every node), so both sides double-count alike. With `edge_only` both sides use the edge fields.
- A logging `custom_condition`, the `/rum-beacon` exclusion and segmented-caching inner requests leave requests out of the logs that Stats still counts.
- Sampling is random, so a sampled service drifts by a fraction of a percent.
- Ingest lag at the window's end can make the logs read low until they catch up.

High-Scale: `fetch_fastly_reported` in [`backend/repositories/value.py`](../../backend/repositories/value.py) makes one paginated hourly Platform Metrics entity query with the service's stored token, cached for 5 minutes (`_FASTLY_REPORTED_CACHE`), in parallel with the ClickHouse reads; the logged totals over the window are one extra `request_facts` count.

Cloud: the admin-only Compute route `GET /api/admin/usage/service-stats?from=<ms>&to=<ms>` ([`compute/cloud/src/admin/logs.rs`](../../compute/cloud/src/admin/logs.rs)) reads the four fields hourly for the logged service (at most 31 days) and returns them with its `sample_rate` and `edge_only`. [`frontend/lib/cloud-engine/value-stats.ts`](../../frontend/lib/cloud-engine/value-stats.ts) starts that read alongside the summaries, waits at most 5 s more for it, and compares it with the summaries' hourly logged series. It is cached per range for 5 minutes, failures included. Analysts get a 403 and no line. A read that falls back to the raw scan has no comparison either.

## Bandwidth Saved (est.)

Logged response bytes of cache hits: an estimate of what origin would have sent without the cache. Stats has no equivalent figure, so it has no comparison line.

## Cloud

Cloud answers `POST /api/value/summary` in the browser. With no filter, every section is served from v3 summaries in one request per tab, with no raw scan; "Data through" shows above the tabs. A filter keeps one raw read per tab.

Counts come from the summaries' per-field value counts and their per-day `value` rows. Those rows split requests by cache class and shield (with bytes, latency and image figures) and hold the WAF signal, bot name and image format-pair lists. The protocol series rebuckets the 5-minute protocol series. P99 latency comes from the elapsed-time histogram and is marked ≈; top WAF signals, top bots and format conversions are marked ≥ when their list was cut. WAF decisions, verified bots, image content types and URL-level opportunities aren't in Cloud logs, so they read as the raw scan's empty values.

The read falls back to the raw scan when a section's `value` rows are missing (summaries written before them), a needed field is cut to its top K or holds non-ASCII values, a custom field shadows one the raw scan reads, the byte total is zero, a day series is asked for at another chart interval, the protocol series is cut or can't be rebucketed to the chart interval, or the range isn't covered by bundles. The raw scan projects only the columns its requested sections read.

## Tests

- Cloud: [`frontend/lib/cloud-engine/value-rollup.test.ts`](../../frontend/lib/cloud-engine/value-rollup.test.ts), [`frontend/lib/cloud-engine/value-stats.test.ts`](../../frontend/lib/cloud-engine/value-stats.test.ts), `service_stats_*` in [`compute/cloud/tests/ingest.rs`](../../compute/cloud/tests/ingest.rs), [`frontend/lib/cloud-engine/adapter-section-rollups.test.ts`](../../frontend/lib/cloud-engine/adapter-section-rollups.test.ts), [`frontend/__tests__/components/AssetsValueRollup.test.tsx`](../../frontend/__tests__/components/AssetsValueRollup.test.tsx)
- Frontend: `fastly-value page mounts and renders title and tab navigation` in [`frontend/__tests__/app/dashboard.test.tsx`](../../frontend/__tests__/app/dashboard.test.tsx), [`frontend/__tests__/app/fastly-value-reported.test.tsx`](../../frontend/__tests__/app/fastly-value-reported.test.tsx)
- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/repositories/test_value_fastly_reported.py`](../../tests/repositories/test_value_fastly_reported.py), [`tests/repositories/test_base_helpers.py`](../../tests/repositories/test_base_helpers.py)
