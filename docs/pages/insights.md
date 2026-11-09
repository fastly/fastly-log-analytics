# Insights (`/insights`)

Automated anomaly detection. Each registered detector compares a recent window against a baseline and renders a card with severity, current vs baseline values and a drill-down modal. There are 51 detectors in five categories.

Source: [`frontend/app/insights/page.tsx`](../../frontend/app/insights/page.tsx) (barrel re-export of [`frontend/app/insights/_sections/InsightsClient.tsx`](../../frontend/app/insights/_sections/InsightsClient.tsx)) and [`frontend/components/Insights/`](../../frontend/components/Insights/) (`index.tsx`, `InsightCard.tsx`, `InsightTabsAndControls.tsx`, `InsightDetailModal/`, `InsightHelpModal/`). Category labels and order are in `INSIGHT_SECTIONS` in [`frontend/lib/utils.ts`](../../frontend/lib/utils.ts).

## URL parameters

| Parameter | Meaning |
|---|---|
| `tab` | `all` or a category key: `security`, `origin`, `edge`, `network`, `traffic`. `performance` is an alias of `edge`, `routing` of `network` (`normalizeTabKey`). |
| `window` | Window in hours: `0.25`, `1`, `4`, `24`. |
| `baseline` | Baseline in hours: `1`, `24`, `168`, `720`. |
| `status` | Severity filter (default `all`). |

Defaults adapt to available history (a young service compares the last hour against the previous hour rather than showing "not enough data"); `pickInsightsDefault` in [`frontend/lib/chartUtils.ts`](../../frontend/lib/chartUtils.ts) and its server-side counterpart in [`backend/utils/time_window.py`](../../backend/utils/time_window.py) must agree so the prewarmer warms the pair the page will request. Tab switching filters the already-loaded cards client-side. Window/baseline changes are debounced by 400 ms.

## Behavior

- Cards load from one `POST /api/insights` call; skeleton cards come from the availability response while it is in flight. Each category shows "No anomalies detected" style empty state when all of its detectors are clean.
- Detectors whose required log fields are not in the service's log format are listed as unavailable in a banner; `GET /api/insight-availability` supplies the available and unavailable lists.
- Admins get a Re-scan action (`force_refresh: true`). Analysts do not see it, and the server forces `force_refresh` to false for analyst sessions.
- A 24h window with a 720h baseline is prefetched when the service has at least 30 days of history, once the selected request has finished.

## Detectors

**traffic** (6): `new_country_traffic`, `city_surges`, `new_city_traffic`, `ua_monoculture`, `referer_monoculture`, `method_drift`

**edge** (10): `cache_collapse`, `cacheability_regression`, `cache_ttl_mismatch`, `cache_hit_cliff`, `city_latency_regressions`, `latency_regression`, `tail_latency`, `image_optimization_opportunities`, `pop_latency_regression`, `payload_compression_regression`

**origin** (8): `error_spikes`, `city_error_spikes`, `origin_error_rate`, `origin_retries`, `origin_ip_failure`, `origin_latency_spike`, `shield_path_degradation`, `timeout_split`

**security** (18): `botnet_grouping`, `new_probe_urls`, `waf_signal_spikes`, `proxy_surge`, `cipher_spread`, `request_size_anomaly`, `connection_abuse`, `repeated_patterns`, `repeated_patterns_fp`, `low_and_slow`, `credential_enumeration`, `content_discovery`, `session_harvesting`, `crawler_disparity`, `stale_browser_version`, `orphaned_deep_crawl`, `residential_fingerprint_dispersion`, `impossible_distance`

**network** (9): `asn_metro_performance`, `region_latency`, `network_asn_health`, `metro_delivery_degradation`, `http3_fallback`, `asn_concentration`, `new_asn_traffic`, `asn_hosting_shift`, `connection_type_mix`

Definitions (id, category, title, `required_fields`, `required_groups`, ClickHouse SQL, row processor) live in [`backend/high_scale/insights/`](../../backend/high_scale/insights/) and [`backend/core/insight_definitions.py`](../../backend/core/insight_definitions.py) (`INSIGHT_DEFINITIONS`). Key invariants:
- Every definition must declare an `InsightCategory` (`INSIGHT_DEFINITIONS`; registration without one fails validation at import). A new insight in an existing category needs no frontend change; a new category needs a tab entry in [`frontend/components/Insights/InsightTabsAndControls.tsx`](../../frontend/components/Insights/InsightTabsAndControls.tsx).
- Set `required_groups=[]` when the gate should be only core fields — declaring a group grays the card out in previews even if it would run.

## Cloud local analysis

Cloud runs 22 of the 51 catalog detectors over downloaded request Parquet using browser-local DuckDB:

- **Traffic:** all six traffic detectors.
- **Edge:** `cache_collapse`, `cacheability_regression`, `cache_hit_cliff`, `city_latency_regressions`, `latency_regression`, `tail_latency`, `pop_latency_regression`.
- **Origin:** `error_spikes`, `city_error_spikes`.
- **Security:** `credential_enumeration`, `content_discovery`, `proxy_surge`.
- **Network:** `asn_concentration`, `new_asn_traffic`, `asn_hosting_shift`, `http3_fallback`.

The other 29 algorithms are explicitly unavailable (`cloud_detector_unsupported`), even when their fields exist. Supported detectors also require their actual loaded Parquet columns; a missing field produces `missing_fields`. Availability never assumes the entire catalog exists when schema discovery returns no columns. The page explains these two restrictions separately.

The local scans share the selected filters and analysis bounds, including fractional-hour windows. Comparisons require at least `min(24, selected baseline hours)` of baseline history; insufficient history or an empty current window produces an informational **not evaluated** card, never a clean result. `content_discovery` and `tail_latency` only need current-window samples and can run without baseline history. Credential enumeration only counts authentication paths and scales baseline denials to the selected window duration.

Cache-collapse drill-down reads actual local hit/miss/pass counts, timeline buckets and recent misses. Masked sessions omit original IPs from findings and drill-down evidence. Local query failures remain visible errors; unsupported algorithms never produce clean cards.

## Roles

| Role | Access |
|---|---|
| Admin | Full, including Re-scan. |
| Analyst (remote share) | Read-only. `/api/insights` is in the analyst POST allowlist; window and baseline are clamped to the invite's lookback ceiling; IP-keyed detections are masked; results use a stable invite-keyed cache so they can be prewarmed. |

## Backend

Routes in [`backend/routers/insights.py`](../../backend/routers/insights.py), models in [`backend/models/analytics/insights.py`](../../backend/models/analytics/insights.py).

- `POST /api/insights` (`InsightsResponse`): request `window_size_hrs` (default 24, max 168), `baseline_hours` (default 168, max 2160), `filters`, `force_refresh`. Response carries the insight items (card payload, severity, per-detector `meta`) and evaluation metadata.
- `POST /api/insights/cache-collapse-detail` (`CacheCollapseDetailResponse`): timeline, disposition breakdown and recent misses for one URL (`url`, `window_size_hrs`, `baseline_hours`).
- `GET /api/insight-availability` ([`backend/routers/bootstrap.py`](../../backend/routers/bootstrap.py)): `InsightsAvailabilityResponse` listing available and unavailable detectors for the active service.

Results are cached in-process (TTL 300 s). If some cards fail (for example a ClickHouse timeout), the successful cards are cached for 60 s with the failed cards' errors kept, so a retry or a queued request doesn't recompute them. Results are warmed by the `insights_prewarmer` cron, which is demand-driven ([cron spec](../cron/jobs/insights-prewarmer.md)).

On High-Scale an evaluation runs every detection over the whole baseline and can take minutes, so page requests never run it inline. Each request shape has at most one evaluation in flight, in a background thread; the prewarmer and *Re-scan* (`force_refresh`) join it and wait for its result. Shapes with no result yet run on their own two-thread pool, so prewarms and stale refreshes (a separate two-thread pool) cannot hold them back; a page request for a shape whose prewarm is still queued moves it to that pool. Once the 300 s or 60 s entry expires, the last result (kept up to 24 h, error cards included) is returned at once with its original `computed_at`, `_is_cached: true`, `refreshing: true` and, if the previous refresh failed, `refresh_error`, while one refresh runs. With no previous result the request waits up to 5 s from when the evaluation starts; if it is still running, the response has `pending: true`, an empty `computed_at` and one `info` card with `placeholder: true` per detection to be evaluated ("Evaluating in the background…"), plus the exact insufficient-baseline cards. While the evaluation is still queued or reading history (up to 15 s more), `insights` is empty. Poll until `pending` and `refreshing` are false. A failed evaluation is raised to the next page request if it failed in the last 60 s (a failed *Re-scan* is raised to its caller only); after that the next request starts a new evaluation. Failures are never shown as pending or clean.

Cache misses use the existing `HighScaleSections` telemetry: metadata reads are `insights.history` and `insights.fields`, and detector reads are `insights.<detector_id>`, all with `source=fact`. These stable labels and request correlation survive the async and worker-thread boundaries. Section events contain timings and outcomes, not SQL, filter values or request bodies.

The Insights engine ([`backend/high_scale/insights/`](../../backend/high_scale/insights/)) runs every registered detection natively in ClickHouse over visible `request_facts` (`sql_queries.py`, one query per detection), so cards, thresholds and severities come from the unified registry. Unfiltered evaluations read the baseline hours of 20 count-only detections from the exact hourly rollup `request_insight_hourly`, but only for whole hours whose row counts and batch checksums match the raw facts; the window, edge hours and any mismatched hour read raw facts, so results are identical ([rollup runbook](../runbooks/high-scale-insights-rollup.md)). Detections whose required field is absent are omitted; a service with less history than the baseline gets the standard `info` card ("Requires Nh of historical data"), except `impossible_distance`, which needs no baseline; a failing query becomes an `error` card, never `clean`.

## Tests

- Backend: [`tests/cron/test_insights_prewarmer.py`](../../tests/cron/test_insights_prewarmer.py), [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py)
- E2E: `Insights Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
