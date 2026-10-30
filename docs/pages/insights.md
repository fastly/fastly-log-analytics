# Insights (`/insights`)

Automated anomaly detection. Each registered detector compares a recent window against a baseline and renders a card with severity, current vs baseline values and a drill-down modal. There are 51 detectors in five categories.

Source: [`frontend/app/insights/page.tsx`](../../frontend/app/insights/page.tsx) (server wrapper that prefetches and dehydrates the default window/baseline, helpers in [`frontend/lib/ssr/upstream.ts`](../../frontend/lib/ssr/upstream.ts)) and [`frontend/components/Insights/`](../../frontend/components/Insights/) (`index.tsx`, `InsightCard.tsx`, `InsightTabsAndControls.tsx`, `InsightDetailModal/`, `InsightHelpModal/`). Category labels and order are in `INSIGHT_SECTIONS` in [`frontend/lib/utils.ts`](../../frontend/lib/utils.ts).

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
- A 24h window with a 720h baseline is prefetched when the service has at least 30 days of history.

## Detectors

**traffic** (6): `new_country_traffic`, `city_surges`, `new_city_traffic`, `ua_monoculture`, `referer_monoculture`, `method_drift`

**edge** (10): `cache_collapse`, `cacheability_regression`, `cache_ttl_mismatch`, `cache_hit_cliff`, `city_latency_regressions`, `latency_regression`, `tail_latency`, `image_optimization_opportunities`, `pop_latency_regression`, `payload_compression_regression`

**origin** (8): `error_spikes`, `city_error_spikes`, `origin_error_rate`, `origin_retries`, `origin_ip_failure`, `origin_latency_spike`, `shield_path_degradation`, `timeout_split`

**security** (18): `botnet_grouping`, `new_probe_urls`, `waf_signal_spikes`, `proxy_surge`, `cipher_spread`, `request_size_anomaly`, `connection_abuse`, `repeated_patterns`, `repeated_patterns_fp`, `low_and_slow`, `credential_enumeration`, `content_discovery`, `session_harvesting`, `crawler_disparity`, `stale_browser_version`, `orphaned_deep_crawl`, `residential_fingerprint_dispersion`, `impossible_distance`

**network** (9): `asn_metro_performance`, `region_latency`, `network_asn_health`, `metro_delivery_degradation`, `http3_fallback`, `asn_concentration`, `new_asn_traffic`, `asn_hosting_shift`, `connection_type_mix`

Definitions (id, category, title, `required_fields`, `required_groups`, SQL, row processor) live in [`backend/repositories/insights/definitions/`](../../backend/repositories/insights/definitions/) and [`backend/core/insight_definitions.py`](../../backend/core/insight_definitions.py) (`INSIGHT_DEFINITIONS`). Key invariants:
- Every definition must declare an `InsightCategory` ([`backend/repositories/insights/__init__.py`](../../backend/repositories/insights/__init__.py); registration without one fails Pydantic at import). A new insight in an existing category needs no frontend change; a new category needs a tab entry in [`frontend/components/Insights/InsightTabsAndControls.tsx`](../../frontend/components/Insights/InsightTabsAndControls.tsx).
- Set `required_groups=[]` when the gate should be only core fields — declaring a group grays the card out in previews even if it would run.
- Because the parameter-binding heuristic counts `sql.count("?")`, regex literals in SQL templates must be `?`-free (no `(?i)`; pass the `'i'` flag to `regexp_matches`).
- Evaluate regex/string predicates once per distinct `url`, `ua`, or `referer` rather than per row, and keep CTEs referenced by both the window and baseline comparison (`AS NOT MATERIALIZED`) from being materialized over the full 30-day table. Zero behavior drift across all registered definitions is enforced by `tests/repositories/test_insights_golden.py`.

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

Results are cached in-process (TTL 300 s) and warmed by the `insights_prewarmer` cron, which is demand-driven and yields to ingest ([cron spec](../cron/jobs/insights-prewarmer.md)).

High-scale mode ([`backend/high_scale/insights/`](../../backend/high_scale/insights/)) runs every registered detection natively in ClickHouse over visible `request_facts` (`sql_queries.py`, one query per detection, same row layout as the standard processors), so cards, thresholds and severities come from the same registry code. Detections whose required field is absent are omitted; a service with less history than the baseline gets the standard `info` card ("Requires Nh of historical data"), except `impossible_distance`, which needs no baseline; a failing query becomes an `error` card, never `clean`.

## Tests

- Backend: [`tests/repositories/test_insights_golden.py`](../../tests/repositories/test_insights_golden.py) (all registered definitions on a synthetic dataset), [`tests/cron/test_insights_prewarmer.py`](../../tests/cron/test_insights_prewarmer.py), [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py)
- E2E: `Insights Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
