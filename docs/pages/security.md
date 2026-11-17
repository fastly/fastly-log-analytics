# Security (`/security`)

Bot and threat analytics: NGWAF and well-known verified bots, TLS fingerprints, request-header anomalies, proxy/VPN usage, and a proxy watchdog with blocklist export.

Source: [`frontend/app/security/`](../../frontend/app/security/): `page.tsx` is a server wrapper that prefetches the default aggregates and dehydrates them (helpers in [`frontend/lib/ssr/upstream.ts`](../../frontend/lib/ssr/upstream.ts)); `_sections/SecurityClient.tsx`, `BotsSection.tsx`, `ThreatIntelSection.tsx`, `ProxyWatchdogSection.tsx`.

## URL parameters

Shared range and filter parameters (see [dashboard.md](dashboard.md)) plus `tab` (`waf-activity` default, `threat-intel`, `proxy-watchdog`).

## Tabs and panels

- **WAF Activity**: Verified Bots (NGWAF) series, Verified Bot Names (NGWAF), Well-Known Bots (with rDNS verification state), Top TLS Fingerprints, Bot & Crawler Cost vs. Referrals chart and Value vs. Cost table. NGWAF cards only render when the service has an NGWAF workspace (`bootstrap.ngwaf_configured`).
- **Threat Intelligence**: TLS fingerprint / threat-intel list (`/api/security/threat-intel`), Request Header Size Distribution, Oversized Request Headers (by IP), IPv6 Adoption over Time, Proxy/Anonymizer Breakdown, Connection Reuse distribution.
- **VPN & Proxy Watchdog**: Active VPN & Proxy Users, Tunnel Requests, Impossible Distance Mismatches, Traffic Quality, Suspicious ISPs, and an export center that downloads a blocklist for a chosen threshold.

Rows link to the Dashboard with the relevant filter applied. Tables have column-visibility dropdowns; cards show per-card loading messages and error states.

## Roles

| Role | Access |
|---|---|
| Admin | Full, including raw IPs. |
| Analyst (remote share) | Read-only; window clamped to the invite; `mask_ips` masks `active_clients[].ip` and `top_ips_header[].ip` in aggregates and in the proxies export. |

## Backend

Routes in [`backend/routers/security.py`](../../backend/routers/security.py), models in [`backend/models/analytics/security.py`](../../backend/models/analytics/security.py).

- `POST /api/security/aggregates` (alias `POST /api/security/bundle`; `GET /api/security/bundle` also exists): `SecurityAggregatesRequest` with `bucket_seconds` and `sections` from `verified_bots_ts`, `ngwaf_verified_bots`, `ngwaf_verified_bots_ts`, `wellknown_bots`, `bot_efficiency`, `tls_fingerprints`, `fingerprint_coverage`, `req_size_dist`, `top_ips_header`, `ipv6_adoption`, `proxy_dist`, `conn_reuse_dist`, `proxies`, `top_bots`. Returns `SecurityAggregatesResponse`.
- `GET /api/security/threat-intel` (`start_time`, `end_time`): `ThreatIntelListResponse` of TLS fingerprints with request count, top country, matched WAF rule count, bot percentage and proxy flag.
- `GET /api/security/proxies/export` (`start_time`, `end_time`, `threshold`, `format` = `fastly-acl` CSV or plain text): streamed blocklist; the router masks IPs for analysts.

Bot enrichment uses the PostgreSQL `ngwaf_bots` and `rdns` tables. The ClickHouse reader ([`backend/high_scale/security.py`](../../backend/high_scale/security.py)) reads `request_facts` with the request filters bound (unfiltered NGWAF bot cards read `security_minute_dimensions`); NGWAF names are resolved at ingest; a ClickHouse failure propagates as an error.

### Known-bot classification

`top_bots.bots` (here and in the dashboard bundle's Fastly Bots card) takes the top 2,000 user agents by request count in the filtered range and classifies them at query time: every well-known-bots entry with an accepted pattern that matches the UA (case-insensitive search) adds that UA's count to the entry. Rows are `{id, name, category, request_count}` with `name` the title-cased id and `category` the entry's first category, sorted descending, top 10. With no UA logged the list is empty.

- High-Scale: `classify_ua_counts` in [`backend/repositories/security/watchdog.py`](../../backend/repositories/security/watchdog.py), using the list `bot_data_refresh` keeps current.
- Cloud: [`frontend/lib/cloud-engine/bots/classify.ts`](../../frontend/lib/cloud-engine/bots/classify.ts) in the browser, against the bundled seed. The seed is generated from [`backend/utils/well_known_bots_seed.json`](../../backend/utils/well_known_bots_seed.json) into `well-known-bots.generated.ts` (id, first category, patterns, referral stems, verification domains and aliases, about 74 KB raw / 19 KB gzip) and loaded lazily on the first classification. After changing the backend seed, run `node scripts/generate-well-known-bots.mjs` from `frontend/`; `classify.test.ts` fails when ids, categories, patterns, domains or aliases drift, and [`tests/utils/test_well_known_bots_browser_copy.py`](../../tests/utils/test_well_known_bots_browser_copy.py) fails when the referral stems differ from the backend's `extract_entry_referral_stems`. Cloud's NGWAF bot lists stay empty.

### Well-Known Bots and Bot Efficiency

Both installs use the same classification and the same row shapes (`WellknownBotRow`, `BotEfficiencyRow`).

- `wellknown_bots` keeps only user agents that match a known bot, takes the busiest 10,000 `(ua, ip)` pairs by request count, and groups them by bot: top 50 by requests, with unique IPs, sample IPs and per-IP verification counts. High-Scale takes up to 5,000 distinct user agents from its UA rollup, keeps up to 500 that match, reads the pairs for those only, and scales sampled counts back up (without a rollup it filters with the bots' UA regex instead). Cloud does the same against the bundled seed, but takes the 5,000 busiest user agents so the candidate list is deterministic.
- Verification: High-Scale checks each IP against bot CIDRs and cached reverse DNS, and counts an IP as `pending` until its lookup finishes. Cloud can't do reverse DNS from the browser and the seed has no CIDRs, so it reports what High-Scale reports before a lookup: `pending` for bots that publish verification domains, `unverified` for the rest, and never `verified` or `spoofed`. On Cloud the card's intro drops the FCrDNS/CIDR verification claim, and the Pending help text and cell tooltip say the IPs are not verified because the browser can't run reverse-DNS checks, instead of High-Scale's "check back soon".
- `bot_efficiency` ranks the top 2,000 user agents by response bytes, groups them by bot (or vendor) name, and sets each bot's share of site bytes against the unique human IPs referred by the same vendor (referer host stem, or a `<vendor>-user` / `-externalfetcher` UA): top 25 by bytes. High-Scale reads the sampled table and scales; Cloud reads every row. Cloud implements both in [`frontend/lib/cloud-engine/bots/summarize.ts`](../../frontend/lib/cloud-engine/bots/summarize.ts), ported from `summarize_wellknown_bot_rows` and `summarize_bot_efficiency_rows` in [`backend/repositories/security/watchdog.py`](../../backend/repositories/security/watchdog.py).

## Tests

- Frontend unit: [`frontend/__tests__/app/security.test.tsx`](../../frontend/__tests__/app/security.test.tsx)
- Backend: [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py), [`tests/utils/test_rdns_cache.py`](../../tests/utils/test_rdns_cache.py)
- E2E: `Security Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts)
