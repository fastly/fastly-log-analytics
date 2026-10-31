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

Bot enrichment uses the PostgreSQL `ngwaf_bots` and `rdns` tables. Standard mode uses the `security_*` and verified-bot rollups where eligible with raw fallback. High-scale mode ([`backend/high_scale/security.py`](../../backend/high_scale/security.py)) reads `request_facts` with the request filters bound (unfiltered NGWAF bot cards read `security_minute_dimensions`); NGWAF names are resolved at ingest; a ClickHouse failure propagates as an error.

## Tests

- Frontend unit: [`frontend/__tests__/app/security.test.tsx`](../../frontend/__tests__/app/security.test.tsx)
- Backend: [`tests/repositories/test_security.py`](../../tests/repositories/test_security.py), [`tests/core/test_rollups_security_and_bots.py`](../../tests/core/test_rollups_security_and_bots.py), [`tests/utils/test_rdns_cache.py`](../../tests/utils/test_rdns_cache.py)
- E2E: `Security Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts)
