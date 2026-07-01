"""SQL templates for `backend.repositories.insights`.

Phase 5b extraction. 28 per-insight templates registered with the
``InsightsRegistry`` plus 2 coalesced multi-insight pre-aggregation
queries used by ``repository.py``.

definitions.py shape decision (Phase 5b §5b.5 open question)
============================================================
Chose **(c) stay as code** — no per-section split, no YAML/TOML
data-driven conversion. The "next contributor adds one new insight"
lens drove the call:

- Per-insight processors are NOT mechanically identical. Each one
  unpacks a different row tuple schema, applies custom severity
  thresholds, and several override ``severity_logic`` or inject
  meta fields that don't fit a generic shape (e.g. ``NEW_PROBE_REGEX``
  is an f-string-built regex baked into the template; the impossible-
  distance processor has a 14-column row; cipher_spread, asn_concentration,
  region_latency each have bespoke severity rules). A data-driven
  shape would need a callable indirection per insight anyway, leaving
  the YAML as a duplicate index of what's already in code.
- (a) split-by-section would push the contributor to wire up one new
  file plus an import in ``__init__.py``, for no readability win
  over the existing ``# ── N. Name ──`` section comments.
- (c) keeps the contributor's diff to "add ``def foo_processor``,
  add one ``registry.register(InsightDefinition(...))`` block, add the
  SQL constant to this module" — three colocated edits, no new files.

SQL constants below are grouped by source file (``definitions.py``
first, ``repository.py`` coalesced pre-aggs last) so a reader who
opens this module sees the registry-driven templates and the manually-
invoked coalesced queries in the same order the repository runs them.

All ``{...}`` placeholders are trusted-identifier or trusted-fragment
substitutions (table names, validated column projections, scalar
floats/ints from the repository call site). User-supplied window
bounds are bound through DuckDB ``?`` parameters by the caller; this
module never interpolates user input.

See ``backend/repositories/_sql/__init__.py`` for the ownership policy.
"""

from __future__ import annotations

import re

# ── Probe-URL regex (used by NEW_PROBE_URLS template) ─────────────────────────
# Plain alternation — no inline ``(?i)`` flag because the literal ``?``
# breaks the repository's ``sql.count("?")`` placeholder-counting heuristic.
# Case-insensitivity is supplied via the third ``regexp_matches`` arg below.
NEW_PROBES = [
    "admin",
    ".env",
    ".git",
    "wp-",
    "phpmyadmin",
    "config",
    "backup",
    "shell",
    "passwd",
    "xmlrpc",
    "actuator",
    "console",
    "cgi-bin",
    ".php",
    ".asp",
    "../../",
    "swagger",
    "api-docs",
    "graphql",
    "debug",
]
NEW_PROBE_REGEX = "|".join(re.escape(p) for p in NEW_PROBES)


# ════════════════════════════════════════════════════════════════════════════
# Templates from ``definitions.py`` — one per registered InsightDefinition
# ════════════════════════════════════════════════════════════════════════════

# ── 1. Error Spikes ───────────────────────────────────────────────────────────

ERROR_SPIKES = """
        WITH base AS (
            SELECT "url", status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
        )
        SELECT "url",
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 3 AND w_rate >= 0.05 AND (b_total < 10 OR w_rate >= b_rate * 2 + 0.05)
        ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
    """

# ── 2. Botnet Grouping ────────────────────────────────────────────────────────

BOTNET_GROUPING = """
        WITH base AS (
            SELECT "{fp_col}", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "{fp_col}" IS NOT NULL AND "{fp_col}" != ''
        )
        SELECT "{fp_col}",
            COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
            COUNT(*) FILTER (WHERE is_w) AS w_reqs,
            COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips,
            w_ips * 1.0 / GREATEST(COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1) AS ip_ratio
        FROM base GROUP BY "{fp_col}"
        HAVING w_ips >= 5 AND w_ips > COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY ip_ratio DESC LIMIT 10
    """

# ── 4. New Country Traffic ────────────────────────────────────────────────────

NEW_COUNTRY_TRAFFIC = """
        SELECT "country",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {table_name}
        WHERE "country" IS NOT NULL
        GROUP BY "country"
        HAVING w_cnt >= 3 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """

# ── 5. City Traffic Surges ────────────────────────────────────────────────────

CITY_SURGES = """
        SELECT {label_expr} AS label, "city", {region_sel}, {country_sel},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            w_cnt * 1.0 / GREATEST(COALESCE(b_cnt, 0) * 1.0 / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1.0) AS spike_ratio
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {loc_cols}, label, "city", {region_sel}, {country_sel}
        HAVING w_cnt >= 20 AND w_cnt > COALESCE(b_cnt, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY spike_ratio DESC LIMIT 15
    """

# ── 6. City Error Spikes ──────────────────────────────────────────────────────

CITY_ERROR_SPIKES = """
        WITH base AS (
            SELECT {loc_cols}, {label_expr} AS label, status, "city", {region_sel} AS region, {country_sel} AS country,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "city" IS NOT NULL AND "city" != ''
        )
        SELECT label, "city", region, country,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY ALL
        HAVING w_total >= 10 AND w_rate >= 0.10 AND (b_total < 50 OR w_rate >= b_rate * 3 + 0.05)
        ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
    """

# ── 7. City Latency Regressions ───────────────────────────────────────────────

CITY_LATENCY_REGRESSIONS = """
        WITH base AS (
            SELECT {loc_cols}, {label_expr} AS label, elapsed, "city", {region_sel} AS region, {country_sel} AS country,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "city" IS NOT NULL AND "city" != '' AND elapsed IS NOT NULL
        )
        SELECT label, "city", region, country,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY ALL
        HAVING w_total >= 10 AND b_total >= 50 AND w_p95 >= b_p95 * 3.0 AND w_p95 - b_p95 >= 500
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """

# ── 8. New City Traffic ───────────────────────────────────────────────────────

NEW_CITY_TRAFFIC = """
        SELECT {label_expr} AS label, "city", {region_sel}, {country_sel},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {loc_cols}, label, "city", {region_sel}, {country_sel}
        HAVING w_cnt >= 5 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """

# ── 9. User-Agent Monoculture ─────────────────────────────────────────────────

UA_MONOCULTURE = """
        SELECT "ua",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS b_total,
            (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS w_total
        FROM {table_name} GROUP BY "ua"
        HAVING w_total > 0 AND w_cnt * 1.0 / w_total >= 0.25 AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """

# ── 10. New Probe URLs ────────────────────────────────────────────────────────
# Uses NEW_PROBE_REGEX (above). f-string-baked into the template at module
# import time — the regex is a fixed literal, not user input.

NEW_PROBE_URLS = f"""
        SELECT "url",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            AVG(CASE WHEN "status" >= 400 THEN 1.0 ELSE 0.0 END) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) * 100 AS w_error_pct
        FROM {{table_name}}
        WHERE "url" IS NOT NULL AND (regexp_matches("url", '{NEW_PROBE_REGEX}', 'i'))
        GROUP BY "url"
        HAVING w_cnt > 0 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 25
    """

# ── 11. WAF Signal Spikes ─────────────────────────────────────────────────────

WAF_SIGNAL_SPIKES = """
        SELECT signal,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            w_cnt * 1.0 / GREATEST(COALESCE(b_cnt, 0) * 1.0 / {baseline_hours} * {window_hours}, 0.5) AS spike_ratio
        FROM {waf_table}
        GROUP BY signal
        HAVING w_cnt >= 3 AND w_cnt > COALESCE(b_cnt, 0) * 1.0 / {baseline_hours} * {window_hours} * 2 + 2
        ORDER BY spike_ratio DESC LIMIT 15
    """

# ── 12. Proxy / VPN Surge ─────────────────────────────────────────────────────

PROXY_SURGE = """
        WITH base AS (
            SELECT "p_type",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {table_name} WHERE "p_type" IS NOT NULL AND "p_type" != '' GROUP BY "p_type"
        ),
        totals AS (
            SELECT
                SUM(w_cnt) AS w_proxy_total,
                SUM(b_cnt) AS b_proxy_total,
                (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name} WHERE "p_type" IS NOT NULL) AS w_total_all,
                (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name} WHERE "p_type" IS NOT NULL) AS b_total_all
            FROM base
        )
        SELECT b."p_type", b.w_cnt, b.b_cnt, t.w_total_all, t.b_total_all
        FROM base b, totals t
        WHERE (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= 5
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= (t.b_proxy_total * 100.0 / NULLIF(t.b_total_all, 0)) * 2 + 5
    """

# ── 13. ASN Concentration ─────────────────────────────────────────────────────

ASN_CONCENTRATION = """
        SELECT "asn",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS b_total,
            (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS w_total
        FROM {table_name} WHERE "asn" IS NOT NULL GROUP BY "asn"
        HAVING w_total > 0 AND w_cnt * 1.0 / w_total >= 0.20 AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """

# ── 14. ASN/Metro Performance Regressions ─────────────────────────────────────

ASN_METRO_PERFORMANCE = """
        WITH base AS (
            SELECT "asn", "metro", tcp_rtt,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "asn" IS NOT NULL AND "metro" IS NOT NULL AND tcp_rtt > 0 AND "country" = 'US'
        )
        SELECT "asn", "metro",
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tcp_rtt) FILTER (WHERE is_w) / 1000.0 AS w_med,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY tcp_rtt) FILTER (WHERE is_b) / 1000.0 AS b_med,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "asn", "metro"
        HAVING w_total >= 20 AND b_total >= 50 AND w_med >= b_med * 1.5 AND w_med - b_med >= 20
        ORDER BY (w_med - b_med) DESC LIMIT 15
    """

# ── 15. Cache Efficiency Collapse ─────────────────────────────────────────────

CACHE_COLLAPSE = """
        WITH base AS (
            SELECT "url", cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
        )
        SELECT "url",
            SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(SUM(CASE WHEN cache ILIKE 'HIT%' OR cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(SUM(CASE WHEN cache ILIKE 'HIT%' OR cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN cache ILIKE 'HIT%' OR cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_total,
            SUM(CASE WHEN cache ILIKE 'HIT%' OR cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 5 AND b_total >= 20 AND b_rate >= 0.40 AND w_rate <= b_rate - 0.20 AND w_rate <= b_rate * 0.6
        ORDER BY (COALESCE(b_rate, 0) - w_rate) DESC LIMIT 15
    """

# Cache hit ratio above is HIT/(HIT+MISS) — the conventional Fastly definition.
# PASS is uncacheable and excluded from both numerator and denominator (a PASS
# surge is surfaced by CACHEABILITY_REGRESSION below, not as a hit-rate drop).
# w_total/b_total are the cacheable sample size (HIT+MISS), so the >=5/>=20 gate
# requires enough *cacheable* traffic to judge a ratio. Keep this in lockstep
# with the coalesced cache_collapse branch in repository.py.

# ── 15b. Cacheability Regression ──────────────────────────────────────────────
# Sibling to cache_collapse: detects URLs that flipped from cacheable to mostly
# PASS (origin started sending Set-Cookie / Cache-Control: private, a varying
# query param, or a VCL `pass`). pass_rate = PASS / ALL requests. Fires only on
# a clear cacheable→uncacheable flip with real volume. Keep in lockstep with the
# coalesced cacheability_regression branch in repository.py.

CACHEABILITY_REGRESSION = """
        WITH base AS (
            SELECT "url", cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
        )
        SELECT "url",
            SUM(CASE WHEN cache ILIKE 'PASS%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN cache ILIKE 'PASS%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30
        ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
    """

# ── 16. Latency Regression ────────────────────────────────────────────────────

LATENCY_REGRESSION = """
        WITH base AS (
            SELECT "url", elapsed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE elapsed IS NOT NULL
        )
        SELECT "url",
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 5 AND b_total >= 20 AND w_p95 >= b_p95 * 2.0 AND w_p95 - b_p95 >= 200
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """

# ── 17. Impossible Distance / Spoofing ────────────────────────────────────────

IMPOSSIBLE_DISTANCE = """
        WITH pop_coords(pop_code, pop_lat, pop_lon) AS (VALUES {pop_values}),
        flagged AS (
            SELECT t."{fp_col}" AS fp, t."ip", t."pop", ROUND(t."lat"::DOUBLE, 3) AS client_lat, ROUND(t."lon"::DOUBLE, 3) AS client_lon, pc.pop_lat, pc.pop_lon, t."tcp_rtt", t."country", t."city",
                ROUND(2 * 6371 * ASIN(SQRT(POWER(SIN(RADIANS(t."lat"::DOUBLE - pc.pop_lat) / 2), 2) + COS(RADIANS(t."lat"::DOUBLE)) * COS(RADIANS(pc.pop_lat)) * POWER(SIN(RADIANS(t."lon"::DOUBLE - pc.pop_lon) / 2), 2))), 1) AS distance_km,
                ROUND((t."tcp_rtt"::DOUBLE / 2.0 / 1e6) * 200000 * 2, 1) AS max_km
            FROM {table_name} t JOIN pop_coords pc ON t."pop" = pc.pop_code
            WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND t."lat" IS NOT NULL AND t."lon" IS NOT NULL AND t."tcp_rtt" IS NOT NULL AND t."tcp_rtt" > 0 AND t."{fp_col}" IS NOT NULL AND t."{fp_col}" != '' {edge_filter}
        )
        SELECT fp, COUNT(*) AS hits, MAX(distance_km - max_km) AS worst_excess_km, MAX(distance_km) AS max_dist_km, MIN(max_km) AS min_allowed_km, ANY_VALUE(pop) AS pop, ANY_VALUE(ip) AS sample_ip, ANY_VALUE(client_lat), ANY_VALUE(client_lon), ANY_VALUE(pop_lat), ANY_VALUE(pop_lon), ANY_VALUE(tcp_rtt), ANY_VALUE(country), ANY_VALUE(city)
        FROM flagged WHERE distance_km > max_km GROUP BY fp HAVING COUNT(*) >= 2 ORDER BY worst_excess_km DESC LIMIT 15
    """

# ── 18. Tail Latency Anomaly ──────────────────────────────────────────────────

TAIL_LATENCY = """
        SELECT "url",
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "elapsed") / 1000.0, 0) AS p99_ms,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "elapsed") / 1000.0, 0) AS p50_ms,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "elapsed") / NULLIF(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "elapsed"), 0), 1) AS ratio,
            COUNT(*) AS total
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "elapsed" IS NOT NULL
        GROUP BY "url" HAVING COUNT(*) >= 20 AND ratio > 5
        ORDER BY ratio DESC LIMIT 15
    """

# ── 19. Cipher Fingerprint Clustering ─────────────────────────────────────────

CIPHER_SPREAD = """
        WITH base AS (
            SELECT "tls_ciphers_sha", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "tls_ciphers_sha" IS NOT NULL AND "tls_ciphers_sha" != ''
        )
        SELECT "tls_ciphers_sha",
            COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
            COUNT(*) FILTER (WHERE is_w) AS w_reqs,
            COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
        FROM base GROUP BY "tls_ciphers_sha"
        HAVING w_ips >= 5 AND w_ips > COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
        ORDER BY (w_ips * 1.0 / GREATEST(COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1)) DESC LIMIT 10
    """

# ── 20. Request Size Anomaly ──────────────────────────────────────────────────

REQUEST_SIZE_ANOMALY = """
        WITH base AS (
            SELECT "ip", req_header_bytes,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE req_header_bytes > 0
        ),
        stats AS (
            SELECT "ip",
                MAX(req_header_bytes) FILTER (WHERE is_w) AS max_bytes,
                AVG(req_header_bytes) FILTER (WHERE is_w) AS avg_bytes,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY req_header_bytes) FILTER (WHERE is_b) AS b_p95
            FROM base GROUP BY "ip"
        )
        SELECT "ip", max_bytes, avg_bytes, w_total, b_p95 FROM stats
        WHERE w_total >= 3 AND max_bytes > b_p95 * 3
        ORDER BY max_bytes DESC LIMIT 15
    """

# ── 21. Connection Reuse Anomaly ──────────────────────────────────────────────

CONNECTION_ABUSE = """
        WITH base AS (
            SELECT "ip", conn_requests,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE conn_requests > 0
        ),
        stats AS (
            SELECT "ip",
                MAX(conn_requests) FILTER (WHERE is_w) AS max_reqs,
                AVG(conn_requests) FILTER (WHERE is_w) AS avg_reqs,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY conn_requests) FILTER (WHERE is_b) AS b_p95
            FROM base GROUP BY "ip"
        )
        SELECT "ip", max_reqs, avg_reqs, w_total, b_p95 FROM stats
        WHERE w_total >= 5 AND max_reqs > b_p95 * 3 AND max_reqs >= 50
        ORDER BY max_reqs DESC LIMIT 15
    """

# ── 22. Regional Latency Degradation ──────────────────────────────────────────

REGION_LATENCY = """
        WITH base AS (
            SELECT server_region, elapsed, ottfb,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE elapsed IS NOT NULL AND server_region != ''
        ),
        region_stats AS (
            SELECT server_region,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total
            FROM base GROUP BY server_region
            HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
        ),
        origin_stats AS (
            SELECT server_region,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) / 1000.0, 0) AS ottfb_p95
            FROM base WHERE is_w AND ottfb IS NOT NULL
            GROUP BY server_region HAVING COUNT(*) >= 20
        )
        SELECT r.server_region, r.w_p95, r.b_p95, r.w_total, r.b_total, o.ottfb_p95
        FROM region_stats r LEFT JOIN origin_stats o ON r.server_region = o.server_region
        ORDER BY (r.w_p95 / NULLIF(r.b_p95, 0)) DESC LIMIT 15
    """

# ── 23. Cache TTL Inefficiency ────────────────────────────────────────────────

CACHE_TTL_MISMATCH = """
        SELECT {q_col} AS label,
            ROUND(AVG("ttl"), 0) AS avg_ttl,
            ROUND(AVG("hits"), 1) AS avg_hits,
            ROUND(AVG("age"), 0) AS avg_age,
            COUNT(*) AS sample_count
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "ttl" IS NOT NULL AND "ttl" > 0 AND "hits" IS NOT NULL AND "age" IS NOT NULL
        GROUP BY {q_col} HAVING sample_count >= 10 AND AVG("hits") < 2 AND AVG("ttl") > 60
        ORDER BY AVG("ttl") DESC LIMIT 20
    """

# ── 24. Image Optimization Opportunities ──────────────────────────────────────

IMAGE_OPTIMIZATION_OPPORTUNITIES = """
        SELECT "url", COUNT(*) as request_count, SUM("resp_bytes") as total_bytes,
            ROUND(AVG("resp_bytes") / 1024, 1) as avg_kb,
            ({ua_mobile_sel}) AS mobile_ratio
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "status" = 200
          AND ("url" ILIKE '%.jpg%' OR "url" ILIKE '%.jpeg%' OR "url" ILIKE '%.png%' OR "url" ILIKE '%.gif%')
          AND "url" NOT ILIKE '%auto=webp%' AND "url" NOT ILIKE '%format=auto%' AND "url" NOT ILIKE '%format=webp%' AND "url" NOT ILIKE '%format=avif%'
        GROUP BY "url" HAVING total_bytes > 1024 * 512
        ORDER BY total_bytes DESC LIMIT 15
    """

# ── 25. Origin Latency Spike ──────────────────────────────────────────────────

ORIGIN_LATENCY_SPIKE = """
        WITH base AS (
            SELECT ottfb, {url_col} AS url,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE ottfb IS NOT NULL
        ),
        overall_stats AS (
            SELECT
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w) / 1000.0 AS w_p95,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_b) / 1000.0 AS b_p95
            FROM base
        ),
        url_stats AS (
            SELECT url, COUNT(*) AS requests, ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) / 1000.0, 1) AS p95_ms
            FROM base WHERE is_w
            GROUP BY url HAVING requests >= 10
        )
        SELECT u.url, u.p95_ms, o.w_p95, o.b_p95, u.requests
        FROM url_stats u, overall_stats o
        WHERE o.w_p95 > o.b_p95 * 2
        ORDER BY u.p95_ms DESC LIMIT 10
    """

# ── 26. Origin Error Rate ─────────────────────────────────────────────────────

ORIGIN_ERROR_RATE = """
        WITH base AS (
            SELECT "ost" AS status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "ost" IS NOT NULL
        ),
        totals AS (
            SELECT
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total,
                COUNT(*) FILTER (WHERE is_w AND status >= 500) AS w_5xx,
                COUNT(*) FILTER (WHERE is_b AND status >= 500) AS b_5xx
            FROM base
        ),
        by_status AS (
            SELECT status, COUNT(*) FILTER (WHERE is_w) AS w_cnt
            FROM base WHERE is_w AND status >= 500
            GROUP BY status
        )
        SELECT s.status, s.w_cnt, t.w_total, t.b_total, t.w_5xx, t.b_5xx
        FROM by_status s, totals t
        WHERE (t.w_5xx * 100.0 / NULLIF(t.w_total, 0)) >= 1.0
          AND (t.w_5xx * 100.0 / NULLIF(t.w_total, 0)) > (t.b_5xx * 100.0 / NULLIF(t.b_total, 0)) * 2
    """

# ── 27. Origin Retries Elevated ───────────────────────────────────────────────

ORIGIN_RETRIES = """
        SELECT {url_col}, COUNT(*) AS requests, ROUND(AVG("oretries"), 2) AS avg_retries, MAX("oretries") AS max_retries
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "oretries" > 0
        GROUP BY {url_col} HAVING requests >= 5
        ORDER BY avg_retries DESC LIMIT 10
    """

# ── 28. Specific Origin IP Failing ────────────────────────────────────────────

ORIGIN_IP_FAILURE = """
        WITH base AS (
            SELECT "oip", "ost" AS status,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} WHERE "oip" IS NOT NULL AND "oip" != '' AND "ost" IS NOT NULL
        ),
        stats AS (
            SELECT "oip",
                COUNT(*) AS requests,
                ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 1) AS error_pct
            FROM base WHERE is_w
            GROUP BY "oip" HAVING requests >= 10
        ),
        median_calc AS (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY error_pct) AS median_rate FROM stats
        )
        SELECT s.oip, s.requests, s.error_pct, m.median_rate
        FROM stats s, median_calc m
        WHERE s.error_pct > m.median_rate * 3 AND s.error_pct > 5
        ORDER BY s.error_pct DESC
    """

# ── 29. Shield Path Degradation ───────────────────────────────────────────────

SHIELD_PATH_DEGRADATION = """
        WITH logs AS (SELECT "rid", "prid", "pop", "ottfb", "edge", timestamp FROM {table_name} WHERE ottfb IS NOT NULL),
        edge_logs AS (SELECT rid, pop, ottfb, timestamp FROM logs WHERE edge = true),
        shield_logs AS (SELECT prid, pop, ottfb, timestamp FROM logs WHERE edge = false AND prid IS NOT NULL AND prid != ''),
        joined AS (
            SELECT e.pop AS edge_pop, COALESCE(s.pop, 'Direct to Origin') AS shield_pop, (e.ottfb - COALESCE(s.ottfb, 0)) / 1000.0 AS transit_ms, e.timestamp
            FROM edge_logs e LEFT JOIN shield_logs s ON s.prid = e.rid
        )
        SELECT edge_pop, shield_pop,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_p50,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_p50,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt
        FROM joined GROUP BY 1, 2 HAVING w_cnt >= 5 AND w_p50 > b_p50 * 1.5
        ORDER BY (w_p50 / NULLIF(b_p50, 0)) DESC LIMIT 20
    """


# ── 30. Scripted Traffic Patterns (repeated_patterns) ─────────────────────────
# Beaconing / periodic-cadence detection (RITA/Zeek-style) applied to web logs:
# flag client IPs whose inter-arrival gaps are highly regular (scrapers, pollers,
# cron jobs). See local-docs/repeated_pattern_detection_FINAL_plan.md.
#
# Curated, ``?``-FREE crawler/monitor allowlist (mirrors NEW_PROBE_REGEX above).
# ``re.escape`` of plain tokens contains no ``?``, so it can't corrupt the
# repository's ``sql.count("?")`` placeholder-counting heuristic — verified bots
# (Googlebot/UptimeRobot/Pingdom) are perfectly periodic and would dominate the
# card, so they're dropped BEFORE scoring. Do NOT inject
# ``bot_sources.get_bot_regex_pattern()`` here: it begins ``(?i)`` and the ``?``
# would mis-count the single window-start bind. Case-insensitivity comes from the
# ``regexp_matches`` 'i' arg, not an inline ``(?i)`` flag.
_BOT_UA_TOKENS = [
    "googlebot",
    "bingbot",
    "slurp",
    "duckduckbot",
    "baiduspider",
    "yandexbot",
    "applebot",
    "uptimerobot",
    "pingdom",
    "statuscake",
    "datadoghq",
    "newrelic",
    "ahrefsbot",
    "semrushbot",
    "gptbot",
    "claudebot",
    "ccbot",
    "facebookexternalhit",
    "bot",
    "crawler",
    "spider",
    "monitor",
]
REPEATED_BOT_UA_REGEX = "|".join(re.escape(t) for t in _BOT_UA_TOKENS)

# Timestamps are 1-SECOND granularity (verified: 0/1105 rows carry a sub-second
# component), so we work in integer seconds. The CV is Sheppard-corrected
# (rounding to whole seconds adds ≈ 1/12 s² of variance) and gated only above a
# 5 s mean where it's trustworthy; below 5 s a modal-dominance fast path carries
# the verdict. ``MODE()`` MUST be aliased (bare ``mode`` is reserved). med/q1/q3
# are computed for the deferred Bowley confirmation term (plan §2) but not yet
# surfaced. Exactly ONE ``?`` (the window start) — keep {bot_ua_regex} ?-free.
REPEATED_PATTERNS = """
    WITH kept AS (
        SELECT "ip" AS ip, epoch("timestamp")::BIGINT AS sec, {ua_col} AS ua
        FROM {table_name}
        WHERE "timestamp" >= CAST(? AS TIMESTAMPTZ)
          AND "ip" IS NOT NULL AND "ip" <> ''
          AND ({ua_col} IS NULL OR NOT regexp_matches({ua_col}, '{bot_ua_regex}', 'i'))
    ),
    secs AS (  -- distinct active seconds (collapses same-second parallel bursts)
        SELECT ip, sec FROM kept GROUP BY ip, sec
    ),
    meta AS (  -- informational only (NOT gated): UA diversity, event count, span
        SELECT ip, COUNT(DISTINCT ua) AS distinct_ua, COUNT(*) AS n_events,
               (MAX(sec) - MIN(sec)) AS span_s
        FROM kept GROUP BY ip
    ),
    gaps AS (
        SELECT ip, sec - LAG(sec) OVER (PARTITION BY ip ORDER BY sec) AS gap FROM secs
    ),
    g AS (SELECT ip, gap FROM gaps WHERE gap IS NOT NULL),
    agg AS (
        SELECT ip, COUNT(*) AS n_gaps, AVG(gap) AS mean_gap, VAR_SAMP(gap) AS var_gap,
               STDDEV_SAMP(gap) AS sd_gap, MEDIAN(gap) AS med, MODE(gap) AS mode_gap,
               QUANTILE_CONT(gap,0.25) AS q1, QUANTILE_CONT(gap,0.75) AS q3
        FROM g GROUP BY ip          -- NOTE: alias MODE() as mode_gap; bare `mode` is reserved
    ),
    modal AS (
        SELECT g.ip, AVG(CASE WHEN abs(g.gap - a.mode_gap) <= 1 THEN 1.0 ELSE 0.0 END) AS modal_frac
        FROM g JOIN agg a USING(ip) GROUP BY g.ip
    )
    SELECT a.ip, a.n_gaps, d.n_events,
           ROUND(a.mean_gap, 2)  AS avg_interval,
           ROUND(a.sd_gap, 2)    AS stddev_interval,
           ROUND(sqrt(GREATEST(a.var_gap - 1.0/12.0, 0)) / NULLIF(a.mean_gap, 0), 3) AS cv_corr,
           ROUND(m.modal_frac, 3) AS modal_frac,
           d.distinct_ua, d.span_s, a.mode_gap
    FROM agg a JOIN modal m USING(ip) JOIN meta d USING(ip)
    WHERE a.n_gaps >= 12
      AND (d.n_events * 1.0 / NULLIF(d.span_s, 0)) < 2.0          -- rps gate (no UA hard-gate, D4)
      AND (
            (a.mean_gap >= 5  AND sqrt(GREATEST(a.var_gap - 1.0/12.0,0))/NULLIF(a.mean_gap,0) < 0.3
                              AND m.modal_frac >= 0.6)               -- slow path: CV + modal
         OR (a.mean_gap >= 1  AND a.mean_gap < 5 AND m.modal_frac >= 0.85)   -- fast path: modal only
          )
    ORDER BY m.modal_frac DESC, a.n_gaps DESC
    LIMIT 15
"""


# ════════════════════════════════════════════════════════════════════════════
# Templates from ``repository.py`` — multi-insight coalesced pre-aggregations
# ════════════════════════════════════════════════════════════════════════════

# ── Coalesced city aggregates ────────────────────────────────────────────────
# ONE pass over the temp table that computes the superset of counts / rates /
# p95s for the 4 city-keyed insights (city_surges, city_error_spikes,
# city_latency_regressions, new_city_traffic). The Python caller demuxes the
# rows into per-insight schemas (see ``_coalesced_city_aggregates`` in
# repository.py for the row-schema docstring).

COALESCED_CITY_AGGREGATES = """
    WITH base AS (
        SELECT
            "city",
            {region_sel} AS region,
            {country_sel} AS country,
            {label_expr} AS label,
            status,
            elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
    )
    SELECT
        label, "city", region, country,
        COUNT(*) FILTER (WHERE is_w) AS w_cnt,
        COUNT(*) FILTER (WHERE is_b) AS b_cnt,
        SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors_4xx,
        SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_errors_4xx,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total
    FROM base
    GROUP BY ALL
    """

# ── Coalesced URL aggregates ─────────────────────────────────────────────────
# ONE pass over the temp table that computes the superset for the 4 URL-keyed
# insights (error_spikes, cache_collapse, latency_regression, tail_latency).
# origin_latency_spike is the 5th URL-keyed insight but has a different shape
# (normalized against the whole-population p95) — kept on its own template.

COALESCED_URL_AGGREGATES = """
    WITH base AS (
        SELECT
            "url",
            status,
            cache,
            elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
        WHERE "url" IS NOT NULL
    )
    SELECT
        "url",
        -- Common counts
        COUNT(*) FILTER (WHERE is_w) AS w_total,
        COUNT(*) FILTER (WHERE is_b) AS b_total,
        -- error_spikes: 5xx counters
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_5xx,
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_5xx,
        -- cache_collapse: cache-hit counters (HIT-only numerator; cacheable
        -- denominator = HIT+MISS, computed from w_miss/b_miss below — PASS is
        -- excluded because it was never eligible to cache)
        SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
        SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
        -- cache_collapse denominator + cacheability_regression: MISS / PASS counters
        SUM(CASE WHEN cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
        SUM(CASE WHEN cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
        SUM(CASE WHEN cache ILIKE 'PASS%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
        SUM(CASE WHEN cache ILIKE 'PASS%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
        -- latency_regression: elapsed-only counts + p95s in MILLISECONDS
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        -- tail_latency: window-only p99/p50 (rounded to whole ms to match
        -- the legacy template's output exactly)
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY elapsed)
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p99,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY elapsed)
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p50
    FROM base
    GROUP BY "url"
    HAVING (COUNT(*) FILTER (WHERE is_w) > 0) OR (COUNT(*) FILTER (WHERE is_b) > 0)
    """


__all__ = [
    "NEW_PROBES",
    "NEW_PROBE_REGEX",
    # definitions.py templates
    "ERROR_SPIKES",
    "BOTNET_GROUPING",
    "NEW_COUNTRY_TRAFFIC",
    "CITY_SURGES",
    "CITY_ERROR_SPIKES",
    "CITY_LATENCY_REGRESSIONS",
    "NEW_CITY_TRAFFIC",
    "UA_MONOCULTURE",
    "NEW_PROBE_URLS",
    "WAF_SIGNAL_SPIKES",
    "PROXY_SURGE",
    "ASN_CONCENTRATION",
    "ASN_METRO_PERFORMANCE",
    "CACHE_COLLAPSE",
    "CACHEABILITY_REGRESSION",
    "LATENCY_REGRESSION",
    "IMPOSSIBLE_DISTANCE",
    "TAIL_LATENCY",
    "CIPHER_SPREAD",
    "REQUEST_SIZE_ANOMALY",
    "CONNECTION_ABUSE",
    "REGION_LATENCY",
    "CACHE_TTL_MISMATCH",
    "IMAGE_OPTIMIZATION_OPPORTUNITIES",
    "ORIGIN_LATENCY_SPIKE",
    "ORIGIN_ERROR_RATE",
    "ORIGIN_RETRIES",
    "ORIGIN_IP_FAILURE",
    "SHIELD_PATH_DEGRADATION",
    "REPEATED_BOT_UA_REGEX",
    "REPEATED_PATTERNS",
    # repository.py templates
    "COALESCED_CITY_AGGREGATES",
    "COALESCED_URL_AGGREGATES",
]
