"""SQL templates for `backend.repositories.insights`.

Phase 5b extraction. 28 per-insight templates registered with the
``InsightsRegistry`` plus coalesced multi-insight pre-aggregation
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
  The ``InsightDefinition`` also requires a ``category=InsightCategory.*``
  ({security, origin, edge, traffic}) — the field has no default so a
  forgotten category fails at import; it drives the sectioned /insights
  page. Mirror the same category in the ``INSIGHT_DEFINITIONS`` dict in
  ``backend/core/_log_fields_data.py`` (a separate availability catalog).

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


# ── 31. Low-and-Slow Scans (low_and_slow) ─────────────────────────────────────
# Phase-3 promotion of the legacy ``low_and_slow`` stub. Flags a client IP that
# touches MANY distinct sensitive/vuln paths (reuses the NEW_PROBE_REGEX
# allow-list) at a DELIBERATELY LOW request rate spread over a long span — the
# classic rate-limit-evasion signature that volumetric WAF rules miss.
#
# Scans the WHOLE retained temp (baseline_start..now), NOT just the window: the
# defining trait is sustained low-volume probing over hours/days, so there is no
# window/baseline split and therefore **zero ``?`` binds** (the caller's
# ``sql.count("?")`` heuristic must see none). The NEW_PROBE_REGEX is f-string-
# baked at import (a fixed literal, ``?``-free via re.escape) exactly like
# NEW_PROBE_URLS; ``{{table_name}}`` is escaped for runtime hydration.
LOW_AND_SLOW = f"""
        WITH base AS (
            SELECT "ip" AS ip, "url" AS url, epoch("timestamp")::BIGINT AS sec
            FROM {{table_name}}
            WHERE "ip" IS NOT NULL AND "ip" <> ''
              AND "url" IS NOT NULL
              AND regexp_matches("url", '{NEW_PROBE_REGEX}', 'i')
        )
        SELECT ip,
            COUNT(*) AS hits,
            COUNT(DISTINCT url) AS distinct_paths,
            (MAX(sec) - MIN(sec)) AS span_s,
            ROUND(COUNT(*) * 1.0 / NULLIF(MAX(sec) - MIN(sec), 0), 5) AS rps
        FROM base
        GROUP BY ip
        HAVING COUNT(*) >= 5
           AND COUNT(DISTINCT url) >= 3
           AND (MAX(sec) - MIN(sec)) >= 600
           AND COUNT(*) * 1.0 / NULLIF(MAX(sec) - MIN(sec), 0) < 0.2
        ORDER BY distinct_paths DESC, span_s DESC
        LIMIT 15
    """

# ── 32. Credential Enumeration / Brute Force (credential_enumeration) ──────────
# 401/403 spike per client IP on authentication paths (login / signin / auth /
# oauth / token / sso / password reset, etc). Window-vs-baseline: fires when an
# IP's window denied-count is high, dominates its own auth traffic (mostly
# failures = guessing, not a logged-in user), AND exceeds its baseline-normalised
# rate. AUTH_PATHS_REGEX is f-string-baked (``?``-free via re.escape); the two
# ``?`` binds are BOTH the window start (is_b/is_w split), matching every other
# baseline/window template. {baseline_hours}/{window_hours} normalise the
# longer baseline span, mirroring BOTNET_GROUPING / CIPHER_SPREAD.
_AUTH_PATHS = [
    "login",
    "signin",
    "sign-in",
    "log-in",
    "auth",
    "oauth",
    "openid",
    "token",
    "sso",
    "saml",
    "session",
    "account",
    "password",
    "passwd",
    "passwrd",
    "credential",
    "wp-login",
    "xmlrpc",
    "mfa",
    "otp",
    "2fa",
    "verify",
]
# Plain alternation — no inline ``(?i)`` flag (the literal ``?`` would corrupt
# the repository's ``sql.count("?")`` window-bind counting heuristic). Case-
# insensitivity comes from the ``regexp_matches`` 'i' arg below.
AUTH_PATHS_REGEX = "|".join(re.escape(p) for p in _AUTH_PATHS)

CREDENTIAL_ENUMERATION = f"""
        WITH base AS (
            SELECT "ip" AS ip, "url" AS url, status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE "ip" IS NOT NULL AND "ip" <> ''
              AND "url" IS NOT NULL
              AND regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i')
        )
        SELECT ip,
            COUNT(*) FILTER (WHERE is_w AND status IN (401, 403)) AS w_denied,
            COUNT(*) FILTER (WHERE is_w) AS w_attempts,
            COUNT(DISTINCT url) FILTER (WHERE is_w) AS w_paths,
            COUNT(*) FILTER (WHERE is_b AND status IN (401, 403)) AS b_denied
        FROM base
        GROUP BY ip
        HAVING w_denied >= 20
           AND w_denied * 1.0 / NULLIF(w_attempts, 0) >= 0.5
           AND w_denied > COALESCE(b_denied, 0) / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3 + 5
        ORDER BY w_denied DESC
        LIMIT 15
    """

# ── 33. Network Path (ASN) Health (network_asn_health) ─────────────────────────
# Phase-3 promotion of the legacy ``network_asn_health`` stub. Per-ASN TCP
# connection-quality degradation: window-vs-baseline packet loss (ploss), jitter
# (rtt_var P95, µs) and retransmissions (retrans avg). Fires when an ASN's window
# packet loss OR jitter OR retransmit rate is materially worse than its own
# baseline, with enough samples on both sides to be trustworthy. Two ``?`` binds
# = the window start (is_b/is_w split). rtt_var is in microseconds; the 20 000 µs
# floor is 20 ms of added jitter. row[0] is the ASN so the repository's asn_names
# hydration (triggered by "asn" in the insight id) resolves ISP names.
NETWORK_ASN_HEALTH = """
        WITH base AS (
            SELECT "asn" AS asn, ploss, rtt_var, retrans,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "asn" IS NOT NULL
        )
        SELECT asn,
            AVG(ploss) FILTER (WHERE is_w) AS w_ploss,
            AVG(ploss) FILTER (WHERE is_b) AS b_ploss,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_w) AS w_jitter,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_b) AS b_jitter,
            AVG(retrans) FILTER (WHERE is_w) AS w_retrans,
            AVG(retrans) FILTER (WHERE is_b) AS b_retrans,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base
        GROUP BY asn
        HAVING w_total >= 50 AND b_total >= 100
           AND (
                (COALESCE(w_ploss, 0) >= 0.02 AND COALESCE(w_ploss, 0) >= COALESCE(b_ploss, 0) * 2 + 0.005)
             OR (w_jitter >= b_jitter * 2 AND w_jitter - b_jitter >= 20000)
             OR (COALESCE(w_retrans, 0) >= COALESCE(b_retrans, 0) * 2 + 0.5)
           )
        ORDER BY COALESCE(w_ploss, 0) DESC, w_jitter DESC
        LIMIT 15
    """


# ── 34. 404 Content-Discovery Scanning (content_discovery) ─────────────────────
# Per client IP: window-vs-baseline 404 enumeration. Fires when an IP's window
# 404-count is high, its 404s dominate its own traffic (mostly-404 = directory /
# endpoint brute-forcing, not one broken link), AND it hit MANY distinct 404 URLs
# (scanning, not a single stale asset). Two ``?`` binds = the window start
# (is_b/is_w split), matching every other baseline/window template. Row[0] is the
# client IP so the repository masks it for analysts (mask_ips → mask_ip). The
# distinct-404-URL gate is the discriminator vs. a legit broken deploy, so no
# baseline gate is needed; b_404 is carried for display context only.
CONTENT_DISCOVERY = """
        WITH base AS (
            SELECT "ip" AS ip, "url" AS url, status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "ip" IS NOT NULL AND "ip" <> ''
              AND "url" IS NOT NULL
        )
        SELECT ip,
            COUNT(*) FILTER (WHERE is_w AND status = 404) AS w_404,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(DISTINCT url) FILTER (WHERE is_w AND status = 404) AS w_distinct_404,
            COUNT(*) FILTER (WHERE is_b AND status = 404) AS b_404
        FROM base
        GROUP BY ip
        HAVING COUNT(*) FILTER (WHERE is_w AND status = 404) >= 20
           AND COUNT(*) FILTER (WHERE is_w AND status = 404) * 1.0
               / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) >= 0.7
           AND COUNT(DISTINCT url) FILTER (WHERE is_w AND status = 404) >= 15
        ORDER BY w_404 DESC
        LIMIT 15
    """


# ── 35. Referer Monoculture (referer_monoculture) ─────────────────────────────
# Track B: a single (usually new/rare) Referer driving an outsized share of
# window traffic vs baseline — scraper/embed/hotlink campaigns or a spoofed
# referer flood. Mirrors UA_MONOCULTURE with a 20% share floor. Row:
# [referer, w_cnt, b_cnt, b_total, w_total]. 4 ``?`` (all window start).
REFERER_MONOCULTURE = """
        SELECT "referer",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS b_total,
            (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS w_total
        FROM {table_name} WHERE "referer" IS NOT NULL AND "referer" != '' GROUP BY "referer"
        HAVING w_total > 0 AND w_cnt * 1.0 / w_total >= 0.20 AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """

# ── 36. HTTP Method Drift (method_drift) ──────────────────────────────────────
# Track B: a write/verb method (anything but GET/HEAD/OPTIONS) surging to an
# outsized share of window traffic vs a read-dominated baseline — API abuse,
# form/credential POST floods, unexpected PUT/DELETE. Row:
# [method, w_cnt, b_cnt, w_total, b_total]. 4 ``?`` (all window start).
METHOD_DRIFT = """
        SELECT "method",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS w_total,
            (SELECT COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) FROM {table_name}) AS b_total
        FROM {table_name}
        WHERE "method" IS NOT NULL AND "method" != '' AND upper("method") NOT IN ('GET', 'HEAD', 'OPTIONS')
        GROUP BY "method"
        HAVING w_total > 0 AND w_cnt >= 20 AND w_cnt * 1.0 / w_total >= 0.10 AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 2 + 0.05)
        ORDER BY w_cnt DESC LIMIT 10
    """

# ── 37. New ASN Traffic (new_asn_traffic) ─────────────────────────────────────
# Track B: an ASN (ISP/datacenter) with zero baseline presence now sending
# meaningful traffic — mirrors new_country_traffic / new_city_traffic. ASN-keyed
# → network section; row[0] is the ASN so asn_names hydration resolves the ISP
# label. Row: [asn, w_cnt, b_cnt]. 2 ``?`` (all window start).
NEW_ASN_TRAFFIC = """
        SELECT "asn",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {table_name} WHERE "asn" IS NOT NULL GROUP BY "asn"
        HAVING w_cnt >= 20 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """

# ── 38. Metro Delivery-Rate Degradation (metro_delivery_degradation) ───────────
# Track B: per-US-metro (DMA) median kernel TCP delivery rate (bytes/sec) that
# halved (or worse) window-vs-baseline — a regional last-mile / peering
# degradation. "metro" in the id triggers the repository's dma_map hydration for
# labels. Row: [metro, w_med, b_med, w_total, b_total]. 2 ``?`` (window start).
METRO_DELIVERY_DEGRADATION = """
        WITH base AS (
            SELECT "metro" AS metro, delivery_rate,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "metro" IS NOT NULL AND delivery_rate > 0
        )
        SELECT metro,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_w) AS w_med,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_b) AS b_med,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY metro
        HAVING w_total >= 20 AND b_total >= 50 AND w_med > 0 AND b_med > 0 AND w_med <= b_med * 0.5
        ORDER BY (b_med - w_med) DESC LIMIT 15
    """

# ── 39. Connection-Type Mix Shift (connection_type_mix) ───────────────────────
# Track B: a client connection (c_type / c_speed) combo surging to an outsized,
# spiking share of typed traffic — e.g. a cellular/datacenter mix swing that
# signals a bot pool or a routing change. Row:
# [c_type, c_speed, w_cnt, b_cnt, w_total, b_total]. 2 ``?`` (window start).
CONNECTION_TYPE_MIX = """
        WITH base AS (
            SELECT "c_type" AS c_type, "c_speed" AS c_speed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "c_type" IS NOT NULL AND "c_type" != ''
        )
        SELECT c_type, c_speed,
            COUNT(*) FILTER (WHERE is_w) AS w_cnt,
            COUNT(*) FILTER (WHERE is_b) AS b_cnt,
            (SELECT COUNT(*) FILTER (WHERE is_w) FROM base) AS w_total,
            (SELECT COUNT(*) FILTER (WHERE is_b) FROM base) AS b_total
        FROM base GROUP BY c_type, c_speed
        HAVING w_cnt >= 20 AND (SELECT COUNT(*) FILTER (WHERE is_w) FROM base) > 0
            AND w_cnt * 1.0 / (SELECT COUNT(*) FILTER (WHERE is_w) FROM base) >= 0.15
            AND ((SELECT COUNT(*) FILTER (WHERE is_b) FROM base) = 0
                 OR w_cnt * 1.0 / (SELECT COUNT(*) FILTER (WHERE is_w) FROM base)
                    >= b_cnt * 1.0 / NULLIF((SELECT COUNT(*) FILTER (WHERE is_b) FROM base), 0) * 2 + 0.05)
        ORDER BY w_cnt DESC LIMIT 15
    """

# ── 40. PoP Latency Regression (pop_latency_regression) ───────────────────────
# Track B: per-Fastly-PoP P95 edge latency (elapsed µs → ms) regression vs
# baseline — finer than region/city, isolates a single datacenter's serving
# slowdown. Row: [pop, w_p95, b_p95, w_total, b_total]. 2 ``?`` (window start).
POP_LATENCY_REGRESSION = """
        WITH base AS (
            SELECT "pop" AS pop, elapsed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "pop" IS NOT NULL AND "pop" != '' AND elapsed IS NOT NULL
        )
        SELECT pop,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY pop
        HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """

# ── 41. HTTP/3 → TCP Fallback Spike (http3_fallback) ──────────────────────────
# Track B: a service-wide drop in QUIC (HTTP/3) share vs a QUIC-healthy baseline
# — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP
# throttling, a client-population shift). Single aggregate row when it fires.
# Row: [w_quic, w_total, b_quic, b_total]. 2 ``?`` (window start).
HTTP3_FALLBACK = """
        WITH base AS (
            SELECT lower("transport") AS transport,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "transport" IS NOT NULL AND "transport" != ''
        )
        SELECT
            COUNT(*) FILTER (WHERE is_w AND transport = 'quic') AS w_quic,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b AND transport = 'quic') AS b_quic,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base
        HAVING COUNT(*) FILTER (WHERE is_w) >= 100 AND COUNT(*) FILTER (WHERE is_b) >= 100
            AND COUNT(*) FILTER (WHERE is_b AND transport = 'quic') * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) >= 0.30
            AND COUNT(*) FILTER (WHERE is_w AND transport = 'quic') * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0)
                <= COUNT(*) FILTER (WHERE is_b AND transport = 'quic') * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) - 0.20
    """

# ── 42. Cache HIT-Ratio Cliff (cache_hit_cliff) ───────────────────────────────
# Track B: a service-wide edge cache HIT ratio (HIT/(HIT+MISS), PASS excluded —
# the conventional Fastly definition, mirroring cache_collapse) that fell off a
# cliff window-vs-baseline. One headline edge card. Single aggregate row when it
# fires. Row: [w_hits, w_cacheable, b_hits, b_cacheable]. 2 ``?`` (window start).
CACHE_HIT_CLIFF = """
        WITH base AS (
            SELECT cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
        ),
        agg AS (
            SELECT
                SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
                SUM(CASE WHEN cache ILIKE 'HIT%' OR cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_cacheable,
                SUM(CASE WHEN cache ILIKE 'HIT%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
                SUM(CASE WHEN cache ILIKE 'HIT%' OR cache ILIKE 'MISS%' THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_cacheable
            FROM base
        )
        SELECT w_hits, w_cacheable, b_hits, b_cacheable
        FROM agg
        WHERE w_cacheable >= 100 AND b_cacheable >= 200
            AND (b_hits * 1.0 / NULLIF(b_cacheable, 0)) >= 0.40
            AND (w_hits * 1.0 / NULLIF(w_cacheable, 0)) <= (b_hits * 1.0 / NULLIF(b_cacheable, 0)) - 0.15
    """


# ════════════════════════════════════════════════════════════════════════════
# Track C — field-gated insights (require the Phase-4 edge log-field additions:
# resp_header_content_encoding, cookie_session, oconnect_ms). These stay empty
# until a service re-provisions to emit the new field AND enough history accrues.
# ════════════════════════════════════════════════════════════════════════════

# ── 43. Payload Compression Regression (payload_compression_regression) ────────
# Compressible responses (text/js/css/json/svg/xml by URL extension, 200, big
# enough to matter) that flipped from compressed (gzip/br/zstd) to served
# UNCOMPRESSED window-vs-baseline — a broken Accept-Encoding path, a VCL
# `unset beresp.http.Content-Encoding`, or an origin regression that inflates
# egress + TTFB. Mirrors CACHEABILITY_REGRESSION's rate-flip shape. Needs the
# `resp_header_content_encoding` field. COMPRESSIBLE_REGEX is f-string-baked
# (``?``-free via re.escape). 2 ``?`` (window start).
_COMPRESSIBLE_EXTS = [".js", ".css", ".html", ".json", ".svg", ".xml", ".txt"]
COMPRESSIBLE_REGEX = "|".join(re.escape(e) for e in _COMPRESSIBLE_EXTS)

PAYLOAD_COMPRESSION_REGRESSION = f"""
        WITH base AS (
            SELECT "url",
                -- "compressed" = the encoding CONTAINS a known token, so a
                -- multi-value ("gzip, br") or whitespace-padded ("gzip ") header
                -- still counts as compressed; empty / "identity" / "none" are
                -- uncompressed.
                (NOT regexp_matches(lower(COALESCE("resp_header_content_encoding", '')), 'gzip|br|zstd|deflate')) AS uncompressed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
            WHERE "status" = 200 AND "resp_bytes" >= 1024 AND "url" IS NOT NULL
              AND regexp_matches("url", '{COMPRESSIBLE_REGEX}', 'i')
        )
        SELECT "url",
            SUM(CASE WHEN uncompressed THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN uncompressed THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30
        ORDER BY (w_rate - COALESCE(b_rate, 0)) DESC LIMIT 15
    """

# ── 44. Session-ID Harvesting / Rotation (session_harvesting) ──────────────────
# One client IP presenting MANY distinct (hashed) session cookies in the window
# — session-token brute forcing, credential stuffing that mints a fresh session
# per attempt, or cookie replay. Keyed on ip; the cookie_session hash is only
# ever COUNTED (COUNT DISTINCT), never surfaced, so the insight never emits a
# session id. The IP is masked for analysts in the processor. Needs the
# `cookie_session` field. 2 ``?`` (window start).
SESSION_HARVESTING = """
        WITH base AS (
            SELECT "ip" AS ip, "cookie_session" AS cs,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE "ip" IS NOT NULL AND "ip" <> '' AND "cookie_session" IS NOT NULL AND "cookie_session" <> ''
        )
        SELECT ip,
            COUNT(DISTINCT cs) FILTER (WHERE is_w) AS w_sessions,
            COUNT(*) FILTER (WHERE is_w) AS w_reqs,
            COUNT(DISTINCT cs) FILTER (WHERE is_b) AS b_sessions
        FROM base GROUP BY ip
        HAVING COUNT(DISTINCT cs) FILTER (WHERE is_w) >= 20
           AND COUNT(DISTINCT cs) FILTER (WHERE is_w) > COALESCE(COUNT(DISTINCT cs) FILTER (WHERE is_b), 0) * 2 + 10
        ORDER BY w_sessions DESC LIMIT 15
    """

# ── 45. Origin Connect vs Read Timeout Split (timeout_split) ───────────────────
# Splits origin slowness into its two phases: CONNECT (TCP+TLS handshake,
# oconnect_ms) vs READ (time from connect to first byte = ottfb µs → ms minus
# connect). Fires when either phase's P95 regressed materially window-vs-baseline
# — slow-connect points at origin TCP/TLS/LB saturation (503-ish), slow-read at
# origin app/DB processing (504-ish). Single aggregate row when it fires. Needs
# the `oconnect_ms` field. 2 ``?`` (window start).
TIMEOUT_SPLIT = """
        WITH base AS (
            SELECT
                oconnect_ms,
                GREATEST(ottfb / 1000.0 - oconnect_ms, 0) AS read_ms,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE oconnect_ms IS NOT NULL AND ottfb IS NOT NULL
        ),
        agg AS (
            SELECT
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_w) AS w_conn,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_b) AS b_conn,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY read_ms) FILTER (WHERE is_w) AS w_read,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY read_ms) FILTER (WHERE is_b) AS b_read,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total
            FROM base
        )
        SELECT w_conn, b_conn, w_read, b_read, w_total, b_total
        FROM agg
        WHERE w_total >= 50 AND b_total >= 100
            AND ((w_conn >= b_conn * 2 AND w_conn - b_conn >= 50)
                 OR (w_read >= b_read * 2 AND w_read - b_read >= 100))
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


# ── Coalesced IP-security aggregates ──────────────────────────────────────────
# ONE ``GROUP BY ip`` pass over the temp table computing the superset for the
# three IP-keyed security scans (low_and_slow, credential_enumeration,
# content_discovery). Pre-coalesce each ran its own GROUP BY ip scan; §9.7 of the
# design plan requires folding same-key insights into a shared pre-agg so
# cold-path latency doesn't grow linearly with the number of IP scans. The Python
# caller (``_coalesced_ip_security_aggregates``) demuxes the columns into each
# insight's existing processor row-schema and applies each insight's
# HAVING/ORDER/LIMIT in Python; on any exception the caller falls back to the
# legacy per-insight scans transparently.
#
# Conditional-aggregate design (design plan Track A "CASE-counter idea"):
#   - low_and_slow: sensitive/probe paths (NEW_PROBE_REGEX) over the WHOLE
#     retained span (no window split) → probe hit-count, distinct paths, and
#     epoch(sec) min/max for the span/rate. Mirrors the standalone LOW_AND_SLOW,
#     whose ``base`` is pre-filtered to probe URLs.
#   - credential_enumeration: auth paths (AUTH_PATHS_REGEX), window/baseline
#     401/403 counters. Mirrors CREDENTIAL_ENUMERATION.
#   - content_discovery: all URLs, window/baseline 404 counters. Mirrors
#     CONTENT_DISCOVERY.
# Both regexes are f-string-baked (``?``-free via re.escape, like LOW_AND_SLOW /
# CREDENTIAL_ENUMERATION) so the only ``?`` in the text are the two window-start
# binds the caller passes explicitly. ``{{table_name}}`` is escaped for runtime
# hydration. The outer HAVING pre-filters to IPs touching at least one signal so
# the returned set stays small; the caller further bounds memory with per-insight
# top-K heaps.
COALESCED_IP_SECURITY_AGGREGATES = f"""
    WITH base AS (
        SELECT
            "ip" AS ip,
            "url" AS url,
            status,
            epoch("timestamp")::BIGINT AS sec,
            regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
            regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}}
        WHERE "ip" IS NOT NULL AND "ip" <> '' AND "url" IS NOT NULL
    )
    SELECT
        ip,
        -- low_and_slow: probe paths over the WHOLE span (no window split)
        COUNT(*) FILTER (WHERE is_sensitive) AS ls_hits,
        COUNT(DISTINCT url) FILTER (WHERE is_sensitive) AS ls_distinct,
        MIN(sec) FILTER (WHERE is_sensitive) AS ls_min_sec,
        MAX(sec) FILTER (WHERE is_sensitive) AS ls_max_sec,
        -- credential_enumeration: auth paths, window/baseline 401/403
        COUNT(*) FILTER (WHERE is_auth AND is_w AND status IN (401, 403)) AS ce_w_denied,
        COUNT(*) FILTER (WHERE is_auth AND is_w) AS ce_w_attempts,
        COUNT(DISTINCT url) FILTER (WHERE is_auth AND is_w) AS ce_w_paths,
        COUNT(*) FILTER (WHERE is_auth AND is_b AND status IN (401, 403)) AS ce_b_denied,
        -- content_discovery: all URLs, window/baseline 404s
        COUNT(*) FILTER (WHERE is_w AND status = 404) AS cd_w_404,
        COUNT(*) FILTER (WHERE is_w) AS cd_w_total,
        COUNT(DISTINCT url) FILTER (WHERE is_w AND status = 404) AS cd_distinct_404,
        COUNT(*) FILTER (WHERE is_b AND status = 404) AS cd_b_404
    FROM base
    GROUP BY ip
    HAVING (COUNT(*) FILTER (WHERE is_sensitive) > 0)
        OR (COUNT(*) FILTER (WHERE is_auth AND is_w) > 0)
        OR (COUNT(*) FILTER (WHERE is_w AND status = 404) > 0)
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
    "LOW_AND_SLOW",
    "AUTH_PATHS_REGEX",
    "CREDENTIAL_ENUMERATION",
    "NETWORK_ASN_HEALTH",
    "CONTENT_DISCOVERY",
    "REFERER_MONOCULTURE",
    "METHOD_DRIFT",
    "NEW_ASN_TRAFFIC",
    "METRO_DELIVERY_DEGRADATION",
    "CONNECTION_TYPE_MIX",
    "POP_LATENCY_REGRESSION",
    "HTTP3_FALLBACK",
    "CACHE_HIT_CLIFF",
    "COMPRESSIBLE_REGEX",
    "PAYLOAD_COMPRESSION_REGRESSION",
    "SESSION_HARVESTING",
    "TIMEOUT_SPLIT",
    # repository.py templates
    "COALESCED_CITY_AGGREGATES",
    "COALESCED_URL_AGGREGATES",
    "COALESCED_IP_SECURITY_AGGREGATES",
]
