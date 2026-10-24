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
        ),
        agg AS (
            SELECT "{fp_col}",
                COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
            FROM base
            GROUP BY "{fp_col}"
            HAVING COUNT(DISTINCT "ip") FILTER (WHERE is_w) >= 5
        )
        SELECT "{fp_col}",
            w_ips,
            w_reqs,
            b_ips,
            w_ips * 1.0 / GREATEST(b_ips / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1) AS ip_ratio
        FROM agg
        WHERE w_ips > b_ips / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
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


def _build_monoculture_sql(col: str, share_floor: str, where_clause: str = "") -> str:
    and_where = f" AND ({where_clause})" if where_clause else ""
    return f"""
        WITH grp AS (
            SELECT "{col}",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{table_name}} GROUP BY "{col}"
        ),
        with_totals AS (
            SELECT "{col}", w_cnt, b_cnt,
                CAST(COALESCE(SUM(w_cnt) OVER (), 0) AS BIGINT) AS w_total,
                CAST(COALESCE(SUM(b_cnt) OVER (), 0) AS BIGINT) AS b_total
            FROM grp
        )
        SELECT "{col}", w_cnt, b_cnt, b_total, w_total
        FROM with_totals
        WHERE w_total > 0{and_where}
          AND w_cnt * 1.0 / w_total >= {share_floor}
          AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 3 + 0.10)
        ORDER BY w_cnt DESC LIMIT 10
    """


UA_MONOCULTURE = _build_monoculture_sql("ua", "0.25")

# ── 10. New Probe URLs ────────────────────────────────────────────────────────
# Uses NEW_PROBE_REGEX (above). f-string-baked into the template at module
# import time — the regex is a fixed literal, not user input.

NEW_PROBE_URLS = f"""
        WITH probe_urls AS (
            SELECT "url"
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
            WHERE regexp_matches("url", '{NEW_PROBE_REGEX}', 'i')
        )
        SELECT t."url",
            COUNT(*) FILTER (WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE t.timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            AVG(CASE WHEN t."status" >= 400 THEN 1.0 ELSE 0.0 END) FILTER (WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ)) * 100 AS w_error_pct
        FROM {{table_name}} t
        JOIN probe_urls p ON t."url" = p."url"
        GROUP BY t."url"
        HAVING w_cnt > 0 AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 25
    """

# ── 11. WAF Signal Spikes ─────────────────────────────────────────────────────

WAF_SIGNAL_SPIKES = """
        WITH waf_combos AS (
            SELECT "waf_sig",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {waf_table}
            WHERE "waf_sig" IS NOT NULL AND "waf_sig" != ''
            GROUP BY "waf_sig"
        ),
        unrolled AS (
            SELECT trim(unnest(string_split("waf_sig", ','))) AS signal,
                w_cnt,
                b_cnt
            FROM waf_combos
        )
        SELECT signal,
            CAST(SUM(w_cnt) AS BIGINT) AS w_cnt,
            CAST(SUM(b_cnt) AS BIGINT) AS b_cnt,
            SUM(w_cnt) * 1.0 / GREATEST(COALESCE(SUM(b_cnt), 0) * 1.0 / {baseline_hours} * {window_hours}, 0.5) AS spike_ratio
        FROM unrolled
        WHERE signal != '' AND signal != 'BOT-ANALYSIS'
        GROUP BY signal
        HAVING SUM(w_cnt) >= 3 AND SUM(w_cnt) > COALESCE(SUM(b_cnt), 0) * 1.0 / {baseline_hours} * {window_hours} * 2 + 2
        ORDER BY spike_ratio DESC LIMIT 15
    """

# ── 12. Proxy / VPN Surge ─────────────────────────────────────────────────────

PROXY_SURGE = """
        WITH all_p AS (
            SELECT "p_type",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {table_name} WHERE "p_type" IS NOT NULL GROUP BY "p_type"
        ),
        totals AS (
            SELECT
                COALESCE(SUM(w_cnt) FILTER (WHERE "p_type" != ''), 0) AS w_proxy_total,
                COALESCE(SUM(b_cnt) FILTER (WHERE "p_type" != ''), 0) AS b_proxy_total,
                COALESCE(SUM(w_cnt), 0) AS w_total_all,
                COALESCE(SUM(b_cnt), 0) AS b_total_all
            FROM all_p
        )
        SELECT b."p_type", b.w_cnt, b.b_cnt, t.w_total_all, t.b_total_all
        FROM all_p b, totals t
        WHERE b."p_type" != ''
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= 5
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= (t.b_proxy_total * 100.0 / NULLIF(t.b_total_all, 0)) * 2 + 5
    """

# ── 13. ASN Concentration ─────────────────────────────────────────────────────

ASN_CONCENTRATION = _build_monoculture_sql("asn", "0.20", '"asn" IS NOT NULL')

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
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_total,
            SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_total
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
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
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
        ),
        agg AS (
            SELECT "tls_ciphers_sha",
                COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
            FROM base
            GROUP BY "tls_ciphers_sha"
            HAVING COUNT(DISTINCT "ip") FILTER (WHERE is_w) >= 5
        )
        SELECT "tls_ciphers_sha",
            w_ips,
            w_reqs,
            COALESCE(b_ips, 0) AS b_ips
        FROM agg
        WHERE w_ips > COALESCE(b_ips, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 3
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
        win_ips AS (
            SELECT "ip",
                MAX(req_header_bytes) AS max_bytes,
                AVG(req_header_bytes) AS avg_bytes,
                COUNT(*) AS w_total
            FROM base
            WHERE is_w
            GROUP BY "ip"
            HAVING COUNT(*) >= 3
        ),
        stats AS (
            SELECT w."ip",
                w.max_bytes,
                w.avg_bytes,
                w.w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY b.req_header_bytes) AS b_p95
            FROM win_ips w
            JOIN base b ON w."ip" = b."ip" AND b.is_b
            GROUP BY w."ip", w.max_bytes, w.avg_bytes, w.w_total
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
        win_ips AS (
            SELECT "ip",
                MAX(conn_requests) AS max_reqs,
                AVG(conn_requests) AS avg_reqs,
                COUNT(*) AS w_total
            FROM base
            WHERE is_w
            GROUP BY "ip"
            HAVING COUNT(*) >= 5 AND MAX(conn_requests) >= 50
        ),
        stats AS (
            SELECT w."ip",
                w.max_reqs,
                w.avg_reqs,
                w.w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY b.conn_requests) AS b_p95
            FROM win_ips w
            JOIN base b ON w."ip" = b."ip" AND b.is_b
            GROUP BY w."ip", w.max_reqs, w.avg_reqs, w.w_total
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
        stats AS (
            SELECT server_region,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
                COUNT(*) FILTER (WHERE is_w) AS w_total,
                COUNT(*) FILTER (WHERE is_b) AS b_total,
                CASE
                    WHEN COUNT(*) FILTER (WHERE is_w AND ottfb IS NOT NULL) >= 20
                    THEN ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w AND ottfb IS NOT NULL) / 1000.0, 0)
                    ELSE NULL
                END AS ottfb_p95
            FROM base GROUP BY server_region
            HAVING w_total >= 20 AND b_total >= 50 AND w_p95 >= b_p95 * 1.5 AND w_p95 - b_p95 >= 100
        )
        SELECT server_region, w_p95, b_p95, w_total, b_total, ottfb_p95
        FROM stats
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
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
        WITH img_urls AS (
            SELECT "url"
            FROM (SELECT DISTINCT "url" FROM {table_name} WHERE "url" IS NOT NULL)
            WHERE ("url" ILIKE '%.jpg%' OR "url" ILIKE '%.jpeg%' OR "url" ILIKE '%.png%' OR "url" ILIKE '%.gif%')
              AND "url" NOT ILIKE '%auto=webp%' AND "url" NOT ILIKE '%format=auto%' AND "url" NOT ILIKE '%format=webp%' AND "url" NOT ILIKE '%format=avif%'
        )
        SELECT t."url", COUNT(*) as request_count, SUM(t."resp_bytes") as total_bytes,
            ROUND(AVG(t."resp_bytes") / 1024, 1) as avg_kb,
            ({ua_mobile_sel}) AS mobile_ratio
        FROM {table_name} t
        JOIN img_urls i ON t."url" = i."url"
        WHERE t.timestamp >= CAST(? AS TIMESTAMPTZ) AND t."status" = 200
        GROUP BY t."url" HAVING total_bytes > 1024 * 512
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
def _build_repeated_patterns_sql(
    *,
    key_col: str,
    key_select: str,
    key_where: str,
    div_select: str,
    div_col: str,
    div_alias: str,
) -> str:
    return f"""
    WITH win_rows AS (
        SELECT {key_select} AS {key_col}, epoch("timestamp")::BIGINT AS sec, {div_select} AS {div_col}, {{ua_col}} AS ua_val
        FROM {{table_name}}
        WHERE "timestamp" >= CAST(? AS TIMESTAMPTZ)
          AND {key_where}
    ),
    bot_uas AS (
        SELECT ua_str
        FROM (SELECT DISTINCT CAST(ua_val AS VARCHAR) AS ua_str FROM win_rows WHERE ua_val IS NOT NULL)
        WHERE regexp_matches(ua_str, '{{bot_ua_regex}}', 'i')
    ),
    kept AS (
        SELECT w.{key_col}, w.sec, w.{div_col}
        FROM win_rows w
        LEFT JOIN bot_uas b ON CAST(w.ua_val AS VARCHAR) = b.ua_str
        WHERE b.ua_str IS NULL
    ),
    meta AS (
        SELECT {key_col}, COUNT(DISTINCT {div_col}) AS {div_alias}, COUNT(*) AS n_events,
               (MAX(sec) - MIN(sec)) AS span_s
        FROM kept GROUP BY {key_col}
        HAVING COUNT(*) >= 13
           AND (COUNT(*) * 1.0 / NULLIF(MAX(sec) - MIN(sec), 0)) < 2.0
    ),
    secs AS (
        SELECT k.{key_col}, k.sec FROM kept k JOIN meta d USING({key_col}) GROUP BY k.{key_col}, k.sec
    ),
    gaps AS (
        SELECT {key_col}, sec - LAG(sec) OVER (PARTITION BY {key_col} ORDER BY sec) AS gap FROM secs
    ),
    g AS (SELECT {key_col}, gap FROM gaps WHERE gap IS NOT NULL),
    agg AS (
        SELECT {key_col}, COUNT(*) AS n_gaps, AVG(gap) AS mean_gap, VAR_SAMP(gap) AS var_gap,
               STDDEV_SAMP(gap) AS sd_gap, MODE(gap) AS mode_gap
        FROM g GROUP BY {key_col}
        HAVING COUNT(*) >= 12
    ),
    modal AS (
        SELECT g.{key_col}, AVG(CASE WHEN abs(g.gap - a.mode_gap) <= 1 THEN 1.0 ELSE 0.0 END) AS modal_frac
        FROM g JOIN agg a USING({key_col}) GROUP BY g.{key_col}
    )
    SELECT a.{key_col}, a.n_gaps, d.n_events,
           ROUND(a.mean_gap, 2)  AS avg_interval,
           ROUND(a.sd_gap, 2)    AS stddev_interval,
           ROUND(sqrt(GREATEST(a.var_gap - 1.0/12.0, 0)) / NULLIF(a.mean_gap, 0), 3) AS cv_corr,
           ROUND(m.modal_frac, 3) AS modal_frac,
           d.{div_alias}, d.span_s, a.mode_gap
    FROM agg a JOIN modal m USING({key_col}) JOIN meta d USING({key_col})
    WHERE a.n_gaps >= 12
      AND (d.n_events * 1.0 / NULLIF(d.span_s, 0)) < 2.0
      AND (
            (a.mean_gap >= 5  AND sqrt(GREATEST(a.var_gap - 1.0/12.0,0))/NULLIF(a.mean_gap,0) < 0.3
                              AND m.modal_frac >= 0.6)
         OR (a.mean_gap >= 1  AND a.mean_gap < 5 AND m.modal_frac >= 0.85)
          )
    ORDER BY m.modal_frac DESC, a.n_gaps DESC
    LIMIT 15
"""


REPEATED_PATTERNS = _build_repeated_patterns_sql(
    key_col="ip",
    key_select='"ip"',
    key_where='"ip" IS NOT NULL AND "ip" <> \'\'',
    div_select="{ua_col}",
    div_col="ua",
    div_alias="distinct_ua",
)

# ── 30b. Scripted Traffic Patterns by TLS Fingerprint (repeated_patterns_fp) ──
# Complementary variant of REPEATED_PATTERNS keyed on the TLS fingerprint
# (JA3/JA4) instead of client IP. Catches IP-rotating scripts that keep the
# same TLS stack. Diversity signal flips: distinct IPs per fingerprint (the IP
# variant counts distinct UAs per IP). Same statistical algorithm, gates, and
# scoring. Exactly ONE ``?`` (the window start).
REPEATED_PATTERNS_FP = _build_repeated_patterns_sql(
    key_col="entity",
    key_select="COALESCE(\"{fp_col}\", '')",
    key_where='"{fp_col}" IS NOT NULL AND "{fp_col}" != \'\'',
    div_select='"ip"',
    div_col="ip",
    div_alias="distinct_ip",
)


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
        ),
        agg AS (
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
            HAVING COUNT(*) FILTER (WHERE is_w) >= 50 AND COUNT(*) FILTER (WHERE is_b) >= 100
        )
        SELECT asn,
            w_ploss,
            b_ploss,
            w_jitter,
            b_jitter,
            w_retrans,
            b_retrans,
            w_total,
            b_total
        FROM agg
        WHERE (
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
REFERER_MONOCULTURE = _build_monoculture_sql(
    "referer",
    "0.20",
    '"referer" IS NOT NULL AND "referer" != \'\'',
)

# ── 36. HTTP Method Drift (method_drift) ──────────────────────────────────────
# Track B: a write/verb method (anything but GET/HEAD/OPTIONS) surging to an
# outsized share of window traffic vs a read-dominated baseline — API abuse,
# form/credential POST floods, unexpected PUT/DELETE. Row:
# [method, w_cnt, b_cnt, w_total, b_total]. 4 ``?`` (all window start).
METHOD_DRIFT = """
        WITH grp AS (
            SELECT "method",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {table_name}
            GROUP BY "method"
        ),
        with_totals AS (
            SELECT "method", w_cnt, b_cnt,
                CAST(COALESCE(SUM(w_cnt) OVER (), 0) AS BIGINT) AS w_total,
                CAST(COALESCE(SUM(b_cnt) OVER (), 0) AS BIGINT) AS b_total
            FROM grp
        )
        SELECT "method", w_cnt, b_cnt, w_total, b_total
        FROM with_totals
        WHERE "method" IS NOT NULL AND "method" != '' AND upper("method") NOT IN ('GET', 'HEAD', 'OPTIONS')
          AND w_total > 0 AND w_cnt >= 20 AND w_cnt * 1.0 / w_total >= 0.10
          AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 2 + 0.05)
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

# ── 37b. ASN Hosting Shift (asn_hosting_shift) ──────────────────────────────────
# Track B: per-ASN ratio of p_type='hosting' traffic shifting from consumer to
# datacenter — bot pools / scrapers spinning up on hosting infra behind that ASN.
# Row: [asn, w_hosting, w_total, b_hosting, b_total]. 2 ``?`` (window start).
ASN_HOSTING_SHIFT = """
        SELECT "asn",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "p_type" = 'hosting') AS w_hosting,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_total,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ) AND "p_type" = 'hosting') AS b_hosting,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_total
        FROM {table_name}
        WHERE "asn" IS NOT NULL AND "p_type" IS NOT NULL AND "p_type" != ''
        GROUP BY "asn"
        HAVING w_total >= 50
            AND w_total > 0
            AND (CAST(w_hosting AS DOUBLE) / w_total) >= 0.3
            AND (b_total = 0 OR (CAST(w_hosting AS DOUBLE) / w_total) >= (CAST(b_hosting AS DOUBLE) / NULLIF(b_total, 0)) * 3 + 0.10)
        ORDER BY w_hosting DESC LIMIT 15
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
        ),
        grouped AS (
            SELECT c_type, c_speed,
                COUNT(*) FILTER (WHERE is_w) AS w_cnt,
                COUNT(*) FILTER (WHERE is_b) AS b_cnt
            FROM base GROUP BY c_type, c_speed
        ),
        with_totals AS (
            SELECT c_type, c_speed, w_cnt, b_cnt,
                SUM(w_cnt) OVER () AS w_total,
                SUM(b_cnt) OVER () AS b_total
            FROM grouped
        )
        SELECT c_type, c_speed, w_cnt, b_cnt, w_total, b_total
        FROM with_totals
        WHERE w_cnt >= 20 AND w_total > 0
            AND w_cnt * 1.0 / w_total >= 0.15
            AND (b_total = 0
                 OR w_cnt * 1.0 / w_total
                    >= b_cnt * 1.0 / NULLIF(b_total, 0) * 2 + 0.05)
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
                SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
                SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_cacheable,
                SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
                SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_cacheable
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
        WITH comp_urls AS (
            SELECT "url"
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
            WHERE regexp_matches("url", '{COMPRESSIBLE_REGEX}', 'i')
        ),
        base AS (
            SELECT t."url",
                -- "compressed" = the encoding CONTAINS a known token, so a
                -- multi-value ("gzip, br") or whitespace-padded ("gzip ") header
                -- still counts as compressed; empty / "identity" / "none" are
                -- uncompressed.
                (NOT regexp_matches(lower(COALESCE(t."resp_header_content_encoding", '')), 'gzip|br|zstd|deflate')) AS uncompressed,
                (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} t
            JOIN comp_urls c ON t."url" = c."url"
            WHERE t."status" = 200 AND t."resp_bytes" >= 1024
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
        ),
        agg AS (
            SELECT ip,
                COUNT(DISTINCT cs) FILTER (WHERE is_w) AS w_sessions,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT cs) FILTER (WHERE is_b) AS b_sessions
            FROM base
            GROUP BY ip
            HAVING COUNT(DISTINCT cs) FILTER (WHERE is_w) >= 20
        )
        SELECT ip,
            w_sessions,
            w_reqs,
            COALESCE(b_sessions, 0) AS b_sessions
        FROM agg
        WHERE w_sessions > COALESCE(b_sessions, 0) * 2 + 10
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
            status,
            elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
        WHERE "city" IS NOT NULL AND "city" != ''
    )
    SELECT
        {label_expr} AS label, "city", region, country,
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
    GROUP BY "city", region, country
    HAVING COUNT(*) FILTER (WHERE is_w) >= 5
    """

# ── Coalesced URL aggregates ─────────────────────────────────────────────────
# ONE pass over the temp table that computes the superset for the URL-keyed
# insights.

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
        SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
        SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
        -- cache_collapse denominator + cacheability_regression: MISS / PASS counters
        SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
        SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
        SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
        SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
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
    HAVING COUNT(*) FILTER (WHERE is_w) >= 3
    """

COALESCED_URL_EXTENDED_AGGREGATES = f"""
    WITH url_flags AS (
        SELECT "url", is_probe, is_comp, is_img
        FROM (
            SELECT
                "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_probe,
                regexp_matches("url", '{COMPRESSIBLE_REGEX}', 'i') AS is_comp,
                (("url" ILIKE '%.jpg%' OR "url" ILIKE '%.jpeg%' OR "url" ILIKE '%.png%' OR "url" ILIKE '%.gif%')
                 AND "url" NOT ILIKE '%auto=webp%' AND "url" NOT ILIKE '%format=auto%'
                 AND "url" NOT ILIKE '%format=webp%' AND "url" NOT ILIKE '%format=avif%') AS is_img
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_probe OR is_comp OR is_img
    ),
    base AS (
        SELECT
            t."url" AS url,
            t.status,
            t.cache,
            t.elapsed,
            {{resp_bytes_sel}} AS resp_bytes,
            {{uncompressed_sel}} AS uncompressed,
            {{is_mobile_sel}} AS is_mobile,
            {{ttl_sel}} AS ttl,
            {{hits_sel}} AS hits,
            {{age_sel}} AS age,
            {{oretries_sel}} AS oretries,
            COALESCE(u.is_probe, FALSE) AS is_probe,
            COALESCE(u.is_comp, FALSE) AS is_comp,
            COALESCE(u.is_img, FALSE) AS is_img,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."url" IS NOT NULL
    )
    SELECT
        url,
        COUNT(*) FILTER (WHERE is_w) AS w_total,
        COUNT(*) FILTER (WHERE is_b) AS b_total,
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_5xx,
        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_5xx,
        SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_hits,
        SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_hits,
        SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_miss,
        SUM(CASE WHEN starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_miss,
        SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_pass,
        SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_pass,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed)
            FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY elapsed)
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p99,
        ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY elapsed)
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p50,
        -- new_probe_urls
        BOOL_OR(is_probe) AS is_probe,
        SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_4xx,
        -- payload_compression_regression
        BOOL_OR(is_comp) AS is_comp,
        COUNT(*) FILTER (WHERE is_w AND is_comp AND status = 200 AND resp_bytes >= 1024 AND uncompressed) AS pcr_w_uncomp,
        COUNT(*) FILTER (WHERE is_w AND is_comp AND status = 200 AND resp_bytes >= 1024) AS pcr_w_total,
        COUNT(*) FILTER (WHERE is_b AND is_comp AND status = 200 AND resp_bytes >= 1024 AND uncompressed) AS pcr_b_uncomp,
        COUNT(*) FILTER (WHERE is_b AND is_comp AND status = 200 AND resp_bytes >= 1024) AS pcr_b_total,
        -- image_optimization_opportunities
        BOOL_OR(is_img) AS is_img,
        COUNT(*) FILTER (WHERE is_w AND is_img AND status = 200) AS img_w_cnt,
        SUM(resp_bytes) FILTER (WHERE is_w AND is_img AND status = 200) AS img_w_bytes,
        AVG(resp_bytes) FILTER (WHERE is_w AND is_img AND status = 200) AS img_w_avg_bytes,
        SUM(is_mobile) FILTER (WHERE is_w AND is_img AND status = 200) AS img_w_mobile,
        -- cache_ttl_mismatch
        COUNT(*) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_cnt,
        AVG(ttl) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_avg_ttl,
        AVG(hits) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_avg_hits,
        AVG(age) FILTER (WHERE is_w AND ttl IS NOT NULL AND ttl > 0 AND hits IS NOT NULL AND age IS NOT NULL) AS ttl_w_avg_age,
        -- origin_retries
        COUNT(*) FILTER (WHERE is_w AND oretries > 0) AS oret_w_cnt,
        AVG(oretries) FILTER (WHERE is_w AND oretries > 0) AS oret_w_avg,
        MAX(oretries) FILTER (WHERE is_w AND oretries > 0) AS oret_w_max
    FROM base
    GROUP BY url
    HAVING COUNT(*) FILTER (WHERE is_w) >= 3
        OR (BOOL_OR(is_probe) AND COUNT(*) FILTER (WHERE is_w) > 0 AND COUNT(*) FILTER (WHERE is_b) = 0)
        OR (BOOL_OR(is_img) AND SUM(resp_bytes) FILTER (WHERE is_w AND is_img AND status = 200) > 524288)
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
    WITH url_flags AS (
        SELECT "url", is_sensitive, is_auth
        FROM (
            SELECT
                "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
                regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_sensitive OR is_auth
    ),
    base AS (
        SELECT
            t."ip" AS ip,
            t."url" AS url,
            t.status,
            epoch(t."timestamp")::BIGINT AS sec,
            COALESCE(u.is_sensitive, FALSE) AS is_sensitive,
            COALESCE(u.is_auth, FALSE) AS is_auth,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."ip" IS NOT NULL AND t."ip" <> ''
          AND t."url" IS NOT NULL
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
    WHERE is_w OR is_sensitive OR status = 404 OR (is_auth AND status IN (401, 403))
    GROUP BY ip
    HAVING (COUNT(*) FILTER (WHERE is_sensitive) > 0)
        OR (COUNT(*) FILTER (WHERE is_auth AND is_w) > 0)
        OR (COUNT(*) FILTER (WHERE is_w AND status = 404) > 0)
    """

COALESCED_IP_EXTENDED_AGGREGATES = f"""
    WITH url_flags AS (
        SELECT "url", is_sensitive, is_auth, is_deep
        FROM (
            SELECT
                "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
                regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth,
                ("url" LIKE '/%/%/%' AND "url" NOT LIKE '%.%') AS is_deep
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_sensitive OR is_auth OR is_deep
    ),
    base AS (
        SELECT
            t."ip" AS ip,
            t."url" AS url,
            t.status,
            epoch(t."timestamp")::BIGINT AS sec,
            {{no_referer_sel}} AS no_referer,
            {{cs_sel}} AS cs,
            {{req_header_bytes_sel}} AS req_header_bytes,
            {{conn_requests_sel}} AS conn_requests,
            COALESCE(u.is_sensitive, FALSE) AS is_sensitive,
            COALESCE(u.is_auth, FALSE) AS is_auth,
            COALESCE(u.is_deep, FALSE) AS is_deep,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."ip" IS NOT NULL AND t."ip" <> ''
    )
    SELECT
        ip,
        -- low_and_slow: probe paths over the WHOLE span (no window split)
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_sensitive) AS ls_hits,
        COUNT(DISTINCT url) FILTER (WHERE url IS NOT NULL AND is_sensitive) AS ls_distinct,
        MIN(sec) FILTER (WHERE url IS NOT NULL AND is_sensitive) AS ls_min_sec,
        MAX(sec) FILTER (WHERE url IS NOT NULL AND is_sensitive) AS ls_max_sec,
        -- credential_enumeration: auth paths, window/baseline 401/403
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_auth AND is_w AND status IN (401, 403)) AS ce_w_denied,
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_auth AND is_w) AS ce_w_attempts,
        COUNT(DISTINCT url) FILTER (WHERE url IS NOT NULL AND is_auth AND is_w) AS ce_w_paths,
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_auth AND is_b AND status IN (401, 403)) AS ce_b_denied,
        -- content_discovery: all URLs, window/baseline 404s
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_w AND status = 404) AS cd_w_404,
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_w) AS cd_w_total,
        COUNT(DISTINCT url) FILTER (WHERE url IS NOT NULL AND is_w AND status = 404) AS cd_distinct_404,
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_b AND status = 404) AS cd_b_404,
        -- orphaned_deep_crawl
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_w) AS odc_w_reqs,
        COUNT(DISTINCT url) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_w) AS odc_w_urls,
        COUNT(*) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_b) AS odc_b_reqs,
        -- session_harvesting
        COUNT(DISTINCT cs) FILTER (WHERE is_w AND cs IS NOT NULL AND cs <> '') AS sh_w_sessions,
        COUNT(*) FILTER (WHERE is_w AND cs IS NOT NULL AND cs <> '') AS sh_w_reqs,
        COUNT(DISTINCT cs) FILTER (WHERE is_b AND cs IS NOT NULL AND cs <> '') AS sh_b_sessions,
        -- request_size_anomaly pre-check (min(B) <= P95(B), so if w_max <= b_min * 3 we know w_max <= b_p95 * 3)
        COUNT(*) FILTER (WHERE is_w AND req_header_bytes > 0) AS rsa_w_cnt,
        MAX(req_header_bytes) FILTER (WHERE is_w AND req_header_bytes > 0) AS rsa_w_max,
        MIN(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) AS rsa_b_min,
        MAX(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) AS rsa_b_max,
        -- connection_abuse pre-check (same exact min(B) <= P95(B) bound)
        COUNT(*) FILTER (WHERE is_w AND conn_requests > 0) AS ca_w_cnt,
        MAX(conn_requests) FILTER (WHERE is_w AND conn_requests > 0) AS ca_w_max,
        MIN(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) AS ca_b_min,
        MAX(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) AS ca_b_max
    FROM base
    GROUP BY ip
    HAVING (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_sensitive) >= 5)
        OR (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_auth AND is_w AND status IN (401, 403)) >= 20)
        OR (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_w AND status = 404) >= 20)
        OR (COUNT(*) FILTER (WHERE url IS NOT NULL AND is_deep AND no_referer AND is_w) >= 50)
        OR (COUNT(DISTINCT cs) FILTER (WHERE is_w AND cs IS NOT NULL AND cs <> '') >= 20)
        OR (COUNT(*) FILTER (WHERE is_w AND req_header_bytes > 0) >= 3 AND MIN(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) IS NOT NULL AND MAX(req_header_bytes) FILTER (WHERE is_w AND req_header_bytes > 0) > MIN(req_header_bytes) FILTER (WHERE is_b AND req_header_bytes > 0) * 3)
        OR (COUNT(*) FILTER (WHERE is_w AND conn_requests > 0) >= 5 AND MAX(conn_requests) FILTER (WHERE is_w AND conn_requests > 0) >= 50 AND MIN(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) IS NOT NULL AND MAX(conn_requests) FILTER (WHERE is_w AND conn_requests > 0) > MIN(conn_requests) FILTER (WHERE is_b AND conn_requests > 0) * 3)
    """


# ── Coalesced ASN aggregates ──────────────────────────────────────────────────
# ONE ``GROUP BY asn`` pass over the temp table computing the superset for the
# four ASN-keyed insights (asn_concentration, new_asn_traffic,
# network_asn_health, asn_hosting_shift).

COALESCED_ASN_AGGREGATES = """
    WITH base AS (
        SELECT
            "asn" AS asn,
            {ploss_sel} AS ploss,
            {rtt_var_sel} AS rtt_var,
            {retrans_sel} AS retrans,
            {p_type_sel} AS p_type,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        asn,
        COUNT(*) FILTER (WHERE is_w) AS w_cnt,
        COUNT(*) FILTER (WHERE is_b) AS b_cnt,
        AVG(ploss) FILTER (WHERE is_w AND asn IS NOT NULL) AS w_ploss,
        AVG(ploss) FILTER (WHERE is_b AND asn IS NOT NULL) AS b_ploss,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_w AND asn IS NOT NULL) AS w_jitter,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY rtt_var) FILTER (WHERE is_b AND asn IS NOT NULL) AS b_jitter,
        AVG(retrans) FILTER (WHERE is_w AND asn IS NOT NULL) AS w_retrans,
        AVG(retrans) FILTER (WHERE is_b AND asn IS NOT NULL) AS b_retrans,
        COUNT(*) FILTER (WHERE is_w AND asn IS NOT NULL AND p_type = 'hosting') AS w_hosting,
        COUNT(*) FILTER (WHERE is_w AND asn IS NOT NULL AND p_type IS NOT NULL AND p_type != '') AS w_pt_total,
        COUNT(*) FILTER (WHERE is_b AND asn IS NOT NULL AND p_type = 'hosting') AS b_hosting,
        COUNT(*) FILTER (WHERE is_b AND asn IS NOT NULL AND p_type IS NOT NULL AND p_type != '') AS b_pt_total
    FROM base
    GROUP BY asn
    """


# ── Coalesced scalar aggregates ───────────────────────────────────────────────
# ONE ungrouped table scan computing the service-wide counters for
# http3_fallback and cache_hit_cliff.

COALESCED_SCALAR_AGGREGATES = """
    WITH base AS (
        SELECT
            {transport_sel} AS transport,
            {cache_sel} AS cache,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS w_quic,
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '') AS w_tr_total,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS b_quic,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '') AS b_tr_total,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_hits,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_cacheable,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_hits,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_cacheable
    FROM base
    """

COALESCED_SCALAR_EXTENDED_AGGREGATES = """
    WITH base AS (
        SELECT
            {transport_sel} AS transport,
            {cache_sel} AS cache,
            {oconnect_ms_sel} AS oconnect_ms,
            {ottfb_sel} AS ottfb,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS w_quic,
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '') AS w_tr_total,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS b_quic,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '') AS b_tr_total,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_hits,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_cacheable,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_hits,
        COALESCE(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_cacheable,
        -- timeout_split
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_w AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_w_conn,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY oconnect_ms) FILTER (WHERE is_b AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_b_conn,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY GREATEST(ottfb / 1000.0 - oconnect_ms, 0)) FILTER (WHERE is_w AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_w_read,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY GREATEST(ottfb / 1000.0 - oconnect_ms, 0)) FILTER (WHERE is_b AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_b_read,
        COUNT(*) FILTER (WHERE is_w AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_w_total,
        COUNT(*) FILTER (WHERE is_b AND oconnect_ms IS NOT NULL AND ottfb IS NOT NULL) AS ts_b_total,
        -- origin_latency_spike overall gate
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w AND ottfb IS NOT NULL) / 1000.0 AS ols_w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_b AND ottfb IS NOT NULL) / 1000.0 AS ols_b_p95
    FROM base
    """

COALESCED_FP_AGGREGATES = """
    WITH base AS (
        SELECT
            {fp_sel} AS fp,
            {cipher_sel} AS cipher,
            {ip_sel} AS ip,
            {asn_sel} AS asn,
            {is_non_hosting_sel} AS is_non_hosting,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        GROUPING_ID(fp, cipher) AS gid,
        fp,
        cipher,
        -- botnet_grouping (gid = 1, grouped by fp)
        COUNT(DISTINCT ip) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '') AS bg_w_ips,
        COUNT(*) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '') AS bg_w_reqs,
        COUNT(DISTINCT ip) FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '') AS bg_b_ips,
        -- residential_fingerprint_dispersion (gid = 1, grouped by fp, non-hosting)
        COUNT(DISTINCT ip) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_w_ips,
        COUNT(DISTINCT asn) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_w_asns,
        COUNT(*) FILTER (WHERE is_w AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_w_reqs,
        COUNT(DISTINCT ip) FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_b_ips,
        COUNT(DISTINCT asn) FILTER (WHERE is_b AND fp IS NOT NULL AND fp != '' AND is_non_hosting) AS rfd_b_asns,
        -- cipher_spread (gid = 2, grouped by cipher)
        COUNT(DISTINCT ip) FILTER (WHERE is_w AND cipher IS NOT NULL AND cipher != '') AS cs_w_ips,
        COUNT(*) FILTER (WHERE is_w AND cipher IS NOT NULL AND cipher != '') AS cs_w_reqs,
        COUNT(DISTINCT ip) FILTER (WHERE is_b AND cipher IS NOT NULL AND cipher != '') AS cs_b_ips
    FROM base
    GROUP BY GROUPING SETS ((fp), (cipher))
    """

COALESCED_DIM_AGGREGATES = """
    WITH base AS (
        SELECT
            {country_sel} AS country,
            {p_type_sel} AS p_type,
            {method_sel} AS method,
            {c_type_sel} AS c_type,
            {c_speed_sel} AS c_speed,
            {ost_sel} AS ost,
            {oip_sel} AS oip,
            {referer_sel} AS referer,
            {ua_sel} AS ua,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        GROUPING_ID(country, p_type, method, c_type, c_speed, ost, oip, referer, ua) AS gid,
        country, p_type, method, c_type, c_speed, ost, oip, referer, ua,
        COUNT(*) FILTER (WHERE is_w) AS w_cnt,
        COUNT(*) FILTER (WHERE is_b) AS b_cnt,
        COUNT(*) FILTER (WHERE is_w AND ost IS NOT NULL) AS w_ost_nn,
        COUNT(*) FILTER (WHERE is_b AND ost IS NOT NULL) AS b_ost_nn,
        COUNT(*) FILTER (WHERE is_w AND ost >= 500) AS w_ost_5xx,
        COUNT(*) FILTER (WHERE is_b AND ost >= 500) AS b_ost_5xx
    FROM base
    GROUP BY GROUPING SETS (
        (country),
        (p_type),
        (method),
        (c_type, c_speed),
        (ost),
        (oip),
        (referer),
        (ua)
    )
    """

COALESCED_GEO_LATENCY_AGGREGATES = """
    WITH base AS (
        SELECT
            {pop_sel} AS pop,
            {server_region_sel} AS server_region,
            {metro_sel} AS metro,
            {elapsed_sel} AS elapsed,
            {ottfb_sel} AS ottfb,
            {delivery_rate_sel} AS delivery_rate,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {table_name}
    )
    SELECT
        GROUPING_ID(pop, server_region, metro) AS gid,
        pop,
        server_region,
        metro,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0 AS w_p95,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b AND elapsed IS NOT NULL) / 1000.0 AS b_p95,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL) AS w_lat_total,
        COUNT(*) FILTER (WHERE is_b AND elapsed IS NOT NULL) AS b_lat_total,
        COUNT(*) FILTER (WHERE is_w AND elapsed IS NOT NULL AND ottfb IS NOT NULL) AS w_ottfb_cnt,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ottfb) FILTER (WHERE is_w AND elapsed IS NOT NULL AND ottfb IS NOT NULL) / 1000.0, 0) AS w_ottfb_p95,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_w AND delivery_rate > 0) AS w_med_dr,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_rate) FILTER (WHERE is_b AND delivery_rate > 0) AS b_med_dr,
        COUNT(*) FILTER (WHERE is_w AND delivery_rate > 0) AS w_dr_total,
        COUNT(*) FILTER (WHERE is_b AND delivery_rate > 0) AS b_dr_total
    FROM base
    GROUP BY GROUPING SETS ((pop), (server_region), (metro))
    """


CRAWLER_DISPARITY = """
        WITH ua_agg AS (
            SELECT
                ua,
                COUNT(*) FILTER (WHERE is_w) AS w_cnt,
                COUNT(*) FILTER (WHERE is_b) AS b_cnt,
                CAST(COALESCE(SUM(COALESCE(resp_bytes, 0)) FILTER (WHERE is_w), 0) AS BIGINT) AS w_bytes,
                CAST(COALESCE(SUM(COALESCE(resp_bytes, 0)) FILTER (WHERE is_b), 0) AS BIGINT) AS b_bytes
            FROM (
                SELECT ua, resp_bytes,
                    (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w,
                    (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b
                FROM {table_name}
            )
            GROUP BY ua
        ),
        site_win AS (
            SELECT
                GREATEST(CAST(COALESCE(SUM(w_bytes), 0) AS BIGINT), 1) AS total_site_bytes
            FROM ua_agg
        ),
        ua_extracted AS (
            SELECT
                ua,
                l_ua,
                w_cnt,
                b_cnt,
                w_bytes,
                b_bytes,
                regexp_extract(
                    l_ua,
                    '([a-z0-9_.-]*(bot|crawler|spider|slurp|archiver|scraper|externalagent|extended|other|preview)[a-z0-9_.-]*|headless[a-z0-9_.-]*|python-[a-z0-9_.-]+|go-http-client|curl|wget|scrapy|aiohttp|httpx|okhttp|axios)',
                    1
                ) AS raw_tok,
                regexp_extract(l_ua, 'http[s]*://(www\\.)*([^/.:;\\s)]+)', 2) AS ua_dom,
                regexp_extract(l_ua, '([a-z0-9]+)-(user|externalfetcher)', 1) AS user_fetcher_stem
            FROM (
                SELECT ua, lower(ua) AS l_ua, w_cnt, b_cnt, w_bytes, b_bytes
                FROM ua_agg
                WHERE ua IS NOT NULL
            )
        ),
        ua_info AS (
            SELECT
                ua,
                w_cnt,
                b_cnt,
                w_bytes,
                b_bytes,
                raw_tok,
                ua_dom,
                user_fetcher_stem,
                (raw_tok != '') AS is_crawler,
                (raw_tok = '') AS is_human,
                NULLIF(
                    regexp_replace(
                        raw_tok,
                        '(-searchbot|-assistbot|-externalagent|-extended|-user|searchbot|assistbot|externalagent|extended|crawler|spider|slurp|archiver|scraper|preview|other|bot).*$',
                        ''
                    ),
                    ''
                ) AS stem
            FROM ua_extracted
        ),
        crawler_traffic AS (
            SELECT
                COALESCE(stem, raw_tok) AS bot_stem,
                MIN(raw_tok) FILTER (WHERE w_cnt > 0) AS raw_tok,
                MAX(ua_dom) FILTER (WHERE w_cnt > 0) AS ua_dom,
                CAST(COALESCE(SUM(w_cnt), 0) AS BIGINT) AS w_crawls,
                CAST(COALESCE(SUM(w_bytes), 0) AS BIGINT) AS w_bytes,
                CAST(COALESCE(SUM(b_cnt), 0) AS BIGINT) AS b_crawls,
                CAST(COALESCE(SUM(b_bytes), 0) AS BIGINT) AS b_bytes
            FROM ua_info
            WHERE is_crawler
            GROUP BY COALESCE(stem, raw_tok)
            HAVING SUM(w_cnt) >= 50
        ),
        ref_info AS (
            SELECT
                referer,
                NULLIF(regexp_extract(l_ref, 'http[s]*://(www\\.)*([^/.:;\\s]+)', 2), '') AS ref_dom
            FROM (
                SELECT referer, lower(referer) AS l_ref
                FROM (
                    SELECT DISTINCT referer
                    FROM {table_name}
                    WHERE referer IS NOT NULL AND EXISTS (SELECT 1 FROM crawler_traffic)
                )
            )
        ),
        all_ref_stems AS (
            SELECT DISTINCT ref_stem
            FROM (
                SELECT user_fetcher_stem AS ref_stem FROM ua_info WHERE is_human AND user_fetcher_stem != ''
                UNION
                SELECT ref_dom AS ref_stem FROM ref_info WHERE ref_dom IS NOT NULL AND ref_dom != ''
            )
            WHERE ref_stem IS NOT NULL AND ref_stem != ''
        ),
        stem_matches AS (
            SELECT
                c.bot_stem,
                rs.ref_stem
            FROM crawler_traffic c
            JOIN all_ref_stems rs
              ON c.bot_stem = rs.ref_stem
              OR (
                  length(regexp_replace(c.bot_stem, '[aeiou]', '', 'g')) >= 3
                  AND regexp_replace(c.bot_stem, '[aeiou]', '', 'g') = regexp_replace(rs.ref_stem, '[aeiou]', '', 'g')
              )
              OR (
                  length(c.bot_stem) >= 3 AND length(rs.ref_stem) >= 3
                  AND (contains(rs.ref_stem, c.bot_stem) OR contains(c.bot_stem, rs.ref_stem))
              )
              OR (c.ua_dom != '' AND c.ua_dom = rs.ref_stem)
        ),
        referred_raw AS (
            SELECT
                t.ip,
                (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w,
                (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                COALESCE(NULLIF(u.user_fetcher_stem, ''), r.ref_dom) AS ref_stem
            FROM {table_name} t
            JOIN ua_info u ON t.ua = u.ua
            LEFT JOIN ref_info r ON t.referer = r.referer
            WHERE u.is_human
              AND t.ip IS NOT NULL
              AND t.timestamp IS NOT NULL
              AND EXISTS (SELECT 1 FROM crawler_traffic)
        ),
        referred_traffic AS (
            SELECT
                sm.bot_stem,
                COUNT(DISTINCT rr.ip) FILTER (WHERE rr.is_w) AS w_referrals,
                COUNT(DISTINCT rr.ip) FILTER (WHERE rr.is_b) AS b_referrals
            FROM referred_raw rr
            JOIN stem_matches sm ON rr.ref_stem = sm.ref_stem
            GROUP BY sm.bot_stem
        )
        SELECT
            c.raw_tok AS label,
            c.w_crawls,
            COALESCE(r.w_referrals, 0) AS w_referrals,
            c.w_bytes,
            c.w_crawls * 1.0 / GREATEST(COALESCE(r.w_referrals, 0), 1) AS crawls_per_referral,
            c.b_crawls * 1.0 / GREATEST(COALESCE(r.b_referrals, 0), 1) AS baseline_crawls_per_referral,
            c.w_bytes * 1.0 / GREATEST(COALESCE(r.w_referrals, 0), 1) AS bytes_per_referral,
            c.b_bytes * 1.0 / GREATEST(COALESCE(r.b_referrals, 0), 1) AS baseline_bytes_per_referral,
            ROUND(c.w_bytes * 100.0 / s.total_site_bytes, 2) AS bandwidth_share_pct
        FROM crawler_traffic c
        CROSS JOIN site_win s
        LEFT JOIN referred_traffic r ON c.bot_stem = r.bot_stem
        WHERE c.w_crawls >= 50
          AND (
              r.w_referrals IS NULL
              OR r.w_referrals = 0
              OR c.w_crawls * 1.0 / GREATEST(r.w_referrals, 1) >= 100.0
              OR c.w_bytes * 1.0 / GREATEST(r.w_referrals, 1) >= 1048576.0
          )
        ORDER BY crawls_per_referral DESC, c.w_bytes DESC LIMIT 10
    """


STALE_BROWSER_VERSION = r"""
        WITH distinct_uas AS (
            SELECT ua, lower(ua) AS l_ua
            FROM (SELECT DISTINCT ua FROM {table_name} WHERE ua IS NOT NULL AND contains(lower(ua), 'chrome'))
        ),
        stale_uas AS (
            SELECT
                ua,
                chrome_version
            FROM (
                SELECT
                    ua,
                    regexp_extract(ua, 'Chrome/([0-9]+)\.', 1) AS chrome_version
                FROM distinct_uas
                WHERE contains(l_ua, 'chrome') AND NOT contains(l_ua, 'bot')
                  AND NOT contains(l_ua, 'crawler') AND NOT contains(l_ua, 'spider')
            )
            WHERE chrome_version IS NOT NULL AND chrome_version != ''
              AND CAST(chrome_version AS INTEGER) < 131
        ),
        ua_parsed AS (
            SELECT
                t.ip,
                s.chrome_version,
                (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name} t
            JOIN stale_uas s ON t.ua = s.ua
        ),
        version_aggregates AS (
            SELECT
                chrome_version,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT ip) FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_b) AS b_reqs
            FROM ua_parsed
            GROUP BY chrome_version
            HAVING COUNT(*) FILTER (WHERE is_w) >= 500
        )
        SELECT
            'Chrome ' || chrome_version AS label,
            w_reqs,
            w_ips,
            w_reqs * 1.0 / GREATEST(COALESCE(b_reqs, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1.0) AS surge_ratio
        FROM version_aggregates
        WHERE CAST(chrome_version AS INTEGER) < 131
          AND w_reqs >= 500
          AND w_reqs > COALESCE(b_reqs, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours} * 2.5
        ORDER BY surge_ratio DESC LIMIT 10
    """


ORPHANED_DEEP_CRAWL = """
        WITH deep_requests AS (
            SELECT
                ip,
                url,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE url LIKE '/%/%/%'
              AND url NOT LIKE '%.%'
              AND (referer IS NULL OR referer = '' OR referer = '-')
        ),
        agg AS (
            SELECT
                ip,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT url) FILTER (WHERE is_w) AS w_distinct_urls,
                COUNT(*) FILTER (WHERE is_b) AS b_reqs
            FROM deep_requests
            GROUP BY ip
            HAVING COUNT(*) FILTER (WHERE is_w) >= 50
               AND COUNT(DISTINCT url) FILTER (WHERE is_w) >= 10
        )
        SELECT
            ip AS label,
            w_reqs,
            w_distinct_urls,
            COALESCE(b_reqs, 0) AS b_reqs,
            w_reqs * 1.0 / GREATEST(w_distinct_urls, 1.0) AS requests_per_url,
            w_reqs * 1.0 / GREATEST(COALESCE(b_reqs, 0) / GREATEST({baseline_hours}, 1.0) * {window_hours}, 1.0) AS surge_ratio
        FROM agg
        ORDER BY w_reqs DESC LIMIT 15
    """


RESIDENTIAL_FINGERPRINT_DISPERSION = """
        WITH fingerprint_asn_maps AS (
            SELECT
                ja4,
                ip,
                asn,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {table_name}
            WHERE ja4 IS NOT NULL AND ja4 != ''
              AND (p_type IS NULL OR p_type != 'hosting')
        ),
        agg AS (
            SELECT
                ja4,
                COUNT(DISTINCT ip) FILTER (WHERE is_w) AS w_ips,
                COUNT(DISTINCT asn) FILTER (WHERE is_w) AS w_asns,
                COUNT(*) FILTER (WHERE is_w) AS w_requests,
                COUNT(DISTINCT ip) FILTER (WHERE is_b) AS b_ips,
                COUNT(DISTINCT asn) FILTER (WHERE is_b) AS b_asns
            FROM fingerprint_asn_maps
            GROUP BY ja4
            HAVING COUNT(DISTINCT ip) FILTER (WHERE is_w) >= 30
               AND COUNT(DISTINCT asn) FILTER (WHERE is_w) >= 5
        )
        SELECT
            ja4 AS label,
            w_ips,
            w_asns,
            w_requests,
            COALESCE(b_ips, 0) AS b_ips,
            w_asns * 1.0 / GREATEST(COALESCE(b_asns, 0), 1.0) AS asn_expansion_ratio
        FROM agg
        ORDER BY asn_expansion_ratio DESC LIMIT 10
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
    "REPEATED_PATTERNS_FP",
    "LOW_AND_SLOW",
    "AUTH_PATHS_REGEX",
    "CREDENTIAL_ENUMERATION",
    "NETWORK_ASN_HEALTH",
    "CONTENT_DISCOVERY",
    "REFERER_MONOCULTURE",
    "METHOD_DRIFT",
    "NEW_ASN_TRAFFIC",
    "ASN_HOSTING_SHIFT",
    "METRO_DELIVERY_DEGRADATION",
    "CONNECTION_TYPE_MIX",
    "POP_LATENCY_REGRESSION",
    "HTTP3_FALLBACK",
    "CACHE_HIT_CLIFF",
    "COMPRESSIBLE_REGEX",
    "PAYLOAD_COMPRESSION_REGRESSION",
    "SESSION_HARVESTING",
    "TIMEOUT_SPLIT",
    "CRAWLER_DISPARITY",
    "STALE_BROWSER_VERSION",
    "ORPHANED_DEEP_CRAWL",
    "RESIDENTIAL_FINGERPRINT_DISPERSION",
    # repository.py templates
    "COALESCED_CITY_AGGREGATES",
    "COALESCED_URL_AGGREGATES",
    "COALESCED_URL_EXTENDED_AGGREGATES",
    "COALESCED_IP_SECURITY_AGGREGATES",
    "COALESCED_IP_EXTENDED_AGGREGATES",
    "COALESCED_ASN_AGGREGATES",
    "COALESCED_SCALAR_AGGREGATES",
    "COALESCED_SCALAR_EXTENDED_AGGREGATES",
    "COALESCED_FP_AGGREGATES",
    "COALESCED_DIM_AGGREGATES",
    "COALESCED_GEO_LATENCY_AGGREGATES",
]
