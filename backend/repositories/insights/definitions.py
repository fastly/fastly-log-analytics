from __future__ import annotations

import sys as _sys
from typing import Any

from backend.core.metric_expressions import coalesce
from backend.core.share_db.validation import mask_ip
from backend.repositories.insights import InsightCategory, InsightDefinition, registry
from backend.utils.countries import format_city_label

SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.insights", _sys.modules[__name__])


def _reg(**kwargs: Any) -> None:
    registry.register(InsightDefinition(**kwargs))


import re

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
ERROR_SPIKES = f"""
        WITH base AS (
            SELECT "url", status,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
        )
        SELECT "url",
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 3 AND w_rate >= 0.05 AND (b_total < 10 OR w_rate >= b_rate * 2 + 0.05)
        ORDER BY (w_rate - {coalesce("b_rate", "0")}) DESC LIMIT 15
    """


def _build_ip_spread_sql(col: str, *, include_ratio: bool = False) -> str:
    ratio_expr = f"""w_ips * 1.0 / GREATEST({coalesce("b_ips", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1)"""
    extra_sel = f",\n            {ratio_expr} AS ip_ratio" if include_ratio else ""
    order_by = "ip_ratio" if include_ratio else f"({ratio_expr})"
    return f"""
        WITH base AS (
            SELECT "{col}", "ip",
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE "{col}" IS NOT NULL AND "{col}" != ''
        ),
        agg AS (
            SELECT "{col}",
                COUNT(DISTINCT "ip") FILTER (WHERE is_w) AS w_ips,
                COUNT(*) FILTER (WHERE is_w) AS w_reqs,
                COUNT(DISTINCT "ip") FILTER (WHERE is_b) AS b_ips
            FROM base GROUP BY "{col}"
            HAVING COUNT(DISTINCT "ip") FILTER (WHERE is_w) >= 5
        )
        SELECT "{col}", w_ips, w_reqs, {coalesce("b_ips", "0")} AS b_ips{extra_sel}
        FROM agg
        WHERE w_ips > {coalesce("b_ips", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3
        ORDER BY {order_by} DESC LIMIT 10
    """


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


def _build_new_dim_traffic_sql(col: str, min_w_cnt: int) -> str:
    return f"""
        SELECT "{col}",
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
        FROM {{table_name}} WHERE "{col}" IS NOT NULL GROUP BY "{col}"
        HAVING w_cnt >= {min_w_cnt} AND b_cnt = 0
        ORDER BY w_cnt DESC LIMIT 20
    """


NEW_COUNTRY_TRAFFIC = _build_new_dim_traffic_sql("country", 3)
CITY_SURGES = f"""
        SELECT {{label_expr}} AS label, "city", {{region_sel}}, {{country_sel}},
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
            COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt,
            w_cnt * 1.0 / GREATEST({coalesce("b_cnt", "0")} * 1.0 / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1.0) AS spike_ratio
        FROM {{table_name}}
        WHERE "city" IS NOT NULL AND "city" != ''
        GROUP BY {{loc_cols}}, label, "city", {{region_sel}}, {{country_sel}}
        HAVING w_cnt >= 20 AND w_cnt > {coalesce("b_cnt", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3
        ORDER BY spike_ratio DESC LIMIT 15
    """
CITY_ERROR_SPIKES = f"""
        WITH base AS (
            SELECT {{loc_cols}}, {{label_expr}} AS label, status, "city", {{region_sel}} AS region, {{country_sel}} AS country,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE "city" IS NOT NULL AND "city" != ''
        )
        SELECT label, "city", region, country,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_errors,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY ALL
        HAVING w_total >= 10 AND w_rate >= 0.10 AND (b_total < 50 OR w_rate >= b_rate * 3 + 0.05)
        ORDER BY (w_rate - {coalesce("b_rate", "0")}) DESC LIMIT 15
    """
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
                CAST({coalesce("SUM(w_cnt) OVER ()", "0")} AS BIGINT) AS w_total,
                CAST({coalesce("SUM(b_cnt) OVER ()", "0")} AS BIGINT) AS b_total
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
WAF_SIGNAL_SPIKES = f"""
        WITH waf_combos AS (
            SELECT "waf_sig",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{waf_table}}
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
            SUM(w_cnt) * 1.0 / GREATEST({coalesce("SUM(b_cnt)", "0")} * 1.0 / {{baseline_hours}} * {{window_hours}}, 0.5) AS spike_ratio
        FROM unrolled
        WHERE signal != '' AND signal != 'BOT-ANALYSIS'
        GROUP BY signal
        HAVING SUM(w_cnt) >= 3 AND SUM(w_cnt) > {coalesce("SUM(b_cnt)", "0")} * 1.0 / {{baseline_hours}} * {{window_hours}} * 2 + 2
        ORDER BY spike_ratio DESC LIMIT 15
    """
PROXY_SURGE = f"""
        WITH all_p AS (
            SELECT "p_type",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{table_name}} WHERE "p_type" IS NOT NULL GROUP BY "p_type"
        ),
        totals AS (
            SELECT
                {coalesce("SUM(w_cnt) FILTER (WHERE \"p_type\" != '')", "0")} AS w_proxy_total,
                {coalesce("SUM(b_cnt) FILTER (WHERE \"p_type\" != '')", "0")} AS b_proxy_total,
                {coalesce("SUM(w_cnt)", "0")} AS w_total_all,
                {coalesce("SUM(b_cnt)", "0")} AS b_total_all
            FROM all_p
        )
        SELECT b."p_type", b.w_cnt, b.b_cnt, t.w_total_all, t.b_total_all
        FROM all_p b, totals t
        WHERE b."p_type" != ''
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= 5
          AND (t.w_proxy_total * 100.0 / NULLIF(t.w_total_all, 0)) >= (t.b_proxy_total * 100.0 / NULLIF(t.b_total_all, 0)) * 2 + 5
    """
ASN_CONCENTRATION = _build_monoculture_sql("asn", "0.20", '"asn" IS NOT NULL')
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
CACHE_COLLAPSE = f"""
        WITH base AS (
            SELECT "url", cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
        )
        SELECT "url",
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b), 0) AS b_rate,
            SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) AS w_total,
            SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 5 AND b_total >= 20 AND b_rate >= 0.40 AND w_rate <= b_rate - 0.20 AND w_rate <= b_rate * 0.6
        ORDER BY ({coalesce("b_rate", "0")} - w_rate) DESC LIMIT 15
    """
CACHEABILITY_REGRESSION = f"""
        WITH base AS (
            SELECT "url", cache,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
        )
        SELECT "url",
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_w) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_w), 0) AS w_rate,
            SUM(CASE WHEN starts_with(cache, 'PASS') THEN 1 ELSE 0 END) FILTER (WHERE is_b) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE is_b), 0) AS b_rate,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "url"
        HAVING w_total >= 10 AND b_total >= 50 AND b_rate <= 0.20 AND w_rate >= 0.50 AND w_rate >= b_rate + 0.30
        ORDER BY (w_rate - {coalesce("b_rate", "0")}) DESC LIMIT 15
    """


def _build_p95_latency_regression_sql(
    col: str, min_w: int, min_b: int, ratio_mult: str, diff_floor: int, extra_where: str = ""
) -> str:
    where_extra = f"{extra_where} AND " if extra_where else ""
    return f"""
        WITH base AS (
            SELECT "{col}", elapsed,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE {where_extra}elapsed IS NOT NULL
        )
        SELECT "{col}",
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_w) / 1000.0 AS w_p95,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY elapsed) FILTER (WHERE is_b) / 1000.0 AS b_p95,
            COUNT(*) FILTER (WHERE is_w) AS w_total,
            COUNT(*) FILTER (WHERE is_b) AS b_total
        FROM base GROUP BY "{col}"
        HAVING w_total >= {min_w} AND b_total >= {min_b} AND w_p95 >= b_p95 * {ratio_mult} AND w_p95 - b_p95 >= {diff_floor}
        ORDER BY (w_p95 / NULLIF(b_p95, 0)) DESC LIMIT 15
    """


LATENCY_REGRESSION = _build_p95_latency_regression_sql("url", 5, 20, "2.0", 200)
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
CIPHER_SPREAD = _build_ip_spread_sql("tls_ciphers_sha")


def _build_ip_max_vs_p95_sql(
    col: str, alias_suffix: str, min_w: int, extra_having: str = "", extra_where: str = ""
) -> str:
    max_alias = f"max_{alias_suffix}"
    avg_alias = f"avg_{alias_suffix}"
    return f"""
        WITH base AS (
            SELECT "ip", {col},
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}} WHERE {col} > 0
        ),
        win_ips AS (
            SELECT "ip",
                MAX({col}) AS {max_alias},
                AVG({col}) AS {avg_alias},
                COUNT(*) AS w_total
            FROM base WHERE is_w GROUP BY "ip"
            HAVING COUNT(*) >= {min_w}{extra_having}
        ),
        stats AS (
            SELECT w."ip", w.{max_alias}, w.{avg_alias}, w.w_total,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY b.{col}) AS b_p95
            FROM win_ips w JOIN base b ON w."ip" = b."ip" AND b.is_b
            GROUP BY w."ip", w.{max_alias}, w.{avg_alias}, w.w_total
        )
        SELECT "ip", {max_alias}, {avg_alias}, w_total, b_p95 FROM stats
        WHERE w_total >= {min_w} AND {max_alias} > b_p95 * 3{extra_where}
        ORDER BY {max_alias} DESC LIMIT 15
    """


REQUEST_SIZE_ANOMALY = _build_ip_max_vs_p95_sql("req_header_bytes", "bytes", 3)
CONNECTION_ABUSE = _build_ip_max_vs_p95_sql(
    "conn_requests", "reqs", 5, " AND MAX(conn_requests) >= 50", " AND max_reqs >= 50"
)
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
ORIGIN_RETRIES = """
        SELECT {url_col}, COUNT(*) AS requests, ROUND(AVG("oretries"), 2) AS avg_retries, MAX("oretries") AS max_retries
        FROM {table_name}
        WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND "oretries" > 0
        GROUP BY {url_col} HAVING requests >= 5
        ORDER BY avg_retries DESC LIMIT 10
    """
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
SHIELD_PATH_DEGRADATION = f"""
        WITH logs AS (SELECT "rid", "prid", "pop", "ottfb", "edge", timestamp FROM {{table_name}} WHERE ottfb IS NOT NULL),
        edge_logs AS (SELECT rid, pop, ottfb, timestamp FROM logs WHERE edge = true),
        shield_logs AS (SELECT prid, pop, ottfb, timestamp FROM logs WHERE edge = false AND prid IS NOT NULL AND prid != ''),
        joined AS (
            SELECT e.pop AS edge_pop, {coalesce("s.pop", "'Direct to Origin'")} AS shield_pop, (e.ottfb - {coalesce("s.ottfb", "0")}) / 1000.0 AS transit_ms, e.timestamp
            FROM edge_logs e LEFT JOIN shield_logs s ON s.prid = e.rid
        )
        SELECT edge_pop, shield_pop,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_p50,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY transit_ms) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_p50,
            COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt
        FROM joined GROUP BY 1, 2 HAVING w_cnt >= 5 AND w_p50 > b_p50 * 1.5
        ORDER BY (w_p50 / NULLIF(b_p50, 0)) DESC LIMIT 20
    """
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


def _build_repeated_patterns_sql(
    *, key_col: str, key_select: str, key_where: str, div_select: str, div_col: str, div_alias: str
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
REPEATED_PATTERNS_FP = _build_repeated_patterns_sql(
    key_col="entity",
    key_select=coalesce('"{fp_col}"', "''"),
    key_where='"{fp_col}" IS NOT NULL AND "{fp_col}" != \'\'',
    div_select='"ip"',
    div_col="ip",
    div_alias="distinct_ip",
)
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
           AND w_denied > {coalesce("b_denied", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 3 + 5
        ORDER BY w_denied DESC
        LIMIT 15
    """
NETWORK_ASN_HEALTH = f"""
        WITH base AS (
            SELECT "asn" AS asn, ploss, rtt_var, retrans,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
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
                ({coalesce("w_ploss", "0")} >= 0.02 AND {coalesce("w_ploss", "0")} >= {coalesce("b_ploss", "0")} * 2 + 0.005)
             OR (w_jitter >= b_jitter * 2 AND w_jitter - b_jitter >= 20000)
             OR ({coalesce("w_retrans", "0")} >= {coalesce("b_retrans", "0")} * 2 + 0.5)
           )
        ORDER BY {coalesce("w_ploss", "0")} DESC, w_jitter DESC
        LIMIT 15
    """
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
REFERER_MONOCULTURE = _build_monoculture_sql("referer", "0.20", '"referer" IS NOT NULL AND "referer" != \'\'')
METHOD_DRIFT = f"""
        WITH grp AS (
            SELECT "method",
                COUNT(*) FILTER (WHERE timestamp >= CAST(? AS TIMESTAMPTZ)) AS w_cnt,
                COUNT(*) FILTER (WHERE timestamp < CAST(? AS TIMESTAMPTZ)) AS b_cnt
            FROM {{table_name}}
            GROUP BY "method"
        ),
        with_totals AS (
            SELECT "method", w_cnt, b_cnt,
                CAST({coalesce("SUM(w_cnt) OVER ()", "0")} AS BIGINT) AS w_total,
                CAST({coalesce("SUM(b_cnt) OVER ()", "0")} AS BIGINT) AS b_total
            FROM grp
        )
        SELECT "method", w_cnt, b_cnt, w_total, b_total
        FROM with_totals
        WHERE "method" IS NOT NULL AND "method" != '' AND upper("method") NOT IN ('GET', 'HEAD', 'OPTIONS')
          AND w_total > 0 AND w_cnt >= 20 AND w_cnt * 1.0 / w_total >= 0.10
          AND (b_total = 0 OR w_cnt * 1.0 / w_total >= b_cnt * 1.0 / NULLIF(b_total, 0) * 2 + 0.05)
        ORDER BY w_cnt DESC LIMIT 10
    """
NEW_ASN_TRAFFIC = _build_new_dim_traffic_sql("asn", 20)
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
POP_LATENCY_REGRESSION = _build_p95_latency_regression_sql(
    "pop", 20, 50, "1.5", 100, '"pop" IS NOT NULL AND "pop" != \'\''
)
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
                (NOT regexp_matches(lower({coalesce('t."resp_header_content_encoding"', "''")}), 'gzip|br|zstd|deflate')) AS uncompressed,
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
        ORDER BY (w_rate - {coalesce("b_rate", "0")}) DESC LIMIT 15
    """
SESSION_HARVESTING = f"""
        WITH base AS (
            SELECT "ip" AS ip, "cookie_session" AS cs,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
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
            {coalesce("b_sessions", "0")} AS b_sessions
        FROM agg
        WHERE w_sessions > {coalesce("b_sessions", "0")} * 2 + 10
        ORDER BY w_sessions DESC LIMIT 15
    """
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
_COALESCED_URL_CORE_COLS = """COUNT(*) FILTER (WHERE is_w) AS w_total,
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
              FILTER (WHERE is_w AND elapsed IS NOT NULL) / 1000.0, 0) AS w_p50"""
COALESCED_URL_AGGREGATES = f"""
    WITH base AS (
        SELECT "url", status, cache, elapsed,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} WHERE "url" IS NOT NULL
    )
    SELECT "url",
        -- Common counts
        {_COALESCED_URL_CORE_COLS}
    FROM base GROUP BY "url" HAVING COUNT(*) FILTER (WHERE is_w) >= 3
    """
COALESCED_URL_EXTENDED_AGGREGATES = f"""
    WITH url_flags AS (
        SELECT "url", is_probe, is_comp, is_img
        FROM (
            SELECT "url",
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
        SELECT t."url" AS url, t.status, t.cache, t.elapsed,
            {{resp_bytes_sel}} AS resp_bytes, {{uncompressed_sel}} AS uncompressed,
            {{is_mobile_sel}} AS is_mobile, {{ttl_sel}} AS ttl, {{hits_sel}} AS hits,
            {{age_sel}} AS age, {{oretries_sel}} AS oretries,
            {coalesce("u.is_probe", "FALSE")} AS is_probe,
            {coalesce("u.is_comp", "FALSE")} AS is_comp,
            {coalesce("u.is_img", "FALSE")} AS is_img,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."url" IS NOT NULL
    )
    SELECT url,
        {_COALESCED_URL_CORE_COLS},
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


def _ip_sec_core_cols(ug: str = "") -> str:
    return f"""-- low_and_slow: probe paths over the WHOLE span (no window split)
        COUNT(*) FILTER (WHERE {ug}is_sensitive) AS ls_hits,
        COUNT(DISTINCT url) FILTER (WHERE {ug}is_sensitive) AS ls_distinct,
        MIN(sec) FILTER (WHERE {ug}is_sensitive) AS ls_min_sec,
        MAX(sec) FILTER (WHERE {ug}is_sensitive) AS ls_max_sec,
        -- credential_enumeration: auth paths, window/baseline 401/403
        COUNT(*) FILTER (WHERE {ug}is_auth AND is_w AND status IN (401, 403)) AS ce_w_denied,
        COUNT(*) FILTER (WHERE {ug}is_auth AND is_w) AS ce_w_attempts,
        COUNT(DISTINCT url) FILTER (WHERE {ug}is_auth AND is_w) AS ce_w_paths,
        COUNT(*) FILTER (WHERE {ug}is_auth AND is_b AND status IN (401, 403)) AS ce_b_denied,
        -- content_discovery: all URLs, window/baseline 404s
        COUNT(*) FILTER (WHERE {ug}is_w AND status = 404) AS cd_w_404,
        COUNT(*) FILTER (WHERE {ug}is_w) AS cd_w_total,
        COUNT(DISTINCT url) FILTER (WHERE {ug}is_w AND status = 404) AS cd_distinct_404,
        COUNT(*) FILTER (WHERE {ug}is_b AND status = 404) AS cd_b_404"""


COALESCED_IP_SECURITY_AGGREGATES = f"""
    WITH url_flags AS (
        SELECT "url", is_sensitive, is_auth
        FROM (
            SELECT "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
                regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_sensitive OR is_auth
    ),
    base AS (
        SELECT t."ip" AS ip, t."url" AS url, t.status, epoch(t."timestamp")::BIGINT AS sec,
            {coalesce("u.is_sensitive", "FALSE")} AS is_sensitive,
            {coalesce("u.is_auth", "FALSE")} AS is_auth,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."ip" IS NOT NULL AND t."ip" <> '' AND t."url" IS NOT NULL
    )
    SELECT ip,
        {_ip_sec_core_cols()}
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
            SELECT "url",
                regexp_matches("url", '{NEW_PROBE_REGEX}', 'i') AS is_sensitive,
                regexp_matches("url", '{AUTH_PATHS_REGEX}', 'i') AS is_auth,
                ("url" LIKE '/%/%/%' AND "url" NOT LIKE '%.%') AS is_deep
            FROM (SELECT DISTINCT "url" FROM {{table_name}} WHERE "url" IS NOT NULL)
        )
        WHERE is_sensitive OR is_auth OR is_deep
    ),
    base AS (
        SELECT t."ip" AS ip, t."url" AS url, t.status, epoch(t."timestamp")::BIGINT AS sec,
            {{no_referer_sel}} AS no_referer, {{cs_sel}} AS cs,
            {{req_header_bytes_sel}} AS req_header_bytes, {{conn_requests_sel}} AS conn_requests,
            {coalesce("u.is_sensitive", "FALSE")} AS is_sensitive,
            {coalesce("u.is_auth", "FALSE")} AS is_auth,
            {coalesce("u.is_deep", "FALSE")} AS is_deep,
            (t.timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (t.timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}} t
        LEFT JOIN url_flags u ON t."url" = u."url"
        WHERE t."ip" IS NOT NULL AND t."ip" <> ''
    )
    SELECT ip,
        {_ip_sec_core_cols("url IS NOT NULL AND ")},
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
COALESCED_SCALAR_EXTENDED_AGGREGATES = f"""
    WITH base AS (
        SELECT
            {{transport_sel}} AS transport,
            {{cache_sel}} AS cache,
            {{oconnect_ms_sel}} AS oconnect_ms,
            {{ottfb_sel}} AS ottfb,
            (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
            (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
        FROM {{table_name}}
    )
    SELECT
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS w_quic,
        COUNT(*) FILTER (WHERE is_w AND transport IS NOT NULL AND transport != '') AS w_tr_total,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '' AND lower(transport) = 'quic') AS b_quic,
        COUNT(*) FILTER (WHERE is_b AND transport IS NOT NULL AND transport != '') AS b_tr_total,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_w)", "0")} AS w_hits,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_w)", "0")} AS w_cacheable,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') THEN 1 ELSE 0 END) FILTER (WHERE is_b)", "0")} AS b_hits,
        {coalesce("SUM(CASE WHEN starts_with(cache, 'HIT') OR starts_with(cache, 'MISS') THEN 1 ELSE 0 END) FILTER (WHERE is_b)", "0")} AS b_cacheable,
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
CRAWLER_DISPARITY = rf"""
        WITH ua_agg AS (
            SELECT
                ua,
                COUNT(*) FILTER (WHERE is_w) AS w_cnt,
                COUNT(*) FILTER (WHERE is_b) AS b_cnt,
                CAST({coalesce(f"SUM({coalesce('resp_bytes', '0')}) FILTER (WHERE is_w)", "0")} AS BIGINT) AS w_bytes,
                CAST({coalesce(f"SUM({coalesce('resp_bytes', '0')}) FILTER (WHERE is_b)", "0")} AS BIGINT) AS b_bytes
            FROM (
                SELECT ua, resp_bytes,
                    (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w,
                    (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b
                FROM {{table_name}}
            )
            GROUP BY ua
        ),
        site_win AS (
            SELECT
                GREATEST(CAST({coalesce("SUM(w_bytes)", "0")} AS BIGINT), 1) AS total_site_bytes
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
                regexp_extract(l_ua, 'http[s]*://(www\.)*([^/.:;\s)]+)', 2) AS ua_dom,
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
                {coalesce("stem", "raw_tok")} AS bot_stem,
                MIN(raw_tok) FILTER (WHERE w_cnt > 0) AS raw_tok,
                MAX(ua_dom) FILTER (WHERE w_cnt > 0) AS ua_dom,
                CAST({coalesce("SUM(w_cnt)", "0")} AS BIGINT) AS w_crawls,
                CAST({coalesce("SUM(w_bytes)", "0")} AS BIGINT) AS w_bytes,
                CAST({coalesce("SUM(b_cnt)", "0")} AS BIGINT) AS b_crawls,
                CAST({coalesce("SUM(b_bytes)", "0")} AS BIGINT) AS b_bytes
            FROM ua_info
            WHERE is_crawler
            GROUP BY {coalesce("stem", "raw_tok")}
            HAVING SUM(w_cnt) >= 50
        ),
        ref_info AS (
            SELECT
                referer,
                NULLIF(regexp_extract(l_ref, 'http[s]*://(www\.)*([^/.:;\s]+)', 2), '') AS ref_dom
            FROM (
                SELECT referer, lower(referer) AS l_ref
                FROM (
                    SELECT DISTINCT referer
                    FROM {{table_name}}
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
                {coalesce("NULLIF(u.user_fetcher_stem, '')", "r.ref_dom")} AS ref_stem
            FROM {{table_name}} t
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
            {coalesce("r.w_referrals", "0")} AS w_referrals,
            c.w_bytes,
            c.w_crawls * 1.0 / GREATEST({coalesce("r.w_referrals", "0")}, 1) AS crawls_per_referral,
            c.b_crawls * 1.0 / GREATEST({coalesce("r.b_referrals", "0")}, 1) AS baseline_crawls_per_referral,
            c.w_bytes * 1.0 / GREATEST({coalesce("r.w_referrals", "0")}, 1) AS bytes_per_referral,
            c.b_bytes * 1.0 / GREATEST({coalesce("r.b_referrals", "0")}, 1) AS baseline_bytes_per_referral,
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
STALE_BROWSER_VERSION = rf"""
        WITH distinct_uas AS (
            SELECT ua, lower(ua) AS l_ua
            FROM (SELECT DISTINCT ua FROM {{table_name}} WHERE ua IS NOT NULL AND contains(lower(ua), 'chrome'))
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
            FROM {{table_name}} t
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
            w_reqs * 1.0 / GREATEST({coalesce("b_reqs", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1.0) AS surge_ratio
        FROM version_aggregates
        WHERE CAST(chrome_version AS INTEGER) < 131
          AND w_reqs >= 500
          AND w_reqs > {coalesce("b_reqs", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}} * 2.5
        ORDER BY surge_ratio DESC LIMIT 10
    """
ORPHANED_DEEP_CRAWL = f"""
        WITH deep_requests AS (
            SELECT
                ip,
                url,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
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
            {coalesce("b_reqs", "0")} AS b_reqs,
            w_reqs * 1.0 / GREATEST(w_distinct_urls, 1.0) AS requests_per_url,
            w_reqs * 1.0 / GREATEST({coalesce("b_reqs", "0")} / GREATEST({{baseline_hours}}, 1.0) * {{window_hours}}, 1.0) AS surge_ratio
        FROM agg
        ORDER BY w_reqs DESC LIMIT 15
    """
RESIDENTIAL_FINGERPRINT_DISPERSION = f"""
        WITH fingerprint_asn_maps AS (
            SELECT
                ja4,
                ip,
                asn,
                (timestamp < CAST(? AS TIMESTAMPTZ)) AS is_b,
                (timestamp >= CAST(? AS TIMESTAMPTZ)) AS is_w
            FROM {{table_name}}
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
            {coalesce("b_ips", "0")} AS b_ips,
            w_asns * 1.0 / GREATEST({coalesce("b_asns", "0")}, 1.0) AS asn_expansion_ratio
        FROM agg
        ORDER BY asn_expansion_ratio DESC LIMIT 10
    """
__all__ = [
    "NEW_PROBES",
    "NEW_PROBE_REGEX",
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
    "COALESCED_CITY_AGGREGATES",
    "COALESCED_URL_AGGREGATES",
    "COALESCED_URL_EXTENDED_AGGREGATES",
    "COALESCED_IP_SECURITY_AGGREGATES",
    "COALESCED_IP_EXTENDED_AGGREGATES",
    "COALESCED_ASN_AGGREGATES",
    "COALESCED_SCALAR_EXTENDED_AGGREGATES",
    "COALESCED_FP_AGGREGATES",
    "COALESCED_DIM_AGGREGATES",
    "COALESCED_GEO_LATENCY_AGGREGATES",
]


def _mask_ip_if_needed(ip: Any, context: dict) -> Any:
    return mask_ip(ip) if (context.get("mask_ips") and ip) else ip


def _fmt_asn_label(asn: Any, context: dict) -> str:
    names_map = context.get("asn_names", {})
    return f"AS{asn} ({names_map[asn]})" if asn in names_map else f"AS{asn}"


def _fmt_metro_label(metro: Any, context: dict) -> str:
    return context.get("dma_map", {}).get(str(metro)) or f"DMA {metro}"


def _scale_baseline(val: Any, context: dict) -> float:
    return float(val) / max(context["baseline_hours"], 1) * context["window_hours"]


def _item(
    label: Any,
    current_val: Any,
    baseline_val: Any,
    unit: str,
    meta: dict[str, Any],
    severity: str,
    *,
    baseline_label: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "label": label,
        "current_val": current_val,
        "baseline_val": baseline_val,
        "unit": unit,
        "meta": meta,
        "severity": severity,
    }
    if baseline_label is not None:
        out["baseline_label"] = baseline_label
    return out


def _p95_latency_item(
    label: str, w_p95: Any, b_p95: Any, reqs: Any, filters: dict[str, Any], *, reqs_key: str = "window_requests"
) -> dict[str, Any]:
    w_f, b_f = float(w_p95 or 0), float(b_p95 or 0)
    return _item(
        label,
        w_f,
        b_f,
        "ms (P95)",
        {"regression_ratio": float(w_p95 / b_p95) if b_p95 else 0, reqs_key: reqs, "filters": filters},
        "critical" if w_f >= 5000 else "warning",
    )


def _traffic_share_rates(w_cnt: Any, w_total: Any, b_cnt: Any, b_total: Any) -> tuple[float, float]:
    return (float(w_cnt * 100.0 / w_total) if w_total else 0.0), (float(b_cnt * 100.0 / b_total) if b_total else 0.0)


def _error_rate_spike_item(
    label: str, w_rate: Any, b_rate: Any, w_errors: Any, w_total: Any, unit: str, filters: dict[str, Any]
) -> dict[str, Any]:
    return _item(
        label,
        float(w_rate or 0) * 100,
        float(b_rate or 0) * 100,
        unit,
        {"requests": w_total, "errors": w_errors, "filters": filters},
        "critical" if (w_rate or 0) >= 0.5 else "warning",
    )


def _monoculture_processor(row: tuple, filter_key: str) -> dict:
    val, w_cnt, b_cnt, b_total, w_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        val or "(empty)",
        w_rate,
        b_rate,
        "% of traffic",
        {"requests": w_cnt, "filters": {filter_key: val}},
        "critical" if w_rate >= 50 else "warning",
    )


def _url_rate_regression_processor(row: tuple, unit: str, *, is_critical: bool) -> dict:
    return _item(
        row[0] or "(empty)",
        float(row[1] or 0) * 100,
        float(row[2] or 0) * 100,
        unit,
        {"window_requests": row[3], "baseline_requests": row[4], "filters": {"url": row[0]}},
        "critical" if is_critical else "warning",
    )


def _ip_max_vs_p95_processor(
    row: tuple, context: dict, *, unit: str, max_key: str, avg_key: str, crit_thresh: int
) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    max_val, avg_val, b_p95 = int(row[1] or 0), int(row[2] or 0), int(row[4] or 0)
    return _item(
        ip or "(unknown)",
        max_val,
        b_p95,
        unit,
        {max_key: max_val, avg_key: avg_val, "requests": row[3], "p95_baseline": b_p95, "filters": {"ip": ip}},
        "critical" if (row[1] or 0) > crit_thresh else "warning",
        baseline_label="P95 baseline",
    )


def error_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _error_rate_spike_item(row[0] or "(empty)", row[1], row[2], row[3], row[4], "% 5xx", {"url": row[0]})


_reg(
    id="error_spikes",
    category=InsightCategory.origin,
    title="Error Spikes",
    description="URLs with abnormally elevated 5xx error rates in the window vs. baseline",
    sql_template=SQL.ERROR_SPIKES,
    required_fields=["url", "status", "timestamp"],
    row_processor=error_spikes_processor,
)


def botnet_grouping_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    fp_col = context.get("fp_col", "ja4")
    return _item(
        row[0],
        row[1],
        row[3],
        "distinct IPs",
        {"requests": row[2], "ip_ratio": round(float(row[4]), 1), "filters": {fp_col: row[0]}},
        "critical" if row[1] >= 50 else "warning",
    )  # Raw baseline IPS


_reg(
    id="botnet_grouping",
    category=InsightCategory.security,
    title="Botnet Grouping",
    description="TLS fingerprints (JA3/JA4) using far more distinct IPs than their baseline",
    sql_template=SQL.BOTNET_GROUPING,
    required_fields=["ip", "timestamp"],
    row_processor=botnet_grouping_processor,
)


def _city_filters(row: tuple) -> dict:
    return {"city": row[1], "region": row[2] if row[2] else None, "country": row[3]}


def _zero_baseline_count_item(label: str, count: int, meta: dict, warn_threshold: int) -> dict:
    return _item(label, count, 0, "requests", meta, "warning" if count >= warn_threshold else "info")


def new_country_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _zero_baseline_count_item(format_city_label("", row[0]), row[1], {"filters": {"country": row[0]}}, 10)


_reg(
    id="new_country_traffic",
    category=InsightCategory.traffic,
    title="New Country Traffic",
    description="Countries that appeared in the window but had zero requests in the baseline",
    sql_template=SQL.NEW_COUNTRY_TRAFFIC,
    required_fields=["country", "timestamp"],
    row_processor=new_country_traffic_processor,
)


def city_surges_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        format_city_label(row[1], row[3], row[2]),
        row[4],
        _scale_baseline(row[5], context),
        "requests",
        {"spike_ratio": round(float(row[6]), 1), "filters": _city_filters(row)},
        "info" if float(row[6]) < 10 else "warning",
    )


_reg(
    id="city_surges",
    category=InsightCategory.traffic,
    title="City Traffic Surges",
    description="Cities experiencing a significant spike in traffic compared to their baseline",
    sql_template=SQL.CITY_SURGES,
    required_fields=["city", "timestamp"],
    row_processor=city_surges_processor,
)


def city_error_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _error_rate_spike_item(
        format_city_label(row[1], row[3], row[2]), row[4], row[5], row[6], row[7], "% error", _city_filters(row)
    )


_reg(
    id="city_error_spikes",
    category=InsightCategory.origin,
    title="City Error Spikes",
    description="Cities with abnormally high error rates in the window vs. baseline",
    sql_template=SQL.CITY_ERROR_SPIKES,
    required_fields=["city", "status", "timestamp"],
    row_processor=city_error_spikes_processor,
)


def city_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _p95_latency_item(
        format_city_label(row[1], row[3], row[2]), row[4], row[5], row[6], _city_filters(row), reqs_key="requests"
    )


_reg(
    id="city_latency_regressions",
    category=InsightCategory.edge,
    title="City Latency Regressions",
    description="Cities experiencing significant increases in P95 latency",
    sql_template=SQL.CITY_LATENCY_REGRESSIONS,
    required_fields=["city", "elapsed", "timestamp"],
    row_processor=city_latency_processor,
)


def new_city_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _zero_baseline_count_item(
        format_city_label(row[1], row[3], row[2]), row[4], {"filters": _city_filters(row)}, 50
    )


_reg(
    id="new_city_traffic",
    category=InsightCategory.traffic,
    title="New City Traffic",
    description="Cities that recently started sending traffic after a period of zero activity",
    sql_template=SQL.NEW_CITY_TRAFFIC,
    required_fields=["city", "timestamp"],
    row_processor=new_city_traffic_processor,
)


def ua_monoculture_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _monoculture_processor(row, "ua")


_reg(
    id="ua_monoculture",
    category=InsightCategory.traffic,
    title="User-Agent Monoculture",
    description="User-agents with an unusually high and spiking share of total traffic",
    sql_template=SQL.UA_MONOCULTURE,
    required_fields=["ua", "timestamp"],
    row_processor=ua_monoculture_processor,
)


def new_probe_urls_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        row[0],
        row[1],
        None,
        "requests",
        {"error_pct": float(row[3]) if row[3] is not None else 0, "filters": {"url": row[0]}},
        "critical" if row[1] >= 5 else "warning",
    )


_reg(
    id="new_probe_urls",
    category=InsightCategory.security,
    title="New Probe URLs",
    description="Common attack patterns and sensitive paths appearing for the first time",
    sql_template=SQL.NEW_PROBE_URLS,
    required_fields=["url", "status", "timestamp"],
    row_processor=new_probe_urls_processor,
)


def waf_signal_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        row[0],
        row[1],
        _scale_baseline(row[2], context),
        "hits",
        {"spike_ratio": float(row[3]), "filters": {"waf_sig_ind": row[0]}},
        "critical" if float(row[3]) >= 10 else "warning",
    )


_reg(
    id="waf_signal_spikes",
    category=InsightCategory.security,
    title="WAF Signal Spikes",
    description="Security signals from the Next-Gen WAF showing unusual activity",
    sql_template=SQL.WAF_SIGNAL_SPIKES,
    required_fields=["waf_sig", "timestamp"],
    row_processor=waf_signal_spikes_processor,
)


def proxy_surge_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    p_type, w_cnt, _b_cnt, w_total_all, _b_total_all = row
    return _item(
        p_type,
        round(w_cnt * 100.0 / max(w_total_all, 1), 1),
        None,
        "% of traffic",
        {"requests": w_cnt, "filters": {"p_type": p_type}},
        "warning",
    )


def proxy_surge_severity(items: list[dict]) -> str:
    return "warning" if items else "clean"


_reg(
    id="proxy_surge",
    category=InsightCategory.security,
    title="Anonymizing Proxy Surge",
    description="Significant increase in traffic from known VPNs and anonymizing proxies",
    sql_template=SQL.PROXY_SURGE,
    required_fields=["p_type", "timestamp"],
    row_processor=proxy_surge_processor,
    severity_logic=proxy_surge_severity,
)


def asn_concentration_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_cnt, b_cnt, b_total, w_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        _fmt_asn_label(asn, context),
        w_rate,
        b_rate,
        "% of traffic",
        {"requests": w_cnt, "asn": asn, "filters": {"asn": asn}},
        "critical" if w_rate >= 50 else "warning",
    )


_reg(
    id="asn_concentration",
    category=InsightCategory.network,
    title="ASN Concentration",
    description="Traffic spiking from specific Autonomous Systems (ISPs/Data Centers)",
    sql_template=SQL.ASN_CONCENTRATION,
    required_fields=["asn", "timestamp"],
    row_processor=asn_concentration_processor,
)


def asn_metro_performance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_f, b_f = row[0], float(row[2] or 0), float(row[3] or 0)
    return _item(
        f"{_fmt_asn_label(asn, context)} in {_fmt_metro_label(row[1], context)}",
        w_f,
        b_f,
        "ms RTT",
        {
            "regression_ratio": float(row[2] / row[3]) if row[3] else 0,
            "requests": row[4],
            "filters": {"asn": asn, "country": "US"},
        },
        "critical" if w_f >= b_f + 100 else "warning",
    )


_reg(
    id="asn_metro_performance",
    category=InsightCategory.network,
    title="ASN/Metro Performance Regressions",
    description="Specific ISP/Metro combinations showing significantly higher network latency than baseline",
    sql_template=SQL.ASN_METRO_PERFORMANCE,
    required_fields=["asn", "metro", "tcp_rtt", "country", "timestamp"],
    row_processor=asn_metro_performance_processor,
)


def cache_collapse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _url_rate_regression_processor(
        row, "% HIT", is_critical=bool((row[1] or 0) <= 0.10 and (row[2] or 0) >= 0.60)
    )


_reg(
    id="cache_collapse",
    category=InsightCategory.edge,
    title="Cache Efficiency Collapse",
    description="URLs whose cacheable hit ratio (HIT/(HIT+MISS)) dropped sharply vs. their baseline",
    sql_template=SQL.CACHE_COLLAPSE,
    required_fields=["url", "cache", "timestamp"],
    row_processor=cache_collapse_processor,
)


def cacheability_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _url_rate_regression_processor(
        row, "% PASS", is_critical=bool((row[1] or 0) >= 0.80 and (row[2] or 0) <= 0.10)
    )


_reg(
    id="cacheability_regression",
    category=InsightCategory.edge,
    title="Cacheability Regression",
    description="URLs that flipped from cacheable to mostly PASS (uncacheable) vs. their baseline",
    sql_template=SQL.CACHEABILITY_REGRESSION,
    required_fields=["url", "cache", "timestamp"],
    row_processor=cacheability_regression_processor,
)


def latency_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _p95_latency_item(row[0] or "(empty)", row[1], row[2], row[3], {"url": row[0]})


_reg(
    id="latency_regression",
    category=InsightCategory.edge,
    title="Latency Regression",
    description="Endpoints showing significantly slower P95 response times than baseline",
    sql_template=SQL.LATENCY_REGRESSION,
    required_fields=["url", "elapsed", "timestamp"],
    row_processor=latency_regression_processor,
)


def impossible_distance_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    sample_ip = _mask_ip_if_needed(row[6], context)
    meta = {
        "hits": row[1],
        "excess_km": round(float(row[2]), 0),
        "pop": row[5],
        "sample_ip": sample_ip,
        "client_lat": row[7],
        "client_lon": row[8],
        "pop_lat": row[9],
        "pop_lon": row[10],
        "tcp_rtt": row[11],
        "country": row[12],
        "city": row[13],
        "filters": {context.get("fp_col", "ja3"): row[0]},
    }
    return _item(
        row[0],
        round(float(row[3]), 0),
        round(float(row[4]), 0),
        "km",
        meta,
        "critical" if float(row[2]) > 5000 else "warning",
        baseline_label="max allowed",
    )


_reg(
    id="impossible_distance",
    category=InsightCategory.security,
    title="Impossible Distance / Spoofing",
    description="Traffic where the network latency (RTT) is physically too low for the reported client distance",
    sql_template=SQL.IMPOSSIBLE_DISTANCE,
    required_fields=["pop", "lat", "lon", "tcp_rtt", "timestamp"],
    row_processor=impossible_distance_processor,
)


def tail_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    p99, p50, ratio = float(row[1]), float(row[2]), float(row[3])
    return _item(
        row[0] or "(empty)",
        p99,
        p50,
        "ms (P99 vs P50)",
        {"p99_ms": p99, "p50_ms": p50, "ratio": ratio, "requests": row[4], "filters": {"url": row[0]}},
        "critical" if ratio > 10 else "warning",
        baseline_label="P50",
    )


_reg(
    id="tail_latency",
    category=InsightCategory.edge,
    title="Tail Latency Anomaly",
    description="Endpoints where P99 latency is more than 5× higher than P50, indicating major outliers",
    sql_template=SQL.TAIL_LATENCY,
    required_fields=["url", "elapsed", "timestamp"],
    row_processor=tail_latency_processor,
)


def cipher_spread_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    label = (row[0] or "")[:12] + "…" if row[0] and len(row[0]) > 12 else (row[0] or "(unknown)")
    return _item(
        label,
        row[1],
        round(_scale_baseline(row[3], context), 1),
        "distinct IPs",
        {"requests": row[2], "baseline_total_ips": row[3], "sha": row[0], "filters": {"tls_ciphers_sha": row[0]}},
        "critical" if row[1] >= 50 else "warning",
    )


_reg(
    id="cipher_spread",
    category=InsightCategory.security,
    title="Cipher Fingerprint Clustering",
    description="TLS cipher suites being used by a suspiciously large and spiking number of distinct IPs",
    sql_template=SQL.CIPHER_SPREAD,
    required_fields=["tls_ciphers_sha", "ip", "timestamp"],
    row_processor=cipher_spread_processor,
)


def request_size_anomaly_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _ip_max_vs_p95_processor(
        row, context, unit="bytes (max header)", max_key="max_bytes", avg_key="avg_bytes", crit_thresh=64000
    )


_reg(
    id="request_size_anomaly",
    category=InsightCategory.security,
    title="Oversized Request Headers",
    description="IPs sending headers significantly larger than their historical baseline, potential for DoS or exfiltration",
    sql_template=SQL.REQUEST_SIZE_ANOMALY,
    required_fields=["ip", "req_header_bytes", "timestamp"],
    row_processor=request_size_anomaly_processor,
)


def connection_abuse_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _ip_max_vs_p95_processor(
        row, context, unit="reqs/conn (max)", max_key="max_conn_reqs", avg_key="avg_conn_reqs", crit_thresh=500
    )


_reg(
    id="connection_abuse",
    category=InsightCategory.security,
    title="Connection Reuse Anomaly",
    description="IPs making an unusually high number of requests per single TCP connection",
    sql_template=SQL.CONNECTION_ABUSE,
    required_fields=["ip", "conn_requests", "timestamp"],
    row_processor=connection_abuse_processor,
)


def region_latency_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    item = _p95_latency_item(row[0] or "(unknown)", row[1], row[2], row[3], {"server_region": row[0]})
    if row[5] is not None:
        item["meta"]["ottfb_p95_ms"] = float(row[5])
    return item


_reg(
    id="region_latency",
    category=InsightCategory.network,
    title="Regional Latency Degradation",
    description="Geographic regions showing a significant increase in P95 latency compared to baseline",
    sql_template=SQL.REGION_LATENCY,
    required_fields=["server_region", "elapsed", "timestamp"],
    row_processor=region_latency_processor,
)


def cache_ttl_mismatch_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        str(row[0] or "(unknown)"),
        float(row[2]),
        None,
        "avg hits",
        {
            "avg_ttl_s": float(row[1]),
            "avg_hits": float(row[2]),
            "avg_age_s": float(row[3]),
            "samples": row[4],
            "filters": {"url": row[0]},
        },
        "warning",
    )


_reg(
    id="cache_ttl_mismatch",
    category=InsightCategory.edge,
    title="Cache TTL Inefficiency",
    description="URLs with high TTL but very low hit counts, potentially wasting cache space",
    sql_template=SQL.CACHE_TTL_MISMATCH,
    required_fields=["ttl", "hits", "age", "timestamp"],
    row_processor=cache_ttl_mismatch_processor,
)


def image_optimization_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    url, req_count, total_bytes, avg_kb, mob_ratio = row
    severity = (
        "critical"
        if total_bytes > 1024 * 1024 * 10 or avg_kb > 1024
        else ("warning" if total_bytes > 1024 * 1024 * 2 or (mob_ratio and mob_ratio > 0.5) else "info")
    )
    return _item(
        url,
        avg_kb,
        None,
        "KB avg",
        {
            "requests": req_count,
            "total_mb": round(total_bytes / (1024 * 1024), 2),
            "mobile_ratio": round(float(mob_ratio), 2) if mob_ratio is not None else 0,
            "filters": {"url": url},
        },
        severity,
    )


_reg(
    id="image_optimization_opportunities",
    category=InsightCategory.edge,
    title="Image Optimization Opportunities",
    description="Large images being served without modern compression (WebP/AVIF), especially to mobile users",
    sql_template=SQL.IMAGE_OPTIMIZATION_OPPORTUNITIES,
    required_fields=["url", "resp_bytes", "status", "timestamp", "ua"],
    row_processor=image_optimization_processor,
)


def origin_latency_spike_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    url, p95_ms, _w_p95, b_p95, requests = row
    return _item(
        url or "(unknown)",
        float(p95_ms),
        round(b_p95, 1),
        "ms P95",
        {"requests": requests, "filters": {"url": url}},
        "critical" if float(p95_ms) > b_p95 * 5 else "warning",
    )


_reg(
    id="origin_latency_spike",
    category=InsightCategory.origin,
    title="Origin Latency Spike",
    description="Sudden and significant increase in P95 response time from the origin server",
    sql_template=SQL.ORIGIN_LATENCY_SPIKE,
    required_fields=["ottfb", "timestamp"],
    row_processor=origin_latency_spike_processor,
)


def origin_error_rate_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    status, w_cnt, w_total, _b_total, w_5xx, _b_5xx = row
    return _item(
        f"HTTP {status}",
        round(w_cnt * 100.0 / w_total, 1),
        None,
        "%",
        {"filters": {"ost": status}},
        "critical" if (w_5xx * 100.0 / w_total) > 5 else "warning",
    )


_reg(
    id="origin_error_rate",
    category=InsightCategory.origin,
    title="Origin Error Rate",
    description="Significant increase in 5xx errors returned by the origin server",
    sql_template=SQL.ORIGIN_ERROR_RATE,
    required_fields=["ost", "timestamp"],
    row_processor=origin_error_rate_processor,
)


def origin_retries_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        row[0] or "(unknown)",
        float(row[2]),
        None,
        "avg retries",
        {"max_retries": int(row[3]), "filters": {"url": row[0]}},
        "warning",
    )


_reg(
    id="origin_retries",
    category=InsightCategory.origin,
    title="Origin Retries Elevated",
    description="URLs experiencing frequent retries when fetching from origin, indicating backend instability",
    sql_template=SQL.ORIGIN_RETRIES,
    required_fields=["oretries", "timestamp"],
    row_processor=origin_retries_processor,
)


def origin_ip_failure_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _item(
        row[0],
        float(row[2]),
        round(float(row[3]), 1),
        "% errors",
        {"requests": row[1], "filters": {"oip": row[0]}},
        "critical" if float(row[2]) > 20 else "warning",
    )


_reg(
    id="origin_ip_failure",
    category=InsightCategory.origin,
    title="Specific Origin IP Failing",
    description="One or more origin IP addresses are returning significantly more errors than their peers",
    sql_template=SQL.ORIGIN_IP_FAILURE,
    required_fields=["oip", "ost", "timestamp"],
    row_processor=origin_ip_failure_processor,
)


def shield_path_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    edge_pop, shield_pop, cur_p50, base_p50, reqs = row
    ratio = cur_p50 / base_p50 if base_p50 else 0
    return _item(
        f"{edge_pop} → {shield_pop}",
        float(cur_p50),
        float(base_p50),
        "ms P50",
        {"ratio": float(ratio), "requests": reqs},
        "critical" if ratio >= 3.0 else "warning",
    )


_reg(
    id="shield_path_degradation",
    category=InsightCategory.origin,
    title="Shield Path Degradation",
    description="Increased latency on the network path between edge POPs and shield POPs",
    sql_template=SQL.SHIELD_PATH_DEGRADATION,
    required_fields=["rid", "prid", "edge", "pop", "ottfb", "timestamp"],
    row_processor=shield_path_degradation_processor,
)


def _process_repeated_patterns(
    row: tuple, *, key_field: str, diversity_key: str, mask_key: bool, context: dict
) -> dict:
    entity = _mask_ip_if_needed(row[0], context) if mask_key else row[0]
    n_gaps, n_events = int(row[1] or 0), int(row[2] or 0)
    mean_interval, stddev = float(row[3] or 0), float(row[4] or 0)
    cv, modal_frac = float(row[5] or 0), float(row[6] or 0)
    diversity_val, span_s = int(row[7] or 0), int(row[8] or 0)
    mode_gap = int(row[9]) if row[9] is not None else None
    rps = round(n_events / span_s, 4) if span_s else 0.0
    score = round(100 * (0.625 * (1 - min(cv, 1.0)) + 0.375 * modal_frac))
    severity = "critical" if (score >= 90 and n_events >= 30) else ("warning" if score >= 70 else "info")
    meta = {
        "score": score,
        "cv": cv,
        "modal_frac": modal_frac,
        "mean_interval_s": mean_interval,
        "stddev_s": stddev,
        "mode_gap_s": mode_gap,
        "n_gaps": n_gaps,
        "n_events": n_events,
        "span_s": span_s,
        "rps": rps,
        diversity_key: diversity_val,
        "filters": {key_field: entity},
    }
    return _item(
        entity or "(unknown)", mean_interval, stddev, "s interval", meta, severity, baseline_label="jitter (σ)"
    )


def repeated_patterns_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _process_repeated_patterns(row, key_field="ip", diversity_key="distinct_ua", mask_key=True, context=context)


def repeated_patterns_severity(items: list[dict]) -> str:
    if not items:
        return "clean"
    if any(i.get("severity") == "critical" for i in items):
        return "critical"
    return "warning" if any(i.get("severity") == "warning" for i in items) else "info"


_reg(
    id="repeated_patterns",
    category=InsightCategory.security,
    title="Scripted Traffic Patterns",
    description="IPs sending requests on a highly regular cadence — automated scrapers, pollers, or cron-scheduled scripts that evade volumetric rate limits",
    sql_template=SQL.REPEATED_PATTERNS,
    required_fields=["ip", "timestamp"],
    row_processor=repeated_patterns_processor,
    severity_logic=repeated_patterns_severity,
)


def repeated_patterns_fp_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _process_repeated_patterns(
        row, key_field=context.get("fp_col", "ja4"), diversity_key="distinct_ip", mask_key=False, context=context
    )


_reg(
    id="repeated_patterns_fp",
    category=InsightCategory.security,
    title="Scripted Traffic Patterns (by TLS Fingerprint)",
    description="TLS fingerprints sending requests on a highly regular cadence — catches IP-rotating scrapers, pollers, or cron-scheduled scripts that share a stable TLS stack across many source addresses",
    sql_template=SQL.REPEATED_PATTERNS_FP,
    required_fields=["ip", "timestamp"],
    row_processor=repeated_patterns_fp_processor,
    severity_logic=repeated_patterns_severity,
)


def low_and_slow_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    hits, distinct_paths, span_s, rps = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0), float(row[4] or 0)
    return _item(
        ip or "(unknown)",
        distinct_paths,
        None,
        "sensitive paths",
        {"hits": hits, "distinct_paths": distinct_paths, "span_s": span_s, "rps": rps, "filters": {"ip": ip}},
        "critical" if distinct_paths >= 10 else "warning",
    )


_reg(
    id="low_and_slow",
    category=InsightCategory.security,
    title="Low and Slow Scans",
    description="IPs probing many sensitive / vulnerability paths at a deliberately low request rate spread over a long span — designed to evade rate limits",
    sql_template=SQL.LOW_AND_SLOW,
    required_fields=["ip", "url"],
    row_processor=low_and_slow_processor,
)


def credential_enumeration_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_denied, w_attempts, w_paths, b_denied = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0), int(row[4] or 0)
    fail_rate = round(w_denied * 100.0 / w_attempts, 1) if w_attempts else 0.0
    return _item(
        ip or "(unknown)",
        w_denied,
        b_denied,
        "denied (401/403)",
        {
            "attempts": w_attempts,
            "denied": w_denied,
            "fail_rate_pct": fail_rate,
            "distinct_paths": w_paths,
            "filters": {"ip": ip},
        },
        "critical" if w_denied >= 100 else "warning",
        baseline_label="baseline denied",
    )


_reg(
    id="credential_enumeration",
    category=InsightCategory.security,
    title="Credential Enumeration / Brute Force",
    description="IPs generating a spike of 401/403 responses on authentication paths (login, auth, OAuth, password reset) — credential stuffing or brute force",
    sql_template=SQL.CREDENTIAL_ENUMERATION,
    required_fields=["ip", "url", "status"],
    row_processor=credential_enumeration_processor,
)


def network_asn_health_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_ploss, b_ploss = row[0], float(row[1] or 0), float(row[2] or 0)
    w_jitter, b_jitter, w_retrans, b_retrans, w_total = (
        float(row[3] or 0),
        float(row[4] or 0),
        float(row[5] or 0),
        float(row[6] or 0),
        int(row[7] or 0),
    )
    meta = {
        "window_packet_loss_pct": round(w_ploss * 100, 2),
        "baseline_packet_loss_pct": round(b_ploss * 100, 2),
        "window_jitter_ms": round(w_jitter / 1000.0, 1),
        "baseline_jitter_ms": round(b_jitter / 1000.0, 1),
        "window_retrans_avg": round(w_retrans, 2),
        "baseline_retrans_avg": round(b_retrans, 2),
        "requests": w_total,
        "asn": asn,
        "filters": {"asn": asn},
    }
    return _item(
        _fmt_asn_label(asn, context),
        round(w_ploss * 100, 2),
        round(b_ploss * 100, 2),
        "% packet loss",
        meta,
        "critical" if w_ploss >= 0.05 else "warning",
    )


_reg(
    id="network_asn_health",
    category=InsightCategory.network,
    title="Network Path (ASN) Health",
    description="ASNs experiencing elevated packet loss, jitter, or TCP retransmissions vs. their baseline",
    sql_template=SQL.NETWORK_ASN_HEALTH,
    required_fields=["asn", "ploss", "rtt_var", "retrans"],
    row_processor=network_asn_health_processor,
)


def content_discovery_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_404, w_total, distinct_404, b_404 = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0), int(row[4] or 0)
    ratio = round(w_404 * 100.0 / w_total, 1) if w_total else 0.0
    return _item(
        ip or "(unknown)",
        w_404,
        b_404,
        "404s",
        {
            "requests": w_total,
            "not_found": w_404,
            "not_found_rate_pct": ratio,
            "distinct_404_urls": distinct_404,
            "filters": {"ip": ip},
        },
        "critical" if w_404 >= 100 else "warning",
        baseline_label="baseline 404s",
    )


_reg(
    id="content_discovery",
    category=InsightCategory.security,
    title="Content-Discovery Scanning",
    description="IPs generating a burst of 404s across many distinct URLs — directory / endpoint enumeration probing for hidden or vulnerable paths",
    sql_template=SQL.CONTENT_DISCOVERY,
    required_fields=["ip", "url", "status"],
    row_processor=content_discovery_processor,
)


def referer_monoculture_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _monoculture_processor(row, "referer")


_reg(
    id="referer_monoculture",
    category=InsightCategory.traffic,
    title="Referer Monoculture",
    description="A single Referer driving an outsized, spiking share of total traffic — scraping, hotlinking, or a spoofed-referer flood",
    sql_template=SQL.REFERER_MONOCULTURE,
    required_fields=["referer", "timestamp"],
    row_processor=referer_monoculture_processor,
)


def method_drift_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    method, w_cnt, b_cnt, w_total, b_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        method or "(unknown)",
        w_rate,
        b_rate,
        "% of traffic",
        {"requests": w_cnt, "method": method, "filters": {"method": method}},
        "critical" if w_rate >= 25 else "warning",
    )


_reg(
    id="method_drift",
    category=InsightCategory.traffic,
    title="HTTP Method Drift",
    description="A write method (POST/PUT/DELETE/…) surging to an outsized share of traffic vs a read-dominated baseline — API abuse or form/credential floods",
    sql_template=SQL.METHOD_DRIFT,
    required_fields=["method", "timestamp"],
    row_processor=method_drift_processor,
)


def new_asn_traffic_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_cnt, _b_cnt = row
    return _zero_baseline_count_item(
        label=_fmt_asn_label(asn, context),
        count=w_cnt,
        meta={"requests": w_cnt, "asn": asn, "filters": {"asn": asn}},
        warn_threshold=100,
    )


_reg(
    id="new_asn_traffic",
    category=InsightCategory.network,
    title="New ASN Traffic",
    description="An ASN (ISP/datacenter) with zero baseline presence now sending meaningful traffic — a new botnet source, proxy pool, or datacenter range",
    sql_template=SQL.NEW_ASN_TRAFFIC,
    required_fields=["asn", "timestamp"],
    row_processor=new_asn_traffic_processor,
)


def asn_hosting_shift_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    asn, w_hosting, w_total, b_hosting, b_total = row
    w_ratio = round(float(w_hosting * 100.0 / w_total), 1) if w_total else 0.0
    b_ratio = round(float(b_hosting * 100.0 / b_total), 1) if b_total else 0.0
    return _item(
        _fmt_asn_label(asn, context),
        w_ratio,
        b_ratio,
        "% hosting",
        {"window_hosting": w_hosting, "window_total": w_total, "asn": asn, "filters": {"asn": asn}},
        "critical" if w_ratio >= 60 else "warning",
    )


_reg(
    id="asn_hosting_shift",
    category=InsightCategory.network,
    title="ASN Hosting Shift",
    description="An ASN's traffic composition shifting from consumer to datacenter/hosting — bot pools or scrapers spinning up on hosting infrastructure",
    sql_template=SQL.ASN_HOSTING_SHIFT,
    required_fields=["asn", "p_type", "timestamp"],
    row_processor=asn_hosting_shift_processor,
)


def metro_delivery_degradation_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    metro = row[0]
    w_mbps, b_mbps = round(float(row[1] or 0) * 8 / 1e6, 1), round(float(row[2] or 0) * 8 / 1e6, 1)
    return _item(
        _fmt_metro_label(metro, context),
        w_mbps,
        b_mbps,
        "Mbps (median)",
        {"window_requests": row[3], "window_mbps": w_mbps, "baseline_mbps": b_mbps, "filters": {"metro": metro}},
        "critical" if b_mbps > 0 and w_mbps <= b_mbps * 0.25 else "warning",
    )


_reg(
    id="metro_delivery_degradation",
    category=InsightCategory.network,
    title="Metro Delivery-Rate Degradation",
    description="US metro areas whose median TCP delivery rate (throughput) collapsed vs. baseline — regional last-mile or peering degradation",
    sql_template=SQL.METRO_DELIVERY_DEGRADATION,
    required_fields=["metro", "delivery_rate", "timestamp"],
    row_processor=metro_delivery_degradation_processor,
)


def connection_type_mix_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    c_type, c_speed, w_cnt, b_cnt, w_total, b_total = row
    w_rate, b_rate = _traffic_share_rates(w_cnt, w_total, b_cnt, b_total)
    return _item(
        f"{c_type or '?'} / {c_speed or '?'}",
        w_rate,
        b_rate,
        "% of typed traffic",
        {"requests": w_cnt, "c_type": c_type, "c_speed": c_speed, "filters": {"c_type": c_type, "c_speed": c_speed}},
        "warning",
    )


_reg(
    id="connection_type_mix",
    category=InsightCategory.network,
    title="Connection-Type Mix Shift",
    description="A client connection type/speed combo (e.g. cellular, datacenter) surging to an outsized share of traffic vs. baseline — a bot pool or routing shift",
    sql_template=SQL.CONNECTION_TYPE_MIX,
    required_fields=["c_type", "c_speed", "timestamp"],
    row_processor=connection_type_mix_processor,
)


def pop_latency_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _p95_latency_item(row[0] or "(unknown)", row[1], row[2], row[3], {"pop": row[0]})


_reg(
    id="pop_latency_regression",
    category=InsightCategory.edge,
    title="PoP Latency Regression",
    description="Individual Fastly PoPs (datacenters) whose P95 edge latency regressed sharply vs. baseline — finer-grained than region or city latency",
    sql_template=SQL.POP_LATENCY_REGRESSION,
    required_fields=["pop", "elapsed", "timestamp"],
    row_processor=pop_latency_regression_processor,
)


def http3_fallback_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    w_quic, w_total, b_quic, b_total = row
    w_share, b_share = (
        (round(w_quic * 100.0 / w_total, 1) if w_total else 0.0),
        (round(b_quic * 100.0 / b_total, 1) if b_total else 0.0),
    )
    return _item(
        "HTTP/3 (QUIC) adoption",
        w_share,
        b_share,
        "% QUIC",
        {
            "window_quic": int(w_quic or 0),
            "window_total": int(w_total or 0),
            "share_drop_pts": round(b_share - w_share, 1),
            "filters": {"transport": "tcp"},
        },
        "critical" if (b_share - w_share) >= 40 else "warning",
        baseline_label="baseline QUIC share",
    )


_reg(
    id="http3_fallback",
    category=InsightCategory.network,
    title="HTTP/3 → TCP Fallback Spike",
    description="A service-wide drop in QUIC (HTTP/3) share vs. baseline — clients failing to sustain QUIC and falling back to TCP (middlebox/UDP throttling)",
    sql_template=SQL.HTTP3_FALLBACK,
    required_fields=["transport", "timestamp"],
    row_processor=http3_fallback_processor,
)


def cache_hit_cliff_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    w_hits, w_cacheable, b_hits, b_cacheable = row
    w_rate, b_rate = (
        (round(w_hits * 100.0 / w_cacheable, 1) if w_cacheable else 0.0),
        (round(b_hits * 100.0 / b_cacheable, 1) if b_cacheable else 0.0),
    )
    return _item(
        "Service-wide cache HIT ratio",
        w_rate,
        b_rate,
        "% HIT",
        {
            "window_cacheable": int(w_cacheable or 0),
            "baseline_cacheable": int(b_cacheable or 0),
            "drop_pts": round(b_rate - w_rate, 1),
        },
        "critical" if (b_rate - w_rate) >= 30 else "warning",
    )


_reg(
    id="cache_hit_cliff",
    category=InsightCategory.edge,
    title="Cache HIT-Ratio Cliff",
    description="The whole service's edge cache HIT ratio (HIT/(HIT+MISS)) fell off a cliff vs. baseline — a purge storm, TTL change, or origin Cache-Control regression",
    sql_template=SQL.CACHE_HIT_CLIFF,
    required_fields=["cache", "timestamp"],
    row_processor=cache_hit_cliff_processor,
)


def payload_compression_regression_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _url_rate_regression_processor(row, "% uncompressed", is_critical=bool((row[1] or 0) >= 0.90))


_reg(
    id="payload_compression_regression",
    category=InsightCategory.edge,
    title="Payload Compression Regression",
    description="Compressible responses (JS/CSS/HTML/JSON/SVG/XML) that flipped from compressed (gzip/br) to served uncompressed vs. baseline — a broken Accept-Encoding path or origin regression inflating egress and TTFB",
    sql_template=SQL.PAYLOAD_COMPRESSION_REGRESSION,
    required_fields=["url", "resp_header_content_encoding", "resp_bytes", "status"],
    row_processor=payload_compression_regression_processor,
)


def session_harvesting_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_sessions, w_reqs, b_sessions = int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
    return _item(
        ip or "(unknown)",
        w_sessions,
        b_sessions,
        "distinct sessions",
        {
            "requests": w_reqs,
            "distinct_sessions": w_sessions,
            "baseline_distinct_sessions": b_sessions,
            "filters": {"ip": ip},
        },
        "critical" if w_sessions >= 100 else "warning",
        baseline_label="baseline sessions",
    )


_reg(
    id="session_harvesting",
    category=InsightCategory.security,
    title="Session-ID Harvesting",
    description="A single IP presenting a large, spiking number of distinct session cookies vs. baseline — session-token brute forcing, cookie replay, or credential stuffing that mints a fresh session per attempt",
    sql_template=SQL.SESSION_HARVESTING,
    required_fields=["ip", "cookie_session"],
    row_processor=session_harvesting_processor,
)


def timeout_split_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    w_conn, b_conn, w_read, b_read = float(row[0] or 0), float(row[1] or 0), float(row[2] or 0), float(row[3] or 0)
    conn_regressed = w_conn >= b_conn * 2 and (w_conn - b_conn) >= 50
    read_regressed = w_read >= b_read * 2 and (w_read - b_read) >= 100
    if conn_regressed and (not read_regressed or (w_conn - b_conn) >= (w_read - b_read)):
        phase, cur, base = "connect", round(w_conn, 1), round(b_conn, 1)
    else:
        phase, cur, base = "read", round(w_read, 1), round(b_read, 1)
    meta = {
        "phase": phase,
        "connect_p95_ms": round(w_conn, 1),
        "baseline_connect_p95_ms": round(b_conn, 1),
        "read_p95_ms": round(w_read, 1),
        "baseline_read_p95_ms": round(b_read, 1),
        "window_requests": int(row[4] or 0),
    }
    return _item(
        f"Origin {phase} P95", cur, base, "ms (P95)", meta, "critical" if base > 0 and cur >= base * 3 else "warning"
    )


_reg(
    id="timeout_split",
    category=InsightCategory.origin,
    title="Origin Connect vs Read Timeout Split",
    description="Splits origin slowness into connect (TCP+TLS handshake) vs read (processing to first byte) phases and flags whichever P95 regressed — slow-connect points at origin/LB saturation, slow-read at app/DB processing",
    sql_template=SQL.TIMEOUT_SPLIT,
    required_fields=["oconnect_ms", "ottfb"],
    row_processor=timeout_split_processor,
)


def crawler_disparity_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    from backend.utils.bot_sources import resolve_bot_display_name

    brand = resolve_bot_display_name(row[0], default="Other Bot")
    w_crawls, w_referrals = int(row[1] or 0), int(row[2] or 0)
    if len(row) >= 8:
        w_bytes, c_per_r, b_c_per_r = int(row[3] or 0), float(row[4] or 0), float(row[5] or 0)
        bytes_per_r, b_bytes_per_r = float(row[6] or 0), float(row[7] or 0)
        bw_pct = round(float(row[8] or 0), 2) if len(row) >= 9 else 0.0
    else:
        w_bytes, c_per_r, b_c_per_r = 0, float(row[3] or 0), float(row[4] or 0)
        bytes_per_r = b_bytes_per_r = bw_pct = 0.0
    is_crit = c_per_r >= 5000 or w_bytes >= 500_000_000 or (bw_pct >= 10.0 and w_referrals == 0)
    meta = {
        "crawls": w_crawls,
        "referrals": w_referrals,
        "w_bytes": w_bytes,
        "bandwidth_share_pct": bw_pct,
        "crawls_per_referral": c_per_r,
        "baseline_crawls_per_referral": b_c_per_r,
        "bytes_per_referral": bytes_per_r,
        "baseline_bytes_per_referral": b_bytes_per_r,
    }
    return _item(brand, c_per_r, b_c_per_r, "crawls/referral", meta, "critical" if is_crit else "warning")


_reg(
    id="crawler_disparity",
    category=InsightCategory.security,
    title="Bot & Crawler Referral & Bandwidth Disparity",
    description="Identifies bots, scrapers, and search/AI crawlers consuming high request volume and egress bandwidth while driving negligible human referral traffic back to the platform",
    sql_template=SQL.CRAWLER_DISPARITY,
    required_fields=["ua", "referer", "ip", "resp_bytes", "timestamp"],
    row_processor=crawler_disparity_processor,
)


def stale_browser_version_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    label, w_reqs, w_ips, surge = row[0] or "Unknown Chrome", int(row[1] or 0), int(row[2] or 0), float(row[3] or 0)
    return _item(
        label,
        w_reqs,
        w_ips,
        "requests",
        {"requests": w_reqs, "ips": w_ips, "surge_ratio": surge},
        "critical" if surge >= 5.0 else "warning",
        baseline_label="distinct IPs",
    )


_reg(
    id="stale_browser_version",
    category=InsightCategory.security,
    title="Stale Headless Browser",
    description="Detects anomalous clusters of request traffic using stale Chrome major versions, a standard fingerprint for un-updated Puppeteer/Playwright headless bots",
    sql_template=SQL.STALE_BROWSER_VERSION,
    required_fields=["ua", "ip", "timestamp"],
    row_processor=stale_browser_version_processor,
)


def orphaned_deep_crawl_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ip = _mask_ip_if_needed(row[0], context)
    w_reqs, w_distinct, b_reqs, reqs_per_url, surge = (
        int(row[1] or 0),
        int(row[2] or 0),
        int(row[3] or 0),
        float(row[4] or 0),
        float(row[5] or 0),
    )
    return _item(
        ip or "(unknown)",
        w_reqs,
        b_reqs,
        "requests",
        {
            "requests": w_reqs,
            "distinct_urls": w_distinct,
            "requests_per_url": reqs_per_url,
            "surge_ratio": surge,
            "filters": {"ip": ip},
        },
        "critical" if reqs_per_url >= 20.0 else "warning",
    )


_reg(
    id="orphaned_deep_crawl",
    category=InsightCategory.security,
    title="Orphaned Deep Crawl",
    description="Flags client IPs performing direct, deep scans of nested catalog/database resource routes with empty HTTP referer headers",
    sql_template=SQL.ORPHANED_DEEP_CRAWL,
    required_fields=["ip", "url", "referer", "timestamp"],
    row_processor=orphaned_deep_crawl_processor,
)


def residential_fingerprint_dispersion_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    ja4, w_ips, w_asns, w_requests, b_ips, asn_expansion = (
        row[0] or "(empty)",
        int(row[1] or 0),
        int(row[2] or 0),
        int(row[3] or 0),
        int(row[4] or 0),
        float(row[5] or 0),
    )
    return _item(
        ja4,
        w_asns,
        b_ips,
        "ASNs",
        {"ja4": ja4, "ips": w_ips, "asns": w_asns, "requests": w_requests, "asn_expansion_ratio": asn_expansion},
        "critical" if asn_expansion >= 3.0 else "warning",
        baseline_label="baseline IPs",
    )


_reg(
    id="residential_fingerprint_dispersion",
    category=InsightCategory.security,
    title="Residential Fingerprint Dispersion",
    description="Identifies identical TLS/TCP client handshakes distributed across multiple residential ASNs, signaling rotated botnet scans evading ASN blocks",
    sql_template=SQL.RESIDENTIAL_FINGERPRINT_DISPERSION,
    required_fields=["ja4", "ip", "asn", "p_type", "timestamp"],
    row_processor=residential_fingerprint_dispersion_processor,
)
