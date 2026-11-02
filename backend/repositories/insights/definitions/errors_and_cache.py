from __future__ import annotations

from backend.core.metric_expressions import coalesce
from backend.repositories.insights import InsightCategory, InsightDefinition
from backend.repositories.insights.definitions.sql_coalesced import (
    _city_filters,
    _error_rate_spike_item,
    _item,
    _reg,
    _url_rate_regression_processor,
)
from backend.utils.countries import format_city_label

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


def error_spikes_processor(row: tuple, definition: InsightDefinition, context: dict) -> dict:
    return _error_rate_spike_item(row[0] or "(empty)", row[1], row[2], row[3], row[4], "% 5xx", {"url": row[0]})


_reg(
    id="error_spikes",
    category=InsightCategory.origin,
    title="Error Spikes",
    description="URLs with abnormally elevated 5xx error rates in the window vs. baseline",
    sql_template=ERROR_SPIKES,
    required_fields=["url", "status", "timestamp"],
    row_processor=error_spikes_processor,
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
    sql_template=CITY_ERROR_SPIKES,
    required_fields=["city", "status", "timestamp"],
    row_processor=city_error_spikes_processor,
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
    sql_template=CACHE_COLLAPSE,
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
    sql_template=CACHEABILITY_REGRESSION,
    required_fields=["url", "cache", "timestamp"],
    row_processor=cacheability_regression_processor,
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
    sql_template=CACHE_TTL_MISMATCH,
    required_fields=["ttl", "hits", "age", "timestamp"],
    row_processor=cache_ttl_mismatch_processor,
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
    sql_template=ORIGIN_ERROR_RATE,
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
    sql_template=ORIGIN_RETRIES,
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
    sql_template=ORIGIN_IP_FAILURE,
    required_fields=["oip", "ost", "timestamp"],
    row_processor=origin_ip_failure_processor,
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
    sql_template=CACHE_HIT_CLIFF,
    required_fields=["cache", "timestamp"],
    row_processor=cache_hit_cliff_processor,
)
