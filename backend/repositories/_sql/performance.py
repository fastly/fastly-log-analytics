"""SQL templates for ``backend.repositories.performance``."""

from __future__ import annotations

from backend.core.metric_expressions import coalesce
from backend.repositories._base import percentile_ms_expr

TOP_URLS = f"""
    SELECT url,
           count(*) AS reqs,
           AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5, approx=True)} as p50,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95, approx=True)} as p95,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99, approx=True)} as p99
    FROM {{temp_table}}
    WHERE url IS NOT NULL AND elapsed IS NOT NULL
    GROUP BY url
    HAVING count(*) > 5
    ORDER BY {{sort_idx}} DESC LIMIT 20
"""

TOP_ASNS = f"""
    SELECT asn,
           count(*) AS reqs,
           AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5, approx=True)} as p50,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95, approx=True)} as p95,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99, approx=True)} as p99
    FROM {{temp_table}}
    WHERE asn IS NOT NULL AND elapsed IS NOT NULL
    GROUP BY asn
    HAVING count(*) > 10
    ORDER BY {{sort_idx}} DESC LIMIT 20
"""

TTL_DIST = """
    SELECT
        CASE
            WHEN ttl <= 0 THEN '0s'
            WHEN ttl <= 10 THEN '<10s'
            WHEN ttl <= 30 THEN '<30s'
            WHEN ttl <= 60 THEN '<1m'
            WHEN ttl <= 300 THEN '<5m'
            WHEN ttl <= 600 THEN '<10m'
            WHEN ttl <= 1800 THEN '<30m'
            WHEN ttl <= 3600 THEN '<1h'
            WHEN ttl <= 10800 THEN '<3h'
            WHEN ttl <= 21600 THEN '<6h'
            WHEN ttl <= 43200 THEN '<12h'
            WHEN ttl <= 86400 THEN '<1d'
            WHEN ttl <= 259200 THEN '<3d'
            WHEN ttl <= 604800 THEN '<1w'
            WHEN ttl <= 1209600 THEN '<2w'
            WHEN ttl <= 2592000 THEN '<30d'
            WHEN ttl <= 7776000 THEN '<90d'
            WHEN ttl <= 31536000 THEN '<1y'
            ELSE '>1y'
        END as bucket,
        count(*) as count,
        min(ttl) as min_ttl
    FROM {temp_table}
    WHERE ttl IS NOT NULL
    GROUP BY 1 ORDER BY min_ttl
"""

SCATTER_WATERFALL = """
    WITH components AS (
        SELECT
            cache::VARCHAR AS cache,
            CAST(ttfb AS DOUBLE) * 1000.0 AS origin_ms,
            (CAST(elapsed AS DOUBLE) / 1000.0 - CAST(ttfb AS DOUBLE) * 1000.0) AS edge_ms,
            {ottfb_expr} AS origin_wait,
            GREATEST(0.0, {ottlb_expr} - {ottfb_expr}) AS origin_download,
            GREATEST(0.0, (CAST(ttfb AS DOUBLE) * 1000.0) - {ottfb_expr}) AS edge_processing,
            GREATEST(0.0, (CAST(elapsed AS DOUBLE) / 1000.0) - GREATEST({ottlb_expr}, CAST(ttfb AS DOUBLE) * 1000.0)) AS client_download
        FROM {source_rel}
        {comp_where}
    )
    SELECT
        's'::VARCHAR AS row_type,
        origin_ms,
        edge_ms,
        cache,
        NULL::DOUBLE AS w_edge_processing,
        NULL::DOUBLE AS w_origin_wait,
        NULL::DOUBLE AS w_origin_download,
        NULL::DOUBLE AS w_client_download
    FROM components
    WHERE edge_ms >= 0
    USING SAMPLE 300
    UNION ALL
    SELECT
        'w'::VARCHAR,
        NULL::DOUBLE,
        NULL::DOUBLE,
        NULL::VARCHAR,
        AVG(edge_processing),
        AVG(origin_wait),
        AVG(origin_download),
        AVG(client_download)
    FROM components
"""


def build_scatter_waterfall_sql(
    *,
    source_rel: str,
    comp_where: str,
    has_ottfb: bool,
    has_ottlb: bool,
) -> str:
    ottfb_expr = coalesce("CAST(ottfb AS DOUBLE) / 1000.0", "0") if has_ottfb else "0"
    ottlb_expr = coalesce("CAST(ottlb AS DOUBLE) / 1000.0", "0") if has_ottlb else "0"
    return SCATTER_WATERFALL.format(
        source_rel=source_rel,
        comp_where=comp_where,
        ottfb_expr=ottfb_expr,
        ottlb_expr=ottlb_expr,
    )
