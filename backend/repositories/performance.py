"""Performance repository — latency analysis and origin/edge breakdown."""

from __future__ import annotations

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    _safe_table,
    percentile_ms_expr,
    safe_interval,
    safe_iso,
    time_bucket_select,
)
from backend.repositories._sql import performance as SQL
from backend.repositories.utils.filters import build_where_clause


def get_performance_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    sort_by: str = "p99",
) -> dict:
    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(
            latency_ts=[], top_urls=[], top_asns=[], ttl_dist=[], scatter=[], **runner.telemetry()
        )

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)

    cols = ["timestamp", "url", "asn", "ttfb", "elapsed", "cache", "ttl", "ottfb", "ottlb"]
    with runner.temp_table(cols, actual_cols, table_name, where_clause, params) as temp_table:
        if temp_table is None:
            from backend.repositories._base import empty_schema_response

            return empty_schema_response(
                latency_ts=[], top_urls=[], top_asns=[], ttl_dist=[], scatter=[], **runner.telemetry()
            )

        results = {**runner.telemetry()}

        sort_idx_map = {"avg": 3, "p50": 4, "p95": 5, "p99": 6}
        sort_idx = sort_idx_map.get(sort_by, 6)

        # 1. Latency Time Series (Stacked: Origin TTFB vs Edge Processing)
        if "ttfb" in actual_cols and "elapsed" in actual_cols:
            ts_q = f"""
                SELECT {time_bucket_select("1 minute")},
                       AVG(CAST(ttfb AS DOUBLE)) * 1000.0 AS origin_ms,
                       AVG(CAST(elapsed AS DOUBLE) / 1000.0 - CAST(ttfb AS DOUBLE) * 1000.0) AS edge_ms
                FROM {temp_table}
                WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL AND (CAST(elapsed AS DOUBLE) / 1000.0) >= (CAST(ttfb AS DOUBLE) * 1000.0)
                GROUP BY 1 ORDER BY 1
            """
            ts_res = runner.execute(ts_q).fetchall()
            results["latency_ts"] = [{"time": safe_iso(r[0]), "origin": r[1], "edge": r[2]} for r in ts_res]
        else:
            results["latency_ts"] = []

        # 2. Top URLs by Latency
        if "url" in actual_cols and "elapsed" in actual_cols:
            url_q = f"""
                SELECT url,
                       count(*) as reqs,
                       AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
                       {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5)} as p50,
                       {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95)} as p95,
                       {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99)} as p99
                FROM {temp_table}
                WHERE url IS NOT NULL AND elapsed IS NOT NULL
                GROUP BY 1 HAVING count(*) > 5 ORDER BY {sort_idx} DESC LIMIT 20
            """
            url_res = runner.execute(url_q).fetchall()
            results["top_urls"] = [
                {"url": r[0], "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]} for r in url_res
            ]
        else:
            results["top_urls"] = []

        # 3. Top ASNs by Latency
        if "asn" in actual_cols and "elapsed" in actual_cols:
            asn_q = f"""
                SELECT asn,
                       count(*) as reqs,
                       AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
                       {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5)} as p50,
                       {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95)} as p95,
                       {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99)} as p99
                FROM {temp_table}
                WHERE asn IS NOT NULL AND elapsed IS NOT NULL
                GROUP BY 1 HAVING count(*) > 10 ORDER BY {sort_idx} DESC LIMIT 20
            """
            asn_res = runner.execute(asn_q).fetchall()

            # Resolve ASN names
            asn_list = [int(r[0]) for r in asn_res if str(r[0]).isdigit()]
            from backend.core import duckdb as _db

            asn_names = _db.get_asn_names(src["name"], asn_list)

            results["top_asns"] = []
            for r in asn_res:
                asn_val = int(r[0]) if str(r[0]).isdigit() else r[0]
                label = (
                    _db.format_asn_label(asn_val, asn_names.get(asn_val, ""))
                    if isinstance(asn_val, int)
                    else str(asn_val)
                )
                results["top_asns"].append(
                    {
                        "asn": asn_val,
                        "label": label,
                        "requests": r[1],
                        "avg": r[2],
                        "p50": r[3],
                        "p95": r[4],
                        "p99": r[5],
                    }
                )
        else:
            results["top_asns"] = []

        # 5. Cache TTL Distribution (Histogram)
        if "ttl" in actual_cols:
            ttl_q = f"""
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
            ttl_res = runner.execute(ttl_q).fetchall()
            results["ttl_dist"] = [{"bucket": r[0], "count": r[1]} for r in ttl_res]
        else:
            results["ttl_dist"] = []

        # 6. Backend vs Fastly Processing Scatter (Sampled)
        if "ttfb" in actual_cols and "elapsed" in actual_cols:
            scatter_q = f"""
                SELECT
                    CAST(ttfb AS DOUBLE) * 1000.0 as origin_ms,
                    (CAST(elapsed AS DOUBLE) / 1000.0 - CAST(ttfb AS DOUBLE) * 1000.0) as edge_ms,
                    cache
                FROM {temp_table}
                WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL AND (CAST(elapsed AS DOUBLE) / 1000.0) >= (CAST(ttfb AS DOUBLE) * 1000.0)
                USING SAMPLE 1000
            """
            scatter_res = runner.execute(scatter_q).fetchall()
            results["scatter"] = [{"origin": r[0], "edge": r[1], "cache": r[2]} for r in scatter_res]
        else:
            results["scatter"] = []

        # 7. Waterfall Components
        if "ttfb" in actual_cols and "elapsed" in actual_cols:
            ottfb_expr = "COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0)" if "ottfb" in actual_cols else "0"
            ottlb_expr = "COALESCE(CAST(ottlb AS DOUBLE) / 1000.0, 0)" if "ottlb" in actual_cols else "0"

            waterfall_q = f"""
                WITH components AS (
                    SELECT
                        {ottfb_expr} as origin_wait,
                        GREATEST(0.0, {ottlb_expr} - {ottfb_expr}) as origin_download,
                        GREATEST(0.0, (CAST(ttfb AS DOUBLE) * 1000.0) - {ottfb_expr}) as edge_processing,
                        GREATEST(0.0, (CAST(elapsed AS DOUBLE) / 1000.0) - GREATEST({ottlb_expr}, CAST(ttfb AS DOUBLE) * 1000.0)) as client_download
                    FROM {temp_table}
                    WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL
                )
                SELECT
                    AVG(edge_processing),
                    {percentile_ms_expr("edge_processing", 0.5)},
                    {percentile_ms_expr("edge_processing", 0.95)},
                    {percentile_ms_expr("edge_processing", 0.99)},

                    AVG(origin_wait),
                    {percentile_ms_expr("origin_wait", 0.5)},
                    {percentile_ms_expr("origin_wait", 0.95)},
                    {percentile_ms_expr("origin_wait", 0.99)},

                    AVG(origin_download),
                    {percentile_ms_expr("origin_download", 0.5)},
                    {percentile_ms_expr("origin_download", 0.95)},
                    {percentile_ms_expr("origin_download", 0.99)},

                    AVG(client_download),
                    {percentile_ms_expr("client_download", 0.5)},
                    {percentile_ms_expr("client_download", 0.95)},
                    {percentile_ms_expr("client_download", 0.99)}
                FROM components
            """
            waterfall_res = runner.execute(waterfall_q).fetchone()
            if waterfall_res:
                results["waterfall"] = {
                    "avg": {
                        "edge_processing": float(waterfall_res[0] or 0.0),
                        "origin_wait": float(waterfall_res[4] or 0.0),
                        "origin_download": float(waterfall_res[8] or 0.0),
                        "client_download": float(waterfall_res[12] or 0.0),
                    },
                    "p50": {
                        "edge_processing": float(waterfall_res[1] or 0.0),
                        "origin_wait": float(waterfall_res[5] or 0.0),
                        "origin_download": float(waterfall_res[9] or 0.0),
                        "client_download": float(waterfall_res[13] or 0.0),
                    },
                    "p95": {
                        "edge_processing": float(waterfall_res[2] or 0.0),
                        "origin_wait": float(waterfall_res[6] or 0.0),
                        "origin_download": float(waterfall_res[10] or 0.0),
                        "client_download": float(waterfall_res[14] or 0.0),
                    },
                    "p99": {
                        "edge_processing": float(waterfall_res[3] or 0.0),
                        "origin_wait": float(waterfall_res[7] or 0.0),
                        "origin_download": float(waterfall_res[11] or 0.0),
                        "client_download": float(waterfall_res[15] or 0.0),
                    },
                }
            else:
                results["waterfall"] = {}
        else:
            results["waterfall"] = {}

        return results


def get_origin_ts(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    chart_interval: str = "1 minute",
    origin_metric: str = "ttfb",
    origin_percentile: str = "p95",
) -> dict:
    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)

    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(timeseries=[], **runner.telemetry())

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)

    metric_col = "ottfb" if origin_metric == "ttfb" else "ottlb"
    is_microseconds = True

    if metric_col not in actual_cols:
        # Fallback to Group C ttfb if ottfb requested but missing
        if origin_metric == "ttfb" and "ttfb" in actual_cols:
            metric_col = "ttfb"
            is_microseconds = False
        else:
            from backend.repositories._base import empty_schema_response
        return empty_schema_response(timeseries=[], **runner.telemetry())

    pct_val = {"p50": 0.5, "p95": 0.95, "p99": 0.99}.get(origin_percentile, 0.95)
    interval_str = safe_interval(chart_interval)

    if is_microseconds:
        val_expr = f"ROUND(COALESCE({percentile_ms_expr(f'"{metric_col}"', pct_val)}, 0), 2)"
    else:
        # Seconds to Milliseconds
        val_expr = f'ROUND(COALESCE(PERCENTILE_CONT({pct_val}) WITHIN GROUP (ORDER BY "{metric_col}") * 1000.0, 0), 2)'

    q = SQL.ORIGIN_TIMESERIES.format(
        time_bucket_select=time_bucket_select(interval_str),
        value_expr=val_expr,
        table=table_name,
        where_clause=where_clause,
        metric_col=metric_col,
    )
    res_cursor = runner.execute_with_retry(q, params)
    if res_cursor is None:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(timeseries=[], **runner.telemetry())

    res = res_cursor.fetchall()
    return {"timeseries": [{"time": safe_iso(r[0]), "value": r[1]} for r in res], **runner.telemetry()}
