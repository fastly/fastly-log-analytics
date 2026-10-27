"""Origin metrics repository."""

from __future__ import annotations

import logging
import sys as _sys
import time
import uuid
from typing import Any

import duckdb

from backend.core import metric_expressions
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _drop_temp_table,
    _safe_table,
    empty_schema_response,
    origin_latency_us_expr,
    safe_iso,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    ORIGIN_SUMMARY_KEYS,
    empty_origin_aggregates,
    empty_origin_summary,
    enrich_row_with_pop_distance,
    haversine_km,
    shape_keyed_rows,
    shape_pop_latency_rows,
    shape_status_code_rows,
)
from backend.utils.bounded_cache import CacheRegistry

logger = logging.getLogger(__name__)

# ── SQL templates (folded from backend/repositories/_sql/origin.py) ──────────

SUMMARY_ROLLUP = """
        SELECT
          COUNT(*)                                                                            AS requests,
          {total_misses}                                                                      AS total_misses,
          {total_passes}                                                                      AS total_passes,
          MEDIAN({lat_val}) / 1000.0                                                          AS ottfb_p50_ms,
          APPROX_QUANTILE({lat_val}, 0.75) / 1000.0                                           AS ottfb_p75_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                                           AS ottfb_p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                                           AS ottfb_p99_ms,
          {ottlb_p50}                                                                          AS ottlb_p50_ms,
          {ottlb_p95}                                                                          AS ottlb_p95_ms,
          {cdn_ovh}                                                                            AS cdn_overhead_p50_ms,
          {ost_5xx}                                                                            AS origin_error_rate,
          {obytes_p50}                                                                         AS obytes_p50
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL)
        """

TIMESERIES_BUCKETED = """
        SELECT
          time_bucket({interval}, "timestamp")                              AS ts,
          COUNT(*)                                                          AS miss_count,
          {agg_expr} {unit_conv}                                            AS value
          {edge_col}
        FROM {table}
        WHERE {where} AND ({lat_expr} IS NOT NULL)
        GROUP BY ts {edge_group}
        ORDER BY ts
        """

SLOW_URLS = """
        SELECT
          "url",
          COUNT(*)                                                         AS requests,
          MEDIAN({lat_val}) / 1000.0                                       AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                        AS p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                        AS p99_ms
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL) AND "url" IS NOT NULL
        GROUP BY "url"
        HAVING COUNT(*) >= ?
        ORDER BY p95_ms DESC
        LIMIT ?
        """

STATUS_CODES = """
        SELECT
          CASE
            WHEN "ost" BETWEEN 100 AND 599 THEN "ost"
            ELSE -1
          END                                              AS status,
          COUNT(*)                                         AS count,
          COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()        AS pct
        FROM {table}
        WHERE {where} AND "ost" IS NOT NULL
        GROUP BY 1
        ORDER BY count DESC
        """

PATH_BREAKDOWN = """
        SELECT
          "edge",
          COUNT(*)                                                          AS requests,
          MEDIAN({lat_val}) / 1000.0                                        AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                         AS p95_ms
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL)
        GROUP BY "edge"
        """

POP_LATENCY = """
        SELECT
          "pop",
          COUNT(*)                                                          AS requests,
          MEDIAN({lat_val}) / 1000.0                                        AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                         AS p95_ms
        FROM {table}
        WHERE {where} AND ({lat_val} IS NOT NULL) AND "pop" IS NOT NULL AND "pop" != ''
        GROUP BY "pop"
        ORDER BY p95_ms DESC
        LIMIT ?
        """

IP_HEALTH = """
        SELECT
          "oip",
          COUNT(*)                                                            AS requests,
          MEDIAN({lat_val}) / 1000.0                                          AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                           AS p95_ms,
          ROUND(COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0
            / NULLIF(COUNT(*), 0), 1)                                         AS error_pct
        FROM {table}
        WHERE {where} AND "oip" IS NOT NULL AND "oip" != '' AND "ost" IS NOT NULL
        GROUP BY "oip"
        HAVING COUNT(*) >= 10
        ORDER BY error_pct DESC
        LIMIT ?
        """

_EDGE_MINUS_SHIELD_OTTFB = (
    f"({metric_expressions.coalesce('e.ottfb', 'e.ttfb * 1000000')} - "
    f"{metric_expressions.coalesce('s.ottfb', 's.ttfb * 1000000')})"
)

SHIELDING_ANALYSIS = f"""
        WITH edge_logs AS (
            SELECT "rid", "pop", "ottfb", "ttfb"
            FROM {{table}}
            WHERE {{where}} AND "edge" = true
        ),
        shield_logs AS (
            SELECT "prid", "pop", "ottfb", "ttfb"
            FROM {{table}}
            WHERE {{time_where}} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != ''
        ),
        pairs AS (
          SELECT
            e.pop                                                                    AS edge_pop,
            s.pop                                                                    AS shield_pop,
            COUNT(*)                                                                 AS requests,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {_EDGE_MINUS_SHIELD_OTTFB}) / 1000.0 AS p50_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {_EDGE_MINUS_SHIELD_OTTFB}) / 1000.0 AS p95_ms,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY {_EDGE_MINUS_SHIELD_OTTFB}) / 1000.0 AS p99_ms
          FROM edge_logs e
          INNER JOIN shield_logs s ON s.prid = e.rid
          GROUP BY 1, 2
        ),
        ranked AS (
          SELECT
            *,
            COUNT(*) OVER ()                                       AS total_routes,
            ROW_NUMBER() OVER (ORDER BY requests DESC)            AS rn_requests,
            ROW_NUMBER() OVER (ORDER BY p50_ms DESC NULLS LAST)  AS rn_overhead
          FROM pairs
        )
        SELECT edge_pop, shield_pop, requests, p50_ms, p95_ms, p99_ms, total_routes
        FROM ranked
        WHERE rn_requests <= ? OR rn_overhead <= ?
        ORDER BY requests DESC
    """

DRILLDOWN = f"""
        SELECT
          {metric_expressions.coalesce('CAST("{dim_col}" AS VARCHAR)', "'unknown'")}                   AS dim_key,
          COUNT(*)                                                            AS requests,
          ROUND(MEDIAN({{lat_val}}) / 1000.0, 2)                                AS p50_ms,
          ROUND(APPROX_QUANTILE({{lat_val}}, 0.95) / 1000.0, 2)                 AS p95_ms,
          ROUND(APPROX_QUANTILE({{lat_val}}, 0.99) / 1000.0, 2)                 AS p99_ms,
          ROUND(COUNT(*) FILTER (WHERE {{error_expr}}) * 100.0
                / NULLIF(COUNT(*), 0), 2)                                     AS error_rate,
          {{bytes_expr}}                                                        AS bytes
        FROM {{table}}
        WHERE {{where}} AND ({{lat_val}} IS NOT NULL) AND "{{dim_col}}" IS NOT NULL AND "{{dim_col}}" != ''
        GROUP BY 1
        ORDER BY requests DESC
        LIMIT ?
        """

AGGREGATES_CREATE_TEMP = (
    "CREATE TABLE {temp_table} AS SELECT {select_cols}, {lat_us_expr} AS lat_us FROM {table} WHERE {where_clause}"
)

SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.origin", _sys.modules[__name__])

# ── Response memo cache ───────────────────────────────────────────────────────
_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 256
_memo = ResponseMemo(ttl=_RESPONSE_CACHE_TTL, maxsize=_RESPONSE_CACHE_MAXSIZE, prewarmer=False)
_response_cache = _memo.cache


def _origin_response_cache_key(
    endpoint: str, src: dict, start_time: str | None, end_time: str | None, filters: FiltersDict, **extra
) -> str:
    payload = {
        "ep": endpoint,
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        **{k: extra[k] for k in sorted(extra)},
    }
    return digest_cache_key(payload, src)


_response_cache_key = _origin_response_cache_key


def _check_response_cache(
    endpoint: str,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    runner: QueryRunner,
    **extras,
) -> tuple[str, dict | None]:
    cache_key = _origin_response_cache_key(endpoint, src, start_time, end_time, filters, **extras)
    return cache_key, _memo.check(cache_key, runner)


def _store_response_cache(cache_key: str, payload: dict, runner: QueryRunner) -> dict:
    return _memo.store(cache_key, payload, runner)


# ── POP location & response shaping helpers ──────────────────────────────────

_haversine_km = haversine_km
_enrich_with_distance = enrich_row_with_pop_distance
_SUMMARY_KEYS = ORIGIN_SUMMARY_KEYS


def _shape_summary(
    runner: QueryRunner, table: str, where: str, params: list, lat_val: str, actual_cols: set[str] | list[str]
) -> dict:
    cols_set = set(actual_cols)
    has_ost, has_ottlb, has_cache = "ost" in cols_set, "ottlb" in cols_set, "cache" in cols_set
    ost_5xx = (
        'COUNT(*) FILTER (WHERE "ost" >= 500 AND "ost" < 600) * 1.0 / NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)'
        if has_ost
        else "NULL"
    )
    sql = SQL.SUMMARY_ROLLUP.format(
        lat_val=lat_val,
        total_misses="COUNT(*) FILTER (WHERE starts_with(\"cache\", 'MISS'))" if has_cache else "NULL",
        total_passes="COUNT(*) FILTER (WHERE starts_with(\"cache\", 'PASS'))" if has_cache else "NULL",
        ottlb_p50='MEDIAN("ottlb") / 1000.0' if has_ottlb else "NULL",
        ottlb_p95='APPROX_QUANTILE("ottlb", 0.95) / 1000.0' if has_ottlb else "NULL",
        cdn_ovh='MEDIAN("elapsed" - "ottlb") / 1000.0' if "elapsed" in cols_set and has_ottlb else "NULL",
        ost_5xx=ost_5xx,
        obytes_p50='MEDIAN("obytes")' if "obytes" in cols_set else "NULL",
        table=table,
        where=where,
    )
    cur = runner.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rollup_row = next((dict(zip(cols, r, strict=False)) for r in cur.fetchall()), None)
    if rollup_row is None or rollup_row["ottfb_p50_ms"] is None:
        return empty_origin_summary()
    return {"has_data": True, **{k: rollup_row[k] for k in _SUMMARY_KEYS}}


def get_summary(
    con: duckdb.DuckDBPyConnection, src: dict, start_time: str | None, end_time: str | None, filters: FiltersDict
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("summary", src, start_time, end_time, filters, runner)
    if hit:
        return hit
    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return empty_schema_response(has_data=False, **{k: None for k in _SUMMARY_KEYS[:6]}, **runner.telemetry())
    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    timer = SectionTimer()
    started = time.perf_counter()
    payload: dict[str, Any] = _shape_summary(
        runner, table_name, where, params, origin_latency_us_expr(actual_cols), actual_cols
    )
    timer.mark("summary", started, source="live", activity="compute", current_execution=True, outcome="served")
    payload["section_timings"] = timer.entries
    return _store_response_cache(cache_key, payload, runner)


def _run_timeseries_query(
    runner: QueryRunner,
    table: str,
    where: str,
    params: list,
    lat_expr: str,
    unit_conv: str,
    *,
    actual_cols: set[str] | list[str],
    bucket_minutes: float,
    split_by_leg: bool,
    percentile: str,
) -> dict:
    pct_val = {"p50": 0.5, "p95": 0.95, "p99": 0.99}.get(percentile, 0.95)
    agg_expr = f"MEDIAN({lat_expr})" if percentile == "p50" else f"APPROX_QUANTILE({lat_expr}, {pct_val})"
    interval = (
        f"INTERVAL '{max(1, int(bucket_minutes * 60))}' seconds"
        if bucket_minutes < 1
        else f"INTERVAL '{int(bucket_minutes)}' minutes"
    )
    has_edge_col = split_by_leg and "edge" in actual_cols
    edge_col = ', "edge"' if has_edge_col else ""
    sql = SQL.TIMESERIES_BUCKETED.format(
        interval=interval,
        agg_expr=agg_expr,
        unit_conv=unit_conv,
        edge_col=edge_col,
        table=table,
        where=where,
        lat_expr=lat_expr,
        edge_group=edge_col,
    )
    rows = runner.execute(sql, params).fetchall()
    series = [
        {"time": safe_iso(r[0]), "miss_count": r[1], "value": r[2], **({"edge": r[3]} if has_edge_col else {})}
        for r in rows
    ]
    return {"has_data": len(series) > 0, "series": series}


_shape_keyed_rows = shape_keyed_rows


def _shape_slow_url_rows(rows: list[tuple]) -> dict:
    return _shape_keyed_rows(rows, ("url", "requests", "p50_ms", "p95_ms", "p99_ms"))


def _shape_status_code_rows(rows: list[tuple]) -> dict:
    return shape_status_code_rows(rows)


def _shape_path_breakdown_rows(rows: list[tuple]) -> dict:
    return _shape_keyed_rows(
        rows, ("edge", "requests", "p50_ms", "p95_ms"), shielding_detected=any(r[0] is False for r in rows)
    )


def _shape_pop_latency_rows(rows: list[tuple]) -> dict:
    return shape_pop_latency_rows(rows)


def _shape_ip_health_rows(rows: list[tuple]) -> dict:
    return _shape_keyed_rows(rows, ("oip", "requests", "p50_ms", "p95_ms", "error_pct"))


def _resolve_origin_ts_expr(
    metric: str, actual_cols: set[str] | list[str], *, use_temp_lat_us: bool = False
) -> tuple[str, str] | None:
    metric_col, unit_conv = ("ottfb" if metric == "ttfb" else "ottlb"), "/ 1000.0"
    if metric_col not in actual_cols:
        if metric == "ttfb" and "ttfb" in actual_cols:
            metric_col, unit_conv = "ttfb", "* 1000.0"
        else:
            return None
    if metric == "ttfb" and "ottfb" in actual_cols and "ttfb" in actual_cols:
        return (
            '"lat_us"' if use_temp_lat_us else (metric_expressions.origin_latency_us(actual_cols) or "NULL")
        ), "/ 1000.0"
    return f'"{metric_col}"', unit_conv


_ORIGIN_CARD_SPECS: dict[str, tuple[str, Any, tuple[str, ...], tuple[str, ...], dict[str, Any], bool]] = {
    "slow_urls": ("SLOW_URLS", _shape_slow_url_rows, (), ("url",), {}, True),
    "status_codes": ("STATUS_CODES", _shape_status_code_rows, ("ost",), ("ost",), {}, False),
    "path_breakdown": (
        "PATH_BREAKDOWN",
        _shape_path_breakdown_rows,
        ("edge",),
        ("edge",),
        {"shielding_detected": False},
        True,
    ),
    "pop_latency": ("POP_LATENCY", _shape_pop_latency_rows, ("pop",), ("pop",), {"requires_group_c": True}, True),
    "ip_health": ("IP_HEALTH", _shape_ip_health_rows, ("oip", "ost"), ("oip", "ost"), {}, True),
}


def _run_origin_card(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    endpoint: str,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    extra_params: list[Any] | None = None,
    **cache_extra: Any,
) -> dict:
    sql_attr, shaper, required_cols, _, empty_extra, use_lat_val = _ORIGIN_CARD_SPECS[endpoint]
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(endpoint, src, start_time, end_time, filters, runner, **cache_extra)
    if hit:
        return hit
    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or any(c not in actual_cols for c in required_cols):
        return empty_schema_response(has_data=False, **empty_extra, rows=[], **runner.telemetry())
    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    fmt_kwargs: dict[str, Any] = {"table": table_name, "where": where}
    if use_lat_val:
        fmt_kwargs["lat_val"] = origin_latency_us_expr(actual_cols)
    timer = SectionTimer()
    started = time.perf_counter()
    rows = runner.execute(getattr(SQL, sql_attr).format(**fmt_kwargs), params + (extra_params or [])).fetchall()
    timer.mark(endpoint, started, source="live", activity="compute", current_execution=True, outcome="served")
    return _store_response_cache(cache_key, {**shaper(rows), "section_timings": timer.entries}, runner)


def get_timeseries(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_minutes: float = 5,
    split_by_leg: bool = False,
    metric: str = "ttfb",
    percentile: str = "p95",
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(
        "timeseries",
        src,
        start_time,
        end_time,
        filters,
        runner,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        metric=metric,
        percentile=percentile,
    )
    if hit:
        return hit
    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return empty_schema_response(has_data=False, series=[], **runner.telemetry())
    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    resolved = _resolve_origin_ts_expr(metric, actual_cols, use_temp_lat_us=False)
    if resolved is None:
        return _store_response_cache(cache_key, {"has_data": False, "series": []}, runner)
    lat_expr, unit_conv = resolved
    timer = SectionTimer()
    started = time.perf_counter()
    payload = _run_timeseries_query(
        runner,
        table_name,
        where,
        params,
        lat_expr,
        unit_conv,
        actual_cols=actual_cols,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        percentile=percentile,
    )
    timer.mark("timeseries", started, source="live", activity="compute", current_execution=True, outcome="served")
    payload["section_timings"] = timer.entries
    return _store_response_cache(cache_key, payload, runner)


def get_slow_urls(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 20,
    min_requests: int = 10,
) -> dict:
    return _run_origin_card(
        con,
        src,
        "slow_urls",
        start_time,
        end_time,
        filters,
        [min_requests, limit],
        limit=limit,
        min_requests=min_requests,
    )


def get_status_codes(
    con: duckdb.DuckDBPyConnection, src: dict, start_time: str | None, end_time: str | None, filters: FiltersDict
) -> dict:
    return _run_origin_card(con, src, "status_codes", start_time, end_time, filters)


def get_path_breakdown(
    con: duckdb.DuckDBPyConnection, src: dict, start_time: str | None, end_time: str | None, filters: FiltersDict
) -> dict:
    return _run_origin_card(con, src, "path_breakdown", start_time, end_time, filters)


def get_pop_latency(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    return _run_origin_card(con, src, "pop_latency", start_time, end_time, filters, [limit], limit=limit)


def get_ip_health(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    return _run_origin_card(con, src, "ip_health", start_time, end_time, filters, [limit], limit=limit)


_DIM_CANDIDATES: dict[str, list[str]] = {
    "host": ["host", "server", "req_header_host"],
    "shield_pop": ["shield_pop", "fastly_info_shield_pop", "shield", "pop"],
    "datacenter": ["server_region", "datacenter", "datacenter_code", "pop"],
    "protocol": ["proto", "protocol"],
    "pop": ["pop"],
    "oip": ["oip"],
}


def get_drilldown(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    dimension: str = "host",
    limit: int = 30,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(
        "drilldown", src, start_time, end_time, filters, runner, dimension=dimension, limit=limit
    )
    if hit:
        return hit
    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    dim_col = (
        next((c for c in _DIM_CANDIDATES.get(dimension, [dimension]) if c in actual_cols), None)
        if actual_cols
        else None
    )
    if not actual_cols or not dim_col:
        return empty_schema_response(has_data=False, dimension=dimension, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    error_expr = '"ost" >= 500' if "ost" in actual_cols else ('"status" >= 500' if "status" in actual_cols else "false")
    bytes_expr = (
        'ROUND(MEDIAN("obytes"), 0)'
        if "obytes" in actual_cols
        else ('ROUND(MEDIAN("bytes"), 0)' if "bytes" in actual_cols else "NULL")
    )

    timer = SectionTimer()
    started = time.perf_counter()
    sql = SQL.DRILLDOWN.format(
        dim_col=dim_col,
        lat_val=origin_latency_us_expr(actual_cols),
        error_expr=error_expr,
        bytes_expr=bytes_expr,
        table=table_name,
        where=where,
    )
    rows = runner.execute(sql, params + [limit]).fetchall()
    timer.mark("drilldown", started, source="live", activity="compute", current_execution=True, outcome="served")

    rows_out = [
        {
            "key": str(r[0]),
            "requests": int(r[1]),
            "p50_ms": float(r[2]) if r[2] is not None else None,
            "p95_ms": float(r[3]) if r[3] is not None else None,
            "p99_ms": float(r[4]) if r[4] is not None else None,
            "error_rate": float(r[5]) if r[5] is not None else 0.0,
            "bytes": int(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]
    payload = {"has_data": bool(rows), "dimension": dimension, "rows": rows_out, "section_timings": timer.entries}
    return _store_response_cache(cache_key, payload, runner)


def get_shielding_analysis(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 50,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(
        "shielding_analysis", src, start_time, end_time, filters, runner, limit=limit
    )
    if hit:
        return hit
    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return _memo.result(empty_schema_response(has_data=False, rows=[], **runner.telemetry()), state="miss")
    missing = {"rid", "prid", "edge", "pop", "ottfb", "ttfb"} - set(actual_cols)
    if missing:
        return _memo.result(
            empty_schema_response(has_data=False, requires_fields=list(missing), rows=[], **runner.telemetry()),
            state="miss",
        )

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    time_params, time_where = build_where_clause(start_time, end_time, {}, actual_cols)
    query = SQL.SHIELDING_ANALYSIS.format(table=table_name, where=where, time_where=time_where)

    timer = SectionTimer()
    started = time.perf_counter()
    cur = runner.execute(query, params + time_params + [limit, limit])
    col_names = [d[0] for d in cur.description]
    raw_rows = [dict(zip(col_names, r, strict=False)) for r in cur.fetchall()]
    timer.mark(
        "shielding_analysis", started, source="live", activity="compute", current_execution=True, outcome="served"
    )

    if not raw_rows:
        started = time.perf_counter()
        shield_exists = runner.execute(
            f'SELECT 1 FROM {table_name} WHERE {time_where} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != \'\' LIMIT 1',
            time_params,
        ).fetchone()
        timer.mark(
            "shielding_presence", started, source="live", activity="compute", current_execution=True, outcome="served"
        )
        extra = {"edge_only": True} if shield_exists is None else {}
        return _store_response_cache(
            cache_key, {"has_data": False, **extra, "rows": [], "section_timings": timer.entries}, runner
        )

    total_routes = raw_rows[0].get("total_routes")
    if total_routes is None:
        total_routes = len(raw_rows)
    _keys = ("edge_pop", "shield_pop", "requests", "p50_ms", "p95_ms", "p99_ms")
    enriched_rows = [_enrich_with_distance({k: r[k] for k in _keys}) for r in raw_rows]
    payload = {
        "has_data": len(enriched_rows) > 0,
        "rows": enriched_rows,
        "total_routes": total_routes,
        "truncated": total_routes > len(enriched_rows),
        "section_timings": timer.entries,
    }
    return _store_response_cache(cache_key, payload, runner)


# ── Composite: get_aggregates ────────────────────────────────────────────────


def _origin_summary_from_temp(runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str]) -> dict:
    return _shape_summary(runner, temp_table, "1=1", [], "lat_us", actual_cols)


def _origin_timeseries_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    bucket_minutes: float,
    split_by_leg: bool,
    metric: str,
    percentile: str,
) -> dict:
    actual_cols_set = set(actual_cols)
    if (resolved := _resolve_origin_ts_expr(metric, actual_cols_set, use_temp_lat_us=True)) is None:
        return {"has_data": False, "series": []}
    return _run_timeseries_query(
        runner,
        temp_table,
        "1=1",
        [],
        resolved[0],
        resolved[1],
        actual_cols=actual_cols_set,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        percentile=percentile,
    )


def _run_temp_origin_card(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    endpoint: str,
    params: list[Any] | None = None,
) -> dict:
    sql_attr, shaper, _, required_cols, empty_extra, use_lat_val = _ORIGIN_CARD_SPECS[endpoint]
    cols = set(actual_cols)
    if any(c not in cols for c in required_cols):
        return {"has_data": False, **empty_extra, "rows": []}
    fmt_kwargs: dict[str, Any] = {"table": temp_table, "where": "1=1"}
    if use_lat_val:
        fmt_kwargs["lat_val"] = "lat_us"
    return shaper(runner.execute(getattr(SQL, sql_attr).format(**fmt_kwargs), params).fetchall())


def _origin_slow_urls_from_temp(
    runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str], min_requests: int, limit: int
) -> dict:
    return _run_temp_origin_card(runner, temp_table, actual_cols, "slow_urls", [min_requests, limit])


def _origin_status_codes_from_temp(runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str]) -> dict:
    return _run_temp_origin_card(runner, temp_table, actual_cols, "status_codes")


def _origin_path_breakdown_from_temp(runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str]) -> dict:
    return _run_temp_origin_card(runner, temp_table, actual_cols, "path_breakdown")


def _origin_pop_latency_from_temp(
    runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str], limit: int
) -> dict:
    return _run_temp_origin_card(runner, temp_table, actual_cols, "pop_latency", [limit])


def _origin_ip_health_from_temp(
    runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str], limit: int
) -> dict:
    return _run_temp_origin_card(runner, temp_table, actual_cols, "ip_health", [limit])


async def get_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    *,
    bucket_minutes: float = 5,
    split_by_leg: bool = False,
    timeseries_metric: str = "ttfb",
    timeseries_percentile: str = "p95",
    slow_urls_limit: int = 20,
    slow_urls_min_requests: int = 50,
    ip_health_limit: int = 30,
    pop_latency_limit: int = 30,
    sections: set[str] | None = None,
) -> dict:
    """Composite origin endpoint — seven origin cards from rollups or one parquet scan."""
    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()
    empty_payload = empty_origin_aggregates()
    if not actual_cols:
        return {**empty_payload, **runner.telemetry()}

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    has_filters = bool(filters)
    actual_set = set(actual_cols)
    wanted_cols = (
        "timestamp",
        "cache",
        "edge",
        "url",
        "oip",
        "ost",
        "pop",
        "ottfb",
        "ottlb",
        "ttfb",
        "elapsed",
        "obytes",
    )
    select_cols = [f'"{c}"' for c in wanted_cols if c in actual_set]
    if not select_cols:
        return {**empty_payload, **runner.telemetry()}
    temp_table = f"t_origin_{uuid.uuid4().hex}"
    create_sql = SQL.AGGREGATES_CREATE_TEMP.format(
        temp_table=temp_table,
        select_cols=", ".join(select_cols),
        lat_us_expr=origin_latency_us_expr(actual_set),
        table=table_name,
        where_clause=where_clause,
    )
    timer = SectionTimer()
    section_timings = timer.entries
    merged: dict[str, Any] = {}
    missed: set[str] = set()

    def _want(name: str) -> bool:
        return sections is None or name in sections

    for name, reader in (
        (
            "summary",
            lambda: runner.try_origin_summary_from_rollup(
                start_time, end_time, has_filters=has_filters, actual_cols=actual_set
            ),
        ),
        (
            "slow_urls",
            lambda: runner.try_slow_urls_from_rollup(
                start_time,
                end_time,
                has_filters=has_filters,
                min_requests=slow_urls_min_requests,
                limit=slow_urls_limit,
            ),
        ),
        ("status_codes", lambda: runner.try_origin_status_from_rollup(start_time, end_time, has_filters=has_filters)),
        (
            "path_breakdown",
            lambda: runner.try_origin_path_breakdown_from_rollup(start_time, end_time, has_filters=has_filters),
        ),
        (
            "pop_latency",
            lambda: runner.try_origin_pop_latency_from_rollup(
                start_time, end_time, has_filters=has_filters, limit=pop_latency_limit
            ),
        ),
        (
            "ip_health",
            lambda: runner.try_origin_ip_health_from_rollup(
                start_time, end_time, has_filters=has_filters, limit=ip_health_limit
            ),
        ),
        (
            "timeseries",
            lambda: runner.try_origin_latency_ts_from_rollup(
                start_time,
                end_time,
                has_filters=has_filters,
                bucket_minutes=bucket_minutes,
                metric=timeseries_metric,
                percentile=timeseries_percentile,
                split_by_leg=split_by_leg,
                table_name=table_name,
                where_clause=where_clause,
                params=params,
            ),
        ),
    ):
        if _want(name):
            _ts = time.perf_counter()
            result = reader()
            timer.mark(
                name,
                _ts,
                source="rollup",
                activity="compute" if result is not None else "overhead",
                current_execution=True,
                outcome="served" if result is not None else "fallback",
            )
            if result is not None:
                merged[name] = result
            else:
                missed.add(name)

    if not missed:
        section_timings.append(
            {"section": "origin:temp_skipped", "time_ms": 0.0, "activity": "skipped", "current_execution": True}
        )
        return {
            "has_data": (merged.get("summary") or {}).get("has_data", False),
            "section_timings": section_timings,
            **merged,
            **runner.telemetry(),
        }

    _t = time.perf_counter()
    if not runner.create_temp_table(create_sql, params):
        return {**empty_payload, **runner.telemetry()}
    timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True, outcome="served")

    try:
        for name, reader in (
            ("summary", lambda: _origin_summary_from_temp(runner, temp_table, actual_set)),
            (
                "slow_urls",
                lambda: _origin_slow_urls_from_temp(
                    runner, temp_table, actual_set, slow_urls_min_requests, slow_urls_limit
                ),
            ),
            (
                "timeseries",
                lambda: _origin_timeseries_from_temp(
                    runner,
                    temp_table,
                    actual_set,
                    bucket_minutes,
                    split_by_leg,
                    timeseries_metric,
                    timeseries_percentile,
                ),
            ),
            ("status_codes", lambda: _origin_status_codes_from_temp(runner, temp_table, actual_set)),
            ("path_breakdown", lambda: _origin_path_breakdown_from_temp(runner, temp_table, actual_set)),
            ("pop_latency", lambda: _origin_pop_latency_from_temp(runner, temp_table, actual_set, pop_latency_limit)),
            ("ip_health", lambda: _origin_ip_health_from_temp(runner, temp_table, actual_set, ip_health_limit)),
        ):
            if _want(name) and name in missed:
                _ts = time.perf_counter()
                merged[name] = reader()
                timer.mark(name, _ts, source="live", activity="compute", current_execution=True, outcome="served")

        return {
            "has_data": (merged.get("summary") or {}).get("has_data", False),
            "section_timings": section_timings,
            **merged,
            **runner.telemetry(),
        }
    finally:
        _drop_temp_table(runner, temp_table, "get_aggregates")


CacheRegistry.register("origin._response_cache", _response_cache)
