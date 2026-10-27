"""Origin, performance, CMCD, and RUM metrics repositories."""

from __future__ import annotations

import logging
import math
import os
import re
import sys as _sys
import time
import time as _time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.core import metric_expressions
from backend.core.metric_expressions import coalesce
from backend.core.retry import retry_call
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _drop_temp_table,
    _safe_table,
    empty_schema_response,
    origin_latency_us_expr,
    percentile_ms_expr,
    safe_iso,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import track_query

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


# ── POP location helpers ──────────────────────────────────────────────────────

SHIELDING_ANOMALY_MIN_REQUESTS = 30


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2.0 * 6371.0 * math.asin(math.sqrt(a))


def _enrich_with_distance(row: dict) -> dict:
    from backend.utils.pop_utils import get_pop_lat_lon_map

    pops = get_pop_lat_lon_map()
    e_coords = pops.get(str(row.get("edge_pop", "")).upper())
    s_coords = pops.get(str(row.get("shield_pop", "")).upper())
    low_sample = (row.get("requests") or 0) < SHIELDING_ANOMALY_MIN_REQUESTS
    dist_km = light_rtt_ms = efficiency = None
    anomaly_eligible = False
    if e_coords and s_coords:
        dist = _haversine_km(e_coords[0], e_coords[1], s_coords[0], s_coords[1])
        dist_km, light_rtt_ms = round(dist, 1), round(2.0 * dist / 200_000.0 * 1000.0, 2)
        p50 = row.get("p50_ms")
        efficiency = round(p50 / light_rtt_ms, 2) if (light_rtt_ms > 0.5 and p50 is not None) else None
        anomaly_eligible = bool(
            efficiency is not None and efficiency > 3.0 and p50 is not None and p50 - light_rtt_ms >= 20.0
        )
    row.update(
        distance_km=dist_km,
        light_speed_rtt_ms=light_rtt_ms,
        efficiency_ratio=efficiency,
        low_sample=low_sample,
        anomaly_eligible=anomaly_eligible,
        anomaly_static=anomaly_eligible and not low_sample,
        edge_lat=e_coords[0] if e_coords else None,
        edge_lon=e_coords[1] if e_coords else None,
        shield_lat=s_coords[0] if s_coords else None,
        shield_lon=s_coords[1] if s_coords else None,
    )
    return row


_SUMMARY_KEYS = (
    "total_misses",
    "total_passes",
    "ottfb_p50_ms",
    "ottfb_p75_ms",
    "ottfb_p95_ms",
    "ottfb_p99_ms",
    "ottlb_p50_ms",
    "ottlb_p95_ms",
    "cdn_overhead_p50_ms",
    "origin_error_rate",
    "obytes_p50",
)


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
        return {"has_data": False, **{k: None for k in _SUMMARY_KEYS}}
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


def _shape_keyed_rows(rows: list[tuple], keys: tuple[str, ...], **extra: Any) -> dict:
    return {"has_data": bool(rows), **extra, "rows": [dict(zip(keys, r, strict=False)) for r in rows]}


def _shape_slow_url_rows(rows: list[tuple]) -> dict:
    return _shape_keyed_rows(rows, ("url", "requests", "p50_ms", "p95_ms", "p99_ms"))


def _shape_status_code_rows(rows: list[tuple]) -> dict:
    return _shape_keyed_rows(rows, ("status", "count", "pct"))


def _shape_path_breakdown_rows(rows: list[tuple]) -> dict:
    return _shape_keyed_rows(
        rows, ("edge", "requests", "p50_ms", "p95_ms"), shielding_detected=any(r[0] is False for r in rows)
    )


def _shape_pop_latency_rows(rows: list[tuple]) -> dict:
    if not rows:
        return {"has_data": False, "requires_group_c": False, "rows": []}
    valid_p95s = sorted(r[3] for r in rows if r[3] is not None)
    median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
    return {
        "has_data": True,
        "requires_group_c": False,
        "median_p95_ms": median_p95,
        "rows": [
            {
                "pop": r[0],
                "requests": r[1],
                "p50_ms": r[2],
                "p95_ms": r[3],
                "elevated": r[3] is not None and median_p95 is not None and r[3] > median_p95 * 2,
            }
            for r in rows
        ],
    }


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
    empty_payload = {
        "has_data": False,
        "summary": {},
        "timeseries": {"has_data": False, "series": []},
        "slow_urls": {"has_data": False, "rows": []},
        "status_codes": {"has_data": False, "rows": []},
        "path_breakdown": {"has_data": False, "shielding_detected": False, "rows": []},
        "pop_latency": {"has_data": False, "requires_group_c": False, "rows": []},
        "ip_health": {"has_data": False, "rows": []},
    }
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


# ── Folded from backend.repositories.performance ──────────────────────────
import sys as _sys  # noqa: E402


def _top_latency_sql(col: str, min_reqs: int) -> str:
    el = "CAST(elapsed AS DOUBLE)"
    return (
        f"SELECT {col}, count(*) AS reqs, AVG({el}) / 1000.0 as avg, "
        f"{percentile_ms_expr(el, 0.5, approx=True)} as p50, "
        f"{percentile_ms_expr(el, 0.95, approx=True)} as p95, "
        f"{percentile_ms_expr(el, 0.99, approx=True)} as p99 "
        f"FROM {{temp_table}} WHERE {col} IS NOT NULL AND elapsed IS NOT NULL "
        f"GROUP BY {col} HAVING count(*) > {min_reqs} ORDER BY {{sort_idx}} DESC LIMIT 20"
    )


TOP_URLS = _top_latency_sql("url", 5)
TOP_ASNS = _top_latency_sql("asn", 10)

_TTL_BUCKETS = (
    (0, "0s"),
    (10, "<10s"),
    (30, "<30s"),
    (60, "<1m"),
    (300, "<5m"),
    (600, "<10m"),
    (1800, "<30m"),
    (3600, "<1h"),
    (10800, "<3h"),
    (21600, "<6h"),
    (43200, "<12h"),
    (86400, "<1d"),
    (259200, "<3d"),
    (604800, "<1w"),
    (1209600, "<2w"),
    (2592000, "<30d"),
    (7776000, "<90d"),
    (31536000, "<1y"),
)
TTL_DIST = (
    "SELECT CASE "
    + " ".join(f"WHEN ttl <= {lim} THEN '{lbl}'" for lim, lbl in _TTL_BUCKETS)
    + " ELSE '>1y' END as bucket, count(*) as count, min(ttl) as min_ttl "
    "FROM {temp_table} WHERE ttl IS NOT NULL GROUP BY 1 ORDER BY min_ttl"
)

SCATTER_WATERFALL = """
    WITH components AS (
        SELECT cache::VARCHAR AS cache, CAST(ttfb AS DOUBLE) * 1000.0 AS origin_ms,
               (CAST(elapsed AS DOUBLE) / 1000.0 - CAST(ttfb AS DOUBLE) * 1000.0) AS edge_ms,
               {ottfb_expr} AS origin_wait, GREATEST(0.0, {ottlb_expr} - {ottfb_expr}) AS origin_download,
               GREATEST(0.0, (CAST(ttfb AS DOUBLE) * 1000.0) - {ottfb_expr}) AS edge_processing,
               GREATEST(0.0, (CAST(elapsed AS DOUBLE) / 1000.0) - GREATEST({ottlb_expr}, CAST(ttfb AS DOUBLE) * 1000.0)) AS client_download
        FROM {source_rel} {comp_where}
    )
    SELECT 's'::VARCHAR AS row_type, origin_ms, edge_ms, cache,
           NULL::DOUBLE AS w_edge_processing, NULL::DOUBLE AS w_origin_wait,
           NULL::DOUBLE AS w_origin_download, NULL::DOUBLE AS w_client_download
    FROM components WHERE edge_ms >= 0 USING SAMPLE 300
    UNION ALL
    SELECT 'w'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR,
           AVG(edge_processing), AVG(origin_wait), AVG(origin_download), AVG(client_download)
    FROM components
"""


def build_scatter_waterfall_sql(*, source_rel: str, comp_where: str, has_ottfb: bool, has_ottlb: bool) -> str:
    ottfb_expr = coalesce("CAST(ottfb AS DOUBLE) / 1000.0", "0") if has_ottfb else "0"
    ottlb_expr = coalesce("CAST(ottlb AS DOUBLE) / 1000.0", "0") if has_ottlb else "0"
    return SCATTER_WATERFALL.format(
        source_rel=source_rel, comp_where=comp_where, ottfb_expr=ottfb_expr, ottlb_expr=ottlb_expr
    )


_sys.modules.setdefault("backend.repositories._sql.performance", _sys.modules[__name__])

_PERF_CACHE_TTL = float(os.getenv("PERF_CACHE_TTL_SECONDS", "30"))
_perf_memo = ResponseMemo(ttl=_PERF_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("performance_aggregates_memo", _perf_memo.cache)


def _perf_memo_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> str:
    return digest_cache_key(
        {
            "ep": "performance_aggregates",
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(filters),
            "sb": sort_by,
            "sec": sorted(sections) if sections is not None else None,
            "cd": str(src.get("_cache_dir_override", "")),
        },
        src,
    )


def _active_perf_memo_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> str | None:
    if _PERF_CACHE_TTL <= 0 or start_time is None or end_time is None:
        return None
    return _perf_memo_key(src, start_time, end_time, filters, sort_by, sections)


def peek_performance_aggregates(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> dict | None:
    key = _active_perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
    cached = _perf_memo.get_with_telemetry(key) if key is not None else None
    return dict(cached) if cached is not None else None


_PERF_TEMP_COL_ORDER = ("url", "asn", "ttfb", "elapsed", "cache", "ttl", "ottfb", "ottlb")
_PERF_SECTION_DEFAULTS: dict[str, Any] = {
    "top_urls": [],
    "top_asns": [],
    "ttl_dist": [],
    "scatter": [],
    "waterfall": {},
}


def get_performance_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> dict:
    cache_key = _active_perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
    if cache_key is not None and (cached := _perf_memo.get_with_telemetry(cache_key)) is not None:
        return dict(cached)

    def _compute() -> dict:
        if cache_key is not None and (cached_inner := _perf_memo.get_with_telemetry(cache_key)) is not None:
            return dict(cached_inner)
        return _compute_performance_aggregates(con, src, start_time, end_time, filters, sort_by, sections, cache_key)

    return (
        _perf_memo.coalesce(f"perf_agg:{cache_key}", _compute)
        if cache_key is not None
        else _perf_memo.result(_compute(), state="bypass")
    )


def _compute_performance_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    sort_by: str,
    sections: set[str] | None,
    cache_key: str | None,
) -> dict:
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    _t = time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)
    if not actual_cols:
        return {
            **{k: ([] if isinstance(v, list) else {}) for k, v in _PERF_SECTION_DEFAULTS.items()},
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    _t = time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)
    results = {**runner.telemetry()}
    sort_idx = {"avg": 3, "p50": 4, "p95": 5, "p99": 6}.get(sort_by, 6)

    def _resolve_asns(asn_res: list) -> list[dict]:
        from backend.core import duckdb as _db

        labels_started = time.perf_counter()
        asn_list = [int(r[0]) for r in asn_res if str(r[0]).isdigit()]
        asn_names = _db.get_asn_names(src["name"], asn_list)
        out: list[dict] = []
        for r in asn_res:
            asn_val = int(r[0]) if str(r[0]).isdigit() else r[0]
            label = (
                _db.format_asn_label(asn_val, asn_names.get(asn_val, "")) if isinstance(asn_val, int) else str(asn_val)
            )
            out.append(
                {"asn": asn_val, "label": label, "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]}
            )
        timer.mark("performance.asn_labels", labels_started, activity="overhead", current_execution=True)
        return out

    needed_cols: set[str] = set()
    live_sections: set[str] = set()

    def _need(section: str, cols: set[str]) -> None:
        live_sections.add(section)
        needed_cols.update(c for c in cols if c in actual_cols)

    def _try_rollup_section(
        sec: str,
        req_cols: set[str],
        rollup_mark: str,
        skip_mark: str,
        fetch_fn: Callable[[], Any],
        apply_fn: Callable[[Any], None],
    ) -> None:
        t0 = time.perf_counter()
        if not _want(sec):
            return
        if not all(c in actual_cols for c in req_cols):
            results[sec] = []
            timer.mark(skip_mark, t0, activity="skipped", current_execution=True)
            return
        rolled = fetch_fn()
        timer.mark(
            rollup_mark,
            t0,
            source="rollup",
            activity="compute" if rolled is not None else "overhead",
            current_execution=True,
            outcome="served" if rolled is not None else "fallback",
        )
        if rolled is not None:
            apply_fn(rolled)
        else:
            _need(sec, req_cols)

    def _apply_top_urls(rolled: dict[str, Any]) -> None:
        results["top_urls"] = [
            {
                "url": r["value"],
                "requests": r["requests"],
                "avg": r["avg_ms"],
                "p50": r["p50_ms"],
                "p95": r["p95_ms"],
                "p99": r["p99_ms"],
            }
            for r in rolled["rows"]
        ]
        results["approx"] = True

    def _apply_top_asns(rolled: dict[str, Any]) -> None:
        results["approx"] = True
        results["top_asns"] = _resolve_asns(
            [(r["value"], r["requests"], r["avg_ms"], r["p50_ms"], r["p95_ms"], r["p99_ms"]) for r in rolled["rows"]]
        )

    for sec, dim, min_reqs, apply_fn in (
        ("top_urls", "url", 5, _apply_top_urls),
        ("top_asns", "asn", 10, _apply_top_asns),
    ):

        def _fetch_latency(_dim: str = dim, _min: int = min_reqs) -> dict[str, Any] | None:
            return runner.try_perf_latency_from_rollup(
                start_time,
                end_time,
                dimension=_dim,
                sort_by=sort_by,
                has_filters=bool(filters),
                min_requests=_min,
                limit=20,
            )

        _try_rollup_section(
            sec,
            {dim, "elapsed"},
            f"{sec}_query_rollup",
            f"{sec}_query",
            _fetch_latency,
            apply_fn,
        )

    _try_rollup_section(
        "ttl_dist",
        {"ttl"},
        "ttl_dist_rollup",
        "ttl_dist_query",
        lambda: runner.try_perf_ttl_dist_from_rollup(start_time, end_time, has_filters=bool(filters)),
        lambda rolled_ttl: results.__setitem__("ttl_dist", rolled_ttl),
    )

    _want_scatter, _want_waterfall = _want("scatter"), _want("waterfall")
    _want_scatter_waterfall = _want_scatter or _want_waterfall
    if _want_scatter_waterfall and "ttfb" in actual_cols and "elapsed" in actual_cols:
        if live_sections:
            _need("scatter_waterfall", {"ttfb", "elapsed", "cache", "ottfb", "ottlb"})
        else:
            live_sections.add("scatter_waterfall")
    elif _want_scatter_waterfall:
        if _want_scatter:
            results["scatter"] = []
        if _want_waterfall:
            results["waterfall"] = {}

    temp_table = None
    if needed_cols:
        cols = [c for c in _PERF_TEMP_COL_ORDER if c in needed_cols]
        _t = time.perf_counter()
        temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
        timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True)
        timer.mark("perf:temp_narrowed", time.perf_counter(), activity="overhead", current_execution=True)
    else:
        timer.mark("perf:temp_skipped", time.perf_counter(), activity="skipped", current_execution=True)

    try:
        if "top_urls" in live_sections and temp_table:
            _t = time.perf_counter()
            url_res = runner.execute(SQL.TOP_URLS.format(temp_table=temp_table, sort_idx=sort_idx)).fetchall()
            results["top_urls"] = [
                {"url": r[0], "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]} for r in url_res
            ]
            timer.mark("top_urls_query", _t, source="live", activity="compute", current_execution=True)

        if "top_asns" in live_sections and temp_table:
            _t = time.perf_counter()
            asn_res = runner.execute(SQL.TOP_ASNS.format(temp_table=temp_table, sort_idx=sort_idx)).fetchall()
            timer.mark("top_asns_query", _t, source="live", activity="compute", current_execution=True)
            results["top_asns"] = _resolve_asns(asn_res)

        if "ttl_dist" in live_sections and temp_table:
            _t = time.perf_counter()
            ttl_res = runner.execute(SQL.TTL_DIST.format(temp_table=temp_table)).fetchall()
            results["ttl_dist"] = [{"bucket": r[0], "count": r[1]} for r in ttl_res]
            timer.mark("ttl_dist_query", _t, source="live", activity="compute", current_execution=True)

        if "scatter_waterfall" in live_sections:
            _t = time.perf_counter()
            if temp_table:
                source_rel, comp_where, q_params = temp_table, "WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL", None
            else:
                source_rel = f'"{table_name}"'
                comp_where = (
                    f"WHERE {where_clause} AND ttfb IS NOT NULL AND elapsed IS NOT NULL"
                    if where_clause.strip()
                    else "WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL"
                )
                q_params = params

            combined_q = SQL.build_scatter_waterfall_sql(
                source_rel=source_rel,
                comp_where=comp_where,
                has_ottfb="ottfb" in actual_cols,
                has_ottlb="ottlb" in actual_cols,
            )
            combined_rows = runner.execute(combined_q, q_params).fetchall()
            scatter: list[dict] = []
            _wf_keys = ("edge_processing", "origin_wait", "origin_download", "client_download")
            waterfall_avg = dict.fromkeys(_wf_keys, 0.0)
            for r in combined_rows:
                if r[0] == "s":
                    scatter.append({"origin": r[1], "edge": r[2], "cache": r[3]})
                else:
                    waterfall_avg = {k: float(r[i + 4] or 0.0) for i, k in enumerate(_wf_keys)}
            if _want_scatter:
                results["scatter"] = scatter
            if _want_waterfall:
                results["waterfall"] = {"avg": waterfall_avg}
            timer.mark("scatter_waterfall_query", _t, source="live", activity="compute", current_execution=True)
    finally:
        _drop_temp_table(runner, temp_table, "get_performance_aggregates")

    for sec, default in _PERF_SECTION_DEFAULTS.items():
        if _want(sec) and sec not in results:
            results[sec] = [] if isinstance(default, list) else {}

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    if cache_key is not None:
        _perf_memo.put(cache_key, dict(results))
    return results


# ── Folded from backend.repositories.cmcd ─────────────────────────────────

OVERVIEW = """
    WITH base_metrics AS (
        SELECT COUNT(DISTINCT cmcd_sid) AS active_sessions, {rebuffer_rate_expr},
               {avg_bitrate_expr} AS avg_bitrate, {avg_buffer_length_expr} AS avg_buffer_length,
               {median_throughput_expr} AS median_throughput
        FROM {table} WHERE {where}
    ),
    peak_metrics AS (
        SELECT MAX(cnt) AS peak_viewers FROM (
            SELECT COUNT(DISTINCT cmcd_sid) AS cnt FROM {table} WHERE {where} GROUP BY {bucket_expr}
        )
    ),
    dur_metrics AS (
        SELECT ROUND(AVG(dur), 0) AS avg_dur FROM (
            SELECT EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
            FROM {table} WHERE {where} GROUP BY cmcd_sid HAVING dur > 0
        )
    )
    SELECT b.active_sessions, b.rebuffer_rate, b.avg_bitrate, b.avg_buffer_length,
           b.median_throughput, p.peak_viewers, d.avg_dur
    FROM base_metrics b CROSS JOIN peak_metrics p CROSS JOIN dur_metrics d
"""

TIMESERIES_COMBINED = "SELECT {select_cols} FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1"
NEW_SESSIONS = (
    "SELECT {first_bucket} AS bucket, COUNT(*) AS new_sessions FROM "
    "(SELECT cmcd_sid, MIN(timestamp) AS first_ts FROM {table} WHERE {where} GROUP BY cmcd_sid) sub GROUP BY 1"
)


def _cmcd_per_session_sql(dim_col: str, session_metrics: str, outer_cols: str, *, non_empty: bool = False) -> str:
    pred = f"{dim_col} IS NOT NULL AND {dim_col} != ''" if non_empty else f"{dim_col} IS NOT NULL"
    return (
        f"WITH per_session AS (SELECT {dim_col}, cmcd_sid, {session_metrics} FROM {{table}} "
        f"WHERE {{where}} AND {pred} GROUP BY 1, 2) "
        f"SELECT {dim_col}, {outer_cols} FROM per_session GROUP BY 1 ORDER BY session_count DESC LIMIT {{top_n}}"
    )


def _cmcd_dist_sql(col: str, count_expr: str) -> str:
    return (
        f"SELECT {col}, {count_expr} FROM {{table}} "
        f"WHERE {{where}} AND {col} IS NOT NULL AND {col} != '' GROUP BY 1 ORDER BY 2 DESC"
    )


_REB_RATE_SQL = "ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate"
_HAD_REB_SQL = "MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer"

TOP_CONTENT = _cmcd_per_session_sql(
    "cmcd_cid",
    "{rebuffer_agg} AS had_rebuffer, {br_agg} AS avg_br, {bl_agg} AS avg_bl",
    f"COUNT(*) AS session_count, {_REB_RATE_SQL}, AVG(avg_br) AS avg_br, AVG(avg_bl) AS avg_bl",
    non_empty=True,
)
REBUFFER_BY_COUNTRY = _cmcd_per_session_sql(
    "country",
    f"{_HAD_REB_SQL}, {{mtp_agg}} AS median_mtp",
    f"{_REB_RATE_SQL}, COUNT(*) AS session_count, AVG(median_mtp) AS median_mtp",
)
REBUFFER_BY_ASN = _cmcd_per_session_sql(
    "asn",
    f"{_HAD_REB_SQL}, {{br_agg}} AS avg_br, {{bl_agg}} AS avg_bl",
    f"{_REB_RATE_SQL}, COUNT(*) AS session_count, AVG(avg_br) AS avg_br, AVG(avg_bl) AS avg_bl",
)

DIST_BOTH = (
    "SELECT 'ot' AS kind, cmcd_ot AS val, COUNT(*) AS cnt FROM {table} "
    "WHERE {where} AND cmcd_ot IS NOT NULL AND cmcd_ot != '' GROUP BY 2 "
    "UNION ALL SELECT 'sf' AS kind, cmcd_sf AS val, COUNT(DISTINCT cmcd_sid) AS cnt FROM {table} "
    "WHERE {where} AND cmcd_sf IS NOT NULL AND cmcd_sf != '' GROUP BY 2 ORDER BY kind ASC, cnt DESC"
)
DIST_OT = _cmcd_dist_sql("cmcd_ot", "COUNT(*) AS request_count")
DIST_SF = _cmcd_dist_sql("cmcd_sf", "COUNT(DISTINCT cmcd_sid) AS session_count")

_DUR_BUCKETS = ((30, "<30s"), (120, "30s–2m"), (300, "2–5m"), (600, "5–10m"), (1800, "10–30m"), (3600, "30m–1h"))
SESSION_DURATION_DIST = (
    "WITH session_spans AS (SELECT cmcd_sid, EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur "
    "FROM {table} WHERE {where} GROUP BY cmcd_sid) SELECT CASE "
    + " ".join(f"WHEN dur < {lim} THEN '{lbl}'" for lim, lbl in _DUR_BUCKETS)
    + " ELSE '>1h' END AS duration_bucket, CASE "
    + " ".join(f"WHEN dur < {lim} THEN {i}" for i, (lim, _) in enumerate(_DUR_BUCKETS, 1))
    + " ELSE 7 END AS sort_order, COUNT(*) AS session_count FROM session_spans GROUP BY 1, 2 ORDER BY 2"
)

_sql: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.cmcd", _sys.modules[__name__])

# CMCD fields that must exist in the schema for any section to return data.
_CMCD_REQUIRED_COL = "cmcd_sid"

_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128


def _cmcd_response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int,
    top_n: int,
    sections: set[str] | None,
) -> str:
    # Key field order is load-bearing (serialized as-is): s, e, f, bs, tn, sec.
    # mask_ips is intentionally omitted because CMCD aggregates return no IP columns.
    sec_val = sorted(list(sections)) if sections is not None else None
    payload = {
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "bs": bucket_seconds,
        "tn": top_n,
        "sec": sec_val,
    }
    return digest_cache_key(payload, src)


def get_cmcd_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
    top_n: int = 30,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> dict[str, Any]:
    """Return CMCD streaming analytics aggregates.

    Scans parquet once into a temp table, then runs all section queries
    against the materialized rows. The four time-series sections share a
    single GROUP BY scan to cut I/O further.
    """
    cache_key = _cmcd_response_cache_key(
        src=src,
        start_time=start_time,
        end_time=end_time,
        filters=filters,
        bucket_seconds=bucket_seconds,
        top_n=top_n,
        sections=sections,
    )
    if start_time is not None and end_time is not None:
        cached_fast = _memo.get(cache_key)
        if cached_fast is not None:
            return {**cached_fast, "section_timings": [], "debug_queries": [], "debug_calls": []}

    def _compute() -> dict[str, Any]:
        if start_time is not None and end_time is not None:
            cached_inner = _memo.get(cache_key)
            if cached_inner is not None:
                return {**cached_inner, "section_timings": [], "debug_queries": [], "debug_calls": []}
        return _compute_cmcd_aggregates(
            con=con,
            src=src,
            start_time=start_time,
            end_time=end_time,
            filters=filters,
            bucket_seconds=bucket_seconds,
            top_n=top_n,
            sections=sections,
            cache_key=cache_key,
        )

    if start_time is not None and end_time is not None:
        return _memo.coalesce(f"cmcd_agg:{cache_key}", _compute)
    result = _compute()
    if result.get("is_cached") is True:
        return result
    return _memo.result(result, state="miss")


def _compute_cmcd_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int,
    top_n: int,
    sections: set[str] | None,
    cache_key: str,
) -> dict[str, Any]:
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)

    if not actual_cols or _CMCD_REQUIRED_COL not in actual_cols:
        return {
            "available": _CMCD_REQUIRED_COL in actual_cols if actual_cols else False,
            "has_data": False,
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    cached = _memo.get(cache_key)
    if cached is not None:
        return {**cached, "section_timings": section_timings, **runner.telemetry()}

    table_name = _safe_table(src["name"])

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time,
        end_time,
        filters,
        actual_cols,
        inline_params=True,
        partition_pruning=True,
    )
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)

    cmcd_where = f"{where_clause} AND cmcd_sid IS NOT NULL AND cmcd_sid != ''"

    bucket_ms = bucket_seconds * 1000

    has_country = "country" in actual_cols
    has_asn = "asn" in actual_cols
    has_bl = "cmcd_bl" in actual_cols
    has_br = "cmcd_br" in actual_cols
    has_bs = "cmcd_bs" in actual_cols
    has_mtp = "cmcd_mtp" in actual_cols
    has_ot = "cmcd_ot" in actual_cols
    has_sf = "cmcd_sf" in actual_cols
    has_su = "cmcd_su" in actual_cols
    has_tb = "cmcd_tb" in actual_cols
    has_rtp = "cmcd_rtp" in actual_cols
    has_cid = "cmcd_cid" in actual_cols
    has_dl = "cmcd_dl" in actual_cols

    results: dict[str, Any] = {"available": True, **runner.telemetry()}

    # ── Temp table: scan parquet once ──────────────────────────────────────────
    temp_cols = ["timestamp", "cmcd_sid"]
    for col in (
        "cmcd_bl",
        "cmcd_br",
        "cmcd_bs",
        "cmcd_mtp",
        "cmcd_ot",
        "cmcd_sf",
        "cmcd_su",
        "cmcd_tb",
        "cmcd_rtp",
        "cmcd_cid",
        "cmcd_dl",
        "country",
        "asn",
    ):
        if col in actual_cols:
            temp_cols.append(col)

    _t = _time.perf_counter()
    with runner.temp_table(temp_cols, actual_cols, table_name, cmcd_where, params) as tmp:
        timer.mark(
            "temp_table_create",
            _t,
            source="live",
            activity="compute",
            current_execution=True,
            outcome="served" if tmp is not None else "failed",
        )
        if tmp is None:
            return {
                "available": False,
                "reason": "Data temporarily unavailable — view refresh failed. Retry in a moment.",
                **runner.telemetry(),
            }

        t = tmp
        w = "1=1"

        # ── Overview ──────────────────────────────────────────────────────
        if _want("overview"):
            _t = _time.perf_counter()
            sql = _sql.OVERVIEW.format(
                rebuffer_rate_expr=_rebuffer_rate_expr(has_bs),
                avg_bitrate_expr="AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL",
                avg_buffer_length_expr="AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL",
                median_throughput_expr=(
                    "APPROX_QUANTILE(cmcd_mtp, 0.5) FILTER (WHERE cmcd_ot = 'v')" if has_mtp and has_ot else "NULL"
                ),
                table=t,
                where=w,
                bucket_expr=_bucket_expr(bucket_ms),
            )
            row = runner.execute(sql).fetchone()
            if row:
                results["overview"] = {
                    "active_sessions": row[0] or 0,
                    "rebuffer_rate": _round_val(row[1]),
                    "avg_bitrate": _round_val(row[2]),
                    "avg_buffer_length": _round_val(row[3]),
                    "median_throughput": _round_val(row[4]),
                    "peak_viewers": row[5] or 0,
                    "avg_session_duration": int(row[6]) if row[6] else None,
                }
            timer.mark("overview", _t, source="live", activity="compute", current_execution=True)

        # ── Combined time-series scan ─────────────────────────────────────
        want_ts = {
            "sessions_ts": _want("sessions_ts"),
            "buffer_health_ts": _want("buffer_health_ts") and has_bl,
            "bitrate_ts": _want("bitrate_ts") and has_br,
            "throughput_ts": _want("throughput_ts") and has_mtp,
            "startup_ts": _want("startup_ts") and has_su,
        }
        if any(want_ts.values()):
            _t = _time.perf_counter()
            video_filter = "cmcd_ot = 'v'" if has_ot else "TRUE"
            select_parts = [f"{_bucket_expr(bucket_ms)} AS bucket"]
            col_map: dict[str, list[str]] = {}

            if want_ts["sessions_ts"]:
                select_parts.append("COUNT(DISTINCT cmcd_sid) AS concurrent_sessions")
                rebuffer_pct = (
                    "ROUND(COUNT(DISTINCT cmcd_sid) FILTER (WHERE cmcd_bs = true) * 100.0"
                    " / NULLIF(COUNT(DISTINCT cmcd_sid), 0), 2)"
                    if has_bs
                    else "NULL"
                )
                select_parts.append(f"{rebuffer_pct} AS rebuffer_session_pct")
                col_map["sessions_ts"] = ["concurrent_sessions", "rebuffer_session_pct"]

            if want_ts["buffer_health_ts"]:
                select_parts.append(f"APPROX_QUANTILE(cmcd_bl, 0.5) FILTER (WHERE {video_filter}) AS p50_buffer")
                select_parts.append(f"APPROX_QUANTILE(cmcd_bl, 0.95) FILTER (WHERE {video_filter}) AS p95_buffer")
                bs_rate = (
                    "ROUND(COUNT(*) FILTER (WHERE cmcd_bs = true) * 100.0 / NULLIF(COUNT(*), 0), 2)"
                    if has_bs
                    else "NULL"
                )
                select_parts.append(f"{bs_rate} AS starvation_rate")
                col_map["buffer_health_ts"] = ["p50_buffer", "p95_buffer", "starvation_rate"]

            if want_ts["bitrate_ts"]:
                select_parts.append(f"AVG(cmcd_br) FILTER (WHERE {video_filter}) AS avg_bitrate")
                util_expr = (
                    f"ROUND(AVG(cmcd_br * 1.0 / NULLIF(cmcd_tb, 0)) FILTER (WHERE {video_filter} AND cmcd_tb > 0), 4)"
                    if has_tb
                    else "NULL"
                )
                select_parts.append(f"{util_expr} AS utilization_ratio")
                col_map["bitrate_ts"] = ["avg_bitrate", "utilization_ratio"]

            if want_ts["throughput_ts"]:
                select_parts.append(f"APPROX_QUANTILE(cmcd_mtp, 0.5) FILTER (WHERE {video_filter}) AS p50_throughput")
                select_parts.append(f"APPROX_QUANTILE(cmcd_mtp, 0.95) FILTER (WHERE {video_filter}) AS p95_throughput")
                select_parts.append(f"APPROX_QUANTILE(cmcd_mtp, 0.99) FILTER (WHERE {video_filter}) AS p99_throughput")
                col_map["throughput_ts"] = ["p50_throughput", "p95_throughput", "p99_throughput"]

            if want_ts["startup_ts"]:
                select_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE cmcd_su = true) * 100.0 / NULLIF(COUNT(*), 0), 2) AS startup_ratio"
                )
                col_map["startup_ts"] = ["startup_ratio"]

            sql = _sql.TIMESERIES_COMBINED.format(select_cols=", ".join(select_parts), table=t, where=w)
            rows = runner.execute(sql).fetchall()
            desc = [d[0] for d in runner.con.description]
            col_idx = {name: i for i, name in enumerate(desc)}

            if len(rows) > 0:
                if want_ts["sessions_ts"]:
                    first_bucket = _bucket_expr(bucket_ms).replace("timestamp", "first_ts")
                    new_sess_sql = _sql.NEW_SESSIONS.format(first_bucket=first_bucket, table=t, where=w)
                    new_sess_rows = runner.execute(new_sess_sql).fetchall()
                    new_sess_map = {str(r[0]): r[1] for r in new_sess_rows}

                    sessions_ts_dict = {
                        str(r[col_idx["bucket"]]): {
                            "concurrent_sessions": r[col_idx["concurrent_sessions"]] or 0,
                            "rebuffer_session_pct": _round_val(r[col_idx["rebuffer_session_pct"]]),
                            "new_sessions": new_sess_map.get(str(r[col_idx["bucket"]]), 0),
                        }
                        for r in rows
                    }
                    results["sessions_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        sessions_ts_dict,
                        {"concurrent_sessions": 0, "rebuffer_session_pct": None, "new_sessions": 0},
                    )
                if want_ts["buffer_health_ts"]:
                    buffer_health_dict = {
                        str(r[col_idx["bucket"]]): {
                            "p50_buffer": _round_val(r[col_idx["p50_buffer"]]),
                            "p95_buffer": _round_val(r[col_idx["p95_buffer"]]),
                            "starvation_rate": _round_val(r[col_idx["starvation_rate"]]),
                        }
                        for r in rows
                    }
                    results["buffer_health_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        buffer_health_dict,
                        {"p50_buffer": None, "p95_buffer": None, "starvation_rate": None},
                    )
                if want_ts["bitrate_ts"]:
                    bitrate_dict = {
                        str(r[col_idx["bucket"]]): {
                            "avg_bitrate": _round_val(r[col_idx["avg_bitrate"]]),
                            "utilization_ratio": _round_val(r[col_idx["utilization_ratio"]]),
                        }
                        for r in rows
                    }
                    results["bitrate_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        bitrate_dict,
                        {"avg_bitrate": None, "utilization_ratio": None},
                    )
                if want_ts["throughput_ts"]:
                    throughput_dict = {
                        str(r[col_idx["bucket"]]): {
                            "p50": _round_val(r[col_idx["p50_throughput"]]),
                            "p95": _round_val(r[col_idx["p95_throughput"]]),
                            "p99": _round_val(r[col_idx["p99_throughput"]]),
                        }
                        for r in rows
                    }
                    results["throughput_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        throughput_dict,
                        {"p50": None, "p95": None, "p99": None},
                    )
                if want_ts["startup_ts"]:
                    startup_dict = {
                        str(r[col_idx["bucket"]]): {
                            "startup_ratio": _round_val(r[col_idx["startup_ratio"]]),
                        }
                        for r in rows
                    }
                    results["startup_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        startup_dict,
                        {"startup_ratio": None},
                    )
            else:
                if want_ts["sessions_ts"]:
                    results["sessions_ts"] = []
                if want_ts["buffer_health_ts"]:
                    results["buffer_health_ts"] = []
                if want_ts["bitrate_ts"]:
                    results["bitrate_ts"] = []
                if want_ts["throughput_ts"]:
                    results["throughput_ts"] = []
                if want_ts["startup_ts"]:
                    results["startup_ts"] = []
            timer.mark("timeseries_combined", _t, source="live", activity="compute", current_execution=True)

        # ── Top Content (single scan, no self-join) ───────────────────────
        if _want("top_content") and has_cid:
            _t = _time.perf_counter()
            br_agg = "AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"
            bl_agg = "AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"
            rebuffer_agg = "MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END)" if has_bs else "0"
            sql = _sql.TOP_CONTENT.format(
                rebuffer_agg=rebuffer_agg,
                br_agg=br_agg,
                bl_agg=bl_agg,
                table=t,
                where=w,
                top_n=top_n,
            )
            rows = runner.execute(sql).fetchall()
            results["top_content"] = [
                {
                    "content_id": r[0],
                    "session_count": r[1],
                    "rebuffer_rate": _round_val(r[2]),
                    "avg_bitrate": _round_val(r[3]),
                    "avg_buffer_length": _round_val(r[4]),
                }
                for r in rows
            ]
            timer.mark("top_content", _t, source="live", activity="compute", current_execution=True)

        # ── Rebuffer by Country (single scan, no self-join) ───────────────
        if _want("rebuffer_by_country") and has_country and has_bs:
            _t = _time.perf_counter()
            # MEDIAN, not APPROX_QUANTILE: this aggregate runs once per (country, session) group, and
            # a t-digest per tiny group is ~9x slower (1.1 s vs 0.12 s on 3M rows / 72k groups).
            mtp_agg = "MEDIAN(cmcd_mtp) FILTER (WHERE cmcd_ot = 'v')" if has_mtp and has_ot else "NULL"
            sql = _sql.REBUFFER_BY_COUNTRY.format(
                mtp_agg=mtp_agg,
                table=t,
                where=w,
                top_n=top_n,
            )
            rows = runner.execute(sql).fetchall()
            results["rebuffer_by_country"] = [
                {
                    "country": r[0],
                    "rebuffer_rate": _round_val(r[1]),
                    "session_count": r[2],
                    "median_throughput": _round_val(r[3]),
                }
                for r in rows
            ]
            timer.mark("rebuffer_by_country", _t, source="live", activity="compute", current_execution=True)

        # ── Rebuffer by ASN (single scan, no self-join) ───────────────────
        if _want("rebuffer_by_asn") and has_asn and has_bs:
            _t = _time.perf_counter()
            br_agg = "AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"
            bl_agg = "AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"
            sql = _sql.REBUFFER_BY_ASN.format(
                br_agg=br_agg,
                bl_agg=bl_agg,
                table=t,
                where=w,
                top_n=top_n,
            )
            rows = runner.execute(sql).fetchall()
            from backend.core import duckdb as _db

            asn_list = [int(r[0]) for r in rows if str(r[0]).isdigit()]
            asn_names = _db.get_asn_names(src["name"], asn_list)
            results["rebuffer_by_asn"] = [
                {
                    "asn": r[0],
                    "label": _db.format_asn_label(int(r[0]), asn_names.get(int(r[0]), ""))
                    if str(r[0]).isdigit()
                    else str(r[0]),
                    "rebuffer_rate": _round_val(r[1]),
                    "session_count": r[2],
                    "avg_bitrate": _round_val(r[3]),
                    "avg_buffer_length": _round_val(r[4]),
                }
                for r in rows
            ]
            timer.mark("rebuffer_by_asn", _t, source="live", activity="compute", current_execution=True)

        # ── Object Type + Streaming Format (combined scan) ────────────────
        want_ot = _want("object_type_dist") and has_ot
        want_sf = _want("streaming_format_dist") and has_sf
        if want_ot or want_sf:
            _t = _time.perf_counter()
            if want_ot and want_sf:
                sql = _sql.DIST_BOTH.format(table=t, where=w)
                rows = runner.execute(sql).fetchall()
                results["object_type_dist"] = [
                    {"object_type": r[1], "request_count": r[2]} for r in rows if r[0] == "ot"
                ]
                results["streaming_format_dist"] = [
                    {"streaming_format": r[1], "session_count": r[2]} for r in rows if r[0] == "sf"
                ]
            elif want_ot:
                sql = _sql.DIST_OT.format(table=t, where=w)
                rows = runner.execute(sql).fetchall()
                results["object_type_dist"] = [{"object_type": r[0], "request_count": r[1]} for r in rows]
            elif want_sf:
                sql = _sql.DIST_SF.format(table=t, where=w)
                rows = runner.execute(sql).fetchall()
                results["streaming_format_dist"] = [{"streaming_format": r[0], "session_count": r[1]} for r in rows]
            timer.mark("distributions", _t, source="live", activity="compute", current_execution=True)

        # ── Session Duration Distribution ─────────────────────────────────────
        if _want("session_duration_dist"):
            _t = _time.perf_counter()
            sql = _sql.SESSION_DURATION_DIST.format(table=t, where=w)
            rows = runner.execute(sql).fetchall()
            results["session_duration_dist"] = [{"duration_bucket": r[0], "session_count": r[2]} for r in rows]
            timer.mark("session_duration_dist", _t, source="live", activity="compute", current_execution=True)

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    _memo.put(cache_key, results, strip=("section_timings",))
    return results


# ── Helpers ──────────────────────────────────────────────────────────────────


def _bucket_expr(bucket_ms: int) -> str:
    return f"EPOCH_MS(CAST((EPOCH_MS(timestamp)::BIGINT // {bucket_ms}) * {bucket_ms} AS BIGINT))::TIMESTAMP"


def _rebuffer_rate_expr(has_bs: bool) -> str:
    if not has_bs:
        return "NULL AS rebuffer_rate"
    return """ROUND(
        COUNT(DISTINCT cmcd_sid) FILTER (WHERE cmcd_bs = true) * 100.0
        / NULLIF(COUNT(DISTINCT cmcd_sid), 0), 2
    ) AS rebuffer_rate"""


def _round_val(v: Any) -> Any:
    if v is None:
        return None
    return round(float(v), 2)


def _pad_timeseries(
    start_time: str | None,
    end_time: str | None,
    bucket_seconds: int,
    db_rows_dict: dict[str, dict[str, Any]],
    default_vals: dict[str, Any],
) -> list[dict[str, Any]]:
    if not start_time or not end_time:
        return sorted([{"bucket": k, **v} for k, v in db_rows_dict.items()], key=lambda x: x["bucket"])

    import datetime
    from datetime import UTC

    st = parse_iso_utc(start_time)
    et = parse_iso_utc(end_time)
    if st is None or et is None:
        return sorted([{"bucket": k, **v} for k, v in db_rows_dict.items()], key=lambda x: x["bucket"])

    st_epoch = int(st.timestamp())
    st_aligned = st_epoch - (st_epoch % bucket_seconds)
    st_dt = datetime.datetime.fromtimestamp(st_aligned, tz=UTC)

    et_epoch = int(et.timestamp())
    et_aligned = et_epoch - (et_epoch % bucket_seconds)
    et_dt = datetime.datetime.fromtimestamp(et_aligned, tz=UTC)

    padded = []
    curr = st_dt
    while curr <= et_dt:
        bucket_str = curr.strftime("%Y-%m-%d %H:%M:%S")
        if bucket_str in db_rows_dict:
            padded.append({"bucket": bucket_str, **db_rows_dict[bucket_str]})
        else:
            padded.append({"bucket": bucket_str, **default_vals})
        curr += datetime.timedelta(seconds=bucket_seconds)
    return padded


_CacheRegistry.register("cmcd._response_cache", _response_cache)


def _response_cache_key(*args, **kwargs) -> str:
    if (args and isinstance(args[0], dict)) or (
        "actual_cols" in kwargs or ("src" in kwargs and "endpoint" not in kwargs)
    ):
        return _cmcd_response_cache_key(*args, **kwargs)
    return _origin_response_cache_key(*args, **kwargs)


# ── Folded from backend.repositories.rum ──────────────────────────────────


def is_stale_view_error(exc: BaseException) -> bool:
    from backend.core.storage._core import is_stale_view_error as _is_stale_view_error

    return _is_stale_view_error(exc)


sql_rum = _sys.modules[__name__]

BEACON_HEALTH_RECENT = (
    "SELECT COUNT(*) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors) "
    "WHERE timestamp > ?"
)

BEACON_HEALTH_LAST = (
    "SELECT MAX(timestamp) FROM (SELECT timestamp FROM client_vitals UNION ALL SELECT timestamp FROM client_errors)"
)

DISTINCT_ID = "hash(COALESCE(NULLIF(req_id, ''), concat(cid, '_', CAST(epoch(timestamp) AS BIGINT))))"

RUM_TEMP_VITALS_CREATE = (
    "CREATE OR REPLACE TEMP TABLE t_client_vitals AS "
    "SELECT timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname "
    "FROM client_vitals WHERE {where_sql}"
)  # str.format template: {where_sql}, {distinct_id}

RUM_TEMP_ERRORS_CREATE = (
    "CREATE OR REPLACE TEMP TABLE t_client_errors AS "
    "SELECT timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col "
    "FROM client_errors WHERE {where_sql}"
)  # str.format template: {where_sql}, {distinct_id}

RUM_ROLLUP_MAX_BUCKET = """
SELECT MAX(bucket_start) FROM (
    SELECT MAX(bucket_start) AS bucket_start FROM rum_vitals_aggregates WHERE service_id = ?
    UNION ALL
    SELECT MAX(bucket_start) AS bucket_start FROM rum_error_aggregates WHERE service_id = ?
)
"""

RUM_TAIL_VITALS_CREATE = """
CREATE OR REPLACE TEMP TABLE t_tail_vitals AS
SELECT timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname
FROM (
    SELECT
        timestamp, req_id, cid, metric_name, metric_value, metric_rating, browser, os, device, pathname,
        ROW_NUMBER() OVER (PARTITION BY timestamp, metric_name, pathname, cid, req_id ORDER BY timestamp) AS _rn
    FROM client_vitals
    WHERE timestamp >= ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
)
WHERE _rn = 1
"""

RUM_TAIL_ERRORS_CREATE = """
CREATE OR REPLACE TEMP TABLE t_tail_errors AS
SELECT timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col
FROM (
    SELECT
        timestamp, req_id, cid, pathname, error_message, error_file, error_line, error_col,
        ROW_NUMBER() OVER (PARTITION BY timestamp, error_message, error_file, error_line, pathname, cid, req_id ORDER BY timestamp) AS _rn
    FROM client_errors
    WHERE timestamp >= ? AND timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)
)
WHERE _rn = 1
"""

RUM_TAIL_HAS_ROWS = "SELECT (SELECT COUNT(*) FROM t_tail_vitals) + (SELECT COUNT(*) FROM t_tail_errors)"


def _rum_tail_vitals_branch(
    dimension: str,
    value_expr: str,
    group_by: str,
    *,
    event_count_expr: str = "COUNT(DISTINCT {distinct_id})",
    value_sum_expr: str = "0.0",
    rating_counts: bool = False,
    p50_expr: str = "0.0",
    p75_expr: str = "0.0",
    p99_expr: str = "0.0",
    where_clause: str = "",
    from_clause: str = "t_tail_vitals",
) -> str:
    ratings = (
        "COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count, "
        "COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count, "
        "COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count"
        if rating_counts
        else "0 AS good_count, 0 AS ni_count, 0 AS poor_count"
    )
    where_sql = f" WHERE {where_clause}" if where_clause else ""
    return (
        f"SELECT ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, "
        f"'{dimension}' AS dimension, {value_expr} AS value, {event_count_expr} AS event_count, "
        f"{value_sum_expr} AS value_sum, {ratings}, "
        f"{p50_expr} AS p50_value, {p75_expr} AS p75_value, {p99_expr} AS p99_value "
        f"FROM {from_clause}{where_sql} GROUP BY {group_by}"
    )


_RUM_TAIL_VITALS_BRANCHES = "\nUNION ALL\n".join(
    [
        _rum_tail_vitals_branch(
            "total",
            "CASE WHEN metric_name LIKE 'event_%' THEN 'interactions' ELSE 'pageviews' END",
            "bucket_start, value",
        ),
        _rum_tail_vitals_branch(
            "metric_name",
            "metric_name",
            "bucket_start, metric_name",
            event_count_expr="COUNT(*)",
            value_sum_expr="CAST(IFNULL(SUM(metric_value), 0.0) AS DOUBLE)",
            rating_counts=True,
            p50_expr="CAST(MEDIAN(metric_value) AS DOUBLE)",
            p75_expr="CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            p99_expr="CAST(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
        ),
        _rum_tail_vitals_branch("browser", "IFNULL(browser, 'Unknown')", "bucket_start, browser"),
        _rum_tail_vitals_branch("os", "IFNULL(os, 'Unknown')", "bucket_start, os"),
        _rum_tail_vitals_branch("device", "IFNULL(device, 'Unknown')", "bucket_start, device"),
        _rum_tail_vitals_branch("pathname", "pathname", "bucket_start, pathname", where_clause="pathname IS NOT NULL"),
        _rum_tail_vitals_branch(
            "path_metric_load",
            "pathname",
            "bucket_start, pathname",
            event_count_expr="COUNT(*)",
            value_sum_expr="CAST(IFNULL(SUM(metric_value), 0.0) AS DOUBLE)",
            where_clause=(
                "pathname IS NOT NULL AND metric_name IN "
                "('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')"
            ),
        ),
        _rum_tail_vitals_branch(
            "path_metric_lcp",
            "pathname",
            "bucket_start, pathname",
            event_count_expr="COUNT(*)",
            p75_expr="CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            where_clause="pathname IS NOT NULL AND metric_name IN ('LCP', 'lcp')",
        ),
        _rum_tail_vitals_branch(
            "path_metric_cls",
            "pathname",
            "bucket_start, pathname",
            event_count_expr="COUNT(*)",
            p75_expr="CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS DOUBLE)",
            where_clause="pathname IS NOT NULL AND metric_name IN ('CLS', 'cls')",
        ),
        _rum_tail_vitals_branch(
            "total",
            "'total_beacons'",
            "bucket_start",
            event_count_expr="COUNT(DISTINCT distinct_id)",
            from_clause=(
                "(SELECT timestamp, {distinct_id} AS distinct_id FROM t_tail_vitals "
                "UNION ALL SELECT timestamp, {distinct_id} AS distinct_id FROM t_tail_errors)"
            ),
        ),
    ]
)

RUM_EFF_VITALS_AGG_CREATE = f"""
CREATE OR REPLACE TEMP TABLE t_eff_vitals_agg AS
WITH tail_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_vitals
    UNION
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_errors
)
SELECT
    service_id, bucket_start, dimension, value,
    event_count, value_sum, good_count, ni_count, poor_count,
    p50_value, p75_value, p99_value
FROM rum_vitals_aggregates
WHERE service_id = ?
  AND bucket_start >= CAST(? AS TIMESTAMPTZ)
  AND bucket_start <= CAST(? AS TIMESTAMPTZ)
  AND bucket_start NOT IN (SELECT bucket_start FROM tail_hours)
UNION ALL
{_RUM_TAIL_VITALS_BRANCHES}
"""

RUM_EFF_ERROR_AGG_CREATE = """
CREATE OR REPLACE TEMP TABLE t_eff_error_agg AS
WITH tail_hours AS (
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_vitals
    UNION
    SELECT DISTINCT DATE_TRUNC('hour', timestamp) AS bucket_start FROM t_tail_errors
)
SELECT
    service_id, bucket_start, dimension, value, error_count
FROM rum_error_aggregates
WHERE service_id = ?
  AND bucket_start >= CAST(? AS TIMESTAMPTZ)
  AND bucket_start <= CAST(? AS TIMESTAMPTZ)
  AND bucket_start NOT IN (SELECT bucket_start FROM tail_hours)
UNION ALL
SELECT
    ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'total' AS dimension,
    'errors' AS value, COUNT(DISTINCT {distinct_id}) AS error_count
FROM t_tail_errors GROUP BY bucket_start
UNION ALL
SELECT
    ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'pathname' AS dimension,
    pathname AS value, COUNT(DISTINCT {distinct_id}) AS error_count
FROM t_tail_errors WHERE pathname IS NOT NULL GROUP BY bucket_start, pathname
UNION ALL
SELECT
    ? AS service_id, DATE_TRUNC('hour', timestamp) AS bucket_start, 'exception' AS dimension,
    concat(IFNULL(error_message, ''), '|', IFNULL(error_file, ''), '|', CAST(IFNULL(error_line, 0) AS VARCHAR), '|', CAST(IFNULL(error_col, 0) AS VARCHAR)) AS value,
    COUNT(*) AS error_count
FROM t_tail_errors GROUP BY bucket_start, value
"""

RUM_ROLLUP_ANY_DATA = "SELECT (SELECT COUNT(*) FROM {vitals_agg_table} WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)) + (SELECT COUNT(*) FROM {error_agg_table} WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ))"


@dataclass(frozen=True)
class _RumRollupQuerySpec:
    select_exprs: str
    table_placeholder: str = "{vitals_agg_table}"
    dimension_filter: str = ""
    group_by: str = ""
    order_by: str = ""
    limit: int | None = None

    def build_sql(self) -> str:
        dim_clause = f" AND dimension = '{self.dimension_filter}'" if self.dimension_filter else ""
        group_clause = f"\nGROUP BY {self.group_by}" if self.group_by else ""
        order_clause = f"\nORDER BY {self.order_by}" if self.order_by else ""
        limit_clause = f"\nLIMIT {self.limit}" if self.limit is not None else ""
        return (
            f"\nSELECT\n    {self.select_exprs}\n"
            f"FROM {self.table_placeholder}\n"
            f"WHERE service_id = ?{dim_clause} AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)"
            f"{group_clause}{order_clause}{limit_clause}\n"
        )


RUM_ROLLUP_COUNTS_VITALS = _RumRollupQuerySpec(
    "COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'pageviews'), 0) AS pageviews,\n"
    "    COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'interactions'), 0) AS interactions"
).build_sql()

RUM_ROLLUP_COUNTS_ERRORS = _RumRollupQuerySpec(
    "COALESCE(SUM(error_count) FILTER (WHERE dimension = 'total' AND value = 'errors'), 0) AS errors_count",
    table_placeholder="{error_agg_table}",
).build_sql()

RUM_ROLLUP_COUNTS_TOTAL_BEACONS = _RumRollupQuerySpec(
    "COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'total_beacons'), 0) AS total_beacons"
).build_sql()

RUM_ROLLUP_VITALS_SUMMARY = _RumRollupQuerySpec(
    "value AS metric_name,\n"
    "    CASE WHEN SUM(event_count) > 0 THEN SUM(p75_value * event_count) / SUM(event_count) ELSE 0.0 END AS p75,\n"
    "    SUM(event_count) AS total_count,\n"
    "    SUM(good_count) AS good_count,\n"
    "    SUM(ni_count) AS ni_count,\n"
    "    SUM(poor_count) AS poor_count",
    dimension_filter="metric_name",
    group_by="value",
).build_sql()

RUM_ROLLUP_BROWSERS = _RumRollupQuerySpec(
    "value, SUM(event_count) AS count", dimension_filter="browser", group_by="value"
).build_sql()

RUM_ROLLUP_OS = _RumRollupQuerySpec(
    "value, SUM(event_count) AS count", dimension_filter="os", group_by="value"
).build_sql()

RUM_ROLLUP_DEVICES = _RumRollupQuerySpec(
    "value, SUM(event_count) AS count", dimension_filter="device", group_by="value"
).build_sql()

RUM_ROLLUP_WORST_PAGES = """
WITH page_metrics AS (
    SELECT
        value AS path,
        COALESCE(SUM(event_count) FILTER (WHERE dimension = 'pathname'), 0) AS views,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'path_metric_load') > 0 THEN SUM(value_sum) FILTER (WHERE dimension = 'path_metric_load') / SUM(event_count) FILTER (WHERE dimension = 'path_metric_load') ELSE 0.0 END AS avg_load_time,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'path_metric_lcp') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'path_metric_lcp') / SUM(event_count) FILTER (WHERE dimension = 'path_metric_lcp') ELSE 0.0 END AS lcp,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'path_metric_cls') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'path_metric_cls') / SUM(event_count) FILTER (WHERE dimension = 'path_metric_cls') ELSE 0.0 END AS cls
    FROM {vitals_agg_table}
    WHERE service_id = ?
      AND dimension IN ('pathname', 'path_metric_load', 'path_metric_lcp', 'path_metric_cls')
      AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY value
),
page_errors AS (
    SELECT value AS path, SUM(error_count) AS error_count
    FROM {error_agg_table}
    WHERE service_id = ? AND dimension = 'pathname' AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY value
)
SELECT
    m.path, m.views, m.avg_load_time, m.lcp, m.cls,
    COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
FROM page_metrics m
LEFT JOIN page_errors e ON m.path = e.path
ORDER BY error_rate DESC, m.avg_load_time DESC
LIMIT 5
"""

RUM_ROLLUP_TOP_EXCEPTIONS = _RumRollupQuerySpec(
    "value AS serialized, SUM(error_count) AS count",
    table_placeholder="{error_agg_table}",
    dimension_filter="exception",
    group_by="value",
    order_by="count DESC",
    limit=3,
).build_sql()

RUM_ROLLUP_TRENDS = """
WITH hourly_vitals AS (
    SELECT
        bucket_start AS hour,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'LCP') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'LCP') / SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'LCP') ELSE 0.0 END AS lcp,
        CASE WHEN SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'CLS') > 0 THEN SUM(p75_value * event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'CLS') / SUM(event_count) FILTER (WHERE dimension = 'metric_name' AND value = 'CLS') ELSE 0.0 END AS cls,
        SUM(event_count) FILTER (WHERE dimension = 'pathname') AS views,
        COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'pageviews'), 0) AS pageviews,
        COALESCE(SUM(event_count) FILTER (WHERE dimension = 'total' AND value = 'interactions'), 0) AS interactions
    FROM {vitals_agg_table}
    WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY hour
),
hourly_errors AS (
    SELECT bucket_start AS hour, SUM(error_count) FILTER (WHERE dimension = 'total' AND value = 'errors') AS errors
    FROM {error_agg_table}
    WHERE service_id = ? AND bucket_start >= CAST(? AS TIMESTAMPTZ) AND bucket_start <= CAST(? AS TIMESTAMPTZ)
    GROUP BY hour
)
SELECT
    COALESCE(v.hour, e.hour) AS hour_ts, v.lcp, v.cls,
    COALESCE(v.pageviews, 0) AS pageviews,
    COALESCE(v.interactions, 0) AS interactions,
    COALESCE(e.errors, 0) AS errors,
    COALESCE(e.errors, 0) * 100.0 / NULLIF(COALESCE(v.views, 0) + COALESCE(e.errors, 0), 0) AS error_rate
FROM hourly_vitals v
FULL OUTER JOIN hourly_errors e ON v.hour = e.hour
ORDER BY hour_ts DESC
"""

RUM_CONSOLIDATED_COUNTS = """
SELECT
    COUNT(DISTINCT CASE WHEN src = 'vitals' AND metric_name NOT LIKE 'event_%' THEN distinct_id END) AS pageviews,
    COUNT(DISTINCT CASE WHEN src = 'vitals' AND metric_name LIKE 'event_%' THEN distinct_id END) AS interactions,
    COUNT(DISTINCT CASE WHEN src = 'errors' THEN distinct_id END) AS errors_count,
    COUNT(DISTINCT distinct_id) AS total_beacons
FROM (
    SELECT 'vitals' AS src, metric_name, {distinct_id} AS distinct_id FROM t_client_vitals
    UNION ALL
    SELECT 'errors' AS src, CAST(NULL AS VARCHAR) AS metric_name, {distinct_id} AS distinct_id FROM t_client_errors
)
"""  # str.format template: {where_sql}, {distinct_id}

RUM_VITALS_SUMMARY = """
SELECT
    metric_name,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) AS p75,
    COUNT(*) AS total_count,
    COUNT(*) FILTER (WHERE metric_rating = 'good') AS good_count,
    COUNT(*) FILTER (WHERE metric_rating = 'needs_improvement') AS ni_count,
    COUNT(*) FILTER (WHERE metric_rating = 'poor') AS poor_count
FROM t_client_vitals
GROUP BY metric_name
"""

RUM_ENVIRONMENTS_CONSOLIDATED = """
SELECT
    COALESCE(browser, 'Unknown') AS browser,
    COALESCE(os, 'Unknown') AS os,
    COALESCE(device, 'Unknown') AS device,
    COUNT(DISTINCT {distinct_id}) AS count,
    grouping(browser) AS g_browser,
    grouping(os) AS g_os,
    grouping(device) AS g_device
FROM t_client_vitals
GROUP BY GROUPING SETS ((browser), (os), (device))
"""  # str.format template: {where_sql}, {distinct_id}

RUM_WORST_PAGES = """
WITH page_metrics AS (
    SELECT
        pathname AS path,
        COUNT(DISTINCT {distinct_id}) AS views,
        COALESCE(AVG(metric_value) FILTER (WHERE metric_name IN ('duration', 'pageLoadTime', 'load_time', 'pageLoad', 'LCP', 'lcp', 'ttfb', 'TTFB', 'fcp', 'FCP')), 0.0) AS avg_load_time,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('LCP', 'lcp')) AS lcp,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('CLS', 'cls')) AS cls
    FROM t_client_vitals
    WHERE pathname IS NOT NULL
    GROUP BY pathname
),
page_errors AS (
    SELECT pathname AS path, COUNT(DISTINCT {distinct_id}) AS error_count
    FROM t_client_errors
    WHERE pathname IS NOT NULL
    GROUP BY pathname
)
SELECT
    m.path, m.views, m.avg_load_time, m.lcp, m.cls,
    COALESCE(e.error_count, 0) * 100.0 / NULLIF(m.views, 0) AS error_rate
FROM page_metrics m
LEFT JOIN page_errors e ON m.path = e.path
ORDER BY error_rate DESC, m.avg_load_time DESC
LIMIT 5
"""  # str.format template: {where_sql}, {distinct_id}

RUM_TOP_EXCEPTIONS = """
SELECT error_message, error_file, error_line, error_col, COUNT(*) AS count
FROM t_client_errors
GROUP BY error_message, error_file, error_line, error_col
ORDER BY count DESC
LIMIT 3
"""

RUM_HOURLY_TRENDS = """
WITH hourly_vitals AS (
    SELECT
        DATE_TRUNC('hour', timestamp) AS hour,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('LCP', 'lcp')) AS lcp,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY metric_value) FILTER (WHERE metric_name IN ('CLS', 'cls')) AS cls,
        COUNT(DISTINCT {distinct_id}) AS views,
        COUNT(DISTINCT {distinct_id}) FILTER (WHERE metric_name NOT LIKE 'event_%') AS pageviews,
        COUNT(DISTINCT {distinct_id}) FILTER (WHERE metric_name LIKE 'event_%') AS interactions
    FROM t_client_vitals
    GROUP BY hour
),
hourly_errors AS (
    SELECT DATE_TRUNC('hour', timestamp) AS hour, COUNT(DISTINCT {distinct_id}) AS errors
    FROM t_client_errors
    GROUP BY hour
)
SELECT
    COALESCE(v.hour, e.hour) AS hour_ts, v.lcp, v.cls,
    COALESCE(v.pageviews, 0) AS pageviews,
    COALESCE(v.interactions, 0) AS interactions,
    COALESCE(e.errors, 0) AS errors,
    COALESCE(e.errors, 0) * 100.0 / NULLIF(COALESCE(v.views, 0) + COALESCE(e.errors, 0), 0) AS error_rate
FROM hourly_vitals v
FULL OUTER JOIN hourly_errors e ON v.hour = e.hour
ORDER BY hour_ts DESC
"""  # str.format template: {where_sql}, {distinct_id}

RUM_ANY_DATA = """
SELECT CASE WHEN EXISTS (SELECT 1 FROM client_vitals) OR EXISTS (SELECT 1 FROM client_errors) THEN 1 ELSE 0 END
"""

RUM_LIVE_EVENTS = """
WITH vitals_recent AS (
    SELECT timestamp, pathname, metric_name, metric_value, metric_rating, browser, os, device, cid, req_id, city, region, country, pop, tls, ttfb
    FROM client_vitals
    WHERE {where_sql}
    ORDER BY timestamp DESC
    LIMIT 250
),
vitals_base AS (
    SELECT 'pageview' AS type, timestamp, pathname, metric_name, metric_value, metric_rating, browser, os, device, cid, req_id, CAST(NULL AS VARCHAR) AS error_message, city, region, country, pop, tls, ttfb
    FROM vitals_recent
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, metric_name, pathname, cid, req_id ORDER BY timestamp) = 1
),
errors_recent AS (
    SELECT timestamp, pathname, browser, os, device, cid, req_id, error_message, error_file, error_line, city, region, country, pop, tls, ttfb
    FROM client_errors
    WHERE {where_sql}
    ORDER BY timestamp DESC
    LIMIT 250
),
errors_base AS (
    SELECT 'error' AS type, timestamp, pathname, CAST(NULL AS VARCHAR) AS metric_name, CAST(NULL AS DOUBLE) AS metric_value, CAST(NULL AS VARCHAR) AS metric_rating, browser, os, device, cid, req_id, error_message, city, region, country, pop, tls, ttfb
    FROM errors_recent
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp, error_message, error_file, error_line, pathname, cid, req_id ORDER BY timestamp) = 1
),
combined AS (
    SELECT * FROM vitals_base
    UNION ALL
    SELECT * FROM errors_base
)
SELECT type, timestamp, pathname, metric_name, metric_value, metric_rating, browser, os, device, cid, req_id, error_message, city, region, country, pop, tls, ttfb
FROM combined
ORDER BY timestamp DESC
LIMIT 50
"""  # str.format template: {where_sql}

HEADER_STATS = (
    f"SELECT COUNT(DISTINCT {DISTINCT_ID}), MAX(timestamp) FROM "
    "(SELECT req_id, cid, timestamp FROM client_vitals UNION ALL SELECT req_id, cid, timestamp FROM client_errors)"
)

AGGREGATE_BUCKET_COUNT = "SELECT COUNT(DISTINCT bucket_start) FROM rum_vitals_aggregates WHERE service_id = ?"

CLIENT_VITALS_SCHEMA_PROBE = "SELECT * FROM client_vitals LIMIT 0"

_READ_PARQUET_LIST_RE = re.compile(r"read_parquet\(\s*(?:\[|(?:main\.)?list_value\()(.*?)(?:\]|\))", re.DOTALL)
_QUOTED_PATH_RE = re.compile(r"'([^']+)'")


def _prune_missing_files_from_rum_views(con) -> None:
    """Fallback view self-heal when ``source`` was not passed to a repository helper."""
    try:
        rows = con.execute(
            "SELECT view_name, sql, temporary FROM duckdb_views() WHERE view_name IN ('client_vitals', 'client_errors')"
        ).fetchall()
    except Exception:
        return
    for view_name, view_sql, is_temp in rows:
        if not view_sql or "read_parquet(" not in view_sql:
            continue

        def _replace_list(match: re.Match[str]) -> str:
            inner = match.group(1)
            paths = _QUOTED_PATH_RE.findall(inner)
            surviving = [p for p in paths if os.path.exists(p)]
            if not surviving and paths:
                buf_dir = os.path.dirname(paths[0])
                if os.path.isdir(buf_dir):
                    surviving = sorted(
                        os.path.join(buf_dir, f)
                        for f in os.listdir(buf_dir)
                        if f.startswith("batch_") and f.endswith(".parquet")
                    )
            if not surviving:
                return match.group(0)
            joined = ", ".join(f"'{p}'" for p in surviving)
            return f"read_parquet([{joined}]"

        new_sql = _READ_PARQUET_LIST_RE.sub(_replace_list, view_sql)
        if new_sql != view_sql:
            stmt = new_sql.strip().rstrip(";")
            if stmt.upper().startswith("CREATE VIEW "):
                prefix = "CREATE OR REPLACE TEMP VIEW " if is_temp else "CREATE OR REPLACE VIEW "
                stmt = prefix + stmt[len("CREATE VIEW ") :]
            elif stmt.upper().startswith("CREATE TEMP VIEW "):
                stmt = "CREATE OR REPLACE TEMP VIEW " + stmt[len("CREATE TEMP VIEW ") :]
            try:
                con.execute(stmt)
            except Exception:
                logger.debug("_prune_missing_files_from_rum_views: failed to rebind %s", view_name, exc_info=True)


def _run_with_rum_view_retry(con, source: dict | None, fn):
    """Run ``fn()`` with bounded self-heal retry on stale RUM buffer file errors."""

    def _recover(_exc: BaseException) -> None:
        if source is not None:
            from backend.core.storage import view as storage_view

            storage_view.clear_source_caches(source.get("name", "default"), keep_snapshot_cache=True)
            storage_view.update_storage_view(con, source, force=True)
        _prune_missing_files_from_rum_views(con)

    return retry_call(fn, retry_on=is_stale_view_error, attempts=3, before_retry=_recover)


def get_beacon_health(con, *, source: dict | None = None) -> tuple[int, datetime | None]:
    """``(beacons in the last hour, newest beacon timestamp)`` across vitals + errors."""

    def _run() -> tuple[int, datetime | None]:
        one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
        with track_query(con, sql_rum.BEACON_HEALTH_RECENT, [one_hour_ago], "rum_health_recent") as cur_recent:
            recent_count = cur_recent.fetchone()[0] or 0
        with track_query(con, sql_rum.BEACON_HEALTH_LAST, [], "rum_health_last") as cur_last:
            last_dt = cur_last.fetchone()[0]
        return recent_count, last_dt

    return _run_with_rum_view_retry(con, source, _run)


def fetch_rum_analytics(
    con,
    *,
    service_id: str,
    start_time: str,
    end_time: str,
    parsed_filters: dict,
    where_sql: str,
    params: list,
    raw_window_max_s: int,
    source: dict | None = None,
) -> dict[str, Any]:
    """Raw aggregate rows for ``/rum/analytics`` (rollup tables when eligible, else raw scan)."""
    return _run_with_rum_view_retry(
        con,
        source,
        lambda: _fetch_rum_analytics_once(
            con,
            service_id=service_id,
            start_time=start_time,
            end_time=end_time,
            parsed_filters=parsed_filters,
            where_sql=where_sql,
            params=params,
            raw_window_max_s=raw_window_max_s,
        ),
    )


def _fetch_rum_analytics_once(
    con,
    *,
    service_id: str,
    start_time: str,
    end_time: str,
    parsed_filters: dict,
    where_sql: str,
    params: list,
    raw_window_max_s: int,
) -> dict[str, Any]:
    from backend.core.rollups.rum import table_exists

    use_rollup = False
    prefer_raw_window = False
    try:
        st = parse_iso_utc(start_time)
        et = parse_iso_utc(end_time)
        if st and et and (et - st).total_seconds() <= raw_window_max_s:
            prefer_raw_window = True
    except Exception:
        logger.debug("fetch_rum_analytics: step failed (ignored)", exc_info=True)

    if not parsed_filters and not prefer_raw_window:
        if table_exists(con, "rum_vitals_aggregates") and table_exists(con, "rum_error_aggregates"):
            use_rollup = True

    if use_rollup:
        vitals_agg_table = "rum_vitals_aggregates"
        error_agg_table = "rum_error_aggregates"
        try:
            with track_query(
                con, sql_rum.RUM_ROLLUP_MAX_BUCKET, [service_id, service_id], "rum_rollup_max_bucket"
            ) as cur_max:
                row_max = cur_max.fetchone()
                rollup_max_bucket = row_max[0] if row_max else None

            if rollup_max_bucket is None:
                use_rollup = False
            elif table_exists(con, "client_vitals") and table_exists(con, "client_errors"):
                with track_query(
                    con,
                    sql_rum.RUM_TAIL_VITALS_CREATE,
                    [rollup_max_bucket, start_time, end_time],
                    "rum_tail_vitals_create",
                ):
                    pass
                with track_query(
                    con,
                    sql_rum.RUM_TAIL_ERRORS_CREATE,
                    [rollup_max_bucket, start_time, end_time],
                    "rum_tail_errors_create",
                ):
                    pass
                with track_query(con, sql_rum.RUM_TAIL_HAS_ROWS, [], "rum_tail_has_rows") as cur_tail:
                    tail_rows = (cur_tail.fetchone() or (0,))[0] or 0

                if tail_rows > 0:
                    with track_query(
                        con,
                        sql_rum.RUM_EFF_VITALS_AGG_CREATE.format(distinct_id=sql_rum.DISTINCT_ID),
                        [service_id, start_time, end_time] + [service_id] * 10,
                        "rum_eff_vitals_agg_create",
                    ):
                        pass
                    with track_query(
                        con,
                        sql_rum.RUM_EFF_ERROR_AGG_CREATE.format(distinct_id=sql_rum.DISTINCT_ID),
                        [service_id, start_time, end_time] + [service_id] * 3,
                        "rum_eff_error_agg_create",
                    ):
                        pass
                    vitals_agg_table = "t_eff_vitals_agg"
                    error_agg_table = "t_eff_error_agg"

            if use_rollup:
                logger.info("[rum_rollups] %s: Querying from precomputed RUM aggregates...", service_id)
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_ANY_DATA.format(
                        vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                    ),
                    [service_id, start_time, end_time, service_id, start_time, end_time],
                    "rum_rollup_any_data",
                ) as cur_any:
                    if cur_any.fetchone()[0] == 0:
                        if not table_exists(con, "client_vitals") or not table_exists(con, "client_errors"):
                            return {"no_data": True}
                        with track_query(con, sql_rum.RUM_ANY_DATA, [], "rum_any_data") as cur_raw_any:
                            if not cur_raw_any.fetchone()[0]:
                                return {"no_data": True}
                        use_rollup = False

            if use_rollup:
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_VITALS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_vitals",
                ) as cur_v:
                    pageviews, interactions = cur_v.fetchone()
                    pageviews = pageviews or 0
                    interactions = interactions or 0

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_ERRORS.format(error_agg_table=error_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_errors",
                ) as cur_e:
                    errors_count = cur_e.fetchone()[0] or 0

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_COUNTS_TOTAL_BEACONS.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_counts_total_beacons",
                ) as cur_t:
                    total_beacons = cur_t.fetchone()[0] or 0

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_VITALS_SUMMARY.format(vitals_agg_table=vitals_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_vitals_summary",
                ) as cur_vitals:
                    vitals_rows = cur_vitals.fetchall()

                browsers: dict[str, Any] = {}
                os_dict: dict[str, Any] = {}
                devices: dict[str, Any] = {}
                for target_dict, sql_tpl, label in (
                    (browsers, sql_rum.RUM_ROLLUP_BROWSERS, "rum_rollup_browsers"),
                    (os_dict, sql_rum.RUM_ROLLUP_OS, "rum_rollup_os"),
                    (devices, sql_rum.RUM_ROLLUP_DEVICES, "rum_rollup_devices"),
                ):
                    with track_query(
                        con,
                        sql_tpl.format(vitals_agg_table=vitals_agg_table),
                        [service_id, start_time, end_time],
                        label,
                    ) as cur_dim:
                        for val, count in cur_dim.fetchall():
                            target_dict[val] = count

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_WORST_PAGES.format(
                        vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                    ),
                    [service_id, start_time, end_time, service_id, start_time, end_time],
                    "rum_rollup_worst_pages",
                ) as cur_pages:
                    worst_pages_rows = cur_pages.fetchall()

                errors_rows = []
                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_TOP_EXCEPTIONS.format(error_agg_table=error_agg_table),
                    [service_id, start_time, end_time],
                    "rum_rollup_top_exceptions",
                ) as cur_errors:
                    for row in cur_errors.fetchall():
                        serialized, count = row
                        parts = serialized.split("|")
                        if len(parts) >= 4:
                            msg, file, line, col = parts[0], parts[1], int(parts[2] or 0), int(parts[3] or 0)
                            errors_rows.append((msg, file, line, col, count))
                        else:
                            errors_rows.append((serialized, "Unknown", 0, 0, count))

                with track_query(
                    con,
                    sql_rum.RUM_ROLLUP_TRENDS.format(
                        vitals_agg_table=vitals_agg_table, error_agg_table=error_agg_table
                    ),
                    [service_id, start_time, end_time, service_id, start_time, end_time],
                    "rum_rollup_trends",
                ) as cur_trends:
                    trends_rows = cur_trends.fetchall()

                return {
                    "no_data": False,
                    "total_beacons": total_beacons,
                    "pageviews": pageviews,
                    "interactions": interactions,
                    "errors_count": errors_count,
                    "vitals_rows": vitals_rows,
                    "browsers": browsers,
                    "os": os_dict,
                    "devices": devices,
                    "worst_pages_rows": worst_pages_rows,
                    "errors_rows": errors_rows,
                    "trends_rows": trends_rows,
                    "_approx": True,
                }
        finally:
            con.execute("DROP TABLE IF EXISTS t_eff_vitals_agg")
            con.execute("DROP TABLE IF EXISTS t_eff_error_agg")
            con.execute("DROP TABLE IF EXISTS t_tail_vitals")
            con.execute("DROP TABLE IF EXISTS t_tail_errors")

    try:
        with track_query(
            con, sql_rum.RUM_TEMP_VITALS_CREATE.format(where_sql=where_sql), params, "rum_temp_vitals_create"
        ):
            pass
        with track_query(
            con, sql_rum.RUM_TEMP_ERRORS_CREATE.format(where_sql=where_sql), params, "rum_temp_errors_create"
        ):
            pass

        with track_query(
            con,
            sql_rum.RUM_CONSOLIDATED_COUNTS.format(distinct_id=sql_rum.DISTINCT_ID),
            [],
            "rum_consolidated_counts",
        ) as cur_counts:
            pageviews, interactions, errors_count, total_beacons = cur_counts.fetchone()
            pageviews = pageviews or 0
            interactions = interactions or 0
            errors_count = errors_count or 0
            total_beacons = total_beacons or 0

        if total_beacons == 0:
            with track_query(con, sql_rum.RUM_ANY_DATA, [], "rum_any_data") as cur_any:
                if not cur_any.fetchone()[0]:
                    return {"no_data": True}

        with track_query(con, sql_rum.RUM_VITALS_SUMMARY, [], "rum_vitals_summary") as cur_vitals:
            vitals_rows = cur_vitals.fetchall()

        browsers = {}
        os_dict = {}
        devices = {}

        with track_query(
            con,
            sql_rum.RUM_ENVIRONMENTS_CONSOLIDATED.format(distinct_id=sql_rum.DISTINCT_ID),
            [],
            "rum_environments_consolidated",
        ) as cur_envs:
            for r_browser, r_os, r_device, count, g_browser, g_os, g_device in cur_envs.fetchall():
                if g_browser == 0 and r_browser:
                    browsers[r_browser] = count
                elif g_os == 0 and r_os:
                    os_dict[r_os] = count
                elif g_device == 0 and r_device:
                    devices[r_device] = count

        with track_query(
            con, sql_rum.RUM_WORST_PAGES.format(distinct_id=sql_rum.DISTINCT_ID), [], "rum_worst_pages"
        ) as cur_pages:
            worst_pages_rows = cur_pages.fetchall()

        with track_query(con, sql_rum.RUM_TOP_EXCEPTIONS, [], "rum_top_exceptions") as cur_errors:
            errors_rows = cur_errors.fetchall()

        with track_query(
            con, sql_rum.RUM_HOURLY_TRENDS.format(distinct_id=sql_rum.DISTINCT_ID), [], "rum_hourly_trends"
        ) as cur_trends:
            trends_rows = cur_trends.fetchall()

        return {
            "no_data": False,
            "total_beacons": total_beacons,
            "pageviews": pageviews,
            "interactions": interactions,
            "errors_count": errors_count,
            "vitals_rows": vitals_rows,
            "browsers": browsers,
            "os": os_dict,
            "devices": devices,
            "worst_pages_rows": worst_pages_rows,
            "errors_rows": errors_rows,
            "trends_rows": trends_rows,
        }
    finally:
        con.execute("DROP TABLE IF EXISTS t_client_vitals")
        con.execute("DROP TABLE IF EXISTS t_client_errors")


def fetch_rum_live_events(con, *, where_sql: str, params: list, source: dict | None = None) -> list[tuple]:
    """Newest 50 vitals + error beacons for the live ticker, newest first."""

    def _run() -> list[tuple]:
        with track_query(
            con, sql_rum.RUM_LIVE_EVENTS.format(where_sql=where_sql), params * 2, "rum_live_events"
        ) as cur:
            return cur.fetchall()

    return _run_with_rum_view_retry(con, source, _run)


def get_header_stats(con, *, source: dict | None = None) -> tuple[int, Any]:
    """``(distinct beacon count, newest beacon timestamp)`` over vitals + errors for the header badge (one scan)."""

    def _run() -> tuple[int, Any]:
        row = con.execute(sql_rum.HEADER_STATS).fetchone()
        return (row[0] or 0, row[1]) if row else (0, None)

    return _run_with_rum_view_retry(con, source, _run)


def count_aggregate_buckets(con, service_id: str) -> int | None:
    """Distinct ``bucket_start`` values in ``rum_vitals_aggregates`` for the service."""
    row = con.execute(sql_rum.AGGREGATE_BUCKET_COUNT, [service_id]).fetchone()
    return row[0] if row is not None else None


def get_client_vitals_columns(con) -> list[str] | None:
    """Return column names of ``client_vitals`` (or ``None`` if unavailable)."""
    try:
        desc = con.execute(sql_rum.CLIENT_VITALS_SCHEMA_PROBE).description
        if desc:
            return [d[0] for d in desc]
    except Exception:
        logger.debug("client_vitals schema probe failed", exc_info=True)
    return None


for _alias in (
    "backend.repositories.performance",
    "backend.repositories.cmcd",
    "backend.repositories.rum",
    "backend.repositories._sql.performance",
    "backend.repositories._sql.cmcd",
    "backend.repositories._sql.rum",
):
    _sys.modules[_alias] = _sys.modules[__name__]
