"""Origin metrics repository — fetch timing, error rates, IP health."""

from __future__ import annotations

import logging
import math
import sys as _sys
import time
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
    endpoint: str,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    **extra,
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
    """Build cache key for ``endpoint`` and return ``(key, cached_response)``."""
    cache_key = _origin_response_cache_key(endpoint, src, start_time, end_time, filters, **extras)
    return cache_key, _memo.check(cache_key, runner)


def _store_response_cache(cache_key: str, payload: dict, runner: QueryRunner) -> dict:
    """Cache ``payload`` under ``cache_key`` and return it with telemetry."""
    return _memo.store(cache_key, payload, runner)


# ── POP location helpers ──────────────────────────────────────────────────────

SHIELDING_ANOMALY_MIN_REQUESTS = 30


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometers between two (lat, lon) points."""
    r_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2.0 * r_km * math.asin(math.sqrt(a))


def _enrich_with_distance(row: dict) -> dict:
    """Add distance_km, light_speed_rtt_ms, efficiency_ratio, and coordinate fields."""
    from backend.utils.pop_utils import get_pop_lat_lon_map

    pops = get_pop_lat_lon_map()
    e_pop = str(row.get("edge_pop", "")).upper()
    s_pop = str(row.get("shield_pop", "")).upper()
    e_coords = pops.get(e_pop)
    s_coords = pops.get(s_pop)
    low_sample = (row.get("requests") or 0) < SHIELDING_ANOMALY_MIN_REQUESTS
    if e_coords and s_coords:
        dist = _haversine_km(e_coords[0], e_coords[1], s_coords[0], s_coords[1])
        light_rtt_ms = round(2.0 * dist / 200_000.0 * 1000.0, 2)
        p50 = row.get("p50_ms")
        efficiency = round(p50 / light_rtt_ms, 2) if (light_rtt_ms > 0.5 and p50 is not None) else None
        anomaly_eligible = (
            efficiency is not None and efficiency > 3.0 and p50 is not None and p50 - light_rtt_ms >= 20.0
        )
        row.update(
            distance_km=round(dist, 1),
            light_speed_rtt_ms=light_rtt_ms,
            efficiency_ratio=efficiency,
            low_sample=low_sample,
            anomaly_eligible=anomaly_eligible,
            anomaly_static=anomaly_eligible and not low_sample,
            edge_lat=e_coords[0],
            edge_lon=e_coords[1],
            shield_lat=s_coords[0],
            shield_lon=s_coords[1],
        )
    else:
        row.update(
            distance_km=None,
            light_speed_rtt_ms=None,
            efficiency_ratio=None,
            low_sample=low_sample,
            anomaly_eligible=False,
            anomaly_static=False,
            edge_lat=e_coords[0] if e_coords else None,
            edge_lon=e_coords[1] if e_coords else None,
            shield_lat=s_coords[0] if s_coords else None,
            shield_lon=s_coords[1] if s_coords else None,
        )
    return row


def _shape_summary(
    runner: QueryRunner,
    table: str,
    where: str,
    params: list,
    lat_val: str,
    actual_cols: set[str] | list[str],
) -> dict:
    """Render SUMMARY_ROLLUP against ``table`` and return the summary payload dict."""
    actual_cols_set = set(actual_cols)
    ost_5xx = (
        'COUNT(*) FILTER (WHERE "ost" >= 500 AND "ost" < 600) * 1.0 / '
        'NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)'
        if "ost" in actual_cols_set
        else "NULL"
    )
    ottlb_p50 = 'MEDIAN("ottlb") / 1000.0' if "ottlb" in actual_cols_set else "NULL"
    ottlb_p95 = 'APPROX_QUANTILE("ottlb", 0.95) / 1000.0' if "ottlb" in actual_cols_set else "NULL"
    cdn_ovh = (
        'MEDIAN("elapsed" - "ottlb") / 1000.0'
        if "elapsed" in actual_cols_set and "ottlb" in actual_cols_set
        else "NULL"
    )
    obytes_p50 = 'MEDIAN("obytes")' if "obytes" in actual_cols_set else "NULL"
    total_misses = "COUNT(*) FILTER (WHERE starts_with(\"cache\", 'MISS'))" if "cache" in actual_cols_set else "NULL"
    total_passes = "COUNT(*) FILTER (WHERE starts_with(\"cache\", 'PASS'))" if "cache" in actual_cols_set else "NULL"

    cur = runner.execute(
        SQL.SUMMARY_ROLLUP.format(
            lat_val=lat_val,
            total_misses=total_misses,
            total_passes=total_passes,
            ottlb_p50=ottlb_p50,
            ottlb_p95=ottlb_p95,
            cdn_ovh=cdn_ovh,
            ost_5xx=ost_5xx,
            obytes_p50=obytes_p50,
            table=table,
            where=where,
        ),
        params,
    )
    cols = [d[0] for d in cur.description]
    rollup_row = next((dict(zip(cols, r, strict=False)) for r in cur.fetchall()), None)

    _summary_keys = (
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
    has_data = rollup_row is not None and rollup_row["ottfb_p50_ms"] is not None
    if not has_data or rollup_row is None:
        return {"has_data": False, **{k: None for k in _summary_keys}}
    return {"has_data": True, **{k: rollup_row[k] for k in _summary_keys}}


def get_summary(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("summary", src, start_time, end_time, filters, runner)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()

    if not actual_cols:
        return empty_schema_response(
            has_data=False,
            total_misses=None,
            total_passes=None,
            ottfb_p50_ms=None,
            ottfb_p75_ms=None,
            ottfb_p95_ms=None,
            ottfb_p99_ms=None,
            **runner.telemetry(),
        )

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    # Unified latency expression: prefer ottfb (micros), fallback to ttfb (seconds).
    lat_val = origin_latency_us_expr(actual_cols)
    timer = SectionTimer()
    started = time.perf_counter()
    payload: dict[str, Any] = _shape_summary(runner, table_name, where, params, lat_val, actual_cols)
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

    rows = runner.execute(
        SQL.TIMESERIES_BUCKETED.format(
            interval=interval,
            agg_expr=agg_expr,
            unit_conv=unit_conv,
            edge_col=edge_col,
            table=table,
            where=where,
            lat_expr=lat_expr,
            edge_group=edge_col,
        ),
        params,
    ).fetchall()

    series = [
        {
            "time": safe_iso(r[0]),
            "miss_count": r[1],
            "value": r[2],
            **({"edge": r[3]} if has_edge_col else {}),
        }
        for r in rows
    ]
    return {"has_data": len(series) > 0, "series": series}


def _shape_slow_url_rows(rows: list[tuple]) -> dict:
    return {
        "has_data": len(rows) > 0,
        "rows": [{"url": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "p99_ms": r[4]} for r in rows],
    }


def _shape_status_code_rows(rows: list[tuple]) -> dict:
    if not rows:
        return {"has_data": False, "rows": []}
    return {
        "has_data": True,
        "rows": [{"status": r[0], "count": r[1], "pct": r[2]} for r in rows],
    }


def _shape_path_breakdown_rows(rows: list[tuple]) -> dict:
    if not rows:
        return {"has_data": False, "shielding_detected": False, "rows": []}
    return {
        "has_data": True,
        "shielding_detected": any(r[0] is False for r in rows),
        "rows": [{"edge": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3]} for r in rows],
    }


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
    if not rows:
        return {"has_data": False, "rows": []}
    return {
        "has_data": True,
        "rows": [{"oip": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "error_pct": r[4]} for r in rows],
    }


def _resolve_origin_ts_expr(
    metric: str,
    actual_cols: set[str] | list[str],
    *,
    use_temp_lat_us: bool = False,
) -> tuple[str, str] | None:
    metric_col = "ottfb" if metric == "ttfb" else "ottlb"
    unit_conv = "/ 1000.0"
    if metric_col not in actual_cols:
        if metric == "ttfb" and "ttfb" in actual_cols:
            metric_col = "ttfb"
            unit_conv = "* 1000.0"
        else:
            return None
    if metric == "ttfb" and "ottfb" in actual_cols and "ttfb" in actual_cols:
        lat_expr = '"lat_us"' if use_temp_lat_us else (metric_expressions.origin_latency_us(actual_cols) or "NULL")
        unit_conv = "/ 1000.0"
    else:
        lat_expr = f'"{metric_col}"'
    return lat_expr, unit_conv


def _run_origin_card(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    endpoint: str,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    *,
    required_cols: tuple[str, ...] = (),
    empty_extra: dict[str, Any] | None = None,
    sql_template: str,
    use_lat_val: bool = True,
    extra_params: list[Any] | None = None,
    shaper: Any,
    **cache_extra: Any,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(endpoint, src, start_time, end_time, filters, runner, **cache_extra)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or any(c not in actual_cols for c in required_cols):
        return empty_schema_response(has_data=False, **(empty_extra or {}), rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    fmt_kwargs: dict[str, Any] = {"table": table_name, "where": where}
    if use_lat_val:
        fmt_kwargs["lat_val"] = origin_latency_us_expr(actual_cols)

    timer = SectionTimer()
    started = time.perf_counter()
    rows = runner.execute(
        sql_template.format(**fmt_kwargs),
        params + (extra_params or []),
    ).fetchall()
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
        sql_template=SQL.SLOW_URLS,
        extra_params=[min_requests, limit],
        shaper=_shape_slow_url_rows,
        limit=limit,
        min_requests=min_requests,
    )


def get_status_codes(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    return _run_origin_card(
        con,
        src,
        "status_codes",
        start_time,
        end_time,
        filters,
        required_cols=("ost",),
        sql_template=SQL.STATUS_CODES,
        use_lat_val=False,
        shaper=_shape_status_code_rows,
    )


def get_path_breakdown(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    return _run_origin_card(
        con,
        src,
        "path_breakdown",
        start_time,
        end_time,
        filters,
        required_cols=("edge",),
        empty_extra={"shielding_detected": False},
        sql_template=SQL.PATH_BREAKDOWN,
        shaper=_shape_path_breakdown_rows,
    )


def get_pop_latency(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    return _run_origin_card(
        con,
        src,
        "pop_latency",
        start_time,
        end_time,
        filters,
        required_cols=("pop",),
        empty_extra={"requires_group_c": True},
        sql_template=SQL.POP_LATENCY,
        extra_params=[limit],
        shaper=_shape_pop_latency_rows,
        limit=limit,
    )


def get_ip_health(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    return _run_origin_card(
        con,
        src,
        "ip_health",
        start_time,
        end_time,
        filters,
        required_cols=("oip", "ost"),
        sql_template=SQL.IP_HEALTH,
        extra_params=[limit],
        shaper=_shape_ip_health_rows,
        limit=limit,
    )


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
    if not actual_cols:
        return empty_schema_response(has_data=False, dimension=dimension, rows=[], **runner.telemetry())

    candidates = _DIM_CANDIDATES.get(dimension, [dimension])
    dim_col = next((c for c in candidates if c in actual_cols), None)
    if not dim_col:
        return empty_schema_response(has_data=False, dimension=dimension, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    lat_val = origin_latency_us_expr(actual_cols)

    if "ost" in actual_cols:
        error_expr = '"ost" >= 500'
    elif "status" in actual_cols:
        error_expr = '"status" >= 500'
    else:
        error_expr = "false"

    if "obytes" in actual_cols:
        bytes_expr = 'ROUND(MEDIAN("obytes"), 0)'
    elif "bytes" in actual_cols:
        bytes_expr = 'ROUND(MEDIAN("bytes"), 0)'
    else:
        bytes_expr = "NULL"

    timer = SectionTimer()
    started = time.perf_counter()
    rows = runner.execute(
        SQL.DRILLDOWN.format(
            dim_col=dim_col,
            lat_val=lat_val,
            error_expr=error_expr,
            bytes_expr=bytes_expr,
            table=table_name,
            where=where,
        ),
        params + [limit],
    ).fetchall()
    timer.mark("drilldown", started, source="live", activity="compute", current_execution=True, outcome="served")

    if not rows:
        payload = {"has_data": False, "dimension": dimension, "rows": [], "section_timings": timer.entries}
        return _store_response_cache(cache_key, payload, runner)

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
    payload = {
        "has_data": True,
        "dimension": dimension,
        "rows": rows_out,
        "section_timings": timer.entries,
    }
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

    required = {"rid", "prid", "edge", "pop", "ottfb", "ttfb"}
    missing = required - set(actual_cols)
    if missing:
        return _memo.result(
            empty_schema_response(has_data=False, requires_fields=list(missing), rows=[], **runner.telemetry()),
            state="miss",
        )

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)
    # Apply only time bounds to shield CTE so edge POP filters don't exclude shield hits.
    time_params, time_where = build_where_clause(start_time, end_time, {}, actual_cols)

    query = SQL.SHIELDING_ANALYSIS.format(
        table=table_name,
        where=where,
        time_where=time_where,
    )

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
        if shield_exists is None:
            payload = {"has_data": False, "edge_only": True, "rows": [], "section_timings": timer.entries}
        else:
            payload = {"has_data": False, "rows": [], "section_timings": timer.entries}
        return _store_response_cache(cache_key, payload, runner)

    total_routes = raw_rows[0].get("total_routes")
    if total_routes is None:
        total_routes = len(raw_rows)

    enriched_rows = [
        _enrich_with_distance(
            {
                "edge_pop": r["edge_pop"],
                "shield_pop": r["shield_pop"],
                "requests": r["requests"],
                "p50_ms": r["p50_ms"],
                "p95_ms": r["p95_ms"],
                "p99_ms": r["p99_ms"],
            }
        )
        for r in raw_rows
    ]

    payload = {
        "has_data": len(enriched_rows) > 0,
        "rows": enriched_rows,
        "total_routes": total_routes,
        "truncated": total_routes > len(enriched_rows),
        "section_timings": timer.entries,
    }
    return _store_response_cache(cache_key, payload, runner)


# ── Composite: get_aggregates ────────────────────────────────────────────────


def _origin_summary_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
) -> dict:
    """get_summary against the per-request TEMP TABLE using precomputed ``lat_us``."""
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
    resolved = _resolve_origin_ts_expr(metric, actual_cols_set, use_temp_lat_us=True)
    if resolved is None:
        return {"has_data": False, "series": []}
    lat_expr, unit_conv = resolved

    return _run_timeseries_query(
        runner,
        temp_table,
        "1=1",
        [],
        lat_expr,
        unit_conv,
        actual_cols=actual_cols_set,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        percentile=percentile,
    )


def _run_temp_origin_card(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    sql_template: str,
    shaper: Any,
    *,
    required_cols: tuple[str, ...] = (),
    empty_extra: dict[str, Any] | None = None,
    use_lat_val: bool = True,
    params: list[Any] | None = None,
) -> dict:
    cols = set(actual_cols)
    if any(c not in cols for c in required_cols):
        return {"has_data": False, **(empty_extra or {}), "rows": []}
    fmt_kwargs: dict[str, Any] = {"table": temp_table, "where": "1=1"}
    if use_lat_val:
        fmt_kwargs["lat_val"] = "lat_us"
    rows = runner.execute(sql_template.format(**fmt_kwargs), params).fetchall()
    return shaper(rows)


def _origin_slow_urls_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    min_requests: int,
    limit: int,
) -> dict:
    return _run_temp_origin_card(
        runner,
        temp_table,
        actual_cols,
        SQL.SLOW_URLS,
        _shape_slow_url_rows,
        required_cols=("url",),
        params=[min_requests, limit],
    )


def _origin_status_codes_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
) -> dict:
    return _run_temp_origin_card(
        runner,
        temp_table,
        actual_cols,
        SQL.STATUS_CODES,
        _shape_status_code_rows,
        required_cols=("ost",),
        use_lat_val=False,
    )


def _origin_path_breakdown_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
) -> dict:
    return _run_temp_origin_card(
        runner,
        temp_table,
        actual_cols,
        SQL.PATH_BREAKDOWN,
        _shape_path_breakdown_rows,
        required_cols=("edge",),
        empty_extra={"shielding_detected": False},
    )


def _origin_pop_latency_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    limit: int,
) -> dict:
    return _run_temp_origin_card(
        runner,
        temp_table,
        actual_cols,
        SQL.POP_LATENCY,
        _shape_pop_latency_rows,
        required_cols=("pop",),
        empty_extra={"requires_group_c": True},
        params=[limit],
    )


def _origin_ip_health_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    limit: int,
) -> dict:
    return _run_temp_origin_card(
        runner,
        temp_table,
        actual_cols,
        SQL.IP_HEALTH,
        _shape_ip_health_rows,
        required_cols=("oip", "ost"),
        params=[limit],
    )


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
    import time as _time
    import uuid as _uuid

    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

    def _want(name: str) -> bool:
        return sections is None or name in sections

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
    wanted_cols = [
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
    ]
    select_cols = [f'"{c}"' for c in wanted_cols if c in actual_set]
    if not select_cols:
        return {**empty_payload, **runner.telemetry()}
    lat_us_expr = origin_latency_us_expr(actual_set)
    temp_table = f"t_origin_{_uuid.uuid4().hex}"
    create_sql = SQL.AGGREGATES_CREATE_TEMP.format(
        temp_table=temp_table,
        select_cols=", ".join(select_cols),
        lat_us_expr=lat_us_expr,
        table=table_name,
        where_clause=where_clause,
    )
    timer = SectionTimer()
    section_timings = timer.entries

    merged: dict[str, Any] = {}
    missed: set[str] = set()

    def _hoist(name: str, reader) -> None:
        if not _want(name):
            return
        _ts = _time.perf_counter()
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

    _hoist(
        "summary",
        lambda: runner.try_origin_summary_from_rollup(
            start_time, end_time, has_filters=has_filters, actual_cols=actual_set
        ),
    )
    _hoist(
        "slow_urls",
        lambda: runner.try_slow_urls_from_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            min_requests=slow_urls_min_requests,
            limit=slow_urls_limit,
        ),
    )
    _hoist(
        "status_codes",
        lambda: runner.try_origin_status_from_rollup(start_time, end_time, has_filters=has_filters),
    )
    _hoist(
        "path_breakdown",
        lambda: runner.try_origin_path_breakdown_from_rollup(start_time, end_time, has_filters=has_filters),
    )
    _hoist(
        "pop_latency",
        lambda: runner.try_origin_pop_latency_from_rollup(
            start_time, end_time, has_filters=has_filters, limit=pop_latency_limit
        ),
    )
    _hoist(
        "ip_health",
        lambda: runner.try_origin_ip_health_from_rollup(
            start_time, end_time, has_filters=has_filters, limit=ip_health_limit
        ),
    )
    _hoist(
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
    )

    if not missed:
        section_timings.append(
            {"section": "origin:temp_skipped", "time_ms": 0.0, "activity": "skipped", "current_execution": True}
        )
        summary_card = merged.get("summary") or {}
        return {
            "has_data": summary_card.get("has_data", False),
            "section_timings": section_timings,
            **merged,
            **runner.telemetry(),
        }

    _t = _time.perf_counter()
    if not runner.create_temp_table(create_sql, params):
        return {**empty_payload, **runner.telemetry()}
    timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True, outcome="served")

    def _run_live(name: str, reader) -> None:
        if not (_want(name) and name in missed):
            return
        _ts = _time.perf_counter()
        merged[name] = reader()
        timer.mark(name, _ts, source="live", activity="compute", current_execution=True, outcome="served")

    try:
        _run_live("summary", lambda: _origin_summary_from_temp(runner, temp_table, actual_set))
        _run_live(
            "slow_urls",
            lambda: _origin_slow_urls_from_temp(
                runner, temp_table, actual_set, slow_urls_min_requests, slow_urls_limit
            ),
        )
        _run_live(
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
        )
        _run_live("status_codes", lambda: _origin_status_codes_from_temp(runner, temp_table, actual_set))
        _run_live("path_breakdown", lambda: _origin_path_breakdown_from_temp(runner, temp_table, actual_set))
        _run_live(
            "pop_latency",
            lambda: _origin_pop_latency_from_temp(runner, temp_table, actual_set, pop_latency_limit),
        )
        _run_live(
            "ip_health",
            lambda: _origin_ip_health_from_temp(runner, temp_table, actual_set, ip_health_limit),
        )

        summary_card = merged.get("summary") or {}
        return {
            "has_data": summary_card.get("has_data", False),
            "section_timings": section_timings,
            **merged,
            **runner.telemetry(),
        }
    finally:
        _drop_temp_table(runner, temp_table, "get_aggregates")


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("origin._response_cache", _response_cache)


# ── Folded from backend.repositories.performance ──────────────────────────
"""Performance repository — latency analysis and origin/edge breakdown."""

import logging
import os
import sys as _sys
import time as _time
from collections.abc import Callable

from backend.core.metric_expressions import coalesce
from backend.repositories._base import (
    percentile_ms_expr,
)
from backend.repositories.utils.response_cache import (
    ResponseMemo,
)
from backend.utils.bounded_cache import CacheRegistry

logger = logging.getLogger(__name__)

# ── SQL templates (folded from backend/repositories/_sql/performance.py) ────


def _top_latency_sql(col: str, min_reqs: int) -> str:
    return f"""
    SELECT {col},
           count(*) AS reqs,
           AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5, approx=True)} as p50,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95, approx=True)} as p95,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99, approx=True)} as p99
    FROM {{temp_table}}
    WHERE {col} IS NOT NULL AND elapsed IS NOT NULL
    GROUP BY {col}
    HAVING count(*) > {min_reqs}
    ORDER BY {{sort_idx}} DESC LIMIT 20
"""


TOP_URLS = _top_latency_sql("url", 5)
TOP_ASNS = _top_latency_sql("asn", 10)

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
    payload = {
        "ep": "performance_aggregates",
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "sb": sort_by,
        "sec": sorted(sections) if sections is not None else None,
        "cd": str(src.get("_cache_dir_override", "")),
    }
    return digest_cache_key(payload, src)


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
    """Return a cached performance aggregates response without acquiring a DuckDB connection."""
    key = _active_perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
    cached = _perf_memo.get_with_telemetry(key) if key is not None else None
    return dict(cached) if cached is not None else None


# Temp-table projection order for the section-aware narrowed temp. Mirrors the
# origin/security ``_TEMP_COL_ORDER`` pattern: the temp is built for ONLY the
# columns the live (rollup-missed) sections touch, in a stable order. No
# ``timestamp`` — the window predicate is applied at temp-creation time and no
# performance section SELECTs timestamp from the temp.
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
    if cache_key is not None:
        cached = _perf_memo.get_with_telemetry(cache_key)
        if cached is not None:
            return dict(cached)

    def _compute() -> dict:
        if cache_key is not None:
            cached_inner = _perf_memo.get_with_telemetry(cache_key)
            if cached_inner is not None:
                return dict(cached_inner)
        return _compute_performance_aggregates(con, src, start_time, end_time, filters, sort_by, sections, cache_key)

    if cache_key is not None:
        return _perf_memo.coalesce(f"perf_agg:{cache_key}", _compute)
    return _perf_memo.result(_compute(), state="bypass")


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
    # Per-phase wall-clock timings surface in the response under
    # _section_timings so the perf harness can attribute /api/performance/
    # aggregates without ad-hoc instrumentation. Mirrors network.py.
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)
    if not actual_cols:
        return {
            **{k: ([] if isinstance(v, list) else {}) for k, v in _PERF_SECTION_DEFAULTS.items()},
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)

    results = {**runner.telemetry()}
    sort_idx = {"avg": 3, "p50": 4, "p95": 5, "p99": 6}.get(sort_by, 6)

    def _resolve_asns(asn_res: list) -> list[dict]:
        """Attach ASN labels to the (asn, reqs, avg, p50, p95, p99) tuples
        the rollup-hit and live paths both produce."""
        from backend.core import duckdb as _db

        labels_started = _time.perf_counter()
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

    # ── Pass 1: serve every section that can come from a parquet rollup WITHOUT
    # the catalog temp, and record which sections still need a live scan plus
    # the exact temp columns they touch. The temp is then built for ONLY that
    # column set (or skipped entirely). The per-column form of the all-or-
    # nothing materialize the origin/aggregates fix removed — so a 30d
    # unfiltered request collapses ``temp_table_create`` to the scatter/
    # waterfall latency columns (or nothing when those aren't requested). ──
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
        t0 = _time.perf_counter()
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

    # 1. Top URLs + 3. Top ASNs by Latency — perf_latency rollup (unfiltered, >= 48 h);
    # per-dim elapsed percentiles request-weight-averaged across hours + re-ranked by sort_by.
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

        def _fetch_perf_rollup(_dim: str = dim, _min: int = min_reqs) -> Any:
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
            _fetch_perf_rollup,
            apply_fn,
        )

    # 5. Cache TTL Distribution — EXACT histogram rollup (COUNT sums, MIN-of-MIN
    # composes; no _approx). Live fallback reads {ttl} from the temp.
    _try_rollup_section(
        "ttl_dist",
        {"ttl"},
        "ttl_dist_rollup",
        "ttl_dist_query",
        lambda: runner.try_perf_ttl_dist_from_rollup(start_time, end_time, has_filters=bool(filters)),
        lambda rolled_ttl: results.__setitem__("ttl_dist", rolled_ttl),
    )

    # 6 + 7. Scatter + waterfall: always live. Scatter is a uniform 300-row
    # SAMPLE (not aggregatable) and waterfall is auto-paired with it
    # (_WATERFALL_SCATTER_PAIR). When top_urls/top_asns/ttl_dist are all
    # served from rollups (or not requested), skip materializing a full-window
    # temp table and query the base logs relation directly with partition_pruning.
    _want_scatter = _want("scatter")
    _want_waterfall = _want("waterfall")
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

    # ── Build the temp for ONLY the missed sections' columns, or skip it. ──
    temp_table = None
    if needed_cols:
        cols = [c for c in _PERF_TEMP_COL_ORDER if c in needed_cols]
        _t = _time.perf_counter()
        temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
        timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True)
        timer.mark("perf:temp_narrowed", _time.perf_counter(), activity="overhead", current_execution=True)
    else:
        timer.mark("perf:temp_skipped", _time.perf_counter(), activity="skipped", current_execution=True)

    # ── Pass 2: live SQL for the missed sections, against the narrowed temp. ──
    try:
        if "top_urls" in live_sections and temp_table:
            _t = _time.perf_counter()
            url_res = runner.execute(SQL.TOP_URLS.format(temp_table=temp_table, sort_idx=sort_idx)).fetchall()
            results["top_urls"] = [
                {"url": r[0], "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]} for r in url_res
            ]
            timer.mark("top_urls_query", _t, source="live", activity="compute", current_execution=True)

        if "top_asns" in live_sections and temp_table:
            _t = _time.perf_counter()
            asn_res = runner.execute(SQL.TOP_ASNS.format(temp_table=temp_table, sort_idx=sort_idx)).fetchall()
            timer.mark("top_asns_query", _t, source="live", activity="compute", current_execution=True)
            results["top_asns"] = _resolve_asns(asn_res)

        if "ttl_dist" in live_sections and temp_table:
            _t = _time.perf_counter()
            ttl_res = runner.execute(SQL.TTL_DIST.format(temp_table=temp_table)).fetchall()
            results["ttl_dist"] = [{"bucket": r[0], "count": r[1]} for r in ttl_res]
            timer.mark("ttl_dist_query", _t, source="live", activity="compute", current_execution=True)

        if "scatter_waterfall" in live_sections:
            # Scatter sample + waterfall averages from a single CTE pass.
            # When top_urls/top_asns/ttl_dist were served from rollups, temp_table
            # is None and we query the base logs relation directly with partition_pruning.
            _t = _time.perf_counter()
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
            waterfall_avg = {
                "edge_processing": 0.0,
                "origin_wait": 0.0,
                "origin_download": 0.0,
                "client_download": 0.0,
            }
            for r in combined_rows:
                if r[0] == "s":
                    scatter.append({"origin": r[1], "edge": r[2], "cache": r[3]})
                else:  # 'w'
                    waterfall_avg = {
                        "edge_processing": float(r[4] or 0.0),
                        "origin_wait": float(r[5] or 0.0),
                        "origin_download": float(r[6] or 0.0),
                        "client_download": float(r[7] or 0.0),
                    }
            if _want_scatter:
                results["scatter"] = scatter
            if _want_waterfall:
                results["waterfall"] = {"avg": waterfall_avg}
            timer.mark("scatter_waterfall_query", _t, source="live", activity="compute", current_execution=True)
    finally:
        _drop_temp_table(runner, temp_table, "get_performance_aggregates")

    # Backstop: any requested section left unpopulated (temp-create failure, or
    # a live section whose required column was absent) gets its empty default so
    # the response shape stays stable regardless of which path served it.
    for sec, default in _PERF_SECTION_DEFAULTS.items():
        if _want(sec) and sec not in results:
            results[sec] = [] if isinstance(default, list) else {}

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    if cache_key is not None:
        _perf_memo.put(cache_key, dict(results))
    return results


# ── Folded from backend.repositories.cmcd ─────────────────────────────────
"""CMCD (Common Media Client Data) repository — streaming QoE analytics."""

import sys as _sys

from backend.repositories.utils.response_cache import (
    ResponseMemo,
)
from backend.utils.date_utils import parse_iso_utc

# ── SQL templates (folded from backend/repositories/_sql/cmcd.py) ───────────

OVERVIEW = """
                WITH base_metrics AS (
                    SELECT
                        COUNT(DISTINCT cmcd_sid) AS active_sessions,
                        {rebuffer_rate_expr},
                        {avg_bitrate_expr} AS avg_bitrate,
                        {avg_buffer_length_expr} AS avg_buffer_length,
                        {median_throughput_expr} AS median_throughput
                    FROM {table} WHERE {where}
                ),
                peak_metrics AS (
                    SELECT MAX(cnt) AS peak_viewers FROM (
                        SELECT COUNT(DISTINCT cmcd_sid) AS cnt
                        FROM {table} WHERE {where}
                        GROUP BY {bucket_expr}
                    )
                ),
                dur_metrics AS (
                    SELECT ROUND(AVG(dur), 0) AS avg_dur FROM (
                        SELECT EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
                        FROM {table} WHERE {where}
                        GROUP BY cmcd_sid
                        HAVING dur > 0
                    )
                )
                SELECT
                    b.active_sessions,
                    b.rebuffer_rate,
                    b.avg_bitrate,
                    b.avg_buffer_length,
                    b.median_throughput,
                    p.peak_viewers,
                    d.avg_dur
                FROM base_metrics b
                CROSS JOIN peak_metrics p
                CROSS JOIN dur_metrics d
            """

TIMESERIES_COMBINED = "SELECT {select_cols} FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1"

NEW_SESSIONS = """
                        SELECT {first_bucket} AS bucket, COUNT(*) AS new_sessions
                        FROM (
                            SELECT cmcd_sid, MIN(timestamp) AS first_ts
                            FROM {table} WHERE {where} GROUP BY cmcd_sid
                        ) sub
                        GROUP BY 1
                    """

TOP_CONTENT = """
                WITH per_session AS (
                    SELECT cmcd_cid, cmcd_sid,
                        {rebuffer_agg} AS had_rebuffer,
                        {br_agg} AS avg_br,
                        {bl_agg} AS avg_bl
                    FROM {table}
                    WHERE {where} AND cmcd_cid IS NOT NULL AND cmcd_cid != ''
                    GROUP BY 1, 2
                )
                SELECT
                    cmcd_cid,
                    COUNT(*) AS session_count,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    AVG(avg_br) AS avg_br,
                    AVG(avg_bl) AS avg_bl
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """

REBUFFER_BY_COUNTRY = """
                WITH per_session AS (
                    SELECT country, cmcd_sid,
                        MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer,
                        {mtp_agg} AS median_mtp
                    FROM {table}
                    WHERE {where} AND country IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    country,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    COUNT(*) AS session_count,
                    AVG(median_mtp) AS median_mtp
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """

REBUFFER_BY_ASN = """
                WITH per_session AS (
                    SELECT asn, cmcd_sid,
                        MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer,
                        {br_agg} AS avg_br,
                        {bl_agg} AS avg_bl
                    FROM {table}
                    WHERE {where} AND asn IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    asn,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    COUNT(*) AS session_count,
                    AVG(avg_br) AS avg_br,
                    AVG(avg_bl) AS avg_bl
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """

DIST_BOTH = """
                    SELECT 'ot' AS kind, cmcd_ot AS val, COUNT(*) AS cnt
                    FROM {table} WHERE {where} AND cmcd_ot IS NOT NULL AND cmcd_ot != ''
                    GROUP BY 2
                    UNION ALL
                    SELECT 'sf' AS kind, cmcd_sf AS val, COUNT(DISTINCT cmcd_sid) AS cnt
                    FROM {table} WHERE {where} AND cmcd_sf IS NOT NULL AND cmcd_sf != ''
                    GROUP BY 2
                    ORDER BY kind ASC, cnt DESC
                """

DIST_OT = """
                    SELECT cmcd_ot, COUNT(*) AS request_count
                    FROM {table} WHERE {where} AND cmcd_ot IS NOT NULL AND cmcd_ot != ''
                    GROUP BY 1 ORDER BY 2 DESC
                """

DIST_SF = """
                    SELECT cmcd_sf, COUNT(DISTINCT cmcd_sid) AS session_count
                    FROM {table} WHERE {where} AND cmcd_sf IS NOT NULL AND cmcd_sf != ''
                    GROUP BY 1 ORDER BY 2 DESC
                """

SESSION_DURATION_DIST = """
                WITH session_spans AS (
                    SELECT cmcd_sid,
                        EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
                    FROM {table} WHERE {where}
                    GROUP BY cmcd_sid
                )
                SELECT
                    CASE
                        WHEN dur < 30 THEN '<30s'
                        WHEN dur < 120 THEN '30s–2m'
                        WHEN dur < 300 THEN '2–5m'
                        WHEN dur < 600 THEN '5–10m'
                        WHEN dur < 1800 THEN '10–30m'
                        WHEN dur < 3600 THEN '30m–1h'
                        ELSE '>1h'
                    END AS duration_bucket,
                    CASE
                        WHEN dur < 30 THEN 1
                        WHEN dur < 120 THEN 2
                        WHEN dur < 300 THEN 3
                        WHEN dur < 600 THEN 4
                        WHEN dur < 1800 THEN 5
                        WHEN dur < 3600 THEN 6
                        ELSE 7
                    END AS sort_order,
                    COUNT(*) AS session_count
                FROM session_spans
                GROUP BY 1, 2
                ORDER BY 2
            """

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


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cmcd._response_cache", _response_cache)


def _response_cache_key(*args, **kwargs) -> str:
    if (args and isinstance(args[0], dict)) or (
        "actual_cols" in kwargs or ("src" in kwargs and "endpoint" not in kwargs)
    ):
        return _cmcd_response_cache_key(*args, **kwargs)
    return _origin_response_cache_key(*args, **kwargs)


import sys as _sys  # noqa: E402

for _alias in (
    "backend.repositories.performance",
    "backend.repositories.cmcd",
    "backend.repositories._sql.performance",
    "backend.repositories._sql.cmcd",
):
    _sys.modules[_alias] = _sys.modules[__name__]
