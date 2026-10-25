"""Assets, Shield & Usage repository for static asset performance, log activity, storage stats, and edge ratio."""

from __future__ import annotations

import logging
import sys as _sys
import time as _time
from typing import Any

import duckdb

from backend import config as svcconfig
from backend.core.metric_expressions import coalesce, column_or_default, sum_or_zero
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    ResponseMemo,
    SectionTimer,
    _compact_sql_for_debug,
    _safe_table,
    bucket_time_to_minute,
    digest_cache_key,
    empty_schema_response,
    record_duckdb_query,
    serialize_filters_for_key,
)
from backend.repositories.utils.filters import build_where_clause
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.telemetry import log_once_per

logger = logging.getLogger(__name__)

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("assets._response_cache", _response_cache)

EDGE_RATIO_PCT = "SELECT count(*) FILTER (WHERE edge = true) * 100.0 / count(*) FROM {table}"
REQUEST_ROW_STATS = "SELECT COUNT(*), MAX(timestamp) FROM {table}"
WAF_FLAG_COUNTS_7D = """
SELECT SUM(CASE WHEN waf = true THEN 1 ELSE 0 END), COUNT(*)
FROM {table}
WHERE timestamp >= now() - INTERVAL '7 days'
"""

_LOG_ACTIVITY_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=30.0)
_CacheRegistry.register("repositories.usage._LOG_ACTIVITY_CACHE", _LOG_ACTIVITY_CACHE)


def _build_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> str:
    return digest_cache_key(
        {
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(filters),
        },
        src,
    )


def check_assets_cache(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict | None:
    """Fast pre-connection cache check for production file-backed services."""
    if not src.get("duckdb_path") and not src.get("cache_dir"):
        return None
    cached = _memo.get_with_telemetry(_build_cache_key(src, start_time, end_time, filters))
    if cached is not None:
        return {**cached, "section_timings": []}
    return None


def get_assets_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    timer = SectionTimer()
    section_timings = timer.entries

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t)

    optional_fields = ["resp_header_content_encoding", "ttl"]
    missing_fields = [f for f in optional_fields if f not in actual_cols]

    if not actual_cols:
        return empty_schema_response(
            asset_type_breakdown=[],
            cache_performance=[],
            compression_performance=[],
            large_uncompressed_assets=[],
            low_ttl_assets=[],
            missing_fields=missing_fields,
            section_timings=section_timings,
            **runner.telemetry(),
        )

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time,
        end_time,
        filters,
        actual_cols,
        inline_params=True,
        partition_pruning=True,
    )
    timer.mark("build_where_clause", _t)

    has_partition_cols = "dt" in actual_cols or "timestamp_hour" in actual_cols
    cache_key: str | None = None
    if has_partition_cols or src.get("duckdb_path") or src.get("cache_dir"):
        cache_key = _build_cache_key(src, start_time, end_time, filters)
        cached = _memo.get_with_telemetry(cache_key)
        if cached is not None and not missing_fields:
            return {**runner.telemetry(), **cached, "section_timings": section_timings}

    results = {
        "missing_fields": missing_fields,
        **runner.telemetry(),
    }

    # Graceful defaults for optional columns
    encoding_expr = column_or_default(actual_cols, "resp_header_content_encoding", "''")
    bytes_expr = column_or_default(actual_cols, "resp_bytes", "0", cast="BIGINT")
    status_expr = column_or_default(actual_cols, "status", "200", cast="INTEGER")
    ttl_expr = column_or_default(actual_cols, "ttl", "0.0", cast="DOUBLE")
    cache_expr = column_or_default(actual_cols, "cache", "'MISS'")
    url_expr = '"url"' if "url" in actual_cols else "''"

    needed_cols = ["url", "cache", "resp_header_content_encoding", "resp_bytes", "status", "ttl"]
    cols = [c for c in needed_cols if c in actual_cols]

    if not cols:
        return empty_schema_response(
            asset_type_breakdown=[],
            cache_performance=[],
            compression_performance=[],
            large_uncompressed_assets=[],
            low_ttl_assets=[],
            missing_fields=missing_fields,
            section_timings=section_timings,
            **runner.telemetry(),
        )

    import uuid as _uuid

    from backend.high_scale.assets import compute_summary_from_dims

    temp_table: str | None = f"t_{_uuid.uuid4().hex}"

    create_sql = f"""
        CREATE TEMP TABLE "{temp_table}" AS
        WITH grouped_raw AS (
            SELECT
                {url_expr} as url,
                {cache_expr} as cache,
                {encoding_expr} as content_encoding,
                {status_expr} as status,
                COUNT(*) as req_cnt,
                {sum_or_zero(bytes_expr)} as sum_bytes,
                {sum_or_zero(ttl_expr, zero="0.0")} as sum_ttl,
                COUNT({ttl_expr}) as cnt_ttl
            FROM {table_name}
            WHERE {where_clause} AND {url_expr} IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ),
        grouped_base AS (
            SELECT
                *,
                LOWER(url) as url_lower
            FROM grouped_raw
        )
        SELECT
            url,
            cache,
            content_encoding,
            status,
            req_cnt,
            sum_bytes,
            sum_ttl,
            cnt_ttl,
            CASE
                WHEN url_lower LIKE '%.png' OR url_lower LIKE '%.jpg' OR url_lower LIKE '%.jpeg' OR url_lower LIKE '%.gif' OR url_lower LIKE '%.svg' OR url_lower LIKE '%.webp' OR url_lower LIKE '%.ico' THEN 'Images'
                WHEN url_lower LIKE '%.pdf' OR url_lower LIKE '%.doc' OR url_lower LIKE '%.docx' OR url_lower LIKE '%.xls' OR url_lower LIKE '%.xlsx' OR url_lower LIKE '%.ppt' OR url_lower LIKE '%.pptx' OR url_lower LIKE '%.txt' OR url_lower LIKE '%.csv' OR url_lower LIKE '%.zip' OR url_lower LIKE '%.tar' OR url_lower LIKE '%.gz' OR url_lower LIKE '%.tgz' OR url_lower LIKE '%.rar' OR url_lower LIKE '%.7z' THEN 'Documents'
                WHEN url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css' THEN 'JavaScript/CSS'
                WHEN url_lower LIKE '%.woff' OR url_lower LIKE '%.woff2' OR url_lower LIKE '%.ttf' OR url_lower LIKE '%.otf' OR url_lower LIKE '%.eot' THEN 'Fonts'
                WHEN url_lower LIKE '%.m3u8' OR url_lower LIKE '%.ts' OR url_lower LIKE '%.mp4' OR url_lower LIKE '%.m4s' OR url_lower LIKE '%.webm' THEN 'Video'
                ELSE 'API/Dynamic'
            END as asset_type,
            CASE
                WHEN LOWER(content_encoding) IN ('gzip', 'br', 'deflate') THEN 1
                ELSE 0
            END as is_compressed,
            CASE
                WHEN url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css' OR url_lower LIKE '%.svg' OR url_lower LIKE '%.html' OR url_lower LIKE '%.json' THEN 1
                ELSE 0
            END as is_compressible
        FROM grouped_base
    """

    _t = _time.perf_counter()
    if not runner.create_temp_table(create_sql, params):
        temp_table = None
    timer.mark("temp_table_create", _t)

    try:
        _t = _time.perf_counter()
        # 1-3 Combined: Asset Type Breakdown, Cache Performance, and Compression Performance
        dims_q = f"""
            SELECT
                asset_type,
                cache as cache_status,
                CASE
                    WHEN LOWER(content_encoding) LIKE '%br%' THEN 'br'
                    WHEN LOWER(content_encoding) LIKE '%gzip%' THEN 'gzip'
                    WHEN content_encoding != '' THEN content_encoding
                    ELSE 'uncompressed'
                END as content_encoding_norm,
                is_compressed,
                is_compressible,
                CAST({sum_or_zero("req_cnt")} AS BIGINT) as requests,
                CAST({sum_or_zero("sum_bytes")} AS BIGINT) as bytes
            FROM "{temp_table}"
            GROUP BY 1, 2, 3, 4, 5
        """
        dim_rows = runner.execute(dims_q).fetchall()
        (
            results["asset_type_breakdown"],
            results["cache_performance"],
            results["compression_performance"],
        ) = compute_summary_from_dims(dim_rows)

        # 4-5 Combined: Large Uncompressed Assets + Low-TTL Cache Hotspots
        hotspots_q = f"""
            WITH large_top AS (
                SELECT
                    'large' AS kind,
                    url,
                    CAST({sum_or_zero("req_cnt")} AS BIGINT) as requests,
                    SUM(sum_bytes) * 1.0 / NULLIF(SUM(req_cnt), 0) as metric_float,
                    CAST({sum_or_zero("sum_bytes")} AS BIGINT) as total_bytes,
                    status,
                    CAST(NULL AS VARCHAR) as asset_type
                FROM "{temp_table}"
                WHERE is_compressed = 0 AND is_compressible = 1 AND status NOT IN (304, 204)
                GROUP BY url, status
                ORDER BY total_bytes DESC
                LIMIT 20
            ),
            low_ttl_top AS (
                SELECT
                    'low_ttl' AS kind,
                    url,
                    CAST({sum_or_zero("req_cnt")} AS BIGINT) as requests,
                    SUM(sum_ttl) * 1.0 / NULLIF(SUM(cnt_ttl), 0) as metric_float,
                    CAST(0 AS BIGINT) as total_bytes,
                    CAST(0 AS INTEGER) as status,
                    asset_type
                FROM "{temp_table}"
                WHERE cache = 'MISS' AND asset_type != 'API/Dynamic'
                GROUP BY url, asset_type
                ORDER BY requests DESC
                LIMIT 20
            )
            SELECT * FROM large_top
            UNION ALL
            SELECT * FROM low_ttl_top
        """
        hotspot_rows = runner.execute(hotspots_q).fetchall()
        large_uncompressed_assets = []
        low_ttl_assets = []
        for kind, url, reqs, metric_float, total_bytes, status, asset_type in hotspot_rows:
            if kind == "large":
                large_uncompressed_assets.append(
                    {
                        "url": url,
                        "requests": int(reqs or 0),
                        "avg_bytes": float(metric_float or 0.0),
                        "total_bytes": int(total_bytes or 0),
                        "status": status,
                    }
                )
            elif kind == "low_ttl":
                low_ttl_assets.append(
                    {
                        "url": url,
                        "requests": int(reqs or 0),
                        "avg_ttl": float(metric_float or 0.0),
                        "asset_type": asset_type,
                    }
                )
        results["large_uncompressed_assets"] = large_uncompressed_assets
        results["low_ttl_assets"] = low_ttl_assets

        timer.mark("queries_execution", _t)

    finally:
        if temp_table:
            try:
                runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except Exception:
                if log_once_per("get_assets_aggregates: DROP TABLE failed (ignored)", 60):
                    logger.debug("get_assets_aggregates: DROP TABLE failed (ignored)", exc_info=True)

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    if cache_key is not None and not missing_fields and results["asset_type_breakdown"]:
        _memo.put(cache_key, results, strip=("section_timings",))
    return results


def get_edge_ratio(con: duckdb.DuckDBPyConnection, src: dict) -> tuple[float | None, list]:
    """Return (edge_ratio_pct_or_None, debug_queries)."""
    runner = QueryRunner(con, src)
    table = _safe_table(src["name"])
    from backend.core.duckdb import get_schema

    actual_cols = [col["name"] for col in get_schema(con, src, stats=False)]
    if "edge" not in actual_cols:
        return None, runner.debug_queries
    result = runner.execute_with_retry(EDGE_RATIO_PCT.format(table=table))
    if result is None:
        return None, runner.debug_queries
    row = result.fetchone()
    ratio = round(float(row[0]), 1) if row and row[0] is not None else None
    return ratio, runner.debug_queries


def get_storage_stats(
    con: duckdb.DuckDBPyConnection | None,
    src: dict,
    start_str: str,
    end_str: str,
) -> dict:
    """Return ingested-file count and total bytes for the given time window.

    Window filter is pushed into SQLite (COUNT/SUM against the source_name
    index) so the cost panel doesn't pull every row per service open.
    """
    from backend.core import metadata as metadata_db

    total_files, total_bytes = metadata_db.get_storage_stats_window(src["name"], start_str, end_str)
    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "_debug_queries": [],
        "_debug_calls": [],
    }


def get_log_activity(
    src: dict,
    start_str: str,
    end_str: str,
    by: str,
) -> dict:
    """Return time-bucketed log activity (rows and bytes per bucket).

    Primary path reads from the DuckDB overview rollup parquets so the
    data covers the full retention window (the old SQLite
    ``ingested_files`` source was trimmed nightly by metadata_cleanup,
    creating a misleading gap in the chart).  Falls back to the SQLite
    path for brand-new services that have no rollup data yet.
    """
    import os
    from datetime import UTC, datetime, timedelta

    import duckdb as _duckdb

    from backend.core.rollups import OVERVIEW_BUNDLE_FILENAME
    from backend.core.rollups._common import _hour_bundled_root, quote_path_list
    from backend.utils.date_utils import parse_iso_utc

    durable_serving = svcconfig.is_durable_serving_mode(src)
    interval_map = (
        {"second": "1 second", "minute": "1 minute", "hour": "1 hour", "day": "1 day"}
        if durable_serving
        else {"hour": "1 hour", "day": "1 day"}
    )
    interval = interval_map.get(by)
    if not interval:
        return _log_activity_fallback(src, start_str, end_str, by)

    st = parse_iso_utc(start_str)
    et = parse_iso_utc(end_str)
    if st is None or et is None or et <= st:
        return _log_activity_fallback(src, start_str, end_str, by)

    svc_id = src.get("name") or src.get("service_id", "")
    cache_key = (
        svc_id,
        st.astimezone(UTC).isoformat(),
        et.astimezone(UTC).isoformat(),
        by,
        bool(durable_serving),
    )
    cached_res = _LOG_ACTIVITY_CACHE.get(cache_key)
    if cached_res is not None:
        for dq in cached_res.get("_debug_queries", []):
            record_duckdb_query(dq.get("sql", ""), float(dq.get("time_ms", 0.0)), service_id=svc_id)
        return {
            **cached_res,
            "data": [dict(p) for p in cached_res.get("data", [])],
        }

    if durable_serving:
        res = _log_activity_durable(src, st, et, by)
        _LOG_ACTIVITY_CACHE[cache_key] = res
        return {**res, "data": [dict(p) for p in res.get("data", [])]}

    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return _log_activity_fallback(src, start_str, end_str, by)

    # Collect available overview parquets without the strict
    # "writer-behind" bailout that collect_hourly_bundle_paths uses.
    # For this chart, partial rollup data beats empty SQLite data.
    active_hour_str = datetime.now(UTC).strftime("%Y-%m-%d-%H")
    rollup_paths: list[str] = []
    cursor = st.replace(minute=0, second=0, microsecond=0)
    while cursor < et:
        hour_str = cursor.strftime("%Y-%m-%d-%H")
        if hour_str >= active_hour_str:
            break
        path = os.path.join(bundled_root, f"hour={hour_str}", OVERVIEW_BUNDLE_FILENAME)
        if os.path.isfile(path):
            rollup_paths.append(path)
        cursor += timedelta(hours=1)

    if not rollup_paths:
        return _log_activity_fallback(src, start_str, end_str, by)

    paths_sql = quote_path_list(rollup_paths)
    st_tz = st.astimezone(UTC).isoformat()
    et_tz = et.astimezone(UTC).isoformat()
    # Parquet files are self-contained — use an in-memory DuckDB
    # connection so we don't contend with the service's DB lock.
    sql = (
        f"SELECT time_bucket(INTERVAL '{interval}', hour_start) AS bucket, "
        f"  SUM(requests) AS total, "
        f"  SUM(total_bandwidth_bytes) AS total_bytes "
        f"FROM read_parquet([{paths_sql}]) "
        f"WHERE hour_start >= TIMESTAMPTZ '{st_tz}' "
        f"  AND hour_start < TIMESTAMPTZ '{et_tz}' "
        f"GROUP BY 1 ORDER BY 1"
    )

    fmt = "%Y-%m-%dT%H:%M" if by == "hour" else "%Y-%m-%d"
    t0 = _time.perf_counter()
    try:
        con = _duckdb.connect()
        try:
            rows = con.execute(sql).fetchall()
        finally:
            con.close()
    except Exception:
        return _log_activity_fallback(src, start_str, end_str, by)
    elapsed_ms = round((_time.perf_counter() - t0) * 1000, 2)
    compact_sql = _compact_sql_for_debug(sql)
    record_duckdb_query(compact_sql, elapsed_ms, service_id=svc_id)

    points: list[dict] = []
    total_rows = 0
    total_bytes = 0
    for r in rows:
        rc = int(r[1])
        bs = int(r[2])
        points.append(
            {
                "time": r[0].strftime(fmt),
                "row_count": rc,
                "bytes": bs,
            }
        )
        total_rows += rc
        total_bytes += bs

    res = {
        "data": points,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "granularity": by,
        "_debug_queries": [{"sql": compact_sql, "time_ms": elapsed_ms, "engine": "DuckDB"}],
        "_debug_calls": [],
    }
    _LOG_ACTIVITY_CACHE[cache_key] = res
    return {**res, "data": [dict(p) for p in points]}


def _log_activity_durable(
    src: dict,
    st,
    et,
    by: str,
) -> dict:
    """Read log activity from durable DuckLake when local rollups are absent."""
    from backend.core.duckdb import get_connection

    interval = {"second": "1 second", "minute": "1 minute", "hour": "1 hour", "day": "1 day"}[by]
    svc_id = str(src.get("name") or src.get("service_id") or "")
    table = _safe_table(src["name"])
    con = get_connection(source=src, read_only=True)
    try:
        actual_cols = {row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()}
    except Exception:
        actual_cols = {"resp_bytes"}
    bytes_expr = "SUM(resp_bytes)" if "resp_bytes" in actual_cols else "0"
    sql = (
        f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS bucket, "
        f"COUNT(*) AS total, "
        f"{coalesce(bytes_expr, '0')} AS total_bytes "
        f"FROM {table} "
        f"WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp < CAST(? AS TIMESTAMPTZ) "
        f"GROUP BY 1 ORDER BY 1"
    )
    t0 = _time.perf_counter()
    try:
        rows = con.execute(sql, [st.isoformat(), et.isoformat()]).fetchall()
    finally:
        con.close()
    elapsed_ms = round((_time.perf_counter() - t0) * 1000, 2)
    compact_sql = _compact_sql_for_debug(sql)
    record_duckdb_query(compact_sql, elapsed_ms, service_id=svc_id)

    fmt = {
        "second": "%Y-%m-%dT%H:%M:%S",
        "minute": "%Y-%m-%dT%H:%M",
        "hour": "%Y-%m-%dT%H:00",
        "day": "%Y-%m-%d",
    }[by]
    points = [
        {
            "time": row[0].strftime(fmt),
            "row_count": int(row[1]),
            "bytes": int(row[2] or 0),
        }
        for row in rows
    ]
    return {
        "data": points,
        "total_rows": sum(point["row_count"] for point in points),
        "total_bytes": sum(point["bytes"] for point in points),
        "granularity": by,
        "_debug_queries": [{"sql": compact_sql, "time_ms": elapsed_ms, "engine": "DuckDB"}],
        "_debug_calls": [],
    }


def _log_activity_fallback(
    src: dict,
    start_str: str,
    end_str: str,
    by: str,
) -> dict:
    """Fall back to SQLite ingested_files for services with no rollup data."""
    from backend.core import metadata as metadata_db

    service_id = src.get("name") or src.get("service_id", "")
    out = metadata_db.get_log_activity(service_id, start_str, end_str, by)
    return {**out, "_debug_queries": [], "_debug_calls": []}


def get_request_row_stats(con, table_name: str) -> tuple[int, Any]:
    """``(total rows, newest timestamp)`` for the request table in one scan; newest is ``None`` when empty."""
    row = con.execute(REQUEST_ROW_STATS.format(table=table_name)).fetchone()
    return (row[0], row[1]) if row else (0, None)


get_service_row_stats = get_request_row_stats


def get_waf_flag_counts_7d(con, table_name: str) -> tuple[Any, Any] | None:
    """``(rows with waf = true, total rows)`` over the last 7 days, or ``None`` when no row is returned."""
    result = con.execute(WAF_FLAG_COUNTS_7D.format(table=table_name)).fetchone()
    return (result[0], result[1]) if result else None


_sys.modules.setdefault("backend.repositories.usage", _sys.modules[__name__])
_repos_pkg = _sys.modules.get("backend.repositories")
if _repos_pkg is not None:
    _repos_pkg.usage = _sys.modules[__name__]  # type: ignore[attr-defined]
