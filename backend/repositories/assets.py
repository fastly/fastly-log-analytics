"""Assets, Shield & Usage repository for static asset performance, log activity, storage stats, and edge ratio."""

from __future__ import annotations

import logging
import sys as _sys
import time as _time
from datetime import UTC
from typing import Any

import duckdb

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.core.metric_expressions import coalesce, column_or_default, get_metric_sql, sum_or_zero
from backend.models.common import Alert, FiltersDict, SavedView
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
from backend.utils.telemetry import log_once_per, track_query

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

    from backend.high_scale.ingest_controller import compute_summary_from_dims

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


# ── Alerts, Cron Logs & Saved Views (folded from alerts.py) ──────────────────

MAX_TIMESTAMP = "SELECT max(timestamp) FROM {table}"

PREVIEW_SERIES = """
        SELECT {group_sql} as ts, {agg_sql} as val
        FROM {table}
        WHERE timestamp >= (SELECT max(timestamp) FROM {table}) - INTERVAL '{lookback_mins} minutes' {scope_filter}
        GROUP BY 1
        ORDER BY 1 ASC
    """

PREVIEW_ANY_ROW = "SELECT 1 FROM {table} LIMIT 1"

PREVIEW_MAX_TS = "SELECT max(timestamp) FROM {table}"

PREVIEW_BASELINE = """
                SELECT
                    EXTRACT(hour FROM ts) AS hr,
                    avg(val) AS mean_val,
                    stddev(val) AS stddev_val
                FROM (
                    SELECT date_trunc('hour', timestamp) AS ts, {agg_sql} AS val
                    FROM {table}
                    WHERE {baseline_where}
                    GROUP BY 1
                )
                GROUP BY 1
            """

PREVIEW_HISTORY = """
                 SELECT {group_sql} as ts, {agg_sql} as val
                 FROM {table}
                 WHERE timestamp >= (SELECT max(timestamp) FROM {table}) - INTERVAL '{start_minutes} minutes'
                   AND timestamp <= (SELECT max(timestamp) FROM {table}) - INTERVAL '{comp_period} minutes'
                   {scope_filter}
                 GROUP BY 1
                 ORDER BY 1 ASC
             """


def get_alerts(service_id: str | None = None, *, limit: int = 500) -> list[dict]:
    """Return alerts for a service or globally-newest across all services."""
    if service_id:
        return metadata_db.list_alerts(service_id, filter_service_id=service_id)
    service_ids: list[str] = [sid for cfg in svcconfig.list_configs() if (sid := cfg.get("service_id"))]
    return metadata_db.list_alerts_cross_service(service_ids, limit=limit)


def save_alert(alert: Alert) -> dict:
    return metadata_db.save_alert(alert.service_id, alert)


def get_alert_by_id(alert_id: str, service_id: str) -> dict | None:
    """Return the alert row whose id matches ``alert_id`` in the given service (or None)."""
    for a in metadata_db.list_alerts(service_id, filter_service_id=service_id):
        if a.get("id") == alert_id:
            return a
    return None


def toggle_alert(alert_id: str, enabled: bool, service_id: str) -> dict:
    """Toggle an alert. ``service_id`` is required."""
    return metadata_db.toggle_alert(service_id, alert_id, enabled)


def delete_alert(alert_id: str, service_id: str) -> dict:
    """Delete an alert. ``service_id`` is required."""
    return metadata_db.delete_alert(service_id, alert_id)


def update_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    metadata_db.update_alert_last_triggered(service_id, alert_id, triggered_ts)


def evaluate_alert(
    con: duckdb.DuckDBPyConnection, src: dict, alert: dict, display_name: str = "", service_id: str = ""
) -> tuple[bool, str | None, dict | None, str | None]:
    """Run one alert against the log table; returns (triggered, webhook_url, payload, max_ts)."""
    from datetime import datetime, timedelta

    from backend.core.duckdb import _safe_table_name

    table_name = _safe_table_name(src["name"])
    metric = alert["metric"]
    window = alert["window_min"]
    eval_type = alert.get("evaluation_type", "absolute")
    eval_scope = alert.get("evaluation_scope", "all")
    comp_period = alert.get("comparison_period_min")
    status_codes = alert.get("status_codes")

    try:
        max_ts_query = MAX_TIMESTAMP.format(table=table_name)
        with track_query(con, max_ts_query, [], "alerts") as cursor:
            max_ts = cursor.fetchone()[0]

        if not max_ts:
            return False, None, None, None

        from backend.utils.date_utils import ensure_utc, parse_iso_utc

        now_utc = datetime.now(UTC)

        max_ts_dt = max_ts
        if isinstance(max_ts_dt, str):
            max_ts_dt = parse_iso_utc(max_ts_dt)
        if hasattr(max_ts_dt, "tzinfo") and max_ts_dt.tzinfo is None:
            max_ts_dt = ensure_utc(max_ts_dt)

        if hasattr(max_ts, "replace"):
            max_ts_aware = ensure_utc(max_ts)
            if (now_utc - max_ts_aware) > timedelta(minutes=30):
                return False, None, None, None

        max_ts_literal = f"'{max_ts}'::TIMESTAMPTZ"

        try:
            existing_cols = set(QueryRunner(con, src).get_schema_cols())
        except Exception:
            existing_cols = set()

        def _window_offset(minutes_ago: float) -> str:
            return f"{max_ts_literal} - INTERVAL '{minutes_ago} minutes'"

        def _append_partition_bounds(where_clause: str, start_dt: datetime, end_dt: datetime) -> str:
            st_utc = start_dt.astimezone(UTC)
            et_utc = end_dt.astimezone(UTC)
            if "dt" in existing_cols:
                where_clause += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"
                where_clause += f" AND dt <= '{et_utc.strftime('%Y-%m-%d')}'"
            if "timestamp_hour" in existing_cols:
                where_clause += f" AND timestamp_hour >= '{st_utc.strftime('%Y-%m-%d-%H')}'"
                where_clause += f" AND timestamp_hour <= '{et_utc.strftime('%Y-%m-%d-%H')}'"
            return where_clause

        def build_metric_query(
            window_start_expr: str, window_end_expr: str, start_dt: datetime, end_dt: datetime
        ) -> str:
            agg_or_sel = get_metric_sql(metric, status_codes)
            where_clause = _append_partition_bounds(
                f"timestamp >= {window_start_expr} AND timestamp <= {window_end_expr}",
                start_dt,
                end_dt,
            )

            if eval_scope == "edge":
                where_clause += " AND edge = true"
            elif eval_scope == "origin":
                where_clause += " AND edge = false"

            if agg_or_sel.startswith("SELECT"):
                if "WHERE" in agg_or_sel:
                    return f"{agg_or_sel} AND {where_clause}"
                return f"{agg_or_sel} WHERE {where_clause}"
            return f"SELECT {agg_or_sel} FROM {table_name} WHERE {where_clause}"

        start_dt_current = max_ts_dt - timedelta(minutes=window)
        end_dt_current = max_ts_dt

        current_start = _window_offset(window)
        current_end = max_ts_literal
        q_current = build_metric_query(current_start, current_end, start_dt_current, end_dt_current)

        with track_query(con, q_current, [], "alerts") as cursor:
            val = cursor.fetchone()[0] or 0

        if eval_type in ("relative_increase", "relative_decrease") and comp_period:
            if metric != "requests":
                q_req_where = _append_partition_bounds(
                    f"timestamp >= {current_start} AND timestamp <= {current_end}",
                    start_dt_current,
                    end_dt_current,
                )
                q_req = f"SELECT count(*) FROM {table_name} WHERE {q_req_where}"
                with track_query(con, q_req, [], "alerts") as cursor:
                    req_count = cursor.fetchone()[0] or 0
            else:
                req_count = val

            if req_count < 10:
                return False, None, None, None

            start_dt_hist = max_ts_dt - timedelta(minutes=comp_period + window)
            end_dt_hist = max_ts_dt - timedelta(minutes=comp_period)

            hist_start = _window_offset(comp_period + window)
            hist_end = _window_offset(comp_period)
            q_hist = build_metric_query(hist_start, hist_end, start_dt_hist, end_dt_hist)

            with track_query(con, q_hist, [], "alerts") as cursor:
                hist_val = cursor.fetchone()[0] or 0

            if hist_val == 0:
                return False, None, None, None

            if eval_type == "relative_increase":
                val = ((val - hist_val) / hist_val) * 100.0
            else:
                val = ((hist_val - val) / hist_val) * 100.0
        elif eval_type == "anomaly_zscore":
            if metric != "requests":
                q_req_where = _append_partition_bounds(
                    f"timestamp >= {current_start} AND timestamp <= {current_end}",
                    start_dt_current,
                    end_dt_current,
                )
                q_req = f"SELECT count(*) FROM {table_name} WHERE {q_req_where}"
                with track_query(con, q_req, [], "alerts") as cursor:
                    req_count = cursor.fetchone()[0] or 0
            else:
                req_count = val

            days = alert.get("baseline_period_days") or 7
            zscore_thresh = alert.get("zscore_threshold") or 3.0

            baseline_start_dt = max_ts_dt - timedelta(days=days)
            baseline_end_dt = max_ts_dt - timedelta(hours=1)

            baseline_start_expr = f"{max_ts_literal} - INTERVAL '{days} days'"
            baseline_end_expr = f"{max_ts_literal} - INTERVAL '1 hours'"

            agg_or_sel = get_metric_sql(metric, status_codes)
            where_clause = _append_partition_bounds(
                f"timestamp >= {baseline_start_expr} AND timestamp <= {baseline_end_expr} "
                f"AND EXTRACT(hour FROM timestamp) = EXTRACT(hour FROM {max_ts_literal})",
                baseline_start_dt,
                baseline_end_dt,
            )

            if agg_or_sel.strip().lower().startswith("select"):
                agg_expr = agg_or_sel.replace(f"FROM {table_name}", "").replace("SELECT", "").strip()
                q_hist_series = f"SELECT {agg_expr} AS val FROM {table_name} WHERE {where_clause} GROUP BY date_trunc('hour', timestamp)"
            else:
                q_hist_series = f"SELECT {agg_or_sel} AS val FROM {table_name} WHERE {where_clause} GROUP BY date_trunc('hour', timestamp)"

            q_stats = f"WITH series AS ({q_hist_series}) SELECT avg(val), stddev(val) FROM series"

            with track_query(con, q_stats, [], "alerts") as cursor:
                mean_val, stddev_val = cursor.fetchone()

            if mean_val is None:
                return False, None, None, None

            stddev_val = stddev_val or 0.0001
            zscore = (val - mean_val) / stddev_val

            val = zscore
            alert["operator"] = ">"
            alert["threshold"] = zscore_thresh
        else:
            req_count = val

    except Exception as e:
        logger.error("Failed to evaluate alert %s: %s", alert.get("id"), e)
        return False, None, None, None

    op = alert["operator"]
    thresh = alert["threshold"]

    triggered = False
    if op == ">":
        triggered = val > thresh
    elif op == "<":
        triggered = val < thresh
    elif op == ">=":
        triggered = val >= thresh
    elif op == "<=":
        triggered = val <= thresh

    if triggered and req_count < 50 and eval_type != "absolute":
        return False, None, None, None

    if triggered:
        if alert.get("last_triggered_at"):
            try:
                from backend.utils.date_utils import ensure_utc, parse_iso_utc

                last = parse_iso_utc(alert["last_triggered_at"]) or datetime.fromtimestamp(0, tz=UTC)
                last = ensure_utc(last)
                if now_utc - last < timedelta(hours=1):
                    return False, None, None, None
            except ValueError:
                pass

        webhook_payload: dict | None = None
        if alert.get("webhook_url"):
            s_id = service_id or alert.get("service_id", "")
            disp = display_name or src.get("name", s_id)
            manage_link = f"https://manage.fastly.com/configure/services/{s_id}" if s_id else "Unknown"

            if eval_type == "relative_increase":
                msg_val = f"increased by {val:.1f}% vs {comp_period}m ago"
            elif eval_type == "relative_decrease":
                msg_val = f"decreased by {val:.1f}% vs {comp_period}m ago"
            else:
                msg_val = f"is {val:.2f}"

            webhook_payload = {
                "text": (
                    "🚨 *Fastly Alert Triggered*\n"
                    f"*Name:* {alert['name']}\n"
                    f"*Metric:* {metric} {msg_val} (Threshold: {op} {thresh})\n"
                    f"*Service:* {disp}\n"
                    f"*Manage:* {manage_link}\n"
                    f"*Window:* Last {window} minutes"
                )
            }

        if hasattr(max_ts, "replace"):
            max_ts_aware = max_ts.replace(tzinfo=UTC) if max_ts.tzinfo is None else max_ts
            max_ts_str = max_ts_aware.isoformat()
        else:
            max_ts_str = str(max_ts)

        return True, alert.get("webhook_url"), webhook_payload, max_ts_str

    return False, None, None, None


def preview_alert_series(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    table_name: str,
    alert: Alert,
    lookback_hours: int,
    *,
    has_status_data,
) -> dict:
    """Time series (plus baseline / comparison overlays) behind ``POST /api/alerts/preview``."""
    import datetime

    from backend.repositories._base import safe_iso

    metric = alert.metric
    window = alert.window_min
    eval_type = getattr(alert, "evaluation_type", "absolute")
    eval_scope = getattr(alert, "evaluation_scope", "all")
    comp_period = getattr(alert, "comparison_period_min", None)
    status_codes = getattr(alert, "status_codes", None)

    lookback_mins = lookback_hours * 60
    if eval_type in ("relative_increase", "relative_decrease") and comp_period:
        lookback_mins = max(lookback_mins, comp_period + window + 60)

    if lookback_mins <= 180:
        group_sql = "time_bucket(INTERVAL '1 minute', timestamp)"
    elif lookback_mins <= 1440:
        group_sql = "time_bucket(INTERVAL '15 minutes', timestamp)"
    else:
        group_sql = "time_bucket(INTERVAL '1 hour', timestamp)"

    agg_sql = get_metric_sql(metric, status_codes)

    base_scope_filter = ""
    if eval_scope == "edge":
        base_scope_filter = " AND edge = true"
    elif eval_scope == "origin":
        base_scope_filter = " AND edge = false"

    anchor_dt = None
    existing_cols: set[str] = set()
    if src:
        try:
            from backend.utils.date_utils import parse_iso_utc

            svc_status = svcconfig.get_status(src.get("name") or src.get("service_id") or "") or {}
            latest_iso = svc_status.get("latest_log_at")
            anchor_dt = parse_iso_utc(latest_iso) if latest_iso else None
            if anchor_dt:
                existing_cols = set(QueryRunner(con, src).get_schema_cols())
        except Exception:
            logger.debug("alerts.preview_alert: anchor_dt lookup failed", exc_info=True)

    def _prune_clause(mins: int) -> str:
        if not anchor_dt or not existing_cols:
            return ""
        clause = ""
        prune_st = (anchor_dt - datetime.timedelta(minutes=mins + 60)).astimezone(datetime.UTC)
        if "dt" in existing_cols:
            clause += f" AND dt >= '{prune_st.strftime('%Y-%m-%d')}'"
        if "timestamp_hour" in existing_cols:
            clause += f" AND timestamp_hour >= '{prune_st.strftime('%Y-%m-%d-%H')}'"
        return clause

    max_ts_prune = _prune_clause(lookback_mins)
    scope_filter = base_scope_filter + max_ts_prune

    q = PREVIEW_SERIES.format(
        group_sql=group_sql,
        agg_sql=agg_sql,
        table=table_name,
        lookback_mins=lookback_mins,
        scope_filter=scope_filter,
    )
    if max_ts_prune:
        q = q.replace(
            f"(SELECT max(timestamp) FROM {table_name})",
            f"(SELECT max(timestamp) FROM {table_name} WHERE 1=1{max_ts_prune})",
        )

    with track_query(con, q, [], "alerts_preview") as cursor:
        rows = cursor.fetchall()

    if not rows and has_status_data():
        with track_query(con, PREVIEW_ANY_ROW.format(table=table_name), [], "alerts_preview_check") as cursor:
            has_any = cursor.fetchone()
        if not has_any:
            from backend.repositories._base import force_rebuild_view

            force_rebuild_view(con, src)
            with track_query(con, q, [], "alerts_preview") as cursor:
                rows = cursor.fetchall()

    times = [safe_iso(r[0]) for r in rows]
    values = [float(r[1] or 0) for r in rows]

    res: dict = {
        "times": times,
        "values": values,
        "type": "anomaly_zscore" if eval_type == "anomaly_zscore" else "absolute",
    }

    if eval_type == "anomaly_zscore":
        days = getattr(alert, "baseline_period_days", 7) or 7
        zscore_thresh = getattr(alert, "zscore_threshold", 3.0) or 3.0

        if not existing_cols:
            try:
                existing_cols = set(QueryRunner(con, src).get_schema_cols())
            except Exception:
                existing_cols = set()

        max_ts_subq = (
            f"(SELECT max(timestamp) FROM {table_name} WHERE 1=1{max_ts_prune})"
            if max_ts_prune
            else f"(SELECT max(timestamp) FROM {table_name})"
        )
        baseline_start_expr = f"{max_ts_subq} - INTERVAL '{days} days'"
        baseline_end_expr = f"{max_ts_subq} - INTERVAL '1 hours'"

        baseline_where = f"timestamp >= {baseline_start_expr} AND timestamp <= {baseline_end_expr} {base_scope_filter}"

        max_ts_dt = anchor_dt
        if not max_ts_dt:
            try:
                with track_query(con, PREVIEW_MAX_TS.format(table=table_name), [], "alerts_preview_max_ts") as cursor:
                    max_ts_row = cursor.fetchone()
                    max_ts_dt = max_ts_row[0] if max_ts_row else None
            except Exception:
                max_ts_dt = None

        if max_ts_dt:
            st_dt = max_ts_dt - datetime.timedelta(days=days)
            st_utc = st_dt.astimezone(datetime.UTC)
            if "dt" in existing_cols:
                baseline_where += f" AND dt >= '{st_utc.strftime('%Y-%m-%d')}'"
            if "timestamp_hour" in existing_cols:
                baseline_where += f" AND timestamp_hour >= '{st_utc.strftime('%Y-%m-%d-%H')}'"

        q_baseline = PREVIEW_BASELINE.format(agg_sql=agg_sql, table=table_name, baseline_where=baseline_where)

        try:
            with track_query(con, q_baseline, [], "alerts_preview_baseline") as cursor:
                baseline_rows = cursor.fetchall()
            baseline_map = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in baseline_rows}
        except Exception as e:
            logger.error(f"Baseline fetch failed: {e}")
            baseline_map = {}

        baseline_means = []
        baseline_thresholds = []

        for r in rows:
            dt = r[0]
            hr = dt.hour if (dt and hasattr(dt, "hour")) else 0
            mean_val, stddev_val = baseline_map.get(hr, (0.0, 0.0))
            stddev_val = stddev_val or 0.0001

            baseline_means.append(mean_val)
            baseline_thresholds.append(mean_val + zscore_thresh * stddev_val)

        res["hist_values"] = baseline_means
        res["threshold_values"] = baseline_thresholds

    if eval_type in ("relative_increase", "relative_decrease") and comp_period:
        res["type"] = "relative"
        hist_scope_filter = base_scope_filter + _prune_clause(lookback_mins + comp_period)
        q_hist = PREVIEW_HISTORY.format(
            group_sql=group_sql,
            agg_sql=agg_sql,
            table=table_name,
            start_minutes=lookback_mins + comp_period,
            comp_period=comp_period,
            scope_filter=hist_scope_filter,
        )
        if max_ts_prune:
            q_hist = q_hist.replace(
                f"(SELECT max(timestamp) FROM {table_name})",
                f"(SELECT max(timestamp) FROM {table_name} WHERE 1=1{max_ts_prune})",
            )
        with track_query(con, q_hist, [], "alerts_preview") as cursor:
            hist_rows = cursor.fetchall()

        hist_map = {}
        for r in hist_rows:
            if r[0]:
                shifted = r[0] + datetime.timedelta(minutes=comp_period)
                hist_map[shifted.isoformat()] = float(r[1] or 0)

        res["hist_values"] = [hist_map.get(t, 0) for t in times]

    return res


def get_cron_logs(
    service_id: str,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
    since_id: int | None = None,
    *,
    skip_total: bool = False,
) -> tuple[int, list[dict]]:
    return metadata_db.get_cron_runs(
        service_id,
        task=task,
        status=status,
        page=page,
        per_page=per_page,
        sort_col=sort_col,
        sort_dir=sort_dir,
        since_id=since_id,
        with_total=since_id is None and not skip_total,
    )


def delete_cron_log(service_id: str, log_id: int) -> None:
    metadata_db.delete_cron_run(service_id, log_id)


def purge_cron_logs(service_id: str, task: str | None = None, days: int | None = None) -> None:
    metadata_db.purge_cron_runs(service_id, task=task, days=days)


def get_views(service_id: str) -> list[dict]:
    return metadata_db.list_views(service_id)


def save_view(view: SavedView) -> dict:
    return metadata_db.save_view(view.service_id, view)


def get_view_by_id(view_id: str, service_id: str) -> dict | None:
    """Return the saved-view row whose id matches ``view_id`` in the given service (or None)."""
    for v in metadata_db.list_views(service_id):
        if v.get("id") == view_id:
            out = dict(v)
            out.setdefault("service_id", service_id)
            return out
    return None


def delete_view(view_id: str, service_id: str) -> dict:
    """Delete the saved-view row in the given service (or report not_found)."""
    res = metadata_db.delete_view(service_id, view_id)
    res["service_id"] = service_id
    return res


_sys.modules.setdefault("backend.repositories.usage", _sys.modules[__name__])
_sys.modules.setdefault("backend.repositories.alerts", _sys.modules[__name__])
_sys.modules.setdefault("backend.repositories._sql.alerts", _sys.modules[__name__])
_repos_pkg = _sys.modules.get("backend.repositories")
if _repos_pkg is not None:
    _repos_pkg.usage = _sys.modules[__name__]  # type: ignore[attr-defined]
    _repos_pkg.alerts = _sys.modules[__name__]  # type: ignore[attr-defined]
