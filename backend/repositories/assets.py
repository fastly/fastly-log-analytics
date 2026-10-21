"""Assets & Shield repository for querying log performance of static assets."""

from __future__ import annotations

import logging
import time as _time

import duckdb

from backend.core.metric_expressions import column_or_default, sum_or_zero
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry
from backend.utils.log_throttle import log_once_per

logger = logging.getLogger(__name__)

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("assets._response_cache", _response_cache)


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
