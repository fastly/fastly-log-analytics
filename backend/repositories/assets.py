"""Assets & Shield repository for querying log performance of static assets."""

from __future__ import annotations

import time as _time

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
)
from backend.repositories.utils.filters import build_where_clause


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

    if not actual_cols:
        return empty_schema_response(
            asset_type_breakdown=[],
            cache_performance=[],
            compression_performance=[],
            large_uncompressed_assets=[],
            low_ttl_assets=[],
            section_timings=section_timings,
            **runner.telemetry(),
        )

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t)

    results = {**runner.telemetry()}

    # Graceful defaults for optional columns
    encoding_expr = (
        "COALESCE(\"resp_header_content_encoding\", '')" if "resp_header_content_encoding" in actual_cols else "''"
    )
    bytes_expr = 'COALESCE(CAST("resp_bytes" AS BIGINT), 0)' if "resp_bytes" in actual_cols else "0"
    status_expr = 'COALESCE(CAST("status" AS INTEGER), 200)' if "status" in actual_cols else "200"
    ttl_expr = 'COALESCE(CAST("ttl" AS DOUBLE), 0.0)' if "ttl" in actual_cols else "0.0"
    cache_expr = "COALESCE(\"cache\", 'MISS')" if "cache" in actual_cols else "'MISS'"
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
            section_timings=section_timings,
            **runner.telemetry(),
        )

    import uuid as _uuid

    temp_table: str | None = f"t_{_uuid.uuid4().hex}"

    create_sql = f"""
        CREATE TEMP TABLE "{temp_table}" AS
        WITH raw_base AS (
            SELECT
                {url_expr} as url,
                {cache_expr} as cache,
                {encoding_expr} as content_encoding,
                {bytes_expr} as bytes,
                {status_expr} as status,
                {ttl_expr} as ttl,
                LOWER({url_expr}) as url_lower
            FROM {table_name}
            WHERE {where_clause}
        )
        SELECT *,
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
        FROM raw_base
        WHERE url IS NOT NULL
    """

    _t = _time.perf_counter()
    if not runner.create_temp_table(create_sql, params):
        temp_table = None
    timer.mark("temp_table_create", _t)

    try:
        _t = _time.perf_counter()
        # Pre-enriched table allows the downstream queries to run directly against the temp table
        cte_base = f"""
            WITH assets_enriched AS (
                SELECT * FROM "{temp_table}"
            )
        """

        # 1. Asset Type Breakdown
        breakdown_q = f"""
            {cte_base}
            SELECT
                asset_type,
                COUNT(*) as requests,
                SUM(bytes) as egress_bytes,
                COALESCE(
                    SUM(CASE WHEN cache IN ('HIT', 'HIT-SYNTHETIC') THEN 1 ELSE 0 END) * 1.0 /
                    NULLIF(SUM(CASE WHEN cache IN ('HIT', 'HIT-SYNTHETIC', 'MISS', 'PASS', 'BYPASS') THEN 1 ELSE 0 END), 0),
                    0.0
                ) as cache_hit_ratio,
                COALESCE(
                    SUM(CASE WHEN is_compressible = 1 AND is_compressed = 1 THEN 1 ELSE 0 END) * 1.0 /
                    NULLIF(SUM(CASE WHEN is_compressible = 1 THEN 1 ELSE 0 END), 0),
                    0.0
                ) as compression_rate
            FROM assets_enriched
            GROUP BY asset_type
            ORDER BY requests DESC
        """
        breakdown_res = runner.execute(breakdown_q).fetchall()
        results["asset_type_breakdown"] = [
            {
                "asset_type": r[0],
                "requests": r[1],
                "egress_bytes": r[2] or 0,
                "cache_hit_ratio": float(r[3] or 0.0),
                "compression_rate": float(r[4] or 0.0),
            }
            for r in breakdown_res
        ]

        # 2. Cache Performance
        cache_q = f"""
            {cte_base}
            SELECT
                asset_type,
                cache as cache_status,
                COUNT(*) as requests,
                SUM(bytes) as bytes
            FROM assets_enriched
            GROUP BY asset_type, cache
            ORDER BY asset_type, requests DESC
        """
        cache_res = runner.execute(cache_q).fetchall()
        results["cache_performance"] = [
            {
                "asset_type": r[0],
                "cache_status": r[1],
                "requests": r[2],
                "bytes": r[3] or 0,
            }
            for r in cache_res
        ]

        # 3. Compression Performance
        comp_q = f"""
            {cte_base}
            SELECT
                asset_type,
                CASE
                    WHEN LOWER(content_encoding) LIKE '%br%' THEN 'br'
                    WHEN LOWER(content_encoding) LIKE '%gzip%' THEN 'gzip'
                    WHEN content_encoding != '' THEN content_encoding
                    ELSE 'uncompressed'
                END as content_encoding,
                COUNT(*) as requests,
                SUM(bytes) as bytes
            FROM assets_enriched
            GROUP BY asset_type, 2
            ORDER BY asset_type, requests DESC
        """
        comp_res = runner.execute(comp_q).fetchall()
        results["compression_performance"] = [
            {
                "asset_type": r[0],
                "content_encoding": r[1],
                "requests": r[2],
                "bytes": r[3] or 0,
            }
            for r in comp_res
        ]

        # 4. Large Uncompressed Assets
        large_q = f"""
            {cte_base}
            SELECT
                url,
                COUNT(*) as requests,
                AVG(bytes) as avg_bytes,
                SUM(bytes) as total_bytes,
                status
            FROM assets_enriched
            WHERE is_compressed = 0 AND is_compressible = 1 AND status NOT IN (304, 204)
            GROUP BY url, status
            ORDER BY total_bytes DESC
            LIMIT 20
        """
        large_res = runner.execute(large_q).fetchall()
        results["large_uncompressed_assets"] = [
            {
                "url": r[0],
                "requests": r[1],
                "avg_bytes": float(r[2] or 0.0),
                "total_bytes": r[3] or 0,
                "status": r[4],
            }
            for r in large_res
        ]

        # 5. Low-TTL Cache Hotspots (Origin Misses)
        low_ttl_q = f"""
            {cte_base}
            SELECT
                url,
                COUNT(*) as requests,
                AVG(ttl) as avg_ttl,
                asset_type
            FROM assets_enriched
            WHERE cache = 'MISS' AND asset_type != 'API/Dynamic'
            GROUP BY url, asset_type
            ORDER BY requests DESC
            LIMIT 20
        """
        low_ttl_res = runner.execute(low_ttl_q).fetchall()
        results["low_ttl_assets"] = [
            {
                "url": r[0],
                "requests": r[1],
                "avg_ttl": float(r[2] or 0.0),
                "asset_type": r[3],
            }
            for r in low_ttl_res
        ]

        timer.mark("queries_execution", _t)

    finally:
        if temp_table:
            try:
                runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
            except Exception:
                pass

    results["section_timings"] = section_timings
    return results
