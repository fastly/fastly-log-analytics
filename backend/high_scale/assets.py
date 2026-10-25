"""High-scale Assets & Shield performance aggregates over ClickHouse ``request_facts``."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from backend.high_scale._common import parse_time_window, request_filter_clauses, visible_fact_clauses
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.models.performance import AssetsAggregatesResponse, AssetsRequest
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("high_scale.assets._response_cache", _response_cache)

_ASSET_TYPE_CASE = """
    CASE
        WHEN url_lower LIKE '%.png' OR url_lower LIKE '%.jpg' OR url_lower LIKE '%.jpeg' OR url_lower LIKE '%.gif' OR url_lower LIKE '%.svg' OR url_lower LIKE '%.webp' OR url_lower LIKE '%.ico' THEN 'Images'
        WHEN url_lower LIKE '%.pdf' OR url_lower LIKE '%.doc' OR url_lower LIKE '%.docx' OR url_lower LIKE '%.xls' OR url_lower LIKE '%.xlsx' OR url_lower LIKE '%.ppt' OR url_lower LIKE '%.pptx' OR url_lower LIKE '%.txt' OR url_lower LIKE '%.csv' OR url_lower LIKE '%.zip' OR url_lower LIKE '%.tar' OR url_lower LIKE '%.gz' OR url_lower LIKE '%.tgz' OR url_lower LIKE '%.rar' OR url_lower LIKE '%.7z' THEN 'Documents'
        WHEN url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css' THEN 'JavaScript/CSS'
        WHEN url_lower LIKE '%.woff' OR url_lower LIKE '%.woff2' OR url_lower LIKE '%.ttf' OR url_lower LIKE '%.otf' OR url_lower LIKE '%.eot' THEN 'Fonts'
        WHEN url_lower LIKE '%.m3u8' OR url_lower LIKE '%.ts' OR url_lower LIKE '%.mp4' OR url_lower LIKE '%.m4s' OR url_lower LIKE '%.webm' THEN 'Video'
        ELSE 'API/Dynamic'
    END
"""


def compute_summary_from_dims(
    dim_rows: list[tuple[str, str, str, int, int, int, int]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Compute breakdown, cache_performance, and compression_performance from grouped dimension rows."""
    by_type_reqs: dict[str, int] = defaultdict(int)
    by_type_bytes: dict[str, int] = defaultdict(int)
    by_type_hit_reqs: dict[str, int] = defaultdict(int)
    by_type_cache_denom: dict[str, int] = defaultdict(int)
    by_type_comp_num: dict[str, int] = defaultdict(int)
    by_type_comp_denom: dict[str, int] = defaultdict(int)

    by_cache: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    by_comp: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])

    for asset_type, cache_status, encoding_norm, is_compressed, is_compressible, reqs, sum_bytes in dim_rows:
        r = int(reqs or 0)
        b = int(sum_bytes or 0)
        by_type_reqs[asset_type] += r
        by_type_bytes[asset_type] += b
        if cache_status in ("HIT", "HIT-SYNTHETIC"):
            by_type_hit_reqs[asset_type] += r
        if cache_status in ("HIT", "HIT-SYNTHETIC", "MISS", "PASS", "BYPASS"):
            by_type_cache_denom[asset_type] += r
        if is_compressible == 1:
            by_type_comp_denom[asset_type] += r
            if is_compressed == 1:
                by_type_comp_num[asset_type] += r

        cache_pair = by_cache[(asset_type, cache_status)]
        cache_pair[0] += r
        cache_pair[1] += b

        comp_pair = by_comp[(asset_type, encoding_norm)]
        comp_pair[0] += r
        comp_pair[1] += b

    asset_type_breakdown = [
        {
            "asset_type": at,
            "requests": by_type_reqs[at],
            "egress_bytes": by_type_bytes[at],
            "cache_hit_ratio": (
                float(by_type_hit_reqs[at]) / float(by_type_cache_denom[at]) if by_type_cache_denom[at] > 0 else 0.0
            ),
            "compression_rate": (
                float(by_type_comp_num[at]) / float(by_type_comp_denom[at]) if by_type_comp_denom[at] > 0 else 0.0
            ),
        }
        for at in sorted(by_type_reqs.keys(), key=lambda k: (-by_type_reqs[k], k))
    ]

    cache_performance = [
        {
            "asset_type": at,
            "cache_status": cs,
            "requests": vals[0],
            "bytes": vals[1],
        }
        for (at, cs), vals in sorted(by_cache.items(), key=lambda item: (item[0][0], -item[1][0], item[0][1]))
    ]

    compression_performance = [
        {
            "asset_type": at,
            "content_encoding": ce,
            "requests": vals[0],
            "bytes": vals[1],
        }
        for (at, ce), vals in sorted(by_comp.items(), key=lambda item: (item[0][0], -item[1][0], item[0][1]))
    ]

    return asset_type_breakdown, cache_performance, compression_performance


def assets_aggregates(
    service: HighScaleService,
    req: AssetsRequest,
    start_time: str | None,
    end_time: str | None,
) -> AssetsAggregatesResponse:
    start_dt, end_dt = parse_time_window(start_time, end_time)

    cache_key = digest_cache_key(
        {
            "svc": service.service_id,
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(req.filters),
        },
        {"name": service.service_id},
    )
    cached = _memo.get_with_telemetry(cache_key)
    if cached is not None:
        return AssetsAggregatesResponse.with_telemetry(**cached)

    filter_clauses, filter_params = request_filter_clauses(req.filters)
    where = " AND ".join(
        [
            *visible_fact_clauses("request"),
            "event_timestamp >= {start_time:DateTime64(3)}",
            "event_timestamp <= {end_time:DateTime64(3)}",
            "url != ''",
            *filter_clauses,
        ]
    )
    params: dict[str, Any] = {
        "service_id": service.service_id,
        "start_time": start_dt,
        "end_time": end_dt,
        **filter_params,
    }

    enriched_cte = f"""
        WITH grouped_raw AS (
            SELECT
                url,
                if(custom_fields['cache'] != '', custom_fields['cache'], 'MISS') AS cache,
                custom_fields['resp_header_content_encoding'] AS content_encoding,
                if(custom_fields['status'] != '', toInt32OrZero(custom_fields['status']), 200) AS status,
                count() AS req_cnt,
                sum(toInt64OrZero(custom_fields['resp_bytes'])) AS sum_bytes,
                sum(coalesce(toFloat64OrNull(custom_fields['ttl']), 0.0)) AS sum_ttl,
                countIf(toFloat64OrNull(custom_fields['ttl']) IS NOT NULL) AS cnt_ttl
            FROM request_facts
            WHERE {where}
            GROUP BY url, cache, content_encoding, status
        ),
        grouped_base AS (
            SELECT
                *,
                lower(url) AS url_lower,
                lower(content_encoding) AS enc_lower
            FROM grouped_raw
        ),
        assets_enriched AS (
            SELECT
                url,
                cache,
                content_encoding,
                status,
                req_cnt,
                sum_bytes,
                sum_ttl,
                cnt_ttl,
                {_ASSET_TYPE_CASE} AS asset_type,
                if(enc_lower IN ('gzip', 'br', 'deflate'), 1, 0) AS is_compressed,
                if(
                    url_lower LIKE '%.js' OR url_lower LIKE '%.mjs' OR url_lower LIKE '%.css'
                    OR url_lower LIKE '%.svg' OR url_lower LIKE '%.html' OR url_lower LIKE '%.json',
                    1,
                    0
                ) AS is_compressible
            FROM grouped_base
        )
    """

    dims_sql = f"""
        /* assets:summary_dims */
        {enriched_cte}
        SELECT
            asset_type,
            cache AS cache_status,
            CASE
                WHEN lower(content_encoding) LIKE '%br%' THEN 'br'
                WHEN lower(content_encoding) LIKE '%gzip%' THEN 'gzip'
                WHEN content_encoding != '' THEN content_encoding
                ELSE 'uncompressed'
            END AS content_encoding_norm,
            is_compressed,
            is_compressible,
            toInt64(sum(req_cnt)) AS requests,
            toInt64(sum(sum_bytes)) AS bytes
        FROM assets_enriched
        GROUP BY asset_type, cache_status, content_encoding_norm, is_compressed, is_compressible
    """

    hotspots_sql = f"""
        /* assets:hotspots */
        {enriched_cte}
        SELECT * FROM (
            SELECT
                'large' AS kind,
                url,
                toInt64(sum(req_cnt)) AS requests,
                toFloat64(sum(sum_bytes)) / nullIf(toFloat64(sum(req_cnt)), 0.0) AS metric_float,
                toInt64(sum(sum_bytes)) AS total_bytes,
                toInt32(status) AS status,
                '' AS asset_type
            FROM assets_enriched
            WHERE is_compressed = 0 AND is_compressible = 1 AND status NOT IN (304, 204)
            GROUP BY url, status
            ORDER BY total_bytes DESC
            LIMIT 20
        )
        UNION ALL
        SELECT * FROM (
            SELECT
                'low_ttl' AS kind,
                url,
                toInt64(sum(req_cnt)) AS requests,
                toFloat64(sum(sum_ttl)) / nullIf(toFloat64(sum(cnt_ttl)), 0.0) AS metric_float,
                toInt64(0) AS total_bytes,
                toInt32(0) AS status,
                asset_type
            FROM assets_enriched
            WHERE cache = 'MISS' AND asset_type != 'API/Dynamic'
            GROUP BY url, asset_type
            ORDER BY requests DESC
            LIMIT 20
        )
    """

    import contextvars
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-assets") as pool:
        fut_dims = pool.submit(contextvars.copy_context().run, lambda: service.client.execute(dims_sql, params))
        fut_hotspots = pool.submit(contextvars.copy_context().run, lambda: service.client.execute(hotspots_sql, params))
        dim_dicts = fut_dims.result()
        hotspot_dicts = fut_hotspots.result()

    dim_tuples = [
        (
            str(d["asset_type"]),
            str(d["cache_status"]),
            str(d["content_encoding_norm"]),
            int(d["is_compressed"] or 0),
            int(d["is_compressible"] or 0),
            int(d["requests"] or 0),
            int(d["bytes"] or 0),
        )
        for d in dim_dicts
    ]
    asset_type_breakdown, cache_performance, compression_performance = compute_summary_from_dims(dim_tuples)

    large_uncompressed_assets: list[dict[str, Any]] = []
    low_ttl_assets: list[dict[str, Any]] = []
    for d in hotspot_dicts:
        if d.get("kind") == "large":
            large_uncompressed_assets.append(
                {
                    "url": str(d["url"]),
                    "requests": int(d["requests"] or 0),
                    "avg_bytes": float(d["metric_float"] or 0.0),
                    "total_bytes": int(d["total_bytes"] or 0),
                    "status": int(d["status"] or 200),
                }
            )
        elif d.get("kind") == "low_ttl":
            low_ttl_assets.append(
                {
                    "url": str(d["url"]),
                    "requests": int(d["requests"] or 0),
                    "avg_ttl": float(d["metric_float"] or 0.0),
                    "asset_type": str(d["asset_type"]),
                }
            )

    payload = {
        "asset_type_breakdown": asset_type_breakdown,
        "cache_performance": cache_performance,
        "compression_performance": compression_performance,
        "large_uncompressed_assets": large_uncompressed_assets,
        "low_ttl_assets": low_ttl_assets,
        "missing_fields": [],
    }
    _memo.put(cache_key, payload)
    return AssetsAggregatesResponse.with_telemetry(**payload)
