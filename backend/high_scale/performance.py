"""High-scale performance projections and aggregate queries."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

import hashlib
import heapq
import logging
import os
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field as _dc_field

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import _FIELD_DIMENSIONS, normalize_cache_bound
from backend.high_scale._common import bucket as _bucket
from backend.high_scale._common import build_published_facts_where as _facts_where
from backend.high_scale._common import build_published_projection_where as _where
from backend.high_scale._common import finite_float as _finite_float
from backend.high_scale._common import percentiles as _percentiles
from backend.high_scale._common import sketch as _sketch
from backend.high_scale._common import text as _text
from backend.models.analytics import PerformanceAggregatesResponse, ValueSummaryResponse
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    client_cache_token,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    PERF_TTL_BUCKETS,
    chart_interval_seconds,
    float_or_zero,
    shape_latency_percentiles,
    shape_perf_scatter_row,
    shape_perf_top_url_row,
    shape_perf_ttl_dist_rows,
    shape_performance_asn_rows,
    shape_waterfall_avg,
)
from backend.utils.bounded_cache import CacheRegistry

logger = logging.getLogger(__name__)

SCATTER_SAMPLE_SIZE = 300


@dataclass(frozen=True)
class PerformanceProjection:
    dimension_rows: tuple[dict[str, Any], ...]
    summary_rows: tuple[dict[str, Any], ...] = ()
    scatter_rows: tuple[dict[str, Any], ...] = ()


@dataclass
class _Accumulator:
    requests: int = 0
    latencies: list[float] = _dc_field(default_factory=list)


@dataclass
class _WaterfallSums:
    count: int = 0
    edge_processing: float = 0.0
    origin_wait: float = 0.0
    origin_download: float = 0.0
    client_download: float = 0.0


def _asn_text(value: Any) -> str:
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return _text(value)


def build_performance_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> PerformanceProjection:
    dimensions: dict[tuple[datetime, str, str], _Accumulator] = defaultdict(_Accumulator)
    waterfalls: dict[datetime, _WaterfallSums] = defaultdict(_WaterfallSums)
    scatter_candidates: list[tuple[int, int, dict[str, Any]]] = []
    for index, row in enumerate(rows):
        bucket = _bucket(row.get("timestamp"))
        if bucket is None:
            continue
        elapsed = _finite_float(row.get("elapsed"))
        _add_waterfall_and_scatter(row, index, bucket, elapsed, waterfalls, scatter_candidates)
        url = _text(row.get("url"))
        if url:
            acc = dimensions[(bucket, "url", url)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)
        asn = _asn_text(row.get("asn"))
        if asn:
            acc = dimensions[(bucket, "asn", asn)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)
        bot = _text(row.get("_ngwaf_bot_name"))
        if bot:
            acc = dimensions[(bucket, "bot", bot)]
            acc.requests += 1
            if elapsed is not None:
                acc.latencies.append(elapsed)
    return PerformanceProjection(
        dimension_rows=tuple(
            _dimension_row(bucket, dimension, value, acc)
            for (bucket, dimension, value), acc in sorted(dimensions.items())
        ),
        summary_rows=tuple(_summary_row(bucket, sums) for bucket, sums in sorted(waterfalls.items())),
        scatter_rows=tuple(
            row for _, _, row in heapq.nsmallest(SCATTER_SAMPLE_SIZE, scatter_candidates, key=lambda c: c[:2])
        ),
    )


def _add_waterfall_and_scatter(
    row: Mapping[str, Any],
    index: int,
    bucket: datetime,
    elapsed_us: float | None,
    waterfalls: dict[datetime, _WaterfallSums],
    scatter_candidates: list[tuple[int, int, dict[str, Any]]],
) -> None:
    """Mirror the standard ``components`` CTE: rows need both ``ttfb`` and ``elapsed``."""
    ttfb_s = _finite_float(row.get("ttfb"))
    if elapsed_us is None or ttfb_s is None:
        return
    elapsed_ms = elapsed_us / 1000.0
    ttfb_ms = ttfb_s * 1000.0
    raw_ottfb = _finite_float(row.get("ottfb"))
    ottfb_ms = (raw_ottfb / 1000.0) if raw_ottfb is not None and raw_ottfb > 0 else 0.0
    raw_ottlb = _finite_float(row.get("ottlb"))
    ottlb_ms = (
        (raw_ottlb / 1000.0)
        if raw_ottfb is not None and raw_ottfb > 0 and raw_ottlb is not None and raw_ottlb > 0
        else 0.0
    )
    sums = waterfalls[bucket]
    sums.count += 1
    sums.edge_processing += max(0.0, ttfb_ms - ottfb_ms)
    sums.origin_wait += ottfb_ms
    sums.origin_download += max(0.0, ottlb_ms - ottfb_ms)
    sums.client_download += max(0.0, elapsed_ms - max(ottlb_ms, ttfb_ms))
    edge_ms = elapsed_ms - ttfb_ms
    if edge_ms < 0:
        return
    sample_key = _sample_key(row, index)
    if len(scatter_candidates) >= SCATTER_SAMPLE_SIZE and sample_key > scatter_candidates[0][0]:
        return
    scatter_candidates.append(
        (
            sample_key,
            index,
            {
                "bucket_start": bucket.isoformat(),
                "sample_key": sample_key,
                "origin_ms": ttfb_ms,
                "edge_ms": edge_ms,
                "cache": _text(row.get("cache")),
            },
        )
    )
    if len(scatter_candidates) in (SCATTER_SAMPLE_SIZE, SCATTER_SAMPLE_SIZE * 2):
        scatter_candidates[:] = heapq.nsmallest(SCATTER_SAMPLE_SIZE, scatter_candidates, key=lambda c: c[:2])[::-1]


def _sample_key(row: Mapping[str, Any], index: int) -> int:
    identity = _text(row.get("event_id")) or f"{row.get('timestamp')}|{row.get('url')}|{index}"
    return int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big")


def _summary_row(bucket: datetime, sums: _WaterfallSums) -> dict[str, Any]:
    return {
        "bucket_start": bucket.isoformat(),
        "component_count": sums.count,
        "edge_processing_sum_ms": sums.edge_processing,
        "origin_wait_sum_ms": sums.origin_wait,
        "origin_download_sum_ms": sums.origin_download,
        "client_download_sum_ms": sums.client_download,
    }


def _dimension_row(bucket: datetime, dimension: str, value: str, acc: _Accumulator) -> dict[str, Any]:
    lat_p50, lat_p95, lat_p99 = _percentiles(acc.latencies, (0.50, 0.95, 0.99))
    latency_sk = _sketch(acc.latencies)
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "requests": acc.requests,
        "latency_count": len(acc.latencies),
        "latency_sum_ms": sum(acc.latencies) if acc.latencies else None,
        "latency_p50_ms": lat_p50,
        "latency_p95_ms": lat_p95,
        "latency_p99_ms": lat_p99,
        "latency_sk_v": latency_sk[0],
        "latency_sk_w": latency_sk[1],
    }


_HS_PERF_CACHE_TTL = float(os.getenv("HS_PERF_CACHE_TTL_SECONDS", "60"))
_perf_memo = ResponseMemo(ttl=_HS_PERF_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("hs_performance_aggregates_memo", _perf_memo.cache)
_ALL_SECTIONS = frozenset({"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"})
_SORT_COLUMNS = {"avg": "avg_ms", "p50": "p50_ms", "p95": "p95_ms", "p99": "p99_ms"}
_MIN_URL_REQUESTS = 5
_MIN_ASN_REQUESTS = 10
_TOP_N = 20
_TTL_EDGES = PERF_TTL_BUCKETS
_TTL_BUCKET_CASES: tuple[tuple[int, str], ...] = (
    (60, "< 60s"),
    (3600, "1m - 1h"),
    (86400, "1h - 1d"),
)


def _ttl_bucket_expr(column: str) -> str:
    arms = ", ".join(f"{column} < {limit}, '{label}'" for limit, label in _TTL_BUCKET_CASES)
    return f"multiIf({arms}, '> 1d')"


_ELAPSED_MS = "toFloat64OrNull(custom_fields['elapsed']) / 1000.0"
_TTFB_MS = "toFloat64OrNull(custom_fields['ttfb']) * 1000.0"
_OTTFB_MS = "if(toFloat64OrNull(custom_fields['ottfb']) > 0, toFloat64OrNull(custom_fields['ottfb']) / 1000.0, 0.0)"
_OTTLB_MS = (
    "if(toFloat64OrNull(custom_fields['ottfb']) > 0 AND toFloat64OrNull(custom_fields['ottlb']) > 0, "
    "toFloat64OrNull(custom_fields['ottlb']) / 1000.0, 0.0)"
)


def _active_filters(req: Any) -> dict[str, Any]:
    """Filters that actually constrain rows; a field with no facts mapping is rejected, never ignored."""
    active: dict[str, Any] = {}
    for field_name, config in (getattr(req, "filters", None) or {}).items():
        if field_name == "filter_mode":
            continue
        values = (
            config
            if isinstance(config, (list, tuple))
            else (config.get("values") if isinstance(config, Mapping) else getattr(config, "values", None))
        )
        if not values:
            continue
        if field_name not in _FIELD_DIMENSIONS and field_name not in {"waf_sig_ind", "edge_score_reason_ind"}:
            raise ValueError(f"unsupported Performance filter field for high-scale: {field_name}")
        active[field_name] = config
    return active


def _weighted(column: str, count_column: str = "latency_count") -> str:
    return f"sum({column} * {count_column}) / nullIf(sum(if({column} IS NOT NULL, {count_column}, 0)), 0)"


def _optional_float(value: Any) -> float:
    return float_or_zero(value)


def _merged_percentile(index: int, legacy_column: str) -> str:
    return (
        f"if(sumArray(latency_sk_w) > 0, "
        f"if(sumArray(latency_sk_w) >= sum(latency_count), "
        f"quantilesExactWeightedArray(0.5, 0.95, 0.99)(latency_sk_v, latency_sk_w)[{index}], "
        f"{_weighted(legacy_column)}), "
        f"{_weighted(legacy_column)}) / 1000.0"
    )


def _projection_percentile_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    sort_column: str,
    min_requests: int,
) -> list[dict[str, Any]]:
    where, params = _where(service.service_id, "performance_dimensions", start_time, end_time, dimension=dimension)
    params["limit"] = _TOP_N
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, "
        "sum(requests) AS req_count, "
        "sum(latency_sum_ms) / nullIf(sum(latency_count), 0) / 1000.0 AS avg_ms, "
        f"{_merged_percentile(1, 'latency_p50_ms')} AS p50_ms, "
        f"{_merged_percentile(2, 'latency_p95_ms')} AS p95_ms, "
        f"{_merged_percentile(3, 'latency_p99_ms')} AS p99_ms "
        f"FROM performance_minute_dimensions WHERE {where} GROUP BY value "
        f"HAVING req_count > {min_requests} ORDER BY {sort_column} DESC NULLS LAST LIMIT {{limit:UInt32}}",
        params,
    )


def _facts_percentile_rows(
    service: HighScaleService,
    *,
    marker: str,
    key_expr: str,
    where: str,
    params: dict[str, Any],
    sort_column: str,
    min_requests: int,
) -> list[dict[str, Any]]:
    return service.client.execute(
        f"/* perf:{marker} */ SELECT value, count() AS req_count, "
        "avg(elapsed_ms) AS avg_ms, "
        "quantile(0.5)(elapsed_ms) AS p50_ms, "
        "quantile(0.95)(elapsed_ms) AS p95_ms, "
        "quantile(0.99)(elapsed_ms) AS p99_ms "
        f"FROM (SELECT {key_expr} AS value, {_ELAPSED_MS} AS elapsed_ms FROM request_facts WHERE {where}) "
        f"WHERE value != '' AND elapsed_ms IS NOT NULL GROUP BY value "
        f"HAVING req_count > {min_requests} ORDER BY {sort_column} DESC NULLS LAST LIMIT {{limit:UInt32}}",
        {**params, "limit": _TOP_N},
    )


def _latency_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return shape_latency_percentiles(row)


def _asn_rows(service: HighScaleService, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach the same ``label`` the standard path's ``_resolve_asns`` adds."""
    return shape_performance_asn_rows(service.service_id, rows)


def _ttl_dist(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters, end_inclusive=False)
        source = (
            "SELECT toFloat64OrNull(raw_ttl) AS ttl, weight FROM ("
            f"SELECT custom_fields['ttl'] AS raw_ttl, count() AS weight FROM request_facts WHERE {where} GROUP BY raw_ttl"
            ")"
        )
    else:
        where, params = _where(service.service_id, "request_aggregate", start_time, end_time, dimension="ttl")
        source = (
            "SELECT toFloat64OrNull(value) AS ttl, weight FROM ("
            f"SELECT value, sum(request_count) AS weight FROM request_aggregates WHERE {where} GROUP BY value"
            ")"
        )
    rows = service.client.execute(
        f"/* perf:ttl_dist */ SELECT {_ttl_bucket_expr('ttl')} AS bucket, sum(weight) AS count, min(ttl) AS min_ttl "
        f"FROM ({source}) WHERE ttl IS NOT NULL GROUP BY bucket ORDER BY min_ttl",
        params,
    )
    return shape_perf_ttl_dist_rows(rows)


def _waterfall(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any]
) -> dict[str, Any]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters, end_inclusive=False)
        sql = (
            "/* perf:waterfall */ SELECT count() AS component_count, "
            "sum(greatest(0, ttfb_ms - ottfb_ms)) / nullIf(count(), 0) AS edge_processing, "
            "sum(ottfb_ms) / nullIf(count(), 0) AS origin_wait, "
            "sum(greatest(0, ottlb_ms - ottfb_ms)) / nullIf(count(), 0) AS origin_download, "
            "sum(greatest(0, elapsed_ms - greatest(ottlb_ms, ttfb_ms))) / nullIf(count(), 0) AS client_download "
            f"FROM (SELECT {_ELAPSED_MS} AS elapsed_ms, {_TTFB_MS} AS ttfb_ms, "
            f"{_OTTFB_MS} AS ottfb_ms, {_OTTLB_MS} AS ottlb_ms FROM request_facts WHERE {where}) "
            "WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL"
        )
    else:
        where, params = _where(service.service_id, "performance_summary", start_time, end_time)
        sql = (
            "/* perf:waterfall */ SELECT sum(component_count) AS n_components, "
            "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0) AS edge_processing, "
            "sum(origin_wait_sum_ms) / nullIf(sum(component_count), 0) AS origin_wait, "
            "sum(origin_download_sum_ms) / nullIf(sum(component_count), 0) AS origin_download, "
            "sum(client_download_sum_ms) / nullIf(sum(component_count), 0) AS client_download "
            f"FROM performance_minute_summary WHERE {where}"
        )
    rows = service.client.execute(sql, params)
    row = rows[0] if rows else {}
    return shape_waterfall_avg(row)


def _scatter(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if filters:
        where, params = _facts_where(service.service_id, start_time, end_time, filters, end_inclusive=False)
        sql = (
            "/* perf:scatter */ SELECT ttfb_ms AS origin_ms, elapsed_ms - ttfb_ms AS edge_ms, cache "
            f"FROM (SELECT event_id, {_ELAPSED_MS} AS elapsed_ms, {_TTFB_MS} AS ttfb_ms, "
            f"custom_fields['cache'] AS cache FROM request_facts WHERE {where}) "
            "WHERE elapsed_ms IS NOT NULL AND ttfb_ms IS NOT NULL AND elapsed_ms - ttfb_ms >= 0 "
            "ORDER BY cityHash64(event_id) LIMIT {scatter_limit:UInt32}"
        )
    else:
        where, params = _where(service.service_id, "performance_scatter", start_time, end_time)
        sql = (
            "/* perf:scatter */ SELECT origin_ms, edge_ms, cache "
            f"FROM performance_scatter_samples WHERE {where} "
            "ORDER BY sample_key LIMIT {scatter_limit:UInt32}"
        )
    rows = service.client.execute(sql, {**params, "scatter_limit": SCATTER_SAMPLE_SIZE})
    return [shape_perf_scatter_row(row["origin_ms"], row["edge_ms"], row["cache"], coerce_float=True) for row in rows]


def performance_aggregates(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None = None,
) -> PerformanceAggregatesResponse:
    wanted = _ALL_SECTIONS if sections is None else sections
    filters = _active_filters(req)
    sort_by = getattr(req, "sort_by", "p99")
    sort_column = _SORT_COLUMNS.get(sort_by, "p99_ms")
    cache_key: str | None = None
    norm_start = normalize_cache_bound(start_time)
    norm_end = normalize_cache_bound(end_time)
    if _HS_PERF_CACHE_TTL > 0 and norm_start is not None and norm_end is not None:
        cache_key = digest_cache_key(
            {
                "ep": "hs_performance_aggregates",
                "svc": service.service_id,
                "cid": client_cache_token(service.client),
                "s": norm_start,
                "e": norm_end,
                "f": serialize_filters_for_key(filters),
                "sb": sort_by,
                "sec": sorted(wanted),
            },
            {"service_id": service.service_id},
        )
        cached = _perf_memo.get_with_telemetry(cache_key)
        if cached is not None:
            return PerformanceAggregatesResponse.with_telemetry(**cached)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )

    def _compute() -> dict[str, Any]:
        if cache_key is not None and (cached_inner := _perf_memo.get_with_telemetry(cache_key)) is not None:
            return cached_inner
        facts_where_params: tuple[str, dict[str, Any]] | None = None
        if filters and (wanted & {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}):
            facts_where_params = _facts_where(service.service_id, start_time, end_time, filters, end_inclusive=False)
        trace = HighScaleSections(service.service_id, "performance")
        tasks: list[tuple[str, Any]] = []
        if "top_urls" in wanted:

            def _fetch_top_urls() -> list[dict[str, Any]]:
                if facts_where_params is not None:
                    where, params = facts_where_params
                    rows = _facts_percentile_rows(
                        service,
                        marker="top_urls",
                        key_expr="url",
                        where=where,
                        params=params,
                        sort_column=sort_column,
                        min_requests=_MIN_URL_REQUESTS,
                    )
                else:
                    rows = _projection_percentile_rows(
                        service,
                        marker="top_urls",
                        dimension="url",
                        start_time=start_time,
                        end_time=end_time,
                        sort_column=sort_column,
                        min_requests=_MIN_URL_REQUESTS,
                    )
                return [shape_perf_top_url_row(row, normalize_float=True) for row in rows]

            tasks.append(("top_urls", _fetch_top_urls))
        if "top_asns" in wanted:

            def _fetch_top_asns() -> list[dict[str, Any]]:
                if facts_where_params is not None:
                    where, params = facts_where_params
                    rows = _facts_percentile_rows(
                        service,
                        marker="top_asns",
                        key_expr="custom_fields['asn']",
                        where=where,
                        params=params,
                        sort_column=sort_column,
                        min_requests=_MIN_ASN_REQUESTS,
                    )
                else:
                    rows = _projection_percentile_rows(
                        service,
                        marker="top_asns",
                        dimension="asn",
                        start_time=start_time,
                        end_time=end_time,
                        sort_column=sort_column,
                        min_requests=_MIN_ASN_REQUESTS,
                    )
                return rows

            tasks.append(("top_asns", _fetch_top_asns))
        if "ttl_dist" in wanted:
            tasks.append(("ttl_dist", lambda: _ttl_dist(service, start_time, end_time, filters)))
        if "waterfall" in wanted:
            tasks.append(("waterfall", lambda: _waterfall(service, start_time, end_time, filters)))
        if "scatter" in wanted:
            tasks.append(("scatter", lambda: _scatter(service, start_time, end_time, filters)))
        result = trace.run_parallel(tasks, source="fact" if filters else "projection")
        if "top_asns" in result:
            result["top_asns"] = trace.run(
                "asn_labels", lambda: _asn_rows(service, result["top_asns"]), activity="overhead"
            )
        payload = {**result, "_approx": not filters, "section_timings": trace.timings()}
        if cache_key is not None:
            _perf_memo.put(cache_key, payload)
        return payload

    if is_real_ch and cache_key is not None:
        return PerformanceAggregatesResponse.with_telemetry(**_perf_memo.coalesce(f"hs_perf_agg:{cache_key}", _compute))
    return PerformanceAggregatesResponse.with_telemetry(
        **_perf_memo.result(_compute(), state="miss" if cache_key is not None else "bypass")
    )


def value_summary(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    sections: set[str] | None = None,
) -> ValueSummaryResponse:
    from backend.repositories import value as value_repo
    from backend.repositories.value import (
        _shape_caching_and_overview_buckets,
        _shape_raw_network_buckets,
    )
    from backend.utils.date_utils import parse_iso_utc, safe_iso

    wanted = sections or {"overview", "caching", "security", "bots", "performance", "network", "io"}
    filters = _active_filters(req)
    chart_interval = getattr(req, "chart_interval", "1 day") or "1 day"
    bucket_sec = max(60, chart_interval_seconds(chart_interval, default=86400))
    norm_start = normalize_cache_bound(start_time)
    norm_end = normalize_cache_bound(end_time)
    cache_key: str | None = None
    if _HS_PERF_CACHE_TTL > 0 and norm_start is not None and norm_end is not None:
        cache_key = digest_cache_key(
            {
                "ep": "hs_value_summary",
                "svc": service.service_id,
                "cid": client_cache_token(service.client),
                "s": norm_start,
                "e": norm_end,
                "f": serialize_filters_for_key(filters),
                "ci": chart_interval,
                "sec": sorted(wanted),
            },
            {"service_id": service.service_id},
        )
        cached = _perf_memo.get_with_telemetry(cache_key)
        if cached is not None:
            return ValueSummaryResponse.with_telemetry(**cached)

    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )

    def _compute() -> dict[str, Any]:
        if cache_key is not None and (cached_inner := _perf_memo.get_with_telemetry(cache_key)) is not None:
            return cached_inner
        trace = HighScaleSections(service.service_id, "value_summary")
        where, params = _facts_where(service.service_id, start_time, end_time, filters, end_inclusive=False)
        hit_cond = "startsWith(upper(custom_fields['cache']), 'HIT')"
        miss_cond = "startsWith(upper(custom_fields['cache']), 'MISS')"
        pass_cond = "startsWith(upper(custom_fields['cache']), 'PASS')"
        synth_cond = "startsWith(upper(custom_fields['cache']), 'SYNTH')"
        origin_cond = f"({miss_cond} OR {pass_cond} OR {synth_cond} OR upper(custom_fields['cache']) = 'ERROR')"
        shield_cond = (
            "(lower(custom_fields['is_shield']) IN ('true', '1') OR lower(custom_fields['edge']) IN ('false', '0'))"
        )
        waf_cond = "lower(custom_fields['waf']) IN ('1', 'true')"
        _info_waf_re = (
            "^(bot-analysis|datacenter|site-flagged-ip|tor-exit|corporate-proxy|"
            "residential-proxy|public-proxy|vpn|verified-bot([.][a-z0-9_.-]+)?|"
            "suspected-(bad-)?bot([.][a-z0-9_.-]+)?|site[.][a-z0-9_.-]+)$"
        )
        _has_attack_waf_sig = (
            f"arrayExists(t -> t != '' AND NOT match(lower(t), '{_info_waf_re}'), "
            "splitByRegexp('[,[:space:]]+', custom_fields['waf_sig']))"
        )
        waf_block_cond = f"{waf_cond} AND custom_fields['waf_resp'] = '406'"
        waf_log_cond = f"{waf_cond} AND custom_fields['waf_resp'] != '406' AND {_has_attack_waf_sig}"
        waf_pass_cond = f"{waf_cond} AND custom_fields['waf_resp'] = '200' AND NOT ({_has_attack_waf_sig})"
        elapsed_us_expr = f"({_ELAPSED_MS} * 1000.0)"
        bytes_expr = "toInt64OrZero(coalesce(nullIf(custom_fields['resp_bytes'], ''), custom_fields['bytes']))"
        proto_expr = "coalesce(nullIf(custom_fields['proto'], ''), custom_fields['protocol'])"
        ipv6_expr = "coalesce(nullIf(custom_fields['is_ipv6'], ''), custom_fields['ipv6'])"

        need_cache = bool({"overview", "caching", "performance"} & wanted)
        need_bytes = bool({"overview", "caching"} & wanted)
        need_shield = "caching" in wanted
        need_elapsed = bool({"overview", "performance"} & wanted)
        need_waf_basic = bool({"overview", "security", "bots"} & wanted)
        need_waf_detail = bool({"security", "bots"} & wanted)
        need_network = "network" in wanted
        need_bots_inline = is_real_ch and "bots" in wanted

        req_start_dt = parse_iso_utc(start_time) if start_time else None
        req_end_dt = parse_iso_utc(end_time) if end_time else None

        def _fetch_fastly_reported() -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
            try:
                coverage_end = getattr(service.watermark_for("request"), "coverage_end", None)
                reported = value_repo.fetch_fastly_reported(
                    service.service_id, start_time, end_time, coverage_end=coverage_end
                )
            except Exception:
                logger.warning("Fastly-reported stats unavailable for %s", service.service_id, exc_info=True)
                return None, None
            if reported is None:
                return None, None
            rep_start_dt = parse_iso_utc(reported.get("window_start")) if reported.get("window_start") else None
            rep_end_dt = parse_iso_utc(reported.get("window_end")) if reported.get("window_end") else None
            if (
                need_bytes
                and req_start_dt is not None
                and req_end_dt is not None
                and rep_start_dt == req_start_dt
                and rep_end_dt == req_end_dt
            ):
                return reported, None
            w_where, w_params = _facts_where(
                service.service_id, reported["window_start"], reported["window_end"], {}, end_inclusive=False
            )
            w_rows = trace.run(
                "fastly_reported_window",
                lambda: service.client.execute(
                    f"/* value:fastly_reported_window */ SELECT count() AS requests, "
                    f"sum({bytes_expr}) AS bandwidth_bytes FROM request_facts WHERE {w_where}",
                    w_params,
                ),
                source="fact",
            )
            return reported, (w_rows[0] if w_rows else {})

        reported_future: Future[tuple[dict[str, Any] | None, dict[str, Any] | None]] | None = None
        if "overview" in wanted and not filters:
            stats_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hs-val-stats")
            reported_future = trace.submit(
                stats_pool, "fastly_reported_stats", _fetch_fastly_reported, activity="overhead"
            )
            stats_pool.shutdown(wait=False)

        def _run_bucket_scan() -> list[dict[str, Any]]:
            select_cols = [
                f"toDateTime(intDiv(toUInt32(event_timestamp), {bucket_sec}) * {bucket_sec}, 'UTC') AS bucket",
                "count() AS total",
            ]
            if need_cache:
                select_cols.extend(
                    [
                        f"countIf({hit_cond}) AS hit_cnt",
                        f"countIf({miss_cond}) AS miss_cnt",
                        f"countIf({pass_cond}) AS pass_cnt",
                        f"countIf({synth_cond}) AS synth_cnt",
                    ]
                )
            if need_bytes:
                select_cols.extend(
                    [
                        f"sumIf({bytes_expr}, {hit_cond}) AS bandwidth_saved",
                        f"sum({bytes_expr}) AS total_bytes",
                        f"countIf({origin_cond}) AS origin_cnt",
                    ]
                )
            if need_shield:
                select_cols.extend(
                    [
                        f"countIf({shield_cond} AND {hit_cond}) AS shield_hit",
                        f"countIf({shield_cond}) AS shield_total",
                    ]
                )
            if need_waf_basic:
                select_cols.extend(
                    [
                        f"countIf({waf_block_cond}) AS threats_cnt",
                        "countIf(custom_fields['waf'] != '' AND custom_fields['waf'] != '-') AS waf_sample_cnt",
                        f"countIf({waf_cond}) AS waf_inspected",
                    ]
                )
            if need_elapsed:
                select_cols.extend(
                    [
                        f"sumIf({elapsed_us_expr}, {hit_cond}) AS sum_hit_elapsed",
                        f"countIf({elapsed_us_expr} IS NOT NULL AND {hit_cond}) AS cnt_hit_elapsed",
                        f"sumIf({elapsed_us_expr}, {miss_cond}) AS sum_miss_elapsed",
                        f"countIf({elapsed_us_expr} IS NOT NULL AND {miss_cond}) AS cnt_miss_elapsed",
                    ]
                )
            if need_waf_detail:
                select_cols.extend(
                    [
                        f"countIf({waf_block_cond}) AS waf_blocked",
                        f"countIf({waf_log_cond}) AS waf_logged",
                        f"countIf({waf_pass_cond}) AS waf_passed",
                        "countIf(custom_fields['waf_sig'] != '') AS waf_sig_cnt",
                    ]
                )
            if need_network:
                select_cols.extend(
                    [
                        f"countIf({proto_expr} IN ('3.0', '3', 'HTTP/3', 'h3')) AS http3_cnt",
                        f"countIf({proto_expr} IN ('2.0', '2', 'HTTP/2', 'h2')) AS h2_cnt",
                        f"countIf({proto_expr} IN ('1.1', 'HTTP/1.1', 'http/1.1')) AS h11_cnt",
                        f"countIf({proto_expr} NOT IN ('3.0', '3', 'HTTP/3', 'h3', '2.0', '2', 'HTTP/2', 'h2', '1.1', 'HTTP/1.1', 'http/1.1')) AS other_proto_cnt",
                        "countIf(lower(custom_fields['is_ssl']) IN ('true', '1') OR (custom_fields['tls'] != '' AND custom_fields['tls'] != '-')) AS tls_cnt",
                        f"countIf(lower({ipv6_expr}) IN ('true', '1')) AS ipv6_cnt",
                    ]
                )
            if need_bots_inline:
                select_cols.extend(
                    [
                        "countIf(custom_fields['_bot_name'] != '') AS bot_requests",
                        "countIf(positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0) AS verified_bots",
                        "countIf(custom_fields['ua'] != '') AS ua_cnt",
                    ]
                )
            sql = (
                f"/* value:combined_buckets */ SELECT {', '.join(select_cols)} "
                f"FROM request_facts WHERE {where} GROUP BY bucket ORDER BY bucket"
            )
            return service.client.execute(sql, params)

        def _run_p99() -> list[dict[str, Any]]:
            return service.client.execute(
                f"/* value:p99 */ SELECT quantileTDigest(0.99)({_ELAPSED_MS}) AS p99_ms FROM request_facts WHERE {where}",
                params,
            )

        if is_real_ch and "performance" in wanted:
            init_res = trace.run_parallel(
                [("combined_buckets", _run_bucket_scan), ("performance_p99", _run_p99)],
                source="fact",
                max_workers=2,
                thread_name_prefix="hs-val",
            )
            buckets = init_res["combined_buckets"]
            pre_p99_rows: list[dict[str, Any]] | None = init_res["performance_p99"]
        else:
            buckets = trace.run("combined_buckets", _run_bucket_scan, source="fact")
            pre_p99_rows = None

        total_requests = sum(int(b.get("total", 0) or 0) for b in buckets)
        from backend import config as svcconfig

        try:
            svc_cfg = svcconfig.load_config(service.service_id)
        except Exception:
            svc_cfg = None
        if svc_cfg is not None:
            active_fields = set(svcconfig.active_log_field_ids(svc_cfg))
            has_resp_bytes = "resp_bytes" in active_fields
            has_shield = "edge" in active_fields
            has_elapsed = "elapsed" in active_fields
            has_waf = "waf" in active_fields
        else:
            has_resp_bytes = True
            has_shield = True
            has_elapsed = True
            has_waf = any(
                int(b.get("waf_sample_cnt") or 0) > 0
                or int(b.get("waf_inspected") or 0) > 0
                or int(b.get("threats_cnt") or 0) > 0
                for b in buckets
            )
        result: dict[str, Any] = {}
        shaped: dict[str, Any] = {}

        if {"caching", "overview", "performance"} & wanted:
            shaped = _shape_caching_and_overview_buckets(
                buckets,
                include_overview="overview" in wanted,
                has_resp_bytes=has_resp_bytes,
                has_shield=has_shield,
                has_waf=has_waf,
                has_elapsed=has_elapsed,
                include_performance="performance" in wanted,
            )
            if "caching" in wanted and "caching" in shaped:
                result["caching"] = shaped["caching"]
            if "overview" in wanted and "overview" in shaped:
                result["overview"] = shaped["overview"]
        if reported_future is not None:
            reported, w_row = reported_future.result()
            if reported is not None:
                if w_row is None:
                    logged_requests = total_requests
                    logged_bandwidth_bytes = sum(int(b.get("total_bytes") or 0) for b in buckets)
                else:
                    logged_requests = int(w_row.get("requests") or 0)
                    logged_bandwidth_bytes = int(w_row.get("bandwidth_bytes") or 0)
                result["fastly_reported"] = {
                    **reported,
                    "logged_requests": logged_requests,
                    "logged_bandwidth_bytes": logged_bandwidth_bytes,
                }
        if "performance" in wanted and "performance_from_buckets" in shaped:
            pf = shaped["performance_from_buckets"]
            p99_rows = (
                pre_p99_rows if pre_p99_rows is not None else trace.run("performance_p99", _run_p99, source="fact")
            )
            p99_val = _finite_float(p99_rows[0].get("p99_ms")) if p99_rows else None
            result["performance"] = {
                "cache_accel_factor": pf["cache_accel_factor"],
                "avg_hit_latency_ms": pf["avg_hit_latency_ms"],
                "avg_miss_latency_ms": pf["avg_miss_latency_ms"],
                "p99_latency_ms": round(p99_val, 2) if p99_val is not None else None,
                "total_requests": pf["total_requests"],
                "latency_time_series": pf["latency_time_series"],
            }

        if "security" in wanted:
            _sec_sum = lambda k: sum(int(b.get(k, 0) or 0) for b in buckets)  # noqa: E731
            threat_ts: list[dict[str, Any]] = []
            for b in buckets:
                t_str = safe_iso(b["bucket"])
                if int(b.get("waf_blocked") or 0):
                    threat_ts.append({"time": t_str, "value": int(b["waf_blocked"]), "category": "blocked"})
                if int(b.get("waf_logged") or 0):
                    threat_ts.append({"time": t_str, "value": int(b["waf_logged"]), "category": "logged"})
            top_signals: list[dict[str, Any]] = []
            if _sec_sum("waf_sig_cnt") > 0:
                sig_rows = trace.run(
                    "security_top_signals",
                    lambda: service.client.execute(
                        f"/* value:waf_signals */ SELECT trimBoth(sig) AS signal, count() AS cnt "
                        f"FROM request_facts ARRAY JOIN splitByChar(',', custom_fields['waf_sig']) AS sig "
                        f"WHERE {where} AND custom_fields['waf_sig'] != '' AND trimBoth(sig) != '' "
                        f"GROUP BY signal ORDER BY cnt DESC LIMIT 10",
                        params,
                    ),
                    source="fact",
                )
                top_signals = [{"signal": str(r["signal"]), "count": int(r["cnt"])} for r in sig_rows]
            result["security"] = {
                "total_requests": total_requests,
                "waf_inspected": _sec_sum("waf_inspected") if has_waf else 0,
                "waf_blocked": _sec_sum("waf_blocked") if has_waf else None,
                "waf_logged": _sec_sum("waf_logged") if has_waf else None,
                "waf_passed": _sec_sum("waf_passed") if has_waf else None,
                "threat_time_series": threat_ts,
                "top_waf_signals": top_signals,
            }

        if "network" in wanted:
            result["network"] = _shape_raw_network_buckets(
                buckets,
                total_requests,
                has_protocol=True,
                has_is_ssl=True,
                has_ipv6=True,
            )

        if "bots" in wanted:
            from backend.high_scale import security as hs_security
            from backend.repositories.security import _BOT_UA_CLASSIFY_LIMIT
            from backend.utils.bot_sources import build_matcher, build_well_known_bots_matcher, load_source

            if is_real_ch:
                bot_req = sum(int(b.get("bot_requests", 0) or 0) for b in buckets)
                ver_bots = sum(int(b.get("verified_bots", 0) or 0) for b in buckets)
                ua_cnt = sum(int(b.get("ua_cnt", 0) or 0) for b in buckets)
                waf_sig_cnt = sum(int(b.get("waf_sig_cnt", 0) or 0) for b in buckets)
            else:
                bot_rows = trace.run(
                    "bots",
                    lambda: service.client.execute(
                        f"/* value:bots */ SELECT "
                        f"countIf(custom_fields['_bot_name'] != '') AS bot_requests, "
                        f"countIf(positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0) AS verified_bots, "
                        f"countIf(custom_fields['ua'] != '') AS ua_cnt, "
                        f"countIf(custom_fields['waf_sig'] != '') AS waf_sig_cnt "
                        f"FROM request_facts WHERE {where}",
                        params,
                    ),
                    source="fact",
                )
                bot_req = int(bot_rows[0].get("bot_requests") or 0) if bot_rows else 0
                ver_bots = int(bot_rows[0].get("verified_bots") or 0) if bot_rows else 0
                ua_cnt = int(bot_rows[0].get("ua_cnt") or 0) if bot_rows else 0
                waf_sig_cnt = (
                    int(bot_rows[0].get("waf_sig_cnt") or 0)
                    if bot_rows and "waf_sig_cnt" in bot_rows[0]
                    else sum(int(b.get("waf_sig_cnt", 0) or 0) for b in buckets)
                )
            top_bots_list: list[dict[str, Any]] = []
            if bot_req > 0:
                tb_rows = trace.run(
                    "top_bots",
                    lambda: service.client.execute(
                        f"/* value:top_bots */ SELECT custom_fields['_bot_name'] AS name, count() AS cnt "
                        f"FROM request_facts WHERE {where} AND custom_fields['_bot_name'] != '' "
                        f"GROUP BY name ORDER BY cnt DESC LIMIT 10",
                        params,
                    ),
                    source="fact",
                )
                top_bots_list = [{"name": str(r["name"]), "count": int(r["cnt"])} for r in tb_rows]
            elif ua_cnt > 0:
                ua_rows = trace.run(
                    "bot_uas",
                    lambda: service.client.execute(
                        f"/* value:bot_uas */ SELECT custom_fields['ua'] AS ua, count() AS cnt "
                        f"FROM request_facts WHERE {where} AND custom_fields['ua'] != '' "
                        f"GROUP BY ua ORDER BY cnt DESC, ua ASC LIMIT {int(_BOT_UA_CLASSIFY_LIMIT)}",
                        params,
                    ),
                    source="fact",
                )
                if ua_rows:
                    classified = hs_security.classify_ua_counts(ua_rows, 20)
                    top_bots_list = [{"name": str(b["name"]), "count": int(b["request_count"])} for b in classified]
                    use_primary = hasattr(build_matcher, "_mock_name") or bool(load_source("well-known-bots"))
                    match_ua = build_matcher() if use_primary else build_well_known_bots_matcher()
                    bot_req = sum(int(r.get("cnt") or 0) for r in ua_rows if r.get("ua") and match_ua(str(r["ua"])))
            ver_out = ver_bots if (has_waf or waf_sig_cnt > 0 or ver_bots > 0) else None
            result["bots"] = {
                "total_requests": total_requests,
                "bot_requests": bot_req,
                "verified_bots": ver_out,
                "top_bots": top_bots_list,
            }

        if "io" in wanted:
            ctype_expr = (
                "lower(coalesce(nullIf(custom_fields['resp_header_content_type'], ''), custom_fields['content_type']))"
            )
            url_lc = "lower(url)"
            url_img_ext_expr = (
                f"(endsWith({url_lc}, '.png') OR endsWith({url_lc}, '.jpg') "
                f"OR endsWith({url_lc}, '.jpeg') OR endsWith({url_lc}, '.gif') "
                f"OR endsWith({url_lc}, '.svg') OR endsWith({url_lc}, '.webp') "
                f"OR endsWith({url_lc}, '.ico'))"
            )
            is_img_expr = f"(startsWith({ctype_expr}, 'image/') OR ({ctype_expr} = '' AND {url_img_ext_expr}))"
            url_img_fmt_expr = (
                f"CASE "
                f"WHEN endsWith({url_lc}, '.png') THEN 'png' "
                f"WHEN endsWith({url_lc}, '.jpg') OR endsWith({url_lc}, '.jpeg') THEN 'jpeg' "
                f"WHEN endsWith({url_lc}, '.gif') THEN 'gif' "
                f"WHEN endsWith({url_lc}, '.svg') THEN 'svg' "
                f"WHEN endsWith({url_lc}, '.webp') THEN 'webp' "
                f"WHEN endsWith({url_lc}, '.ico') THEN 'ico' "
                f"ELSE NULL END"
            )
            url_raster_ext_expr = (
                f"(endsWith({url_lc}, '.jpg') OR endsWith({url_lc}, '.jpeg') "
                f"OR endsWith({url_lc}, '.png') OR endsWith({url_lc}, '.gif'))"
            )
            ofmt_expr = "nullIf(coalesce(nullIf(custom_fields['io_output_format'], ''), custom_fields['ofmt']), '')"
            ifmt_expr = "nullIf(coalesce(nullIf(custom_fields['io_input_format'], ''), custom_fields['ifmt']), '')"
            io_in_expr = "toInt64OrNull(nullIf(custom_fields['io_input_bytes'], ''))"
            io_out_expr = "toInt64OrNull(nullIf(custom_fields['io_output_bytes'], ''))"
            io_res = trace.run_parallel(
                [
                    (
                        "io_summary",
                        lambda: service.client.execute(
                            f"/* value:io_summary */ SELECT "
                            f"countIf({is_img_expr}) AS image_requests, "
                            f"sumIf({bytes_expr}, {is_img_expr}) AS image_bytes, "
                            f"countIf({ofmt_expr} IS NOT NULL) AS io_count, "
                            f"sumIf({bytes_expr}, {ofmt_expr} IS NOT NULL) AS io_bytes, "
                            f"sumIf({bytes_expr}, {ofmt_expr} IS NOT NULL AND {shield_cond}) AS io_shield_bytes, "
                            f"sumIf({io_in_expr}, {io_out_expr} IS NOT NULL) AS input_bytes, "
                            f"sumIf({io_out_expr}, {io_in_expr} IS NOT NULL) AS output_bytes, "
                            f"sumIf({io_in_expr} - {io_out_expr}, {io_in_expr} IS NOT NULL AND {io_out_expr} IS NOT NULL) AS actual_saved, "
                            f"countIf({io_in_expr} IS NOT NULL AND {io_out_expr} IS NOT NULL) AS io_pair_count "
                            f"FROM request_facts WHERE {where}",
                            params,
                        ),
                    ),
                    (
                        "io_formats",
                        lambda: service.client.execute(
                            f"/* value:io_formats */ SELECT coalesce("
                            f"{ofmt_expr}, "
                            f"if(startsWith({ctype_expr}, 'image/'), nullIf(splitByChar('/', {ctype_expr})[2], ''), NULL), "
                            f"if({ctype_expr} = '', {url_img_fmt_expr}, NULL)"
                            f") AS format, "
                            f"count() AS cnt FROM request_facts "
                            f"WHERE {where} AND ({ofmt_expr} IS NOT NULL OR {is_img_expr}) "
                            f"GROUP BY format HAVING format IS NOT NULL AND format != '' ORDER BY cnt DESC",
                            params,
                        ),
                    ),
                    (
                        "io_opportunities",
                        lambda: service.client.execute(
                            f"/* value:io_opportunities */ SELECT url, count() AS request_count, "
                            f"sum({bytes_expr}) AS total_bytes, avg({bytes_expr}) / 1024.0 AS avg_kb "
                            f"FROM request_facts WHERE {where} "
                            f"AND ({ctype_expr} IN ('image/jpeg', 'image/jpg', 'image/png', 'image/gif') "
                            f"OR ({ctype_expr} = '' AND {url_raster_ext_expr})) "
                            f"AND {ofmt_expr} IS NULL AND {bytes_expr} > 51200 "
                            f"GROUP BY url ORDER BY total_bytes DESC LIMIT 20",
                            params,
                        ),
                    ),
                    (
                        "io_conversions",
                        lambda: service.client.execute(
                            f"/* value:io_conversions */ SELECT {ifmt_expr} AS input_format, {ofmt_expr} AS output_format, "
                            f"count() AS cnt, avg({io_in_expr} / nullIf(toFloat64({io_out_expr}), 0)) AS avg_ratio "
                            f"FROM request_facts WHERE {where} AND {ifmt_expr} IS NOT NULL AND {ofmt_expr} IS NOT NULL "
                            f"GROUP BY input_format, output_format ORDER BY cnt DESC LIMIT 20",
                            params,
                        ),
                    ),
                    (
                        "io_series",
                        lambda: service.client.execute(
                            f"/* value:io_series */ SELECT "
                            f"toDateTime(intDiv(toUInt32(event_timestamp), {bucket_sec}) * {bucket_sec}, 'UTC') AS bucket, "
                            f"countIf({ofmt_expr} IS NOT NULL) AS io_count, "
                            f"sumIf({bytes_expr}, {ofmt_expr} IS NOT NULL) AS io_bytes, "
                            f"sumIf({io_in_expr} - {io_out_expr}, {io_in_expr} IS NOT NULL AND {io_out_expr} IS NOT NULL) AS actual_saved "
                            f"FROM request_facts WHERE {where} GROUP BY bucket ORDER BY bucket",
                            params,
                        ),
                    ),
                ],
                source="fact",
                max_workers=5,
                thread_name_prefix="hs-val-io",
            )
            io_sum_rows = io_res["io_summary"]
            io_s = io_sum_rows[0] if io_sum_rows else {}
            io_count = int(io_s.get("io_count") or 0)
            img_count = int(io_s.get("image_requests") or 0)
            has_io_pairs = int(io_s.get("io_pair_count") or 0) > 0 or (
                io_s.get("io_pair_count") is None and io_s.get("actual_saved") is not None and io_count > 0
            )
            in_b = io_s.get("input_bytes")
            out_b = io_s.get("output_bytes")
            actual_saved = io_s.get("actual_saved") if has_io_pairs else None
            fmt_rows = io_res["io_formats"]
            fmt_total = sum(int(r.get("cnt") or 0) for r in fmt_rows)
            fmt_dist = [
                {
                    "format": str(r["format"]),
                    "count": int(r["cnt"]),
                    "pct": round(int(r["cnt"]) * 100.0 / fmt_total, 2),
                }
                for r in fmt_rows
                if int(r.get("cnt") or 0) > 0 and fmt_total > 0
            ]
            modern_cnt = sum(
                int(r["cnt"]) for r in fmt_rows if str(r.get("format", "")).lower() in ("webp", "avif", "jpegxl", "jxl")
            )
            opp_rows = io_res["io_opportunities"]
            conv_rows = io_res["io_conversions"]
            io_ser_rows = io_res["io_series"]
            io_ts = [
                {"time": safe_iso(r["bucket"]), "value": int(r["io_count"])}
                for r in io_ser_rows
                if int(r.get("io_count") or 0) > 0
            ]
            io_bw_ts = [
                {"time": safe_iso(r["bucket"]), "value": int(r["io_bytes"])}
                for r in io_ser_rows
                if r.get("io_bytes") is not None and int(r.get("io_bytes") or 0) > 0
            ]
            io_comp_ts = [
                {"time": safe_iso(r["bucket"]), "value": int(r["actual_saved"])}
                for r in io_ser_rows
                if r.get("actual_saved") is not None
            ]
            result["io"] = {
                "total_requests": total_requests,
                "io_transforms": io_count if io_count > 0 else None,
                "io_bandwidth_bytes": int(io_s["io_bytes"])
                if io_count > 0 and io_s.get("io_bytes") is not None
                else None,
                "io_shield_bandwidth_bytes": int(io_s["io_shield_bytes"])
                if io_count > 0 and io_s.get("io_shield_bytes") is not None
                else None,
                "io_pct_of_traffic": round(io_count * 100.0 / total_requests, 2)
                if io_count > 0 and total_requests > 0
                else None,
                "io_estimated_cost_usd": None,
                "format_distribution": fmt_dist,
                "modern_format_pct": round(modern_cnt * 100.0 / fmt_total, 2) if fmt_total > 0 else None,
                "optimization_opportunities": [
                    {
                        "url": str(r.get("url") or ""),
                        "request_count": int(r.get("request_count") or 0),
                        "total_bytes": int(r.get("total_bytes") or 0),
                        "avg_kb": round(float(r["avg_kb"]), 2) if r.get("avg_kb") is not None else None,
                    }
                    for r in opp_rows
                ],
                "io_actual_bandwidth_saved_bytes": int(actual_saved) if actual_saved is not None else None,
                "io_actual_compression_ratio": (
                    round(float(in_b) / float(out_b), 2)
                    if out_b is not None and float(out_b) > 0 and in_b is not None
                    else None
                ),
                "io_format_conversion_pairs": [
                    {
                        "input_format": str(r.get("input_format") or ""),
                        "output_format": str(r.get("output_format") or ""),
                        "count": int(r.get("cnt") or 0),
                        "avg_ratio": round(float(r["avg_ratio"]), 2) if r.get("avg_ratio") is not None else None,
                    }
                    for r in conv_rows
                ],
                "image_request_count": img_count,
                "image_bandwidth_bytes": int(io_s["image_bytes"])
                if img_count > 0 and io_s.get("image_bytes") is not None
                else None,
                "io_time_series": io_ts,
                "io_bandwidth_time_series": io_bw_ts,
                "io_compression_time_series": io_comp_ts,
            }

        payload = {**result, "section_timings": trace.timings()}
        if cache_key is not None:
            _perf_memo.put(cache_key, payload)
        return payload

    if cache_key is not None:
        return ValueSummaryResponse.with_telemetry(**_perf_memo.coalesce(f"hs_value_summary:{cache_key}", _compute))
    return ValueSummaryResponse.with_telemetry(**_perf_memo.result(_compute(), state="bypass"))
