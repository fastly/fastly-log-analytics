"""High-scale performance projections and aggregate queries."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

import hashlib
import heapq
import os
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as _dc_field

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import _FIELD_DIMENSIONS, normalize_cache_bound
from backend.high_scale._common import bucket as _bucket
from backend.high_scale._common import build_published_facts_where as _facts_where
from backend.high_scale._common import build_published_projection_where as _where
from backend.high_scale._common import finite_float as _finite_float
from backend.high_scale._common import percentiles as _percentiles
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
    PERF_TTL_LAST_BUCKET,
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
    ottfb_ms = (_finite_float(row.get("ottfb")) or 0.0) / 1000.0
    ottlb_ms = (_finite_float(row.get("ottlb")) or 0.0) / 1000.0
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


def _ttl_bucket_expr(column: str) -> str:
    from backend.high_scale._common import _bucket_case

    return _bucket_case(column, _TTL_EDGES, PERF_TTL_LAST_BUCKET)


_ELAPSED_MS = "toFloat64OrNull(custom_fields['elapsed']) / 1000.0"
_TTFB_MS = "toFloat64OrNull(custom_fields['ttfb']) * 1000.0"
_OTTFB_MS = "coalesce(toFloat64OrNull(custom_fields['ottfb']), 0) / 1000.0"
_OTTLB_MS = "coalesce(toFloat64OrNull(custom_fields['ottlb']), 0) / 1000.0"


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
        f"{_weighted('latency_p50_ms')} / 1000.0 AS p50_ms, "
        f"{_weighted('latency_p95_ms')} / 1000.0 AS p95_ms, "
        f"{_weighted('latency_p99_ms')} / 1000.0 AS p99_ms "
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
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
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
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
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
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
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
            facts_where_params = _facts_where(service.service_id, start_time, end_time, filters)
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
    from backend.repositories.value import (
        _shape_caching_and_overview_buckets,
        _shape_raw_network_buckets,
    )
    from backend.utils.date_utils import safe_iso

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
        if cache_key is not None and (cached_inner := _perf_memo.get(cache_key)) is not None:
            return cached_inner
        trace = HighScaleSections(service.service_id, "value_summary")
        where, params = _facts_where(service.service_id, start_time, end_time, filters)
        hit_cond = "custom_fields['cache'] IN ('HIT', 'HIT-STALE', 'HIT-CLUSTER')"
        miss_cond = "custom_fields['cache'] = 'MISS'"
        pass_cond = "custom_fields['cache'] = 'PASS'"
        synth_cond = "custom_fields['cache'] = 'SYNTH'"
        origin_cond = "custom_fields['cache'] IN ('MISS', 'PASS', 'SYNTH', 'ERROR')"
        shield_cond = (
            "(lower(custom_fields['is_shield']) IN ('true', '1') OR lower(custom_fields['edge']) IN ('false', '0'))"
        )
        waf_cond = "custom_fields['waf'] IN ('1', 'true', 'TRUE')"
        waf_block_cond = "custom_fields['waf'] IN ('1', 'true', 'TRUE') AND custom_fields['waf_resp'] = '406'"
        waf_log_cond = (
            "custom_fields['waf'] IN ('1', 'true', 'TRUE') "
            "AND custom_fields['waf_resp'] != '406' AND custom_fields['waf_sig'] != ''"
        )
        waf_pass_cond = "custom_fields['waf'] IN ('1', 'true', 'TRUE') AND custom_fields['waf_resp'] = '200'"
        elapsed_us_expr = f"({_ELAPSED_MS} * 1000.0)"
        bytes_expr = "toInt64OrZero(coalesce(nullIf(custom_fields['resp_bytes'], ''), custom_fields['bytes']))"
        proto_expr = "coalesce(nullIf(custom_fields['proto'], ''), custom_fields['protocol'])"
        ipv6_expr = "coalesce(nullIf(custom_fields['is_ipv6'], ''), custom_fields['ipv6'])"

        def _run_bucket_scan() -> list[dict[str, Any]]:
            bot_select = (
                ", countIf(custom_fields['_bot_name'] != '') AS bot_requests, "
                "countIf(positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0) AS verified_bots "
                if is_real_ch
                else " "
            )
            sql = (
                f"/* value:combined_buckets */ SELECT "
                f"toDateTime(intDiv(toUInt32(event_timestamp), {bucket_sec}) * {bucket_sec}, 'UTC') AS bucket, "
                f"count() AS total, "
                f"countIf({hit_cond}) AS hit_cnt, "
                f"countIf({miss_cond}) AS miss_cnt, "
                f"countIf({pass_cond}) AS pass_cnt, "
                f"countIf({synth_cond}) AS synth_cnt, "
                f"sumIf({bytes_expr}, {hit_cond}) AS bandwidth_saved, "
                f"sum({bytes_expr}) AS total_bytes, "
                f"countIf({origin_cond}) AS origin_cnt, "
                f"countIf({shield_cond} AND {hit_cond}) AS shield_hit, "
                f"countIf({shield_cond}) AS shield_total, "
                f"countIf({waf_block_cond}) AS threats_cnt, "
                f"sumIf({elapsed_us_expr}, {hit_cond}) AS sum_hit_elapsed, "
                f"countIf({elapsed_us_expr} IS NOT NULL AND {hit_cond}) AS cnt_hit_elapsed, "
                f"sumIf({elapsed_us_expr}, {miss_cond}) AS sum_miss_elapsed, "
                f"countIf({elapsed_us_expr} IS NOT NULL AND {miss_cond}) AS cnt_miss_elapsed, "
                f"countIf({waf_cond}) AS waf_inspected, "
                f"countIf({waf_block_cond}) AS waf_blocked, "
                f"countIf({waf_log_cond}) AS waf_logged, "
                f"countIf({waf_pass_cond}) AS waf_passed, "
                f"countIf(custom_fields['waf_sig'] != '') AS waf_sig_cnt, "
                f"countIf({proto_expr} IN ('3.0', '3', 'HTTP/3', 'h3')) AS http3_cnt, "
                f"countIf({proto_expr} IN ('2.0', '2', 'HTTP/2', 'h2')) AS h2_cnt, "
                f"countIf({proto_expr} IN ('1.1', 'HTTP/1.1', 'http/1.1')) AS h11_cnt, "
                f"countIf({proto_expr} NOT IN ('3.0', '3', 'HTTP/3', 'h3', '2.0', '2', 'HTTP/2', 'h2', '1.1', 'HTTP/1.1', 'http/1.1')) AS other_proto_cnt, "
                f"countIf(lower(custom_fields['is_ssl']) IN ('true', '1') OR (custom_fields['tls'] != '' AND custom_fields['tls'] != '-')) AS tls_cnt, "
                f"countIf(lower({ipv6_expr}) IN ('true', '1')) AS ipv6_cnt"
                f"{bot_select}"
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
        result: dict[str, Any] = {}

        if {"caching", "overview", "performance"} & wanted:
            shaped = _shape_caching_and_overview_buckets(
                buckets,
                include_overview="overview" in wanted,
                has_resp_bytes=True,
                has_shield=True,
                has_waf=True,
                has_elapsed=True,
                include_performance="performance" in wanted,
            )
            if "caching" in wanted and "caching" in shaped:
                result["caching"] = shaped["caching"]
            if "overview" in wanted and "overview" in shaped:
                result["overview"] = shaped["overview"]
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
                "waf_inspected": _sec_sum("waf_inspected"),
                "waf_blocked": _sec_sum("waf_blocked"),
                "waf_logged": _sec_sum("waf_logged"),
                "waf_passed": _sec_sum("waf_passed"),
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
            if is_real_ch:
                bot_req = sum(int(b.get("bot_requests", 0) or 0) for b in buckets)
                ver_bots = sum(int(b.get("verified_bots", 0) or 0) for b in buckets)
            else:
                bot_rows = trace.run(
                    "bots",
                    lambda: service.client.execute(
                        f"/* value:bots */ SELECT "
                        f"countIf(custom_fields['_bot_name'] != '') AS bot_requests, "
                        f"countIf(positionCaseInsensitive(custom_fields['waf_sig'], 'VERIFIED-BOT.') > 0) AS verified_bots "
                        f"FROM request_facts WHERE {where}",
                        params,
                    ),
                    source="fact",
                )
                bot_req = int(bot_rows[0].get("bot_requests") or 0) if bot_rows else 0
                ver_bots = int(bot_rows[0].get("verified_bots") or 0) if bot_rows else 0
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
            result["bots"] = {
                "total_requests": total_requests,
                "bot_requests": bot_req,
                "verified_bots": ver_bots,
                "top_bots": top_bots_list,
            }

        payload = {**result, "section_timings": trace.timings()}
        if cache_key is not None:
            _perf_memo.put(cache_key, payload)
        return payload

    if cache_key is not None:
        return ValueSummaryResponse.with_telemetry(**_perf_memo.coalesce(f"hs_value_summary:{cache_key}", _compute))
    return ValueSummaryResponse.with_telemetry(**_perf_memo.result(_compute(), state="bypass"))
