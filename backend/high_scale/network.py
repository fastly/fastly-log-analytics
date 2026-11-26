"""High-scale network health, transport quality, POP health, shielding analysis, and minute projections."""

from __future__ import annotations

import contextvars
import math
import os
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field as _dc_field
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any

from backend.core.request_telemetry import HighScaleSections
from backend.high_scale._common import (
    bucket as _bucket,
)
from backend.high_scale._common import (
    build_clickhouse_filters as _build_clickhouse_filters,
)
from backend.high_scale._common import (
    finite_float as _finite_float,
)
from backend.high_scale._common import (
    normalize_cache_bound,
    parse_time_window,
    resolve_asn_display_labels,
    visible_publication_fence_sql,
    visible_rollup_publication_fence_sql,
)
from backend.high_scale._common import (
    percentiles as _percentiles,
)
from backend.high_scale._common import (
    range_value as _range_value,
)
from backend.high_scale._common import (
    text as _text,
)
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    cache_result,
    client_cache_token,
    coalesce,
    digest_cache_key,
    register_service_cache_invalidator,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

_network_memo = ResponseMemo(ttl=60.0, maxsize=128, prewarmer=False, invalidate_on_ingest=False)
CacheRegistry.register("high_scale.network", _network_memo.cache)
_network_model_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
CacheRegistry.register("high_scale.network.models", _network_model_cache)
_network_json_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
CacheRegistry.register("high_scale.network.json", _network_json_cache)
_pop_rollup_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
CacheRegistry.register("high_scale.network.pop_rollup", _pop_rollup_cache)
_pop_fact_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=60.0)
CacheRegistry.register("high_scale.network.pop_fact", _pop_fact_cache)
_SHIELD_ACTIVE_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=600)
CacheRegistry.register("high_scale.network._SHIELD_ACTIVE_CACHE", _SHIELD_ACTIVE_CACHE)


def _invalidate_network_caches(_service_id: str) -> int:
    n = (
        len(_network_model_cache)
        + len(_network_json_cache)
        + len(_pop_rollup_cache)
        + len(_pop_fact_cache)
        + len(_SHIELD_ACTIVE_CACHE)
    )
    _network_model_cache.clear()
    _network_json_cache.clear()
    _pop_rollup_cache.clear()
    _pop_fact_cache.clear()
    _SHIELD_ACTIVE_CACHE.clear()
    return n


register_service_cache_invalidator(_invalidate_network_caches, on_ingest=False)


def _cache_minute_bound(value: str | datetime | None) -> int | str | None:
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else _range_value(value)
    if parsed is not None:
        dt = parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
        return int(dt.timestamp()) // 60
    return normalize_cache_bound(value)


def _pop_fact_key(
    service: HighScaleService, start: datetime | None, end: datetime | None, filters: Any
) -> tuple[Any, ...] | None:
    if start is None or end is None:
        return None
    f_items = tuple((k, v[0], tuple(v[1])) for k, v in serialize_filters_for_key(filters or {}).items())
    return (
        service.service_id,
        client_cache_token(service.client),
        _cache_minute_bound(start),
        _cache_minute_bound(end),
        f_items,
    )


def _fast_model_hit(memo_key: str) -> Any | None:
    if (cached := _network_memo.get_with_telemetry(memo_key)) is None:
        return None
    if (cached_model := _network_model_cache.get(memo_key)) is None:
        return None
    hit_resp = cached_model.model_copy(
        update={
            "is_cached": True,
            "response_cache": cached.get("_response_cache") or cached.get("response_cache"),
            "debug_queries": [],
            "debug_calls": [],
            "debug_sqlite": [],
            "debug_postgres": [],
        }
    )
    if (cached_json := _network_json_cache.get(memo_key)) is not None:
        object.__setattr__(
            hit_resp,
            "_cached_json_dict",
            {
                **cached_json,
                "_is_cached": True,
                "_response_cache": cached.get("_response_cache") or cached.get("response_cache"),
                "_debug_queries": [],
                "_debug_calls": [],
                "_debug_sqlite": [],
                "_debug_postgres": [],
            },
        )
    return hit_resp


from backend.high_scale._common import optional_int as _finite_int


def _asn_text(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if (parsed := _finite_int(value)) is not None:
        return str(parsed)
    return ""


from backend.models.analytics import (
    NetworkBundleRequest,
    NetworkBundleResponse,
    NetworkCityEntry,
    NetworkHealthResponse,
    NetworkHealthSummary,
    NetworkQualityRequest,
    NetworkQualityResponse,
    NetworkWorstEntry,
    OriginShieldingAnalysisResponse,
    PopHealthItem,
    PopHealthListResponse,
    ShieldingAnalysis,
    ShieldingRow,
    TransportMetrics,
    TransportProtocolShare,
)
from backend.models.common import SectionTimingEntry
from backend.repositories.utils.response_shaping import (
    build_asn_speed_mix,
    build_network_asn_leaderboard,
    clamp_bucket_seconds,
    compute_network_global_summary,
    deduplicate_metro_leaderboard,
    generate_epoch_aligned_buckets,
    shape_protocol_distribution,
)
from backend.repositories.utils.response_shaping import (
    compute_network_health_score as _health_score,
)
from backend.repositories.utils.response_shaping import (
    enrich_row_with_pop_distance as _enrich_with_distance,
)
from backend.repositories.utils.response_shaping import (
    opt_float as _flt,
)
from backend.repositories.utils.response_shaping import (
    parse_metro_code as _parse_metro_code,
)
from backend.utils.countries import COUNTRY_MAP, format_city_label


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None).isoformat()


def _network_sample_stride(span_s: float) -> int:
    if span_s >= 1209600:
        return 500
    if span_s >= 604800:
        return 100
    if span_s >= 172800:
        return 50
    if span_s >= 43200:
        return 20
    if span_s >= 21600:
        return 5
    return 1


def _to_utc_epoch_s(dt: datetime) -> int:
    utc_dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return int(utc_dt.timestamp())


def _pk_range_or_clause(windows: list[tuple[int, int]]) -> str:
    clauses = [
        f"(event_timestamp >= toDateTime({w_start}, 'UTC') AND event_timestamp < toDateTime({w_end}, 'UTC'))"
        for w_start, w_end in windows
    ]
    return f" AND ({' OR '.join(clauses)})"


def _network_time_sample_clause(
    stride: int,
    *,
    bucket_seconds: int | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> str:
    """Granule- and partition-skipping primary-key time-window sample clause for ClickHouse PREWHERE.

    `request_facts` is partitioned by `toYYYYMMDD(event_timestamp)` and ordered by
    `(service_id, toStartOfHour(event_timestamp), event_timestamp)` (or `(service_id, event_timestamp, event_id)`).
    Modulo predicates like `intDiv(toUnixTimestamp(event_timestamp), 30) % stride = 0` or
    `line_ordinal % stride = 0` are non-monotonic so ClickHouse's `KeyCondition` cannot prune
    partitions or granules and still scans 100% of rows. Explicit `(event_timestamp >= ... AND
    event_timestamp < ...)` range disjunctions are monotonic on the primary key and let
    ClickHouse skip ~(1 - 1/stride) of granules before reading `_cf_*` columns.
    """
    if stride <= 1:
        return ""
    if start is None or end is None:
        if bucket_seconds is not None:
            if bucket_seconds < stride:
                return f" AND line_ordinal % {stride} = 0"
            chunk_s = max(1, min(30, bucket_seconds // stride))
        else:
            chunk_s = 30
        return f" AND intDiv(toUnixTimestamp(event_timestamp), {chunk_s}) % {stride} = 0"

    start_ts, end_ts = _to_utc_epoch_s(start), _to_utc_epoch_s(end)
    span_s = end_ts - start_ts
    if span_s <= stride:
        return ""

    if bucket_seconds is not None:
        if bucket_seconds < stride:
            return f" AND line_ordinal % {stride} = 0"
        first_b = (start_ts // bucket_seconds) * bucket_seconds
        last_b = ((end_ts - 1) // bucket_seconds) * bucket_seconds
        n_buckets = ((last_b - first_b) // bucket_seconds) + 1
        step_buckets = 1 if n_buckets <= 720 else math.ceil(n_buckets / 360)
        windows: list[tuple[int, int]] = []
        for idx in range(0, n_buckets, step_buckets):
            b_lo = max(start_ts, first_b + idx * bucket_seconds)
            b_hi = min(end_ts, first_b + (idx + step_buckets) * bucket_seconds)
            b_dur = b_hi - b_lo
            if b_dur < stride:
                continue
            win_s = max(1, b_dur // stride)
            slack = b_dur - win_s
            offset = ((idx * 13) % 60) * slack // 60 if slack > 0 else 0
            windows.append((b_lo + offset, b_lo + offset + win_s))
        return _pk_range_or_clause(windows) if windows else ""

    target_sampled_s = max(1, span_s // stride)
    n_windows = min(24, max(1, target_sampled_s // 60)) if target_sampled_s >= 60 else 1
    win_s = max(1, target_sampled_s // n_windows)
    step_s = span_s // n_windows
    windows = []
    for i in range(n_windows):
        slot_lo = start_ts + i * step_s
        slot_hi = end_ts if i == n_windows - 1 else start_ts + (i + 1) * step_s
        slot_dur = slot_hi - slot_lo
        w_dur = min(win_s, slot_dur)
        if w_dur <= 0:
            continue
        slack = slot_dur - w_dur
        offset = ((i * 13) % 24) * slack // 24 if slack > 0 else 0
        windows.append((slot_lo + offset, slot_lo + offset + w_dur))
    return _pk_range_or_clause(windows) if windows else ""


def _ch_bucket_expr(bucket_seconds: int) -> str:
    return f"intDiv(toUnixTimestamp(event_timestamp), {bucket_seconds}) * {bucket_seconds}"


def _ch_fact_query(
    select_cols: str,
    where_sql: str,
    *,
    group_by: str = "",
    having: str = "",
    order_by: str = "",
    limit: int | None = None,
) -> str:
    parts = [f"SELECT {select_cols} FROM request_facts WHERE {where_sql}"]
    if group_by:
        parts.append(f"GROUP BY {group_by}")
    if having:
        parts.append(f"HAVING {having}")
    if order_by:
        parts.append(f"ORDER BY {order_by}")
    if limit is not None:
        parts.append(f"LIMIT {limit}")
    return " ".join(parts)


_RTT = "toFloat64OrNull(custom_fields['tcp_rtt'])"
_RTT_MIN = "toFloat64OrNull(custom_fields['rtt_min'])"
_RTT_VAR = "toFloat64OrNull(custom_fields['rtt_var'])"
_PLOSS = "toFloat64OrNull(custom_fields['ploss'])"
_STATUS = "toInt32OrZero(custom_fields['status'])"
_ASN = "toInt32OrNull(custom_fields['asn'])"
_BASE_WHERE = (
    f"service_id={{service_id:String}} AND {visible_publication_fence_sql('request')} "
    "AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}"
)
_RTT_FILTER = f"{_BASE_WHERE} AND {_RTT} > 0"
_TTFB = "toFloat64OrNull(custom_fields['ttfb'])"
_ELAPSED = "toFloat64OrZero(custom_fields['elapsed'])"
_RESP_BYTES = "toFloat64OrZero(custom_fields['resp_bytes'])"
_NET_HIT = "custom_fields['cache'] LIKE '%HIT%'"
_CORE_SECTIONS = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
_SCATTER_SAMPLE = 2000
_SHIELDING_ROUTE_LIMIT = 50
_TRANSPORT_FIELDS = ("tcp_rtt", "retrans", "ploss", "delivery_rate")
_QUALITY_BAR_SPECS: tuple[tuple[str, str, str], ...] = (
    ("country", "country", ""),
    ("asn", _ASN, ""),
    ("region", "custom_fields['region']", " AND country = {region_country:String}"),
    ("pop", "custom_fields['pop']", ""),
)
_TRANSPORT_ROUND_SPECS: tuple[tuple[str, str, int], ...] = (
    ("tcp_rtt_p50_ms", "rtt_p50", 2),
    ("tcp_rtt_p90_ms", "rtt_p90", 2),
    ("tcp_rtt_p95_ms", "rtt_p95", 2),
    ("tcp_rtt_p99_ms", "rtt_p99", 2),
    ("retrans_rate_pct", "avg_retrans", 3),
    ("packet_loss_pct", "avg_ploss_pct", 3),
    ("delivery_rate_mbps", "avg_del_rate_mbps", 2),
)


def _filter_clause(filters: Any, *, drop_asn: bool = False) -> tuple[str, dict[str, Any]]:
    spec = dict(filters or {})
    if drop_asn:
        spec.pop("asn", None)
    clause, params = _build_clickhouse_filters(spec)
    return ("", {}) if clause == "1=1" else (f" AND {clause}", params)


def _want_core(sections: Any) -> bool:
    return not sections or "health" in sections or bool(_CORE_SECTIONS & set(sections))


def _want_shielding(sections: Any) -> bool:
    return not sections or "health" in sections or "shielding_analysis" in sections


def _shielding_analysis(service: HighScaleService, start: datetime, end: datetime, filters: Any) -> ShieldingAnalysis:
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    shield_where = f"{_BASE_WHERE} AND lower(custom_fields['edge']) IN ('false', '0') AND custom_fields['prid'] != ''"
    sh_span_s = (end - start).total_seconds() if start is not None and end is not None else 0.0
    sh_join_stride = 1
    if is_real_ch:
        shield_cache_key = (service.service_id, client_cache_token(service.client))
        shield_win_key = (
            (
                service.service_id,
                client_cache_token(service.client),
                _to_utc_epoch_s(start) // 300,
                _to_utc_epoch_s(end) // 300,
            )
            if start is not None and end is not None
            else None
        )
        use_shield_cache = not os.getenv("PYTEST_CURRENT_TEST")
        cached_shield = _SHIELD_ACTIVE_CACHE.get(shield_cache_key) if use_shield_cache else None
        if cached_shield is None and use_shield_cache and shield_win_key is not None:
            cached_shield = _SHIELD_ACTIVE_CACHE.get(shield_win_key)
        if cached_shield is False:
            return ShieldingAnalysis(
                **cache_result({}, state="bypass", prewarmer=False), has_data=False, edge_only=True, rows=[]
            )
        if cached_shield is not True:
            shield_prewhere = (
                f"{_BASE_WHERE} AND custom_fields['edge'] IN ('false', '0', 'False', 'FALSE') "
                "AND custom_fields['prid'] != ''"
            )
            shield_rows = service.client.execute(
                f"SELECT count() AS n FROM (SELECT 1 FROM request_facts PREWHERE {shield_prewhere} LIMIT 1)",
                {"service_id": service.service_id, "start": start, "end": end},
            )
            if not shield_rows or int(shield_rows[0]["n"]) == 0:
                if use_shield_cache:
                    if sh_span_s >= 86400:
                        _SHIELD_ACTIVE_CACHE[shield_cache_key] = False
                    elif shield_win_key is not None:
                        _SHIELD_ACTIVE_CACHE[shield_win_key] = False
                return ShieldingAnalysis(
                    **cache_result({}, state="bypass", prewarmer=False), has_data=False, edge_only=True, rows=[]
                )
            if use_shield_cache:
                _SHIELD_ACTIVE_CACHE[shield_cache_key] = True
        if sh_span_s >= 1800 and not os.getenv("PYTEST_CURRENT_TEST"):
            sh_join_stride = (
                100
                if sh_span_s >= 604800
                else (50 if sh_span_s >= 172800 else (20 if sh_span_s >= 43200 else (10 if sh_span_s >= 21600 else 4)))
            )
    filter_sql, filter_params = _filter_clause(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    hop_ms = "coalesce(toFloat64OrNull(custom_fields['ottfb']), toFloat64OrNull(custom_fields['ttfb']) * 1000000.0)"
    if is_real_ch:
        edge_hash = f" AND cityHash64(custom_fields['rid']) % {sh_join_stride} = 0" if sh_join_stride > 1 else ""
        shield_hash = f" AND cityHash64(custom_fields['prid']) % {sh_join_stride} = 0" if sh_join_stride > 1 else ""
        edge_sub = (
            f"SELECT custom_fields['rid'] AS rid, custom_fields['pop'] AS edge_pop, {hop_ms} AS hop_us "
            f"FROM request_facts PREWHERE {_BASE_WHERE}{filter_sql} "
            f"AND custom_fields['edge'] IN ('true', '1', 'True', 'TRUE') AND custom_fields['rid'] != ''{edge_hash}"
        )
        shield_sub = (
            f"SELECT custom_fields['prid'] AS prid, custom_fields['pop'] AS shield_pop, {hop_ms} AS hop_us "
            f"FROM request_facts PREWHERE {_BASE_WHERE} "
            f"AND custom_fields['edge'] IN ('false', '0', 'False', 'FALSE') AND custom_fields['prid'] != ''{shield_hash}"
        )
        pairs = service.client.execute(
            "SELECT edge_pop, shield_pop, requests, "
            "toFloat64(ifNotFinite(quants[1] / 1000.0, 0)) AS p50_ms, "
            "toFloat64(ifNotFinite(quants[2] / 1000.0, 0)) AS p95_ms, "
            "toFloat64(ifNotFinite(quants[3] / 1000.0, 0)) AS p99_ms "
            "FROM (SELECT e.edge_pop AS edge_pop, s.shield_pop AS shield_pop, "
            f"count() * {sh_join_stride} AS requests, "
            "quantilesTDigest(0.50, 0.95, 0.99)(e.hop_us - s.hop_us) AS quants "
            f"FROM ({edge_sub}) AS e INNER JOIN ({shield_sub}) AS s ON s.prid = e.rid GROUP BY edge_pop, shield_pop)",
            params,
        )
    else:
        edge_sub = _ch_fact_query(
            f"custom_fields['rid'] AS rid, custom_fields['pop'] AS edge_pop, {hop_ms} AS hop_us",
            f"{_BASE_WHERE}{filter_sql} AND lower(custom_fields['edge']) IN ('true', '1') AND custom_fields['rid'] != ''",
        )
        shield_sub = _ch_fact_query(
            f"custom_fields['prid'] AS prid, custom_fields['pop'] AS shield_pop, {hop_ms} AS hop_us", shield_where
        )
        pairs = service.client.execute(
            "SELECT e.edge_pop AS edge_pop, s.shield_pop AS shield_pop, count() AS requests, "
            "quantile(0.50)(e.hop_us - s.hop_us) / 1000.0 AS p50_ms, "
            "quantile(0.95)(e.hop_us - s.hop_us) / 1000.0 AS p95_ms, "
            "quantile(0.99)(e.hop_us - s.hop_us) / 1000.0 AS p99_ms "
            f"FROM ({edge_sub}) AS e INNER JOIN ({shield_sub}) AS s ON s.prid = e.rid GROUP BY edge_pop, shield_pop",
            params,
        )
    if not pairs:
        if is_real_ch:
            return ShieldingAnalysis(
                **cache_result({}, state="bypass", prewarmer=False), has_data=False, edge_only=None, rows=[]
            )
        shield_rows = service.client.execute(
            f"SELECT count() AS n FROM ({_ch_fact_query('1', shield_where, limit=1)})",
            {"service_id": service.service_id, "start": start, "end": end},
        )
        edge_only = True if (shield_rows and int(shield_rows[0]["n"]) == 0) else None
        return ShieldingAnalysis(
            **cache_result({}, state="bypass", prewarmer=False), has_data=False, edge_only=edge_only, rows=[]
        )

    by_requests = sorted(pairs, key=lambda r: int(r["requests"]), reverse=True)[:_SHIELDING_ROUTE_LIMIT]
    by_overhead = sorted(
        pairs, key=lambda r: float(r["p50_ms"]) if r["p50_ms"] is not None else float("-inf"), reverse=True
    )[:_SHIELDING_ROUTE_LIMIT]
    kept = {(r["edge_pop"], r["shield_pop"]): r for r in by_requests + by_overhead}
    rows = [
        ShieldingRow(
            **_enrich_with_distance(
                {
                    "edge_pop": r["edge_pop"],
                    "shield_pop": r["shield_pop"],
                    "requests": int(r["requests"]),
                    "p50_ms": _flt(r["p50_ms"]),
                    "p95_ms": _flt(r["p95_ms"]),
                    "p99_ms": _flt(r["p99_ms"]),
                }
            )
        )
        for r in sorted(kept.values(), key=lambda r: int(r["requests"]), reverse=True)
    ]
    return ShieldingAnalysis(
        **cache_result({}, state="bypass", prewarmer=False),
        has_data=True,
        rows=rows,
        total_routes=len(pairs),
        truncated=len(pairs) > len(rows),
    )


def origin_shielding_analysis(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> OriginShieldingAnalysisResponse:
    start, end = parse_time_window(start_time, end_time)
    limit = int(getattr(req, "limit", 50) or 50)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch and start_time is not None and end_time is not None:
        memo_key = digest_cache_key(
            {
                "ep": "hs_origin_shielding",
                "cid": client_cache_token(service.client),
                "s": _cache_minute_bound(start_time),
                "e": _cache_minute_bound(end_time),
                "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
                "l": limit,
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (cached := _network_memo.get_with_telemetry(memo_key)) is not None:
            return OriginShieldingAnalysisResponse.with_telemetry(**cached)

    def _compute_shielding() -> dict[str, Any]:
        if memo_key is not None and (cached_inner := _network_memo.get_with_telemetry(memo_key)) is not None:
            return cached_inner
        trace = HighScaleSections(service.service_id, "origin")
        analysis = trace.run(
            "shielding_analysis",
            lambda: _shielding_analysis(service, start, end, getattr(req, "filters", None)),
            source="fact",
        )
        rows = analysis.rows[:limit]
        truncated = (bool(analysis.truncated) or len(analysis.rows) > limit) if analysis.has_data else None
        payload = {
            "section_timings": trace.timings(),
            "has_data": analysis.has_data,
            "requires_fields": analysis.requires_fields or [],
            "edge_only": bool(analysis.edge_only),
            "rows": rows,
            "total_routes": analysis.total_routes,
            "truncated": truncated,
            "error": False,
        }
        if memo_key is not None:
            _network_memo.put(memo_key, dict(payload))
        return payload

    if memo_key is not None:
        return OriginShieldingAnalysisResponse.with_telemetry(
            **_network_memo.coalesce(f"hs_origin_shielding:{memo_key}", _compute_shielding)
        )
    return OriginShieldingAnalysisResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False),
        **_compute_shielding(),
    )


def network_health(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkHealthResponse:
    start, end = parse_time_window(start_time, end_time)
    bs = int(getattr(req, "bucket_seconds", 300) or 300)
    top_n = int(getattr(req, "top_n", 30) or 30)
    map_asn = str(getattr(req, "map_asn", "all") or "all")
    sections = getattr(req, "sections", None)

    trace = HighScaleSections(service.service_id, "network.health")
    if start is not None and end is not None and _want_core(sections):
        bs = clamp_bucket_seconds(bs, start, end) or bs
    memo_key = digest_cache_key(
        {
            "ep": "hs_network_health",
            "cid": client_cache_token(service.client),
            "s": _cache_minute_bound(start_time),
            "e": _cache_minute_bound(end_time),
            "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
            "bs": int(bs),
            "tn": int(top_n),
            "ma": str(map_asn),
            "sec": sorted(sections) if sections is not None else None,
            "m": str(getattr(req, "metric", "health_score") or "health_score"),
        },
        {"name": service.service_id, "service_id": service.service_id},
    )
    if (hit := _fast_model_hit(memo_key)) is not None:
        return hit
    if (cached := _network_memo.get_with_telemetry(memo_key)) is not None:
        return NetworkHealthResponse.with_telemetry(**cached)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )

    def _compute_health() -> NetworkHealthResponse:
        if (inner_hit := _fast_model_hit(memo_key)) is not None:
            return inner_hit
        if not _want_core(sections):
            shielding_only = (
                trace.run(
                    "shielding_analysis",
                    lambda: _shielding_analysis(service, start, end, getattr(req, "filters", None)),
                    source="fact",
                )
                if _want_shielding(sections)
                else None
            )
            sh_payload: dict[str, Any] = {
                "available": True,
                "shielding_analysis": shielding_only,
                "section_timings": trace.timings(),
            }
            _network_memo.put(memo_key, dict(sh_payload))
            sh_resp = NetworkHealthResponse.with_telemetry(
                **_network_memo.result(sh_payload, state="miss"),
            )
            _network_model_cache[memo_key] = sh_resp
            if is_real_ch:
                sh_dumped = sh_resp.model_dump(mode="json", by_alias=True)
                _network_json_cache[memo_key] = sh_dumped
                object.__setattr__(sh_resp, "_cached_json_dict", sh_dumped)
            return sh_resp
        filter_sql, filter_params = _filter_clause(getattr(req, "filters", None), drop_asn=map_asn != "all")
        params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
        rtt_where, bucket_expr = f"{_RTT_FILTER}{filter_sql}", _ch_bucket_expr(bs)
        map_asn_sql = ""
        if map_asn != "all":
            map_asn_sql, params["map_asn"] = f" AND {_ASN} = {{map_asn:Int32}}", int(map_asn)

        err_pct_expr = f"countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct"
        map_only = sections is not None and len(sections) > 0 and set(sections) <= {"buckets", "cities", "map_buckets"}
        if is_real_ch:
            nh_span_s = (end - start).total_seconds() if start is not None and end is not None else 0.0
            sample_stride = _network_sample_stride(nh_span_s) if not os.getenv("PYTEST_CURRENT_TEST") else 1
            stride_sql = _network_time_sample_clause(sample_stride, bucket_seconds=bs, start=start, end=end)
            rtt_prewhere = f"{_BASE_WHERE}{filter_sql}{stride_sql} AND custom_fields['tcp_rtt'] != '' AND custom_fields['tcp_rtt'] != '0'"
            top_asn_ints: list[int] = []
            raw_speed_rows: list[dict[str, Any]] | None = None
            if not map_only and not filter_sql and start is not None and end is not None:
                from backend.high_scale.aggregate_query import _hourly_edge_time_clause, can_use_hourly_rollup

                hourly_bounds = can_use_hourly_rollup(service.client, "request", start, end)
                agg_fence = visible_publication_fence_sql(
                    "request_aggregate", client=service.client, service_id=service.service_id
                )
                if hourly_bounds is not None:
                    h_start, h_end = hourly_bounds
                    edge_clause = _hourly_edge_time_clause(start, end, h_start, h_end)
                    rollup_fence = visible_rollup_publication_fence_sql("request_aggregate")
                    union_sql = (
                        "SELECT value AS asn_s, sum(request_count) AS cnt FROM request_aggregates_1h "
                        "WHERE service_id={service_id:String} AND dimension='asn' AND value != '' "
                        "AND bucket_start >= {h_start:DateTime64(3)} AND bucket_start < {h_end:DateTime64(3)} "
                        f"AND {rollup_fence} GROUP BY asn_s"
                    )
                    if edge_clause is not None:
                        union_sql += (
                            f" UNION ALL SELECT value AS asn_s, sum(request_count) AS cnt FROM request_aggregates "
                            f"WHERE service_id={{service_id:String}} AND dimension='asn' AND value != '' AND {edge_clause} AND {agg_fence} GROUP BY asn_s"
                        )
                    top_asn_sql = (
                        f"SELECT toInt32OrNull(asn_s) AS asn, sum(cnt) AS cnt FROM ({union_sql}) "
                        f"GROUP BY asn HAVING asn IS NOT NULL ORDER BY cnt DESC LIMIT {top_n}"
                    )
                    top_asn_rows = trace.run(
                        "top_asns",
                        lambda: service.client.execute(top_asn_sql, {**params, "h_start": h_start, "h_end": h_end}),
                        source="projection",
                    )
                else:
                    top_asn_sql = (
                        f"SELECT toInt32OrNull(value) AS asn, sum(request_count) AS cnt FROM request_aggregates "
                        f"WHERE service_id={{service_id:String}} AND dimension='asn' AND value != '' "
                        f"AND bucket_start >= toStartOfMinute({{start:DateTime64(3)}}) AND bucket_start < {{end:DateTime64(3)}} AND {agg_fence} "
                        f"GROUP BY asn HAVING asn IS NOT NULL ORDER BY cnt DESC LIMIT {top_n}"
                    )
                    top_asn_rows = trace.run(
                        "top_asns",
                        lambda: service.client.execute(top_asn_sql, params),
                        source="projection",
                    )
                top_asn_ints = [int(r["asn"]) for r in top_asn_rows if r.get("asn") is not None][:top_n]
            if not map_only and not top_asn_ints:
                speed_sql = (
                    f"SELECT {_ASN} AS asn, custom_fields['c_speed'] AS c_speed, count() AS cnt "
                    f"FROM request_facts PREWHERE {rtt_prewhere} AND custom_fields['asn'] != '' "
                    f"WHERE {_ASN} IS NOT NULL AND {_RTT} > 0 GROUP BY asn, c_speed ORDER BY cnt DESC LIMIT {top_n * 20}"
                )
                raw_speed_rows = trace.run("speed", lambda: service.client.execute(speed_sql, params), source="fact")
                asn_totals: dict[int, int] = {}
                for r in raw_speed_rows:
                    if r["asn"] is not None:
                        a_int = int(r["asn"])
                        asn_totals[a_int] = asn_totals.get(a_int, 0) + int(r["cnt"])
                top_asn_ints = sorted(asn_totals, key=lambda a: asn_totals[a], reverse=True)[:top_n]
            asn_str_in = ", ".join(repr(str(a)) for a in top_asn_ints)
            asn_in_filter = f" AND custom_fields['asn'] IN ({asn_str_in})" if top_asn_ints else " AND 1=0"
            combine_metro_in_map = map_only or map_asn == "all"
            fast_queries_list: list[tuple[str, str]] = [
                (
                    "map",
                    f"WITH {_RTT} AS _rtt "
                    f"SELECT country AS country, custom_fields['city'] AS city, custom_fields['region'] AS region, "
                    f"custom_fields['metro'] AS metro, {bucket_expr} AS bucket_ts, "
                    "anyIf(custom_fields['lat'], custom_fields['lat'] != '') AS lat, "
                    "anyIf(custom_fields['lon'], custom_fields['lon'] != '') AS lon, "
                    f"quantilesTDigest(0.1, 0.5)(_rtt) AS rtt_quants, "
                    f"avg({_PLOSS}) AS avg_ploss, {err_pct_expr}, count() AS reqs "
                    f"FROM request_facts PREWHERE {rtt_prewhere} AND country != ''{map_asn_sql} "
                    "WHERE _rtt > 0 "
                    "GROUP BY country, city, region, metro, bucket_ts ORDER BY bucket_ts ASC, reqs DESC LIMIT 100 BY bucket_ts LIMIT 20000",
                ),
            ]
            if not map_only:
                fast_queries_list.insert(
                    0,
                    (
                        "heatmap",
                        f"WITH {_RTT} AS _rtt, {_RTT_MIN} AS _rtt_min, {_RTT_VAR} AS _rtt_var, {_ELAPSED} AS _elapsed "
                        f"SELECT {_ASN} AS asn, {bucket_expr} AS bucket_ts, count() AS reqs, "
                        f"quantileTDigest(0.5)(if(position(custom_fields['cache'], 'HIT') > 0 AND _elapsed > 0, {_RESP_BYTES} * 1000000.0 / _elapsed, NULL)) AS throughput_bps, "
                        f"avg(_rtt) AS rtt_avg_us, quantilesTDigest(0.1, 0.5, 0.95, 0.99)(_rtt) AS rtt_quants, "
                        f"quantileTDigest(0.5)(_rtt_min) AS rtt_baseline_us, "
                        f"quantileTDigest(0.5)(_rtt - _rtt_min) AS rtt_congestion_us, avg({_PLOSS}) AS avg_ploss, "
                        f"quantileTDigest(0.5)(_rtt_var) AS rtt_jitter_us, {err_pct_expr}, "
                        "sumMap(if(custom_fields['c_speed'] != '', [custom_fields['c_speed']], []), "
                        "if(custom_fields['c_speed'] != '', [toUInt64(1)], [])) AS speed_map "
                        f"FROM request_facts PREWHERE {rtt_prewhere}{asn_in_filter} "
                        f"WHERE {'_rtt > 0' if top_asn_ints else f'{_ASN} IS NOT NULL AND _rtt > 0'} GROUP BY asn, bucket_ts ORDER BY reqs DESC LIMIT {top_n * 200}",
                    ),
                )
            if not combine_metro_in_map:
                fast_queries_list.append(
                    (
                        "metro",
                        f"WITH {_RTT} AS _rtt "
                        f"SELECT country AS country, custom_fields['city'] AS city, custom_fields['region'] AS region, "
                        f"custom_fields['metro'] AS metro, "
                        f"quantilesTDigest(0.1, 0.5)(_rtt) AS rtt_quants, "
                        f"avg({_PLOSS}) AS avg_ploss, {err_pct_expr}, count() AS total_reqs "
                        f"FROM request_facts PREWHERE {rtt_prewhere} AND country != '' "
                        "WHERE _rtt > 0 "
                        "GROUP BY country, city, region, metro ORDER BY total_reqs DESC LIMIT 200",
                    )
                )
            parallel_tasks: list[tuple[str, Callable[[], Any]]] = [
                (name, partial(service.client.execute, sql, params)) for name, sql in fast_queries_list
            ]
            if not map_only and _want_shielding(sections):
                parallel_tasks.append(
                    (
                        "shielding_analysis",
                        lambda: _shielding_analysis(service, start, end, getattr(req, "filters", None)),
                    )
                )
            q_results = trace.run_parallel(
                parallel_tasks,
                source="fact",
                max_workers=2,
                thread_name_prefix="hs-net-health",
            )
            if sample_stride > 1 and not q_results.get("heatmap") and not q_results.get("map"):
                fallback_tasks = [
                    (f"{name}_fallback", partial(service.client.execute, sql.replace(stride_sql, ""), params))
                    for name, sql in fast_queries_list
                ]
                fb_res = trace.run_parallel(
                    fallback_tasks,
                    source="fact",
                    max_workers=2,
                    thread_name_prefix="hs-net-health-fb",
                )
                for name, _ in fast_queries_list:
                    q_results[name] = fb_res.get(f"{name}_fallback", [])
                sample_stride = 1
            shielding = q_results.get("shielding_analysis") if (not map_only and _want_shielding(sections)) else None

            def _unpack_rtt_quants(row: dict[str, Any], expect: int) -> dict[str, Any]:
                quants = row.get("rtt_quants")
                if isinstance(quants, (list, tuple)) and len(quants) == expect:
                    if expect == 4:
                        return {
                            **row,
                            "rtt_p10_us": quants[0],
                            "rtt_med_us": quants[1],
                            "rtt_p95_us": quants[2],
                            "rtt_p99_us": quants[3],
                        }
                    return {**row, "rtt_p10_us": quants[0], "rtt_med_us": quants[1]}
                return row

            heatmap_rows = [_unpack_rtt_quants(r, 4) for r in q_results.get("heatmap", [])]
            raw_map_rows = [_unpack_rtt_quants(r, 2) for r in q_results.get("map", [])]
            if not combine_metro_in_map:
                q_results["metro"] = [_unpack_rtt_quants(r, 2) for r in q_results.get("metro", [])]
            pct_acc: dict[int, list[float]] = {}
            speed_acc: dict[tuple[int, str], int] = {}
            for hr in heatmap_rows:
                if hr.get("asn") is None:
                    continue
                a_int = int(hr["asn"])
                rq = float(hr.get("reqs") or 0)
                if rq > 0:
                    acc = pct_acc.setdefault(a_int, [0.0, 0.0, 0.0, 0.0])
                    if hr.get("rtt_p95_us") is not None:
                        acc[0] += float(hr["rtt_p95_us"]) * rq
                        acc[1] += rq
                    if hr.get("rtt_p99_us") is not None:
                        acc[2] += float(hr["rtt_p99_us"]) * rq
                        acc[3] += rq
                sm = hr.get("speed_map")
                pairs: list[tuple[Any, Any]]
                if isinstance(sm, Mapping):
                    pairs = list(sm.items())
                elif isinstance(sm, (list, tuple)) and len(sm) == 2 and isinstance(sm[0], (list, tuple)):
                    pairs = list(zip(sm[0], sm[1], strict=False))
                else:
                    pairs = []
                for cs, cnt in pairs:
                    if cs:
                        k_cs = (a_int, str(cs))
                        speed_acc[k_cs] = speed_acc.get(k_cs, 0) + int(cnt) * sample_stride
            pct_rows = [
                {
                    "asn": a_int,
                    "p95": (v[0] / v[1]) if v[1] > 0 else None,
                    "p99": (v[2] / v[3]) if v[3] > 0 else None,
                }
                for a_int, v in pct_acc.items()
            ]
            if sample_stride > 1:
                heatmap_rows = [{**r, "reqs": int(r.get("reqs") or 0) * sample_stride} for r in heatmap_rows]
                raw_map_rows = [{**mr, "reqs": int(mr.get("reqs") or 0) * sample_stride} for mr in raw_map_rows]
            coord_lookup: dict[tuple[str, str], tuple[Any, Any]] = {}
            for mr in raw_map_rows:
                if _flt(mr.get("lat")) is not None and _flt(mr.get("lon")) is not None:
                    ckey = (str(mr.get("country") or ""), str(mr.get("city") or ""))
                    if ckey not in coord_lookup:
                        coord_lookup[ckey] = (mr.get("lat"), mr.get("lon"))
            map_rows = [
                {
                    **mr,
                    "lat": mr.get("lat")
                    if _flt(mr.get("lat")) is not None
                    else coord_lookup.get((str(mr.get("country") or ""), str(mr.get("city") or "")), (None, None))[0],
                    "lon": mr.get("lon")
                    if _flt(mr.get("lon")) is not None
                    else coord_lookup.get((str(mr.get("country") or ""), str(mr.get("city") or "")), (None, None))[1],
                }
                for mr in raw_map_rows
            ]
            if raw_speed_rows is not None:
                speed_rows = [
                    {**r, "cnt": int(r.get("cnt") or 0) * sample_stride} for r in raw_speed_rows if r.get("c_speed")
                ]
            else:
                speed_rows = [{"asn": a_int, "c_speed": cs, "cnt": cnt} for (a_int, cs), cnt in speed_acc.items()]
            metro_rows: list[dict[str, Any]]
            if combine_metro_in_map:
                metro_acc: dict[tuple[str, str, str, str], list[float]] = {}
                for mr in map_rows:
                    key = (
                        str(mr.get("country") or ""),
                        str(mr.get("city") or ""),
                        str(mr.get("region") or ""),
                        str(mr.get("metro") or ""),
                    )
                    acc = metro_acc.setdefault(key, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
                    rq = float(mr.get("reqs") or 0)
                    acc[0] += rq
                    if mr.get("avg_ploss") is not None:
                        acc[1] += float(mr["avg_ploss"]) * rq
                        acc[2] += rq
                    if mr.get("error_pct") is not None:
                        acc[3] += float(mr["error_pct"]) * rq
                    if mr.get("rtt_med_us") is not None:
                        acc[4] += float(mr["rtt_med_us"]) * rq
                        acc[5] += rq
                    if mr.get("rtt_p10_us") is not None:
                        acc[6] += float(mr["rtt_p10_us"]) * rq
                        acc[7] += rq
                metro_rows = [
                    {
                        "country": k[0],
                        "city": k[1],
                        "region": k[2],
                        "metro": k[3],
                        "total_reqs": int(v[0]),
                        "avg_ploss": (v[1] / v[2]) if v[2] > 0 else None,
                        "error_pct": (v[3] / v[0]) if v[0] > 0 else 0.0,
                        "rtt_med_us": (v[4] / v[5]) if v[5] > 0 else None,
                        "rtt_p10_us": (v[6] / v[7]) if v[7] > 0 else None,
                    }
                    for k, v in sorted(metro_acc.items(), key=lambda kv: kv[1][0], reverse=True)[:200]
                ]
            else:
                metro_rows = [
                    {**r, "total_reqs": int(r.get("total_reqs") or 0) * sample_stride}
                    for r in q_results.get("metro", [])
                ]
        else:
            top_asn_subquery = (
                f"SELECT {_ASN} FROM request_facts "
                f"WHERE {rtt_where} AND {_ASN} IS NOT NULL "
                f"GROUP BY {_ASN} ORDER BY count() DESC LIMIT {top_n}"
            )
            health_queries: tuple[tuple[str, str], ...] = (
                (
                    "heatmap",
                    _ch_fact_query(
                        f"{_ASN} AS asn, {bucket_expr} AS bucket_ts, count() AS reqs, "
                        f"median(if({_NET_HIT} AND {_ELAPSED} > 0, {_RESP_BYTES} * 1000000.0 / {_ELAPSED}, NULL)) AS throughput_bps, "
                        f"avg({_RTT}) AS rtt_avg_us, median({_RTT}) AS rtt_med_us, quantile(0.1)({_RTT}) AS rtt_p10_us, "
                        f"median({_RTT_MIN}) AS rtt_baseline_us, "
                        f"median({_RTT} - {_RTT_MIN}) AS rtt_congestion_us, avg({_PLOSS}) AS avg_ploss, "
                        f"median({_RTT_VAR}) AS rtt_jitter_us, {err_pct_expr}",
                        f"{rtt_where} AND {_ASN} IS NOT NULL AND {_ASN} IN ({top_asn_subquery})",
                        group_by="asn, bucket_ts",
                        order_by="reqs DESC",
                        limit=top_n * 200,
                    ),
                ),
                (
                    "map",
                    _ch_fact_query(
                        "country AS country, custom_fields['city'] AS city, "
                        "any(toFloat64OrNull(custom_fields['lat'])) AS lat, any(toFloat64OrNull(custom_fields['lon'])) AS lon, "
                        f"custom_fields['metro'] AS metro, {bucket_expr} AS bucket_ts, "
                        f"median({_RTT}) AS rtt_med_us, quantile(0.1)({_RTT}) AS rtt_p10_us, "
                        f"avg({_PLOSS}) AS avg_ploss, {err_pct_expr}, count() AS reqs",
                        f"{rtt_where} AND country != ''{map_asn_sql}",
                        group_by="country, city, metro, bucket_ts",
                        order_by="bucket_ts ASC, reqs DESC LIMIT 50 BY bucket_ts",
                        limit=15000,
                    ),
                ),
                (
                    "metro",
                    _ch_fact_query(
                        "country AS country, custom_fields['city'] AS city, custom_fields['region'] AS region, "
                        f"custom_fields['metro'] AS metro, median({_RTT}) AS rtt_med_us, quantile(0.1)({_RTT}) AS rtt_p10_us, "
                        f"avg({_PLOSS}) AS avg_ploss, {err_pct_expr}, count() AS total_reqs",
                        f"{rtt_where} AND country != ''",
                        group_by="country, city, region, metro",
                        order_by="total_reqs DESC",
                        limit=200,
                    ),
                ),
                (
                    "speed",
                    _ch_fact_query(
                        f"{_ASN} AS asn, custom_fields['c_speed'] AS c_speed, count() AS cnt",
                        f"{rtt_where} AND {_ASN} IS NOT NULL AND custom_fields['c_speed'] != ''",
                        group_by="asn, c_speed",
                        order_by="cnt DESC",
                        limit=top_n * 20,
                    ),
                ),
                (
                    "percentiles",
                    _ch_fact_query(
                        f"{_ASN} AS asn, quantile(0.95)({_RTT}) AS p95, quantile(0.99)({_RTT}) AS p99",
                        f"{rtt_where} AND {_ASN} IS NOT NULL",
                        group_by="asn",
                        order_by="count() DESC",
                        limit=top_n * 5,
                    ),
                ),
            )
            active_queries = tuple((name, sql) for name, sql in health_queries if not map_only or name == "map")
            parallel_tasks = [(name, partial(service.client.execute, sql, params)) for name, sql in active_queries]
            if _want_shielding(sections):
                parallel_tasks.append(
                    (
                        "shielding_analysis",
                        lambda: _shielding_analysis(service, start, end, getattr(req, "filters", None)),
                    )
                )
            q_results = trace.run_parallel(
                parallel_tasks,
                source="fact",
                max_workers=2,
                thread_name_prefix="hs-net-health",
            )
            shielding = q_results.get("shielding_analysis") if _want_shielding(sections) else None
            heatmap_rows, map_rows, metro_rows, speed_rows, pct_rows = (
                q_results.get(k, []) for k in ("heatmap", "map", "metro", "speed", "percentiles")
            )

        all_asns_seen: dict[int, int] = {}
        for r in heatmap_rows:
            if r["asn"] is not None:
                all_asns_seen[int(r["asn"])] = all_asns_seen.get(int(r["asn"]), 0) + int(r["reqs"])
        top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
        top_asn_set, asn_labels = set(top_asns), resolve_asn_display_labels(service.service_id, top_asns)

        all_buckets = (
            generate_epoch_aligned_buckets(start, end - timedelta(microseconds=1), bs)
            if start is not None and end is not None and end > start
            else []
        )
        bucket_idx = {b: i for i, b in enumerate(all_buckets)}

        asn_speed_mix = build_asn_speed_mix(
            (r["asn"], r["c_speed"], r["cnt"]) for r in speed_rows if r["asn"] is not None
        )
        asn_rtt_pct = {
            int(r["asn"]): {"p95_rtt_us": _flt(r["p95"]), "p99_rtt_us": _flt(r["p99"])}
            for r in pct_rows
            if r["asn"] is not None
        }

        asn_bucket_data: dict[int, dict[str, dict]] = {}
        for r in heatmap_rows:
            if (
                r["asn"] is None
                or (asn := int(r["asn"])) not in top_asn_set
                or (bucket := _fmt_ts(int(r["bucket_ts"]))) not in bucket_idx
            ):
                continue
            tp, rtt_med, rtt_cong, pkt, jitter, err = (
                _flt(r.get("throughput_bps")),
                _flt(r.get("rtt_med_us")),
                _flt(r["rtt_congestion_us"]),
                _flt(r["avg_ploss"]),
                _flt(r["rtt_jitter_us"]),
                _flt(r["error_pct"]),
            )
            rtt_base = _flt(r.get("rtt_p10_us"))
            if rtt_base is None:
                rtt_base = _flt(r.get("rtt_baseline_us"))
            hs = (
                _health_score(rtt_med, max(1.0, rtt_base if rtt_base is not None else rtt_med), pkt, err)
                if rtt_med is not None
                else _health_score(tp, rtt_cong, pkt, jitter, err)
            )
            asn_bucket_data.setdefault(asn, {})[bucket] = {
                "bucket_idx": bucket_idx[bucket],
                "bucket": bucket,
                "throughput_bps": _flt(tp, 0),
                "rtt_med_us": _flt(rtt_med, 0),
                "rtt_baseline_us": _flt(r["rtt_baseline_us"], 0),
                "rtt_congestion_us": _flt(rtt_cong, 0),
                "avg_ploss": _flt(pkt, 5),
                "rtt_jitter_us": _flt(jitter, 0),
                "error_pct": _flt(err, 2),
                "health_score": hs,
                "reqs": int(r["reqs"]),
            }

        heatmap = [
            {
                "asn": asn,
                "label": asn_labels.get(asn, f"AS{asn}"),
                "total_reqs": all_asns_seen.get(asn, 0),
                "buckets": list(asn_bucket_data.get(asn, {}).values()),
            }
            for asn in top_asns
        ]

        has_metro = any((r.get("metro") or "") for r in (*map_rows, *metro_rows))
        dma_map: dict = {}
        if has_metro:
            try:
                from backend.core import fos_client as _db

                dma_map = _db._get_dma_map()
            except Exception:
                dma_map = {}

        cities_list: list[dict[str, Any]] = []
        cities_index: dict[tuple[str, float | None, float | None], int] = {}

        def _intern_city(name: str, lat: float | None, lon: float | None) -> int:
            if (idx := cities_index.get((name, lat, lon))) is None:
                idx = cities_index[(name, lat, lon)] = len(cities_list)
                cities_list.append({"name": name, "lat": lat, "lon": lon})
            return idx

        def _geo_health_score(row: Mapping[str, Any], pkt: float | None, err: float | None) -> float:
            rtt_med = _flt(row.get("rtt_med_us"))
            if rtt_med is None:
                return _health_score(None, None, pkt, None, err)
            rtt_base = _flt(row.get("rtt_p10_us"))
            if rtt_base is None:
                rtt_base = _flt(row.get("rtt_baseline_us"))
            return _health_score(rtt_med, max(1.0, rtt_base if rtt_base is not None else rtt_med), pkt, err)

        map_by_bucket: dict[str, list[dict]] = {}
        countries_set: set[str] = set()
        # Accumulate full-window per-country health scores across all map_rows before
        # per-bucket LIMIT 50 truncation so worst_country does not shift with bucket_seconds (NET-02).
        country_health_agg: dict[str, list[float]] = {}
        for r in map_rows:
            if ctry := (r["country"] or ""):
                countries_set.add(ctry)
            pkt, err = _flt(r["avg_ploss"]), _flt(r["error_pct"])
            row_reqs = int(r["reqs"])
            row_hs = _geo_health_score(r, pkt, err)
            if ctry and row_reqs > 0:
                c_acc = country_health_agg.setdefault(ctry, [0.0, 0.0])
                c_acc[0] += row_hs * row_reqs
                c_acc[1] += row_reqs
            if (bucket := _fmt_ts(int(r["bucket_ts"]))) not in bucket_idx:
                continue
            metro_code, city = _parse_metro_code(r.get("metro")), r["city"] or ""
            display_city = (
                dma_map.get(str(metro_code), city.title() if city else "")
                if metro_code is not None
                else (city.title() if city else "")
            )
            bucket_list = map_by_bucket.setdefault(bucket, [])
            if len(bucket_list) < 50:
                bucket_list.append(
                    {
                        "country": ctry,
                        "city_idx": _intern_city(display_city, _flt(r["lat"]), _flt(r["lon"])),
                        "metro_code": metro_code,
                        "rtt_med_us": _flt(r["rtt_med_us"]),
                        "avg_ploss": pkt,
                        "error_pct": _flt(err, 2),
                        "health_score": row_hs,
                        "reqs": row_reqs,
                    }
                )

        map_buckets = (
            [{"bucket_idx": i, "bucket": b, "cities": map_by_bucket.get(b, [])} for i, b in enumerate(all_buckets)]
            if map_by_bucket
            else []
        )

        raw_metro_entries: list[dict[str, Any]] = []
        for r in metro_rows:
            ctry, city, region = r["country"] or "", r["city"] or "", r["region"] or ""
            pkt, err = _flt(r["avg_ploss"]), _flt(r["error_pct"])
            hs, metro_code, reqs = (
                _geo_health_score(r, pkt, err),
                _parse_metro_code(r.get("metro")),
                int(r["total_reqs"]),
            )
            city_name = dma_map[str(metro_code)] if metro_code is not None and str(metro_code) in dma_map else city
            display = format_city_label(city_name, ctry, region)
            raw_metro_entries.append(
                {
                    "country": ctry,
                    "region": region,
                    "raw_city": city,
                    "city": display,
                    "total_reqs": reqs,
                    "health_score": hs,
                }
            )
        metro_leaderboard = deduplicate_metro_leaderboard(raw_metro_entries)

        leaderboard = build_network_asn_leaderboard(
            top_asns=top_asns,
            asn_bucket_data=asn_bucket_data,
            all_buckets=all_buckets,
            bucket_seconds=bs,
            all_asns_seen=all_asns_seen,
            asn_speed_mix=asn_speed_mix,
            asn_rtt_pct=asn_rtt_pct,
            asn_labels=asn_labels,
        )

        rtt_pairs = [
            (
                float(r["rtt_avg_us"]) if r.get("rtt_avg_us") is not None else float(r["rtt_med_us"]),
                max(0, int(r.get("reqs") or 0)),
            )
            for r in heatmap_rows
            if (r.get("rtt_avg_us") if r.get("rtt_avg_us") is not None else r.get("rtt_med_us")) is not None
        ]
        raw_summary = compute_network_global_summary(
            leaderboard=leaderboard,
            rtt_values=[v for v, _ in rtt_pairs],
            rtt_weights=[w for _, w in rtt_pairs],
            total_reqs=sum(int(r["reqs"]) for r in heatmap_rows),
            map_buckets=map_buckets,
            cities_list=cities_list,
            zero_as_int=False,
        )
        worst_country_entry = raw_summary["worst_country"]
        if country_health_agg:
            total_country_reqs = sum(v[1] for v in country_health_agg.values())
            scored_countries = [
                (ctry, round(v[0] / v[1], 1), v[1]) for ctry, v in country_health_agg.items() if v[1] > 0
            ]
            sig_countries = [c for c in scored_countries if c[2] > total_country_reqs * 0.01]
            pool = sig_countries or scored_countries
            if pool:
                wc_code, wc_score, _ = min(pool, key=lambda c: c[1])
                worst_country_entry = {"label": COUNTRY_MAP.get(wc_code, wc_code), "score": wc_score}
        summary = NetworkHealthSummary(
            global_health_score=raw_summary["global_health_score"],
            avg_rtt_ms=raw_summary["avg_rtt_ms"],
            total_reqs=raw_summary["total_reqs"],
            worst_asn=NetworkWorstEntry(**raw_summary["worst_asn"]) if raw_summary["worst_asn"] else None,
            worst_country=NetworkWorstEntry(**worst_country_entry) if worst_country_entry else None,
        )
        want_map = not sections or bool(set(sections) & {"health", "map_buckets", "cities"})
        req_metric = str(getattr(req, "metric", "health_score") or "health_score")
        payload: dict[str, Any] = {
            "available": True,
            "has_data": bool(heatmap_rows or map_rows),
            "section_timings": trace.timings(),
            "metric": req_metric,
            "bucket_seconds": bs,
            "buckets": all_buckets,
            "heatmap": heatmap,
            "map_buckets": map_buckets if want_map else [],
            "cities": [NetworkCityEntry(**c) for c in cities_list] if want_map else [],
            "leaderboard": leaderboard,
            "metro_leaderboard": metro_leaderboard,
            "summary": summary,
            "countries": sorted(countries_set),
            "has_metro": has_metro,
            "shielding_analysis": shielding,
        }
        _network_memo.put(memo_key, dict(payload))
        resp = NetworkHealthResponse.with_telemetry(
            **_network_memo.result(payload, state="miss"),
        )
        _network_model_cache[memo_key] = resp
        if is_real_ch:
            dumped = resp.model_dump(mode="json", by_alias=True)
            _network_json_cache[memo_key] = dumped
            object.__setattr__(resp, "_cached_json_dict", dumped)
        return resp

    if is_real_ch:
        resp, is_leader = coalesce(f"hs_net_health:{memo_key}", _compute_health)
        if not is_leader and (follower_hit := _fast_model_hit(memo_key)) is not None:
            return follower_hit
        return resp
    return _compute_health()


def _unpack_sum_map(val: Any) -> list[tuple[Any, Any]]:
    if isinstance(val, Mapping):
        return list(val.items())
    if isinstance(val, (list, tuple)) and len(val) == 2 and isinstance(val[0], (list, tuple)):
        return list(zip(val[0], val[1], strict=False))
    return []


def network_quality(
    service: HighScaleService,
    req: Any,
    start_time: str | None,
    end_time: str | None,
    *,
    pop_rollup_sink: Future[dict[str, tuple[int, int, Any, Any]] | None] | None = None,
) -> NetworkQualityResponse:
    start, end = parse_time_window(start_time, end_time)
    region_country = getattr(req, "region_country", "US") or "US"
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    pop_rollup_cache_key: tuple[Any, ...] | None = (
        (
            service.service_id,
            client_cache_token(service.client),
            _cache_minute_bound(start),
            _cache_minute_bound(end),
        )
        if is_real_ch and start_time is not None and end_time is not None and not getattr(req, "filters", None)
        else None
    )

    def _finish_pop_rollup(val: dict[str, tuple[int, int, Any, Any]] | None) -> None:
        if pop_rollup_sink is not None and not pop_rollup_sink.done():
            pop_rollup_sink.set_result(val)

    try:
        memo_key: str | None = None
        if is_real_ch and start_time is not None and end_time is not None:
            memo_key = digest_cache_key(
                {
                    "ep": "hs_network_quality",
                    "cid": client_cache_token(service.client),
                    "s": _cache_minute_bound(start_time),
                    "e": _cache_minute_bound(end_time),
                    "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
                    "rc": str(region_country),
                },
                {"name": service.service_id, "service_id": service.service_id},
            )
            if (cached := _network_memo.get_with_telemetry(memo_key)) is not None:
                _finish_pop_rollup(
                    _pop_rollup_cache.get(pop_rollup_cache_key) if pop_rollup_cache_key is not None else None
                )
                return NetworkQualityResponse.with_telemetry(**cached)

        def _compute_quality() -> dict[str, Any]:
            if memo_key is not None and (cached_inner := _network_memo.get_with_telemetry(memo_key)) is not None:
                _finish_pop_rollup(
                    _pop_rollup_cache.get(pop_rollup_cache_key) if pop_rollup_cache_key is not None else None
                )
                return cached_inner
            filter_sql, filter_params = _filter_clause(getattr(req, "filters", None))
            params = {
                "service_id": service.service_id,
                "start": start,
                "end": end,
                "region_country": region_country,
                **filter_params,
            }
            rtt_where = f"{_RTT_FILTER}{filter_sql}"

            def _bar(group_expr: str, extra_where: str = "") -> list[dict]:
                q_span_s = (end - start).total_seconds() if start is not None and end is not None else 0.0
                scale_factor = _network_sample_stride(q_span_s) if is_real_ch else 1
                hash_clause = _network_time_sample_clause(scale_factor, start=start, end=end)
                if is_real_ch and group_expr != _ASN:
                    sql = (
                        f"SELECT {group_expr} AS label, avg({_RTT} / 1000.0) AS rtt_ms, count() AS reqs "
                        f"FROM request_facts PREWHERE {_BASE_WHERE}{filter_sql}{extra_where}{hash_clause} "
                        f"AND {group_expr} != '' AND custom_fields['tcp_rtt'] != '' AND custom_fields['tcp_rtt'] != '0' "
                        f"WHERE {_RTT} > 0 GROUP BY label ORDER BY reqs DESC LIMIT 25"
                    )
                elif is_real_ch:
                    sql = (
                        f"SELECT {group_expr} AS label, avg({_RTT} / 1000.0) AS rtt_ms, count() AS reqs "
                        f"FROM request_facts PREWHERE {_BASE_WHERE}{filter_sql}{extra_where}{hash_clause} "
                        f"AND custom_fields['asn'] != '' AND custom_fields['tcp_rtt'] != '' AND custom_fields['tcp_rtt'] != '0' "
                        f"WHERE {group_expr} IS NOT NULL AND {_RTT} > 0 GROUP BY label ORDER BY reqs DESC LIMIT 25"
                    )
                else:
                    sql = _ch_fact_query(
                        f"{group_expr} AS label, avg({_RTT} / 1000.0) AS rtt_ms, count() AS reqs",
                        f"{rtt_where}{extra_where}",
                        group_by="label",
                        order_by="reqs DESC",
                        limit=25,
                    )
                rows = service.client.execute(sql, params)
                if scale_factor > 1 and not rows:
                    scale_factor = 1
                    rows = service.client.execute(sql.replace(hash_clause, ""), params)
                return [
                    {
                        "value": str(r["label"]),
                        "label": str(r["label"]),
                        "rtt_ms": _flt(r["rtt_ms"], 2) or 0.0,
                        "reqs": int(r["reqs"]) * scale_factor,
                    }
                    for r in rows
                    if r["label"] is not None and r["label"] != ""
                ]

            def _bars_from_rollup_and_region() -> dict[str, list[dict]]:
                fence = visible_publication_fence_sql(
                    "network_dimensions", client=service.client, service_id=service.service_id
                )
                q_span_s = (end - start).total_seconds() if start is not None and end is not None else 0.0
                region_scale = _network_sample_stride(q_span_s)
                region_hash = _network_time_sample_clause(region_scale, start=start, end=end)
                want_pop_rollup = pop_rollup_sink is not None or (
                    pop_rollup_cache_key is not None and _pop_rollup_cache.get(pop_rollup_cache_key) is None
                )
                pop_cols = (
                    ", toUInt64(sum(requests)) AS pop_reqs, "
                    "toUInt64(sum(errors)) AS pop_errors, "
                    "sum(tcp_rtt_p50_us * tcp_rtt_count) / nullIf(sum(tcp_rtt_count), 0) AS p50_rtt_us, "
                    "sum(ttfb_p95_us * requests) / nullIf(sumIf(requests, ttfb_p95_us IS NOT NULL), 0) * 1000.0 AS p95_ttfb_ms"
                    if want_pop_rollup
                    else ""
                )
                pop_zero = (
                    ", toUInt64(0) AS pop_reqs, toUInt64(0) AS pop_errors, "
                    "CAST(NULL, 'Nullable(Float64)') AS p50_rtt_us, "
                    "CAST(NULL, 'Nullable(Float64)') AS p95_ttfb_ms"
                    if want_pop_rollup
                    else ""
                )
                outer_cols = (
                    "dimension, label, rtt_ms, reqs, pop_reqs, pop_errors, p50_rtt_us, p95_ttfb_ms"
                    if want_pop_rollup
                    else "dimension, label, rtt_ms, reqs"
                )
                dim_where = (
                    f"(dimension = 'pop' OR tcp_rtt_count > 0) AND {fence}"
                    if want_pop_rollup
                    else f"tcp_rtt_count > 0 AND {fence}"
                )
                dim_having = "(reqs > 0 OR dimension = 'pop')" if want_pop_rollup else "reqs > 0"
                dim_limit = 200 if want_pop_rollup else 25
                sql = (
                    f"SELECT {outer_cols} FROM ("
                    "SELECT dimension, value AS label, "
                    "coalesce(sum(tcp_rtt_sum), sum(tcp_rtt_p50_us * tcp_rtt_count)) / nullIf(sum(tcp_rtt_count), 0) / 1000.0 AS rtt_ms, "
                    f"toUInt64(sum(tcp_rtt_count)) AS reqs{pop_cols} "
                    "FROM network_minute_dimensions "
                    "PREWHERE service_id={service_id:String} "
                    "AND bucket_start >= {start:DateTime64(3)} AND bucket_start < {end:DateTime64(3)} "
                    "AND dimension IN ('country', 'asn', 'pop') AND value != '' "
                    f"WHERE {dim_where} "
                    f"GROUP BY dimension, label HAVING {dim_having} "
                    f"ORDER BY dimension ASC, reqs DESC LIMIT {dim_limit} BY dimension) "
                    "UNION ALL "
                    f"SELECT {outer_cols} FROM ("
                    f"SELECT 'region' AS dimension, custom_fields['region'] AS label, avg({_RTT} / 1000.0) AS rtt_ms, "
                    f"toUInt64(count() * {region_scale}) AS reqs{pop_zero} "
                    f"FROM request_facts PREWHERE {_BASE_WHERE}{region_hash} AND country = {{region_country:String}} "
                    "AND custom_fields['region'] != '' AND custom_fields['tcp_rtt'] != '' AND custom_fields['tcp_rtt'] != '0' "
                    f"WHERE {_RTT} > 0 GROUP BY label ORDER BY reqs DESC LIMIT 25)"
                )
                out: dict[str, list[dict]] = {"country": [], "asn": [], "pop": [], "region": []}
                pop_rollup: dict[str, tuple[int, int, Any, Any]] = {}
                for r in service.client.execute(sql, params):
                    dim = str(r.get("dimension") or "")
                    lbl = str(r.get("label") or "")
                    if not lbl:
                        continue
                    reqs_val = int(r.get("reqs") or 0)
                    if dim == "pop" and want_pop_rollup:
                        pop_reqs = int(r.get("pop_reqs") or reqs_val)
                        pop_rollup[lbl] = (
                            pop_reqs,
                            int(r.get("pop_errors") or 0),
                            r.get("p50_rtt_us"),
                            r.get("p95_ttfb_ms"),
                        )
                    if dim in out and reqs_val > 0:
                        out[dim].append(
                            {
                                "value": lbl,
                                "label": lbl,
                                "rtt_ms": _flt(r["rtt_ms"], 2) or 0.0,
                                "reqs": reqs_val,
                            }
                        )
                for dim, dim_rows in out.items():
                    dim_rows.sort(key=lambda row: row["reqs"], reverse=True)
                    if len(dim_rows) > 25:
                        out[dim] = dim_rows[:25]
                if want_pop_rollup:
                    if pop_rollup_cache_key is not None and pop_rollup:
                        _pop_rollup_cache[pop_rollup_cache_key] = pop_rollup
                    _finish_pop_rollup(pop_rollup)
                return out

            scatter_sql = _ch_fact_query(
                f"{_RTT} / 1000.0 AS rtt_ms, {_TTFB} * 1000.0 AS ttfb_ms, "
                "if(custom_fields['cache'] = '', 'UNKNOWN', custom_fields['cache']) AS cache_state",
                f"{rtt_where} AND {_TTFB} > 0",
                order_by="cityHash64(event_id)",
                limit=_SCATTER_SAMPLE,
            )

            def _run_scatter() -> list[dict[str, Any]]:
                if is_real_ch:

                    def _scatter_query(sample_clause: str) -> str:
                        return (
                            f"SELECT {_RTT} / 1000.0 AS rtt_ms, {_TTFB} * 1000.0 AS ttfb_ms, "
                            "if(custom_fields['cache'] = '', 'UNKNOWN', custom_fields['cache']) AS cache_state "
                            f"FROM request_facts PREWHERE {_BASE_WHERE}{filter_sql} "
                            f"AND custom_fields['tcp_rtt'] != '' AND custom_fields['tcp_rtt'] != '0' "
                            f"AND custom_fields['ttfb'] != '' AND custom_fields['ttfb'] != '0'{sample_clause} "
                            f"WHERE {_RTT} > 0 AND {_TTFB} > 0 LIMIT {_SCATTER_SAMPLE}"
                        )

                    q_span_s = (end - start).total_seconds() if start is not None and end is not None else 0.0
                    ord_stride = 16 if q_span_s <= 86400 else 64
                    fast_rows = service.client.execute(_scatter_query(f" AND line_ordinal % {ord_stride} = 0"), params)
                    if len(fast_rows) >= 50:
                        return fast_rows
                    return service.client.execute(_scatter_query(""), params)
                return service.client.execute(scatter_sql, params)

            trace = HighScaleSections(service.service_id, "network.quality")
            if is_real_ch and not filter_sql:
                q_res_mixed: dict[str, Any] = trace.run_parallel(
                    [
                        ("rollup_bars", _bars_from_rollup_and_region),
                        ("scatter", _run_scatter),
                    ],
                    source="projection",
                    max_workers=2,
                    thread_name_prefix="hs-net-quality",
                )
                rollup_bars = q_res_mixed["rollup_bars"]
                by_country = rollup_bars.get("country", [])
                by_asn = rollup_bars.get("asn") or _bar(_ASN, "")
                by_pop = rollup_bars.get("pop", [])
                by_region = rollup_bars.get("region", [])
                scatter_rows = q_res_mixed["scatter"]
            else:
                q_tasks: list[tuple[str, Callable[[], list[dict[str, Any]]]]] = [
                    *((name, partial(_bar, expr, extra)) for name, expr, extra in _QUALITY_BAR_SPECS),
                    ("scatter", _run_scatter),
                ]
                q_res: dict[str, list[dict[str, Any]]] = trace.run_parallel(
                    q_tasks,
                    source="fact",
                    max_workers=2,
                    thread_name_prefix="hs-net-quality",
                )
                by_country, by_asn, by_region, by_pop, scatter_rows = (
                    q_res[k] for k in ("country", "asn", "region", "pop", "scatter")
                )
            for row in by_country:
                row["label"] = COUNTRY_MAP.get(row["value"], row["label"])
            if by_asn:
                labels = resolve_asn_display_labels(
                    service.service_id, [int(r["value"]) for r in by_asn if r["value"].isdigit()]
                )
                for row in by_asn:
                    if row["value"].isdigit():
                        row["label"] = labels[int(row["value"])]
            payload = {
                "available": True,
                "section_timings": trace.timings(),
                "by_country": by_country,
                "by_asn": by_asn,
                "by_region": by_region,
                "region_country": region_country,
                "by_pop": by_pop,
                "scatter": [
                    {
                        "rtt_ms": round(float(r["rtt_ms"]), 2),
                        "ttfb_ms": round(float(r["ttfb_ms"]), 2),
                        "cache": str(r["cache_state"]),
                    }
                    for r in scatter_rows
                ],
                "countries": sorted({row["value"] for row in by_country}),
            }
            if memo_key is not None:
                _network_memo.put(memo_key, dict(payload))
            return payload

        if memo_key is not None:
            result = _network_memo.coalesce(f"hs_net_quality:{memo_key}", _compute_quality)
            _finish_pop_rollup(
                _pop_rollup_cache.get(pop_rollup_cache_key) if pop_rollup_cache_key is not None else None
            )
            return NetworkQualityResponse.with_telemetry(**result)
        return NetworkQualityResponse.with_telemetry(
            **cache_result({}, state="bypass", prewarmer=False),
            **_compute_quality(),
        )
    finally:
        _finish_pop_rollup(None)


def get_pop_health(
    service: HighScaleService,
    start_time: datetime | None,
    end_time: datetime | None,
    *,
    filters: Any = None,
    pop_rollup_fut: Future[dict[str, tuple[int, int, Any, Any]] | None] | None = None,
    pop_fact_fut: Future[tuple[list[dict[str, Any]], int] | None] | None = None,
) -> PopHealthListResponse:
    end = end_time or datetime.now(UTC)
    start = start_time or (end - timedelta(hours=24))
    start, end = (dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt for dt in (start, end))
    filter_sql, filter_params = _filter_clause(filters)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch:
        memo_key = digest_cache_key(
            {
                "ep": "hs_pop_health",
                "cid": client_cache_token(service.client),
                "s": int(start.timestamp()) // 60,
                "e": int(end.timestamp()) // 60,
                "f": serialize_filters_for_key(filters or {}),
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (cached := _network_memo.get_with_telemetry(memo_key)) is not None:
            return PopHealthListResponse.with_telemetry(**cached)

    def _compute_pop_health() -> dict[str, Any]:
        if memo_key is not None and (cached_inner := _network_memo.get_with_telemetry(memo_key)) is not None:
            return cached_inner
        trace = HighScaleSections(service.service_id, "network")
        params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
        base_where = f"{_BASE_WHERE}{filter_sql}"
        if is_real_ch:
            pop_span_s = (end - start).total_seconds()
            pop_stride = _network_sample_stride(pop_span_s)
            hash_clause = _network_time_sample_clause(pop_stride, start=start, end=end)
            cache_hit_pred = "(startsWith(custom_fields['cache'], 'HIT') OR startsWith(custom_fields['cache'], 'hit'))"
            fact_sql = (
                f"SELECT custom_fields['pop'] AS pop, count() AS requests, countIf({_STATUS} >= 500) AS errors, "
                f"quantileTDigest(0.5)({_RTT}) AS p50_rtt_us, quantileTDigest(0.95)({_TTFB}) * 1000.0 AS p95_ttfb_ms, "
                f"countIf({cache_hit_pred}) AS cache_hits, "
                "sum(toInt64OrZero(custom_fields['resp_bytes'])) AS bandwidth_bytes "
                f"FROM request_facts PREWHERE {base_where}{hash_clause} AND custom_fields['pop'] != '' "
                "GROUP BY pop ORDER BY requests DESC"
            )
            rollup_by_pop: dict[str, tuple[int, int, Any, Any]] = {}
            if not filter_sql:
                pop_rollup_cache_key = (
                    service.service_id,
                    client_cache_token(service.client),
                    _cache_minute_bound(start),
                    _cache_minute_bound(end),
                )
                pop_fact_cache_key = _pop_fact_key(service, start, end, filters)
                shared_rollup = pop_rollup_fut.result() if pop_rollup_fut is not None else None
                if shared_rollup is None:
                    shared_rollup = _pop_rollup_cache.get(pop_rollup_cache_key)
                shared_fact = pop_fact_fut.result() if pop_fact_fut is not None else None
                if shared_fact is None and pop_fact_cache_key is not None:
                    shared_fact = _pop_fact_cache.get(pop_fact_cache_key)
                light_sql = (
                    "SELECT custom_fields['pop'] AS pop, count() AS requests, "
                    f"countIf({cache_hit_pred}) AS cache_hits, "
                    "sum(toInt64OrZero(custom_fields['resp_bytes'])) AS bandwidth_bytes "
                    f"FROM request_facts PREWHERE {base_where}{hash_clause} AND custom_fields['pop'] != '' "
                    "GROUP BY pop ORDER BY requests DESC"
                )
                if shared_rollup is not None and shared_fact is not None:
                    rollup_by_pop = shared_rollup
                    fact_rows, pop_stride = shared_fact
                    trace.run("pop_health", lambda: None, source="projection" if rollup_by_pop else "fact")
                elif shared_rollup is not None:
                    rollup_by_pop = shared_rollup
                    fact_rows = trace.run(
                        "pop_health",
                        lambda: service.client.execute(light_sql, params),
                        source="fact",
                    )
                    if pop_fact_cache_key is not None and fact_rows:
                        _pop_fact_cache[pop_fact_cache_key] = (fact_rows, pop_stride)
                elif shared_fact is not None:
                    fact_rows, pop_stride = shared_fact
                    fence = visible_publication_fence_sql(
                        "network_dimensions", client=service.client, service_id=service.service_id
                    )
                    rollup_sql = (
                        "SELECT value AS pop, toUInt64(sum(requests)) AS req_count, "
                        "toUInt64(sum(errors)) AS errors, "
                        "sum(tcp_rtt_p50_us * tcp_rtt_count) / nullIf(sum(tcp_rtt_count), 0) AS p50_rtt_us, "
                        "sum(ttfb_p95_us * requests) / nullIf(sumIf(requests, ttfb_p95_us IS NOT NULL), 0) * 1000.0 AS p95_ttfb_ms "
                        "FROM network_minute_dimensions "
                        "PREWHERE service_id={service_id:String} "
                        "AND bucket_start >= {start:DateTime64(3)} AND bucket_start < {end:DateTime64(3)} "
                        "AND dimension='pop' AND value != '' "
                        f"WHERE {fence} "
                        "GROUP BY pop"
                    )
                    rollup_rows = trace.run(
                        "pop_health",
                        lambda: service.client.execute(rollup_sql, params),
                        source="projection",
                    )
                    for rr in rollup_rows:
                        if rr.get("pop"):
                            rollup_by_pop[str(rr["pop"])] = (
                                int(rr.get("req_count") or 0),
                                int(rr.get("errors") or 0),
                                rr.get("p50_rtt_us"),
                                rr.get("p95_ttfb_ms"),
                            )
                    if rollup_by_pop:
                        _pop_rollup_cache[pop_rollup_cache_key] = rollup_by_pop
                else:
                    fence = visible_publication_fence_sql(
                        "network_dimensions", client=service.client, service_id=service.service_id
                    )
                    combined_sql = (
                        "SELECT 'rollup' AS kind, value AS pop, toUInt64(sum(requests)) AS req_count, "
                        "CAST(toUInt64(sum(errors)), 'Nullable(UInt64)') AS errors, "
                        "sum(tcp_rtt_p50_us * tcp_rtt_count) / nullIf(sum(tcp_rtt_count), 0) AS p50_rtt_us, "
                        "sum(ttfb_p95_us * requests) / nullIf(sumIf(requests, ttfb_p95_us IS NOT NULL), 0) * 1000.0 AS p95_ttfb_ms, "
                        "toUInt64(0) AS cache_hits, toInt64(0) AS bandwidth_bytes "
                        "FROM network_minute_dimensions "
                        "PREWHERE service_id={service_id:String} "
                        "AND bucket_start >= {start:DateTime64(3)} AND bucket_start < {end:DateTime64(3)} "
                        "AND dimension='pop' AND value != '' "
                        f"WHERE {fence} "
                        "GROUP BY pop "
                        "UNION ALL "
                        "SELECT 'fact' AS kind, custom_fields['pop'] AS pop, toUInt64(count()) AS req_count, "
                        "CAST(NULL, 'Nullable(UInt64)') AS errors, "
                        "CAST(NULL, 'Nullable(Float64)') AS p50_rtt_us, "
                        "CAST(NULL, 'Nullable(Float64)') AS p95_ttfb_ms, "
                        f"toUInt64(countIf({cache_hit_pred})) AS cache_hits, "
                        "toInt64(sum(toInt64OrZero(custom_fields['resp_bytes']))) AS bandwidth_bytes "
                        f"FROM request_facts PREWHERE {base_where}{hash_clause} AND custom_fields['pop'] != '' "
                        "GROUP BY pop"
                    )
                    combined_rows = trace.run(
                        "pop_health",
                        lambda: service.client.execute(combined_sql, params),
                        source="fact",
                    )
                    fact_rows = []
                    for cr in combined_rows:
                        if cr.get("kind") == "rollup":
                            if cr.get("pop"):
                                rollup_by_pop[str(cr["pop"])] = (
                                    int(cr.get("req_count") or 0),
                                    int(cr.get("errors") or 0),
                                    cr.get("p50_rtt_us"),
                                    cr.get("p95_ttfb_ms"),
                                )
                        else:
                            fact_rows.append({**cr, "requests": cr.get("req_count", cr.get("requests"))})
                    if rollup_by_pop:
                        _pop_rollup_cache[pop_rollup_cache_key] = rollup_by_pop
                    if pop_fact_cache_key is not None and fact_rows:
                        _pop_fact_cache[pop_fact_cache_key] = (fact_rows, pop_stride)
                if (not fact_rows and (shared_fact is None or pop_stride > 1)) or (
                    not rollup_by_pop and pop_stride == 1 and shared_fact is None
                ):
                    pop_stride = 1
                    if rollup_by_pop:
                        light_fb_sql = light_sql.replace(hash_clause, "")
                        fact_rows = trace.run(
                            "pop_health_fallback",
                            lambda: service.client.execute(light_fb_sql, params),
                            source="fact",
                        )
                    else:
                        fact_rows = trace.run(
                            "pop_health_fallback",
                            lambda: service.client.execute(fact_sql.replace(hash_clause, ""), params),
                            source="fact",
                        )
            else:
                fact_rows = trace.run(
                    "pop_health",
                    lambda: service.client.execute(fact_sql, params),
                    source="fact",
                )
                if pop_stride > 1 and not fact_rows:
                    pop_stride = 1
                    fact_rows = trace.run(
                        "pop_health_fallback",
                        lambda: service.client.execute(fact_sql.replace(hash_clause, ""), params),
                        source="fact",
                    )
                elif pop_stride > 1:
                    fact_rows = [
                        {
                            **r,
                            "requests": int(r.get("requests") or 0) * pop_stride,
                            "errors": int(r.get("errors") or 0) * pop_stride,
                            "cache_hits": int(r.get("cache_hits") or 0) * pop_stride,
                            "bandwidth_bytes": int(r.get("bandwidth_bytes") or 0) * pop_stride,
                        }
                        for r in fact_rows
                    ]
            rows = []
            for r in fact_rows:
                pop_key = str(r["pop"])
                r_entry = rollup_by_pop.get(pop_key, (0, 0, None, None))
                sampled_reqs = max(1, int(r.get("requests") or 0))
                if pop_stride > 1:
                    exact_reqs = r_entry[0] if r_entry[0] > 0 else sampled_reqs * pop_stride
                    scale = exact_reqs / sampled_reqs
                    row_reqs = exact_reqs
                    row_errors = (
                        r_entry[1]
                        if (r_entry[0] > 0 and r.get("errors") is None)
                        else min(exact_reqs, int(round(int(r.get("errors") or 0) * scale)))
                    )
                    row_hits = min(exact_reqs, int(round(int(r.get("cache_hits") or 0) * scale)))
                    row_bytes = int(round(int(r.get("bandwidth_bytes") or 0) * scale))
                else:
                    row_reqs = (
                        r_entry[0] if (r_entry[0] > 0 and r.get("errors") is None) else int(r.get("requests") or 0)
                    )
                    row_errors = (
                        r_entry[1] if (r_entry[0] > 0 and r.get("errors") is None) else int(r.get("errors") or 0)
                    )
                    row_hits = int(r.get("cache_hits") or 0)
                    row_bytes = int(r.get("bandwidth_bytes") or 0)
                rows.append(
                    {
                        **r,
                        "requests": row_reqs,
                        "errors": row_errors,
                        "cache_hits": row_hits,
                        "bandwidth_bytes": row_bytes,
                        "p50_rtt_us": r.get("p50_rtt_us") if r.get("p50_rtt_us") is not None else r_entry[2],
                        "p95_ttfb_ms": r.get("p95_ttfb_ms") if r.get("p95_ttfb_ms") is not None else r_entry[3],
                    }
                )
            rows.sort(key=lambda x: int(x.get("requests") or 0), reverse=True)
        else:
            sql = _ch_fact_query(
                f"custom_fields['pop'] AS pop, count() AS requests, countIf({_STATUS} >= 500) AS errors, "
                f"quantile(0.5)({_RTT}) AS p50_rtt_us, quantile(0.95)({_TTFB}) * 1000.0 AS p95_ttfb_ms, "
                "countIf(startsWith(upper(custom_fields['cache']), 'HIT')) AS cache_hits, "
                "sum(toInt64OrZero(custom_fields['resp_bytes'])) AS bandwidth_bytes",
                f"{base_where} AND custom_fields['pop'] != ''",
                group_by="pop",
                order_by="requests DESC",
            )
            rows = trace.run(
                "pop_health",
                lambda: service.client.execute(sql, params),
                source="fact",
            )
        data = [
            PopHealthItem(
                pop=str(r["pop"]),
                requests=(requests := int(r["requests"])),
                errors=(errors := int(r["errors"])),
                error_rate=round(errors * 100.0 / requests, 2) if requests else 0.0,
                p50_rtt_us=_flt(r["p50_rtt_us"]),
                p95_ttfb_ms=_flt(r["p95_ttfb_ms"]),
                cache_hit_rate=round(int(r["cache_hits"]) * 100.0 / requests, 2) if requests else 0.0,
                bandwidth_bytes=int(r["bandwidth_bytes"] or 0),
            )
            for r in rows
        ]
        payload = {"data": data, "section_timings": trace.timings()}
        if memo_key is not None:
            _network_memo.put(memo_key, dict(payload))
        return payload

    if memo_key is not None:
        return PopHealthListResponse.with_telemetry(
            **_network_memo.coalesce(f"hs_pop_health:{memo_key}", _compute_pop_health)
        )
    return PopHealthListResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False), **_compute_pop_health()
    )


def _with_transport_settings(service_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Name the transport fields the service's log field settings leave off; outside the memo so an edit shows at once."""
    from backend import config as svcconfig

    try:
        cfg = svcconfig.load_config(service_id) or {}
    except Exception:
        cfg = {}
    enabled = set(svcconfig.active_log_field_ids(cfg))
    missing = [f for f in _TRANSPORT_FIELDS if f not in enabled]
    return {**payload, "missing_fields": missing, "has_transport_data": len(missing) < len(_TRANSPORT_FIELDS)}


def get_transport_metrics(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Any = None,
    *,
    pop_fact_sink: Future[tuple[list[dict[str, Any]], int] | None] | None = None,
) -> TransportMetrics:
    start, end = parse_time_window(start_time, end_time)
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )

    def _finish_pop_fact(val: tuple[list[dict[str, Any]], int] | None) -> None:
        if pop_fact_sink is not None and not pop_fact_sink.done():
            pop_fact_sink.set_result(val)

    try:
        memo_key: str | None = None
        if is_real_ch and start_time is not None and end_time is not None:
            memo_key = digest_cache_key(
                {
                    "ep": "hs_transport_metrics",
                    "cid": client_cache_token(service.client),
                    "s": _cache_minute_bound(start_time),
                    "e": _cache_minute_bound(end_time),
                    "f": serialize_filters_for_key(filters or {}),
                },
                {"name": service.service_id, "service_id": service.service_id},
            )
            if (cached := _network_memo.get_with_telemetry(memo_key)) is not None:
                _finish_pop_fact(None)
                return TransportMetrics(**_with_transport_settings(service.service_id, cached))

        def _compute_transport() -> dict[str, Any]:
            if memo_key is not None and (cached_inner := _network_memo.get_with_telemetry(memo_key)) is not None:
                _finish_pop_fact(None)
                return cached_inner
            filter_sql, filter_params = _filter_clause(filters)
            params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
            base_where = f"{_BASE_WHERE}{filter_sql}"
            proto_clean_expr = (
                "caseWithExpression(custom_fields['proto'], '3.0', 'HTTP/3', '3', 'HTTP/3', '2.0', 'HTTP/2', '2', 'HTTP/2', "
                "'1.1', 'HTTP/1.1', '1.0', 'HTTP/1.0', if(empty(custom_fields['proto']), 'Other', custom_fields['proto']))"
            )
            trace = HighScaleSections(service.service_id, "network.transport")
            retrans_expr = (
                "if(countIf(custom_fields['retrans'] != '') > 0, "
                "countIf(toFloat64OrNull(custom_fields['retrans']) > 0) * 100.0 / nullIf(count(), 0), NULL)"
            )
            if is_real_ch:
                tr_span_s = (end - start).total_seconds() if start is not None and end is not None else 0.0
                scale_factor = _network_sample_stride(tr_span_s)
                hash_clause = _network_time_sample_clause(scale_factor, start=start, end=end)
                pop_fact_cache_key = _pop_fact_key(service, start, end, filters) if not filter_sql else None
                want_pop_fact = not filter_sql and (
                    pop_fact_sink is not None
                    or (pop_fact_cache_key is not None and _pop_fact_cache.get(pop_fact_cache_key) is None)
                )
                cache_hit_pred = (
                    "(startsWith(custom_fields['cache'], 'HIT') OR startsWith(custom_fields['cache'], 'hit'))"
                )
                pop_select = (
                    ", sumMap(if(custom_fields['pop'] != '', [custom_fields['pop']], []), "
                    "if(custom_fields['pop'] != '', [toUInt64(1)], [])) AS pop_reqs_map, "
                    f"sumMap(if(custom_fields['pop'] != '' AND {cache_hit_pred}, [custom_fields['pop']], []), "
                    f"if(custom_fields['pop'] != '' AND {cache_hit_pred}, [toUInt64(1)], [])) AS pop_hits_map, "
                    "sumMap(if(custom_fields['pop'] != '', [custom_fields['pop']], []), "
                    "if(custom_fields['pop'] != '', [toInt64OrZero(custom_fields['resp_bytes'])], [])) AS pop_bytes_map"
                    if want_pop_fact
                    else ""
                )
                combined_sql = (
                    f"SELECT count() AS total_reqs, sum(toInt64OrZero(custom_fields['resp_bytes'])) AS total_bytes, "
                    f"quantilesTDigest(0.50, 0.90, 0.95, 0.99)({_RTT}) AS rtt_quants, "
                    f"{retrans_expr} AS avg_retrans, avg({_PLOSS}) * 100.0 AS avg_ploss_pct, "
                    f"avg(toFloat64OrNull(custom_fields['delivery_rate'])) * 8.0 / 1e6 AS avg_del_rate_mbps, "
                    f"sumMap([{proto_clean_expr}], [toUInt64(1)]) AS proto_map{pop_select} "
                    f"FROM request_facts PREWHERE {base_where}{hash_clause}"
                )
                rows = trace.run("summary", lambda: service.client.execute(combined_sql, params), source="fact")
                if scale_factor > 1 and (not rows or int(rows[0].get("total_reqs") or 0) == 0):
                    scale_factor = 1
                    rows = trace.run(
                        "summary_fallback",
                        lambda: service.client.execute(combined_sql.replace(hash_clause, ""), params),
                        source="fact",
                    )
                r_raw: Mapping[str, Any] = rows[0] if rows else {}
                if want_pop_fact:
                    reqs_d = {str(k): int(v) for k, v in _unpack_sum_map(r_raw.get("pop_reqs_map")) if k}
                    hits_d = {str(k): int(v) for k, v in _unpack_sum_map(r_raw.get("pop_hits_map")) if k}
                    bytes_d = {str(k): int(v) for k, v in _unpack_sum_map(r_raw.get("pop_bytes_map")) if k}
                    pop_fact_rows: list[dict[str, Any]] = [
                        {
                            "pop": pop_code,
                            "requests": req_cnt,
                            "cache_hits": hits_d.get(pop_code, 0),
                            "bandwidth_bytes": bytes_d.get(pop_code, 0),
                        }
                        for pop_code, req_cnt in sorted(reqs_d.items(), key=lambda kv: kv[1], reverse=True)
                    ]
                    pop_fact_val = (pop_fact_rows, scale_factor)
                    if pop_fact_cache_key is not None and pop_fact_rows:
                        _pop_fact_cache[pop_fact_cache_key] = pop_fact_val
                    _finish_pop_fact(pop_fact_val)
                quants = r_raw.get("rtt_quants")
                q_vals = (
                    [float(x) / 1000.0 if x is not None else None for x in quants]
                    if isinstance(quants, (list, tuple)) and len(quants) == 4
                    else [None, None, None, None]
                )
                r = {
                    **r_raw,
                    "total_reqs": int(r_raw.get("total_reqs") or 0) * scale_factor,
                    "total_bytes": int(r_raw.get("total_bytes") or 0) * scale_factor,
                    "rtt_p50": q_vals[0],
                    "rtt_p90": q_vals[1],
                    "rtt_p95": q_vals[2],
                    "rtt_p99": q_vals[3],
                }
                p_pairs = _unpack_sum_map(r_raw.get("proto_map"))
                p_rows = [
                    {"proto_clean": str(k), "reqs": int(v) * scale_factor}
                    for k, v in sorted(p_pairs, key=lambda kv: int(kv[1]), reverse=True)
                ]
            else:
                sql = _ch_fact_query(
                    f"count() AS total_reqs, sum(toInt64OrZero(custom_fields['resp_bytes'])) AS total_bytes, "
                    f"quantile(0.50)({_RTT}) / 1000.0 AS rtt_p50, quantile(0.90)({_RTT}) / 1000.0 AS rtt_p90, "
                    f"quantile(0.95)({_RTT}) / 1000.0 AS rtt_p95, quantile(0.99)({_RTT}) / 1000.0 AS rtt_p99, "
                    f"{retrans_expr} AS avg_retrans, avg({_PLOSS}) * 100.0 AS avg_ploss_pct, "
                    f"avg(toFloat64OrNull(custom_fields['delivery_rate'])) * 8.0 / 1e6 AS avg_del_rate_mbps",
                    base_where,
                )
                proto_sql = _ch_fact_query(
                    f"{proto_clean_expr} AS proto_clean, count() AS reqs",
                    base_where,
                    group_by="proto_clean",
                    order_by="reqs DESC",
                )
                q_res = trace.run_parallel(
                    [
                        ("summary", lambda: service.client.execute(sql, params)),
                        ("protocols", lambda: service.client.execute(proto_sql, params)),
                    ],
                    source="fact",
                    max_workers=2,
                    thread_name_prefix="hs-net-transport",
                )
                rows, p_rows = q_res["summary"], q_res["protocols"]
                r = rows[0] if rows else {}
            total_requests = int(r.get("total_reqs") or 0)
            _transport_vals: dict[str, Any] = {attr: _flt(r.get(col), nd) for attr, col, nd in _TRANSPORT_ROUND_SPECS}
            payload = {
                "section_timings": [SectionTimingEntry.model_validate(t) for t in trace.timings()],
                **_transport_vals,
                "bandwidth_bytes": int(r.get("total_bytes") or 0),
                "total_requests": total_requests,
                "protocol_distribution": [
                    TransportProtocolShare(**item)
                    for item in shape_protocol_distribution(
                        ((pr.get("proto_clean", "Other"), pr.get("reqs", 0)) for pr in p_rows),
                        total_requests=total_requests,
                    )
                ],
            }
            if memo_key is not None:
                _network_memo.put(memo_key, dict(payload))
            return payload

        if memo_key is not None:
            computed = _network_memo.coalesce(f"hs_transport_metrics:{memo_key}", _compute_transport)
            return TransportMetrics(**_with_transport_settings(service.service_id, computed))
        return TransportMetrics(
            **cache_result({}, state="bypass", prewarmer=False),
            **_with_transport_settings(service.service_id, _compute_transport()),
        )
    finally:
        _finish_pop_fact(None)


def network_bundle(
    service: HighScaleService, req: NetworkBundleRequest, start_time: str | None, end_time: str | None
) -> NetworkBundleResponse:
    sections = set(req.sections) if req.sections else None
    is_real_ch = (
        hasattr(service.client, "_http") and "MockTransport" not in type(service.client._http._transport).__name__
    )
    memo_key: str | None = None
    if is_real_ch and start_time is not None and end_time is not None:
        from backend import config as svcconfig

        try:
            _svc_cfg = svcconfig.load_config(service.service_id) or {}
        except Exception:
            _svc_cfg = {}
        memo_key = digest_cache_key(
            {
                "ep": "hs_network_bundle",
                "cid": client_cache_token(service.client),
                "s": _cache_minute_bound(start_time),
                "e": _cache_minute_bound(end_time),
                "f": serialize_filters_for_key(getattr(req, "filters", None) or {}),
                "bs": int(getattr(req, "bucket_seconds", 300) or 300),
                "tn": int(getattr(req, "top_n", 30) or 30),
                "ma": str(getattr(req, "map_asn", "all") or "all"),
                "rc": str(getattr(req, "region_country", "US") or "US"),
                "sec": sorted(sections) if sections is not None else None,
                "m": str(getattr(req, "metric", "health_score") or "health_score"),
                "af": sorted(svcconfig.active_log_field_ids(_svc_cfg)),
            },
            {"name": service.service_id, "service_id": service.service_id},
        )
        if (hit := _fast_model_hit(memo_key)) is not None:
            return hit

    def _compute_bundle() -> NetworkBundleResponse:
        if memo_key is not None and (inner_hit := _fast_model_hit(memo_key)) is not None:
            return inner_hit
        q_req = NetworkQualityRequest(
            region_country=req.region_country, filters=req.filters, start_time=start_time, end_time=end_time
        )
        want_health = (
            sections is None or "health" in sections or bool(sections & (_CORE_SECTIONS | {"shielding_analysis"}))
        )
        want_quality = sections is None or "quality" in sections
        want_pop = sections is None or "pop_health" in sections
        want_transport = sections is None or "transport" in sections
        share_pop_rollup = is_real_ch and want_quality and want_pop and not getattr(req, "filters", None)
        pop_rollup_fut: Future[dict[str, tuple[int, int, Any, Any]] | None] | None = (
            Future() if share_pop_rollup else None
        )
        pop_fact_fut: Future[tuple[list[dict[str, Any]], int] | None] | None = (
            Future() if (share_pop_rollup and want_transport) else None
        )
        bundle_specs: tuple[tuple[str, bool, Callable[[], Any]], ...] = (
            (
                "health",
                want_health,
                lambda: network_health(service, req, start_time, end_time),
            ),
            (
                "quality",
                want_quality,
                lambda: network_quality(service, q_req, start_time, end_time, pop_rollup_sink=pop_rollup_fut),
            ),
            (
                "pop",
                want_pop,
                lambda: get_pop_health(
                    service,
                    _range_value(start_time),
                    _range_value(end_time),
                    pop_rollup_fut=pop_rollup_fut,
                    pop_fact_fut=pop_fact_fut,
                ),
            ),
            (
                "transport",
                want_transport,
                lambda: get_transport_metrics(service, start_time, end_time, req.filters, pop_fact_sink=pop_fact_fut),
            ),
        )
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="hs-net-bundle") as pool:
            futs = [(k, pool.submit(contextvars.copy_context().run, fn)) for k, want, fn in bundle_specs if want]
            res = {k: fut.result() for k, fut in futs}
        health_resp, quality_resp, pop_response, transport_resp = (
            res.get("health"),
            res.get("quality"),
            res.get("pop"),
            res.get("transport"),
        )
        combined_timings = [
            t
            for r in (health_resp, quality_resp, pop_response, transport_resp)
            if r is not None
            for t in (getattr(r, "section_timings", None) or [])
        ]
        payload: dict[str, Any] = {
            "available": True,
            "section_timings": combined_timings,
            "component_observations": {"pop_health": pop_response} if pop_response is not None else None,
            "health": health_resp,
            "quality": quality_resp,
            "pop_health": pop_response.data if pop_response is not None else None,
            "transport": transport_resp,
        }
        if memo_key is not None:
            _network_memo.put(memo_key, dict(payload))
            bundle_resp = NetworkBundleResponse.with_telemetry(**_network_memo.result(payload, state="miss"))
            _network_model_cache[memo_key] = bundle_resp
            dumped = bundle_resp.model_dump(mode="json", by_alias=True)
            _network_json_cache[memo_key] = dumped
            object.__setattr__(bundle_resp, "_cached_json_dict", dumped)
            return bundle_resp
        return NetworkBundleResponse.with_telemetry(
            **cache_result({}, state="bypass", prewarmer=False),
            **payload,
        )

    if memo_key is not None:
        resp, is_leader = coalesce(f"hs_net_bundle:{memo_key}", _compute_bundle)
        if not is_leader and (follower_hit := _fast_model_hit(memo_key)) is not None:
            return follower_hit
        return resp
    return _compute_bundle()


@dataclass(frozen=True)
class NetworkProjection:
    dimension_rows: tuple[dict[str, Any], ...]


@dataclass
class _NetworkAccumulator:
    requests: int = 0
    errors: int = 0
    tcp_rtts: list[float] = _dc_field(default_factory=list)
    ttfbs: list[float] = _dc_field(default_factory=list)
    ploss_sum: float = 0.0
    ploss_count: int = 0


def build_network_projection_rows(rows: tuple[Mapping[str, Any], ...]) -> NetworkProjection:
    dimensions: dict[tuple[datetime, str, str, str], _NetworkAccumulator] = defaultdict(_NetworkAccumulator)
    for row in rows:
        if (bucket := _bucket(row.get("timestamp"))) is None:
            continue
        status = _finite_int(row.get("status"))
        is_error = status is not None and status >= 500
        tcp_rtt, ttfb, ploss = (
            _finite_float(row.get("tcp_rtt")),
            _finite_float(row.get("ttfb")),
            _finite_float(row.get("ploss")),
        )
        c_speed = _text(row.get("c_speed"))
        for dim in ("country", "asn", "pop", "region"):
            if val := (_asn_text(row.get(dim)) if dim == "asn" else _text(row.get(dim))):
                _network_update(
                    dimensions[(bucket, dim, val, c_speed if dim == "asn" else "")], is_error, tcp_rtt, ttfb, ploss
                )
    return NetworkProjection(
        dimension_rows=tuple(
            _network_dimension_row(b, d, v, cs, acc) for (b, d, v, cs), acc in sorted(dimensions.items())
        )
    )


def _network_update(
    acc: _NetworkAccumulator, is_error: bool, tcp_rtt: float | None, ttfb: float | None, ploss: float | None
) -> None:
    acc.requests += 1
    acc.errors += int(is_error)
    if tcp_rtt is not None and tcp_rtt > 0:
        acc.tcp_rtts.append(tcp_rtt)
    if ttfb is not None and ttfb > 0:
        acc.ttfbs.append(ttfb)
    if ploss is not None:
        acc.ploss_sum += ploss
        acc.ploss_count += 1


def _network_dimension_row(
    bucket: datetime, dimension: str, value: str, c_speed: str, acc: _NetworkAccumulator
) -> dict[str, Any]:
    rtt_p50, rtt_p95, rtt_p99 = _percentiles(acc.tcp_rtts, (0.50, 0.95, 0.99))
    ttfb_p50, ttfb_p95 = _percentiles(acc.ttfbs, (0.50, 0.95))
    return {
        "bucket_start": bucket.isoformat(),
        "dimension": dimension,
        "value": value,
        "c_speed": c_speed,
        "requests": acc.requests,
        "errors": acc.errors,
        "tcp_rtt_count": len(acc.tcp_rtts),
        "tcp_rtt_sum": sum(acc.tcp_rtts) if acc.tcp_rtts else None,
        "tcp_rtt_p50_us": rtt_p50,
        "tcp_rtt_p95_us": rtt_p95,
        "tcp_rtt_p99_us": rtt_p99,
        "ploss_sum": acc.ploss_sum if acc.ploss_count > 0 else None,
        "ploss_count": acc.ploss_count,
        "ttfb_p50_us": ttfb_p50,
        "ttfb_p95_us": ttfb_p95,
    }


if (_hs_pkg := sys.modules.get("backend.high_scale")) is not None:
    setattr(_hs_pkg, "network", sys.modules[__name__])
