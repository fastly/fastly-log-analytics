"""High-scale network health, transport quality, POP health, shielding analysis, and minute projections."""

from __future__ import annotations

import contextvars
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field as _dc_field
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import TYPE_CHECKING, Any

from backend.core.high_scale_sections import HighScaleSections
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
    parse_time_window,
    resolve_asn_display_labels,
    visible_publication_fence_sql,
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
from backend.repositories.utils.response_cache import cache_result

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService

from backend.high_scale._common import optional_int as _finite_int
from backend.models.common import SectionTimingEntry
from backend.models.network import (
    NetworkBundleRequest,
    NetworkBundleResponse,
    NetworkCityEntry,
    NetworkHealthResponse,
    NetworkHealthSummary,
    NetworkQualityRequest,
    NetworkQualityResponse,
    NetworkWorstEntry,
    PopHealthItem,
    PopHealthListResponse,
    ShieldingAnalysis,
    ShieldingRow,
    TransportMetrics,
    TransportProtocolShare,
)
from backend.models.origin import OriginShieldingAnalysisResponse
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
from backend.utils.countries import format_city_label


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None).isoformat()


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
    filter_sql, filter_params = _filter_clause(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    hop_ms = "coalesce(toFloat64OrNull(custom_fields['ottfb']), toFloat64OrNull(custom_fields['ttfb']) * 1000000.0)"
    edge_sub = _ch_fact_query(
        f"custom_fields['rid'] AS rid, custom_fields['pop'] AS edge_pop, {hop_ms} AS hop_us",
        f"{_BASE_WHERE}{filter_sql} AND lower(custom_fields['edge']) IN ('true', '1')",
    )
    shield_where = f"{_BASE_WHERE} AND lower(custom_fields['edge']) IN ('false', '0') AND custom_fields['prid'] != ''"
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
    trace = HighScaleSections(service.service_id, "origin")
    analysis = trace.run(
        "shielding_analysis",
        lambda: _shielding_analysis(service, start, end, getattr(req, "filters", None)),
        source="fact",
    )
    limit = int(getattr(req, "limit", 50) or 50)
    rows = analysis.rows[:limit]
    truncated = (bool(analysis.truncated) or len(analysis.rows) > limit) if analysis.has_data else None
    return OriginShieldingAnalysisResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False),
        section_timings=trace.timings(),
        has_data=analysis.has_data,
        requires_fields=analysis.requires_fields or [],
        edge_only=bool(analysis.edge_only),
        rows=rows,
        total_routes=analysis.total_routes,
        truncated=truncated,
        error=False,
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
    shielding = (
        trace.run(
            "shielding_analysis",
            lambda: _shielding_analysis(service, start, end, getattr(req, "filters", None)),
            source="fact",
        )
        if _want_shielding(sections)
        else None
    )
    if not _want_core(sections):
        return NetworkHealthResponse.with_telemetry(
            **cache_result({}, state="bypass", prewarmer=False),
            available=True,
            shielding_analysis=shielding,
            section_timings=trace.timings(),
        )

    if start is not None and end is not None:
        bs = clamp_bucket_seconds(bs, start, end) or bs
    filter_sql, filter_params = _filter_clause(getattr(req, "filters", None), drop_asn=map_asn != "all")
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    rtt_where, bucket_expr = f"{_RTT_FILTER}{filter_sql}", _ch_bucket_expr(bs)
    map_asn_sql = ""
    if map_asn != "all":
        map_asn_sql, params["map_asn"] = f" AND {_ASN} = {{map_asn:Int32}}", int(map_asn)

    err_pct_expr = f"countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct"
    health_queries: tuple[tuple[str, str], ...] = (
        (
            "heatmap",
            _ch_fact_query(
                f"{_ASN} AS asn, {bucket_expr} AS bucket_ts, count() AS reqs, "
                f"median(if({_NET_HIT} AND {_ELAPSED} > 0, {_RESP_BYTES} * 1000000.0 / {_ELAPSED}, NULL)) AS throughput_bps, "
                f"median({_RTT}) AS rtt_med_us, median({_RTT_MIN}) AS rtt_baseline_us, "
                f"median({_RTT} - {_RTT_MIN}) AS rtt_congestion_us, avg({_PLOSS}) AS avg_ploss, "
                f"median({_RTT_VAR}) AS rtt_jitter_us, {err_pct_expr}",
                f"{rtt_where} AND {_ASN} IS NOT NULL",
                group_by="asn, bucket_ts",
                order_by="reqs DESC",
                limit=top_n * 200,
            ),
        ),
        (
            "map",
            _ch_fact_query(
                "country AS country, custom_fields['city'] AS city, "
                "toFloat64OrNull(custom_fields['lat']) AS lat, toFloat64OrNull(custom_fields['lon']) AS lon, "
                f"custom_fields['metro'] AS metro, {bucket_expr} AS bucket_ts, "
                f"median({_RTT}) AS rtt_med_us, avg({_PLOSS}) AS avg_ploss, {err_pct_expr}, count() AS reqs",
                f"{rtt_where} AND country != ''{map_asn_sql}",
                group_by="country, city, lat, lon, metro, bucket_ts",
                order_by="reqs DESC",
                limit=5000,
            ),
        ),
        (
            "metro",
            _ch_fact_query(
                "country AS country, custom_fields['city'] AS city, custom_fields['region'] AS region, "
                f"custom_fields['metro'] AS metro, avg({_PLOSS}) AS avg_ploss, {err_pct_expr}, count() AS total_reqs",
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
            ),
        ),
        (
            "percentiles",
            _ch_fact_query(
                f"{_ASN} AS asn, quantile(0.95)({_RTT}) AS p95, quantile(0.99)({_RTT}) AS p99",
                f"{rtt_where} AND {_ASN} IS NOT NULL",
                group_by="asn",
            ),
        ),
    )
    q_results = trace.run_parallel(
        [(name, partial(service.client.execute, sql, params)) for name, sql in health_queries],
        source="fact",
        max_workers=2,
        thread_name_prefix="hs-net-health",
    )
    heatmap_rows, map_rows, metro_rows, speed_rows, pct_rows = (
        q_results[k] for k in ("heatmap", "map", "metro", "speed", "percentiles")
    )

    all_asns_seen: dict[int, int] = {}
    for r in heatmap_rows:
        if r["asn"] is not None:
            all_asns_seen[int(r["asn"])] = all_asns_seen.get(int(r["asn"]), 0) + int(r["reqs"])
    top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
    top_asn_set, asn_labels = set(top_asns), resolve_asn_display_labels(service.service_id, top_asns)

    all_buckets = generate_epoch_aligned_buckets(start, end, bs) if start is not None and end is not None else []
    bucket_idx = {b: i for i, b in enumerate(all_buckets)}

    asn_speed_mix = build_asn_speed_mix((r["asn"], r["c_speed"], r["cnt"]) for r in speed_rows if r["asn"] is not None)
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
        tp, rtt_cong, pkt, jitter, err = (
            _flt(r.get("throughput_bps")),
            _flt(r["rtt_congestion_us"]),
            _flt(r["avg_ploss"]),
            _flt(r["rtt_jitter_us"]),
            _flt(r["error_pct"]),
        )
        asn_bucket_data.setdefault(asn, {})[bucket] = {
            "bucket_idx": bucket_idx[bucket],
            "bucket": bucket,
            "throughput_bps": _flt(tp, 0),
            "rtt_med_us": _flt(r["rtt_med_us"], 0),
            "rtt_baseline_us": _flt(r["rtt_baseline_us"], 0),
            "rtt_congestion_us": _flt(rtt_cong, 0),
            "avg_ploss": _flt(pkt, 5),
            "rtt_jitter_us": _flt(jitter, 0),
            "error_pct": _flt(err, 2),
            "health_score": _health_score(tp, rtt_cong, pkt, jitter, err),
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
            from backend.core import duckdb as _db

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

    map_by_bucket: dict[str, list[dict]] = {}
    countries_set: set[str] = set()
    for r in map_rows:
        if ctry := (r["country"] or ""):
            countries_set.add(ctry)
        if (bucket := _fmt_ts(int(r["bucket_ts"]))) not in bucket_idx:
            continue
        pkt, err, metro_code, city = (
            _flt(r["avg_ploss"]),
            _flt(r["error_pct"]),
            _parse_metro_code(r.get("metro")),
            r["city"] or "",
        )
        display_city = (
            dma_map.get(str(metro_code), city.title() if city else "")
            if metro_code is not None
            else (city.title() if city else "")
        )
        map_by_bucket.setdefault(bucket, []).append(
            {
                "country": ctry,
                "city_idx": _intern_city(display_city, _flt(r["lat"]), _flt(r["lon"])),
                "metro_code": metro_code,
                "rtt_med_us": _flt(r["rtt_med_us"]),
                "avg_ploss": pkt,
                "error_pct": _flt(err, 2),
                "health_score": _health_score(None, None, pkt, None, err),
                "reqs": int(r["reqs"]),
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
            _health_score(None, None, pkt, None, err),
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

    raw_summary = compute_network_global_summary(
        leaderboard=leaderboard,
        rtt_values=[float(r["rtt_med_us"]) for r in heatmap_rows if r["rtt_med_us"] is not None],
        total_reqs=sum(int(r["reqs"]) for r in heatmap_rows),
        map_buckets=map_buckets,
        cities_list=cities_list,
        zero_as_int=False,
    )
    summary = NetworkHealthSummary(
        global_health_score=raw_summary["global_health_score"],
        avg_rtt_ms=raw_summary["avg_rtt_ms"],
        total_reqs=raw_summary["total_reqs"],
        worst_asn=NetworkWorstEntry(**raw_summary["worst_asn"]) if raw_summary["worst_asn"] else None,
        worst_country=NetworkWorstEntry(**raw_summary["worst_country"]) if raw_summary["worst_country"] else None,
    )
    want_map = not sections or bool(set(sections) & {"health", "map_buckets", "cities"})
    return NetworkHealthResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False),
        available=True,
        has_data=bool(heatmap_rows),
        section_timings=trace.timings(),
        metric="health_score",
        bucket_seconds=bs,
        buckets=all_buckets,
        heatmap=heatmap,
        map_buckets=map_buckets if want_map else [],
        cities=[NetworkCityEntry(**c) for c in cities_list] if want_map else [],
        leaderboard=leaderboard,
        metro_leaderboard=metro_leaderboard,
        summary=summary,
        countries=sorted(countries_set),
        has_metro=has_metro,
        shielding_analysis=shielding,
    )


def network_quality(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkQualityResponse:
    start, end = parse_time_window(start_time, end_time)
    region_country = getattr(req, "region_country", "US") or "US"
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
        sql = _ch_fact_query(
            f"{group_expr} AS label, median({_RTT} / 1000.0) AS rtt_ms, count() AS reqs",
            f"{rtt_where}{extra_where}",
            group_by="label",
            order_by="reqs DESC",
            limit=25,
        )
        return [
            {
                "value": str(r["label"]),
                "label": str(r["label"]),
                "rtt_ms": _flt(r["rtt_ms"], 2) or 0.0,
                "reqs": int(r["reqs"]),
            }
            for r in service.client.execute(sql, params)
            if r["label"] is not None and r["label"] != ""
        ]

    scatter_sql = _ch_fact_query(
        f"{_RTT} / 1000.0 AS rtt_ms, {_TTFB} * 1000.0 AS ttfb_ms, "
        "if(custom_fields['cache'] = '', 'UNKNOWN', custom_fields['cache']) AS cache_state",
        f"{rtt_where} AND {_TTFB} > 0",
        order_by="cityHash64(event_id)",
        limit=_SCATTER_SAMPLE,
    )
    trace = HighScaleSections(service.service_id, "network.quality")
    q_tasks: list[tuple[str, Callable[[], list[dict[str, Any]]]]] = [
        *((name, partial(_bar, expr, extra)) for name, expr, extra in _QUALITY_BAR_SPECS),
        ("scatter", lambda: service.client.execute(scatter_sql, params)),
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
    if by_asn:
        labels = resolve_asn_display_labels(
            service.service_id, [int(r["value"]) for r in by_asn if r["value"].isdigit()]
        )
        for row in by_asn:
            if row["value"].isdigit():
                row["label"] = labels[int(row["value"])]
    return NetworkQualityResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False),
        available=True,
        section_timings=trace.timings(),
        by_country=by_country,
        by_asn=by_asn,
        by_region=by_region,
        region_country=region_country,
        by_pop=by_pop,
        scatter=[
            {
                "rtt_ms": round(float(r["rtt_ms"]), 2),
                "ttfb_ms": round(float(r["ttfb_ms"]), 2),
                "cache": str(r["cache_state"]),
            }
            for r in scatter_rows
        ],
        countries=sorted({row["value"] for row in by_country}),
    )


def get_pop_health(
    service: HighScaleService, start_time: datetime | None, end_time: datetime | None
) -> PopHealthListResponse:
    end = end_time or datetime.now(UTC)
    start = start_time or (end - timedelta(hours=24))
    start, end = (dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt for dt in (start, end))
    trace = HighScaleSections(service.service_id, "network")
    sql = _ch_fact_query(
        f"custom_fields['pop'] AS pop, count() AS requests, countIf({_STATUS} >= 400 OR {_STATUS} = 0) AS errors, "
        f"quantile(0.5)({_RTT}) AS p50_rtt_us, quantile(0.95)({_TTFB}) * 1000.0 AS p95_ttfb_ms, "
        "countIf(custom_fields['cache'] IN ('HIT', 'HIT-STALE')) AS cache_hits, "
        "sum(toInt64OrZero(custom_fields['resp_bytes'])) AS bandwidth_bytes",
        f"{_BASE_WHERE} AND custom_fields['pop'] != ''",
        group_by="pop",
        order_by="requests DESC",
    )
    rows = trace.run(
        "pop_health",
        lambda: service.client.execute(sql, {"service_id": service.service_id, "start": start, "end": end}),
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
    return PopHealthListResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False), data=data, section_timings=trace.timings()
    )


def get_transport_metrics(
    service: HighScaleService, start_time: str | None, end_time: str | None, filters: Any = None
) -> TransportMetrics:
    start, end = parse_time_window(start_time, end_time)
    filter_sql, filter_params = _filter_clause(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    base_where = f"{_BASE_WHERE}{filter_sql}"
    seen_select = ", ".join(f"countIf(custom_fields['{f}'] != '') AS seen_{f}" for f in _TRANSPORT_FIELDS)
    sql = _ch_fact_query(
        f"count() AS total_reqs, sum(toInt64OrZero(custom_fields['resp_bytes'])) AS total_bytes, "
        f"quantile(0.50)({_RTT}) / 1000.0 AS rtt_p50, quantile(0.90)({_RTT}) / 1000.0 AS rtt_p90, "
        f"quantile(0.95)({_RTT}) / 1000.0 AS rtt_p95, quantile(0.99)({_RTT}) / 1000.0 AS rtt_p99, "
        f"avg(toFloat64OrNull(custom_fields['retrans'])) AS avg_retrans, avg({_PLOSS}) * 100.0 AS avg_ploss_pct, "
        f"avg(toFloat64OrNull(custom_fields['delivery_rate'])) * 8.0 / 1e6 AS avg_del_rate_mbps, {seen_select}",
        base_where,
    )
    proto_sql = _ch_fact_query(
        "caseWithExpression(custom_fields['proto'], '3.0', 'HTTP/3', '3', 'HTTP/3', '2.0', 'HTTP/2', '2', 'HTTP/2', "
        "'1.1', 'HTTP/1.1', '1.0', 'HTTP/1.0', if(empty(custom_fields['proto']), 'Other', custom_fields['proto'])) AS proto_clean, "
        "count() AS reqs",
        base_where,
        group_by="proto_clean",
        order_by="reqs DESC",
    )
    trace = HighScaleSections(service.service_id, "network.transport")
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
    r: Mapping[str, Any] = rows[0] if rows else {}
    total_requests = int(r.get("total_reqs") or 0)
    missing_fields = [f for f in _TRANSPORT_FIELDS if not int(r.get(f"seen_{f}") or 0)] if total_requests > 0 else []
    return TransportMetrics(
        **cache_result({}, state="bypass", prewarmer=False),
        section_timings=[SectionTimingEntry.model_validate(t) for t in trace.timings()],
        has_transport_data=len(missing_fields) < len(_TRANSPORT_FIELDS),
        **{attr: _flt(r.get(col), nd) for attr, col, nd in _TRANSPORT_ROUND_SPECS},
        bandwidth_bytes=int(r.get("total_bytes") or 0),
        total_requests=total_requests,
        protocol_distribution=[
            TransportProtocolShare(**item)
            for item in shape_protocol_distribution(
                ((pr.get("proto_clean", "Other"), pr.get("reqs", 0)) for pr in p_rows),
                total_requests=total_requests,
            )
        ],
        missing_fields=missing_fields,
    )


def network_bundle(
    service: HighScaleService, req: NetworkBundleRequest, start_time: str | None, end_time: str | None
) -> NetworkBundleResponse:
    sections = set(req.sections) if req.sections else None
    q_req = NetworkQualityRequest(
        region_country=req.region_country, filters=req.filters, start_time=start_time, end_time=end_time
    )
    bundle_specs: tuple[tuple[str, bool, Callable[[], Any]], ...] = (
        (
            "health",
            sections is None or "health" in sections or bool(sections & (_CORE_SECTIONS | {"shielding_analysis"})),
            lambda: network_health(service, req, start_time, end_time),
        ),
        (
            "quality",
            sections is None or "quality" in sections,
            lambda: network_quality(service, q_req, start_time, end_time),
        ),
        (
            "pop",
            sections is None or "pop_health" in sections,
            lambda: get_pop_health(service, _range_value(start_time), _range_value(end_time)),
        ),
        (
            "transport",
            sections is None or "transport" in sections,
            lambda: get_transport_metrics(service, start_time, end_time, req.filters),
        ),
    )
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-net-bundle") as pool:
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
    return NetworkBundleResponse.with_telemetry(
        **cache_result({}, state="bypass", prewarmer=False),
        available=True,
        section_timings=combined_timings,
        component_observations={"pop_health": pop_response} if pop_response is not None else None,
        health=health_resp,
        quality=quality_resp,
        pop_health=pop_response.data if pop_response is not None else None,
        transport=transport_resp,
    )


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
            if val := _text(row.get(dim)):
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
