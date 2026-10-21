import contextvars
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale._filters import build_clickhouse_filters as _build_clickhouse_filters
from backend.high_scale._projection_utils import (
    range_value as _range_value,
)
from backend.high_scale.registry import HighScaleService
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
from backend.utils.geo import format_city_label


def _health_score(
    throughput_bps: float | None,
    rtt_congestion_us: float | None,
    avg_ploss: float | None,
    rtt_jitter_us: float | None,
    error_pct: float | None,
) -> float:
    """Byte-identical to repositories.network._health_score (throughput unused)."""
    pkt = min((avg_ploss or 0) / 0.05, 1.0) if avg_ploss is not None else 0
    cong = min((rtt_congestion_us or 0) / 200_000, 1.0) if rtt_congestion_us is not None else 0
    jitter = min((rtt_jitter_us or 0) / 100_000, 1.0) if rtt_jitter_us is not None else 0
    err = min((error_pct or 0) / 10.0, 1.0) if error_pct is not None else 0
    weighted = pkt * 0.40 + cong * 0.30 + jitter * 0.20 + err * 0.10
    return round((1.0 - weighted) * 100, 1)


def _avg_hs(buckets_data: dict[str, dict], keys: list[str]) -> float | None:
    vals = [buckets_data[k]["health_score"] for k in keys if buckets_data[k].get("health_score") is not None]
    return round(sum(vals) / len(vals), 1) if vals else None


def _fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None).isoformat()


# Shared row expressions. custom_fields values are strings in ClickHouse, so
# numeric fields go through toFloat64OrNull / toInt32* before aggregation; this
# mirrors the typed columns the standard DuckDB path reads directly.
_RTT = "toFloat64OrNull(custom_fields['tcp_rtt'])"
_RTT_MIN = "toFloat64OrNull(custom_fields['rtt_min'])"
_RTT_VAR = "toFloat64OrNull(custom_fields['rtt_var'])"
_PLOSS = "toFloat64OrNull(custom_fields['ploss'])"
_STATUS = "toInt32OrZero(custom_fields['status'])"
_ASN = "toInt32OrNull(custom_fields['asn'])"

_BASE_WHERE = (
    "service_id={service_id:String} AND publication_state='visible' "
    "AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}"
)
# RTT filter mirrors the standard: only rows with a real TCP RTT contribute to
# network-quality aggregates (tcp_rtt IS NOT NULL AND tcp_rtt > 0).
_RTT_FILTER = f"{_BASE_WHERE} AND {_RTT} > 0"
_TTFB = "toFloat64OrNull(custom_fields['ttfb'])"
_ELAPSED = "toFloat64OrZero(custom_fields['elapsed'])"
_RESP_BYTES = "toFloat64OrZero(custom_fields['resp_bytes'])"
_HIT = "custom_fields['cache'] LIKE '%HIT%'"

# Sections that need the core (heatmap / map / leaderboard / summary) scans,
# mirroring routers.network.CORE_SECTIONS (core cannot import routers).
_CORE_SECTIONS = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
_SCATTER_SAMPLE = 2000
_SHIELDING_ROUTE_LIMIT = 50
_TRANSPORT_FIELDS = ("tcp_rtt", "retrans", "ploss", "delivery_rate")


def _filter_clause(filters: Any, *, drop_asn: bool = False) -> tuple[str, dict[str, Any]]:
    """ClickHouse predicate (leading ``AND``) + params for the request's column filters."""
    spec = dict(filters or {})
    if drop_asn:
        spec.pop("asn", None)
    clause, params = _build_clickhouse_filters(spec)
    if clause == "1=1":
        return "", {}
    return f" AND {clause}", params


def _want_core(sections: Any) -> bool:
    return not sections or "health" in sections or bool(_CORE_SECTIONS & set(sections))


def _want_shielding(sections: Any) -> bool:
    return not sections or "health" in sections or "shielding_analysis" in sections


def _asn_names(service: HighScaleService, asns: list[int]) -> dict[int, str]:
    if not asns:
        return {}
    try:
        from backend.core import duckdb as _db

        return _db.get_asn_names(service.service_id, asns)
    except Exception:
        return {}


def _asn_label(asn: int, names: dict[int, str]) -> str:
    from backend.core import duckdb as _db

    return _db.format_asn_label(asn, names.get(asn) or f"AS{asn}")


def _shielding_analysis(service: HighScaleService, start: datetime, end: datetime, filters: Any) -> ShieldingAnalysis:
    """Edge-to-shield POP latency pairs via the rid/prid self-join (standard-path parity).

    User filters constrain the edge side only; the shield side keeps just the time
    bounds so an edge filter such as ``pop = DEN`` cannot strip the shield hit.
    """
    from backend.repositories.origin import _enrich_with_distance

    filter_sql, filter_params = _filter_clause(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    hop_ms = "coalesce(toFloat64OrNull(custom_fields['ottfb']), toFloat64OrNull(custom_fields['ttfb']) * 1000000.0)"
    shield_where = f"{_BASE_WHERE} AND lower(custom_fields['edge']) IN ('false', '0') AND custom_fields['prid'] != ''"
    pairs = service.client.execute(
        f"""
        SELECT
            e.edge_pop AS edge_pop,
            s.shield_pop AS shield_pop,
            count() AS requests,
            quantile(0.50)(e.hop_us - s.hop_us) / 1000.0 AS p50_ms,
            quantile(0.95)(e.hop_us - s.hop_us) / 1000.0 AS p95_ms,
            quantile(0.99)(e.hop_us - s.hop_us) / 1000.0 AS p99_ms
        FROM (
            SELECT custom_fields['rid'] AS rid, custom_fields['pop'] AS edge_pop, {hop_ms} AS hop_us
            FROM request_facts
            WHERE {_BASE_WHERE}{filter_sql} AND lower(custom_fields['edge']) IN ('true', '1')
        ) AS e
        INNER JOIN (
            SELECT custom_fields['prid'] AS prid, custom_fields['pop'] AS shield_pop, {hop_ms} AS hop_us
            FROM request_facts
            WHERE {shield_where}
        ) AS s ON s.prid = e.rid
        GROUP BY edge_pop, shield_pop
        """,
        params,
    )
    if not pairs:
        shield_rows = service.client.execute(
            f"SELECT count() AS n FROM (SELECT 1 FROM request_facts WHERE {shield_where} LIMIT 1)",
            {"service_id": service.service_id, "start": start, "end": end},
        )
        if shield_rows and int(shield_rows[0]["n"]) == 0:
            return ShieldingAnalysis(has_data=False, edge_only=True, rows=[])
        return ShieldingAnalysis(has_data=False, rows=[])

    # Keep the union of the top routes by volume and by transit overhead so a
    # low-volume mis-peered route is never truncated away behind busy ones.
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
                    "p50_ms": float(r["p50_ms"]) if r["p50_ms"] is not None else None,
                    "p95_ms": float(r["p95_ms"]) if r["p95_ms"] is not None else None,
                    "p99_ms": float(r["p99_ms"]) if r["p99_ms"] is not None else None,
                }
            )
        )
        for r in sorted(kept.values(), key=lambda r: int(r["requests"]), reverse=True)
    ]
    return ShieldingAnalysis(has_data=True, rows=rows, total_routes=len(pairs), truncated=len(pairs) > len(rows))


def origin_shielding_analysis(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> OriginShieldingAnalysisResponse:
    """Serve ``/api/origin/shielding-analysis`` from ClickHouse with the Network card's query."""
    end = _range_value(end_time) or datetime.now(UTC)
    start = _range_value(start_time) or (end - timedelta(days=1))
    analysis = _shielding_analysis(service, start, end, getattr(req, "filters", None))
    limit = int(getattr(req, "limit", 50) or 50)
    rows = analysis.rows[:limit]
    truncated = (bool(analysis.truncated) or len(analysis.rows) > limit) if analysis.has_data else None
    return OriginShieldingAnalysisResponse.with_telemetry(
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
    now = datetime.now(UTC)
    end = _range_value(end_time) or now
    start = _range_value(start_time) or (end - timedelta(days=1))
    bucket_seconds = int(getattr(req, "bucket_seconds", 300) or 300)
    top_n = int(getattr(req, "top_n", 30) or 30)
    map_asn = str(getattr(req, "map_asn", "all") or "all")
    sections = getattr(req, "sections", None)

    shielding = None
    if _want_shielding(sections):
        shielding = _shielding_analysis(service, start, end, getattr(req, "filters", None))
    if not _want_core(sections):
        return NetworkHealthResponse.with_telemetry(available=True, shielding_analysis=shielding)

    # Coarsen for wide windows exactly like the standard path (network.py:348)
    # so the heatmap x-axis never exceeds ~8640 buckets.
    if start is not None and end is not None:
        span_secs = int((end - start).total_seconds())
        bucket_seconds = max(bucket_seconds, span_secs // 8640) or bucket_seconds

    bs = bucket_seconds
    # A map_asn drill-down replaces the ASN filter for the whole window, like the
    # standard path (the map is then pinned to that ASN instead).
    filter_sql, filter_params = _filter_clause(getattr(req, "filters", None), drop_asn=map_asn != "all")
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    rtt_where = f"{_RTT_FILTER}{filter_sql}"
    bucket_expr = f"intDiv(toUnixTimestamp(event_timestamp), {bs}) * {bs}"

    def _run(sql: str) -> list[dict]:
        return service.client.execute(sql, params)

    map_asn_sql = ""
    if map_asn != "all":
        map_asn_sql = f" AND {_ASN} = {{map_asn:Int32}}"
        params["map_asn"] = int(map_asn)

    heatmap_sql = f"""
        SELECT
            {_ASN} AS asn,
            {bucket_expr} AS bucket_ts,
            count() AS reqs,
            median(if({_HIT} AND {_ELAPSED} > 0, {_RESP_BYTES} * 1000000.0 / {_ELAPSED}, NULL)) AS throughput_bps,
            median({_RTT}) AS rtt_med_us,
            median({_RTT_MIN}) AS rtt_baseline_us,
            median({_RTT} - {_RTT_MIN}) AS rtt_congestion_us,
            avg({_PLOSS}) AS avg_ploss,
            median({_RTT_VAR}) AS rtt_jitter_us,
            countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct
        FROM request_facts
        WHERE {rtt_where} AND {_ASN} IS NOT NULL
        GROUP BY asn, bucket_ts
        ORDER BY reqs DESC
        LIMIT {top_n * 200}
    """
    map_sql = f"""
        SELECT
            country AS country,
            custom_fields['city'] AS city,
            toFloat64OrNull(custom_fields['lat']) AS lat,
            toFloat64OrNull(custom_fields['lon']) AS lon,
            custom_fields['metro'] AS metro,
            {bucket_expr} AS bucket_ts,
            median({_RTT}) AS rtt_med_us,
            avg({_PLOSS}) AS avg_ploss,
            countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct,
            count() AS reqs
        FROM request_facts
        WHERE {rtt_where} AND country != ''{map_asn_sql}
        GROUP BY country, city, lat, lon, metro, bucket_ts
        ORDER BY reqs DESC
        LIMIT 5000
    """
    metro_sql = f"""
        SELECT
            country AS country,
            custom_fields['city'] AS city,
            custom_fields['region'] AS region,
            custom_fields['metro'] AS metro,
            avg({_PLOSS}) AS avg_ploss,
            countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct,
            count() AS total_reqs
        FROM request_facts
        WHERE {rtt_where} AND country != ''
        GROUP BY country, city, region, metro
        ORDER BY total_reqs DESC
        LIMIT 200
    """
    speed_sql = f"""
        SELECT {_ASN} AS asn, custom_fields['c_speed'] AS c_speed, count() AS cnt
        FROM request_facts
        WHERE {rtt_where} AND {_ASN} IS NOT NULL AND custom_fields['c_speed'] != ''
        GROUP BY asn, c_speed
        ORDER BY cnt DESC
    """
    pct_sql = f"""
        SELECT {_ASN} AS asn, quantile(0.95)({_RTT}) AS p95, quantile(0.99)({_RTT}) AS p99
        FROM request_facts
        WHERE {rtt_where} AND {_ASN} IS NOT NULL
        GROUP BY asn
    """
    sql_batch = [heatmap_sql, map_sql, metro_sql, speed_sql, pct_sql]
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-net-health") as pool:
        futs = [pool.submit(contextvars.copy_context().run, _run, q) for q in sql_batch]
        heatmap_rows, map_rows, metro_rows, speed_rows, pct_rows = [f.result() for f in futs]

    # ── Derive top ASNs by request volume ───────────────────────────────────
    all_asns_seen: dict[int, int] = {}
    for r in heatmap_rows:
        if r["asn"] is None:
            continue
        asn = int(r["asn"])
        all_asns_seen[asn] = all_asns_seen.get(asn, 0) + int(r["reqs"])
    top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
    top_asn_set = set(top_asns)
    asn_names = _asn_names(service, top_asns)

    # ── Full bucket axis (no gaps) ──────────────────────────────────────────
    all_buckets: list[str] = []
    if start is not None and end is not None:
        curr = (int(start.timestamp()) // bs) * bs
        last = (int(end.timestamp()) // bs) * bs
        while curr <= last:
            all_buckets.append(_fmt_ts(curr))
            curr += bs
    bucket_idx = {b: i for i, b in enumerate(all_buckets)}

    # ── Speed mix (top 5 classes per ASN, normalized) ───────────────────────
    asn_speed_rows: dict[int, list[tuple[str, int]]] = {}
    for r in speed_rows:
        if r["asn"] is None:
            continue
        asn_v = int(r["asn"])
        asn_speed_rows.setdefault(asn_v, [])
        if len(asn_speed_rows[asn_v]) < 5:
            asn_speed_rows[asn_v].append((str(r["c_speed"]), int(r["cnt"])))
    asn_speed_mix: dict[int, dict[str, float]] = {}
    for asn_v, rows in asn_speed_rows.items():
        total = sum(cnt for _, cnt in rows)
        if total > 0:
            asn_speed_mix[asn_v] = {cs: round(cnt / total, 3) for cs, cnt in rows}

    asn_rtt_pct: dict[int, dict[str, float | None]] = {}
    for r in pct_rows:
        if r["asn"] is None:
            continue
        asn_rtt_pct[int(r["asn"])] = {
            "p95_rtt_us": float(r["p95"]) if r["p95"] is not None else None,
            "p99_rtt_us": float(r["p99"]) if r["p99"] is not None else None,
        }

    # ── Per-ASN bucket cells ────────────────────────────────────────────────
    asn_bucket_data: dict[int, dict[str, dict]] = {}
    for r in heatmap_rows:
        if r["asn"] is None or int(r["asn"]) not in top_asn_set:
            continue
        asn = int(r["asn"])
        bucket = _fmt_ts(int(r["bucket_ts"]))
        if bucket not in bucket_idx:
            continue
        tp = float(r["throughput_bps"]) if r.get("throughput_bps") is not None else None
        rtt = float(r["rtt_med_us"]) if r["rtt_med_us"] is not None else None
        rtt_base = float(r["rtt_baseline_us"]) if r["rtt_baseline_us"] is not None else None
        rtt_cong = float(r["rtt_congestion_us"]) if r["rtt_congestion_us"] is not None else None
        pkt = float(r["avg_ploss"]) if r["avg_ploss"] is not None else None
        jitter = float(r["rtt_jitter_us"]) if r["rtt_jitter_us"] is not None else None
        err = float(r["error_pct"]) if r["error_pct"] is not None else None
        reqs = int(r["reqs"])
        hs = _health_score(tp, rtt_cong, pkt, jitter, err)
        asn_bucket_data.setdefault(asn, {})[bucket] = {
            "bucket_idx": bucket_idx[bucket],
            "bucket": bucket,
            "throughput_bps": round(tp, 0) if tp is not None else None,
            "rtt_med_us": round(rtt, 0) if rtt is not None else None,
            "rtt_baseline_us": round(rtt_base, 0) if rtt_base is not None else None,
            "rtt_congestion_us": round(rtt_cong, 0) if rtt_cong is not None else None,
            "avg_ploss": round(pkt, 5) if pkt is not None else None,
            "rtt_jitter_us": round(jitter, 0) if jitter is not None else None,
            "error_pct": round(err, 2) if err is not None else None,
            "health_score": hs,
            "reqs": reqs,
        }

    heatmap = []
    for asn in top_asns:
        heatmap.append(
            {
                "asn": asn,
                "label": _asn_label(asn, asn_names),
                "total_reqs": all_asns_seen.get(asn, 0),
                "buckets": list(asn_bucket_data.get(asn, {}).values()),
            }
        )

    # ── World map buckets + interned cities ─────────────────────────────────
    has_metro = any((r.get("metro") or "") for r in map_rows) or any((r.get("metro") or "") for r in metro_rows)
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
        key = (name, lat, lon)
        idx = cities_index.get(key)
        if idx is None:
            idx = len(cities_list)
            cities_list.append({"name": name, "lat": lat, "lon": lon})
            cities_index[key] = idx
        return idx

    map_by_bucket: dict[str, list[dict]] = {}
    countries_set: set[str] = set()
    for r in map_rows:
        ctry = r["country"] or ""
        if ctry:
            countries_set.add(ctry)
        city = r["city"] or ""
        lat = float(r["lat"]) if r["lat"] is not None else None
        lon = float(r["lon"]) if r["lon"] is not None else None
        bucket = _fmt_ts(int(r["bucket_ts"]))
        if bucket not in bucket_idx:
            continue
        rtt = float(r["rtt_med_us"]) if r["rtt_med_us"] is not None else None
        pkt = float(r["avg_ploss"]) if r["avg_ploss"] is not None else None
        err = float(r["error_pct"]) if r["error_pct"] is not None else None
        reqs = int(r["reqs"])
        hs = _health_score(None, None, pkt, None, err)

        metro_code: int | None = None
        metro_raw = r.get("metro")
        if metro_raw:
            try:
                metro_code = int(float(metro_raw))
            except (ValueError, TypeError):
                metro_code = None

        display_city = city.title() if city else ""
        if metro_code is not None and str(metro_code) in dma_map:
            display_city = dma_map[str(metro_code)]

        map_by_bucket.setdefault(bucket, []).append(
            {
                "country": ctry,
                "city_idx": _intern_city(display_city, lat, lon),
                "metro_code": metro_code,
                "rtt_med_us": rtt,
                "avg_ploss": pkt,
                "error_pct": round(err, 2) if err is not None else None,
                "health_score": hs,
                "reqs": reqs,
            }
        )

    map_buckets: list[dict] = []
    if map_by_bucket:
        for i, bucket in enumerate(all_buckets):
            map_buckets.append({"bucket_idx": i, "bucket": bucket, "cities": map_by_bucket.get(bucket, [])})

    # ── Metro leaderboard ───────────────────────────────────────────────────
    metro_leaderboard: list[dict] = []
    for r in metro_rows:
        ctry = r["country"] or ""
        city = r["city"] or ""
        region = r["region"] or ""
        pkt = float(r["avg_ploss"]) if r["avg_ploss"] is not None else None
        err = float(r["error_pct"]) if r["error_pct"] is not None else None
        hs = _health_score(None, None, pkt, None, err)

        metro_code = None
        metro_raw = r.get("metro")
        if metro_raw:
            try:
                metro_code = int(float(metro_raw))
            except (ValueError, TypeError):
                metro_code = None

        if metro_code is not None and str(metro_code) in dma_map:
            display = format_city_label(dma_map[str(metro_code)], ctry, region)
        else:
            display = format_city_label(city, ctry, region)

        metro_leaderboard.append(
            {
                "country": ctry,
                "region": region,
                "raw_city": city,
                "city": display,
                "total_reqs": int(r["total_reqs"]),
                "health_score": hs,
            }
        )
    # Deduplicate by display name (same city with/without metro code).
    seen: dict[str, dict] = {}
    for entry in metro_leaderboard:
        key = entry["city"]
        if key in seen:
            prev = seen[key]
            total = prev["total_reqs"] + entry["total_reqs"]
            if prev["health_score"] is not None and entry["health_score"] is not None:
                prev["health_score"] = round(
                    (prev["health_score"] * prev["total_reqs"] + entry["health_score"] * entry["total_reqs"]) / total,
                    1,
                )
            prev["total_reqs"] = total
        else:
            seen[key] = dict(entry)
    metro_leaderboard = sorted(seen.values(), key=lambda x: x["total_reqs"], reverse=True)

    # ── ASN leaderboard (trend + percentiles + speed mix) ───────────────────
    leaderboard: list[dict] = []
    for asn in top_asns:
        buckets_data = asn_bucket_data.get(asn, {})
        if not buckets_data:
            continue
        latest_bucket = max(buckets_data)
        hs_now = buckets_data[latest_bucket].get("health_score")
        hs_1h = hs_now
        hs_1w = hs_now
        if len(all_buckets) > 1:
            n_buckets_1h = max(1, 3600 // bs)
            sorted_buckets = sorted(buckets_data)
            early = sorted_buckets[: max(1, len(sorted_buckets) // 4)]
            hs_1w = _avg_hs(buckets_data, early)
            mid = sorted_buckets[max(0, len(sorted_buckets) - n_buckets_1h) :]
            hs_1h = _avg_hs(buckets_data, mid)
        trend = "stable"
        if hs_now is not None and hs_1h is not None:
            delta = hs_now - hs_1h
            if delta < -5:
                trend = "degrading"
            elif delta > 5:
                trend = "improving"
        rtt_pct = asn_rtt_pct.get(asn, {})
        leaderboard.append(
            {
                "asn": asn,
                "label": _asn_label(asn, asn_names),
                "health_score_now": hs_now,
                "health_score_1h_ago": hs_1h,
                "health_score_1w_ago": hs_1w,
                "trend": trend,
                "total_reqs": all_asns_seen.get(asn, 0),
                "c_speed_mix": asn_speed_mix.get(asn, {}),
                "p95_rtt_us": rtt_pct.get("p95_rtt_us"),
                "p99_rtt_us": rtt_pct.get("p99_rtt_us"),
            }
        )

    # ── Global summary ──────────────────────────────────────────────────────
    all_hs = [le["health_score_now"] for le in leaderboard if le["health_score_now"] is not None]
    global_hs = round(sum(all_hs) / len(all_hs), 1) if all_hs else 0.0

    all_rtt: list[float] = []
    total_reqs = 0
    for r in heatmap_rows:
        if r["rtt_med_us"] is not None:
            all_rtt.append(float(r["rtt_med_us"]))
        total_reqs += int(r["reqs"])
    avg_rtt_ms = round(sum(all_rtt) / len(all_rtt) / 1000.0, 1) if all_rtt else 0.0

    worst_asn = None
    if leaderboard:
        significant = [le for le in leaderboard if le["total_reqs"] > total_reqs * 0.01]
        if significant:
            worst = min(
                significant, key=lambda le: le["health_score_now"] if le["health_score_now"] is not None else 100
            )
            worst_asn = NetworkWorstEntry(label=worst["label"], score=worst["health_score_now"])

    worst_country = None
    if map_buckets:
        for b in reversed(map_buckets):
            sig = [c for c in (b.get("cities") or []) if c.get("reqs", 0) >= 1]
            if sig:
                wc = min(sig, key=lambda c: c["health_score"] if c["health_score"] is not None else 100)
                idx = wc.get("city_idx", -1)
                city_name = cities_list[idx]["name"] if 0 <= idx < len(cities_list) else ""
                worst_country = NetworkWorstEntry(
                    label=format_city_label(city_name, wc["country"]), score=wc["health_score"]
                )
                break

    summary = NetworkHealthSummary(
        global_health_score=global_hs,
        avg_rtt_ms=avg_rtt_ms,
        total_reqs=total_reqs,
        worst_asn=worst_asn,
        worst_country=worst_country,
    )

    want_map_payload = not sections or "health" in sections or "map_buckets" in sections or "cities" in sections
    return NetworkHealthResponse.with_telemetry(
        available=True,
        has_data=bool(heatmap_rows),
        metric="health_score",
        bucket_seconds=bs,
        buckets=all_buckets,
        heatmap=heatmap,
        map_buckets=map_buckets if want_map_payload else [],
        cities=[NetworkCityEntry(**c) for c in cities_list] if want_map_payload else [],
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
    now = datetime.now(UTC)
    end = _range_value(end_time) or now
    start = _range_value(start_time) or (end - timedelta(days=1))
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
        sql = f"""
            SELECT {group_expr} AS label, median({_RTT} / 1000.0) AS rtt_ms, count() AS reqs
            FROM request_facts
            WHERE {rtt_where}{extra_where}
            GROUP BY label
            ORDER BY reqs DESC
            LIMIT 25
        """
        rows = service.client.execute(sql, params)
        out = []
        for r in rows:
            label = r["label"]
            if label is None or label == "":
                continue
            out.append(
                {
                    "value": str(label),
                    "label": str(label),
                    "rtt_ms": round(float(r["rtt_ms"]), 2) if r["rtt_ms"] is not None else 0.0,
                    "reqs": int(r["reqs"]),
                }
            )
        return out

    scatter_sql = f"""
        SELECT
            {_RTT} / 1000.0 AS rtt_ms,
            {_TTFB} * 1000.0 AS ttfb_ms,
            if(custom_fields['cache'] = '', 'UNKNOWN', custom_fields['cache']) AS cache_state
        FROM request_facts
        WHERE {rtt_where} AND {_TTFB} > 0
        ORDER BY cityHash64(event_id)
        LIMIT {_SCATTER_SAMPLE}
        """

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-net-quality") as pool:
        f_country = pool.submit(contextvars.copy_context().run, _bar, "country")
        f_asn = pool.submit(contextvars.copy_context().run, _bar, _ASN)
        f_region = pool.submit(
            contextvars.copy_context().run,
            _bar,
            "custom_fields['region']",
            " AND country = {region_country:String}",
        )
        f_pop = pool.submit(contextvars.copy_context().run, _bar, "custom_fields['pop']")
        f_scatter = pool.submit(contextvars.copy_context().run, service.client.execute, scatter_sql, params)
        by_country, by_asn, by_region, by_pop, scatter_rows = [
            f.result() for f in (f_country, f_asn, f_region, f_pop, f_scatter)
        ]

    if by_asn:
        # Mirror the standard path: "Name (7922)" label, bare ASN stays the filter key.
        names = _asn_names(service, [int(row["value"]) for row in by_asn if row["value"].isdigit()])
        for row in by_asn:
            if row["value"].isdigit():
                row["label"] = _asn_label(int(row["value"]), names)

    countries = sorted({row["value"] for row in by_country})
    scatter = [
        {
            "rtt_ms": round(float(r["rtt_ms"]), 2),
            "ttfb_ms": round(float(r["ttfb_ms"]), 2),
            "cache": str(r["cache_state"]),
        }
        for r in scatter_rows
    ]

    return NetworkQualityResponse.with_telemetry(
        available=True,
        by_country=by_country,
        by_asn=by_asn,
        by_region=by_region,
        region_country=region_country,
        by_pop=by_pop,
        scatter=scatter,
        countries=countries,
    )


def get_pop_health(
    service: HighScaleService, start_time: datetime | None, end_time: datetime | None
) -> PopHealthListResponse:
    end = end_time or datetime.now(UTC)
    start = start_time or (end - timedelta(hours=24))
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    rows = service.client.execute(
        f"""
        SELECT
            custom_fields['pop'] AS pop,
            count() AS requests,
            countIf({_STATUS} >= 400 OR {_STATUS} = 0) AS errors,
            quantile(0.5)({_RTT}) AS p50_rtt_us,
            quantile(0.95)({_TTFB}) * 1000.0 AS p95_ttfb_ms,
            countIf(custom_fields['cache'] IN ('HIT', 'HIT-STALE')) AS cache_hits,
            sum(toInt64OrZero(custom_fields['resp_bytes'])) AS bandwidth_bytes
        FROM request_facts
        WHERE {_BASE_WHERE} AND custom_fields['pop'] != ''
        GROUP BY pop
        ORDER BY requests DESC
        """,
        {"service_id": service.service_id, "start": start, "end": end},
    )
    data = []
    for r in rows:
        requests = int(r["requests"])
        errors = int(r["errors"])
        data.append(
            PopHealthItem(
                pop=str(r["pop"]),
                requests=requests,
                errors=errors,
                error_rate=round(errors * 100.0 / requests, 2) if requests else 0.0,
                p50_rtt_us=float(r["p50_rtt_us"]) if r["p50_rtt_us"] is not None else None,
                p95_ttfb_ms=float(r["p95_ttfb_ms"]) if r["p95_ttfb_ms"] is not None else None,
                cache_hit_rate=round(int(r["cache_hits"]) * 100.0 / requests, 2) if requests else 0.0,
                bandwidth_bytes=int(r["bandwidth_bytes"] or 0),
            )
        )
    return PopHealthListResponse.with_telemetry(data=data)


def get_transport_metrics(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
    filters: Any = None,
) -> TransportMetrics:
    now = datetime.now(UTC)
    end = _range_value(end_time) or now
    start = _range_value(start_time) or (end - timedelta(days=1))
    filter_sql, filter_params = _filter_clause(filters)
    params = {"service_id": service.service_id, "start": start, "end": end, **filter_params}
    base_where = f"{_BASE_WHERE}{filter_sql}"

    sql = f"""
        SELECT
            count() AS total_reqs,
            sum(toInt64OrZero(custom_fields['resp_bytes'])) AS total_bytes,
            quantile(0.50)({_RTT}) / 1000.0 AS rtt_p50,
            quantile(0.90)({_RTT}) / 1000.0 AS rtt_p90,
            quantile(0.95)({_RTT}) / 1000.0 AS rtt_p95,
            quantile(0.99)({_RTT}) / 1000.0 AS rtt_p99,
            avg(toFloat64OrNull(custom_fields['retrans'])) AS avg_retrans,
            avg({_PLOSS}) * 100.0 AS avg_ploss_pct,
            avg(toFloat64OrNull(custom_fields['delivery_rate'])) * 8.0 / 1e6 AS avg_del_rate_mbps,
            countIf(custom_fields['tcp_rtt'] != '') AS seen_tcp_rtt,
            countIf(custom_fields['retrans'] != '') AS seen_retrans,
            countIf(custom_fields['ploss'] != '') AS seen_ploss,
            countIf(custom_fields['delivery_rate'] != '') AS seen_delivery_rate
        FROM request_facts
        WHERE {base_where}
    """
    proto_sql = f"""
        SELECT
            caseWithExpression(
                custom_fields['proto'],
                '3.0', 'HTTP/3',
                '3', 'HTTP/3',
                '2.0', 'HTTP/2',
                '2', 'HTTP/2',
                '1.1', 'HTTP/1.1',
                '1.0', 'HTTP/1.0',
                if(empty(custom_fields['proto']), 'Other', custom_fields['proto'])
            ) AS proto_clean,
            count() AS reqs
        FROM request_facts
        WHERE {base_where}
        GROUP BY proto_clean
        ORDER BY reqs DESC
    """
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-net-transport") as pool:
        f_main = pool.submit(contextvars.copy_context().run, service.client.execute, sql, params)
        f_proto = pool.submit(contextvars.copy_context().run, service.client.execute, proto_sql, params)
        rows = f_main.result()
        p_rows = f_proto.result()

    if rows:
        r = rows[0]
        total_requests = int(r.get("total_reqs") or 0)
        bandwidth_bytes = int(r.get("total_bytes") or 0)
        tcp_rtt_p50 = round(float(r["rtt_p50"]), 2) if r.get("rtt_p50") is not None else None
        tcp_rtt_p90 = round(float(r["rtt_p90"]), 2) if r.get("rtt_p90") is not None else None
        tcp_rtt_p95 = round(float(r["rtt_p95"]), 2) if r.get("rtt_p95") is not None else None
        tcp_rtt_p99 = round(float(r["rtt_p99"]), 2) if r.get("rtt_p99") is not None else None
        retrans_rate = round(float(r["avg_retrans"]), 3) if r.get("avg_retrans") is not None else None
        packet_loss = round(float(r["avg_ploss_pct"]), 3) if r.get("avg_ploss_pct") is not None else None
        delivery_rate = round(float(r["avg_del_rate_mbps"]), 2) if r.get("avg_del_rate_mbps") is not None else None
        # No request in the window carries the field => the service does not log it.
        missing_fields = (
            [f for f in _TRANSPORT_FIELDS if not int(r.get(f"seen_{f}") or 0)] if total_requests > 0 else []
        )
    else:
        missing_fields = []
        total_requests = 0
        bandwidth_bytes = 0
        tcp_rtt_p50 = tcp_rtt_p90 = tcp_rtt_p95 = tcp_rtt_p99 = None
        retrans_rate = packet_loss = delivery_rate = None

    protocol_distribution = []
    sum_p = sum(r.get("reqs", 0) for r in p_rows) or total_requests or 1
    for pr in p_rows:
        p_name = str(pr.get("proto_clean", "Other"))
        p_cnt = int(pr.get("reqs", 0))
        protocol_distribution.append(
            TransportProtocolShare(
                protocol=p_name,
                requests=p_cnt,
                share_pct=round(p_cnt * 100.0 / sum_p, 2),
            )
        )

    return TransportMetrics(
        has_transport_data=len(missing_fields) < len(_TRANSPORT_FIELDS),
        tcp_rtt_p50_ms=tcp_rtt_p50,
        tcp_rtt_p90_ms=tcp_rtt_p90,
        tcp_rtt_p95_ms=tcp_rtt_p95,
        tcp_rtt_p99_ms=tcp_rtt_p99,
        retrans_rate_pct=retrans_rate,
        packet_loss_pct=packet_loss,
        delivery_rate_mbps=delivery_rate,
        bandwidth_bytes=bandwidth_bytes,
        total_requests=total_requests,
        protocol_distribution=protocol_distribution,
        missing_fields=missing_fields,
    )


def network_bundle(
    service: HighScaleService,
    req: NetworkBundleRequest,
    start_time: str | None,
    end_time: str | None,
) -> NetworkBundleResponse:
    sections = set(req.sections) if req.sections else None
    want_health = sections is None or "health" in sections or bool(sections & (_CORE_SECTIONS | {"shielding_analysis"}))
    want_quality = sections is None or "quality" in sections
    want_pop = sections is None or "pop_health" in sections
    want_transport = sections is None or "transport" in sections

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="hs-net-bundle") as pool:
        f_health = (
            pool.submit(contextvars.copy_context().run, network_health, service, req, start_time, end_time)
            if want_health
            else None
        )
        f_quality = (
            pool.submit(
                contextvars.copy_context().run,
                network_quality,
                service,
                NetworkQualityRequest(
                    region_country=req.region_country, filters=req.filters, start_time=start_time, end_time=end_time
                ),
                start_time,
                end_time,
            )
            if want_quality
            else None
        )
        f_pop = (
            pool.submit(
                contextvars.copy_context().run,
                lambda: get_pop_health(service, _range_value(start_time), _range_value(end_time)).data,
            )
            if want_pop
            else None
        )
        f_transport = (
            pool.submit(
                contextvars.copy_context().run, get_transport_metrics, service, start_time, end_time, req.filters
            )
            if want_transport
            else None
        )
        health_resp = f_health.result() if f_health else None
        quality_resp = f_quality.result() if f_quality else None
        pop_health = f_pop.result() if f_pop else None
        transport_resp = f_transport.result() if f_transport else None

    return NetworkBundleResponse.with_telemetry(
        available=True,
        health=health_resp,
        quality=quality_resp,
        pop_health=pop_health,
        transport=transport_resp,
    )
