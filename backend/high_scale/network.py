from datetime import UTC, datetime, timedelta
from typing import Any

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
    PopHealthListResponse,
    TransportMetrics,
    TransportProtocolShare,
)
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


def network_health(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkHealthResponse:
    now = datetime.now(UTC)
    end = _range_value(end_time) or now
    start = _range_value(start_time) or (end - timedelta(days=1))
    bucket_seconds = int(getattr(req, "bucket_seconds", 300) or 300)
    top_n = int(getattr(req, "top_n", 30) or 30)

    # Coarsen for wide windows exactly like the standard path (network.py:348)
    # so the heatmap x-axis never exceeds ~8640 buckets.
    if start is not None and end is not None:
        span_secs = int((end - start).total_seconds())
        bucket_seconds = max(bucket_seconds, span_secs // 8640) or bucket_seconds

    bs = bucket_seconds
    params = {"service_id": service.service_id, "start": start, "end": end}
    bucket_expr = f"intDiv(toUnixTimestamp(event_timestamp), {bs}) * {bs}"

    def _run(sql: str) -> list[dict]:
        try:
            return service.client.execute(sql, params)
        except Exception:
            return []

    # ── Heatmap inputs: per (asn, bucket) health components ─────────────────
    heatmap_rows = _run(
        f"""
        SELECT
            {_ASN} AS asn,
            {bucket_expr} AS bucket_ts,
            count() AS reqs,
            median({_RTT}) AS rtt_med_us,
            median({_RTT_MIN}) AS rtt_baseline_us,
            median({_RTT} - {_RTT_MIN}) AS rtt_congestion_us,
            avg({_PLOSS}) AS avg_ploss,
            median({_RTT_VAR}) AS rtt_jitter_us,
            countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct
        FROM request_facts
        WHERE {_RTT_FILTER} AND {_ASN} IS NOT NULL
        GROUP BY asn, bucket_ts
        """
    )

    # ── World-map inputs: per (country, city, bucket) ───────────────────────
    map_rows = _run(
        f"""
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
        WHERE {_RTT_FILTER} AND country != ''
        GROUP BY country, city, lat, lon, metro, bucket_ts
        """
    )

    # ── Metro leaderboard inputs: per (country, city, region, metro) ────────
    metro_rows = _run(
        f"""
        SELECT
            country AS country,
            custom_fields['city'] AS city,
            custom_fields['region'] AS region,
            custom_fields['metro'] AS metro,
            avg({_PLOSS}) AS avg_ploss,
            countIf({_STATUS} >= 500) * 100.0 / count() AS error_pct,
            count() AS total_reqs
        FROM request_facts
        WHERE {_RTT_FILTER} AND country != ''
        GROUP BY country, city, region, metro
        ORDER BY total_reqs DESC
        LIMIT 200
        """
    )

    # ── Speed-class mix per ASN ─────────────────────────────────────────────
    speed_rows = _run(
        f"""
        SELECT {_ASN} AS asn, custom_fields['c_speed'] AS c_speed, count() AS cnt
        FROM request_facts
        WHERE {_RTT_FILTER} AND {_ASN} IS NOT NULL AND custom_fields['c_speed'] != ''
        GROUP BY asn, c_speed
        ORDER BY cnt DESC
        """
    )

    # ── P95 / P99 RTT per ASN ───────────────────────────────────────────────
    pct_rows = _run(
        f"""
        SELECT {_ASN} AS asn, quantile(0.95)({_RTT}) AS p95, quantile(0.99)({_RTT}) AS p99
        FROM request_facts
        WHERE {_RTT_FILTER} AND {_ASN} IS NOT NULL
        GROUP BY asn
        """
    )

    # ── Derive top ASNs by request volume ───────────────────────────────────
    all_asns_seen: dict[int, int] = {}
    for r in heatmap_rows:
        if r["asn"] is None:
            continue
        asn = int(r["asn"])
        all_asns_seen[asn] = all_asns_seen.get(asn, 0) + int(r["reqs"])
    top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
    top_asn_set = set(top_asns)

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
        rtt = float(r["rtt_med_us"]) if r["rtt_med_us"] is not None else None
        rtt_base = float(r["rtt_baseline_us"]) if r["rtt_baseline_us"] is not None else None
        rtt_cong = float(r["rtt_congestion_us"]) if r["rtt_congestion_us"] is not None else None
        pkt = float(r["avg_ploss"]) if r["avg_ploss"] is not None else None
        jitter = float(r["rtt_jitter_us"]) if r["rtt_jitter_us"] is not None else None
        err = float(r["error_pct"]) if r["error_pct"] is not None else None
        reqs = int(r["reqs"])
        hs = _health_score(None, rtt_cong, pkt, jitter, err)
        asn_bucket_data.setdefault(asn, {})[bucket] = {
            "bucket_idx": bucket_idx[bucket],
            "bucket": bucket,
            "throughput_bps": None,
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
                "label": f"AS{asn}",
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
                "label": f"AS{asn}",
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

    return NetworkHealthResponse.with_telemetry(
        available=True,
        has_data=bool(heatmap_rows),
        metric="health_score",
        bucket_seconds=bs,
        buckets=all_buckets,
        heatmap=heatmap,
        map_buckets=map_buckets,
        cities=[NetworkCityEntry(**c) for c in cities_list],
        leaderboard=leaderboard,
        metro_leaderboard=metro_leaderboard,
        summary=summary,
        countries=sorted(countries_set),
        has_metro=has_metro,
    )


def network_quality(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> NetworkQualityResponse:
    now = datetime.now(UTC)
    end = _range_value(end_time) or now
    start = _range_value(start_time) or (end - timedelta(days=1))
    region_country = getattr(req, "region_country", "US") or "US"
    params = {"service_id": service.service_id, "start": start, "end": end, "region_country": region_country}

    def _bar(group_expr: str, extra_where: str = "") -> list[dict]:
        sql = f"""
            SELECT {group_expr} AS label, median({_RTT} / 1000.0) AS rtt_ms, count() AS reqs
            FROM request_facts
            WHERE {_RTT_FILTER}{extra_where}
            GROUP BY label
            ORDER BY reqs DESC
            LIMIT 25
        """
        try:
            rows = service.client.execute(sql, params)
        except Exception:
            return []
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

    by_country = _bar("country")
    by_asn = _bar(_ASN)
    by_region = _bar("custom_fields['region']", extra_where=" AND country = {region_country:String}")
    by_pop = _bar("custom_fields['pop']")

    countries = sorted({row["value"] for row in by_country})

    return NetworkQualityResponse.with_telemetry(
        available=True,
        by_country=by_country,
        by_asn=by_asn,
        by_region=by_region,
        region_country=region_country,
        by_pop=by_pop,
        scatter=[],
        countries=countries,
    )


def get_pop_health(
    service: HighScaleService, start_time: datetime | None, end_time: datetime | None
) -> PopHealthListResponse:
    return PopHealthListResponse.with_telemetry(
        data=[],
    )


def get_transport_metrics(
    service: HighScaleService,
    start_time: str | None,
    end_time: str | None,
) -> TransportMetrics:
    now = datetime.now(UTC)
    end = _range_value(end_time) or now
    start = _range_value(start_time) or (end - timedelta(days=1))
    params = {"service_id": service.service_id, "start": start, "end": end}

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
            avg(toFloat64OrNull(custom_fields['delivery_rate'])) * 8.0 / 1e6 AS avg_del_rate_mbps
        FROM request_facts
        WHERE {_BASE_WHERE}
    """
    try:
        rows = service.client.execute(sql, params)
    except Exception:
        rows = []

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
    else:
        total_requests = 0
        bandwidth_bytes = 0
        tcp_rtt_p50 = tcp_rtt_p90 = tcp_rtt_p95 = tcp_rtt_p99 = None
        retrans_rate = packet_loss = delivery_rate = None

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
        WHERE {_BASE_WHERE}
        GROUP BY proto_clean
        ORDER BY reqs DESC
    """
    protocol_distribution = []
    try:
        p_rows = service.client.execute(proto_sql, params)
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
    except Exception:
        pass

    return TransportMetrics(
        has_transport_data=True,
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
        missing_fields=[],
    )


def network_bundle(
    service: HighScaleService,
    req: NetworkBundleRequest,
    start_time: str | None,
    end_time: str | None,
) -> NetworkBundleResponse:
    health_resp = network_health(service, req, start_time, end_time)
    quality_req = NetworkQualityRequest(region_country=req.region_country, start_time=start_time, end_time=end_time)
    quality_resp = network_quality(service, quality_req, start_time, end_time)
    transport_resp = get_transport_metrics(service, start_time, end_time)
    pop_health_resp = get_pop_health(service, _range_value(start_time), _range_value(end_time))

    return NetworkBundleResponse.with_telemetry(
        available=True,
        health=health_resp,
        quality=quality_resp,
        pop_health=pop_health_resp.data,
        transport=transport_resp,
    )
