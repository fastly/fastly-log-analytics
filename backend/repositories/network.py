"""Network repository — health heatmap, world map, quality metrics."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import duckdb

from backend.core import duckdb as _db
from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories._sql import network as SQL
from backend.repositories.utils.filters import build_where_clause
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.geo import format_city_label

# ── Response memo cache ───────────────────────────────────────────────────────
# /api/network-health does a per-request TEMP TABLE build (19 cols, multi-second
# on 30d windows) followed by 6+ aggregate scans. Re-renders triggered by
# mapAsn toggle / filter tweak / refetch tick re-do the entire pipeline even
# when (src, start_time, end_time, filters, bucket_seconds, top_n, map_asn) is
# unchanged. Same standing rule as origin's response cache: "a little behind
# the data" beats "redo the cloud read" — 30 s is well below ingest cadence.
_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128
_response_cache: BoundedTTLCache = BoundedTTLCache(maxsize=_RESPONSE_CACHE_MAXSIZE, ttl_seconds=_RESPONSE_CACHE_TTL)


def _bucket_time_to_minute(ts: str | None) -> str | None:
    if not ts or len(ts) < 16:
        return ts
    return ts[:16]


def _response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    metric: str,
    bucket_seconds: int,
    top_n: int,
    map_asn: str,
) -> str:
    def _filter_attr(v, attr):
        # Filters arrive as either FilterSpec objects (attribute access) OR
        # plain dicts (tests, internal callers). ``getattr(dict, "values", ...)``
        # returns the bound dict ``.values`` method, NOT the "values" key —
        # that bug raised ``TypeError: 'builtin_function_or_method' object
        # is not iterable`` the moment a non-Pydantic filter reached this
        # cache-key serializer.
        return v.get(attr) if isinstance(v, dict) else getattr(v, attr, None)

    serialised_filters = {
        k: (_filter_attr(v, "mode"), sorted(str(x) for x in (_filter_attr(v, "values") or [])))
        for k, v in sorted((filters or {}).items())
    }
    payload = json.dumps(
        {
            "s": _bucket_time_to_minute(start_time),
            "e": _bucket_time_to_minute(end_time),
            "f": serialised_filters,
            "metric": metric,
            "bs": bucket_seconds,
            "tn": top_n,
            "ma": map_asn,
        },
        separators=(",", ":"),
        default=str,
    )
    svc = src.get("name") or src.get("service_id") or ""
    return hashlib.sha256(f"{payload}:{svc}".encode()).hexdigest()


def _response_cache_get(key: str) -> dict | None:
    cached = _response_cache.get(key)
    if cached is None:
        return None
    result = cached.copy()
    result["is_cached"] = True
    return result


def _response_cache_put(key: str, value: dict) -> None:
    sanitised = {
        k: v
        for k, v in value.items()
        if k not in ("debug_queries", "debug_calls", "is_cached", "_is_cached", "section_timings")
    }
    _response_cache[key] = sanitised


def _avg_hs(buckets_data: dict, keys: list[str]) -> float | None:
    """Average health_score over a set of bucket keys, or None if no data."""
    scores = [
        buckets_data[b].get("health_score")
        for b in keys
        if b in buckets_data and buckets_data[b].get("health_score") is not None
    ]
    return round(sum(scores) / len(scores), 1) if scores else None


def _health_score(
    throughput_bps: float | None,
    rtt_congestion_us: float | None,
    avg_ploss: float | None,
    rtt_jitter_us: float | None,
    error_pct: float | None,
) -> float:
    pkt = min((avg_ploss or 0) / 0.05, 1.0) if avg_ploss is not None else 0
    cong = min((rtt_congestion_us or 0) / 200_000, 1.0) if rtt_congestion_us is not None else 0
    jitter = min((rtt_jitter_us or 0) / 100_000, 1.0) if rtt_jitter_us is not None else 0
    err = min((error_pct or 0) / 10.0, 1.0) if error_pct is not None else 0
    weighted = pkt * 0.40 + cong * 0.30 + jitter * 0.20 + err * 0.10
    return round((1.0 - weighted) * 100, 1)


def get_health(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    metric: str = "health_score",
    bucket_seconds: int = 300,
    top_n: int = 30,
    map_asn: str = "all",
) -> dict[str, Any]:
    """Return ASN × time heatmap, world map buckets, metro leaderboard, and ASN leaderboard."""
    import time as _time

    # Per-phase wall-clock timings surface in the response under
    # _section_timings so the perf harness can attribute /api/network-health
    # without ad-hoc instrumentation. Mirrors dashboard.py.
    timer = SectionTimer()
    section_timings = timer.entries

    # Short-TTL response memo (30 s). Cuts the mapAsn toggle / filter
    # tweak / refetch tick cost from the full ~13 s 30d pipeline to
    # ~50 µs. Cache key excludes section_timings + debug envelope so
    # the per-request telemetry stays request-scoped.
    cache_key = _response_cache_key(src, start_time, end_time, filters, metric, bucket_seconds, top_n, map_asn)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        runner = QueryRunner(con, src)
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)

    if not {"tcp_rtt", "asn"}.issubset(actual_cols):
        return {
            "available": False,
            "reason": "Enable Groups F and G (Network Quality) in your log field configuration.",
            **runner.telemetry(),
        }

    has_ploss = "ploss" in actual_cols
    has_rtt_min = "rtt_min" in actual_cols
    has_rtt_var = "rtt_var" in actual_cols
    has_country = "country" in actual_cols
    has_lat = "lat" in actual_cols and "lon" in actual_cols
    has_c_speed = "c_speed" in actual_cols
    has_metro = "metro" in actual_cols

    # If filtering by a specific ASN for the map, remove the asn column filter so it doesn't conflict
    effective_filters = dict(filters)
    if map_asn != "all" and "asn" in effective_filters:
        del effective_filters["asn"]

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time, end_time, effective_filters, list(actual_cols), inline_params=True
    )
    timer.mark("build_where_clause", _t)

    # Drop ``dt`` and ``resp_state`` from the temp projection — neither is
    # read by any downstream SQL template in backend/repositories/_sql/network.py
    # (verified via grep). Materialising them on every 30d window was 5-15%
    # of the temp-table create cost.
    all_net_cols = [
        "timestamp",
        "asn",
        "country",
        "city",
        "region",
        "lat",
        "lon",
        "metro",
        "tcp_rtt",
        "rtt_min",
        "rtt_var",
        "ploss",
        "status",
        "cache",
        "elapsed",
        "resp_bytes",
        "c_speed",
    ]
    _t = _time.perf_counter()
    temp_table = runner.create_filtered_temp_table(all_net_cols, list(actual_cols), table_name, where_clause, params)
    timer.mark("temp_table_create", _t)
    if temp_table is None:
        return {
            "available": False,
            "reason": "Data temporarily unavailable — view refresh failed. Retry in a moment.",
            **runner.telemetry(),
        }

    # All further queries hit the temp table
    t = temp_table
    w = "1=1"
    p: list[Any] = []

    bucket_ms = bucket_seconds * 1000

    ploss_expr = "AVG(ploss)" if has_ploss else "NULL"
    rtt_min_expr = "MEDIAN(rtt_min)" if has_rtt_min else "NULL"
    rtt_var_expr = "MEDIAN(rtt_var)" if has_rtt_var else "NULL"
    congestion_expr = "MEDIAN(COALESCE(tcp_rtt, 0) - COALESCE(rtt_min, 0))" if has_rtt_min else "NULL"

    try:
        # ── Countries list ─────────────────────────────────────────────────
        countries: list[str] = []
        if has_country:
            rows = runner.execute(
                f"SELECT DISTINCT country FROM {t} WHERE {w} AND country IS NOT NULL AND country != '' ORDER BY country",
                p,
            ).fetchall()
            countries = [r[0] for r in rows]

        # ── Heatmap (ASN × bucket) ─────────────────────────────────────────
        heatmap_sql = SQL.HEATMAP_BY_ASN_BUCKET.format(
            bucket_ms=bucket_ms,
            rtt_min_expr=rtt_min_expr,
            congestion_expr=congestion_expr,
            ploss_expr=ploss_expr,
            rtt_var_expr=rtt_var_expr,
            table=t,
            where=w,
            row_limit=top_n * 200,
        )
        _t = _time.perf_counter()
        heatmap_rows = runner.execute(heatmap_sql, p).fetchall()
        timer.mark("heatmap_query", _t)

        # ── Map (country × bucket) ─────────────────────────────────────────
        map_rows: list[Any] = []
        if has_country:
            lat_col = "lat" if has_lat else "NULL"
            lon_col = "lon" if has_lat else "NULL"
            metro_col = "metro" if has_metro else "NULL"
            city_col = "city" if "city" in actual_cols else "''"

            map_where = w
            map_params = list(p)
            if map_asn != "all":
                map_where += " AND asn = ?"
                map_params.append(int(map_asn))

            # Cap to top 5000 (country, city, bucket) cells by request
            # volume — the map UI renders dots, and the long tail beyond a
            # few thousand points is invisible. Without the cap the
            # response body grew to 5.8MB on busy windows, dominating
            # /network cold-load wall time via transfer + JSON parse.
            # Re-sorted by (bucket, reqs DESC) after the cap to preserve
            # the downstream chronological ordering the map expects.
            map_sql = SQL.MAP_BY_COUNTRY_BUCKET.format(
                city_col=city_col,
                lat_col=lat_col,
                lon_col=lon_col,
                metro_col=metro_col,
                bucket_ms=bucket_ms,
                ploss_expr=ploss_expr,
                table=t,
                where=map_where,
            )
            _t = _time.perf_counter()
            map_rows = runner.execute(map_sql, map_params).fetchall()
            timer.mark("map_query", _t)

        # ── Metro leaderboard ──────────────────────────────────────────────
        metro_rows: list[Any] = []
        if has_country:
            metro_col_m = "metro" if has_metro else "NULL"
            city_col = "city" if "city" in actual_cols else "''"
            region_col = "region" if "region" in actual_cols else "''"
            metro_sql = SQL.METRO_LEADERBOARD.format(
                city_col=city_col,
                region_col=region_col,
                metro_col=metro_col_m,
                ploss_expr=ploss_expr,
                table=t,
                where=w,
            )
            _t = _time.perf_counter()
            metro_rows = runner.execute(metro_sql, p).fetchall()
            timer.mark("metro_query", _t)

        # ── Derive top ASNs ────────────────────────────────────────────────
        all_asns_seen: dict[int, int] = {}
        all_buckets_set: set[str] = set()
        for r in heatmap_rows:
            asn = int(r[0])
            bucket = r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1])
            reqs = int(r[9])
            all_asns_seen[asn] = all_asns_seen.get(asn, 0) + reqs
            all_buckets_set.add(bucket)

        all_buckets = sorted(all_buckets_set)
        bucket_idx = {b: i for i, b in enumerate(all_buckets)}
        top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
        top_asn_set = set(top_asns)

        # ── Speed distribution (bulk, one query) ──────────────────────────
        asn_speed_mix: dict[int, dict[str, float]] = {}
        if has_c_speed and top_asns:
            placeholders = ",".join(["?"] * len(top_asns))
            _t = _time.perf_counter()
            speed_rows = runner.execute(
                SQL.SPEED_DISTRIBUTION_BY_ASN.format(
                    table=t,
                    where=w,
                    placeholders=placeholders,
                ),
                p + top_asns,
            ).fetchall()
            timer.mark("speed_distribution_query", _t)
            asn_speed_rows: dict[int, list[tuple]] = {}
            for r in speed_rows:
                asn_v = int(r[0])
                if asn_v not in asn_speed_rows:
                    asn_speed_rows[asn_v] = []
                if len(asn_speed_rows[asn_v]) < 5:
                    asn_speed_rows[asn_v].append((r[1], r[2]))
            for asn_v, rows in asn_speed_rows.items():
                total = sum(cnt for _, cnt in rows)
                if total > 0:
                    asn_speed_mix[asn_v] = {cs: round(cnt / total, 3) for cs, cnt in rows}

        asn_names_map = _db.get_asn_names(src["name"], top_asns)

        # ── Build heatmap entries ──────────────────────────────────────────
        asn_bucket_data: dict[int, dict[str, dict]] = {}
        for r in heatmap_rows:
            asn = int(r[0])
            if asn not in top_asn_set:
                continue
            bucket = r[1].isoformat() if hasattr(r[1], "isoformat") else str(r[1])
            tp = float(r[2]) if r[2] is not None else None
            rtt = float(r[3]) if r[3] is not None else None
            rtt_base = float(r[4]) if r[4] is not None else None
            rtt_cong = float(r[5]) if r[5] is not None else None
            pkt = float(r[6]) if r[6] is not None else None
            jitter = float(r[7]) if r[7] is not None else None
            err = float(r[8]) if r[8] is not None else None
            reqs = int(r[9])
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
            name = asn_names_map.get(asn) or f"AS{asn}"
            label = _db.format_asn_label(asn, name)
            bucket_list = list(asn_bucket_data.get(asn, {}).values())
            heatmap.append(
                {
                    "asn": asn,
                    "label": label,
                    "total_reqs": all_asns_seen.get(asn, 0),
                    "buckets": bucket_list,
                }
            )

        # ── World map buckets ──────────────────────────────────────────────
        map_buckets: list[dict] = []
        if map_rows:
            dma_map = _db._get_dma_map() if has_metro else {}
            map_by_bucket: dict[str, list[dict]] = {}
            for r in map_rows:
                ctry = r[0]
                city = r[1] or ""
                lat = float(r[2]) if r[2] is not None else None
                lon = float(r[3]) if r[3] is not None else None
                metro_raw = r[4]
                bucket = r[5].isoformat() if hasattr(r[5], "isoformat") else str(r[5])
                rtt = float(r[6]) if r[6] is not None else None
                pkt = float(r[7]) if r[7] is not None else None
                err = float(r[8]) if r[8] is not None else None
                reqs = int(r[9])
                hs = _health_score(None, None, pkt, None, err)

                metro_code: int | None = None
                if metro_raw is not None:
                    try:
                        metro_code = int(float(metro_raw))
                    except (ValueError, TypeError):
                        pass

                display_city = city.title() if city else ""
                if metro_code is not None and str(metro_code) in dma_map:
                    display_city = dma_map[str(metro_code)]

                map_by_bucket.setdefault(bucket, []).append(
                    {
                        "country": ctry,
                        "city": display_city,
                        "lat": lat,
                        "lon": lon,
                        "metro_code": metro_code,
                        "rtt_med_us": rtt,
                        "avg_ploss": pkt,
                        "error_pct": round(err, 2) if err is not None else None,
                        "health_score": hs,
                        "reqs": reqs,
                    }
                )

            for i, bucket in enumerate(all_buckets):
                map_buckets.append(
                    {
                        "bucket_idx": i,
                        "bucket": bucket,
                        "cities": map_by_bucket.get(bucket, []),
                    }
                )

        # ── Metro leaderboard ──────────────────────────────────────────────
        metro_leaderboard: list[dict] = []
        if metro_rows:
            dma_map = _db._get_dma_map() if has_metro else {}
            for r in metro_rows:
                ctry = r[0]
                city = r[1] or ""
                region = r[2] or ""
                metro_raw = r[3]
                pkt = float(r[5]) if r[5] is not None else None
                err = float(r[6]) if r[6] is not None else None
                hs = _health_score(None, None, pkt, None, err)

                metro_code = None
                if metro_raw is not None:
                    try:
                        metro_code = int(float(metro_raw))
                    except (ValueError, TypeError):
                        pass

                if metro_code is not None and str(metro_code) in dma_map:
                    city_name = dma_map[str(metro_code)]
                    display = format_city_label(city_name, ctry, region)
                else:
                    display = format_city_label(city, ctry, region)

                metro_leaderboard.append(
                    {
                        "country": ctry,
                        "region": region,
                        "raw_city": city,
                        "city": display,
                        "total_reqs": int(r[7]),
                        "health_score": hs,
                    }
                )

            # Deduplicate by display name (same city can appear with and without metro code)
            seen: dict[str, dict] = {}
            for entry in metro_leaderboard:
                key = entry["city"]
                if key in seen:
                    prev = seen[key]
                    prev_reqs = prev["total_reqs"]
                    new_reqs = entry["total_reqs"]
                    total = prev_reqs + new_reqs
                    if prev["health_score"] is not None and entry["health_score"] is not None:
                        prev["health_score"] = round(
                            (prev["health_score"] * prev_reqs + entry["health_score"] * new_reqs) / total, 1
                        )
                    prev["total_reqs"] = total
                else:
                    seen[key] = dict(entry)
            metro_leaderboard = sorted(seen.values(), key=lambda x: x["total_reqs"], reverse=True)

        # ── P95/P99 RTT per ASN (bulk) ─────────────────────────────────────
        asn_rtt_pct: dict[int, dict[str, float | None]] = {}
        if top_asns:
            placeholders = ",".join(["?"] * len(top_asns))
            pct_rows = runner.execute(
                SQL.RTT_PERCENTILES_BY_ASN.format(
                    table=t,
                    where=w,
                    placeholders=placeholders,
                ),
                p + top_asns,
            ).fetchall()
            for row in pct_rows:
                asn_v = int(row[0])
                asn_rtt_pct[asn_v] = {
                    "p95_rtt_us": round(float(row[1]), 0) if row[1] is not None else None,
                    "p99_rtt_us": round(float(row[2]), 0) if row[2] is not None else None,
                }

        # ── ASN leaderboard ────────────────────────────────────────────────
        leaderboard: list[dict] = []
        for asn in top_asns:
            name = asn_names_map.get(asn) or f"AS{asn}"
            label = _db.format_asn_label(asn, name)
            buckets_data = asn_bucket_data.get(asn, {})
            if not buckets_data:
                continue

            latest_bucket = max(buckets_data)
            latest = buckets_data[latest_bucket]
            hs_now = latest.get("health_score")

            hs_1h = hs_now
            hs_1w = hs_now
            if len(all_buckets) > 1:
                n_buckets_1h = max(1, 3600 // bucket_seconds)
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
                    "label": label,
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

        # ── Global summary ─────────────────────────────────────────────────
        all_hs = [le["health_score_now"] for le in leaderboard if le["health_score_now"] is not None]
        global_hs = round(sum(all_hs) / len(all_hs), 1) if all_hs else 0

        all_rtt: list[float] = []
        total_reqs = 0
        for r in heatmap_rows:
            if r[3] is not None:
                all_rtt.append(float(r[3]))
            total_reqs += int(r[9])

        avg_rtt_ms = round(sum(all_rtt) / len(all_rtt) / 1000.0, 1) if all_rtt else 0

        worst_asn = None
        if leaderboard:
            significant = [le for le in leaderboard if le["total_reqs"] > total_reqs * 0.01]
            if significant:
                worst = min(
                    significant, key=lambda le: le["health_score_now"] if le["health_score_now"] is not None else 100
                )
                worst_asn = {"label": worst["label"], "score": worst["health_score_now"]}

        worst_country = None
        if has_country and map_buckets:
            latest_cities = map_buckets[-1]["cities"]
            # M-4: the prior ``reqs > 10`` floor frequently left worst_country
            # blank on low-traffic 24h windows, rendering "Worst Region: --"
            # alongside a populated Worst ASN. Drop to 1 so the panel
            # surfaces something whenever the data has any city signal at
            # all — operators reading "--" assumed the page was broken.
            sig_countries = [c for c in latest_cities if c["reqs"] >= 1]
            if sig_countries:
                wc = min(sig_countries, key=lambda c: c["health_score"] if c["health_score"] is not None else 100)
                label = format_city_label(wc.get("city"), wc["country"])
                worst_country = {"label": label, "score": wc["health_score"]}

        payload = {
            "available": True,
            "metric": metric,
            "bucket_seconds": bucket_seconds,
            "buckets": all_buckets,
            "heatmap": heatmap,
            "map_buckets": map_buckets,
            "leaderboard": leaderboard,
            "metro_leaderboard": metro_leaderboard,
            "summary": {
                "global_health_score": global_hs,
                "avg_rtt_ms": avg_rtt_ms,
                "total_reqs": total_reqs,
                "worst_asn": worst_asn,
                "worst_country": worst_country,
            },
            "countries": countries,
            "has_metro": has_metro,
            "section_timings": section_timings,
            **runner.telemetry(),
        }
        _response_cache_put(cache_key, payload)
        return payload

    finally:
        try:
            runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        except Exception:
            pass


def get_quality(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    region_country: str = "US",
) -> dict[str, Any]:
    """Return TCP RTT metrics aggregated by country, ASN, region, PoP, and a scatter sample."""
    import time as _time

    timer = SectionTimer()
    section_timings = timer.entries

    table_name = _safe_table(src["name"])

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)

    if not actual_cols or "tcp_rtt" not in actual_cols:
        return {
            "available": False,
            "by_country": [],
            "by_asn": [],
            "by_region": [],
            "region_country": region_country,
            "by_pop": [],
            "scatter": [],
            "countries": [],
            **runner.telemetry(),
        }

    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols), inline_params=True)
    rtt_filter = f"{where_clause} AND tcp_rtt IS NOT NULL AND tcp_rtt > 0"

    try:
        runner.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except duckdb.CatalogException:
        return {
            "available": False,
            "by_country": [],
            "by_asn": [],
            "by_region": [],
            "region_country": region_country,
            "by_pop": [],
            "scatter": [],
            "countries": [],
            **runner.telemetry(),
        }

    def run_bar(group_col: str, extra_where: str = "", extra_params: list | None = None) -> list[dict]:
        sql = SQL.QUALITY_BAR_BY_GROUP.format(
            group_col=group_col,
            table=table_name,
            rtt_filter=rtt_filter,
            extra_where=extra_where,
        )
        _t = _time.perf_counter()
        rows = runner.execute(sql, params + (extra_params or [])).fetchall()
        timer.mark(f"quality_bar:{group_col}", _t)
        return [{"label": str(r[0]), "rtt_ms": round(float(r[1]), 2), "reqs": int(r[2])} for r in rows]

    countries_sql = SQL.QUALITY_COUNTRIES_DISTINCT.format(
        table=table_name,
        where_clause=where_clause,
    )
    _t = _time.perf_counter()
    countries = [r[0] for r in runner.execute(countries_sql, params).fetchall()]
    timer.mark("countries_distinct", _t)

    by_country = run_bar("country")
    by_asn = run_bar("asn") if "asn" in actual_cols else []
    by_region = (
        run_bar("region", extra_where=" AND country = ?", extra_params=[region_country])
        if "region" in actual_cols
        else []
    )
    by_pop = run_bar("pop") if "pop" in actual_cols else []

    scatter: list[dict] = []
    if "ttfb" in actual_cols:
        scatter_sql = SQL.QUALITY_SCATTER.format(
            table=table_name,
            rtt_filter=rtt_filter,
        )
        _t = _time.perf_counter()
        scatter = [
            {"rtt_ms": round(float(r[0]), 2), "ttfb_ms": round(float(r[1]), 2), "cache": str(r[2])}
            for r in runner.execute(scatter_sql, params).fetchall()
        ]
        timer.mark("scatter_query", _t)

    return {
        "available": True,
        "by_country": by_country,
        "by_asn": by_asn,
        "by_region": by_region,
        "region_country": region_country,
        "by_pop": by_pop,
        "scatter": scatter,
        "countries": countries,
        "section_timings": section_timings,
        **runner.telemetry(),
    }
