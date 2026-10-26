"""Network repository — health heatmap, world map, quality metrics."""

from __future__ import annotations

import logging
import sys as _sys
import time as _time
from collections.abc import Callable
from datetime import UTC
from datetime import datetime as _datetime
from typing import Any

import duckdb

from backend.core.metric_expressions import coalesce as _sql_coalesce
from backend.core.metric_expressions import sum_or_zero

logger = logging.getLogger(__name__)

from backend.core import duckdb as _db
from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _drop_temp_table, _safe_table
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    coalesce,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.countries import format_city_label

# ── SQL templates (folded from backend/repositories/_sql/network.py) ─────────

HEATMAP_BY_ASN_BUCKET = """
            WITH precomputed AS (
                SELECT
                    *,
                    EPOCH_MS(
                        CAST((EPOCH_MS(timestamp)::BIGINT // {bucket_ms}) * {bucket_ms} AS BIGINT)
                    )::TIMESTAMP AS bucket
                FROM {table}
                WHERE {where}
                  AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
            ),
            top_cells AS (
                SELECT
                    asn,
                    bucket,
                    COUNT(*) AS reqs,
                    SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS err_count
                FROM precomputed
                WHERE {where}
                  AND asn IS NOT NULL
                GROUP BY 1, 2
                ORDER BY reqs DESC
                LIMIT {row_limit}
            )
            SELECT
                tc.asn,
                tc.bucket,
                APPROX_QUANTILE(
                    CASE WHEN cache LIKE '%HIT%' AND elapsed > 0
                    THEN resp_bytes * 1000000.0 / elapsed END,
                    0.5
                ) AS throughput_bps,
                APPROX_QUANTILE(tcp_rtt, 0.5)          AS rtt_med_us,
                {rtt_min_expr}           AS rtt_baseline_us,
                {congestion_expr}        AS rtt_congestion_us,
                {ploss_expr}             AS avg_ploss,
                {rtt_var_expr}           AS rtt_jitter_us,
                tc.err_count * 100.0 / NULLIF(tc.reqs, 0) AS error_pct,
                tc.reqs
            FROM precomputed AS {table}
            INNER JOIN top_cells tc
                ON {table}.asn = tc.asn
               AND {table}.bucket = tc.bucket
            GROUP BY tc.asn, tc.bucket, tc.reqs, tc.err_count
            ORDER BY tc.reqs DESC
        """

MAP_BY_COUNTRY_BUCKET = """
                WITH precomputed AS (
                    SELECT
                        *,
                        {city_col} AS city,
                        {lat_col}  AS lat,
                        {lon_col}  AS lon,
                        {metro_col} AS metro,
                        EPOCH_MS(
                            CAST((EPOCH_MS(timestamp)::BIGINT // {bucket_ms}) * {bucket_ms} AS BIGINT)
                        )::TIMESTAMP AS bucket
                    FROM {table}
                    WHERE {where}
                      AND country IS NOT NULL AND country != ''
                      AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                ),
                top_cells AS (
                    SELECT
                        country,
                        city,
                        lat,
                        lon,
                        metro,
                        bucket,
                        COUNT(*) AS reqs,
                        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS err_count
                    FROM precomputed
                    WHERE {where}
                    GROUP BY 1, 2, 3, 4, 5, 6
                    ORDER BY reqs DESC
                    LIMIT 5000
                )
                SELECT * FROM (
                    SELECT
                        tc.country,
                        tc.city,
                        tc.lat,
                        tc.lon,
                        tc.metro,
                        tc.bucket,
                        APPROX_QUANTILE({table}.tcp_rtt, 0.5) AS rtt_med_us,
                        {ploss_expr}    AS avg_ploss,
                        tc.err_count * 100.0 / NULLIF(tc.reqs, 0) AS error_pct,
                        tc.reqs
                    FROM precomputed AS {table}
                    INNER JOIN top_cells tc
                        ON {table}.country = tc.country
                       AND ({join_city_col})  IS NOT DISTINCT FROM tc.city
                       AND ({join_lat_col})   IS NOT DISTINCT FROM tc.lat
                       AND ({join_lon_col})   IS NOT DISTINCT FROM tc.lon
                       AND ({join_metro_col}) IS NOT DISTINCT FROM tc.metro
                       AND {table}.bucket = tc.bucket
                    WHERE 1=1
                      AND {table}.tcp_rtt IS NOT NULL AND {table}.tcp_rtt > 0
                    GROUP BY tc.country, tc.city, tc.lat, tc.lon, tc.metro, tc.bucket, tc.reqs, tc.err_count
                ) ranked
                ORDER BY bucket, reqs DESC
            """

METRO_LEADERBOARD = """
                WITH top_cells AS (
                    SELECT
                        country,
                        {city_col}   AS city,
                        {region_col} AS region,
                        {metro_col}  AS metro,
                        COUNT(*) AS reqs,
                        SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS err_count
                    FROM {table}
                    WHERE {where}
                      AND country IS NOT NULL AND country != ''
                      AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                    GROUP BY 1, 2, 3, 4
                    ORDER BY reqs DESC
                    LIMIT 100
                )
                SELECT
                    tc.country,
                    tc.city,
                    tc.region,
                    tc.metro,
                    APPROX_QUANTILE(tcp_rtt, 0.5) AS rtt_med_us,
                    {ploss_expr} AS avg_ploss,
                    tc.err_count * 100.0 / NULLIF(tc.reqs, 0) AS error_pct,
                    tc.reqs
                FROM {table}
                INNER JOIN top_cells tc
                    ON {table}.country = tc.country
                   AND ({join_city_col})   IS NOT DISTINCT FROM tc.city
                   AND ({join_region_col}) IS NOT DISTINCT FROM tc.region
                   AND ({join_metro_col})  IS NOT DISTINCT FROM tc.metro
                WHERE {where}
                  AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                GROUP BY tc.country, tc.city, tc.region, tc.metro, tc.reqs, tc.err_count
                ORDER BY tc.reqs DESC
            """

SPEED_DISTRIBUTION_BY_ASN = """
                SELECT asn, c_speed, COUNT(*) AS cnt FROM {table}
                WHERE {where} AND asn IN ({placeholders})
                  AND c_speed IS NOT NULL AND c_speed != ''
                GROUP BY asn, c_speed
                ORDER BY asn, cnt DESC
                """

RTT_PERCENTILES_BY_ASN = """
                SELECT asn,
                    APPROX_QUANTILE(tcp_rtt, 0.95) AS p95_us,
                    APPROX_QUANTILE(tcp_rtt, 0.99) AS p99_us
                FROM {table}
                WHERE {where} AND asn IN ({placeholders})
                  AND tcp_rtt IS NOT NULL AND tcp_rtt > 0
                GROUP BY asn
                """

QUALITY_BAR_BY_GROUP = """
            SELECT "{group_col}" AS label, APPROX_QUANTILE(tcp_rtt, 0.5) / 1000.0 AS rtt_ms, COUNT(*) AS reqs
            FROM {table}
            WHERE {rtt_filter}{extra_where}
              AND "{group_col}" IS NOT NULL AND CAST("{group_col}" AS VARCHAR) != ''
            GROUP BY "{group_col}"
            ORDER BY reqs DESC
            LIMIT 25
        """

QUALITY_COUNTRIES_DISTINCT = """
        SELECT DISTINCT country FROM {table}
        WHERE {where_clause} AND country IS NOT NULL AND country != ''
        ORDER BY country
    """

QUALITY_SCATTER = f"""
            SELECT tcp_rtt / 1000.0 AS rtt_ms, ttfb * 1000.0 AS ttfb_ms,
                   {_sql_coalesce("cache", "'UNKNOWN'")} AS cache_state
            FROM {{table}}
            WHERE {{rtt_filter}} AND ttfb IS NOT NULL AND ttfb > 0
            USING SAMPLE 2000
        """

POP_HEALTH_LIVE = """
            SELECT
                pop,
                COUNT(*) AS requests,
                COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS errors,
                ROUND(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) * 100.0 / COUNT(*), 2) AS error_rate,
                approx_quantile(tcp_rtt, 0.5) AS p50_rtt_us,
                approx_quantile(ttfb, 0.95) * 1000.0 AS p95_ttfb_ms,
                ROUND(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) * 100.0 / COUNT(*), 2) AS cache_hit_rate,
                SUM(resp_bytes) AS bandwidth_bytes
            FROM {table_name}
            WHERE timestamp >= ? AND timestamp <= ? AND pop IS NOT NULL AND pop != ''
            GROUP BY pop
            ORDER BY requests DESC
        """

SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.network", _sys.modules[__name__])


class _TempTableUnavailable(Exception):
    """Raised inside the single-flight builder when the network temp-table
    build fails, so every waiter (leader and followers alike) takes the same
    "available: False" branch instead of only the leader seeing it."""


# ── Response memo cache ───────────────────────────────────────────────────────
_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128
_memo = ResponseMemo(ttl=_RESPONSE_CACHE_TTL, maxsize=_RESPONSE_CACHE_MAXSIZE, prewarmer=False)
_response_cache = _memo.cache


def _response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    metric: str,
    bucket_seconds: int,
    top_n: int,
    map_asn: str,
    sections: set[str] | None,
    mask_ips: bool,
    range_token: str | None = None,
    quantized_anchor: str | None = None,
    invite_clamp_fingerprint: str | None = None,
) -> str:
    """Build tenant/auth-partitioned cache key (stable relative-range when range_token is present, else anchor-faithful)."""
    common_tail = {
        "f": serialize_filters_for_key(filters),
        "metric": metric,
        "bs": bucket_seconds,
        "tn": top_n,
        "ma": map_asn,
        "sec": sorted(sections) if sections is not None else None,
        "mi": int(mask_ips),
    }
    if range_token is not None:
        stable_payload = {
            "k": "rel",
            "rt": range_token,
            "qa": quantized_anchor,
            "icf": invite_clamp_fingerprint,
            **common_tail,
        }
        return digest_cache_key(stable_payload, src)
    payload = {
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        **common_tail,
    }
    return digest_cache_key(payload, src)


def _has_signal(payload: dict[str, Any]) -> bool:
    """True when a get_health payload carries real data worth caching (guards against caching transient empty views)."""
    summary = payload.get("summary")
    if isinstance(summary, dict) and summary.get("total_reqs"):
        return True
    return any(payload.get(k) for k in ("heatmap", "map_buckets", "leaderboard", "metro_leaderboard", "cities"))


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


def _rtt_congestion_expr() -> str:
    """Subtract unsigned RTT columns without allowing UINT underflow."""
    return "APPROX_QUANTILE(CAST(COALESCE(tcp_rtt, 0) AS BIGINT) - CAST(COALESCE(rtt_min, 0) AS BIGINT), 0.5)"


def _bucket_str(val: Any) -> str:
    return val.isoformat().replace("+00:00", "") if hasattr(val, "isoformat") else str(val)


def _opt_float(val: Any, ndigits: int | None = None) -> float | None:
    if val is None:
        return None
    f = float(val)
    return round(f, ndigits) if ndigits is not None else f


def _get_schema_cols_cached(src: dict, table_name: str, get_runner: Callable[[], QueryRunner]) -> set[str]:
    cached_schema = _db._schema_cache.get((src["name"], table_name, True))
    if cached_schema is not None:
        return {c["name"] for c in cached_schema}
    return set(get_runner().get_schema_cols())


def _lazy_runner(con_factory: Any, src: dict) -> tuple[Callable[[], QueryRunner], Callable[[], dict[str, Any]]]:
    runner: QueryRunner | None = None

    def get_runner() -> QueryRunner:
        nonlocal runner
        if runner is None:
            runner = QueryRunner(con_factory, src)
        return runner

    def telemetry() -> dict[str, Any]:
        return runner.telemetry() if runner is not None else {"telemetry": {}}

    return get_runner, telemetry


def get_health(
    con_factory: Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    metric: str = "health_score",
    bucket_seconds: int = 300,
    top_n: int = 30,
    map_asn: str = "all",
    sections: set[str] | None = None,
    mask_ips: bool = False,
    force_refresh: bool = False,
    range_token: str | None = None,
    quantized_anchor: str | None = None,
    invite_clamp_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Return ASN × time heatmap, world map buckets, metro leaderboard, and ASN leaderboard."""

    def _want(name: str) -> bool:
        return sections is None or name in sections

    _want_heatmap_query = _want("heatmap") or _want("leaderboard") or _want("summary") or _want("buckets")
    _want_map_query = _want("cities") or _want("map_buckets") or _want("summary")
    _want_metro_query = _want("metro_leaderboard")
    _want_leaderboard = _want("leaderboard")

    timer = SectionTimer()
    section_timings = timer.entries

    cache_key = _response_cache_key(
        src,
        start_time,
        end_time,
        filters,
        metric,
        bucket_seconds,
        top_n,
        map_asn,
        sections,
        mask_ips,
        range_token=range_token,
        quantized_anchor=quantized_anchor,
        invite_clamp_fingerprint=invite_clamp_fingerprint,
    )
    if not force_refresh:
        cached = _memo.get(cache_key)
        if cached is not None:
            return {**cached, "telemetry": {}}

    observation = _memo.result({}, state="bypass" if force_refresh else "miss")
    table_name = _safe_table(src["name"])
    get_runner, _telemetry = _lazy_runner(con_factory, src)

    def _unavailable(reason: str) -> dict[str, Any]:
        return {"available": False, "reason": reason, **observation, **_telemetry()}

    _t = _time.perf_counter()
    actual_cols = _get_schema_cols_cached(src, table_name, get_runner)
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)

    if not {"tcp_rtt", "asn"}.issubset(actual_cols):
        return _unavailable("Enable Groups F and G (Network Quality) in your log field configuration.")

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
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)

    # Floor bucket width to at most 8640 buckets across the window.
    _st0 = None
    _et0 = None
    try:
        from backend.utils.date_utils import parse_iso_utc as _parse_iso_utc

        if start_time and end_time:
            _st0 = _parse_iso_utc(start_time)
            _et0 = _parse_iso_utc(end_time)
            if _st0 is not None and _et0 is not None:
                span_secs = max(1, int((_et0 - _st0).total_seconds()))
                bucket_seconds = max(bucket_seconds, span_secs // 8640)
    except Exception:
        logger.debug("get_health: step failed (ignored)", exc_info=True)

    bucket_ms = bucket_seconds * 1000

    ploss_expr = "AVG(ploss)" if has_ploss else "NULL"
    rtt_min_expr = "APPROX_QUANTILE(rtt_min, 0.5)" if has_rtt_min else "NULL"
    rtt_var_expr = "APPROX_QUANTILE(rtt_var, 0.5)" if has_rtt_var else "NULL"
    congestion_expr = _rtt_congestion_expr() if has_rtt_min else "NULL"

    # ── Rollup fast path ────────────────────────────────────────────────────
    _net_rolled: dict[str, Any] = {}
    _net_missed: set[str] = set()

    def _hoist_net(key: str, want: bool, thunk: Any) -> None:
        if not want:
            return
        _t0 = _time.perf_counter()
        result = thunk()
        timer.mark(
            f"{key}_rollup",
            _t0,
            source="rollup",
            activity="compute" if result is not None else "overhead",
            current_execution=True,
            outcome="served" if result is not None else "fallback",
        )
        if result is not None:
            _net_rolled[key] = result
        else:
            _net_missed.add(key)

    _hoist_net(
        "heatmap",
        _want_heatmap_query,
        lambda: get_runner().try_network_heatmap_from_rollup(
            start_time, end_time, has_filters=bool(filters), bucket_seconds=bucket_seconds
        ),
    )
    _hoist_net(
        "map_geo",
        _want_map_query or _want_metro_query,
        lambda: get_runner().try_network_geo_from_rollup(
            start_time, end_time, map_asn=map_asn, has_filters=bool(filters), bucket_seconds=bucket_seconds
        ),
    )

    if not _net_missed:
        section_timings.append(
            {"section": "network:temp_skipped", "time_ms": 0.0, "activity": "skipped", "current_execution": True}
        )
        heatmap_rows: list[Any] = _net_rolled.get("heatmap") or []
        _geo = _net_rolled.get("map_geo")
        map_rows: list[Any] = _geo[0] if _geo else []
        metro_rows: list[Any] = _geo[1] if _geo else []
        countries: list[str] = sorted({str(r[0]) for r in map_rows if r[0]}) if has_country else []
        temp_table = None
        t = None
        w = "1=1"
        p: list[Any] = []
    else:
        # ── Temp table path (single-flight coalesced across parallel core/map requests) ──
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
        temp_key = digest_cache_key(
            {
                "table": table_name,
                "where": where_clause,
                "params": [str(x) for x in params],
                "cols": sorted(actual_cols),
                "bucket_ms": bucket_ms,
                "map_asn": map_asn,
                "top_n": top_n,
            },
            src,
        )
        _leader_temp_name: list[str | None] = [None]

        def _build_temp_results() -> tuple[list[str], list[Any], list[Any], list[Any]]:
            _t0 = _time.perf_counter()
            temp_name = get_runner().create_filtered_temp_table(
                all_net_cols, list(actual_cols), table_name, where_clause, params
            )
            timer.mark("temp_table_create", _t0, source="live", activity="compute", current_execution=True)
            if temp_name is None:
                raise _TempTableUnavailable()
            _leader_temp_name[0] = temp_name
            countries_b: list[str] = []
            if has_country:
                rows = (
                    get_runner()
                    .execute(
                        f"SELECT DISTINCT country FROM {temp_name} WHERE 1=1"
                        f" AND country IS NOT NULL AND country != '' ORDER BY country",
                        [],
                    )
                    .fetchall()
                )
                countries_b = [r[0] for r in rows]

            heatmap_rows_b: list[Any] = []
            if _want_heatmap_query:
                if "heatmap" in _net_rolled:
                    heatmap_rows_b = _net_rolled["heatmap"]
                else:
                    heatmap_sql = SQL.HEATMAP_BY_ASN_BUCKET.format(
                        bucket_ms=bucket_ms,
                        rtt_min_expr=rtt_min_expr,
                        congestion_expr=congestion_expr,
                        ploss_expr=ploss_expr,
                        rtt_var_expr=rtt_var_expr,
                        table=temp_name,
                        where="1=1",
                        row_limit=top_n * 200,
                    )
                    _t1 = _time.perf_counter()
                    heatmap_rows_b = get_runner().execute(heatmap_sql, []).fetchall()
                    timer.mark("heatmap_query", _t1, source="live", activity="compute", current_execution=True)

            map_rows_b: list[Any] = []
            metro_rows_b: list[Any] = []
            if "map_geo" in _net_rolled and map_asn == "all":
                _geo = _net_rolled["map_geo"]
                map_rows_b = _geo[0] if _geo else []
                metro_rows_b = _geo[1] if _geo else []
            else:

                def _qual_col(col: str, present: bool, default: str) -> tuple[str, str]:
                    return (col, f"{temp_name}.{col}") if present else (default, default)

                if _want_map_query and has_country:
                    lat_col, join_lat_col = _qual_col("lat", has_lat, "NULL")
                    lon_col, join_lon_col = _qual_col("lon", has_lat, "NULL")
                    metro_col, join_metro_col = _qual_col("metro", has_metro, "NULL")
                    city_col, join_city_col = _qual_col("city", "city" in actual_cols, "''")

                    map_where = "1=1"
                    map_params: list[Any] = []
                    if map_asn != "all":
                        map_where += " AND asn = ?"
                        map_params.append(int(map_asn))

                    map_sql = SQL.MAP_BY_COUNTRY_BUCKET.format(
                        city_col=city_col,
                        lat_col=lat_col,
                        lon_col=lon_col,
                        metro_col=metro_col,
                        join_city_col=join_city_col,
                        join_lat_col=join_lat_col,
                        join_lon_col=join_lon_col,
                        join_metro_col=join_metro_col,
                        bucket_ms=bucket_ms,
                        ploss_expr=ploss_expr,
                        table=temp_name,
                        where=map_where,
                    )
                    _t2 = _time.perf_counter()
                    map_rows_b = get_runner().execute(map_sql, map_params + map_params).fetchall()
                    timer.mark("map_query", _t2, source="live", activity="compute", current_execution=True)

                if has_country:
                    metro_col_m, join_metro_col = _qual_col("metro", has_metro, "NULL")
                    city_col, join_city_col = _qual_col("city", "city" in actual_cols, "''")
                    region_col, join_region_col = _qual_col("region", "region" in actual_cols, "''")
                    metro_sql = SQL.METRO_LEADERBOARD.format(
                        city_col=city_col,
                        region_col=region_col,
                        metro_col=metro_col_m,
                        join_city_col=join_city_col,
                        join_region_col=join_region_col,
                        join_metro_col=join_metro_col,
                        ploss_expr=ploss_expr,
                        table=temp_name,
                        where="1=1",
                    )
                    _t3 = _time.perf_counter()
                    metro_rows_b = get_runner().execute(metro_sql, []).fetchall()
                    timer.mark("metro_query", _t3, source="live", activity="compute", current_execution=True)

            return countries_b, heatmap_rows_b, map_rows_b, metro_rows_b

        try:
            (countries, heatmap_rows, map_rows, metro_rows), is_leader = coalesce(temp_key, _build_temp_results)
        except _TempTableUnavailable:
            return _unavailable("Data temporarily unavailable — view refresh failed. Retry in a moment.")
        if not is_leader:
            section_timings.append(
                {"section": "network:temp_coalesced", "time_ms": 0.0, "activity": "overhead", "current_execution": True}
            )

        temp_table = _leader_temp_name[0]
        t = temp_table
        w = "1=1"
        p = []

    try:
        # ── Derive top ASNs ────────────────────────────────────────────────
        all_asns_seen: dict[int, int] = {}
        for r in heatmap_rows:
            asn = int(r[0])
            reqs = int(r[9])
            all_asns_seen[asn] = all_asns_seen.get(asn, 0) + reqs

        # Generate the complete sequence of buckets from start_time to end_time
        # at bucket_seconds intervals so that there are no gaps in the animation playback.
        all_buckets_list: list[str] = []
        if _st0 is not None and _et0 is not None:
            _st_epoch = int(_st0.timestamp())
            _st_floored = (_st_epoch // bucket_seconds) * bucket_seconds
            _et_epoch = int(_et0.timestamp())
            _et_floored = (_et_epoch // bucket_seconds) * bucket_seconds

            curr = _st_floored
            # Generate up to _et_floored (inclusive of the last bucket within the window)
            while curr <= _et_floored:
                bucket_str = _datetime.fromtimestamp(curr, tz=UTC).replace(tzinfo=None).isoformat()
                all_buckets_list.append(bucket_str)
                curr += bucket_seconds

        if not all_buckets_list:
            # Fallback to sparse buckets present in the query results
            all_buckets_set: set[str] = {_bucket_str(r[1]) for r in heatmap_rows}

            # Selector callers that ask for map_buckets/cities WITHOUT heatmap skip
            # the heatmap query, but the map-bucket assembly still needs the
            # sorted bucket axis for positional bucket_idx; pull bucket times
            # from map_rows in that case so the per-bucket map cells survive.
            if not heatmap_rows and map_rows:
                all_buckets_set.update(_bucket_str(r[5]) for r in map_rows)

            all_buckets = sorted(all_buckets_set)
        else:
            all_buckets = all_buckets_list

        bucket_idx = {b: i for i, b in enumerate(all_buckets)}
        top_asns = sorted(all_asns_seen, key=lambda a: all_asns_seen[a], reverse=True)[:top_n]
        top_asn_set = set(top_asns)

        # ── Speed distribution (bulk, one query) ──────────────────────────
        # Try the per-hour network_speed rollup first for unfiltered
        # windows >= 48 h. Exact integer SUM across hours (no
        # approximation), so the rollup result is byte-identical to the
        # live SQL — just faster (~50 ms vs ~2.9 s on prod 30 d).
        asn_speed_mix: dict[int, dict[str, float]] = {}
        if _want_leaderboard and has_c_speed and top_asns:
            _t = _time.perf_counter()
            rolled_speed = get_runner().try_network_speed_from_rollup(
                start_time,
                end_time,
                top_asns=top_asns,
                has_filters=bool(filters),
            )
            if rolled_speed is not None:
                speed_rows = rolled_speed
                timer.mark(
                    "speed_distribution_query_rollup", _t, source="rollup", activity="compute", current_execution=True
                )
            elif t is not None:
                placeholders = ",".join(["?"] * len(top_asns))
                _t = _time.perf_counter()
                speed_rows = (
                    get_runner()
                    .execute(
                        SQL.SPEED_DISTRIBUTION_BY_ASN.format(
                            table=t,
                            where=w,
                            placeholders=placeholders,
                        ),
                        p + top_asns,
                    )
                    .fetchall()
                )
                timer.mark("speed_distribution_query", _t, source="live", activity="compute", current_execution=True)
            else:
                speed_rows = []  # skip-temp path + speed rollup missed → no fallback
            asn_speed_rows: dict[int, list[tuple]] = {}
            for r in speed_rows:
                rows_for_asn = asn_speed_rows.setdefault(int(r[0]), [])
                if len(rows_for_asn) < 5:
                    rows_for_asn.append((r[1], r[2]))
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
            bucket = _bucket_str(r[1])
            tp, rtt, rtt_base, rtt_cong, pkt, jitter, err = (_opt_float(v) for v in r[2:9])
            reqs = int(r[9])
            hs = _health_score(tp, rtt_cong, pkt, jitter, err)
            asn_bucket_data.setdefault(asn, {})[bucket] = {
                "bucket_idx": bucket_idx[bucket],
                "bucket": bucket,
                "throughput_bps": _opt_float(tp, 0),
                "rtt_med_us": _opt_float(rtt, 0),
                "rtt_baseline_us": _opt_float(rtt_base, 0),
                "rtt_congestion_us": _opt_float(rtt_cong, 0),
                "avg_ploss": _opt_float(pkt, 5),
                "rtt_jitter_us": _opt_float(jitter, 0),
                "error_pct": _opt_float(err, 2),
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

        # ── World map buckets (interned city + lat/lon) ────────────────────
        map_buckets: list[dict] = []
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

        dma_map = _db._get_dma_map() if has_metro and (map_rows or metro_rows) else {}

        def _dma_city(metro_raw: Any, fallback: str) -> tuple[int | None, str]:
            mc = _parse_metro_code(metro_raw)
            return mc, dma_map[str(mc)] if mc is not None and str(mc) in dma_map else fallback

        if map_rows:
            map_by_bucket: dict[str, list[dict]] = {}
            for r in map_rows:
                ctry = r[0]
                city = r[1] or ""
                lat, lon = _opt_float(r[2]), _opt_float(r[3])
                bucket = _bucket_str(r[5])
                rtt, pkt, err = _opt_float(r[6]), _opt_float(r[7]), _opt_float(r[8])
                reqs = int(r[9])
                hs = _health_score(None, None, pkt, None, err)
                metro_code, display_city = _dma_city(r[4], city.title() if city else "")

                map_by_bucket.setdefault(bucket, []).append(
                    {
                        "country": ctry,
                        "city_idx": _intern_city(display_city, lat, lon),
                        "metro_code": metro_code,
                        "rtt_med_us": rtt,
                        "avg_ploss": pkt,
                        "error_pct": _opt_float(err, 2),
                        "health_score": hs,
                        "reqs": reqs,
                    }
                )

            map_buckets = [
                {"bucket_idx": i, "bucket": bucket, "cities": map_by_bucket.get(bucket, [])}
                for i, bucket in enumerate(all_buckets)
            ]

        # ── Metro leaderboard ──────────────────────────────────────────────
        metro_leaderboard: list[dict] = []
        if metro_rows:
            for r in metro_rows:
                ctry = r[0]
                city = r[1] or ""
                region = r[2] or ""
                pkt, err = _opt_float(r[5]), _opt_float(r[6])
                hs = _health_score(None, None, pkt, None, err)
                _, city_for_label = _dma_city(r[3], city)
                display = format_city_label(city_for_label, ctry, region)

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
        # Try the per-hour network_rtt rollup first for unfiltered windows
        # >= 48 h — the rollup serves in ~50 ms on prod 30 d vs the live
        # bulk query's ~5.2 s. The rollup gates on `not filters` so the
        # filtered path always falls through to the live SQL below.
        asn_rtt_pct: dict[int, dict[str, float | None]] = {}
        if _want_leaderboard and top_asns:
            _t = _time.perf_counter()
            rolled = get_runner().try_network_rtt_from_rollup(
                start_time,
                end_time,
                top_asns=top_asns,
                has_filters=bool(filters),
            )
            if rolled is not None:
                asn_rtt_pct = rolled
                timer.mark(
                    "rtt_percentiles_query_rollup", _t, source="rollup", activity="compute", current_execution=True
                )
            elif t is not None:
                placeholders = ",".join(["?"] * len(top_asns))
                _t = _time.perf_counter()
                pct_rows = (
                    get_runner()
                    .execute(
                        SQL.RTT_PERCENTILES_BY_ASN.format(
                            table=t,
                            where=w,
                            placeholders=placeholders,
                        ),
                        p + top_asns,
                    )
                    .fetchall()
                )
                timer.mark("rtt_percentiles_query", _t, source="live", activity="compute", current_execution=True)
                for row in pct_rows:
                    asn_rtt_pct[int(row[0])] = {
                        "p95_rtt_us": _opt_float(row[1], 0),
                        "p99_rtt_us": _opt_float(row[2], 0),
                    }
            # else: skip-temp path + RTT rollup missed → leave asn_rtt_pct empty

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
            for b in reversed(map_buckets):
                cities_in_b = b.get("cities") or []
                sig_countries = [c for c in cities_in_b if c.get("reqs", 0) >= 1]
                if sig_countries:
                    wc = min(sig_countries, key=lambda c: c["health_score"] if c["health_score"] is not None else 100)
                    idx = wc.get("city_idx", -1)
                    city_entry = cities_list[idx] if 0 <= idx < len(cities_list) else None
                    city_name = city_entry["name"] if city_entry else ""
                    label = format_city_label(city_name, wc["country"])
                    worst_country = {"label": label, "score": wc["health_score"]}
                    break

        payload: dict[str, Any] = {
            "available": True,
            "metric": metric,
            "bucket_seconds": bucket_seconds,
            "countries": countries,
            "has_metro": has_metro,
            "section_timings": section_timings,
            **_telemetry(),
        }
        for sec_key, sec_val in (
            ("buckets", all_buckets),
            ("heatmap", heatmap),
            ("map_buckets", map_buckets),
            ("cities", cities_list),
            ("leaderboard", leaderboard),
            ("metro_leaderboard", metro_leaderboard),
            (
                "summary",
                {
                    "global_health_score": global_hs,
                    "avg_rtt_ms": avg_rtt_ms,
                    "total_reqs": total_reqs,
                    "worst_asn": worst_asn,
                    "worst_country": worst_country,
                },
            ),
        ):
            if _want(sec_key):
                payload[sec_key] = sec_val
        if _has_signal(payload):
            _memo.put(cache_key, payload, strip=("section_timings",))
        return {**payload, **observation}

    finally:
        if temp_table is not None:
            _drop_temp_table(get_runner(), temp_table, "get_health")


def _parse_metro_code(metro_raw: Any) -> int | None:
    if metro_raw is None:
        return None
    try:
        return int(float(metro_raw))
    except (ValueError, TypeError):
        return None


def _empty_quality_payload(region_country: str, runner: QueryRunner) -> dict[str, Any]:
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


def get_quality(
    con_factory: Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    region_country: str = "US",
) -> dict[str, Any]:
    """Return TCP RTT metrics aggregated by country, ASN, region, PoP, and a scatter sample."""
    timer = SectionTimer()
    section_timings = timer.entries

    table_name = _safe_table(src["name"])
    get_runner, _ = _lazy_runner(con_factory, src)

    _t = _time.perf_counter()
    rolled = get_runner().try_network_quality_from_rollup(
        start_time, end_time, has_filters=bool(filters), region_country=region_country
    )
    timer.mark(
        "network_quality_rollup",
        _t,
        source="rollup",
        activity="compute" if rolled is not None else "overhead",
        current_execution=True,
        outcome="served" if rolled is not None else "fallback",
    )
    if rolled is not None:
        return {
            **rolled,
            "section_timings": section_timings,
            **get_runner().telemetry(),
        }

    _t = _time.perf_counter()
    actual_cols = _get_schema_cols_cached(src, table_name, get_runner)
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)

    if not actual_cols or "tcp_rtt" not in actual_cols:
        return _empty_quality_payload(region_country, get_runner())

    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols), inline_params=True)

    try:
        get_runner().execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except duckdb.CatalogException:
        return _empty_quality_payload(region_country, get_runner())

    # ── Temp table optimization ──────────────────────────────────────────
    # Build a temp table containing only the required quality columns for
    # matching rows. This completely avoids repeatedly scanning the heavy base
    # table logs_view (which requires read_parquet) for each sub-query.
    quality_cols = [
        "country",
        "asn",
        "region",
        "pop",
        "tcp_rtt",
        "ttfb",
        "cache",
    ]

    _t0 = _time.perf_counter()
    temp_table = get_runner().create_filtered_temp_table(quality_cols, actual_cols, table_name, where_clause, params)
    timer.mark("temp_table_create", _t0, source="live", activity="compute", current_execution=True)

    if temp_table is not None:
        # If temp table successfully created, hit it directly!
        # The temp table already contains ONLY the filtered rows, so we don't
        # need start_time / end_time / other filter parameters in the sub-queries.
        query_table, query_where, query_params = temp_table, "1=1", []
    else:
        # Fallback to the slow direct path on catalog exception or uuid collision (unlikely)
        query_table, query_where, query_params = table_name, where_clause, params

    rtt_filter = f"{query_where} AND tcp_rtt IS NOT NULL AND tcp_rtt > 0"

    try:

        def run_bar(group_col: str, extra_where: str = "", extra_params: list | None = None) -> list[dict]:
            sql = SQL.QUALITY_BAR_BY_GROUP.format(
                group_col=group_col,
                table=query_table,
                rtt_filter=rtt_filter,
                extra_where=extra_where,
            )
            _t = _time.perf_counter()
            rows = get_runner().execute(sql, query_params + (extra_params or [])).fetchall()
            timer.mark(f"quality_bar:{group_col}", _t, source="live", activity="compute", current_execution=True)
            return [
                {"value": str(r[0]), "label": str(r[0]), "rtt_ms": round(float(r[1]), 2), "reqs": int(r[2])}
                for r in rows
            ]

        countries_sql = SQL.QUALITY_COUNTRIES_DISTINCT.format(
            table=query_table,
            where_clause=query_where,
        )
        _t = _time.perf_counter()
        countries = [r[0] for r in get_runner().execute(countries_sql, query_params).fetchall()]
        timer.mark("countries_distinct", _t, source="live", activity="compute", current_execution=True)

        by_country = run_bar("country")
        by_asn = run_bar("asn") if "asn" in actual_cols else []
        if by_asn:
            # Mirror the ASN leaderboard / dashboard: display "Name (7922)" instead
            # of the bare number, keeping `value` as the click-to-filter key.
            try:
                _db.enrich_asn_labels(by_asn, src["name"])
            except Exception:
                logger.debug("get_quality: _db.enrich_asn_labels failed (ignored)", exc_info=True)
        by_region = (
            run_bar("region", extra_where=" AND country = ?", extra_params=[region_country])
            if "region" in actual_cols
            else []
        )
        # by_pop rows stay as the bare PoP code (value == label); the frontend
        # renders the city/region/country via the shared <PopLabel> component
        # (fed by bootstrap's pop_geo map). See frontend/lib/pop.ts.
        by_pop = run_bar("pop") if "pop" in actual_cols else []

        scatter: list[dict] = []
        if "ttfb" in actual_cols:
            scatter_sql = SQL.QUALITY_SCATTER.format(
                table=query_table,
                rtt_filter=rtt_filter,
            )
            _t = _time.perf_counter()
            scatter = [
                {"rtt_ms": round(float(r[0]), 2), "ttfb_ms": round(float(r[1]), 2), "cache": str(r[2])}
                for r in get_runner().execute(scatter_sql, query_params).fetchall()
            ]
            timer.mark("scatter_query", _t, source="live", activity="compute", current_execution=True)

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
            **get_runner().telemetry(),
        }

    finally:
        if temp_table is not None:
            _drop_temp_table(get_runner(), temp_table, "get_quality")


def get_transport_metrics(
    con_factory: Any,
    src: dict[str, Any],
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None = None,
) -> dict[str, Any]:
    timer = SectionTimer()
    table_name = _safe_table(src["name"])
    get_runner, _ = _lazy_runner(con_factory, src)

    actual_cols = set(get_runner().get_schema_cols())
    optional_fields = ["tcp_rtt", "retrans", "ploss", "delivery_rate"]
    missing_fields = [f for f in optional_fields if f not in actual_cols]
    has_transport_data = len(missing_fields) < len(optional_fields)

    default_resp = {
        "has_transport_data": has_transport_data,
        "tcp_rtt_p50_ms": None,
        "tcp_rtt_p90_ms": None,
        "tcp_rtt_p95_ms": None,
        "tcp_rtt_p99_ms": None,
        "retrans_rate_pct": None,
        "packet_loss_pct": None,
        "delivery_rate_mbps": None,
        "bandwidth_bytes": 0,
        "total_requests": 0,
        "protocol_distribution": [],
        "missing_fields": missing_fields,
        "section_timings": timer.entries,
        **get_runner().telemetry(),
    }

    try:
        get_runner().execute(f"SELECT 1 FROM {table_name} LIMIT 1")
    except Exception:
        return default_resp

    params, where_clause = build_where_clause(
        start_time, end_time, filters or {}, list(actual_cols), inline_params=True
    )

    _rtt_quants = (("0.50", "rtt_p50"), ("0.90", "rtt_p90"), ("0.95", "rtt_p95"), ("0.99", "rtt_p99"))
    bytes_col = "resp_bytes" if "resp_bytes" in actual_cols else ("bytes" if "bytes" in actual_cols else None)
    select_parts = [
        "COUNT(*) AS total_reqs",
        f"{sum_or_zero(bytes_col)} AS total_bytes" if bytes_col else "0 AS total_bytes",
        *(
            (
                f"approx_quantile(CAST(tcp_rtt AS DOUBLE), {q}) FILTER (WHERE tcp_rtt IS NOT NULL AND tcp_rtt > 0) / 1000.0 AS {alias}"
                if "tcp_rtt" in actual_cols
                else f"NULL AS {alias}"
            )
            for q, alias in _rtt_quants
        ),
        "AVG(CAST(retrans AS DOUBLE)) FILTER (WHERE retrans IS NOT NULL) AS avg_retrans"
        if "retrans" in actual_cols
        else "NULL AS avg_retrans",
        "AVG(CAST(ploss AS DOUBLE)) FILTER (WHERE ploss IS NOT NULL) * 100.0 AS avg_ploss_pct"
        if "ploss" in actual_cols
        else "NULL AS avg_ploss_pct",
        "AVG(CAST(delivery_rate AS DOUBLE)) FILTER (WHERE delivery_rate IS NOT NULL AND delivery_rate > 0) * 8.0 / 1e6 AS avg_del_rate_mbps"
        if "delivery_rate" in actual_cols
        else "NULL AS avg_del_rate_mbps",
    ]

    main_sql = f"SELECT {', '.join(select_parts)} FROM {table_name} WHERE {where_clause}"
    _t0 = _time.perf_counter()
    try:
        row = get_runner().execute(main_sql, params).fetchone()
        timer.mark("transport_aggregates", _t0, source="live", activity="compute", current_execution=True)
        if row:
            total_requests = int(row[0] or 0)
            bandwidth_bytes = int(row[1] or 0)
            tcp_rtt_p50, tcp_rtt_p90, tcp_rtt_p95, tcp_rtt_p99 = (_opt_float(v, 2) for v in row[2:6])
            retrans_rate, packet_loss = _opt_float(row[6], 3), _opt_float(row[7], 3)
            delivery_rate = _opt_float(row[8], 2)
        else:
            total_requests = bandwidth_bytes = 0
            tcp_rtt_p50 = tcp_rtt_p90 = tcp_rtt_p95 = tcp_rtt_p99 = None
            retrans_rate = packet_loss = delivery_rate = None
    except Exception as e:
        logger.warning("Failed to query transport aggregates: %s", e)
        return default_resp

    protocol_distribution: list[dict[str, Any]] = []
    proto_col = "protocol" if "protocol" in actual_cols else ("proto" if "proto" in actual_cols else None)
    proto_expr = (
        f"""CASE
                    WHEN CAST({proto_col} AS VARCHAR) IN ('3.0', '3', 'HTTP/3') THEN 'HTTP/3'
                    WHEN CAST({proto_col} AS VARCHAR) IN ('2.0', '2', 'HTTP/2') THEN 'HTTP/2'
                    WHEN CAST({proto_col} AS VARCHAR) IN ('1.1', 'HTTP/1.1') THEN 'HTTP/1.1'
                    WHEN CAST({proto_col} AS VARCHAR) IN ('1.0', 'HTTP/1.0') THEN 'HTTP/1.0'
                    ELSE COALESCE(CAST({proto_col} AS VARCHAR), 'Other')
                END"""
        if proto_col
        else ("UPPER(COALESCE(CAST(transport AS VARCHAR), 'TCP'))" if "transport" in actual_cols else None)
    )
    if proto_expr:
        _t1 = _time.perf_counter()
        proto_sql = f"""
            SELECT
                {proto_expr} AS proto_clean,
                COUNT(*) AS reqs
            FROM {table_name}
            WHERE {where_clause}
            GROUP BY proto_clean
            ORDER BY reqs DESC
        """
        try:
            proto_rows = get_runner().execute(proto_sql, params).fetchall()
            timer.mark("protocol_distribution", _t1, source="live", activity="compute", current_execution=True)
            sum_proto_reqs = sum(r[1] for r in proto_rows) or total_requests or 1
            protocol_distribution = [
                {
                    "protocol": str(r[0]),
                    "requests": int(r[1]),
                    "share_pct": round(int(r[1]) * 100.0 / sum_proto_reqs, 2),
                }
                for r in proto_rows
            ]
        except Exception as e:
            logger.warning("Failed to query protocol distribution: %s", e)

    return {
        **default_resp,
        "tcp_rtt_p50_ms": tcp_rtt_p50,
        "tcp_rtt_p90_ms": tcp_rtt_p90,
        "tcp_rtt_p95_ms": tcp_rtt_p95,
        "tcp_rtt_p99_ms": tcp_rtt_p99,
        "retrans_rate_pct": retrans_rate,
        "packet_loss_pct": packet_loss,
        "delivery_rate_mbps": delivery_rate,
        "bandwidth_bytes": bandwidth_bytes,
        "total_requests": total_requests,
        "protocol_distribution": protocol_distribution,
        **get_runner().telemetry(),
    }


# A-3 (CacheRegistry): register the network response cache (also
# pointed at by R-1's tests/conftest.py follow-on).
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("network._response_cache", _response_cache)


def get_pop_health_rows(con, table_name: str, start: Any, end: Any) -> list[tuple]:
    """Live per-POP aggregate rows for ``[start, end]`` (rollup-miss fallback)."""
    from backend.utils.telemetry import track_query

    with track_query(con, SQL.POP_HEALTH_LIVE.format(table_name=table_name), [start, end], "pop_health") as cursor:
        return cursor.fetchall()
