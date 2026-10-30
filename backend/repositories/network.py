"""Network repository — health heatmap, world map, quality metrics."""

from __future__ import annotations

import logging
import sys as _sys
import time as _time
from collections.abc import Callable
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
from backend.repositories.utils.response_shaping import (
    avg_bucket_health_score,
    build_asn_speed_mix,
    build_network_asn_leaderboard,
    clamp_bucket_seconds,
    compute_network_global_summary,
    compute_network_health_score,
    deduplicate_metro_leaderboard,
    empty_network_quality,
    empty_transport_metrics,
    generate_epoch_aligned_buckets,
    opt_float,
    parse_metro_code,
    shape_protocol_distribution,
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


_avg_hs = avg_bucket_health_score
_health_score = compute_network_health_score


def _rtt_congestion_expr() -> str:
    """Subtract unsigned RTT columns without allowing UINT underflow."""
    return "APPROX_QUANTILE(CAST(COALESCE(tcp_rtt, 0) AS BIGINT) - CAST(COALESCE(rtt_min, 0) AS BIGINT), 0.5)"


def _bucket_str(val: Any) -> str:
    return val.isoformat().replace("+00:00", "") if hasattr(val, "isoformat") else str(val)


_opt_float = opt_float


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


def _try_network_heatmap_from_rollup_aligned(
    runner: QueryRunner,
    start_time: str | None,
    end_time: str | None,
    *,
    has_filters: bool,
    bucket_seconds: int,
    want_map_query: bool,
) -> list[Any] | None:
    from backend.repositories.rollup_executor.time_series import _TimeSeriesRollupMixin

    if QueryRunner.try_network_heatmap_from_rollup is not _TimeSeriesRollupMixin.try_network_heatmap_from_rollup:
        return runner.try_network_heatmap_from_rollup(
            start_time, end_time, has_filters=has_filters, bucket_seconds=bucket_seconds
        )
    if not want_map_query:
        return runner.try_network_heatmap_from_rollup(
            start_time, end_time, has_filters=has_filters, bucket_seconds=bucket_seconds
        )
    bs = int(bucket_seconds)
    if bs < 3600 or bs % 3600 != 0:
        return None
    rows = runner.try_network_heatmap_from_rollup(start_time, end_time, has_filters=has_filters, bucket_seconds=3600)
    if rows is None or bs == 3600:
        return rows
    from datetime import UTC, datetime

    grouped: dict[tuple[int, datetime], list[float]] = {}
    for r in rows:
        asn = int(r[0])
        ts = r[1]
        epoch = int(ts.timestamp()) if hasattr(ts, "timestamp") else 0
        aligned_dt = datetime.fromtimestamp((epoch // bs) * bs, tz=UTC)
        reqs = int(r[9] or 0)
        if reqs <= 0:
            continue
        acc = grouped.get((asn, aligned_dt))
        if acc is None:
            acc = [0.0] * 9
            grouped[(asn, aligned_dt)] = acc
        acc[0] += float(r[2] or 0.0)
        acc[1] += float(r[3] or 0.0) * reqs
        acc[2] += float(r[4] or 0.0) * reqs
        acc[3] += float(r[5] or 0.0) * reqs
        acc[4] += float(r[6] or 0.0) * reqs
        acc[5] += float(r[7] or 0.0) * reqs
        acc[6] += float(r[8] or 0.0) * reqs
        acc[7] += float(reqs)
        acc[8] += 1.0
    out: list[Any] = []
    for (asn, aligned_dt), acc in grouped.items():
        tot = acc[7]
        hrs = max(1.0, acc[8])
        out.append(
            (
                asn,
                aligned_dt,
                acc[0] / hrs,
                acc[1] / tot,
                acc[2] / tot,
                acc[3] / tot,
                acc[4] / tot,
                acc[5] / tot,
                acc[6] / tot,
                int(tot),
            )
        )
    out.sort(key=lambda x: x[9], reverse=True)
    return out


def _try_network_geo_from_rollup_per_bucket(
    runner: QueryRunner,
    start_time: str | None,
    end_time: str | None,
    *,
    map_asn: str,
    has_filters: bool,
    bucket_seconds: int,
    per_bucket_limit: int = 50,
) -> tuple[list[Any], list[Any]] | None:
    from backend.core.rollups._common import NETWORK_GEO_BUNDLE_FILENAME
    from backend.repositories.rollup_executor.time_series import _TimeSeriesRollupMixin

    if QueryRunner.try_network_geo_from_rollup is not _TimeSeriesRollupMixin.try_network_geo_from_rollup:
        return runner.try_network_geo_from_rollup(
            start_time, end_time, map_asn=map_asn, has_filters=has_filters, bucket_seconds=bucket_seconds
        )
    bs = int(bucket_seconds)
    if bs < 3600 or bs % 3600 != 0 or map_asn != "all":
        return None
    resolved = runner._rollup_window_and_paths_with_active_hour(
        start_time, end_time, NETWORK_GEO_BUNDLE_FILENAME, has_filters=has_filters
    )
    if resolved is None:
        return None
    st, et, paths_sql, active_hour_start, base_table = resolved
    st_iso, et_iso = st.isoformat(), et.isoformat()
    cols = set(runner.get_schema_cols())
    if "country" not in cols:
        return None
    has_city, has_lat, has_metro = "city" in cols, ("lat" in cols and "lon" in cols), "metro" in cols
    city_col = "city" if has_city else "CAST('' AS VARCHAR)"
    lat_col = "CAST(lat AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
    lon_col = "CAST(lon AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
    metro_col = "metro" if has_metro else "CAST(NULL AS VARCHAR)"
    city_grp, lat_grp, lon_grp, metro_grp = (
        ("city" if has_city else "''"),
        ("lat" if has_lat else "NULL"),
        ("lon" if has_lat else "NULL"),
        ("metro" if has_metro else "NULL"),
    )
    ploss_live_expr = "CAST(AVG(ploss) AS DOUBLE)" if "ploss" in cols else "0::DOUBLE"
    if et > active_hour_start:
        ah_iso = active_hour_start.isoformat()
        map_sql = (
            f"SELECT * FROM (  SELECT    country, city, lat, lon, metro, hour_ts AS bucket_ts,"
            f"    rtt_sum / NULLIF(rtt_count, 0) AS rtt_med_us,    ploss_sum / NULLIF(ploss_count, 0) AS avg_ploss,"
            f"    errors * 100.0 / NULLIF(reqs, 0) AS error_pct,    CAST(reqs AS BIGINT) AS reqs"
            f"  FROM read_parquet([{paths_sql}])  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f"  UNION ALL   SELECT    country,    {city_col} AS city,    {lat_col} AS lat,    {lon_col} AS lon,"
            f"    {metro_col} AS metro,    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
            f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {ploss_live_expr} AS avg_ploss,"
            f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
            f"    CAST(COUNT(*) AS BIGINT) AS reqs  FROM {base_table} "
            f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'    AND country IS NOT NULL AND country != ''"
            f"  GROUP BY country, {city_grp}, {lat_grp}, {lon_grp}, {metro_grp}) "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY reqs DESC) <= {per_bucket_limit} "
            f"ORDER BY bucket_ts, reqs DESC LIMIT 25000"
        )
        metro_sql = (
            f"SELECT country, city, CAST('' AS VARCHAR) AS region, metro, "
            f"  CAST(SUM(rtt_med_us * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS rtt_med_us,"
            f"  CAST(SUM(avg_ploss * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS avg_ploss,"
            f"  CAST(SUM(error_pct * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS error_pct,"
            f"  CAST(SUM(reqs) AS BIGINT) AS total_reqs FROM (  SELECT country, city, metro,"
            f"    SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0) AS rtt_med_us,    SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0) AS avg_ploss,"
            f"    SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0) AS error_pct,    CAST(SUM(reqs) AS BIGINT) AS reqs"
            f"  FROM read_parquet([{paths_sql}])  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f"  GROUP BY country, city, metro  UNION ALL   SELECT country, {city_col} AS city, {metro_col} AS metro,"
            f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {ploss_live_expr} AS avg_ploss,"
            f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
            f"    CAST(COUNT(*) AS BIGINT) AS reqs  FROM {base_table} "
            f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'    AND country IS NOT NULL AND country != ''"
            f"  GROUP BY country, {city_grp}, {metro_grp}) GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
        )
    else:
        map_sql = (
            f"SELECT  country,  city,  lat,  lon,  metro,"
            f"  hour_ts                                           AS bucket_ts,"
            f"  rtt_sum / NULLIF(rtt_count, 0)                  AS rtt_med_us,"
            f"  ploss_sum / NULLIF(ploss_count, 0)              AS avg_ploss,"
            f"  errors * 100.0 / NULLIF(reqs, 0)                AS error_pct,"
            f"  CAST(reqs AS BIGINT)                             AS reqs"
            f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f" QUALIFY ROW_NUMBER() OVER (PARTITION BY hour_ts ORDER BY reqs DESC) <= {per_bucket_limit}"
            f" ORDER BY hour_ts, reqs DESC LIMIT 25000"
        )
        metro_sql = (
            f"SELECT  country,  city,  CAST('' AS VARCHAR)                                   AS region,  metro,"
            f"  SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0)             AS rtt_med_us,"
            f"  SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0)         AS avg_ploss,"
            f"  SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0)           AS error_pct,"
            f"  CAST(SUM(reqs) AS BIGINT)                             AS total_reqs"
            f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
            f" GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
        )
    try:
        map_rows = list(runner.execute(map_sql).fetchall())
        metro_rows = list(runner.execute(metro_sql).fetchall())
    except duckdb.InterruptException:
        raise
    except duckdb.Error:
        return None
    if not map_rows and not metro_rows:
        return None
    if bs > 3600 and map_rows:
        from datetime import UTC, datetime

        by_cell: dict[tuple[Any, Any, Any, Any, Any, datetime], list[float]] = {}
        for r in map_rows:
            ts = r[5]
            epoch = int(ts.timestamp()) if hasattr(ts, "timestamp") else 0
            aligned_dt = datetime.fromtimestamp((epoch // bs) * bs, tz=UTC)
            reqs = int(r[9] or 0)
            if reqs <= 0:
                continue
            key = (r[0], r[1], r[2], r[3], r[4], aligned_dt)
            acc = by_cell.get(key)
            if acc is None:
                acc = [0.0, 0.0, 0.0, 0.0]
                by_cell[key] = acc
            acc[0] += float(r[6] or 0.0) * reqs
            acc[1] += float(r[7] or 0.0) * reqs
            acc[2] += float(r[8] or 0.0) * reqs
            acc[3] += float(reqs)
        by_bucket_cells: dict[datetime, list[tuple[Any, ...]]] = {}
        for (ctry, city, lat, lon, metro, aligned_dt), acc in by_cell.items():
            tot = acc[3]
            by_bucket_cells.setdefault(aligned_dt, []).append(
                (ctry, city, lat, lon, metro, aligned_dt, acc[0] / tot, acc[1] / tot, acc[2] / tot, int(tot))
            )
        re_map_rows: list[Any] = []
        for aligned_dt in sorted(by_bucket_cells):
            cells = sorted(by_bucket_cells[aligned_dt], key=lambda x: x[9], reverse=True)[:per_bucket_limit]
            re_map_rows.extend(cells)
        map_rows = re_map_rows
    return (map_rows, metro_rows)


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

    _want_heatmap_query = _want("heatmap") or _want("leaderboard") or _want("summary")
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
            return {**cached, "metric": metric, "telemetry": {}}

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
            bucket_seconds = clamp_bucket_seconds(bucket_seconds, _st0, _et0)
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

    _est_buckets = (
        int(max(1.0, (_et0 - _st0).total_seconds() / max(1, bucket_seconds)))
        if (_st0 is not None and _et0 is not None)
        else 24
    )
    _per_bucket_limit = 30 if _est_buckets > 200 else 50

    _hoist_net(
        "heatmap",
        _want_heatmap_query,
        lambda: _try_network_heatmap_from_rollup_aligned(
            get_runner(),
            start_time,
            end_time,
            has_filters=bool(filters),
            bucket_seconds=bucket_seconds,
            want_map_query=_want_map_query,
        ),
    )
    _hoist_net(
        "map_geo",
        _want_map_query or _want_metro_query,
        lambda: _try_network_geo_from_rollup_per_bucket(
            get_runner(),
            start_time,
            end_time,
            map_asn=map_asn,
            has_filters=bool(filters),
            bucket_seconds=bucket_seconds,
            per_bucket_limit=_per_bucket_limit,
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
                    ).replace(
                        "GROUP BY 1, 2, 3, 4, 5, 6\n                    ORDER BY reqs DESC\n                    LIMIT 5000",
                        f"GROUP BY 1, 2, 3, 4, 5, 6\n                    QUALIFY ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY reqs DESC) <= {_per_bucket_limit}\n                    ORDER BY bucket ASC, reqs DESC\n                    LIMIT 25000",
                    )
                    _t2 = _time.perf_counter()
                    map_rows_b = get_runner().execute(map_sql, map_params + map_params).fetchall()
                    timer.mark("map_query", _t2, source="live", activity="compute", current_execution=True)

                if _want_metro_query and has_country:
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
        all_buckets_list: list[str] = (
            generate_epoch_aligned_buckets(_st0, _et0, bucket_seconds) if _st0 is not None and _et0 is not None else []
        )

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
            asn_speed_mix = build_asn_speed_mix((r[0], r[1], r[2]) for r in speed_rows)

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
            metro_leaderboard = deduplicate_metro_leaderboard(metro_leaderboard)

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
        asn_labels = {asn: _db.format_asn_label(asn, asn_names_map.get(asn) or f"AS{asn}") for asn in top_asns}
        leaderboard = build_network_asn_leaderboard(
            top_asns=top_asns,
            asn_bucket_data=asn_bucket_data,
            all_buckets=all_buckets,
            bucket_seconds=bucket_seconds,
            all_asns_seen=all_asns_seen,
            asn_speed_mix=asn_speed_mix,
            asn_rtt_pct=asn_rtt_pct,
            asn_labels=asn_labels,
        )

        # ── Global summary ─────────────────────────────────────────────────
        summary_dict = compute_network_global_summary(
            leaderboard=leaderboard,
            rtt_values=[float(r[3]) for r in heatmap_rows if r[3] is not None],
            total_reqs=sum(int(r[9]) for r in heatmap_rows),
            map_buckets=map_buckets if has_country else [],
            cities_list=cities_list,
            zero_as_int=True,
        )

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
            ("summary", summary_dict),
        ):
            if _want(sec_key):
                payload[sec_key] = sec_val
        if _has_signal(payload):
            _memo.put(cache_key, payload, strip=("section_timings",))
        return {**payload, **observation}

    finally:
        if temp_table is not None:
            _drop_temp_table(get_runner(), temp_table, "get_health")


_parse_metro_code = parse_metro_code


def _empty_quality_payload(region_country: str, runner: QueryRunner) -> dict[str, Any]:
    return empty_network_quality(region_country=region_country, **runner.telemetry())


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

    default_resp = empty_transport_metrics(
        has_transport_data=has_transport_data,
        missing_fields=missing_fields,
        section_timings=timer.entries,
        **get_runner().telemetry(),
    )

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
            protocol_distribution = shape_protocol_distribution(
                ((r[0], r[1]) for r in proto_rows),
                total_requests=total_requests,
            )
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
