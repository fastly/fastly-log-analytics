"""Origin metrics repository — fetch timing, error rates, IP health."""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from collections import OrderedDict

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, _safe_table, safe_iso
from backend.repositories.utils.filters import build_where_clause

# ── Response memo cache ───────────────────────────────────────────────────────
# Frontend Origin page fires 6 endpoints in parallel; on cold load each one
# does its own parquet scan (~1-4s). The aggregation predicates differ across
# endpoints, but for a given (start_time, end_time, filters) tuple the *same*
# endpoint will be re-hit every nav-back, browser refresh, or React Query
# refetch tick. Memoizing each endpoint's full response for a short TTL turns
# all subsequent loads within the window into ~50µs dict lookups.
#
# Why: standing rule "i would rather be a little behind the data than read
# extra from the cloud" — 30s is well below the cron tick + ingest latency,
# so a cached response is at most ~30s behind ingest, which is a non-issue
# for an interactive analytics view.
_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 256
_response_cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_response_cache_lock = threading.Lock()


def _bucket_time_to_minute(ts: str | None) -> str | None:
    # Cache-key only: frontend zustand store re-runs `new Date()` on full page
    # reload, so reloads seconds apart would miss the response cache.
    if not ts or len(ts) < 16:
        return ts
    return ts[:16]


def _response_cache_key(
    endpoint: str,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    **extra,
) -> str:
    serialised_filters = {
        k: (getattr(v, "mode", None), sorted(str(x) for x in (getattr(v, "values", None) or [])))
        for k, v in sorted((filters or {}).items())
    }
    payload = json.dumps(
        {
            "ep": endpoint,
            "s": _bucket_time_to_minute(start_time),
            "e": _bucket_time_to_minute(end_time),
            "f": serialised_filters,
            **{k: extra[k] for k in sorted(extra)},
        },
        separators=(",", ":"),
        default=str,
    )
    svc = src.get("name") or src.get("service_id") or ""
    return hashlib.sha256(f"{payload}:{svc}".encode()).hexdigest()


def _response_cache_get(key: str) -> dict | None:
    with _response_cache_lock:
        cached = _response_cache.get(key)
        if cached is None:
            return None
        cached_at, value = cached
        if time.time() - cached_at >= _RESPONSE_CACHE_TTL:
            _response_cache.pop(key, None)
            return None
        _response_cache.move_to_end(key)
        result = value.copy()
        # Pydantic BaseResponse field is `is_cached` (no underscore);
        # serialization_alias renders it as `_is_cached` in JSON.
        # Setting the underscored key here would be silently dropped.
        result["is_cached"] = True
        return result


def _response_cache_put(key: str, value: dict) -> None:
    # Don't cache the response telemetry — debug_queries/debug_calls are
    # per-request and would leak across requests if kept in the cache.
    # Also don't cache `is_cached` itself — it's a per-response marker.
    sanitised = {k: v for k, v in value.items() if k not in ("debug_queries", "debug_calls", "is_cached", "_is_cached")}
    with _response_cache_lock:
        _response_cache[key] = (time.time(), sanitised)
        _response_cache.move_to_end(key)
        while len(_response_cache) > _RESPONSE_CACHE_MAXSIZE:
            _response_cache.popitem(last=False)


# ── POP location helpers ──────────────────────────────────────────────────────


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2.0 * R * math.asin(math.sqrt(a))


def _enrich_with_distance(row: dict) -> dict:
    """Add distance_km, light_speed_rtt_ms, efficiency_ratio, and coordinate fields."""
    from backend.utils.pop_utils import get_pop_lat_lon_map

    pops = get_pop_lat_lon_map()
    # Normalize POP codes to uppercase for reliable lookup
    e_pop = str(row.get("edge_pop", "")).upper()
    s_pop = str(row.get("shield_pop", "")).upper()
    e_coords = pops.get(e_pop)
    s_coords = pops.get(s_pop)
    if e_coords and s_coords:
        dist = _haversine_km(e_coords[0], e_coords[1], s_coords[0], s_coords[1])
        # Fiber propagation: ~200,000 km/s; RTT = 2-way trip
        light_rtt_ms = round(2.0 * dist / 200_000.0 * 1000.0, 2)
        p50 = row.get("p50_ms")
        if light_rtt_ms > 0.5 and p50 is not None:
            efficiency = round(p50 / light_rtt_ms, 2)
        else:
            efficiency = None
        row.update(
            distance_km=round(dist, 1),
            light_speed_rtt_ms=light_rtt_ms,
            efficiency_ratio=efficiency,
            # High ratio alone isn't meaningful for short hops where TCP overhead dominates;
            # require ≥20ms absolute overhead above the theoretical floor before flagging.
            anomaly_static=efficiency is not None and efficiency > 3.0 and p50 - light_rtt_ms >= 20.0,
            edge_lat=e_coords[0],
            edge_lon=e_coords[1],
            shield_lat=s_coords[0],
            shield_lon=s_coords[1],
        )
    else:
        row.update(
            distance_km=None,
            light_speed_rtt_ms=None,
            efficiency_ratio=None,
            anomaly_static=False,
            edge_lat=e_coords[0] if e_coords else None,
            edge_lon=e_coords[1] if e_coords else None,
            shield_lat=s_coords[0] if s_coords else None,
            shield_lon=s_coords[1] if s_coords else None,
        )
    return row


def get_summary(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    cache_key = _response_cache_key("summary", src, start_time, end_time, filters)
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    from backend.repositories._base import empty_schema_response

    if not actual_cols:
        return empty_schema_response(
            has_data=False,
            total_misses=None,
            total_passes=None,
            ottfb_p50_ms=None,
            ottfb_p75_ms=None,
            ottfb_p95_ms=None,
            ottfb_p99_ms=None,
            **runner.telemetry(),
        )

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    # Unified latency expression: prefer ottfb (micros), fallback to ttfb (seconds)
    from backend.repositories._base import origin_latency_us_expr

    lat_val = origin_latency_us_expr(actual_cols)

    ost_5xx = (
        'COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0 / NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)'
        if "ost" in actual_cols
        else "NULL"
    )
    ottlb_p50 = 'MEDIAN("ottlb") / 1000.0' if "ottlb" in actual_cols else "NULL"
    ottlb_p95 = 'APPROX_QUANTILE("ottlb", 0.95) / 1000.0' if "ottlb" in actual_cols else "NULL"
    cdn_ovh = 'MEDIAN("elapsed" - "ottlb") / 1000.0' if "elapsed" in actual_cols and "ottlb" in actual_cols else "NULL"
    obytes_p50 = 'MEDIAN("obytes")' if "obytes" in actual_cols else "NULL"

    row = runner.execute(
        f"""
        SELECT
          COUNT(*) FILTER (WHERE "cache" ILIKE 'MISS%')                                    AS total_misses,
          COUNT(*) FILTER (WHERE "cache" ILIKE 'PASS%')                                    AS total_passes,
          MEDIAN({lat_val}) / 1000.0                                                       AS ottfb_p50_ms,
          APPROX_QUANTILE({lat_val}, 0.75) / 1000.0                                        AS ottfb_p75_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                                        AS ottfb_p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                                        AS ottfb_p99_ms,
          {ottlb_p50}                                                                       AS ottlb_p50_ms,
          {ottlb_p95}                                                                       AS ottlb_p95_ms,
          {cdn_ovh}                                                                         AS cdn_overhead_p50_ms,
          {ost_5xx}                                                                         AS origin_error_rate,
          {obytes_p50}                                                                      AS obytes_p50
        FROM {table_name}
        WHERE {where} AND ({lat_val} IS NOT NULL)
        """,
        params,
    ).fetchone()

    # When no rows match the WHERE clause, DuckDB returns one row of (0 / NULL)
    # aggregates. ottfb_p50_ms being NULL is the canonical "no data" signal —
    # it's the median of the latency expression itself, so it can only be
    # non-NULL if at least one row matched the predicate. Used instead of a
    # separate SELECT 1 ... LIMIT 1 probe, which previously ran ~3s per
    # parallel endpoint on cold caches.
    has_data = row is not None and row[2] is not None

    if not has_data:
        payload = {
            "has_data": False,
            "total_misses": None,
            "total_passes": None,
            "ottfb_p50_ms": None,
            "ottfb_p75_ms": None,
            "ottfb_p95_ms": None,
            "ottfb_p99_ms": None,
            "ottlb_p50_ms": None,
            "ottlb_p95_ms": None,
            "cdn_overhead_p50_ms": None,
            "origin_error_rate": None,
            "obytes_p50": None,
            "by_leg": [],
        }
        _response_cache_put(cache_key, payload)
        return {**payload, **runner.telemetry()}

    edge_rows = []
    if "edge" in actual_cols:
        edge_rows = runner.execute(
            f"""
            SELECT "edge",
              COUNT(*)                                                     AS requests,
              MEDIAN({lat_val}) / 1000.0                                   AS p50_ms,
              APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                    AS p95_ms
            FROM {table_name}
            WHERE {where} AND ({lat_val} IS NOT NULL)
            GROUP BY "edge"
            """,
            params,
        ).fetchall()

    payload = {
        "has_data": True,
        "total_misses": row[0],
        "total_passes": row[1],
        "ottfb_p50_ms": row[2],
        "ottfb_p75_ms": row[3],
        "ottfb_p95_ms": row[4],
        "ottfb_p99_ms": row[5],
        "ottlb_p50_ms": row[6],
        "ottlb_p95_ms": row[7],
        "cdn_overhead_p50_ms": row[8],
        "origin_error_rate": row[9],
        "obytes_p50": row[10],
        "by_leg": [{"edge": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3]} for r in edge_rows],
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_timeseries(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_minutes: float = 5,
    split_by_leg: bool = False,
    metric: str = "ttfb",
    percentile: str = "p95",
) -> dict:
    cache_key = _response_cache_key(
        "timeseries",
        src,
        start_time,
        end_time,
        filters,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        metric=metric,
        percentile=percentile,
    )
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    from backend.repositories._base import empty_schema_response

    if not actual_cols:
        return empty_schema_response(has_data=False, series=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    # Resolve metric column
    metric_col = "ottfb" if metric == "ttfb" else "ottlb"
    if metric_col not in actual_cols:
        if metric == "ttfb" and "ttfb" in actual_cols:
            metric_col = "ttfb"
            unit_conv = "* 1000.0"  # seconds to ms
        else:
            payload = {"has_data": False, "series": []}
            _response_cache_put(cache_key, payload)
            return {**payload, **runner.telemetry()}
    else:
        unit_conv = "/ 1000.0"  # micros to ms

    # Fallback logic for when metric_col exists but might be null (transition period)
    if metric == "ttfb" and "ottfb" in actual_cols and "ttfb" in actual_cols:
        lat_expr = 'COALESCE("ottfb", "ttfb" * 1000000.0)'
        unit_conv = "/ 1000.0"
    else:
        lat_expr = f'"{metric_col}"'

    pct_val = {"p50": 0.5, "p95": 0.95, "p99": 0.99}.get(percentile, 0.95)
    if percentile == "p50":
        agg_expr = f"MEDIAN({lat_expr})"
    else:
        agg_expr = f"APPROX_QUANTILE({lat_expr}, {pct_val})"

    # Bucket interval - handle sub-minute intervals for '1 second' resolution
    if bucket_minutes < 1:
        interval = f"INTERVAL '{max(1, int(bucket_minutes * 60))}' seconds"
    else:
        interval = f"INTERVAL '{int(bucket_minutes)}' minutes"

    edge_col = ', "edge"' if (split_by_leg and "edge" in actual_cols) else ""
    edge_group = ', "edge"' if (split_by_leg and "edge" in actual_cols) else ""

    rows = runner.execute(
        f"""
        SELECT
          time_bucket({interval}, "timestamp")                              AS ts,
          COUNT(*)                                                          AS miss_count,
          {agg_expr} {unit_conv}                                            AS value
          {edge_col}
        FROM {table_name}
        WHERE {where} AND ({lat_expr} IS NOT NULL)
        GROUP BY ts {edge_group}
        ORDER BY ts
        """,
        params,
    ).fetchall()

    has_edge_col = split_by_leg and "edge" in actual_cols
    series = [
        {
            "time": safe_iso(r[0]),
            "miss_count": r[1],
            "value": r[2],
            **({"edge": r[3]} if has_edge_col else {}),
        }
        for r in rows
    ]
    payload = {"has_data": len(series) > 0, "series": series}
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_slow_urls(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 20,
    min_requests: int = 10,
) -> dict:
    cache_key = _response_cache_key(
        "slow_urls", src, start_time, end_time, filters, limit=limit, min_requests=min_requests
    )
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    from backend.repositories._base import origin_latency_us_expr

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        f"""
        SELECT
          "url",
          COUNT(*)                                                         AS requests,
          MEDIAN({lat_val}) / 1000.0                                       AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                        AS p95_ms,
          APPROX_QUANTILE({lat_val}, 0.99) / 1000.0                        AS p99_ms
        FROM {table_name}
        WHERE {where} AND ({lat_val} IS NOT NULL) AND "url" IS NOT NULL
        GROUP BY "url"
        HAVING COUNT(*) >= ?
        ORDER BY p95_ms DESC
        LIMIT ?
        """,
        params + [min_requests, limit],
    ).fetchall()

    payload = {
        "has_data": len(rows) > 0,
        "rows": [{"url": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "p99_ms": r[4]} for r in rows],
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_status_codes(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    cache_key = _response_cache_key("status_codes", src, start_time, end_time, filters)
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "ost" not in actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    rows = runner.execute(
        f"""
        SELECT
          "ost"                                             AS status,
          COUNT(*)                                         AS count,
          COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()        AS pct
        FROM {table_name}
        WHERE {where} AND "ost" IS NOT NULL
        GROUP BY "ost"
        ORDER BY count DESC
        """,
        params,
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "rows": []}
        _response_cache_put(cache_key, payload)
        return {**payload, **runner.telemetry()}

    payload = {
        "has_data": True,
        "rows": [{"status": r[0], "count": r[1], "pct": r[2]} for r in rows],
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_path_breakdown(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    cache_key = _response_cache_key("path_breakdown", src, start_time, end_time, filters)
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "edge" not in actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, shielding_detected=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    from backend.repositories._base import origin_latency_us_expr

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        f"""
        SELECT
          "edge",
          COUNT(*)                                                          AS requests,
          MEDIAN({lat_val}) / 1000.0                                        AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                         AS p95_ms
        FROM {table_name}
        WHERE {where} AND ({lat_val} IS NOT NULL)
        GROUP BY "edge"
        """,
        params,
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "shielding_detected": False, "rows": []}
        _response_cache_put(cache_key, payload)
        return {**payload, **runner.telemetry()}

    # Shielding is in play iff at least one row group has edge=false (shield-leg log).
    # Folds the prior separate "SELECT 1 LIMIT 1" probe into the main aggregate.
    shielding_detected = any(r[0] is False for r in rows)

    payload = {
        "has_data": True,
        "shielding_detected": shielding_detected,
        "rows": [{"edge": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3]} for r in rows],
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_pop_latency(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    cache_key = _response_cache_key("pop_latency", src, start_time, end_time, filters, limit=limit)
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "pop" not in actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, requires_group_c=True, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    from backend.repositories._base import origin_latency_us_expr

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        f"""
        SELECT
          "pop",
          COUNT(*)                                                          AS requests,
          MEDIAN({lat_val}) / 1000.0                                        AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                         AS p95_ms
        FROM {table_name}
        WHERE {where} AND ({lat_val} IS NOT NULL) AND "pop" IS NOT NULL AND "pop" != ''
        GROUP BY "pop"
        ORDER BY p95_ms DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "requires_group_c": False, "rows": []}
        _response_cache_put(cache_key, payload)
        return {**payload, **runner.telemetry()}

    valid_p95s = sorted(r[3] for r in rows if r[3] is not None)
    median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
    payload = {
        "has_data": True,
        "requires_group_c": False,
        "median_p95_ms": median_p95,
        "rows": [
            {
                "pop": r[0],
                "requests": r[1],
                "p50_ms": r[2],
                "p95_ms": r[3],
                "elevated": r[3] is not None and median_p95 is not None and r[3] > median_p95 * 2,
            }
            for r in rows
        ],
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_ip_health(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    cache_key = _response_cache_key("ip_health", src, start_time, end_time, filters, limit=limit)
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "oip" not in actual_cols or "ost" not in actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    from backend.repositories._base import origin_latency_us_expr

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        f"""
        SELECT
          "oip",
          COUNT(*)                                                            AS requests,
          MEDIAN({lat_val}) / 1000.0                                          AS p50_ms,
          APPROX_QUANTILE({lat_val}, 0.95) / 1000.0                           AS p95_ms,
          ROUND(COUNT(*) FILTER (WHERE "ost" >= 500) * 100.0
            / NULLIF(COUNT(*), 0), 1)                                         AS error_pct
        FROM {table_name}
        WHERE {where} AND "oip" IS NOT NULL AND "oip" != '' AND "ost" IS NOT NULL
        GROUP BY "oip"
        HAVING COUNT(*) >= 10
        ORDER BY error_pct DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "rows": []}
        _response_cache_put(cache_key, payload)
        return {**payload, **runner.telemetry()}

    payload = {
        "has_data": True,
        "rows": [{"oip": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "error_pct": r[4]} for r in rows],
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}


def get_shielding_analysis(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 50,
) -> dict:
    cache_key = _response_cache_key("shielding_analysis", src, start_time, end_time, filters, limit=limit)
    runner = QueryRunner(con, src)
    cached = _response_cache_get(cache_key)
    if cached is not None:
        return {**cached, **runner.telemetry()}

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    # We need rid, prid, edge, pop, ottfb for this analysis
    required = {"rid", "prid", "edge", "pop", "ottfb"}
    missing = required - set(actual_cols)
    if missing:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(has_data=False, requires_fields=list(missing), rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    # Shield logs must not be restricted by edge-specific filters like "pop = DEN"
    # otherwise the shield hit at IAD will be filtered out before the join.
    # We only apply time bounds to the shield CTE.
    time_params, time_where = build_where_clause(start_time, end_time, {}, actual_cols)

    query = f"""
        WITH edge_logs AS (
            SELECT "rid", "pop", "ottfb"
            FROM {table_name}
            WHERE {where} AND "edge" = true AND "ottfb" IS NOT NULL
        ),
        shield_logs AS (
            SELECT "prid", "pop", "ottfb", "ttfb"
            FROM {table_name}
            WHERE {time_where} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != ''
        )
        SELECT
          e.pop                                                                    AS edge_pop,
          s.pop                                                                    AS shield_pop,
          COUNT(*)                                                                 AS requests,
          PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (e.ottfb - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p50_ms,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY (e.ottfb - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p95_ms,
          PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY (e.ottfb - COALESCE(s.ottfb, s.ttfb * 1000000))) / 1000.0 AS p99_ms
        FROM edge_logs e
        INNER JOIN shield_logs s ON s.prid = e.rid
        GROUP BY 1, 2
        ORDER BY requests DESC
        LIMIT ?
    """

    rows = runner.execute(query, params + time_params + [limit]).fetchall()

    # If the join returned zero rows, distinguish "no shield logs at all" (edge_only)
    # from "shield logs exist but didn't match any edge rid". Old code did a separate
    # shield-existence probe up-front; we fold that into a single cheap check on the
    # already-scanned data instead by re-checking the time window for any shield log.
    if not rows:
        shield_exists = runner.execute(
            f'SELECT 1 FROM {table_name} WHERE {time_where} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != \'\' LIMIT 1',
            time_params,
        ).fetchone()
        if shield_exists is None:
            payload = {"has_data": False, "edge_only": True, "rows": []}
        else:
            payload = {"has_data": False, "rows": []}
        _response_cache_put(cache_key, payload)
        return {**payload, **runner.telemetry()}

    enriched_rows = [
        _enrich_with_distance(
            {
                "edge_pop": r[0],
                "shield_pop": r[1],
                "requests": r[2],
                "p50_ms": r[3],
                "p95_ms": r[4],
                "p99_ms": r[5],
            }
        )
        for r in rows
    ]

    # Only claim has_data if we have at least one row with valid coordinates
    has_valid_arcs = any(r.get("edge_lat") is not None and r.get("shield_lat") is not None for r in enriched_rows)

    payload = {
        "has_data": has_valid_arcs,
        "rows": enriched_rows,
    }
    _response_cache_put(cache_key, payload)
    return {**payload, **runner.telemetry()}
