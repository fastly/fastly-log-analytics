"""Origin metrics repository — fetch timing, error rates, IP health."""

from __future__ import annotations

import math
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
    origin_latency_us_expr,
    safe_iso,
)
from backend.repositories._sql import origin as SQL
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    bucket_time_to_minute,
    cache_get,
    cache_put,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import BoundedTTLCache

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
_response_cache: BoundedTTLCache = BoundedTTLCache(maxsize=_RESPONSE_CACHE_MAXSIZE, ttl_seconds=_RESPONSE_CACHE_TTL)


def _response_cache_key(
    endpoint: str,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    **extra,
) -> str:
    # Key field order is load-bearing (serialized as-is): ep, s, e, f, then the
    # extras sorted by name. digest_cache_key keeps the emitted bytes stable.
    payload = {
        "ep": endpoint,
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        **{k: extra[k] for k in sorted(extra)},
    }
    return digest_cache_key(payload, src)


def _check_response_cache(
    endpoint: str,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    runner: QueryRunner,
    **extras,
) -> tuple[str, dict | None]:
    """Build the cache key for ``endpoint`` and return ``(key, cached_response)``.

    When the cache hits, ``cached_response`` is the stored payload with
    fresh telemetry merged in (so the response shape matches what a
    live miss would have produced). When it misses, ``cached_response``
    is None — callers compute the payload and finish with
    :func:`_store_response_cache` to persist + merge telemetry.

    Eight origin endpoints all repeat this same cache-key + get + miss
    dance; the helper makes the invariant ("cache stores stripped
    payload, telemetry is per-request") un-skippable.
    """
    cache_key = _response_cache_key(endpoint, src, start_time, end_time, filters, **extras)
    cached = cache_get(_response_cache, cache_key)
    if cached is not None:
        return cache_key, {**cached, **runner.telemetry()}
    return cache_key, None


def _store_response_cache(cache_key: str, payload: dict, runner: QueryRunner) -> dict:
    """Cache the payload under ``cache_key`` and return it with telemetry.

    Pairs with :func:`_check_response_cache` — every miss path ends with
    ``return _store_response_cache(key, payload, runner)``. Strips
    telemetry from the stored copy (per :func:`cache_put`) and merges it
    onto the returned response, so the cache stays per-request-clean while
    callers see the full shape.
    """
    cache_put(_response_cache, cache_key, payload)
    return {**payload, **runner.telemetry()}


# ── POP location helpers ──────────────────────────────────────────────────────


# Minimum edge→shield request count before a route's transit is trustworthy
# enough to flag as anomalous. The transit metric keys off the *median*
# (p50), but a "median" over a handful of requests is noise — a single cold
# TLS handshake or an ottfb→ttfb fallback row can drag a 1-3 request route
# past the ratio/overhead gates and paint a false "suboptimal peering" flag.
# (Observed on prod 2026-06-30: 14 of 15 anomaly flags were on <30-request
# routes; median requests/flagged-route was 2 vs 147 for non-flagged.)
# Below this floor we still SHOW the route (operators want low-volume routes
# visible — that's the M1 intent) but mark it ``low_sample`` and never flag it.
# Mirrors the ``min_requests`` gate the slow-URLs analysis already applies.
SHIELDING_ANOMALY_MIN_REQUESTS = 30


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
    # Too few samples to trust the percentiles — keep the route visible but never
    # flag it (see SHIELDING_ANOMALY_MIN_REQUESTS). The FE mutes low_sample rows.
    low_sample = (row.get("requests") or 0) < SHIELDING_ANOMALY_MIN_REQUESTS
    if e_coords and s_coords:
        dist = _haversine_km(e_coords[0], e_coords[1], s_coords[0], s_coords[1])
        # Fiber propagation: ~200,000 km/s; RTT = 2-way trip
        light_rtt_ms = round(2.0 * dist / 200_000.0 * 1000.0, 2)
        p50 = row.get("p50_ms")
        if light_rtt_ms > 0.5 and p50 is not None:
            efficiency = round(p50 / light_rtt_ms, 2)
        else:
            efficiency = None
        # The latency verdict, independent of sample size. High ratio alone
        # isn't meaningful for short hops where TCP overhead dominates; require
        # ≥20ms absolute overhead above the theoretical floor before flagging.
        anomaly_eligible = (
            efficiency is not None and efficiency > 3.0 and p50 is not None and p50 - light_rtt_ms >= 20.0
        )
        row.update(
            distance_km=round(dist, 1),
            light_speed_rtt_ms=light_rtt_ms,
            efficiency_ratio=efficiency,
            low_sample=low_sample,
            anomaly_eligible=anomaly_eligible,
            # Never flag a route with too few requests for its median to mean
            # anything. The FE can re-derive this against a user-chosen floor
            # from (anomaly_eligible, requests) without the latency rule above.
            anomaly_static=anomaly_eligible and not low_sample,
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
            low_sample=low_sample,
            anomaly_eligible=False,
            anomaly_static=False,
            edge_lat=e_coords[0] if e_coords else None,
            edge_lon=e_coords[1] if e_coords else None,
            shield_lat=s_coords[0] if s_coords else None,
            shield_lon=s_coords[1] if s_coords else None,
        )
    return row


def _shape_summary(
    runner: QueryRunner,
    table: str,
    where: str,
    params: list,
    lat_val: str,
    actual_cols: set[str] | list[str],
) -> dict:
    """Render SUMMARY_GROUPING_SETS against ``table``, return the payload dict.

    Shared between :func:`get_summary` (live path, full base table) and
    :func:`_origin_summary_from_temp` (per-request TEMP TABLE path,
    ``table='<temp_table>'``, ``where='1=1'``, ``lat_val='lat_us'``).
    The two paths used to be byte-identical Python with different SQL
    templates (TEMP_SUMMARY_ROLLUP + TEMP_SUMMARY_BY_EDGE on the TEMP
    side); folded to one template + one helper so the column shape can
    only drift in one place.

    Rows are consumed via ``cursor.description`` dict access rather than
    positional indices. The previous shape (``row[3]``, ``row[5]``, …)
    silently shifted every downstream column when SUMMARY_GROUPING_SETS
    gained a new column without a matching update here — the offset-by-N
    footgun the b10 audit finding flagged.
    """
    actual_cols_set = set(actual_cols)

    # N-8: return a ratio (0.0–1.0), NOT a percentage. The frontend at
    # ``frontend/app/origin/_sections/Aggregates.tsx`` already multiplies
    # the value by 100 to render; the prior ``* 100.0`` here made the
    # display show 2181.11% on a real 21.81% error rate. Also clamp the
    # 5xx filter to (500-599) — counting any "ost >= 500" let buggy
    # synthetic codes leak in (origin status 829 was observed in prod).
    ost_5xx = (
        'COUNT(*) FILTER (WHERE "ost" >= 500 AND "ost" < 600) * 1.0 / '
        'NULLIF(COUNT(*) FILTER (WHERE "ost" IS NOT NULL), 0)'
        if "ost" in actual_cols_set
        else "NULL"
    )
    ottlb_p50 = 'MEDIAN("ottlb") / 1000.0' if "ottlb" in actual_cols_set else "NULL"
    ottlb_p95 = 'APPROX_QUANTILE("ottlb", 0.95) / 1000.0' if "ottlb" in actual_cols_set else "NULL"
    cdn_ovh = (
        'MEDIAN("elapsed" - "ottlb") / 1000.0'
        if "elapsed" in actual_cols_set and "ottlb" in actual_cols_set
        else "NULL"
    )
    obytes_p50 = 'MEDIAN("obytes")' if "obytes" in actual_cols_set else "NULL"

    # Single () grouping over the requested window. The previous
    # GROUPING SETS shape returned a per-edge breakdown as ``by_leg``
    # in the same scan, but no UI surface consumed it (the page
    # hard-codes ``split_by_leg: false``) — DuckDB was paying the
    # second hash partition + per-edge percentile sorts for nothing.
    cur = runner.execute(
        SQL.SUMMARY_ROLLUP.format(
            lat_val=lat_val,
            ottlb_p50=ottlb_p50,
            ottlb_p95=ottlb_p95,
            cdn_ovh=cdn_ovh,
            ost_5xx=ost_5xx,
            obytes_p50=obytes_p50,
            table=table,
            where=where,
        ),
        params,
    )
    cols = [d[0] for d in cur.description]
    rollup_row = next(
        (dict(zip(cols, r, strict=False)) for r in cur.fetchall()),
        None,
    )

    # ``ottfb_p50_ms`` being NULL is the canonical "no data" signal — it's
    # MEDIAN(lat_val), so it can only be non-NULL if at least one row
    # matched ``lat_val IS NOT NULL``.
    has_data = rollup_row is not None and rollup_row["ottfb_p50_ms"] is not None

    if not has_data:
        return {
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
        }

    assert rollup_row is not None  # has_data check above narrowed this
    return {
        "has_data": True,
        "total_misses": rollup_row["total_misses"],
        "total_passes": rollup_row["total_passes"],
        "ottfb_p50_ms": rollup_row["ottfb_p50_ms"],
        "ottfb_p75_ms": rollup_row["ottfb_p75_ms"],
        "ottfb_p95_ms": rollup_row["ottfb_p95_ms"],
        "ottfb_p99_ms": rollup_row["ottfb_p99_ms"],
        "ottlb_p50_ms": rollup_row["ottlb_p50_ms"],
        "ottlb_p95_ms": rollup_row["ottlb_p95_ms"],
        "cdn_overhead_p50_ms": rollup_row["cdn_overhead_p50_ms"],
        "origin_error_rate": rollup_row["origin_error_rate"],
        "obytes_p50": rollup_row["obytes_p50"],
    }


def get_summary(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("summary", src, start_time, end_time, filters, runner)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()

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
    # Unified latency expression: prefer ottfb (micros), fallback to ttfb (seconds).
    lat_val = origin_latency_us_expr(actual_cols)
    payload: dict[str, Any] = _shape_summary(runner, table_name, where, params, lat_val, actual_cols)
    return _store_response_cache(cache_key, payload, runner)


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
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(
        "timeseries",
        src,
        start_time,
        end_time,
        filters,
        runner,
        bucket_minutes=bucket_minutes,
        split_by_leg=split_by_leg,
        metric=metric,
        percentile=percentile,
    )
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()

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
            return _store_response_cache(cache_key, payload, runner)
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
        SQL.TIMESERIES_BUCKETED.format(
            interval=interval,
            agg_expr=agg_expr,
            unit_conv=unit_conv,
            edge_col=edge_col,
            table=table_name,
            where=where,
            lat_expr=lat_expr,
            edge_group=edge_group,
        ),
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
    return _store_response_cache(cache_key, payload, runner)


def get_slow_urls(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 20,
    min_requests: int = 10,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(
        "slow_urls", src, start_time, end_time, filters, runner, limit=limit, min_requests=min_requests
    )
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.SLOW_URLS.format(lat_val=lat_val, table=table_name, where=where),
        params + [min_requests, limit],
    ).fetchall()

    payload = {
        "has_data": len(rows) > 0,
        "rows": [{"url": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "p99_ms": r[4]} for r in rows],
    }
    return _store_response_cache(cache_key, payload, runner)


def get_status_codes(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("status_codes", src, start_time, end_time, filters, runner)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "ost" not in actual_cols:
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    rows = runner.execute(
        SQL.STATUS_CODES.format(table=table_name, where=where),
        params,
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "rows": []}
        return _store_response_cache(cache_key, payload, runner)

    payload = {
        "has_data": True,
        "rows": [{"status": r[0], "count": r[1], "pct": r[2]} for r in rows],
    }
    return _store_response_cache(cache_key, payload, runner)


def get_path_breakdown(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("path_breakdown", src, start_time, end_time, filters, runner)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "edge" not in actual_cols:
        return empty_schema_response(has_data=False, shielding_detected=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.PATH_BREAKDOWN.format(lat_val=lat_val, table=table_name, where=where),
        params,
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "shielding_detected": False, "rows": []}
        return _store_response_cache(cache_key, payload, runner)

    # Shielding is in play iff at least one row group has edge=false (shield-leg log).
    # Folds the prior separate "SELECT 1 LIMIT 1" probe into the main aggregate.
    shielding_detected = any(r[0] is False for r in rows)

    payload = {
        "has_data": True,
        "shielding_detected": shielding_detected,
        "rows": [{"edge": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3]} for r in rows],
    }
    return _store_response_cache(cache_key, payload, runner)


def get_pop_latency(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("pop_latency", src, start_time, end_time, filters, runner, limit=limit)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "pop" not in actual_cols:
        return empty_schema_response(has_data=False, requires_group_c=True, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.POP_LATENCY.format(lat_val=lat_val, table=table_name, where=where),
        params + [limit],
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "requires_group_c": False, "rows": []}
        return _store_response_cache(cache_key, payload, runner)

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
    return _store_response_cache(cache_key, payload, runner)


def get_ip_health(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 30,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache("ip_health", src, start_time, end_time, filters, runner, limit=limit)
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols or "oip" not in actual_cols or "ost" not in actual_cols:
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.IP_HEALTH.format(lat_val=lat_val, table=table_name, where=where),
        params + [limit],
    ).fetchall()

    if not rows:
        payload = {"has_data": False, "rows": []}
        return _store_response_cache(cache_key, payload, runner)

    payload = {
        "has_data": True,
        "rows": [{"oip": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "error_pct": r[4]} for r in rows],
    }
    return _store_response_cache(cache_key, payload, runner)


def get_shielding_analysis(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    limit: int = 50,
) -> dict:
    runner = QueryRunner(con, src)
    cache_key, hit = _check_response_cache(
        "shielding_analysis", src, start_time, end_time, filters, runner, limit=limit
    )
    if hit:
        return hit

    table_name = _safe_table(src["name"])
    actual_cols = runner.get_schema_cols()
    if not actual_cols:
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    # We need rid, prid, edge, pop, ottfb, ttfb for this analysis
    required = {"rid", "prid", "edge", "pop", "ottfb", "ttfb"}
    missing = required - set(actual_cols)
    if missing:
        return empty_schema_response(has_data=False, requires_fields=list(missing), rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    # Shield logs must not be restricted by edge-specific filters like "pop = DEN"
    # otherwise the shield hit at IAD will be filtered out before the join.
    # We only apply time bounds to the shield CTE.
    time_params, time_where = build_where_clause(start_time, end_time, {}, actual_cols)

    query = SQL.SHIELDING_ANALYSIS.format(
        table=table_name,
        where=where,
        time_where=time_where,
    )

    # The query binds two trailing rank cutoffs (top-by-requests OR
    # top-by-overhead) — see SHIELDING_ANALYSIS docstring (M1).
    cur = runner.execute(query, params + time_params + [limit, limit])
    # Consume rows via ``cursor.description`` (dict access) rather than
    # positional ``r[0..5]``. A column reorder in SHIELDING_ANALYSIS would
    # otherwise silently misalign every downstream key — the same b10
    # offset-by-N footgun ``_shape_summary`` was already hardened against.
    col_names = [d[0] for d in cur.description]
    raw_rows = [dict(zip(col_names, r, strict=False)) for r in cur.fetchall()]

    # If the join returned zero rows, distinguish "no shield logs at all" (edge_only)
    # from "shield logs exist but didn't match any edge rid". Old code did a separate
    # shield-existence probe up-front; we fold that into a single cheap check on the
    # already-scanned data instead by re-checking the time window for any shield log.
    if not raw_rows:
        shield_exists = runner.execute(
            f'SELECT 1 FROM {table_name} WHERE {time_where} AND "edge" = false AND "prid" IS NOT NULL AND "prid" != \'\' LIMIT 1',
            time_params,
        ).fetchone()
        if shield_exists is None:
            payload = {"has_data": False, "edge_only": True, "rows": []}
        else:
            payload = {"has_data": False, "rows": []}
        return _store_response_cache(cache_key, payload, runner)

    # ``total_routes`` is a window COUNT(*) OVER () — identical across rows.
    total_routes = raw_rows[0].get("total_routes")
    if total_routes is None:
        total_routes = len(raw_rows)

    enriched_rows = [
        _enrich_with_distance(
            {
                "edge_pop": r["edge_pop"],
                "shield_pop": r["shield_pop"],
                "requests": r["requests"],
                "p50_ms": r["p50_ms"],
                "p95_ms": r["p95_ms"],
                "p99_ms": r["p99_ms"],
            }
        )
        for r in raw_rows
    ]

    # has_data gates on ROW presence, not coordinate availability (L3). The
    # table renders POP codes + latencies fine without coords; the MAP
    # handles the no-coordinate case on its own ("POP coordinates
    # unavailable"). Gating on coords previously hid the whole table + CSV
    # export whenever a POP was absent from pop_locations.json.
    payload = {
        "has_data": len(enriched_rows) > 0,
        "rows": enriched_rows,
        # M1: surface the full route count so the UI can show "Top N of M"
        # and flag truncation instead of implying the table is complete.
        "total_routes": total_routes,
        "truncated": total_routes > len(enriched_rows),
    }
    return _store_response_cache(cache_key, payload, runner)


# ── Composite: get_aggregates ────────────────────────────────────────────────
#
# Phase 3 item 9. One CREATE TEMP TABLE filtered to the requested window;
# every origin card on the /origin page reads from the same materialization
# instead of issuing its own parquet scan. Shielding analysis stays in its
# own endpoint (item 13 moves it to /api/network-health) because its
# self-join semantics don't share the projection cleanly.
#
# Granular endpoints (/api/origin/summary etc.) remain alive for one
# release so the frontend can flip back during a rollback without a
# backend redeploy. The composite is purely additive — existing per-card
# endpoints are unaffected.


def _origin_summary_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
) -> dict:
    """get_summary against the per-request TEMP TABLE.

    Uses the pre-computed ``lat_us`` column populated when the TEMP TABLE
    was created — saves the per-row COALESCE evaluation that turned the
    composite into a regression on local benchmarks. Otherwise byte-
    identical to :func:`get_summary`'s SQL via the shared
    :func:`_shape_summary` helper (the TEMP-specific templates
    ``TEMP_SUMMARY_ROLLUP`` / ``TEMP_SUMMARY_BY_EDGE`` were folded into
    ``SUMMARY_GROUPING_SETS`` per the b10 audit finding).

    PURE temp-path: the rollup-first attempt was hoisted into
    :func:`get_aggregates`'s pre-temp phase (Part B skip-temp guard), so
    this helper now only runs when the section MISSED the rollup and the
    temp was built. ``start_time`` / ``end_time`` / ``has_filters`` are
    retained for signature stability but no longer consulted here.
    """
    return _shape_summary(runner, temp_table, "1=1", [], "lat_us", actual_cols)


def _origin_timeseries_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    bucket_minutes: float,
    split_by_leg: bool,
    metric: str,
    percentile: str,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
    table_name: str | None = None,
    where_clause: str | None = None,
    params: list | None = None,
) -> dict:
    # PURE temp-path: the origin_latency_ts rollup-first attempt was hoisted
    # into get_aggregates's pre-temp phase (Part B skip-temp guard). This helper
    # now only runs for the MISSED case (filtered / <48h / split_by_leg /
    # sub-minute bucket / partial coverage) against the materialized temp table.
    # The ``table_name`` / ``where_clause`` / ``params`` / ``start_time`` /
    # ``end_time`` / ``has_filters`` kwargs are retained for signature stability
    # but no longer consulted here.
    actual_cols_set = set(actual_cols)
    metric_col = "ottfb" if metric == "ttfb" else "ottlb"
    unit_conv = "/ 1000.0"
    if metric_col not in actual_cols_set:
        if metric == "ttfb" and "ttfb" in actual_cols_set:
            metric_col = "ttfb"
            unit_conv = "* 1000.0"
        else:
            return {"has_data": False, "series": []}

    if metric == "ttfb" and "ottfb" in actual_cols_set and "ttfb" in actual_cols_set:
        lat_expr = '"lat_us"'
        unit_conv = "/ 1000.0"
    else:
        lat_expr = f'"{metric_col}"'

    pct_val = {"p50": 0.5, "p95": 0.95, "p99": 0.99}.get(percentile, 0.95)
    agg_expr = f"MEDIAN({lat_expr})" if percentile == "p50" else f"APPROX_QUANTILE({lat_expr}, {pct_val})"

    if bucket_minutes < 1:
        interval = f"INTERVAL '{max(1, int(bucket_minutes * 60))}' seconds"
    else:
        interval = f"INTERVAL '{int(bucket_minutes)}' minutes"

    edge_col = ', "edge"' if (split_by_leg and "edge" in actual_cols_set) else ""
    edge_group = ', "edge"' if (split_by_leg and "edge" in actual_cols_set) else ""

    rows = runner.execute(
        SQL.TIMESERIES_BUCKETED.format(
            interval=interval,
            agg_expr=agg_expr,
            unit_conv=unit_conv,
            edge_col=edge_col,
            table=temp_table,
            where="1=1",
            lat_expr=lat_expr,
            edge_group=edge_group,
        )
    ).fetchall()

    has_edge_col = split_by_leg and "edge" in actual_cols_set
    series = [
        {
            "time": safe_iso(r[0]),
            "miss_count": r[1],
            "value": r[2],
            **({"edge": r[3]} if has_edge_col else {}),
        }
        for r in rows
    ]
    return {"has_data": len(series) > 0, "series": series}


def _origin_slow_urls_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    min_requests: int,
    limit: int,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
) -> dict:
    actual_cols_set = set(actual_cols)
    if "url" not in actual_cols_set:
        return {"has_data": False, "rows": []}

    # PURE temp-path: the slow_urls rollup-first attempt was hoisted into
    # get_aggregates's pre-temp phase (Part B skip-temp guard). This helper now
    # only runs for the MISSED case. ``start_time`` / ``end_time`` /
    # ``has_filters`` are retained for signature stability but unused here.
    #
    # Use the pre-computed lat_us column so percentile sorts can leverage
    # column-store layout instead of paying COALESCE per row.
    rows = runner.execute(
        SQL.SLOW_URLS.format(lat_val="lat_us", table=temp_table, where="1=1"),
        [min_requests, limit],
    ).fetchall()
    return {
        "has_data": len(rows) > 0,
        "rows": [{"url": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "p99_ms": r[4]} for r in rows],
    }


def _origin_status_codes_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
) -> dict:
    if "ost" not in set(actual_cols):
        return {"has_data": False, "rows": []}

    # PURE temp-path: the status_codes rollup-first attempt was hoisted into
    # get_aggregates's pre-temp phase (Part B skip-temp guard). This helper now
    # only runs for the MISSED case. ``start_time`` / ``end_time`` /
    # ``has_filters`` are retained for signature stability but unused here.
    rows = runner.execute(SQL.STATUS_CODES.format(table=temp_table, where="1=1")).fetchall()
    if not rows:
        return {"has_data": False, "rows": []}
    return {
        "has_data": True,
        "rows": [{"status": r[0], "count": r[1], "pct": r[2]} for r in rows],
    }


def _origin_path_breakdown_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
) -> dict:
    actual_cols_set = set(actual_cols)
    if "edge" not in actual_cols_set:
        return {"has_data": False, "shielding_detected": False, "rows": []}

    # PURE temp-path: the origin_path rollup-first attempt was hoisted into
    # get_aggregates's pre-temp phase (Part B skip-temp guard). This helper now
    # only runs for the MISSED case. ``start_time`` / ``end_time`` /
    # ``has_filters`` are retained for signature stability but unused here.
    rows = runner.execute(SQL.PATH_BREAKDOWN.format(lat_val="lat_us", table=temp_table, where="1=1")).fetchall()
    if not rows:
        return {"has_data": False, "shielding_detected": False, "rows": []}
    shielding_detected = any(r[0] is False for r in rows)
    return {
        "has_data": True,
        "shielding_detected": shielding_detected,
        "rows": [{"edge": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3]} for r in rows],
    }


def _origin_pop_latency_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    limit: int,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
) -> dict:
    actual_cols_set = set(actual_cols)
    if "pop" not in actual_cols_set:
        return {"has_data": False, "requires_group_c": True, "rows": []}

    # PURE temp-path: the origin_pop rollup-first attempt was hoisted into
    # get_aggregates's pre-temp phase (Part B skip-temp guard). This helper now
    # only runs for the MISSED case. ``start_time`` / ``end_time`` /
    # ``has_filters`` are retained for signature stability but unused here.
    rows = runner.execute(
        SQL.POP_LATENCY.format(lat_val="lat_us", table=temp_table, where="1=1"),
        [limit],
    ).fetchall()
    if not rows:
        return {"has_data": False, "requires_group_c": False, "rows": []}
    valid_p95s = sorted(r[3] for r in rows if r[3] is not None)
    median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
    return {
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


def _origin_ip_health_from_temp(
    runner: QueryRunner,
    temp_table: str,
    actual_cols: set[str] | list[str],
    limit: int,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    has_filters: bool = True,
) -> dict:
    actual_cols_set = set(actual_cols)
    if "oip" not in actual_cols_set or "ost" not in actual_cols_set:
        return {"has_data": False, "rows": []}

    # PURE temp-path: the origin_ip rollup-first attempt was hoisted into
    # get_aggregates's pre-temp phase (Part B skip-temp guard). This helper now
    # only runs for the MISSED case. ``start_time`` / ``end_time`` /
    # ``has_filters`` are retained for signature stability but unused here.
    rows = runner.execute(
        SQL.IP_HEALTH.format(lat_val="lat_us", table=temp_table, where="1=1"),
        [limit],
    ).fetchall()
    if not rows:
        return {"has_data": False, "rows": []}
    return {
        "has_data": True,
        "rows": [{"oip": r[0], "requests": r[1], "p50_ms": r[2], "p95_ms": r[3], "error_pct": r[4]} for r in rows],
    }


async def get_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    *,
    bucket_minutes: float = 5,
    split_by_leg: bool = False,
    timeseries_metric: str = "ttfb",
    timeseries_percentile: str = "p95",
    slow_urls_limit: int = 20,
    slow_urls_min_requests: int = 50,
    ip_health_limit: int = 30,
    pop_latency_limit: int = 30,
    sections: set[str] | None = None,
) -> dict:
    """Composite origin endpoint — seven origin cards from one parquet scan.

    Materializes a per-request catalog table once on ``ctx.con`` then
    runs the seven reads (summary, timeseries, slow_urls, status_codes,
    path_breakdown, pop_latency, ip_health) across four pool connections
    via ``asyncio.gather``. Shielding-analysis stays separate
    (/api/origin/shielding-analysis).

    ``sections`` is the resolved selector set produced by the router's
    ``_expand_sections`` (coupling rules already applied). ``None``
    preserves the pre-selector contract — every section computes and is
    returned. When set, branches whose sections are all excluded are
    skipped entirely (no pool checkout, no SQL), and within a still-live
    branch only the requested sections execute. The shared temp
    materialization always runs because every live branch reads from it
    — that's the floor cost the composite exists to amortize.
    """
    import asyncio
    import time as _time
    import uuid as _uuid

    from backend.core.duckdb_pool import _PoolBusy, checkout_connection

    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

    def _want(name: str) -> bool:
        return sections is None or name in sections

    empty_payload = {
        "has_data": False,
        "summary": {},
        "timeseries": {"has_data": False, "series": []},
        "slow_urls": {"has_data": False, "rows": []},
        "status_codes": {"has_data": False, "rows": []},
        "path_breakdown": {"has_data": False, "shielding_detected": False, "rows": []},
        "pop_latency": {"has_data": False, "requires_group_c": False, "rows": []},
        "ip_health": {"has_data": False, "rows": []},
    }

    if not actual_cols:
        return {**empty_payload, **runner.telemetry()}

    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    has_filters = bool(filters)

    # Union of columns needed across the seven sub-queries. Filtered to
    # those the schema actually has before materialization so missing
    # columns don't break the CREATE. Plus a precomputed `lat_us` column
    # — the percentile sub-queries all use the same COALESCE("ottfb",
    # "ttfb"*1000000.0) expression and computing it once at
    # materialization time lets DuckDB store it in the column-store
    # layout. Without the precompute, the in-memory table was
    # SLOWER than per-endpoint parquet scans because the COALESCE
    # forces per-row evaluation during percentile sort.
    actual_set = set(actual_cols)
    wanted_cols = [
        "timestamp",
        "cache",
        "edge",
        "url",
        "oip",
        "ost",
        "pop",
        "ottfb",
        "ottlb",
        "ttfb",
        "elapsed",
        "obytes",
    ]
    select_cols = [f'"{c}"' for c in wanted_cols if c in actual_set]
    if not select_cols:
        return {**empty_payload, **runner.telemetry()}
    lat_us_expr = origin_latency_us_expr(actual_set)
    temp_table = f"t_origin_{_uuid.uuid4().hex}"
    create_sql = SQL.AGGREGATES_CREATE_TEMP.format(
        temp_table=temp_table,
        select_cols=", ".join(select_cols),
        lat_us_expr=lat_us_expr,
        table=table_name,
        where_clause=where_clause,
    )
    # Per-phase wall-clock timings surface in the response under
    # ``section_timings`` so the perf harness can attribute time inside
    # /api/origin/aggregates without re-running ad-hoc instrumentation —
    # mirrors the pattern used by dashboard.py, network.py, etc.
    timer = SectionTimer()
    section_timings = timer.entries

    # ── Part B: hoisted rollup phase (BEFORE the temp build) ──────────────
    #
    # For each REQUESTED section, attempt its try_*_from_rollup on the primary
    # runner (cheap parquet reads, no temp). A non-None hit lands in ``merged``
    # and is timed; a None means the section MISSED the rollup (filtered, <48h,
    # split_by_leg, sub-minute bucket, or partial coverage) and must be served
    # live from the temp. The temp_table_create — the dominant cost on the wide
    # unfiltered path — is built ONLY when at least one section missed.
    merged: dict[str, Any] = {}
    missed: set[str] = set()

    def _hoist(name: str, reader) -> None:
        if not _want(name):
            return
        _ts = _time.perf_counter()
        result = reader()
        timer.mark(name, _ts)
        if result is not None:
            merged[name] = result
        else:
            missed.add(name)

    _hoist(
        "summary",
        lambda: runner.try_origin_summary_from_rollup(
            start_time, end_time, has_filters=has_filters, actual_cols=actual_set
        ),
    )
    _hoist(
        "slow_urls",
        lambda: runner.try_slow_urls_from_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            min_requests=slow_urls_min_requests,
            limit=slow_urls_limit,
        ),
    )
    _hoist(
        "status_codes",
        lambda: runner.try_origin_status_from_rollup(start_time, end_time, has_filters=has_filters),
    )
    _hoist(
        "path_breakdown",
        lambda: runner.try_origin_path_breakdown_from_rollup(start_time, end_time, has_filters=has_filters),
    )
    _hoist(
        "pop_latency",
        lambda: runner.try_origin_pop_latency_from_rollup(
            start_time, end_time, has_filters=has_filters, limit=pop_latency_limit
        ),
    )
    _hoist(
        "ip_health",
        lambda: runner.try_origin_ip_health_from_rollup(
            start_time, end_time, has_filters=has_filters, limit=ip_health_limit
        ),
    )
    _hoist(
        "timeseries",
        lambda: runner.try_origin_latency_ts_from_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bucket_minutes=bucket_minutes,
            metric=timeseries_metric,
            percentile=timeseries_percentile,
            split_by_leg=split_by_leg,
            # The reader's active-hour live merge reads the BASE view (not the
            # temp); pass the base table + the (time-only, since unfiltered)
            # inlined where_clause + params. Mirrors dashboard.py's
            # try_time_series_from_rollup call shape.
            table_name=table_name,
            where_clause=where_clause,
            params=params,
        ),
    )

    # All requested sections hit the rollup → skip the temp build entirely.
    # This is the wide-unfiltered + fully-backfilled win: temp_table_create is
    # ABSENT from section_timings. ``origin:temp_skipped`` lets the harness
    # confirm the guard fired. has_data still derives from the summary card.
    if not missed:
        section_timings.append({"section": "origin:temp_skipped", "time_ms": 0.0})
        summary_card = merged.get("summary") or {}
        # Same envelope as the temp-path return below: only requested sections
        # appear (they're all in ``merged`` here), has_data from the summary
        # card. No temp_table_create mark — that's the guard's signature.
        return {
            "has_data": summary_card.get("has_data", False),
            "section_timings": section_timings,
            **merged,
            **runner.telemetry(),
        }

    _t = _time.perf_counter()
    if not runner.create_temp_table(create_sql, params):
        return {**empty_payload, **runner.telemetry()}
    timer.mark("temp_table_create", _t)

    # Per-branch helpers — each runs on its own runner so they can fan
    # out across connections via ``asyncio.to_thread``. ``SectionTimer``'s
    # entries list is just a ``list.append`` from each thread which is
    # atomic in CPython, so the timings interleave safely.
    #
    # Each helper returns a dict keyed by section name; the caller merges
    # those into the outer response. When the selector excludes a section,
    # both its read and its timer mark are suppressed — perf-harness
    # attribution would treat phantom zero-time marks as real reads.
    # A section runs its live from_temp helper only when it was REQUESTED and
    # MISSED the hoisted rollup phase. Rollup hits are already in ``merged``.
    def _run(name: str) -> bool:
        return _want(name) and name in missed

    def _branch_summary(r: QueryRunner) -> dict:
        if not _run("summary"):
            return {}
        _ts = _time.perf_counter()
        result = _origin_summary_from_temp(
            r,
            temp_table,
            actual_set,
            start_time=start_time,
            end_time=end_time,
            has_filters=has_filters,
        )
        timer.mark("summary", _ts)
        return {"summary": result}

    def _branch_slow_urls(r: QueryRunner) -> dict:
        if not _run("slow_urls"):
            return {}
        _ts = _time.perf_counter()
        result = _origin_slow_urls_from_temp(
            r,
            temp_table,
            actual_set,
            slow_urls_min_requests,
            slow_urls_limit,
            start_time=start_time,
            end_time=end_time,
            has_filters=has_filters,
        )
        timer.mark("slow_urls", _ts)
        return {"slow_urls": result}

    def _branch_ts_status_path(r: QueryRunner) -> dict:
        out: dict[str, Any] = {}
        if _run("timeseries"):
            _ts = _time.perf_counter()
            out["timeseries"] = _origin_timeseries_from_temp(
                r,
                temp_table,
                actual_set,
                bucket_minutes,
                split_by_leg,
                timeseries_metric,
                timeseries_percentile,
                start_time=start_time,
                end_time=end_time,
                has_filters=has_filters,
                # The rollup reader's active-hour live merge reads the BASE
                # view, not the temp. Pass the base table + the (time-only,
                # since unfiltered) inlined where_clause + params (empty under
                # inline_params=True). Mirrors dashboard.py's
                # try_time_series_from_rollup call shape.
                table_name=table_name,
                where_clause=where_clause,
                params=params,
            )
            timer.mark("timeseries", _ts)
        if _run("status_codes"):
            _ts = _time.perf_counter()
            out["status_codes"] = _origin_status_codes_from_temp(
                r,
                temp_table,
                actual_set,
                start_time=start_time,
                end_time=end_time,
                has_filters=has_filters,
            )
            timer.mark("status_codes", _ts)
        if _run("path_breakdown"):
            _ts = _time.perf_counter()
            out["path_breakdown"] = _origin_path_breakdown_from_temp(
                r,
                temp_table,
                actual_set,
                start_time=start_time,
                end_time=end_time,
                has_filters=has_filters,
            )
            timer.mark("path_breakdown", _ts)
        return out

    def _branch_pop_ip(r: QueryRunner) -> dict:
        out: dict[str, Any] = {}
        if _run("pop_latency"):
            _ts = _time.perf_counter()
            out["pop_latency"] = _origin_pop_latency_from_temp(
                r,
                temp_table,
                actual_set,
                pop_latency_limit,
                start_time=start_time,
                end_time=end_time,
                has_filters=has_filters,
            )
            timer.mark("pop_latency", _ts)
        if _run("ip_health"):
            _ts = _time.perf_counter()
            out["ip_health"] = _origin_ip_health_from_temp(
                r,
                temp_table,
                actual_set,
                ip_health_limit,
                start_time=start_time,
                end_time=end_time,
                has_filters=has_filters,
            )
            timer.mark("ip_health", _ts)
        return out

    # Per-branch occupancy — a branch runs (and acquires an extra conn) only
    # when it owns at least one MISSED requested section. Rollup-hit sections
    # are already merged, so they neither run a helper nor cost a pool checkout.
    # Branch 1 (primary runner) always uses ``runner`` so we never need an extra
    # conn for the summary branch. The remaining three are gated wholesale so a
    # single missed-card request only pays for one pool checkout instead of
    # three.
    branch_slow_active = _run("slow_urls")
    branch_ts_active = bool(_run("timeseries") or _run("status_codes") or _run("path_breakdown"))
    branch_pop_active = bool(_run("pop_latency") or _run("ip_health"))
    extras_needed = sum(int(b) for b in (branch_slow_active, branch_ts_active, branch_pop_active))

    try:
        # Acquire one extra pool conn per occupied non-primary branch so
        # they run in parallel via asyncio.gather. ``max_wait=0.2`` absorbs
        # brief contention but bails before the wait itself eats the
        # parallel-execution savings. On _PoolBusy or any other acquire
        # failure we roll back partial acquires and fall through to serial
        # on ``runner``. Pool size defaults to 8 per service
        # (DUCKDB_POOL_MAX_SIZE), so 2 concurrent origin requests fit
        # before fallback kicks in for the third.
        extra_cms: list = []
        extra_runners: list[QueryRunner] = []
        parallel = extras_needed > 0
        if parallel:
            try:
                for _ in range(extras_needed):
                    # skip_view_update=True: ctx.con was just validated by the
                    # request's primary checkout; the per-service iceberg view
                    # state can't rotate within this same-request window without
                    # being caught by QueryRunner.execute's stale-view retry.
                    # Saves ~200-400 ms × N extras on the rebind probe that
                    # _prepare_checkout would otherwise run.
                    cm = checkout_connection(src, max_wait=0.2, skip_view_update=True)
                    extra_cms.append(cm)
                    extra_runners.append(QueryRunner(cm.__enter__(), src))
            except _PoolBusy:
                parallel = False
            except Exception:
                parallel = False

        if not parallel:
            for cm in extra_cms:
                try:
                    cm.__exit__(None, None, None)
                except Exception:
                    pass
            extra_cms = []
            extra_runners = []

        # ``merged`` already holds the hoisted rollup hits — the live branches
        # below only ADD the missed sections (do NOT reinitialize it here, or
        # the rollup hits for this same request would be dropped).
        if parallel:
            try:
                # Build the gather list in the same order we consume
                # extra_runners — branch 1 (summary) always uses the
                # primary runner; other branches consume extras only when
                # they have work to do.
                tasks: list = [asyncio.to_thread(_branch_summary, runner)]
                next_extra = 0
                if branch_slow_active:
                    tasks.append(asyncio.to_thread(_branch_slow_urls, extra_runners[next_extra]))
                    next_extra += 1
                if branch_ts_active:
                    tasks.append(asyncio.to_thread(_branch_ts_status_path, extra_runners[next_extra]))
                    next_extra += 1
                if branch_pop_active:
                    tasks.append(asyncio.to_thread(_branch_pop_ip, extra_runners[next_extra]))
                    next_extra += 1
                # return_exceptions=True forces gather() to wait for ALL
                # to_thread workers to finish before returning, even if one
                # branch raises or the client cancels mid-flight. Without
                # it, a raising branch propagates immediately and the
                # ``finally`` below returns the extra conns to the pool
                # (errored=False) while the sibling branches' worker threads
                # are still executing DuckDB queries against them — and
                # DuckDB connections are not safe for concurrent use. A
                # subsequent checkout of such a conn would deadlock on the
                # internal mutex (leaked-active conns exhaust the pool → DoS)
                # or corrupt in-process DuckDB state. Mirrors the F015 fix in
                # backend/routers/dashboard.py (audit run 7ba15352).
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for part in results:
                    if isinstance(part, BaseException):
                        raise part
                    merged.update(part)
                # Fold the extra runners' debug telemetry into the primary
                # runner's so the response's ``_debug_queries`` /
                # ``_debug_calls`` envelope still surfaces every query the
                # harness needs to attribute. Without this fold the extra-
                # runner queries would be invisible to the harness and the
                # live-query monitor.
                for r in extra_runners:
                    runner.debug_queries.extend(r.debug_queries)
                    runner.debug_calls.extend(r.debug_calls)
                section_timings.append({"section": "origin:parallel", "time_ms": 0.0})
            finally:
                for cm in extra_cms:
                    try:
                        cm.__exit__(None, None, None)
                    except Exception:
                        pass
        else:
            merged.update(_branch_summary(runner))
            if branch_slow_active:
                merged.update(_branch_slow_urls(runner))
            if branch_ts_active:
                merged.update(_branch_ts_status_path(runner))
            if branch_pop_active:
                merged.update(_branch_pop_ip(runner))

        summary_card = merged.get("summary") or {}
        return {
            "has_data": summary_card.get("has_data", False),
            "section_timings": section_timings,
            **merged,
            **runner.telemetry(),
        }
    finally:
        try:
            runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        except Exception:
            pass


# R-1: register the response TTL cache so the autouse fixture in
# tests/conftest.py drains it via CacheRegistry.clear_all() — twin of
# network.py's _response_cache registration.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("origin._response_cache", _response_cache)
