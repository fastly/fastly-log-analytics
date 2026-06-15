"""Origin metrics repository — fetch timing, error rates, IP health."""

from __future__ import annotations

import hashlib
import json
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
    cached = _response_cache.get(key)
    if cached is None:
        return None
    result = cached.copy()
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
    _response_cache[key] = sanitised


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
            anomaly_static=(
                efficiency is not None and efficiency > 3.0 and p50 is not None and p50 - light_rtt_ms >= 20.0
            ),
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

    # Combine the rollup-totals query AND the per-edge breakdown into ONE
    # scan using GROUPING SETS. DuckDB computes the () grouping (overall
    # totals) and the ("edge") grouping in a single pass, halving the
    # wall-clock — the previous two-scan shape did 138 ms + 132 ms = 270 ms
    # on prod 1 h windows; the combined scan does the same work in ~150 ms.
    #
    # When the schema has no ``edge`` column (rare — older services), fall
    # back to a single () grouping. GROUPING() requires a real column
    # reference, so we can't use it in the no-edge branch.
    has_edge = "edge" in actual_cols_set
    if has_edge:
        edge_select = '"edge"'
        grouping_clause = 'GROUP BY GROUPING SETS ((), ("edge"))'
        grouping_expr = 'GROUPING("edge")'
    else:
        edge_select = "NULL"
        grouping_clause = ""  # single rollup row, no need for GROUPING SETS
        grouping_expr = "1"  # always-rollup

    cur = runner.execute(
        SQL.SUMMARY_GROUPING_SETS.format(
            edge_select=edge_select,
            grouping_expr=grouping_expr,
            lat_val=lat_val,
            ottlb_p50=ottlb_p50,
            ottlb_p95=ottlb_p95,
            cdn_ovh=cdn_ovh,
            ost_5xx=ost_5xx,
            obytes_p50=obytes_p50,
            table=table,
            where=where,
            grouping_clause=grouping_clause,
        ),
        params,
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
    rollup_row = next((r for r in rows if r["is_total"] == 1), None)
    edge_rows = [r for r in rows if r["is_total"] == 0] if has_edge else []

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
            "by_leg": [],
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
        "by_leg": [
            {
                "edge": r["edge_group"],
                "requests": r["requests"],
                "p50_ms": r["ottfb_p50_ms"],
                "p95_ms": r["ottfb_p95_ms"],
            }
            for r in edge_rows
        ],
    }


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
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    rows = runner.execute(
        SQL.STATUS_CODES.format(table=table_name, where=where),
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
        return empty_schema_response(has_data=False, shielding_detected=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.PATH_BREAKDOWN.format(lat_val=lat_val, table=table_name, where=where),
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
        return empty_schema_response(has_data=False, requires_group_c=True, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.POP_LATENCY.format(lat_val=lat_val, table=table_name, where=where),
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
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    params, where = build_where_clause(start_time, end_time, filters, actual_cols)

    lat_val = origin_latency_us_expr(actual_cols)

    rows = runner.execute(
        SQL.IP_HEALTH.format(lat_val=lat_val, table=table_name, where=where),
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
        return empty_schema_response(has_data=False, rows=[], **runner.telemetry())

    # We need rid, prid, edge, pop, ottfb for this analysis
    required = {"rid", "prid", "edge", "pop", "ottfb"}
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


def _origin_summary_from_temp(runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str]) -> dict:
    """get_summary against the per-request TEMP TABLE.

    Uses the pre-computed ``lat_us`` column populated when the TEMP TABLE
    was created — saves the per-row COALESCE evaluation that turned the
    composite into a regression on local benchmarks. Otherwise byte-
    identical to :func:`get_summary`'s SQL via the shared
    :func:`_shape_summary` helper (the TEMP-specific templates
    ``TEMP_SUMMARY_ROLLUP`` / ``TEMP_SUMMARY_BY_EDGE`` were folded into
    ``SUMMARY_GROUPING_SETS`` per the b10 audit finding).
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
) -> dict:
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
        lat_expr = 'COALESCE("ottfb", "ttfb" * 1000000.0)'
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
) -> dict:
    actual_cols_set = set(actual_cols)
    if "url" not in actual_cols_set:
        return {"has_data": False, "rows": []}
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


def _origin_status_codes_from_temp(runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str]) -> dict:
    if "ost" not in set(actual_cols):
        return {"has_data": False, "rows": []}
    rows = runner.execute(SQL.STATUS_CODES.format(table=temp_table, where="1=1")).fetchall()
    if not rows:
        return {"has_data": False, "rows": []}
    return {
        "has_data": True,
        "rows": [{"status": r[0], "count": r[1], "pct": r[2]} for r in rows],
    }


def _origin_path_breakdown_from_temp(runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str]) -> dict:
    actual_cols_set = set(actual_cols)
    if "edge" not in actual_cols_set:
        return {"has_data": False, "shielding_detected": False, "rows": []}
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
    runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str], limit: int
) -> dict:
    actual_cols_set = set(actual_cols)
    if "pop" not in actual_cols_set:
        return {"has_data": False, "requires_group_c": True, "rows": []}
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
    runner: QueryRunner, temp_table: str, actual_cols: set[str] | list[str], limit: int
) -> dict:
    actual_cols_set = set(actual_cols)
    if "oip" not in actual_cols_set or "ost" not in actual_cols_set:
        return {"has_data": False, "rows": []}
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


def get_aggregates(
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
    slow_urls_min_requests: int = 10,
    ip_health_limit: int = 30,
    pop_latency_limit: int = 30,
) -> dict:
    """Composite origin endpoint — six origin cards from one parquet scan.

    Replaces the cold-load fan-out of /api/origin/{summary, timeseries,
    slow-urls, status-codes, path-breakdown, pop-latency, ip-health}
    (7643 ms total per the r2 audit) with a single CREATE TEMP TABLE
    + 6 reads against it. Shielding-analysis stays separate (item 13
    moves it to /api/network-health).
    """
    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()

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

    # Union of columns needed across the six sub-queries. Filtered to
    # those the schema actually has before materialization so missing
    # columns don't break the CREATE. Plus a precomputed `lat_us` column
    # — the percentile sub-queries all use the same COALESCE("ottfb",
    # "ttfb"*1000000.0) expression and computing it once at
    # materialization time lets DuckDB store it in the column-store
    # layout. Without the precompute, the in-memory TEMP TABLE was
    # SLOWER than per-endpoint parquet scans because the COALESCE
    # forces per-row evaluation during percentile sort.
    import uuid as _uuid

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
    import time as _time

    timer = SectionTimer()
    section_timings = timer.entries

    _t = _time.perf_counter()
    if not runner.create_temp_table(create_sql, params):
        return {**empty_payload, **runner.telemetry()}
    timer.mark("temp_table_create", _t)
    try:
        _t = _time.perf_counter()
        summary = _origin_summary_from_temp(runner, temp_table, actual_set)
        timer.mark("summary", _t)
        _t = _time.perf_counter()
        timeseries = _origin_timeseries_from_temp(
            runner,
            temp_table,
            actual_set,
            bucket_minutes,
            split_by_leg,
            timeseries_metric,
            timeseries_percentile,
        )
        timer.mark("timeseries", _t)
        _t = _time.perf_counter()
        slow_urls = _origin_slow_urls_from_temp(runner, temp_table, actual_set, slow_urls_min_requests, slow_urls_limit)
        timer.mark("slow_urls", _t)
        _t = _time.perf_counter()
        status_codes = _origin_status_codes_from_temp(runner, temp_table, actual_set)
        timer.mark("status_codes", _t)
        _t = _time.perf_counter()
        path_breakdown = _origin_path_breakdown_from_temp(runner, temp_table, actual_set)
        timer.mark("path_breakdown", _t)
        _t = _time.perf_counter()
        pop_latency = _origin_pop_latency_from_temp(runner, temp_table, actual_set, pop_latency_limit)
        timer.mark("pop_latency", _t)
        _t = _time.perf_counter()
        ip_health = _origin_ip_health_from_temp(runner, temp_table, actual_set, ip_health_limit)
        timer.mark("ip_health", _t)

        return {
            "has_data": summary.get("has_data", False),
            "summary": summary,
            "timeseries": timeseries,
            "slow_urls": slow_urls,
            "status_codes": status_codes,
            "path_breakdown": path_breakdown,
            "pop_latency": pop_latency,
            "ip_health": ip_health,
            "section_timings": section_timings,
            **runner.telemetry(),
        }
    finally:
        try:
            runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        except Exception:
            pass
