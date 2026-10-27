"""Performance metrics repository."""

from __future__ import annotations

import os
import sys as _sys
import time
from collections.abc import Callable
from typing import Any

import duckdb

from backend.core.metric_expressions import coalesce
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _drop_temp_table,
    _safe_table,
    percentile_ms_expr,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    PERF_SECTION_DEFAULTS,
    WATERFALL_KEYS,
    empty_performance_aggregates,
    shape_performance_asn_rows,
    shape_waterfall_avg,
)
from backend.utils.bounded_cache import CacheRegistry


def _top_latency_sql(col: str, min_reqs: int) -> str:
    el = "CAST(elapsed AS DOUBLE)"
    return (
        f"SELECT {col}, count(*) AS reqs, AVG({el}) / 1000.0 as avg, "
        f"{percentile_ms_expr(el, 0.5, approx=True)} as p50, "
        f"{percentile_ms_expr(el, 0.95, approx=True)} as p95, "
        f"{percentile_ms_expr(el, 0.99, approx=True)} as p99 "
        f"FROM {{temp_table}} WHERE {col} IS NOT NULL AND elapsed IS NOT NULL "
        f"GROUP BY {col} HAVING count(*) > {min_reqs} ORDER BY {{sort_idx}} DESC LIMIT 20"
    )


TOP_URLS = _top_latency_sql("url", 5)
TOP_ASNS = _top_latency_sql("asn", 10)

_TTL_BUCKETS = (
    (0, "0s"),
    (10, "<10s"),
    (30, "<30s"),
    (60, "<1m"),
    (300, "<5m"),
    (600, "<10m"),
    (1800, "<30m"),
    (3600, "<1h"),
    (10800, "<3h"),
    (21600, "<6h"),
    (43200, "<12h"),
    (86400, "<1d"),
    (259200, "<3d"),
    (604800, "<1w"),
    (1209600, "<2w"),
    (2592000, "<30d"),
    (7776000, "<90d"),
    (31536000, "<1y"),
)
TTL_DIST = (
    "SELECT CASE "
    + " ".join(f"WHEN ttl <= {lim} THEN '{lbl}'" for lim, lbl in _TTL_BUCKETS)
    + " ELSE '>1y' END as bucket, count(*) as count, min(ttl) as min_ttl "
    "FROM {temp_table} WHERE ttl IS NOT NULL GROUP BY 1 ORDER BY min_ttl"
)

SCATTER_WATERFALL = """
    WITH components AS (
        SELECT cache::VARCHAR AS cache, CAST(ttfb AS DOUBLE) * 1000.0 AS origin_ms,
               (CAST(elapsed AS DOUBLE) / 1000.0 - CAST(ttfb AS DOUBLE) * 1000.0) AS edge_ms,
               {ottfb_expr} AS origin_wait, GREATEST(0.0, {ottlb_expr} - {ottfb_expr}) AS origin_download,
               GREATEST(0.0, (CAST(ttfb AS DOUBLE) * 1000.0) - {ottfb_expr}) AS edge_processing,
               GREATEST(0.0, (CAST(elapsed AS DOUBLE) / 1000.0) - GREATEST({ottlb_expr}, CAST(ttfb AS DOUBLE) * 1000.0)) AS client_download
        FROM {source_rel} {comp_where}
    )
    SELECT 's'::VARCHAR AS row_type, origin_ms, edge_ms, cache,
           NULL::DOUBLE AS w_edge_processing, NULL::DOUBLE AS w_origin_wait,
           NULL::DOUBLE AS w_origin_download, NULL::DOUBLE AS w_client_download
    FROM components WHERE edge_ms >= 0 USING SAMPLE 300
    UNION ALL
    SELECT 'w'::VARCHAR, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR,
           AVG(edge_processing), AVG(origin_wait), AVG(origin_download), AVG(client_download)
    FROM components
"""


def build_scatter_waterfall_sql(*, source_rel: str, comp_where: str, has_ottfb: bool, has_ottlb: bool) -> str:
    ottfb_expr = coalesce("CAST(ottfb AS DOUBLE) / 1000.0", "0") if has_ottfb else "0"
    ottlb_expr = coalesce("CAST(ottlb AS DOUBLE) / 1000.0", "0") if has_ottlb else "0"
    return SCATTER_WATERFALL.format(
        source_rel=source_rel, comp_where=comp_where, ottfb_expr=ottfb_expr, ottlb_expr=ottlb_expr
    )


SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.performance", _sys.modules[__name__])

_PERF_CACHE_TTL = float(os.getenv("PERF_CACHE_TTL_SECONDS", "30"))
_perf_memo = ResponseMemo(ttl=_PERF_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("performance_aggregates_memo", _perf_memo.cache)


def _perf_memo_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> str:
    return digest_cache_key(
        {
            "ep": "performance_aggregates",
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": serialize_filters_for_key(filters),
            "sb": sort_by,
            "sec": sorted(sections) if sections is not None else None,
            "cd": str(src.get("_cache_dir_override", "")),
        },
        src,
    )


def _active_perf_memo_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> str | None:
    if _PERF_CACHE_TTL <= 0 or start_time is None or end_time is None:
        return None
    return _perf_memo_key(src, start_time, end_time, filters, sort_by, sections)


def peek_performance_aggregates(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> dict | None:
    key = _active_perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
    cached = _perf_memo.get_with_telemetry(key) if key is not None else None
    return dict(cached) if cached is not None else None


_PERF_TEMP_COL_ORDER = ("url", "asn", "ttfb", "elapsed", "cache", "ttl", "ottfb", "ottlb")
_PERF_SECTION_DEFAULTS: dict[str, Any] = PERF_SECTION_DEFAULTS


def get_performance_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> dict:
    cache_key = _active_perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
    if cache_key is not None and (cached := _perf_memo.get_with_telemetry(cache_key)) is not None:
        return dict(cached)

    def _compute() -> dict:
        if cache_key is not None and (cached_inner := _perf_memo.get_with_telemetry(cache_key)) is not None:
            return dict(cached_inner)
        return _compute_performance_aggregates(con, src, start_time, end_time, filters, sort_by, sections, cache_key)

    return (
        _perf_memo.coalesce(f"perf_agg:{cache_key}", _compute)
        if cache_key is not None
        else _perf_memo.result(_compute(), state="bypass")
    )


def _compute_performance_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    sort_by: str,
    sections: set[str] | None,
    cache_key: str | None,
) -> dict:
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    table_name = _safe_table(src["name"])
    runner = QueryRunner(con, src)
    _t = time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)
    if not actual_cols:
        return empty_performance_aggregates(section_timings=section_timings, **runner.telemetry())

    _t = time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)
    results = {**runner.telemetry()}
    sort_idx = {"avg": 3, "p50": 4, "p95": 5, "p99": 6}.get(sort_by, 6)

    def _resolve_asns(asn_res: list) -> list[dict]:
        labels_started = time.perf_counter()
        out = shape_performance_asn_rows(src["name"], asn_res)
        timer.mark("performance.asn_labels", labels_started, activity="overhead", current_execution=True)
        return out

    needed_cols: set[str] = set()
    live_sections: set[str] = set()

    def _need(section: str, cols: set[str]) -> None:
        live_sections.add(section)
        needed_cols.update(c for c in cols if c in actual_cols)

    def _try_rollup_section(
        sec: str,
        req_cols: set[str],
        rollup_mark: str,
        skip_mark: str,
        fetch_fn: Callable[[], Any],
        apply_fn: Callable[[Any], None],
    ) -> None:
        t0 = time.perf_counter()
        if not _want(sec):
            return
        if not all(c in actual_cols for c in req_cols):
            results[sec] = []
            timer.mark(skip_mark, t0, activity="skipped", current_execution=True)
            return
        rolled = fetch_fn()
        timer.mark(
            rollup_mark,
            t0,
            source="rollup",
            activity="compute" if rolled is not None else "overhead",
            current_execution=True,
            outcome="served" if rolled is not None else "fallback",
        )
        if rolled is not None:
            apply_fn(rolled)
        else:
            _need(sec, req_cols)

    def _apply_top_urls(rolled: dict[str, Any]) -> None:
        results["top_urls"] = [
            {
                "url": r["value"],
                "requests": r["requests"],
                "avg": r["avg_ms"],
                "p50": r["p50_ms"],
                "p95": r["p95_ms"],
                "p99": r["p99_ms"],
            }
            for r in rolled["rows"]
        ]
        results["approx"] = True

    def _apply_top_asns(rolled: dict[str, Any]) -> None:
        results["approx"] = True
        results["top_asns"] = _resolve_asns(
            [(r["value"], r["requests"], r["avg_ms"], r["p50_ms"], r["p95_ms"], r["p99_ms"]) for r in rolled["rows"]]
        )

    for sec, dim, min_reqs, apply_fn in (
        ("top_urls", "url", 5, _apply_top_urls),
        ("top_asns", "asn", 10, _apply_top_asns),
    ):

        def _fetch_latency(_dim: str = dim, _min: int = min_reqs) -> dict[str, Any] | None:
            return runner.try_perf_latency_from_rollup(
                start_time,
                end_time,
                dimension=_dim,
                sort_by=sort_by,
                has_filters=bool(filters),
                min_requests=_min,
                limit=20,
            )

        _try_rollup_section(
            sec,
            {dim, "elapsed"},
            f"{sec}_query_rollup",
            f"{sec}_query",
            _fetch_latency,
            apply_fn,
        )

    _try_rollup_section(
        "ttl_dist",
        {"ttl"},
        "ttl_dist_rollup",
        "ttl_dist_query",
        lambda: runner.try_perf_ttl_dist_from_rollup(start_time, end_time, has_filters=bool(filters)),
        lambda rolled_ttl: results.__setitem__("ttl_dist", rolled_ttl),
    )

    _want_scatter, _want_waterfall = _want("scatter"), _want("waterfall")
    _want_scatter_waterfall = _want_scatter or _want_waterfall
    if _want_scatter_waterfall and "ttfb" in actual_cols and "elapsed" in actual_cols:
        if live_sections:
            _need("scatter_waterfall", {"ttfb", "elapsed", "cache", "ottfb", "ottlb"})
        else:
            live_sections.add("scatter_waterfall")
    elif _want_scatter_waterfall:
        if _want_scatter:
            results["scatter"] = []
        if _want_waterfall:
            results["waterfall"] = {}

    temp_table = None
    if needed_cols:
        cols = [c for c in _PERF_TEMP_COL_ORDER if c in needed_cols]
        _t = time.perf_counter()
        temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
        timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True)
        timer.mark("perf:temp_narrowed", time.perf_counter(), activity="overhead", current_execution=True)
    else:
        timer.mark("perf:temp_skipped", time.perf_counter(), activity="skipped", current_execution=True)

    try:
        if "top_urls" in live_sections and temp_table:
            _t = time.perf_counter()
            url_res = runner.execute(SQL.TOP_URLS.format(temp_table=temp_table, sort_idx=sort_idx)).fetchall()
            results["top_urls"] = [
                {"url": r[0], "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]} for r in url_res
            ]
            timer.mark("top_urls_query", _t, source="live", activity="compute", current_execution=True)

        if "top_asns" in live_sections and temp_table:
            _t = time.perf_counter()
            asn_res = runner.execute(SQL.TOP_ASNS.format(temp_table=temp_table, sort_idx=sort_idx)).fetchall()
            timer.mark("top_asns_query", _t, source="live", activity="compute", current_execution=True)
            results["top_asns"] = _resolve_asns(asn_res)

        if "ttl_dist" in live_sections and temp_table:
            _t = time.perf_counter()
            ttl_res = runner.execute(SQL.TTL_DIST.format(temp_table=temp_table)).fetchall()
            results["ttl_dist"] = [{"bucket": r[0], "count": r[1]} for r in ttl_res]
            timer.mark("ttl_dist_query", _t, source="live", activity="compute", current_execution=True)

        if "scatter_waterfall" in live_sections:
            _t = time.perf_counter()
            if temp_table:
                source_rel, comp_where, q_params = temp_table, "WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL", None
            else:
                source_rel = f'"{table_name}"'
                comp_where = (
                    f"WHERE {where_clause} AND ttfb IS NOT NULL AND elapsed IS NOT NULL"
                    if where_clause.strip()
                    else "WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL"
                )
                q_params = params

            combined_q = SQL.build_scatter_waterfall_sql(
                source_rel=source_rel,
                comp_where=comp_where,
                has_ottfb="ottfb" in actual_cols,
                has_ottlb="ottlb" in actual_cols,
            )
            combined_rows = runner.execute(combined_q, q_params).fetchall()
            scatter: list[dict] = []
            waterfall_payload = shape_waterfall_avg(None)
            for r in combined_rows:
                if r[0] == "s":
                    scatter.append({"origin": r[1], "edge": r[2], "cache": r[3]})
                else:
                    waterfall_payload = shape_waterfall_avg({k: r[i + 4] for i, k in enumerate(WATERFALL_KEYS)})
            if _want_scatter:
                results["scatter"] = scatter
            if _want_waterfall:
                results["waterfall"] = waterfall_payload
            timer.mark("scatter_waterfall_query", _t, source="live", activity="compute", current_execution=True)
    finally:
        _drop_temp_table(runner, temp_table, "get_performance_aggregates")

    for sec, default in _PERF_SECTION_DEFAULTS.items():
        if _want(sec) and sec not in results:
            results[sec] = [] if isinstance(default, list) else {}

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    if cache_key is not None:
        _perf_memo.put(cache_key, dict(results))
    return results
