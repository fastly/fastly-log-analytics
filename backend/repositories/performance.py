"""Performance repository — latency analysis and origin/edge breakdown."""

from __future__ import annotations

import logging
import os
import sys as _sys
import time as _time
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
from backend.utils.bounded_cache import CacheRegistry

logger = logging.getLogger(__name__)

# ── SQL templates (folded from backend/repositories/_sql/performance.py) ────

TOP_URLS = f"""
    SELECT url,
           count(*) AS reqs,
           AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5, approx=True)} as p50,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95, approx=True)} as p95,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99, approx=True)} as p99
    FROM {{temp_table}}
    WHERE url IS NOT NULL AND elapsed IS NOT NULL
    GROUP BY url
    HAVING count(*) > 5
    ORDER BY {{sort_idx}} DESC LIMIT 20
"""

TOP_ASNS = f"""
    SELECT asn,
           count(*) AS reqs,
           AVG(CAST(elapsed AS DOUBLE)) / 1000.0 as avg,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.5, approx=True)} as p50,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.95, approx=True)} as p95,
           {percentile_ms_expr("CAST(elapsed AS DOUBLE)", 0.99, approx=True)} as p99
    FROM {{temp_table}}
    WHERE asn IS NOT NULL AND elapsed IS NOT NULL
    GROUP BY asn
    HAVING count(*) > 10
    ORDER BY {{sort_idx}} DESC LIMIT 20
"""

TTL_DIST = """
    SELECT
        CASE
            WHEN ttl <= 0 THEN '0s'
            WHEN ttl <= 10 THEN '<10s'
            WHEN ttl <= 30 THEN '<30s'
            WHEN ttl <= 60 THEN '<1m'
            WHEN ttl <= 300 THEN '<5m'
            WHEN ttl <= 600 THEN '<10m'
            WHEN ttl <= 1800 THEN '<30m'
            WHEN ttl <= 3600 THEN '<1h'
            WHEN ttl <= 10800 THEN '<3h'
            WHEN ttl <= 21600 THEN '<6h'
            WHEN ttl <= 43200 THEN '<12h'
            WHEN ttl <= 86400 THEN '<1d'
            WHEN ttl <= 259200 THEN '<3d'
            WHEN ttl <= 604800 THEN '<1w'
            WHEN ttl <= 1209600 THEN '<2w'
            WHEN ttl <= 2592000 THEN '<30d'
            WHEN ttl <= 7776000 THEN '<90d'
            WHEN ttl <= 31536000 THEN '<1y'
            ELSE '>1y'
        END as bucket,
        count(*) as count,
        min(ttl) as min_ttl
    FROM {temp_table}
    WHERE ttl IS NOT NULL
    GROUP BY 1 ORDER BY min_ttl
"""

SCATTER_WATERFALL = """
    WITH components AS (
        SELECT
            cache::VARCHAR AS cache,
            CAST(ttfb AS DOUBLE) * 1000.0 AS origin_ms,
            (CAST(elapsed AS DOUBLE) / 1000.0 - CAST(ttfb AS DOUBLE) * 1000.0) AS edge_ms,
            {ottfb_expr} AS origin_wait,
            GREATEST(0.0, {ottlb_expr} - {ottfb_expr}) AS origin_download,
            GREATEST(0.0, (CAST(ttfb AS DOUBLE) * 1000.0) - {ottfb_expr}) AS edge_processing,
            GREATEST(0.0, (CAST(elapsed AS DOUBLE) / 1000.0) - GREATEST({ottlb_expr}, CAST(ttfb AS DOUBLE) * 1000.0)) AS client_download
        FROM {source_rel}
        {comp_where}
    )
    SELECT
        's'::VARCHAR AS row_type,
        origin_ms,
        edge_ms,
        cache,
        NULL::DOUBLE AS w_edge_processing,
        NULL::DOUBLE AS w_origin_wait,
        NULL::DOUBLE AS w_origin_download,
        NULL::DOUBLE AS w_client_download
    FROM components
    WHERE edge_ms >= 0
    USING SAMPLE 300
    UNION ALL
    SELECT
        'w'::VARCHAR,
        NULL::DOUBLE,
        NULL::DOUBLE,
        NULL::VARCHAR,
        AVG(edge_processing),
        AVG(origin_wait),
        AVG(origin_download),
        AVG(client_download)
    FROM components
"""


def build_scatter_waterfall_sql(
    *,
    source_rel: str,
    comp_where: str,
    has_ottfb: bool,
    has_ottlb: bool,
) -> str:
    ottfb_expr = coalesce("CAST(ottfb AS DOUBLE) / 1000.0", "0") if has_ottfb else "0"
    ottlb_expr = coalesce("CAST(ottlb AS DOUBLE) / 1000.0", "0") if has_ottlb else "0"
    return SCATTER_WATERFALL.format(
        source_rel=source_rel,
        comp_where=comp_where,
        ottfb_expr=ottfb_expr,
        ottlb_expr=ottlb_expr,
    )


SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.performance", _sys.modules[__name__])

_PERF_CACHE_TTL = float(os.getenv("PERF_CACHE_TTL_SECONDS", "30"))
_memo = ResponseMemo(ttl=_PERF_CACHE_TTL, maxsize=128, prewarmer=False)
CacheRegistry.register("performance_aggregates_memo", _memo.cache)


def _perf_memo_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> str:
    payload = {
        "ep": "performance_aggregates",
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "sb": sort_by,
        "sec": sorted(sections) if sections is not None else None,
        "cd": str(src.get("_cache_dir_override", "")),
    }
    return digest_cache_key(payload, src)


def peek_performance_aggregates(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> dict | None:
    """Return a cached performance aggregates response without acquiring a DuckDB connection."""
    if _PERF_CACHE_TTL <= 0 or start_time is None or end_time is None:
        return None
    key = _perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
    cached = _memo.get_with_telemetry(key)
    return dict(cached) if cached is not None else None


# Temp-table projection order for the section-aware narrowed temp. Mirrors the
# origin/security ``_TEMP_COL_ORDER`` pattern: the temp is built for ONLY the
# columns the live (rollup-missed) sections touch, in a stable order. No
# ``timestamp`` — the window predicate is applied at temp-creation time and no
# performance section SELECTs timestamp from the temp.
_PERF_TEMP_COL_ORDER = ("url", "asn", "ttfb", "elapsed", "cache", "ttl", "ottfb", "ottlb")


def get_performance_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    sort_by: str = "p99",
    sections: set[str] | None = None,
) -> dict:
    cache_key: str | None = None
    if _PERF_CACHE_TTL > 0 and start_time is not None and end_time is not None:
        cache_key = _perf_memo_key(src, start_time, end_time, filters, sort_by, sections)
        cached = _memo.get_with_telemetry(cache_key)
        if cached is not None:
            return dict(cached)

    def _compute() -> dict:
        if cache_key is not None:
            cached_inner = _memo.get_with_telemetry(cache_key)
            if cached_inner is not None:
                return dict(cached_inner)
        return _compute_performance_aggregates(con, src, start_time, end_time, filters, sort_by, sections, cache_key)

    if cache_key is not None:
        return _memo.coalesce(f"perf_agg:{cache_key}", _compute)
    return _memo.result(_compute(), state="bypass")


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
    # Per-phase wall-clock timings surface in the response under
    # _section_timings so the perf harness can attribute /api/performance/
    # aggregates without ad-hoc instrumentation. Mirrors network.py.
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)
    if not actual_cols:
        return {
            "top_urls": [],
            "top_asns": [],
            "ttl_dist": [],
            "scatter": [],
            "waterfall": {},
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)

    results = {**runner.telemetry()}

    sort_idx_map = {"avg": 3, "p50": 4, "p95": 5, "p99": 6}
    sort_idx = sort_idx_map.get(sort_by, 6)

    _want_top_urls = _want("top_urls")
    _want_top_asns = _want("top_asns")
    _want_ttl = _want("ttl_dist")
    _want_waterfall = _want("waterfall")
    _want_scatter = _want("scatter")

    def _resolve_asns(asn_res: list) -> list[dict]:
        """Attach ASN labels to the (asn, reqs, avg, p50, p95, p99) tuples
        the rollup-hit and live paths both produce."""
        from backend.core import duckdb as _db

        labels_started = _time.perf_counter()
        asn_list = [int(r[0]) for r in asn_res if str(r[0]).isdigit()]
        asn_names = _db.get_asn_names(src["name"], asn_list)
        out: list[dict] = []
        for r in asn_res:
            asn_val = int(r[0]) if str(r[0]).isdigit() else r[0]
            label = (
                _db.format_asn_label(asn_val, asn_names.get(asn_val, "")) if isinstance(asn_val, int) else str(asn_val)
            )
            out.append(
                {"asn": asn_val, "label": label, "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]}
            )
        timer.mark("performance.asn_labels", labels_started, activity="overhead", current_execution=True)
        return out

    # ── Pass 1: serve every section that can come from a parquet rollup WITHOUT
    # the catalog temp, and record which sections still need a live scan plus
    # the exact temp columns they touch. The temp is then built for ONLY that
    # column set (or skipped entirely). The per-column form of the all-or-
    # nothing materialize the origin/aggregates fix removed — so a 30d
    # unfiltered request collapses ``temp_table_create`` to the scatter/
    # waterfall latency columns (or nothing when those aren't requested). ──
    needed_cols: set[str] = set()
    live_sections: set[str] = set()

    def _need(section: str, cols: set[str]) -> None:
        live_sections.add(section)
        needed_cols.update(c for c in cols if c in actual_cols)

    # 1. Top URLs by Latency — perf_latency rollup (unfiltered, >= 48 h);
    # per-URL elapsed percentiles request-weight-averaged across hours + re-
    # ranked by sort_by. Live fallback reads {url, elapsed} from the temp.
    _t = _time.perf_counter()
    if _want_top_urls and "url" in actual_cols and "elapsed" in actual_cols:
        rolled = runner.try_perf_latency_from_rollup(
            start_time,
            end_time,
            dimension="url",
            sort_by=sort_by,
            has_filters=bool(filters),
            min_requests=5,
            limit=20,
        )
        timer.mark(
            "top_urls_query_rollup",
            _t,
            source="rollup",
            activity="compute" if rolled is not None else "overhead",
            current_execution=True,
            outcome="served" if rolled is not None else "fallback",
        )
        if rolled is not None:
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
        else:
            _need("top_urls", {"url", "elapsed"})
    elif _want_top_urls:
        results["top_urls"] = []
        timer.mark("top_urls_query", _t, activity="skipped", current_execution=True)

    # 3. Top ASNs by Latency — same rollup; live fallback reads {asn, elapsed}.
    _t = _time.perf_counter()
    if _want_top_asns and "asn" in actual_cols and "elapsed" in actual_cols:
        rolled = runner.try_perf_latency_from_rollup(
            start_time,
            end_time,
            dimension="asn",
            sort_by=sort_by,
            has_filters=bool(filters),
            min_requests=10,
            limit=20,
        )
        timer.mark(
            "top_asns_query_rollup",
            _t,
            source="rollup",
            activity="compute" if rolled is not None else "overhead",
            current_execution=True,
            outcome="served" if rolled is not None else "fallback",
        )
        if rolled is not None:
            asn_res = [
                (r["value"], r["requests"], r["avg_ms"], r["p50_ms"], r["p95_ms"], r["p99_ms"]) for r in rolled["rows"]
            ]
            results["approx"] = True
            results["top_asns"] = _resolve_asns(asn_res)
        else:
            _need("top_asns", {"asn", "elapsed"})
    elif _want_top_asns:
        results["top_asns"] = []
        timer.mark("top_asns_query", _t, activity="skipped", current_execution=True)

    # 5. Cache TTL Distribution — EXACT histogram rollup (COUNT sums, MIN-of-MIN
    # composes; no _approx). Live fallback reads {ttl} from the temp.
    _t = _time.perf_counter()
    if _want_ttl and "ttl" in actual_cols:
        rolled_ttl = runner.try_perf_ttl_dist_from_rollup(start_time, end_time, has_filters=bool(filters))
        timer.mark(
            "ttl_dist_rollup",
            _t,
            source="rollup",
            activity="compute" if rolled_ttl is not None else "overhead",
            current_execution=True,
            outcome="served" if rolled_ttl is not None else "fallback",
        )
        if rolled_ttl is not None:
            results["ttl_dist"] = rolled_ttl
        else:
            _need("ttl_dist", {"ttl"})
    elif _want_ttl:
        results["ttl_dist"] = []
        timer.mark("ttl_dist_query", _t, activity="skipped", current_execution=True)

    # 6 + 7. Scatter + waterfall: always live. Scatter is a uniform 300-row
    # SAMPLE (not aggregatable) and waterfall is auto-paired with it
    # (_WATERFALL_SCATTER_PAIR). When top_urls/top_asns/ttl_dist are all
    # served from rollups (or not requested), skip materializing a full-window
    # temp table and query the base logs relation directly with partition_pruning.
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

    # ── Build the temp for ONLY the missed sections' columns, or skip it. ──
    temp_table = None
    if needed_cols:
        cols = [c for c in _PERF_TEMP_COL_ORDER if c in needed_cols]
        _t = _time.perf_counter()
        temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
        timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True)
        timer.mark("perf:temp_narrowed", _time.perf_counter(), activity="overhead", current_execution=True)
    else:
        timer.mark("perf:temp_skipped", _time.perf_counter(), activity="skipped", current_execution=True)

    # ── Pass 2: live SQL for the missed sections, against the narrowed temp. ──
    try:
        if "top_urls" in live_sections and temp_table:
            _t = _time.perf_counter()
            url_q = SQL.TOP_URLS.format(temp_table=temp_table, sort_idx=sort_idx)
            url_res = runner.execute(url_q).fetchall()
            results["top_urls"] = [
                {"url": r[0], "requests": r[1], "avg": r[2], "p50": r[3], "p95": r[4], "p99": r[5]} for r in url_res
            ]
            timer.mark("top_urls_query", _t, source="live", activity="compute", current_execution=True)

        if "top_asns" in live_sections and temp_table:
            _t = _time.perf_counter()
            asn_q = SQL.TOP_ASNS.format(temp_table=temp_table, sort_idx=sort_idx)
            asn_res = runner.execute(asn_q).fetchall()
            timer.mark("top_asns_query", _t, source="live", activity="compute", current_execution=True)
            results["top_asns"] = _resolve_asns(asn_res)

        if "ttl_dist" in live_sections and temp_table:
            _t = _time.perf_counter()
            ttl_q = SQL.TTL_DIST.format(temp_table=temp_table)
            ttl_res = runner.execute(ttl_q).fetchall()
            results["ttl_dist"] = [{"bucket": r[0], "count": r[1]} for r in ttl_res]
            timer.mark("ttl_dist_query", _t, source="live", activity="compute", current_execution=True)

        if "scatter_waterfall" in live_sections:
            # Scatter sample + waterfall averages from a single CTE pass.
            # When top_urls/top_asns/ttl_dist were served from rollups, temp_table
            # is None and we query the base logs relation directly with partition_pruning.
            _t = _time.perf_counter()
            if temp_table:
                source_rel = temp_table
                comp_where = "WHERE ttfb IS NOT NULL AND elapsed IS NOT NULL"
                q_params = None
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
            waterfall_avg = {
                "edge_processing": 0.0,
                "origin_wait": 0.0,
                "origin_download": 0.0,
                "client_download": 0.0,
            }
            for r in combined_rows:
                if r[0] == "s":
                    scatter.append({"origin": r[1], "edge": r[2], "cache": r[3]})
                else:  # 'w'
                    waterfall_avg = {
                        "edge_processing": float(r[4] or 0.0),
                        "origin_wait": float(r[5] or 0.0),
                        "origin_download": float(r[6] or 0.0),
                        "client_download": float(r[7] or 0.0),
                    }
            if _want_scatter:
                results["scatter"] = scatter
            if _want_waterfall:
                results["waterfall"] = {"avg": waterfall_avg}
            timer.mark("scatter_waterfall_query", _t, source="live", activity="compute", current_execution=True)
    finally:
        _drop_temp_table(runner, temp_table, "get_performance_aggregates")

    # Backstop: any requested section left unpopulated (temp-create failure, or
    # a live section whose required column was absent) gets its empty default so
    # the response shape stays stable regardless of which path served it.
    if _want_top_urls and "top_urls" not in results:
        results["top_urls"] = []
    if _want_top_asns and "top_asns" not in results:
        results["top_asns"] = []
    if _want_ttl and "ttl_dist" not in results:
        results["ttl_dist"] = []
    if _want_scatter and "scatter" not in results:
        results["scatter"] = []
    if _want_waterfall and "waterfall" not in results:
        results["waterfall"] = {}

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    if cache_key is not None:
        _memo.put(cache_key, dict(results))
    return results
