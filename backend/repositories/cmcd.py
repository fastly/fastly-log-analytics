"""CMCD streaming analytics repository."""

from __future__ import annotations

import sys as _sys
import time as _time
from collections.abc import Callable
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc

OVERVIEW = """
    WITH base_metrics AS (
        SELECT COUNT(DISTINCT cmcd_sid) AS active_sessions, {rebuffer_rate_expr},
               {avg_bitrate_expr} AS avg_bitrate, {avg_buffer_length_expr} AS avg_buffer_length,
               {median_throughput_expr} AS median_throughput
        FROM {table} WHERE {where}
    ),
    peak_metrics AS (
        SELECT MAX(cnt) AS peak_viewers FROM (
            SELECT COUNT(DISTINCT cmcd_sid) AS cnt FROM {table} WHERE {where} GROUP BY {bucket_expr}
        )
    ),
    dur_metrics AS (
        SELECT ROUND(AVG(dur), 0) AS avg_dur FROM (
            SELECT EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
            FROM {table} WHERE {where} GROUP BY cmcd_sid HAVING dur > 0
        )
    )
    SELECT b.active_sessions, b.rebuffer_rate, b.avg_bitrate, b.avg_buffer_length,
           b.median_throughput, p.peak_viewers, d.avg_dur
    FROM base_metrics b CROSS JOIN peak_metrics p CROSS JOIN dur_metrics d
"""

TIMESERIES_COMBINED = "SELECT {select_cols} FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1"
NEW_SESSIONS = (
    "SELECT {first_bucket} AS bucket, COUNT(*) AS new_sessions FROM "
    "(SELECT cmcd_sid, MIN(timestamp) AS first_ts FROM {table} WHERE {where} GROUP BY cmcd_sid) sub GROUP BY 1"
)


def _cmcd_per_session_sql(dim_col: str, session_metrics: str, outer_cols: str, *, non_empty: bool = False) -> str:
    pred = f"{dim_col} IS NOT NULL AND {dim_col} != ''" if non_empty else f"{dim_col} IS NOT NULL"
    return (
        f"WITH per_session AS (SELECT {dim_col}, cmcd_sid, {session_metrics} FROM {{table}} "
        f"WHERE {{where}} AND {pred} GROUP BY 1, 2) "
        f"SELECT {dim_col}, {outer_cols} FROM per_session GROUP BY 1 ORDER BY session_count DESC LIMIT {{top_n}}"
    )


def _cmcd_dist_sql(col: str, count_expr: str) -> str:
    return (
        f"SELECT {col}, {count_expr} FROM {{table}} "
        f"WHERE {{where}} AND {col} IS NOT NULL AND {col} != '' GROUP BY 1 ORDER BY 2 DESC"
    )


_REB_RATE_SQL = "ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate"
_HAD_REB_SQL = "MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer"

TOP_CONTENT = _cmcd_per_session_sql(
    "cmcd_cid",
    "{rebuffer_agg} AS had_rebuffer, {br_agg} AS avg_br, {bl_agg} AS avg_bl",
    f"COUNT(*) AS session_count, {_REB_RATE_SQL}, AVG(avg_br) AS avg_br, AVG(avg_bl) AS avg_bl",
    non_empty=True,
)
REBUFFER_BY_COUNTRY = _cmcd_per_session_sql(
    "country",
    f"{_HAD_REB_SQL}, {{mtp_agg}} AS median_mtp",
    f"{_REB_RATE_SQL}, COUNT(*) AS session_count, AVG(median_mtp) AS median_mtp",
)
REBUFFER_BY_ASN = _cmcd_per_session_sql(
    "asn",
    f"{_HAD_REB_SQL}, {{br_agg}} AS avg_br, {{bl_agg}} AS avg_bl",
    f"{_REB_RATE_SQL}, COUNT(*) AS session_count, AVG(avg_br) AS avg_br, AVG(avg_bl) AS avg_bl",
)

DIST_BOTH = (
    "SELECT 'ot' AS kind, cmcd_ot AS val, COUNT(*) AS cnt FROM {table} "
    "WHERE {where} AND cmcd_ot IS NOT NULL AND cmcd_ot != '' GROUP BY 2 "
    "UNION ALL SELECT 'sf' AS kind, cmcd_sf AS val, COUNT(DISTINCT cmcd_sid) AS cnt FROM {table} "
    "WHERE {where} AND cmcd_sf IS NOT NULL AND cmcd_sf != '' GROUP BY 2 ORDER BY kind ASC, cnt DESC"
)
DIST_OT = _cmcd_dist_sql("cmcd_ot", "COUNT(*) AS request_count")
DIST_SF = _cmcd_dist_sql("cmcd_sf", "COUNT(DISTINCT cmcd_sid) AS session_count")

_DUR_BUCKETS = ((30, "<30s"), (120, "30s–2m"), (300, "2–5m"), (600, "5–10m"), (1800, "10–30m"), (3600, "30m–1h"))
SESSION_DURATION_DIST = (
    "WITH session_spans AS (SELECT cmcd_sid, EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur "
    "FROM {table} WHERE {where} GROUP BY cmcd_sid) SELECT CASE "
    + " ".join(f"WHEN dur < {lim} THEN '{lbl}'" for lim, lbl in _DUR_BUCKETS)
    + " ELSE '>1h' END AS duration_bucket, CASE "
    + " ".join(f"WHEN dur < {lim} THEN {i}" for i, (lim, _) in enumerate(_DUR_BUCKETS, 1))
    + " ELSE 7 END AS sort_order, COUNT(*) AS session_count FROM session_spans GROUP BY 1, 2 ORDER BY 2"
)

_sql: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.cmcd", _sys.modules[__name__])

# CMCD fields that must exist in the schema for any section to return data.
_CMCD_REQUIRED_COL = "cmcd_sid"

_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128

_memo = ResponseMemo(ttl=_RESPONSE_CACHE_TTL, maxsize=_RESPONSE_CACHE_MAXSIZE, prewarmer=False)
_response_cache = _memo.cache


def _cmcd_response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int,
    top_n: int,
    sections: set[str] | None,
) -> str:
    # Key field order is load-bearing (serialized as-is): s, e, f, bs, tn, sec.
    # mask_ips is intentionally omitted because CMCD aggregates return no IP columns.
    sec_val = sorted(list(sections)) if sections is not None else None
    payload = {
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "bs": bucket_seconds,
        "tn": top_n,
        "sec": sec_val,
    }
    return digest_cache_key(payload, src)


def get_cmcd_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
    top_n: int = 30,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> dict[str, Any]:
    """Return CMCD streaming analytics aggregates.

    Scans parquet once into a temp table, then runs all section queries
    against the materialized rows. The four time-series sections share a
    single GROUP BY scan to cut I/O further.
    """
    cache_key = _cmcd_response_cache_key(
        src=src,
        start_time=start_time,
        end_time=end_time,
        filters=filters,
        bucket_seconds=bucket_seconds,
        top_n=top_n,
        sections=sections,
    )
    if start_time is not None and end_time is not None:
        cached_fast = _memo.get(cache_key)
        if cached_fast is not None:
            return {**cached_fast, "section_timings": [], "debug_queries": [], "debug_calls": []}

    def _compute() -> dict[str, Any]:
        if start_time is not None and end_time is not None:
            cached_inner = _memo.get(cache_key)
            if cached_inner is not None:
                return {**cached_inner, "section_timings": [], "debug_queries": [], "debug_calls": []}
        return _compute_cmcd_aggregates(
            con=con,
            src=src,
            start_time=start_time,
            end_time=end_time,
            filters=filters,
            bucket_seconds=bucket_seconds,
            top_n=top_n,
            sections=sections,
            cache_key=cache_key,
        )

    if start_time is not None and end_time is not None:
        return _memo.coalesce(f"cmcd_agg:{cache_key}", _compute)
    result = _compute()
    if result.get("is_cached") is True:
        return result
    return _memo.result(result, state="miss")


def _compute_cmcd_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int,
    top_n: int,
    sections: set[str] | None,
    cache_key: str,
) -> dict[str, Any]:
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)

    if not actual_cols or _CMCD_REQUIRED_COL not in actual_cols:
        return {
            "available": _CMCD_REQUIRED_COL in actual_cols if actual_cols else False,
            "has_data": False,
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    cached = _memo.get(cache_key)
    if cached is not None:
        return {**cached, "section_timings": section_timings, **runner.telemetry()}

    table_name = _safe_table(src["name"])

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(
        start_time,
        end_time,
        filters,
        actual_cols,
        inline_params=True,
        partition_pruning=True,
    )
    timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)

    cmcd_where = f"{where_clause} AND cmcd_sid IS NOT NULL AND cmcd_sid != ''"

    bucket_ms = bucket_seconds * 1000

    has_country = "country" in actual_cols
    has_asn = "asn" in actual_cols
    has_bl = "cmcd_bl" in actual_cols
    has_br = "cmcd_br" in actual_cols
    has_bs = "cmcd_bs" in actual_cols
    has_mtp = "cmcd_mtp" in actual_cols
    has_ot = "cmcd_ot" in actual_cols
    has_sf = "cmcd_sf" in actual_cols
    has_su = "cmcd_su" in actual_cols
    has_tb = "cmcd_tb" in actual_cols
    has_rtp = "cmcd_rtp" in actual_cols
    has_cid = "cmcd_cid" in actual_cols
    has_dl = "cmcd_dl" in actual_cols
    del has_rtp, has_dl

    results: dict[str, Any] = {"available": True, **runner.telemetry()}

    # ── Temp table: scan parquet once ──────────────────────────────────────────
    temp_cols = ["timestamp", "cmcd_sid"]
    for col in (
        "cmcd_bl",
        "cmcd_br",
        "cmcd_bs",
        "cmcd_mtp",
        "cmcd_ot",
        "cmcd_sf",
        "cmcd_su",
        "cmcd_tb",
        "cmcd_rtp",
        "cmcd_cid",
        "cmcd_dl",
        "country",
        "asn",
    ):
        if col in actual_cols:
            temp_cols.append(col)

    _t = _time.perf_counter()
    with runner.temp_table(temp_cols, actual_cols, table_name, cmcd_where, params) as tmp:
        timer.mark(
            "temp_table_create",
            _t,
            source="live",
            activity="compute",
            current_execution=True,
            outcome="served" if tmp is not None else "failed",
        )
        if tmp is None:
            return {
                "available": False,
                "reason": "Data temporarily unavailable — view refresh failed. Retry in a moment.",
                **runner.telemetry(),
            }

        t = tmp
        w = "1=1"

        # ── Overview ──────────────────────────────────────────────────────
        if _want("overview"):
            _t = _time.perf_counter()
            sql = _sql.OVERVIEW.format(
                rebuffer_rate_expr=_rebuffer_rate_expr(has_bs),
                avg_bitrate_expr="AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL",
                avg_buffer_length_expr="AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL",
                median_throughput_expr=(
                    "APPROX_QUANTILE(cmcd_mtp, 0.5) FILTER (WHERE cmcd_ot = 'v')" if has_mtp and has_ot else "NULL"
                ),
                table=t,
                where=w,
                bucket_expr=_bucket_expr(bucket_ms),
            )
            row = runner.execute(sql).fetchone()
            if row:
                results["overview"] = {
                    "active_sessions": row[0] or 0,
                    "rebuffer_rate": _round_val(row[1]),
                    "avg_bitrate": _round_val(row[2]),
                    "avg_buffer_length": _round_val(row[3]),
                    "median_throughput": _round_val(row[4]),
                    "peak_viewers": row[5] or 0,
                    "avg_session_duration": int(row[6]) if row[6] else None,
                }
            timer.mark("overview", _t, source="live", activity="compute", current_execution=True)

        # ── Combined time-series scan ─────────────────────────────────────
        want_ts = {
            "sessions_ts": _want("sessions_ts"),
            "buffer_health_ts": _want("buffer_health_ts") and has_bl,
            "bitrate_ts": _want("bitrate_ts") and has_br,
            "throughput_ts": _want("throughput_ts") and has_mtp,
            "startup_ts": _want("startup_ts") and has_su,
        }
        if any(want_ts.values()):
            _t = _time.perf_counter()
            video_filter = "cmcd_ot = 'v'" if has_ot else "TRUE"
            select_parts = [f"{_bucket_expr(bucket_ms)} AS bucket"]
            col_map: dict[str, list[str]] = {}

            if want_ts["sessions_ts"]:
                select_parts.append("COUNT(DISTINCT cmcd_sid) AS concurrent_sessions")
                rebuffer_pct = (
                    "ROUND(COUNT(DISTINCT cmcd_sid) FILTER (WHERE cmcd_bs = true) * 100.0"
                    " / NULLIF(COUNT(DISTINCT cmcd_sid), 0), 2)"
                    if has_bs
                    else "NULL"
                )
                select_parts.append(f"{rebuffer_pct} AS rebuffer_session_pct")
                col_map["sessions_ts"] = ["concurrent_sessions", "rebuffer_session_pct"]

            if want_ts["buffer_health_ts"]:
                select_parts.append(f"APPROX_QUANTILE(cmcd_bl, 0.5) FILTER (WHERE {video_filter}) AS p50_buffer")
                select_parts.append(f"APPROX_QUANTILE(cmcd_bl, 0.95) FILTER (WHERE {video_filter}) AS p95_buffer")
                bs_rate = (
                    "ROUND(COUNT(*) FILTER (WHERE cmcd_bs = true) * 100.0 / NULLIF(COUNT(*), 0), 2)"
                    if has_bs
                    else "NULL"
                )
                select_parts.append(f"{bs_rate} AS starvation_rate")
                col_map["buffer_health_ts"] = ["p50_buffer", "p95_buffer", "starvation_rate"]

            if want_ts["bitrate_ts"]:
                select_parts.append(f"AVG(cmcd_br) FILTER (WHERE {video_filter}) AS avg_bitrate")
                util_expr = (
                    f"ROUND(AVG(cmcd_br * 1.0 / NULLIF(cmcd_tb, 0)) FILTER (WHERE {video_filter} AND cmcd_tb > 0), 4)"
                    if has_tb
                    else "NULL"
                )
                select_parts.append(f"{util_expr} AS utilization_ratio")
                col_map["bitrate_ts"] = ["avg_bitrate", "utilization_ratio"]

            if want_ts["throughput_ts"]:
                select_parts.append(f"APPROX_QUANTILE(cmcd_mtp, 0.5) FILTER (WHERE {video_filter}) AS p50_throughput")
                select_parts.append(f"APPROX_QUANTILE(cmcd_mtp, 0.95) FILTER (WHERE {video_filter}) AS p95_throughput")
                select_parts.append(f"APPROX_QUANTILE(cmcd_mtp, 0.99) FILTER (WHERE {video_filter}) AS p99_throughput")
                col_map["throughput_ts"] = ["p50_throughput", "p95_throughput", "p99_throughput"]

            if want_ts["startup_ts"]:
                select_parts.append(
                    "ROUND(COUNT(*) FILTER (WHERE cmcd_su = true) * 100.0 / NULLIF(COUNT(*), 0), 2) AS startup_ratio"
                )
                col_map["startup_ts"] = ["startup_ratio"]

            sql = _sql.TIMESERIES_COMBINED.format(select_cols=", ".join(select_parts), table=t, where=w)
            rows = runner.execute(sql).fetchall()
            desc = [d[0] for d in runner.con.description]
            col_idx = {name: i for i, name in enumerate(desc)}

            if len(rows) > 0:
                if want_ts["sessions_ts"]:
                    first_bucket = _bucket_expr(bucket_ms).replace("timestamp", "first_ts")
                    new_sess_sql = _sql.NEW_SESSIONS.format(first_bucket=first_bucket, table=t, where=w)
                    new_sess_rows = runner.execute(new_sess_sql).fetchall()
                    new_sess_map = {str(r[0]): r[1] for r in new_sess_rows}

                    sessions_ts_dict = {
                        str(r[col_idx["bucket"]]): {
                            "concurrent_sessions": r[col_idx["concurrent_sessions"]] or 0,
                            "rebuffer_session_pct": _round_val(r[col_idx["rebuffer_session_pct"]]),
                            "new_sessions": new_sess_map.get(str(r[col_idx["bucket"]]), 0),
                        }
                        for r in rows
                    }
                    results["sessions_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        sessions_ts_dict,
                        {"concurrent_sessions": 0, "rebuffer_session_pct": None, "new_sessions": 0},
                    )
                if want_ts["buffer_health_ts"]:
                    buffer_health_dict = {
                        str(r[col_idx["bucket"]]): {
                            "p50_buffer": _round_val(r[col_idx["p50_buffer"]]),
                            "p95_buffer": _round_val(r[col_idx["p95_buffer"]]),
                            "starvation_rate": _round_val(r[col_idx["starvation_rate"]]),
                        }
                        for r in rows
                    }
                    results["buffer_health_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        buffer_health_dict,
                        {"p50_buffer": None, "p95_buffer": None, "starvation_rate": None},
                    )
                if want_ts["bitrate_ts"]:
                    bitrate_dict = {
                        str(r[col_idx["bucket"]]): {
                            "avg_bitrate": _round_val(r[col_idx["avg_bitrate"]]),
                            "utilization_ratio": _round_val(r[col_idx["utilization_ratio"]]),
                        }
                        for r in rows
                    }
                    results["bitrate_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        bitrate_dict,
                        {"avg_bitrate": None, "utilization_ratio": None},
                    )
                if want_ts["throughput_ts"]:
                    throughput_dict = {
                        str(r[col_idx["bucket"]]): {
                            "p50": _round_val(r[col_idx["p50_throughput"]]),
                            "p95": _round_val(r[col_idx["p95_throughput"]]),
                            "p99": _round_val(r[col_idx["p99_throughput"]]),
                        }
                        for r in rows
                    }
                    results["throughput_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        throughput_dict,
                        {"p50": None, "p95": None, "p99": None},
                    )
                if want_ts["startup_ts"]:
                    startup_dict = {
                        str(r[col_idx["bucket"]]): {
                            "startup_ratio": _round_val(r[col_idx["startup_ratio"]]),
                        }
                        for r in rows
                    }
                    results["startup_ts"] = _pad_timeseries(
                        start_time,
                        end_time,
                        bucket_seconds,
                        startup_dict,
                        {"startup_ratio": None},
                    )
            else:
                if want_ts["sessions_ts"]:
                    results["sessions_ts"] = []
                if want_ts["buffer_health_ts"]:
                    results["buffer_health_ts"] = []
                if want_ts["bitrate_ts"]:
                    results["bitrate_ts"] = []
                if want_ts["throughput_ts"]:
                    results["throughput_ts"] = []
                if want_ts["startup_ts"]:
                    results["startup_ts"] = []
            timer.mark("timeseries_combined", _t, source="live", activity="compute", current_execution=True)

        # ── Top Content (single scan, no self-join) ───────────────────────
        if _want("top_content") and has_cid:
            _t = _time.perf_counter()
            br_agg = "AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"
            bl_agg = "AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"
            rebuffer_agg = "MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END)" if has_bs else "0"
            sql = _sql.TOP_CONTENT.format(
                rebuffer_agg=rebuffer_agg,
                br_agg=br_agg,
                bl_agg=bl_agg,
                table=t,
                where=w,
                top_n=top_n,
            )
            rows = runner.execute(sql).fetchall()
            results["top_content"] = [
                {
                    "content_id": r[0],
                    "session_count": r[1],
                    "rebuffer_rate": _round_val(r[2]),
                    "avg_bitrate": _round_val(r[3]),
                    "avg_buffer_length": _round_val(r[4]),
                }
                for r in rows
            ]
            timer.mark("top_content", _t, source="live", activity="compute", current_execution=True)

        # ── Rebuffer by Country (single scan, no self-join) ───────────────
        if _want("rebuffer_by_country") and has_country and has_bs:
            _t = _time.perf_counter()
            mtp_agg = "MEDIAN(cmcd_mtp) FILTER (WHERE cmcd_ot = 'v')" if has_mtp and has_ot else "NULL"
            sql = _sql.REBUFFER_BY_COUNTRY.format(
                mtp_agg=mtp_agg,
                table=t,
                where=w,
                top_n=top_n,
            )
            rows = runner.execute(sql).fetchall()
            results["rebuffer_by_country"] = [
                {
                    "country": r[0],
                    "rebuffer_rate": _round_val(r[1]),
                    "session_count": r[2],
                    "median_throughput": _round_val(r[3]),
                }
                for r in rows
            ]
            timer.mark("rebuffer_by_country", _t, source="live", activity="compute", current_execution=True)

        # ── Rebuffer by ASN (single scan, no self-join) ───────────────────
        if _want("rebuffer_by_asn") and has_asn and has_bs:
            _t = _time.perf_counter()
            br_agg = "AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"
            bl_agg = "AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"
            sql = _sql.REBUFFER_BY_ASN.format(
                br_agg=br_agg,
                bl_agg=bl_agg,
                table=t,
                where=w,
                top_n=top_n,
            )
            rows = runner.execute(sql).fetchall()
            from backend.core import duckdb as _db

            asn_list = [int(r[0]) for r in rows if str(r[0]).isdigit()]
            asn_names = _db.get_asn_names(src["name"], asn_list)
            results["rebuffer_by_asn"] = [
                {
                    "asn": r[0],
                    "label": _db.format_asn_label(int(r[0]), asn_names.get(int(r[0]), ""))
                    if str(r[0]).isdigit()
                    else str(r[0]),
                    "rebuffer_rate": _round_val(r[1]),
                    "session_count": r[2],
                    "avg_bitrate": _round_val(r[3]),
                    "avg_buffer_length": _round_val(r[4]),
                }
                for r in rows
            ]
            timer.mark("rebuffer_by_asn", _t, source="live", activity="compute", current_execution=True)

        # ── Object Type + Streaming Format (combined scan) ────────────────
        want_ot = _want("object_type_dist") and has_ot
        want_sf = _want("streaming_format_dist") and has_sf
        if want_ot or want_sf:
            _t = _time.perf_counter()
            if want_ot and want_sf:
                sql = _sql.DIST_BOTH.format(table=t, where=w)
                rows = runner.execute(sql).fetchall()
                results["object_type_dist"] = [
                    {"object_type": r[1], "request_count": r[2]} for r in rows if r[0] == "ot"
                ]
                results["streaming_format_dist"] = [
                    {"streaming_format": r[1], "session_count": r[2]} for r in rows if r[0] == "sf"
                ]
            elif want_ot:
                sql = _sql.DIST_OT.format(table=t, where=w)
                rows = runner.execute(sql).fetchall()
                results["object_type_dist"] = [{"object_type": r[0], "request_count": r[1]} for r in rows]
            elif want_sf:
                sql = _sql.DIST_SF.format(table=t, where=w)
                rows = runner.execute(sql).fetchall()
                results["streaming_format_dist"] = [{"streaming_format": r[0], "session_count": r[1]} for r in rows]
            timer.mark("distributions", _t, source="live", activity="compute", current_execution=True)

        # ── Session Duration Distribution ─────────────────────────────────────
        if _want("session_duration_dist"):
            _t = _time.perf_counter()
            sql = _sql.SESSION_DURATION_DIST.format(table=t, where=w)
            rows = runner.execute(sql).fetchall()
            results["session_duration_dist"] = [{"duration_bucket": r[0], "session_count": r[2]} for r in rows]
            timer.mark("session_duration_dist", _t, source="live", activity="compute", current_execution=True)

    results.update(runner.telemetry())
    results["section_timings"] = section_timings
    _memo.put(cache_key, results, strip=("section_timings",))
    return results


# ── Helpers ──────────────────────────────────────────────────────────────────


def _bucket_expr(bucket_ms: int) -> str:
    return f"EPOCH_MS(CAST((EPOCH_MS(timestamp)::BIGINT // {bucket_ms}) * {bucket_ms} AS BIGINT))::TIMESTAMP"


def _rebuffer_rate_expr(has_bs: bool) -> str:
    if not has_bs:
        return "NULL AS rebuffer_rate"
    return """ROUND(
        COUNT(DISTINCT cmcd_sid) FILTER (WHERE cmcd_bs = true) * 100.0
        / NULLIF(COUNT(DISTINCT cmcd_sid), 0), 2
    ) AS rebuffer_rate"""


def _round_val(v: Any) -> Any:
    if v is None:
        return None
    return round(float(v), 2)


def _pad_timeseries(
    start_time: str | None,
    end_time: str | None,
    bucket_seconds: int,
    db_rows_dict: dict[str, dict[str, Any]],
    default_vals: dict[str, Any],
) -> list[dict[str, Any]]:
    if not start_time or not end_time:
        return sorted([{"bucket": k, **v} for k, v in db_rows_dict.items()], key=lambda x: x["bucket"])

    import datetime
    from datetime import UTC

    st = parse_iso_utc(start_time)
    et = parse_iso_utc(end_time)
    if st is None or et is None:
        return sorted([{"bucket": k, **v} for k, v in db_rows_dict.items()], key=lambda x: x["bucket"])

    st_epoch = int(st.timestamp())
    st_aligned = st_epoch - (st_epoch % bucket_seconds)
    st_dt = datetime.datetime.fromtimestamp(st_aligned, tz=UTC)

    et_epoch = int(et.timestamp())
    et_aligned = et_epoch - (et_epoch % bucket_seconds)
    et_dt = datetime.datetime.fromtimestamp(et_aligned, tz=UTC)

    padded = []
    curr = st_dt
    while curr <= et_dt:
        bucket_str = curr.strftime("%Y-%m-%d %H:%M:%S")
        if bucket_str in db_rows_dict:
            padded.append({"bucket": bucket_str, **db_rows_dict[bucket_str]})
        else:
            padded.append({"bucket": bucket_str, **default_vals})
        curr += datetime.timedelta(seconds=bucket_seconds)
    return padded


_CacheRegistry.register("cmcd._response_cache", _response_cache)
_response_cache_key = _cmcd_response_cache_key
