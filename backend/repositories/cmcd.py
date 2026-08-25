"""CMCD (Common Media Client Data) repository — streaming QoE analytics."""

from __future__ import annotations

import time as _time
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    bucket_time_to_minute,
    cache_get,
    cache_put,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.utils.bounded_cache import BoundedTTLCache

# CMCD fields that must exist in the schema for any section to return data.
_CMCD_REQUIRED_COL = "cmcd_sid"

_RESPONSE_CACHE_TTL = 30.0
_RESPONSE_CACHE_MAXSIZE = 128
_response_cache: BoundedTTLCache = BoundedTTLCache(maxsize=_RESPONSE_CACHE_MAXSIZE, ttl_seconds=_RESPONSE_CACHE_TTL)


def _response_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int,
    top_n: int,
    sections: set[str] | None,
    mask_ips: bool,
) -> str:
    # Key field order is load-bearing (serialized as-is): s, e, f, bs, tn, sec, mi.
    sec_val = sorted(list(sections)) if sections is not None else None
    payload = {
        "s": bucket_time_to_minute(start_time),
        "e": bucket_time_to_minute(end_time),
        "f": serialize_filters_for_key(filters),
        "bs": bucket_seconds,
        "tn": top_n,
        "sec": sec_val,
        "mi": mask_ips,
    }
    return digest_cache_key(payload, src)


def get_cmcd_aggregates(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
    top_n: int = 30,
    sections: set[str] | None = None,
    mask_ips: bool = False,
    **kwargs,
) -> dict[str, Any]:
    """Return CMCD streaming analytics aggregates.

    Scans parquet once into a temp table, then runs all section queries
    against the materialized rows. The four time-series sections share a
    single GROUP BY scan to cut I/O further.
    """
    timer = SectionTimer()
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    runner = QueryRunner(con, src)

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("get_schema_cols", _t)

    if not actual_cols or _CMCD_REQUIRED_COL not in actual_cols:
        return {
            "available": _CMCD_REQUIRED_COL in actual_cols if actual_cols else False,
            "has_data": False,
            "section_timings": section_timings,
            **runner.telemetry(),
        }

    cache_key = _response_cache_key(
        src=src,
        start_time=start_time,
        end_time=end_time,
        filters=filters,
        bucket_seconds=bucket_seconds,
        top_n=top_n,
        sections=sections,
        mask_ips=mask_ips,
    )
    cached = cache_get(_response_cache, cache_key)
    if cached is not None:
        return {**cached, "section_timings": section_timings, **runner.telemetry()}

    table_name = _safe_table(src["name"])

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t)

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
        timer.mark("temp_table_create", _t)
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
            sql = f"""
                SELECT
                    COUNT(DISTINCT cmcd_sid) AS active_sessions,
                    {_rebuffer_rate_expr(has_bs)},
                    {"AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"} AS avg_bitrate,
                    {"AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"} AS avg_buffer_length,
                    {"APPROX_QUANTILE(cmcd_mtp, 0.5) FILTER (WHERE cmcd_ot = 'v')" if has_mtp and has_ot else "NULL"} AS median_throughput
                FROM {t} WHERE {w}
            """
            row = runner.execute(sql).fetchone()
            peak_sql = f"""
                SELECT MAX(cnt) FROM (
                    SELECT COUNT(DISTINCT cmcd_sid) AS cnt
                    FROM {t} WHERE {w}
                    GROUP BY {_bucket_expr(bucket_ms)}
                )
            """
            peak_row = runner.execute(peak_sql).fetchone()
            dur_sql = f"""
                SELECT ROUND(AVG(dur), 0) FROM (
                    SELECT EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
                    FROM {t} WHERE {w}
                    GROUP BY cmcd_sid
                    HAVING dur > 0
                )
            """
            dur_row = runner.execute(dur_sql).fetchone()
            if row:
                results["overview"] = {
                    "active_sessions": row[0] or 0,
                    "rebuffer_rate": _round_pct(row[1]),
                    "avg_bitrate": _round_val(row[2]),
                    "avg_buffer_length": _round_val(row[3]),
                    "median_throughput": _round_val(row[4]),
                    "peak_viewers": (peak_row[0] or 0) if peak_row else 0,
                    "avg_session_duration": int(dur_row[0]) if dur_row and dur_row[0] else None,
                }
            timer.mark("overview", _t)

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

            sql = f"SELECT {', '.join(select_parts)} FROM {t} WHERE {w} GROUP BY 1 ORDER BY 1"
            rows = runner.execute(sql).fetchall()
            desc = [d[0] for d in runner.con.description]
            col_idx = {name: i for i, name in enumerate(desc)}

            if len(rows) > 0:
                if want_ts["sessions_ts"]:
                    first_bucket = _bucket_expr(bucket_ms).replace("timestamp", "first_ts")
                    new_sess_sql = f"""
                        SELECT {first_bucket} AS bucket, COUNT(*) AS new_sessions
                        FROM (
                            SELECT cmcd_sid, MIN(timestamp) AS first_ts
                            FROM {t} WHERE {w} GROUP BY cmcd_sid
                        ) sub
                        GROUP BY 1
                    """
                    new_sess_rows = runner.execute(new_sess_sql).fetchall()
                    new_sess_map = {str(r[0]): r[1] for r in new_sess_rows}

                    sessions_ts_dict = {
                        str(r[col_idx["bucket"]]): {
                            "concurrent_sessions": r[col_idx["concurrent_sessions"]] or 0,
                            "rebuffer_session_pct": _round_pct(r[col_idx["rebuffer_session_pct"]]),
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
                            "starvation_rate": _round_pct(r[col_idx["starvation_rate"]]),
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
                            "startup_ratio": _round_pct(r[col_idx["startup_ratio"]]),
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
            timer.mark("timeseries_combined", _t)

        # ── Top Content (single scan, no self-join) ───────────────────────
        if _want("top_content") and has_cid:
            _t = _time.perf_counter()
            br_agg = "AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"
            bl_agg = "AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"
            rebuffer_agg = "MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END)" if has_bs else "0"
            sql = f"""
                WITH per_session AS (
                    SELECT cmcd_cid, cmcd_sid,
                        {rebuffer_agg} AS had_rebuffer,
                        {br_agg} AS avg_br,
                        {bl_agg} AS avg_bl
                    FROM {t}
                    WHERE {w} AND cmcd_cid IS NOT NULL AND cmcd_cid != ''
                    GROUP BY 1, 2
                )
                SELECT
                    cmcd_cid,
                    COUNT(*) AS session_count,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    AVG(avg_br) AS avg_br,
                    AVG(avg_bl) AS avg_bl
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """
            rows = runner.execute(sql).fetchall()
            results["top_content"] = [
                {
                    "content_id": r[0],
                    "session_count": r[1],
                    "rebuffer_rate": _round_pct(r[2]),
                    "avg_bitrate": _round_val(r[3]),
                    "avg_buffer_length": _round_val(r[4]),
                }
                for r in rows
            ]
            timer.mark("top_content", _t)

        # ── Rebuffer by Country (single scan, no self-join) ───────────────
        if _want("rebuffer_by_country") and has_country and has_bs:
            _t = _time.perf_counter()
            mtp_agg = "APPROX_QUANTILE(cmcd_mtp, 0.5) FILTER (WHERE cmcd_ot = 'v')" if has_mtp and has_ot else "NULL"
            sql = f"""
                WITH per_session AS (
                    SELECT country, cmcd_sid,
                        MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer,
                        {mtp_agg} AS median_mtp
                    FROM {t}
                    WHERE {w} AND country IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    country,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    COUNT(*) AS session_count,
                    AVG(median_mtp) AS median_mtp
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """
            rows = runner.execute(sql).fetchall()
            results["rebuffer_by_country"] = [
                {
                    "country": r[0],
                    "rebuffer_rate": _round_pct(r[1]),
                    "session_count": r[2],
                    "median_throughput": _round_val(r[3]),
                }
                for r in rows
            ]
            timer.mark("rebuffer_by_country", _t)

        # ── Rebuffer by ASN (single scan, no self-join) ───────────────────
        if _want("rebuffer_by_asn") and has_asn and has_bs:
            _t = _time.perf_counter()
            br_agg = "AVG(cmcd_br) FILTER (WHERE cmcd_ot = 'v')" if has_br and has_ot else "NULL"
            bl_agg = "AVG(cmcd_bl) FILTER (WHERE cmcd_ot = 'v')" if has_bl and has_ot else "NULL"
            sql = f"""
                WITH per_session AS (
                    SELECT asn, cmcd_sid,
                        MAX(CASE WHEN cmcd_bs = true THEN 1 ELSE 0 END) AS had_rebuffer,
                        {br_agg} AS avg_br,
                        {bl_agg} AS avg_bl
                    FROM {t}
                    WHERE {w} AND asn IS NOT NULL
                    GROUP BY 1, 2
                )
                SELECT
                    asn,
                    ROUND(SUM(had_rebuffer) * 100.0 / NULLIF(COUNT(*), 0), 2) AS rebuffer_rate,
                    COUNT(*) AS session_count,
                    AVG(avg_br) AS avg_br,
                    AVG(avg_bl) AS avg_bl
                FROM per_session
                GROUP BY 1
                ORDER BY session_count DESC
                LIMIT {top_n}
            """
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
                    "rebuffer_rate": _round_pct(r[1]),
                    "session_count": r[2],
                    "avg_bitrate": _round_val(r[3]),
                    "avg_buffer_length": _round_val(r[4]),
                }
                for r in rows
            ]
            timer.mark("rebuffer_by_asn", _t)

        # ── Object Type + Streaming Format (combined scan) ────────────────
        want_ot = _want("object_type_dist") and has_ot
        want_sf = _want("streaming_format_dist") and has_sf
        if want_ot or want_sf:
            _t = _time.perf_counter()
            if want_ot:
                sql = f"""
                    SELECT cmcd_ot, COUNT(*) AS request_count
                    FROM {t} WHERE {w} AND cmcd_ot IS NOT NULL AND cmcd_ot != ''
                    GROUP BY 1 ORDER BY 2 DESC
                """
                rows = runner.execute(sql).fetchall()
                results["object_type_dist"] = [{"object_type": r[0], "request_count": r[1]} for r in rows]

            if want_sf:
                sql = f"""
                    SELECT cmcd_sf, COUNT(DISTINCT cmcd_sid) AS session_count
                    FROM {t} WHERE {w} AND cmcd_sf IS NOT NULL AND cmcd_sf != ''
                    GROUP BY 1 ORDER BY 2 DESC
                """
                rows = runner.execute(sql).fetchall()
                results["streaming_format_dist"] = [{"streaming_format": r[0], "session_count": r[1]} for r in rows]
            timer.mark("distributions", _t)

        # ── Session Duration Distribution ─────────────────────────────────────
        if _want("session_duration_dist"):
            _t = _time.perf_counter()
            sql = f"""
                WITH session_spans AS (
                    SELECT cmcd_sid,
                        EPOCH(MAX(timestamp)) - EPOCH(MIN(timestamp)) AS dur
                    FROM {t} WHERE {w}
                    GROUP BY cmcd_sid
                )
                SELECT
                    CASE
                        WHEN dur < 30 THEN '<30s'
                        WHEN dur < 120 THEN '30s–2m'
                        WHEN dur < 300 THEN '2–5m'
                        WHEN dur < 600 THEN '5–10m'
                        WHEN dur < 1800 THEN '10–30m'
                        WHEN dur < 3600 THEN '30m–1h'
                        ELSE '>1h'
                    END AS duration_bucket,
                    CASE
                        WHEN dur < 30 THEN 1
                        WHEN dur < 120 THEN 2
                        WHEN dur < 300 THEN 3
                        WHEN dur < 600 THEN 4
                        WHEN dur < 1800 THEN 5
                        WHEN dur < 3600 THEN 6
                        ELSE 7
                    END AS sort_order,
                    COUNT(*) AS session_count
                FROM session_spans
                GROUP BY 1, 2
                ORDER BY 2
            """
            rows = runner.execute(sql).fetchall()
            results["session_duration_dist"] = [{"duration_bucket": r[0], "session_count": r[2]} for r in rows]
            timer.mark("session_duration_dist", _t)

    results["section_timings"] = section_timings
    cache_put(_response_cache, cache_key, results, strip=("section_timings",))
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


def _round_pct(v: Any) -> Any:
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

    import dateutil.parser

    try:
        st = dateutil.parser.isoparse(start_time)
        et = dateutil.parser.isoparse(end_time)
    except Exception:
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


from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cmcd._response_cache", _response_cache)
