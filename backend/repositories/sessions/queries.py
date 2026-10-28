"""Sessions CTE pipeline, response caching, get_sessions, and get_session_detail."""

from __future__ import annotations

import logging
import sys as _sys
import time
from datetime import UTC
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories.sessions.rollup_reader import (
    _build_asn_label_map,
    _get_sessions_from_rollup,
    _schema_cap_flags,
    _sessions_envelope,
    _shape_session_rows,
)
from backend.repositories.utils.filters import build_where_clause, calc_offset
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG,
    SESSIONS_DEFAULT_MIN_REQS_FLAG,
    SESSIONS_SORTABLE_COLUMNS,
    build_asn_label_map,
    empty_sessions_response,
    shape_sessions_envelope,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

# ── Sessions CTE pipeline ────────────────────────────────────────────────────

SESSIONS_CTE_PIPELINE = """
    WITH base AS (
        SELECT {group_key}
               {ua_proj}
               , timestamp AS ts
               {status_proj}
               {resp_bytes_proj}
               {rtt_proj}
               {asn_proj}
               {country_proj}
               {url_proj}
               {edge_proj}
               {edge_sid_proj}
               {cmcd_sid_proj}
        FROM {table_name}
        WHERE {where_clause} AND timestamp IS NOT NULL
    ),
    gaps AS (
        SELECT *,
               ts - LAG(ts) OVER (PARTITION BY {part_key} ORDER BY ts) AS gap
        FROM base
    ),
    marks AS (
        SELECT *,
               CASE WHEN gap IS NULL OR gap > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new
        FROM gaps
    ),
    sessions_raw AS (
        SELECT *,
               SUM(is_new) OVER (PARTITION BY {part_key} ORDER BY ts
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sid
        FROM marks
    ),
    sessions_agg AS (
        SELECT {group_key},
               MIN(ts) AS session_start,
               MAX(ts) AS session_end,
               COUNT(*) AS req_count
               {extra_aggs}
               , sid
        FROM sessions_raw
        GROUP BY {group_key}, sid
    )
"""


SESSIONS_PAGE_SELECT = """
    {cte_prefix}
    SELECT * FROM (
        SELECT *, ({flag_expr}) AS flagged
        FROM sessions_agg
    ) sub
    {flagged_filter}
    ORDER BY {sort_by} {sort_dir}
    LIMIT {limit} OFFSET {offset}
"""


SESSIONS_COUNT_WRAPPER = """
    {cte_prefix}
    SELECT COUNT(*) FROM (SELECT *, ({flag_expr}) AS flagged FROM sessions_agg) sub
    {flagged_filter}
"""

SQL: Any = _sys.modules[__name__]

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("sessions._response_cache", _response_cache)


def get_sessions(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    page: int,
    limit: int,
    sort_by: str,
    sort_dir: str,
    flagged_only: bool,
    min_reqs_flag: int | None,
    min_4xx_pct_flag: float | None,
    streaming_only: bool = False,
) -> dict:
    if min_reqs_flag is None:
        min_reqs_flag = SESSIONS_DEFAULT_MIN_REQS_FLAG
    if min_4xx_pct_flag is None:
        min_4xx_pct_flag = SESSIONS_DEFAULT_MIN_4XX_PCT_FLAG

    timer = SectionTimer()
    section_timings = timer.entries

    runner = QueryRunner(con, src)
    table_name = _safe_table(src["name"])
    offset = calc_offset(page, limit)

    if start_time and end_time:
        from backend.utils.date_utils import parse_iso_utc

        s = parse_iso_utc(str(start_time))
        e = parse_iso_utc(str(end_time))
        if s and e and (e - s).days > 7:
            raise ValueError("Sessions view is limited to 7 days. Please narrow your date range.")

    def _empty_resp(cols: set[str]) -> dict:
        return empty_sessions_response(
            page=page,
            limit=limit,
            **_schema_cap_flags(cols),
            **runner.telemetry(),
        )

    _t = time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    timer.mark("get_schema_cols", _t)
    if not actual_cols:
        return _empty_resp(set())

    _t = time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols), partition_pruning=True)
    timer.mark("build_where_clause", _t)

    has_partition_cols = "dt" in actual_cols or "timestamp_hour" in actual_cols
    cache_key: str | None = None
    if has_partition_cols and src.get("cache_dir"):
        cache_key = digest_cache_key(
            {
                "op": "sessions",
                "s": bucket_time_to_minute(start_time),
                "e": bucket_time_to_minute(end_time),
                "f": serialize_filters_for_key(filters),
                "p": page,
                "l": limit,
                "sb": sort_by,
                "sd": sort_dir,
                "fo": flagged_only,
                "mr": min_reqs_flag,
                "m4": min_4xx_pct_flag,
                "so": streaming_only,
            },
            src,
        )
        cached = _memo.get(cache_key)
        if cached is not None:
            return {**cached, "section_timings": section_timings, **runner.telemetry()}

    has_ja4 = "ja4" in actual_cols
    has_asn = "asn" in actual_cols
    has_country = "country" in actual_cols
    has_rtt = "tcp_rtt" in actual_cols
    has_status = "status" in actual_cols
    has_resp_bytes = "resp_bytes" in actual_cols
    has_ua = "ua" in actual_cols
    has_url = "url" in actual_cols
    has_edge = "edge" in actual_cols
    has_edge_sid = "edge_sid" in actual_cols
    has_cmcd = "cmcd_sid" in actual_cols

    # Sessions-rollup fast path for windows > 1h with rollup-compatible filters (country, asn).
    _ROLLUP_FILTERABLE = {"country", "asn"}
    if start_time and end_time and not streaming_only and all(k in _ROLLUP_FILTERABLE for k in filters):
        try:
            from backend.utils.date_utils import parse_iso_utc

            _st = parse_iso_utc(str(start_time))
            _et = parse_iso_utc(str(end_time))
        except (ValueError, TypeError):
            _st = _et = None
        if _st and _et and _et > _st:
            _t = time.perf_counter()
            rollup_result = _get_sessions_from_rollup(
                runner=runner,
                con=con,
                src=src,
                table_name=table_name,
                actual_cols=actual_cols,
                start_dt=_st,
                end_dt=_et,
                page=page,
                limit=limit,
                sort_by=sort_by,
                sort_dir=sort_dir,
                flagged_only=flagged_only,
                min_reqs_flag=min_reqs_flag,
                min_4xx_pct_flag=min_4xx_pct_flag,
                has_ja4=has_ja4,
                has_rtt=has_rtt,
                has_edge=has_edge,
                has_edge_sid=has_edge_sid,
                has_cmcd=has_cmcd,
                section_timings=section_timings,
                rollup_filters=filters,
            )
            timer.mark("sessions_rollup_attempt", _t)
            if rollup_result is not None:
                if cache_key is not None:
                    _memo.put(cache_key, rollup_result, strip=("section_timings",))
                return rollup_result

    group_cols = ["ip"] + (["ja4"] if has_ja4 else [])
    group_key = ", ".join(f'"{c}"' for c in group_cols)
    part_key = group_key

    # Representative cookie session id per (ip[, ja4]) session uses MAX("edge_sid")
    # so rows where the inbound request had no valid cookie ('') lose to real IDs.
    extra_aggs = "".join(
        frag
        for flag, frag in (
            (has_cmcd, """, COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != '') AS streaming_reqs"""),
            (has_edge_sid, ', MAX("edge_sid") AS edge_sid'),
            (
                has_edge,
                ', SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS edge_count'
                ', SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS shield_count',
            ),
            (has_asn, ', MIN("asn") AS asn'),
            (has_country, ', MIN("country") AS country'),
            (
                has_status,
                ', SUM(CASE WHEN "status" >= 400 AND "status" < 500 THEN 1 ELSE 0 END) AS reqs_4xx'
                ', SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS reqs_5xx',
            ),
            (has_resp_bytes, ', SUM("resp_bytes") AS total_bytes'),
            (has_rtt, ', MEDIAN("tcp_rtt") / 1000.0 AS median_rtt_ms'),
            (has_ua, ', MIN("ua") AS ua'),
            (has_url, ', COUNT(DISTINCT "url") AS unique_urls'),
        )
        if flag
    )

    flag_parts = [f"req_count >= {min_reqs_flag}"]
    if has_status:
        flag_parts.append(f"(reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= {min_4xx_pct_flag}")
    flag_expr = " OR ".join(f"({p})" for p in flag_parts)

    post_conditions: list[str] = []
    if flagged_only:
        post_conditions.append("flagged = true")
    if streaming_only and has_cmcd:
        post_conditions.append("streaming_reqs > 0")
    flagged_filter = ("WHERE " + " AND ".join(post_conditions)) if post_conditions else ""

    if sort_by not in SESSIONS_SORTABLE_COLUMNS:
        sort_by = "session_start"

    # Single CTE pipeline: filter → window functions → aggregation.
    # Replaces the item-19 three-stage TEMP TABLE approach now that
    # profiling identified sessions_raw materialization as the bottleneck
    # (~3000ms of ~3700ms total). DuckDB pipelines single-consumer CTEs
    # without intermediate materialization, saving the I/O overhead.
    cte_prefix = SQL.SESSIONS_CTE_PIPELINE.format(
        group_key=group_key,
        ua_proj=', "ua"' if has_ua else "",
        status_proj=', "status"' if has_status else "",
        resp_bytes_proj=', "resp_bytes"' if has_resp_bytes else "",
        rtt_proj=', "tcp_rtt"' if has_rtt else "",
        asn_proj=', "asn"' if has_asn else "",
        country_proj=', "country"' if has_country else "",
        url_proj=', "url"' if has_url else "",
        edge_proj=', "edge"' if has_edge else "",
        edge_sid_proj=', "edge_sid"' if has_edge_sid else "",
        cmcd_sid_proj=', "cmcd_sid"' if has_cmcd else "",
        table_name=table_name,
        where_clause=where_clause,
        part_key=part_key,
        extra_aggs=extra_aggs,
    )

    data_sql = SQL.SESSIONS_PAGE_SELECT.format(
        cte_prefix=cte_prefix,
        flag_expr=flag_expr,
        flagged_filter=flagged_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )
    _t = time.perf_counter()
    result = runner.execute_with_retry(data_sql, params)
    timer.mark("sessions_query", _t)
    if result is None:
        return _empty_resp(actual_cols)

    _t = time.perf_counter()
    rows = result.fetchall()
    timer.mark("fetchall", _t)
    col_names = [desc[0] for desc in con.description]

    sessions, total = _shape_session_rows(rows, col_names, from_rollup=False, has_cmcd=has_cmcd)

    if (not rows and offset > 0) or len(sessions) == limit:
        _t = time.perf_counter()
        count_sql = SQL.SESSIONS_COUNT_WRAPPER.format(
            cte_prefix=cte_prefix,
            flag_expr=flag_expr,
            flagged_filter=flagged_filter,
        )
        c_row = runner.execute(count_sql, params).fetchone()
        total = int(c_row[0]) if c_row and c_row[0] is not None else (offset + len(sessions))
        timer.mark("count_query", _t)
    elif rows and offset > 0:
        total = offset + len(sessions)

    res = _sessions_envelope(
        sessions,
        src,
        total,
        page,
        limit,
        actual_cols,
        min_reqs_flag,
        min_4xx_pct_flag,
        section_timings,
        runner.telemetry(),
    )
    if cache_key is not None:
        _memo.put(cache_key, res, strip=("section_timings",))
    return res


def get_session_detail(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    ip: str,
    session_start: str,
    session_end: str,
    ua: str | None = None,
    ja4: str | None = None,
) -> dict:
    runner = QueryRunner(con, src)
    table_name = _safe_table(src["name"])
    actual_cols = set(runner.get_schema_cols())
    has_partition_cols = "dt" in actual_cols or "timestamp_hour" in actual_cols

    cache_key: str | None = None
    if has_partition_cols and src.get("cache_dir"):
        cache_key = digest_cache_key(
            {
                "op": "detail",
                "ip": ip,
                "s": session_start,
                "e": session_end,
                "ua": ua,
                "ja4": ja4,
            },
            src,
        )
        cached = _memo.get(cache_key)
        if cached is not None:
            return {**cached, **runner.telemetry()}

    conditions = [
        '"ip" = ?',
        "timestamp >= CAST(? AS TIMESTAMPTZ)",
        "timestamp <= CAST(? AS TIMESTAMPTZ)",
    ]
    p: list[Any] = [ip, session_start, session_end]

    if has_partition_cols:
        try:
            from backend.utils.date_utils import parse_iso_utc

            s_dt = parse_iso_utc(str(session_start))
            e_dt = parse_iso_utc(str(session_end))
            if s_dt and e_dt:
                s_utc = s_dt.astimezone(UTC)
                e_utc = e_dt.astimezone(UTC)
                for col, fmt in (("dt", "%Y-%m-%d"), ("timestamp_hour", "%Y-%m-%d-%H")):
                    if col in actual_cols:
                        conditions.extend((f'"{col}" >= ?', f'"{col}" <= ?'))
                        p.extend((s_utc.strftime(fmt), e_utc.strftime(fmt)))
        except Exception:
            logging.getLogger(__name__).debug(
                "sessions.get_session_detail: partition bound parse failed", exc_info=True
            )

    if ua is not None:
        conditions.append('"ua" IS NOT DISTINCT FROM ?')
        p.append(ua)
    if ja4 is not None:
        conditions.append('"ja4" IS NOT DISTINCT FROM ?')
        p.append(ja4)

    where = " AND ".join(conditions)
    q = f"SELECT * FROM {table_name} WHERE {where} ORDER BY timestamp ASC LIMIT 500"
    rows = runner.execute(q, p).fetchall()
    col_names = [desc[0] for desc in con.description]
    records = [dict(zip(col_names, r)) for r in rows]
    for rec in records:
        for k, v in rec.items():
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()

    res = {"data": records, "columns": col_names, **runner.telemetry()}
    if cache_key is not None:
        _memo.put(cache_key, res)
    return res


__all__ = [
    "SESSIONS_CTE_PIPELINE",
    "SESSIONS_PAGE_SELECT",
    "SESSIONS_COUNT_WRAPPER",
    "SQL",
    "_memo",
    "_response_cache",
    "get_sessions",
    "get_session_detail",
    "_schema_cap_flags",
    "_shape_session_rows",
    "_sessions_envelope",
    "_build_asn_label_map",
    "build_asn_label_map",
    "empty_sessions_response",
    "shape_sessions_envelope",
]
