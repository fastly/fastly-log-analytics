"""Sessions repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import time
from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, _safe_table, empty_schema_response
from backend.repositories._sql import sessions as SQL
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.pagination import calc_offset


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
) -> dict:
    if min_reqs_flag is None:
        min_reqs_flag = 1000
    if min_4xx_pct_flag is None:
        min_4xx_pct_flag = 20.0

    # Per-phase timings surface in the response under _section_timings
    # so the perf harness can attribute wall time inside /api/sessions
    # without re-running ad-hoc instrumentation. Mirrors the pattern in
    # dashboard.py / bootstrap.py.
    section_timings: list[dict] = []

    def _phase(name: str, t0: float) -> None:
        section_timings.append({"section": name, "time_ms": round((time.perf_counter() - t0) * 1000, 2)})

    runner = QueryRunner(con, src)
    table_name = _safe_table(src["name"])
    offset = calc_offset(page, limit)

    _t = time.perf_counter()
    actual_cols = set(runner.get_schema_cols())
    _phase("get_schema_cols", _t)
    if not actual_cols:
        return empty_schema_response(
            sessions=[],
            total=0,
            page=page,
            limit=limit,
            has_rtt=False,
            has_ja4=False,
            has_edge=False,
            has_edge_sid=False,
            **runner.telemetry(),
        )

    # Max 7-day range guard
    if start_time and end_time:
        try:
            from backend.utils.date_utils import parse_iso_utc

            s = parse_iso_utc(str(start_time))
            e = parse_iso_utc(str(end_time))
            if s and e and (e - s).days > 7:
                raise ValueError("Sessions view is limited to 7 days. Please narrow your date range.")
        except ValueError:
            raise

    _t = time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols))
    _phase("build_where_clause", _t)

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

    group_cols = ["ip"]
    if has_ja4:
        group_cols.append("ja4")

    group_key = ", ".join(f'"{c}"' for c in group_cols)
    part_key = group_key

    extra_aggs = ""
    if has_edge_sid:
        # Representative cookie session id per (ip[, ja4]) session.
        # MAX() across rows in the same session ensures a stable value;
        # rows where the inbound request had no valid cookie store ''
        # (see backend/provision/session_scoring_orchestrator.py).
        extra_aggs += ', MAX("edge_sid") AS edge_sid'
    if has_edge:
        extra_aggs += ', SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS edge_count'
        extra_aggs += ', SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS shield_count'
    if has_asn:
        extra_aggs += ', MIN("asn") AS asn'
    if has_country:
        extra_aggs += ', MIN("country") AS country'
    if has_status:
        extra_aggs += (
            ', SUM(CASE WHEN "status" >= 400 AND "status" < 500 THEN 1 ELSE 0 END) AS reqs_4xx'
            ', SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS reqs_5xx'
        )
    if has_resp_bytes:
        extra_aggs += ', SUM("resp_bytes") AS total_bytes'
    if has_rtt:
        extra_aggs += ', MEDIAN("tcp_rtt") / 1000.0 AS median_rtt_ms'
    if has_ua:
        extra_aggs += ', MIN("ua") AS ua'
    if has_url:
        extra_aggs += ', COUNT(DISTINCT "url") AS unique_urls'

    flag_parts = [f"req_count >= {min_reqs_flag}"]
    if has_status:
        flag_parts.append(f"(reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= {min_4xx_pct_flag}")
    flag_expr = " OR ".join(f"({p})" for p in flag_parts)

    flagged_filter = "WHERE flagged = true" if flagged_only else ""

    valid_sorts = {
        "session_start",
        "session_end",
        "req_count",
        "edge_count",
        "shield_count",
        "unique_urls",
        "median_rtt_ms",
        "total_bytes",
    }
    if sort_by not in valid_sorts:
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
    _phase("sessions_query", _t)
    if result is None:
        return empty_schema_response(
            sessions=[],
            total=0,
            page=page,
            limit=limit,
            has_rtt=has_rtt,
            has_ja4=has_ja4,
            has_edge=has_edge,
            has_edge_sid=has_edge_sid,
            **runner.telemetry(),
        )

    _t = time.perf_counter()
    rows = result.fetchall()
    _phase("fetchall", _t)
    col_names = [desc[0] for desc in con.description]

    sessions: list[dict] = []
    for row in rows:
        d = dict(zip(col_names, row))
        for k in ("session_start", "session_end"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        sessions.append(d)
    total = len(sessions)

    if not rows and offset > 0:
        _t = time.perf_counter()
        count_sql = SQL.SESSIONS_COUNT_WRAPPER.format(
            cte_prefix=cte_prefix,
            flag_expr=flag_expr,
            flagged_filter=flagged_filter,
        )
        total = runner.execute(count_sql, params).fetchone()[0]
        _phase("count_query", _t)

    return {
        "sessions": sessions,
        "total": total,
        "page": page,
        "limit": limit,
        "has_rtt": has_rtt,
        "has_ja4": has_ja4,
        "has_edge": has_edge,
        "has_edge_sid": has_edge_sid,
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
        "section_timings": section_timings,
        **runner.telemetry(),
    }


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

    conditions = [
        '"ip" = ?',
        "timestamp >= CAST(? AS TIMESTAMPTZ)",
        "timestamp <= CAST(? AS TIMESTAMPTZ)",
    ]
    p: list[Any] = [ip, session_start, session_end]

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

    return {"data": records, "columns": col_names, **runner.telemetry()}
