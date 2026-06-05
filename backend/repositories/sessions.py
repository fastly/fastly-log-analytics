"""Sessions repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

from typing import Any

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, _safe_table
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

    runner = QueryRunner(con, src)
    table_name = _safe_table(src["name"])
    offset = calc_offset(page, limit)

    actual_cols = set(runner.get_schema_cols())
    if not actual_cols:
        from backend.repositories._base import empty_schema_response

        return empty_schema_response(
            sessions=[],
            total=0,
            page=page,
            limit=limit,
            has_rtt=False,
            has_ja4=False,
            has_edge=False,
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

    params, where_clause = build_where_clause(start_time, end_time, filters, list(actual_cols))

    has_ja4 = "ja4" in actual_cols
    has_asn = "asn" in actual_cols
    has_country = "country" in actual_cols
    has_rtt = "tcp_rtt" in actual_cols
    has_ttfb = "ttfb" in actual_cols
    has_status = "status" in actual_cols
    has_resp_bytes = "resp_bytes" in actual_cols
    has_ua = "ua" in actual_cols
    has_url = "url" in actual_cols
    has_edge = "edge" in actual_cols

    group_cols = ["ip"]
    if has_ja4:
        group_cols.append("ja4")

    group_key = ", ".join(f'"{c}"' for c in group_cols)
    part_key = group_key

    extra_aggs = ""
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

    sessions_cte = f"""
        WITH ordered AS (
            SELECT {group_key}
                   {', "ua"' if has_ua else ""}
                   {', "ja4"' if has_ja4 and "ja4" not in group_cols else ""}
                   , timestamp AS ts
                   {', "status"' if has_status else ""}
                   {', "resp_bytes"' if has_resp_bytes else ""}
                   {', "tcp_rtt"' if has_rtt else ""}
                   {', "ttfb"' if has_ttfb else ""}
                   {', "asn"' if has_asn else ""}
                   {', "country"' if has_country else ""}
                   {', "url"' if has_url else ""}
                   {', "edge"' if has_edge else ""}
            FROM {table_name}
            WHERE {where_clause} AND timestamp IS NOT NULL
        ),
        gaps AS (
            SELECT *,
                   ts - LAG(ts) OVER (PARTITION BY {part_key} ORDER BY ts) AS gap
            FROM ordered
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

    data_sql = f"""
        {sessions_cte}
        SELECT *, ({flag_expr}) AS flagged
        FROM sessions_agg
        {flagged_filter}
        ORDER BY {sort_by} {sort_dir}
        LIMIT {limit} OFFSET {offset}
    """
    rows = runner.execute(data_sql, params).fetchall()
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
        count_sql = f"""
            {sessions_cte}
            SELECT COUNT(*) FROM (SELECT ({flag_expr}) AS flagged FROM sessions_agg) sub
            {flagged_filter}
        """
        total = runner.execute(count_sql, params).fetchone()[0]

    return {
        "sessions": sessions,
        "total": total,
        "page": page,
        "limit": limit,
        "has_rtt": has_rtt,
        "has_ja4": has_ja4,
        "has_edge": has_edge,
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
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
