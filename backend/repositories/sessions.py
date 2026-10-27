"""Sessions repository — pure SQL functions, no HTTP imports."""

from __future__ import annotations

import logging
import os
import sys as _sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb
from fastapi import HTTPException

from backend.core.metric_expressions import sum_or_zero
from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner, SectionTimer, _safe_table
from backend.repositories.utils.filters import build_where_clause, calc_offset
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    bucket_time_to_minute,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    build_asn_label_map,
    empty_sessions_response,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.sql_validator import escape_sql_literal

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
_sys.modules.setdefault("backend.repositories._sql.sessions", _sys.modules[__name__])

_memo = ResponseMemo(ttl=30.0, maxsize=128)
_response_cache = _memo.cache
_CacheRegistry.register("sessions._response_cache", _response_cache)


def _collect_sessions_rollup_paths(src: dict, st: datetime, et: datetime) -> tuple[list[str], bool] | None:
    """Enumerate per-hour sessions.parquet files covering ``[st, et)``."""
    from backend.core.rollups import SESSIONS_BUNDLE_FILENAME, _hour_bundled_root
    from backend.repositories.rollup_coverage import collect_hourly_bundle_paths

    bundled_root = _hour_bundled_root(src)
    if not os.path.isdir(bundled_root):
        return None
    return collect_hourly_bundle_paths(src, st, et, bundled_root, SESSIONS_BUNDLE_FILENAME)


_SESSION_ACTIVE_HOUR_COLS = (
    "timestamp",
    "ip",
    "ja4",
    "country",
    "asn",
    "status",
    "resp_bytes",
    "tcp_rtt",
    "edge",
    "ua",
    "edge_sid",
    "cmcd_sid",
)


def _parse_rollup_hour_dt(path: str) -> datetime | None:
    """Extract the UTC hour datetime from a ``.../hour=YYYY-MM-DD-HH/sessions.parquet`` path."""
    parent = os.path.basename(os.path.dirname(path))
    for prefix in ("hour=", "dt="):
        if parent.startswith(prefix):
            from backend.utils.date_utils import parse_hour_key

            return parse_hour_key(parent[len(prefix) :])
    return None


def _build_direct_active_hour_session_sql(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    actual_cols: set,
    active_hour_dt: datetime,
    user_start: datetime,
    user_end: datetime,
) -> str | None:
    """Build active-hour session aggregation query directly from DuckLake and mtime-pruned buffer parquets."""
    from backend import config as svcconfig
    from backend.core.duckdb import _cache_dir

    if svcconfig.is_durable_serving_mode(src):
        return None

    try:
        cache_dir = _cache_dir(src)
    except Exception:
        return None

    live_start = max(active_hour_dt, user_start)
    live_end = min(active_hour_dt + timedelta(hours=1), user_end)
    start_iso = live_start.isoformat()
    end_iso = live_end.isoformat()

    buf_dir = os.path.join(cache_dir, "buffer")
    hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={live_start.strftime('%Y-%m-%d-%H')}")
    cutoff_epoch = live_start.timestamp() - 300.0

    try:
        from backend.core.storage.buffer import _tombstoned_parquet_paths

        tombstoned = _tombstoned_parquet_paths(buf_dir)
    except Exception:
        tombstoned = set()

    def _list_parquets(d: str, *, prune_mtime: bool) -> list[str]:
        out: list[str] = []
        try:
            with os.scandir(d) as it:
                for e in it:
                    if not e.name.endswith(".parquet") or e.name.startswith(".tmp_"):
                        continue
                    if e.path in tombstoned:
                        continue
                    if prune_mtime:
                        try:
                            if e.stat().st_mtime < cutoff_epoch:
                                continue
                        except OSError:
                            pass
                    out.append(e.path)
        except OSError:
            pass
        out.sort()
        return out

    buffer_files = _list_parquets(buf_dir, prune_mtime=True)

    lake_table: str | None = None
    try:
        from backend.core.storage._ducklake import ducklake_table_name

        lake_tname = ducklake_table_name(src)
        row = con.execute(
            "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1",
            [lake_tname],
        ).fetchone()
        if row:
            lake_table = f'lake."{lake_tname}"'
    except Exception:
        lake_table = None

    hourly_files = [] if lake_table else _list_parquets(hourly_dir, prune_mtime=False)

    if not lake_table and not buffer_files and not hourly_files:
        return None

    needed_cols = [c for c in _SESSION_ACTIVE_HOUR_COLS if c in actual_cols]
    if "timestamp" not in needed_cols or "ip" not in needed_cols:
        return None
    col_list = ", ".join(f'"{c}"' for c in needed_cols)
    where_cond = (
        f"timestamp >= TIMESTAMPTZ '{start_iso}' AND timestamp < TIMESTAMPTZ '{end_iso}' AND \"ip\" IS NOT NULL"
    )

    all_local_files = hourly_files + buffer_files
    raw_parts: list[str] = []
    if lake_table:
        raw_parts.append(f"SELECT {col_list} FROM {lake_table} WHERE {where_cond}")
    if all_local_files:
        file_list = ", ".join("'" + escape_sql_literal(f) + "'" for f in all_local_files)
        raw_parts.append(
            f"SELECT {col_list} FROM read_parquet([{file_list}], union_by_name=false, hive_partitioning=false) "
            f"WHERE {where_cond}"
        )

    raw_union = " UNION ALL BY NAME ".join(raw_parts)
    sql, _ = _build_active_hour_session_sql(f"({raw_union})", actual_cols, active_hour_dt, user_start, user_end)
    return sql


def _build_active_hour_session_sql(
    table_name: str,
    actual_cols: set,
    active_hour_dt: datetime,
    user_start: datetime,
    user_end: datetime,
) -> tuple[str, list]:
    """Build a SELECT that emits the same rollup-shaped columns as
    ``sessions.parquet`` for the slice
    ``[max(active_hour_start, user_start), min(active_hour_end, user_end))``.
    Used to UNION with rollup paths so the chart is current to the second.
    """

    def _col(c: str, expr: str, default: str = "CAST(0 AS BIGINT)") -> str:
        return expr if c in actual_cols else default

    ja4_expr = _col("ja4", '"ja4"', "CAST(NULL AS VARCHAR)")
    country_expr = _col("country", 'CAST(MIN("country") AS VARCHAR)', "CAST(NULL AS VARCHAR)")
    asn_expr = _col("asn", 'CAST(MIN("asn") AS INTEGER)', "CAST(NULL AS INTEGER)")
    reqs_4xx = _col("status", 'CAST(SUM(CASE WHEN "status" BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS BIGINT)')
    reqs_5xx = _col("status", 'CAST(SUM(CASE WHEN "status" >= 500 THEN 1 ELSE 0 END) AS BIGINT)')
    total_bytes = _col("resp_bytes", f"CAST({sum_or_zero('"resp_bytes"')} AS BIGINT)")
    rtt_sum = _col("tcp_rtt", f"CAST({sum_or_zero('"tcp_rtt"', zero='0.0')} AS DOUBLE)", "CAST(0.0 AS DOUBLE)")
    rtt_count = _col("tcp_rtt", 'CAST(COUNT(*) FILTER (WHERE "tcp_rtt" IS NOT NULL) AS BIGINT)')
    edge_cnt = _col("edge", 'CAST(SUM(CASE WHEN "edge" = 1 THEN 1 ELSE 0 END) AS BIGINT)')
    shield_cnt = _col("edge", 'CAST(SUM(CASE WHEN "edge" = 0 THEN 1 ELSE 0 END) AS BIGINT)')
    ua_min_expr = _col("ua", 'CAST(MIN("ua") AS VARCHAR)', "CAST(NULL AS VARCHAR)")
    edge_sid_expr = _col("edge_sid", 'CAST(MAX("edge_sid") AS VARCHAR)', "CAST(NULL AS VARCHAR)")
    cmcd_cnt = _col("cmcd_sid", 'CAST(COUNT(*) FILTER (WHERE "cmcd_sid" IS NOT NULL AND "cmcd_sid" != \'\') AS BIGINT)')

    live_start = max(active_hour_dt, user_start)
    live_end = min(active_hour_dt + timedelta(hours=1), user_end)
    sql = f"""
        SELECT
            time_bucket(INTERVAL '1 hour', timestamp) AS bucket,
            CAST("ip" AS VARCHAR) AS ip,
            CAST({ja4_expr} AS VARCHAR) AS ja4,
            MIN(timestamp) AS first_ts,
            MAX(timestamp) AS last_ts,
            CAST(COUNT(*) AS BIGINT) AS req_count,
            {country_expr} AS country,
            {asn_expr} AS asn,
            {reqs_4xx} AS reqs_4xx,
            {reqs_5xx} AS reqs_5xx,
            {total_bytes} AS total_bytes,
            {rtt_sum} AS rtt_sum,
            {rtt_count} AS rtt_count,
            {edge_cnt} AS edge_count,
            {shield_cnt} AS shield_count,
            {ua_min_expr} AS ua_min,
            {edge_sid_expr} AS edge_sid_max,
            {cmcd_cnt} AS cmcd_count
        FROM {table_name}
        WHERE timestamp >= TIMESTAMPTZ '{live_start.isoformat()}'
          AND timestamp <  TIMESTAMPTZ '{live_end.isoformat()}'
          AND "ip" IS NOT NULL
        GROUP BY 1, 2, 3
    """
    return sql, []


def _build_rollup_filter_sql(rollup_filters: FiltersDict | None) -> str:
    """Build a SQL WHERE clause fragment from rollup-compatible filter pills (country, asn)."""
    if not rollup_filters:
        return ""
    parts: list[str] = []
    for col, spec_raw in rollup_filters.items():
        if col not in ("country", "asn"):
            return ""
        spec: Any = spec_raw
        if isinstance(spec, dict):
            values = spec.get("values", [])
            mode = spec.get("mode", "include")
        else:
            values = spec.values
            mode = spec.mode
        if not values:
            continue
        op = "NOT IN" if mode == "exclude" else "IN"
        if col == "asn":
            int_literals: list[str] = []
            for v in values:
                try:
                    int_literals.append(str(int(v)))
                except (TypeError, ValueError):
                    continue
            if not int_literals:
                continue
            parts.append(f'"asn" {op} ({", ".join(int_literals)})')
        else:
            country_literals = ", ".join("'" + escape_sql_literal(str(v)) + "'" for v in values)
            parts.append(f'"country" {op} ({country_literals})')
    return (" AND " + " AND ".join(parts)) if parts else ""


def _schema_cap_flags(actual_cols: set[str]) -> dict[str, bool]:
    return {
        "has_rtt": "tcp_rtt" in actual_cols,
        "has_ja4": "ja4" in actual_cols,
        "has_edge": "edge" in actual_cols,
        "has_edge_sid": "edge_sid" in actual_cols,
        "has_cmcd": "cmcd_sid" in actual_cols,
    }


def _shape_session_rows(
    rows: list[tuple],
    col_names: list[str],
    *,
    from_rollup: bool,
    has_cmcd: bool,
) -> tuple[list[dict], int]:
    sessions: list[dict] = []
    total = 0
    for row in rows:
        d = dict(zip(col_names, row))
        if from_rollup:
            row_total = d.pop("total_sessions", None)
            if row_total is not None and total == 0:
                total = int(row_total)
        for k in ("session_start", "session_end"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        if from_rollup:
            d.pop("rtt_sum", None)
            d.pop("rtt_count", None)
            d["is_streaming"] = d.get("cmcd_count", 0) > 0
        elif has_cmcd:
            d["is_streaming"] = (d.pop("streaming_reqs", 0) or 0) > 0
        sessions.append(d)
    return sessions, max(total, len(sessions))


def _sessions_envelope(
    sessions: list[dict],
    src: dict,
    total: int,
    page: int,
    limit: int,
    actual_cols: set[str],
    min_reqs_flag: int,
    min_4xx_pct_flag: float,
    section_timings: list,
    telemetry: dict,
    *,
    rollup_served: bool = False,
) -> dict:
    res = {
        "sessions": sessions,
        "asn_names": _build_asn_label_map(sessions, src),
        "total": total,
        "page": page,
        "limit": limit,
        **_schema_cap_flags(actual_cols),
        "min_reqs_flag": min_reqs_flag,
        "min_4xx_pct_flag": min_4xx_pct_flag,
        "section_timings": section_timings,
    }
    if rollup_served:
        res["_rollup_served"] = True
    res.update(telemetry)
    return res


def _build_asn_label_map(sessions: list[dict], src: dict) -> dict[int, str]:
    """Return ``{asn_int: 'AS<n> Name'}`` for every ASN appearing in ``sessions``."""
    return build_asn_label_map(src["name"], (sess.get("asn") for sess in sessions if sess.get("asn") is not None))


def _get_sessions_from_rollup(
    runner: QueryRunner,
    con: duckdb.DuckDBPyConnection,
    src: dict,
    table_name: str,
    actual_cols: set,
    start_dt: datetime,
    end_dt: datetime,
    page: int,
    limit: int,
    sort_by: str,
    sort_dir: str,
    flagged_only: bool,
    min_reqs_flag: int,
    min_4xx_pct_flag: float,
    has_ja4: bool,
    has_rtt: bool,
    has_edge: bool,
    has_edge_sid: bool,
    has_cmcd: bool,
    section_timings: list,
    rollup_filters: FiltersDict | None = None,
) -> dict | None:
    """Rollup-served version of get_sessions for windows > 1h, or None on fallback."""
    if (end_dt - start_dt) <= timedelta(hours=1):
        return None

    _t = time.perf_counter()
    paths_result = _collect_sessions_rollup_paths(src, start_dt, end_dt)
    section_timings.append({"section": "rollup_paths_collect", "time_ms": round((time.perf_counter() - _t) * 1000, 2)})
    if paths_result is None:
        return None
    rollup_paths, crosses_active = paths_result
    if not rollup_paths and not crosses_active:
        return None

    active_hour_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) if crosses_active else None
    direct_live_sql: str | None = None
    fallback_live_sql: str | None = None
    if crosses_active and active_hour_dt is not None:
        direct_live_sql = _build_direct_active_hour_session_sql(con, src, actual_cols, active_hour_dt, start_dt, end_dt)
        fallback_live_sql, _ = _build_active_hour_session_sql(table_name, actual_cols, active_hour_dt, start_dt, end_dt)

    sort_col_sql = {
        "session_start": "session_start",
        "session_end": "session_end",
        "req_count": "req_count",
        "edge_count": "edge_count",
        "shield_count": "shield_count",
        "unique_urls": "session_start",
        "median_rtt_ms": "median_rtt_ms",
        "total_bytes": "total_bytes",
    }.get(sort_by, "session_start")
    sort_dir_sql = "DESC" if sort_dir.upper() == "DESC" else "ASC"
    offset = calc_offset(page, limit)
    rollup_filter_sql = _build_rollup_filter_sql(rollup_filters)

    extra_sort_agg = ""
    extra_sort_col = ""
    if sort_col_sql in ("total_bytes", "edge_count", "shield_count"):
        extra_sort_agg = f", SUM({sort_col_sql}) AS {sort_col_sql}"
        extra_sort_col = f", {sort_col_sql}"
    elif sort_col_sql == "median_rtt_ms":
        extra_sort_agg = (
            ", CASE WHEN SUM(rtt_count) > 0 THEN SUM(rtt_sum) / SUM(rtt_count) / 1000.0 ELSE NULL END AS median_rtt_ms"
        )
        extra_sort_col = ", median_rtt_ms"

    def _compose_stitch_sql(
        paths_subset: list[str],
        *,
        min_session_start: datetime | None = None,
        use_fast_sources: bool = True,
    ) -> str:
        union_parts: list[str] = []
        if paths_subset:
            paths_sql = ", ".join("'" + escape_sql_literal(p) + "'" for p in paths_subset)
            union_flag = "false" if use_fast_sources else "true"
            union_parts.append(
                f"SELECT bucket, ip, ja4, first_ts, last_ts, req_count, country, asn, "
                f"reqs_4xx, reqs_5xx, total_bytes, rtt_sum, rtt_count, edge_count, shield_count, "
                f"ua_min, edge_sid_max, COALESCE(cmcd_count, 0) AS cmcd_count "
                f"FROM read_parquet([{paths_sql}], union_by_name={union_flag})"
            )
        if crosses_active:
            live_part = direct_live_sql if use_fast_sources and direct_live_sql is not None else fallback_live_sql
            if live_part:
                union_parts.append(live_part)
        union_sql = " UNION ALL ".join(union_parts)

        where_clauses: list[str] = []
        if flagged_only:
            where_clauses.append("flagged = true")
        if min_session_start is not None:
            where_clauses.append(f"session_start >= TIMESTAMPTZ '{min_session_start.isoformat()}'")
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        return f"""
            WITH src AS ({union_sql}),
            filtered AS (
                SELECT * FROM src WHERE 1=1 {rollup_filter_sql}
            ),
            by_pair AS (
                SELECT ip,
                       ja4,
                       COUNT(*) AS row_cnt,
                       MIN(first_ts) AS session_start,
                       MAX(last_ts)  AS session_end,
                       SUM(req_count) AS req_count,
                       SUM(reqs_4xx) AS reqs_4xx
                       {extra_sort_agg}
                FROM filtered
                GROUP BY ip, ja4
            ),
            multi_rows AS (
                SELECT f.ip, f.ja4, f.first_ts, f.last_ts, f.req_count, f.reqs_4xx,
                       f.total_bytes, f.rtt_sum, f.rtt_count, f.edge_count, f.shield_count,
                       LAG(f.last_ts) OVER (PARTITION BY f.ip, f.ja4 ORDER BY f.first_ts) AS prev_last_ts
                FROM filtered f
                SEMI JOIN by_pair bp
                  ON f.ip = bp.ip AND f.ja4 IS NOT DISTINCT FROM bp.ja4 AND bp.row_cnt > 1
            ),
            multi_sids AS (
                SELECT *,
                       SUM(CASE WHEN prev_last_ts IS NULL
                                  OR (first_ts - prev_last_ts) > INTERVAL 30 MINUTES
                                THEN 1 ELSE 0 END)
                           OVER (PARTITION BY ip, ja4 ORDER BY first_ts
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sid
                FROM multi_rows
            ),
            multi_agg AS (
                SELECT ip,
                       ja4,
                       sid,
                       MIN(first_ts) AS session_start,
                       MAX(last_ts)  AS session_end,
                       SUM(req_count) AS req_count,
                       SUM(reqs_4xx) AS reqs_4xx
                       {extra_sort_agg}
                FROM multi_sids
                GROUP BY ip, ja4, sid
            ),
            all_skinny AS (
                SELECT ip, ja4, 1 AS sid, session_start, session_end, req_count, reqs_4xx {extra_sort_col}
                FROM by_pair
                WHERE row_cnt = 1
                UNION ALL
                SELECT ip, ja4, sid, session_start, session_end, req_count, reqs_4xx {extra_sort_col}
                FROM multi_agg
            ),
            skinny_flagged AS (
                SELECT *,
                       (req_count >= {min_reqs_flag}
                        OR (reqs_4xx * 100.0 / NULLIF(req_count, 0)) >= {min_4xx_pct_flag}) AS flagged
                FROM all_skinny
            ),
            top_sessions AS (
                SELECT ip, ja4, sid, session_start, session_end, flagged,
                       COUNT(*) OVER () AS total_sessions
                FROM skinny_flagged
                {where_sql}
                ORDER BY {sort_col_sql} {sort_dir_sql}
                LIMIT {limit} OFFSET {offset}
            ),
            agg AS (
                SELECT t.ip,
                       t.ja4,
                       t.session_start,
                       t.session_end,
                       SUM(f.req_count) AS req_count,
                       MIN(f.country)  AS country,
                       MIN(f.asn)      AS asn,
                       SUM(f.reqs_4xx) AS reqs_4xx,
                       SUM(f.reqs_5xx) AS reqs_5xx,
                       SUM(f.total_bytes) AS total_bytes,
                       SUM(f.rtt_sum)  AS rtt_sum,
                       SUM(f.rtt_count) AS rtt_count,
                       SUM(f.edge_count) AS edge_count,
                       SUM(f.shield_count) AS shield_count,
                       MIN(f.ua_min)   AS ua,
                       MAX(f.edge_sid_max) AS edge_sid,
                       SUM(f.cmcd_count) AS cmcd_count,
                       CASE WHEN SUM(f.rtt_count) > 0 THEN SUM(f.rtt_sum) / SUM(f.rtt_count) / 1000.0 ELSE NULL END AS median_rtt_ms,
                       CAST(NULL AS BIGINT) AS unique_urls,
                       t.flagged,
                       t.total_sessions
                FROM top_sessions t
                JOIN filtered f
                  ON f.ip = t.ip
                 AND f.ja4 IS NOT DISTINCT FROM t.ja4
                 AND f.first_ts >= t.session_start
                 AND f.last_ts <= t.session_end
                GROUP BY t.ip, t.ja4, t.sid, t.session_start, t.session_end, t.flagged, t.total_sessions
            )
            SELECT * FROM agg
            ORDER BY {sort_col_sql} {sort_dir_sql}
        """

    def _run_stitch_with_fallback(
        paths_subset: list[str],
        *,
        min_session_start: datetime | None = None,
    ) -> tuple[list[tuple], list[str]] | None:
        for use_fast in (True, False):
            sql = _compose_stitch_sql(
                paths_subset,
                min_session_start=min_session_start,
                use_fast_sources=use_fast,
            )
            try:
                res = runner.execute(sql, [])
                fetched = res.fetchall()
                cols = [desc[0] for desc in con.description]
                return fetched, cols
            except duckdb.Error as err:
                if not use_fast:
                    import logging as _logging

                    _logging.getLogger(__name__).warning("[sessions] rollup query failed, falling back: %s", err)
                    return None
        return None

    _t = time.perf_counter()
    exec_out: tuple[list[tuple], list[str]] | None = None
    if sort_col_sql == "session_start" and sort_dir_sql == "DESC" and offset == 0 and len(rollup_paths) > 3:
        cutoff_dt = _parse_rollup_hour_dt(rollup_paths[-2])
        prev_dt = _parse_rollup_hour_dt(rollup_paths[-3])
        if cutoff_dt is not None and prev_dt is not None and (cutoff_dt - prev_dt) == timedelta(hours=1):
            pruned_out = _run_stitch_with_fallback(rollup_paths[-3:], min_session_start=cutoff_dt)
            if pruned_out is not None and len(pruned_out[0]) >= limit:
                exec_out = pruned_out

    if exec_out is None:
        exec_out = _run_stitch_with_fallback(rollup_paths, min_session_start=None)

    if exec_out is None:
        section_timings.append(
            {"section": "sessions_rollup_failed", "time_ms": round((time.perf_counter() - _t) * 1000, 2)}
        )
        return None
    rows, col_names = exec_out
    section_timings.append({"section": "sessions_rollup_query", "time_ms": round((time.perf_counter() - _t) * 1000, 2)})

    sessions, total = _shape_session_rows(rows, col_names, from_rollup=True, has_cmcd=has_cmcd)
    return _sessions_envelope(
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
        rollup_served=True,
    )


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
        min_reqs_flag = 1000
    if min_4xx_pct_flag is None:
        min_4xx_pct_flag = 20.0

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


# ── Session-scoring analytics repository (folded from backend.repositories.session_scoring) ──

logger = logging.getLogger(__name__)

TOP_FLAGGED = """
        SELECT
            timestamp,
            edge_sid,
            edge_score,
            edge_score_l1,
            edge_score_l2,
            edge_cookie_compliance,
            edge_score_reason,
            ip,
            ua,
            url,
            status,
            country
        FROM {src}
        WHERE edge_score IS NOT NULL
          AND {ts_pred}
        ORDER BY edge_score DESC, timestamp DESC
        LIMIT {limit}
    """

SCORE_DISTRIBUTION = """
        SELECT
            date_trunc('hour', timestamp) AS hour,
            CASE
                WHEN edge_score < 25 THEN '0-25'
                WHEN edge_score < 50 THEN '25-50'
                WHEN edge_score < 75 THEN '50-75'
                ELSE '75-100'
            END AS bucket,
            COUNT(*) AS count
        FROM {src}
        WHERE edge_score IS NOT NULL
          AND {ts_pred}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

COMPLIANCE_BREAKDOWN = """
        SELECT
            date_trunc('hour', timestamp) AS hour,
            edge_cookie_compliance AS compliance,
            COUNT(*) AS count
        FROM {src}
        WHERE edge_cookie_compliance IS NOT NULL
          AND edge_cookie_compliance != ''
          AND {ts_pred}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

LATENCY_TIMESERIES = """
        SELECT
            {select_sql}
        FROM {src}
        WHERE (edge = true OR edge_score IS NOT NULL)
          AND {ts_pred}
        GROUP BY 1
        ORDER BY 1
    """


def latency_timeseries_sql(src: str, ts_pred: str, bucket: str, has_rtt: bool, has_exec: bool) -> str:
    """Per-bucket scored / fail-open counts plus optional RTT and Wasm-exec percentiles."""
    select_parts = [
        f"date_trunc('{bucket}', timestamp) AS hour",
        "COUNT(*) FILTER (WHERE edge_score IS NOT NULL) AS scored_count",
        "COUNT(*) FILTER (WHERE edge_score_reason ILIKE '%compute-unavailable%') AS fail_open_count",
        "COUNT(*) AS total_count",
    ]
    if has_rtt:
        select_parts += [
            "CAST(quantile_cont(edge_score_rtt_us, 0.5)  FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p50_us",
            "CAST(quantile_cont(edge_score_rtt_us, 0.95) FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p95_us",
            "CAST(quantile_cont(edge_score_rtt_us, 0.99) FILTER (WHERE edge_score_rtt_us IS NOT NULL) AS BIGINT) AS rtt_p99_us",
        ]
    if has_exec:
        select_parts += [
            "CAST(quantile_cont(edge_score_exec_us, 0.5)  FILTER (WHERE edge_score_exec_us IS NOT NULL) AS BIGINT) AS exec_p50_us",
            "CAST(quantile_cont(edge_score_exec_us, 0.95) FILTER (WHERE edge_score_exec_us IS NOT NULL) AS BIGINT) AS exec_p95_us",
        ]
    select_sql = ",\n            ".join(select_parts)
    return LATENCY_TIMESERIES.format(select_sql=select_sql, src=src, ts_pred=ts_pred)


HEALTH_SNAPSHOT = """
        WITH edge_rows AS (
            SELECT
                edge_score,
                edge_score_l2,
                edge_score_reason,
                edge_cookie_compliance,
                edge_sid{rtt_cte}{exec_cte}
            FROM {src}
            WHERE (edge = true OR edge_score IS NOT NULL)
               AND {ts_pred}
        ),
        scored AS (
            SELECT * FROM edge_rows WHERE edge_score IS NOT NULL
        ),
        reason_rows AS (
            SELECT trim(reason) AS reason
            FROM (
                SELECT unnest(string_split(edge_score_reason, ',')) AS reason
                FROM scored
                WHERE edge_score_reason IS NOT NULL AND edge_score_reason != ''
            )
            WHERE trim(reason) != ''
        ),
        top_reasons AS (
            SELECT reason, COUNT(*) AS n
            FROM reason_rows
            GROUP BY reason
            ORDER BY n DESC
            LIMIT 10
        ),
        fail_open_reasons AS (
            SELECT reason, COUNT(*) AS n
            FROM reason_rows
            WHERE reason ILIKE 'compute-unavailable%'
               OR reason ILIKE 'internal-error%'
            GROUP BY reason
            ORDER BY n DESC
        )
        SELECT
            (SELECT COUNT(*) FROM edge_rows)                                    AS total_edge_rows,
            (SELECT COUNT(*) FROM scored)                                       AS scored_rows,
            (SELECT COUNT(DISTINCT edge_sid) FROM scored WHERE edge_sid <> '') AS distinct_sids,
            (SELECT AVG(edge_score) FROM scored)                                AS avg_score,
            (SELECT quantile_cont(edge_score, 0.5) FROM scored)                 AS p50_score,
            (SELECT quantile_cont(edge_score, 0.95) FROM scored)                AS p95_score,
            (SELECT MAX(edge_score) FROM scored)                                AS max_score,
            (SELECT COUNT(*) FROM scored
              WHERE edge_score_reason ILIKE '%compute-unavailable%')            AS scorer_errors,
            (SELECT list({{'reason': reason, 'count': n}}) FROM top_reasons)    AS top_reasons,
            (SELECT list({{'reason': reason, 'count': n}}) FROM fail_open_reasons) AS fail_open_breakdown,
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 IS NOT NULL)       AS l2_evaluated,
            (SELECT COUNT(*) FROM scored WHERE edge_score_l2 >= 50)             AS l2_high_count{latency_select}
    """


def health_snapshot_sql(src: str, ts_pred: str, has_rtt: bool, has_exec: bool) -> str:
    """One-row scoring health aggregate; latency columns are optional (older services lack them)."""
    rtt_cte = ",\n                edge_score_rtt_us" if has_rtt else ""
    exec_cte = ",\n                edge_score_exec_us" if has_exec else ""
    lat_parts: list[str] = []
    if has_rtt:
        lat_parts += [
            "(SELECT quantile_cont(edge_score_rtt_us, 0.5)  FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p50_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.95) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p95_us",
            "(SELECT quantile_cont(edge_score_rtt_us, 0.99) FROM edge_rows WHERE edge_score_rtt_us IS NOT NULL) AS rtt_p99_us",
            "(SELECT MAX(edge_score_rtt_us) FROM edge_rows)                                                     AS rtt_max_us",
        ]
    if has_exec:
        lat_parts += [
            "(SELECT quantile_cont(edge_score_exec_us, 0.5)  FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p50_us",
            "(SELECT quantile_cont(edge_score_exec_us, 0.95) FROM scored WHERE edge_score_exec_us IS NOT NULL) AS exec_p95_us",
        ]
    latency_select = (",\n            " + ",\n            ".join(lat_parts)) if lat_parts else ""
    return HEALTH_SNAPSHOT.format(
        rtt_cte=rtt_cte, exec_cte=exec_cte, src=src, ts_pred=ts_pred, latency_select=latency_select
    )


THRESHOLD_PREVIEW_LABELED = """
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
                    WHERE edge_score IS NOT NULL
                      AND edge_sid IS NOT NULL
                      AND edge_sid <> ''
                      AND {ts_pred}
                    GROUP BY edge_sid
                ),
                labels(edge_sid, label) AS (
                    VALUES {values_placeholders}
                ),
                joined AS (
                    SELECT s.max_score, l.label
                    FROM sid_scores s
                    LEFT JOIN labels l USING (edge_sid)
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN max_score >= {threshold} THEN 1 ELSE 0 END) AS flagged_total,
                    SUM(CASE WHEN max_score >= {threshold} AND label = 'good' THEN 1 ELSE 0 END) AS flagged_good,
                    SUM(CASE WHEN max_score >= {threshold} AND label = 'bad'  THEN 1 ELSE 0 END) AS flagged_bad,
                    SUM(CASE WHEN max_score <  {threshold} AND label = 'good' THEN 1 ELSE 0 END) AS passed_good,
                    SUM(CASE WHEN max_score <  {threshold} AND label = 'bad'  THEN 1 ELSE 0 END) AS passed_bad
                FROM joined
            """

THRESHOLD_PREVIEW_UNLABELED = """
                WITH sid_scores AS (
                    SELECT edge_sid, MAX(edge_score) AS max_score
                    FROM {src}
                    WHERE edge_score IS NOT NULL
                      AND edge_sid IS NOT NULL
                      AND edge_sid <> ''
                      AND {ts_pred}
                    GROUP BY edge_sid
                )
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN max_score >= {threshold} THEN 1 ELSE 0 END) AS flagged_total
                FROM sid_scores
            """

sql_scoring: Any = _sys.modules[__name__]
_sys.modules["backend.repositories._sql.session_scoring"] = _sys.modules[__name__]
_sys.modules["backend.repositories.session_scoring"] = _sys.modules[__name__]


def query_logs(service_id: str, sql: str, params: tuple = ()) -> list[dict]:
    """Execute ``sql`` against the per-service logs view and return ``list[dict]``."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_connection, get_source_for_service
    from backend.core.storage import execute_with_stale_view_retry
    from backend.repositories._base import _compact_sql_for_debug
    from backend.utils.telemetry import get_queries

    src = get_source_for_service(service_id)
    if src is None:
        raise HTTPException(status_code=404, detail={"error": f"No service {service_id}"})
    con = None
    t0 = time.monotonic()
    try:
        skip_view = not svcconfig.is_durable_serving_mode(src)
        con = get_connection(source=src, max_wait=3, skip_view_update=skip_view, read_only=True)

        def _exec(c: Any) -> Any:
            return c.execute(sql, params).fetchall() if params else c.execute(sql).fetchall()

        rows = execute_with_stale_view_retry(con, src, _exec)
        cols = [d[0] for d in con.description] if con.description else []
        result = [dict(zip(cols, r)) for r in rows]
        get_queries().append(
            {
                "sql": _compact_sql_for_debug(sql.strip()),
                "time_ms": round((time.monotonic() - t0) * 1000, 2),
                "rows": len(result),
                "engine": "DuckDB",
            }
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                logger.debug("query_logs: con.close failed (ignored)", exc_info=True)


_SCORING_COLUMN_TYPES = {
    "edge_score": "DOUBLE",
    "edge_score_l1": "DOUBLE",
    "edge_score_l2": "DOUBLE",
    "edge_sid": "VARCHAR",
    "edge_score_reason": "VARCHAR",
    "edge_cookie_compliance": "VARCHAR",
}


def scoring_source(table: str, cols: set[str]) -> str:
    """A FROM-clause source for ``table`` guaranteed to expose the core scoring columns."""
    if not cols:
        return table
    missing = [(c, t) for c, t in _SCORING_COLUMN_TYPES.items() if c not in cols]
    if not missing:
        return table
    synth = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in missing)
    return f"(SELECT *, {synth} FROM {table})"


def _scoring_table_columns(service_id: str, table: str) -> set[str]:
    """Column-name set for ``table``, or empty set on error."""
    try:
        rows = describe_table(service_id, table)
    except HTTPException:
        return set()
    return {str(r.get("column_name")) for r in rows if r.get("column_name")}


def fetch_session_events(
    service_id: str,
    sids: list[str],
    since_days: int = 30,
    limit_per_sid: int = 500,
    *,
    ts_predicate: str | None = None,
) -> dict[str, list[dict]]:
    """Return per-sid event list within the last ``since_days`` days."""
    if not sids:
        return {}

    from backend.core.duckdb import _safe_table_name

    table = _safe_table_name(service_id)
    src = scoring_source(table, _scoring_table_columns(service_id, table))
    placeholders = ",".join("?" for _ in sids)
    where_ts = ts_predicate or f"timestamp >= now() - INTERVAL {int(since_days)} DAY"
    per_sid_cap = int(limit_per_sid)
    sql = f"""
        WITH ranked AS (
            SELECT edge_sid, timestamp AS ts, url, status, ip, ua,
                   edge_score, edge_cookie_compliance, edge_score_reason,
                   row_number() OVER (PARTITION BY edge_sid ORDER BY timestamp) AS _rn
            FROM {src}
            WHERE edge_sid IN ({placeholders})
              AND {where_ts}
        )
        SELECT edge_sid, ts, url, status, ip, ua,
               edge_score, edge_cookie_compliance, edge_score_reason
        FROM ranked
        WHERE _rn <= {per_sid_cap}
        ORDER BY edge_sid, ts
    """
    rows = query_logs(service_id, sql, tuple(sids))

    grouped: dict[str, list[dict]] = {}
    for r in rows:
        sid = r.get("edge_sid")
        if not sid:
            continue
        bucket = grouped.setdefault(sid, [])
        if len(bucket) >= limit_per_sid:
            continue
        ts = r.get("ts")
        if ts is None:
            ts_str: str | None = None
        elif hasattr(ts, "isoformat"):
            ts_str = ts.isoformat()
        else:
            ts_str = str(ts)
        bucket.append(
            {
                "ts": ts_str,
                "url": r.get("url") or "/",
                "status": r.get("status"),
                "ip": r.get("ip"),
                "ua": r.get("ua"),
                "edge_score": r.get("edge_score"),
                "edge_cookie_compliance": r.get("edge_cookie_compliance"),
                "edge_score_reason": r.get("edge_score_reason"),
            }
        )
    return grouped


def reconstruct_labeled_sessions(
    service_id: str,
    labels: list[dict],
    *,
    ts_predicate: str | None = None,
) -> list[tuple[dict, str]]:
    """Replay each labeled sid into the ``(session_dict, label)`` tuples that ``evaluate()`` expects."""
    if not labels:
        return []
    sid_to_label = {row["sid"]: row["label"] for row in labels if row.get("sid")}
    if not sid_to_label:
        return []
    grouped = fetch_session_events(service_id, list(sid_to_label.keys()), since_days=30, ts_predicate=ts_predicate)
    out: list[tuple[dict, str]] = []
    for sid, label in sid_to_label.items():
        events = grouped.get(sid, [])
        if not events:
            continue
        scored_values: list[float] = [v for e in events if (v := e.get("edge_score")) is not None]
        max_score = max(scored_values) if scored_values else None
        out.append(
            (
                {
                    "session_id": sid,
                    "events": events,
                    "max_edge_score": max_score,
                },
                label,
            )
        )
    return out


def top_flagged_rows(service_id: str, src: str, ts_pred: str, limit: int) -> list[dict]:
    """Highest-scored recent rows (score DESC, newest first), at most ``limit``."""
    return query_logs(service_id, sql_scoring.TOP_FLAGGED.format(src=src, ts_pred=ts_pred, limit=int(limit)))


def score_distribution_rows(service_id: str, src: str, ts_pred: str) -> list[dict]:
    """Hourly counts per score bucket (0-25 / 25-50 / 50-75 / 75-100)."""
    return query_logs(service_id, sql_scoring.SCORE_DISTRIBUTION.format(src=src, ts_pred=ts_pred))


def compliance_breakdown_rows(service_id: str, src: str, ts_pred: str) -> list[dict]:
    """Hourly counts grouped by ``edge_cookie_compliance``."""
    return query_logs(service_id, sql_scoring.COMPLIANCE_BREAKDOWN.format(src=src, ts_pred=ts_pred))


def latency_timeseries_rows(
    service_id: str, src: str, ts_pred: str, bucket: str, has_rtt: bool, has_exec: bool
) -> list[dict]:
    """Per-bucket scored / fail-open counts and (when the columns exist) RTT and Wasm-exec percentiles."""
    return query_logs(service_id, sql_scoring.latency_timeseries_sql(src, ts_pred, bucket, has_rtt, has_exec))


def health_snapshot_row(service_id: str, src: str, ts_pred: str, has_rtt: bool, has_exec: bool) -> dict:
    """The single aggregate row behind the scoring-health card (``{}`` when the query returns nothing)."""
    rows = query_logs(service_id, sql_scoring.health_snapshot_sql(src, ts_pred, has_rtt, has_exec))
    return rows[0] if rows else {}


def threshold_preview_counts(
    service_id: str, src: str, ts_pred: str, threshold: int, labeled_pairs: list[tuple[str, str]]
) -> dict:
    """Per-sid max-score bucket counts at ``threshold``, joined to ``(sid, label)`` pairs when any exist."""
    threshold_int = int(threshold)
    if labeled_pairs:
        values_placeholders = ", ".join("(?, ?)" for _ in labeled_pairs)
        values_params: tuple = tuple(p for pair in labeled_pairs for p in pair)
        single_sql = sql_scoring.THRESHOLD_PREVIEW_LABELED.format(
            src=src, ts_pred=ts_pred, values_placeholders=values_placeholders, threshold=threshold_int
        )
        rows = query_logs(service_id, single_sql, values_params)
    else:
        agg_sql = sql_scoring.THRESHOLD_PREVIEW_UNLABELED.format(src=src, ts_pred=ts_pred, threshold=threshold_int)
        rows = query_logs(service_id, agg_sql)
    return rows[0] if rows else {}


def describe_table(service_id: str, table: str) -> list[dict]:
    """``DESCRIBE`` rows (``column_name``, ``column_type``, ...) for a ``_safe_table_name`` identifier."""
    return query_logs(service_id, f"DESCRIBE {table}")
