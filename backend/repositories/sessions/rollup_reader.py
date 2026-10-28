"""Session rollup path discovery, SQL builders, and rollup reader."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import sum_or_zero
from backend.models.common import FiltersDict
from backend.repositories._base import QueryRunner
from backend.repositories.utils.filters import calc_offset
from backend.repositories.utils.response_shaping import (
    build_asn_label_map,
    shape_sessions_envelope,
)


def _collect_sessions_rollup_paths(src: dict, st: datetime, et: datetime) -> tuple[list[str], bool] | None:
    """Enumerate per-hour sessions.parquet files covering ``[st, et)``."""
    from backend.core.rollups import SESSIONS_BUNDLE_FILENAME, _hour_bundled_root
    from backend.core.rollups.recompute import collect_hourly_bundle_paths

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
        from backend.core.storage._core import _tombstoned_parquet_paths

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
        from backend.core.storage._core import ducklake_table_name

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


def _schema_cap_flags(actual_cols: set[str]) -> dict[str, Any]:
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
    return shape_sessions_envelope(
        sessions,
        total=total,
        page=page,
        limit=limit,
        **_schema_cap_flags(actual_cols),
        min_reqs_flag=min_reqs_flag,
        min_4xx_pct_flag=min_4xx_pct_flag,
        asn_names=_build_asn_label_map(sessions, src),
        section_timings=section_timings,
        telemetry=telemetry,
        rollup_served=rollup_served,
    )


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
