"""Query execution repository."""

from __future__ import annotations

import logging
import re
import sys as _sys
import time
from typing import Any

import duckdb

from backend.core.share_db import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip_values
from backend.core.sql_validator import (
    SQLValidationError,
    apply_user_query_limits,
    is_simple_select_statement,
    validate_user_sql,
)
from backend.repositories._base import (
    SectionTimer,
    _compact_sql_for_debug,
    _safe_table,
    record_duckdb_query,
)
from backend.repositories.utils.response_shaping import build_query_response_envelope
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_iso_utc
from backend.utils.telemetry import get_tracked_calls, log_once_per, register_salt

logger = logging.getLogger(__name__)

EXPLAIN_WRAPPER = "EXPLAIN {sql}"
AUTO_LIMIT_WRAPPER = "SELECT * FROM ({inner}) AS _q LIMIT {limit}"
PRESET_SAMPLE_ROWS = "SELECT * FROM {table} LIMIT 100"
PRESET_ROW_COUNT = "SELECT count(*) AS total_rows FROM {table}"
PRESET_COLUMN_STATS = "SUMMARIZE {table}"

MAX_QUERY_ROWS = 10_000
_ANALYST_WINDOW_VIEW = "_analyst_window_logs"
_ANALYST_CID_SALT_TABLE = "_analyst_cid_salt"

_PII_COLS_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=300.0)
_CacheRegistry.register("query._PII_COLS_CACHE", _PII_COLS_CACHE)

_PRESETS_CACHE_TTL = 300.0
_presets_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_PRESETS_CACHE_TTL)
_CacheRegistry.register("query._presets_cache", _presets_cache)

SQL: Any = _sys.modules[__name__]
_sys.modules.setdefault("backend.repositories._sql.query", _sys.modules[__name__])


def execute_query(
    con: duckdb.DuckDBPyConnection,
    src: dict | None,
    sql: str,
    max_rows: int,
    want_explain: bool,
    *,
    session_id: str | None = None,
    service_id: str | None = None,
    time_filter: tuple[str | None, str | None] | None = None,
    mask_ips: bool = False,
    dataset: str = "logs",
) -> dict:
    """Execute a validated user SQL statement."""
    timer = SectionTimer()
    section_timings = timer.entries
    max_rows = max(1, min(int(max_rows), MAX_QUERY_ROWS))

    table_name: str | None = None
    if src:
        table_name = dataset if dataset != "logs" else _safe_table(src["name"])
        if table_name != "logs":
            sql = re.sub(r"\blogs\b", table_name, sql, flags=re.IGNORECASE)
        allowed_tables: frozenset[str] | None = frozenset({"logs", table_name.lower()})
    else:
        allowed_tables = None

    _t = time.perf_counter()
    try:
        validate_user_sql(
            sql,
            parser_con=con,
            session_id=session_id,
            service_id=service_id,
            allowed_tables=allowed_tables,
        )
    except SQLValidationError as exc:
        raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t)

    redact_cols = _pii_redact_cols(con, table_name) if mask_ips else frozenset()
    window_view = _rebind_table_to_window_view(con, table_name, time_filter, redact_cols, service_id=service_id)
    if window_view is not None and table_name is not None:
        sql = re.sub(rf"\b{re.escape(table_name)}\b", lambda _m: window_view, sql, flags=re.IGNORECASE)

    try:
        return _run_validated_query(
            con,
            sql,
            src=src,
            max_rows=max_rows,
            want_explain=want_explain,
            mask_ips=mask_ips,
            timer=timer,
            section_timings=section_timings,
        )
    finally:
        if window_view is not None:
            try:
                con.execute(f"DROP VIEW IF EXISTS {window_view}")
            except Exception:
                if log_once_per("[query] failed to drop analyst window view", 60):
                    logger.debug("[query] failed to drop analyst window view", exc_info=True)
            _drop_analyst_cid_salt(con)


def get_cached_presets(service_id: str, format_hash: str | None) -> Any | None:
    if not format_hash:
        return None
    return _presets_cache.get((service_id, format_hash))


def set_cached_presets(service_id: str, format_hash: str | None, payload: Any) -> None:
    if not format_hash:
        return
    _presets_cache[(service_id, format_hash)] = payload


def invalidate_presets_cache(service_id: str, format_hash: str | None) -> None:
    if format_hash:
        _presets_cache.pop((service_id, format_hash), None)


clear_presets_cache = invalidate_presets_cache


def _pii_redact_cols(con: duckdb.DuckDBPyConnection, table_name: str | None) -> frozenset[str]:
    keys = SESSION_ID_KEYS | IP_FAMILY_KEYS
    if table_name is None or not keys:
        return frozenset()
    cached_res = _PII_COLS_CACHE.get(table_name)
    if cached_res is not None:
        return cached_res
    try:
        cols = {row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()}
        res = frozenset(c for c in keys if c in cols)
        _PII_COLS_CACHE[table_name] = res
        return res
    except Exception:
        return frozenset()


def _rebind_table_to_window_view(
    con: duckdb.DuckDBPyConnection,
    table_name: str | None,
    time_filter: tuple[str | None, str | None] | None,
    redact_cols: frozenset[str] = frozenset(),
    *,
    service_id: str | None = None,
) -> str | None:
    if table_name is None:
        return None
    start_iso, end_iso = time_filter if time_filter is not None else (None, None)
    has_window = bool(start_iso and end_iso)
    if not has_window and not redact_cols:
        return None

    where_sql = ""
    if has_window:
        start_dt = parse_iso_utc(start_iso)
        end_dt = parse_iso_utc(end_iso)
        if start_dt is None or end_dt is None:
            raise ValueError(f"invalid analyst window bounds: {start_iso!r}..{end_iso!r}")
        where_sql = (
            f" WHERE timestamp >= TIMESTAMPTZ '{start_dt.isoformat()}' "
            f"AND timestamp < TIMESTAMPTZ '{end_dt.isoformat()}'"
        )

    select_list = "*"
    if redact_cols:
        repl_list = []
        for col in sorted(redact_cols):
            escaped_col = col.replace('"', '""')
            if col == "cid" and service_id:
                from backend.core.duckdb import get_or_generate_cid_salt

                cid_salt = get_or_generate_cid_salt(service_id)
                register_salt(cid_salt)
                con.execute(
                    f"CREATE OR REPLACE TEMP TABLE {_ANALYST_CID_SALT_TABLE} AS SELECT ?::VARCHAR AS salt",
                    [cid_salt],
                )
                expr = (
                    f'CASE WHEN "{escaped_col}" IS NULL OR "{escaped_col}" = \'\' THEN "{escaped_col}" '
                    f'ELSE sha256(CONCAT((SELECT salt FROM {_ANALYST_CID_SALT_TABLE}), "{escaped_col}")) '
                    f'END AS "{escaped_col}"'
                )
            else:
                expr = f'CASE WHEN "{escaped_col}" IS NULL OR "{escaped_col}" = \'\' THEN "{escaped_col}" ELSE \'[redacted]\' END AS "{escaped_col}"'
            repl_list.append(expr)
        select_list = f"* REPLACE ({', '.join(repl_list)})"

    try:
        con.execute(
            f"CREATE OR REPLACE TEMP VIEW {_ANALYST_WINDOW_VIEW} AS SELECT {select_list} FROM {table_name}{where_sql}"
        )
    except BaseException:
        _drop_analyst_cid_salt(con)
        raise
    return _ANALYST_WINDOW_VIEW


def _drop_analyst_cid_salt(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute(f"DROP TABLE IF EXISTS {_ANALYST_CID_SALT_TABLE}")
    except Exception:
        if log_once_per("[query] failed to drop analyst cid-salt table", 60):
            logger.debug("[query] failed to drop analyst cid-salt table", exc_info=True)


def _run_validated_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    *,
    src: dict | None,
    max_rows: int,
    want_explain: bool,
    mask_ips: bool,
    timer: SectionTimer,
    section_timings: list,
) -> dict:
    apply_user_query_limits(con)

    _debug_queries: list[dict] = []
    if src:
        from backend.core.storage import inject_view_debug

        inject_view_debug(_debug_queries, src)

    explain_plan: str | None = None
    if want_explain:
        t_exp = time.perf_counter()
        explain_sql = SQL.EXPLAIN_WRAPPER.format(sql=sql)
        plan_rows = con.execute(explain_sql).fetchall()
        explain_plan = "\n".join(r[1] for r in plan_rows if r[1])
        _debug_queries.append(
            {
                "sql": _compact_sql_for_debug(explain_sql),
                "time_ms": round((time.perf_counter() - t_exp) * 1000, 2),
                "engine": "DuckDB",
            }
        )
        timer.mark("explain", t_exp)

    exec_sql = sql
    is_simple_select = is_simple_select_statement(sql, parser_con=con)
    if is_simple_select:
        inner = sql.rstrip().rstrip(";")
        exec_sql = SQL.AUTO_LIMIT_WRAPPER.format(inner=inner, limit=max_rows + 1)

    t0 = time.perf_counter()
    result = con.execute(exec_sql)
    _t_fetch = time.perf_counter()
    timer.mark("execute", t0)
    arrow_table = result.to_arrow_table()
    timer.mark("fetch_arrow", _t_fetch)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    compact_exec_sql = _compact_sql_for_debug(exec_sql.strip())
    _debug_queries.append({"sql": compact_exec_sql, "time_ms": elapsed_ms, "engine": "DuckDB"})
    svc_id = (src.get("name") or src.get("service_id")) if src else None
    for dq in _debug_queries:
        record_duckdb_query(dq.get("sql", ""), float(dq.get("time_ms", 0.0)), service_id=svc_id)

    fetched_rows = arrow_table.num_rows
    if is_simple_select:
        truncated = fetched_rows > max_rows
        if truncated:
            arrow_table = arrow_table.slice(0, max_rows)
        total_rows = -1 if truncated else fetched_rows
    else:
        truncated = fetched_rows > max_rows
        if truncated:
            arrow_table = arrow_table.slice(0, max_rows)
        total_rows = fetched_rows

    _t_serialize = time.perf_counter()
    columns = list(arrow_table.schema.names)
    records: list[dict[str, Any]] = arrow_table.to_pylist()
    if mask_ips:
        records = mask_ip_values(records)
    timer.mark("serialize_json", _t_serialize)

    return build_query_response_envelope(
        columns=columns,
        data=records,
        total_rows=total_rows,
        truncated=truncated,
        elapsed_ms=elapsed_ms,
        debug_queries=_debug_queries,
        debug_calls=get_tracked_calls(),
        section_timings=section_timings,
        explain_plan=explain_plan,
    )


def get_presets(src: dict | None, con: duckdb.DuckDBPyConnection | None = None) -> list[dict]:
    del con
    if not src:
        return []
    table_name = _safe_table(src["name"])
    return [
        {
            "name": "Sample rows",
            "description": "Preview 100 raw log rows",
            "sql": SQL.PRESET_SAMPLE_ROWS.format(table=table_name),
        },
        {
            "name": "Row count",
            "description": "Total number of rows",
            "sql": SQL.PRESET_ROW_COUNT.format(table=table_name),
        },
        {
            "name": "Column stats",
            "description": "Non-null counts and unique values per column",
            "sql": SQL.PRESET_COLUMN_STATS.format(table=table_name),
        },
    ]
