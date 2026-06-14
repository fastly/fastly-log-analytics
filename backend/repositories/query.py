"""Query repository — SQL execution helpers, no HTTP imports."""

from __future__ import annotations

import json
import re
import time
from typing import Any

import duckdb

from backend.repositories._base import SectionTimer, _compact_sql_for_debug, _get_schema, _safe_table
from backend.repositories._sql import query as SQL
from backend.utils.sql_validator import (
    SQLValidationError,
    apply_user_query_limits,
    is_simple_select_statement,
    validate_user_sql,
)
from backend.utils.telemetry import get_tracked_calls


def execute_query(
    con: duckdb.DuckDBPyConnection,
    src: dict | None,
    sql: str,
    max_rows: int,
    want_explain: bool,
    *,
    session_id: str | None = None,
    service_id: str | None = None,
) -> dict:
    # Per-phase wall-clock timings — complements the existing
    # _debug_queries (per-SQL granularity) with a higher-level view of
    # where validate / explain / execute / serialize each contribute.
    timer = SectionTimer()
    section_timings = timer.entries

    if src:
        table_name = _safe_table(src["name"])
        if table_name != "logs":
            sql = re.sub(r"\blogs\b", table_name, sql, flags=re.IGNORECASE)

    _t = time.perf_counter()
    try:
        validate_user_sql(
            sql,
            parser_con=con,
            session_id=session_id,
            service_id=service_id,
        )
    except SQLValidationError as exc:
        # PermissionError is what the route handler maps to HTTP 403.
        raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t)

    # Execution-side defense-in-depth: cap memory and timeout on the
    # connection before running the user query. Independent of parse
    # validation — a legal query can still scan 100M rows.
    apply_user_query_limits(con)

    _debug_queries: list[dict] = []
    if src:
        from backend.core.iceberg import inject_view_debug

        inject_view_debug(_debug_queries, src)

    explain_plan: str | None = None
    if want_explain:
        t_exp = time.perf_counter()
        explain_sql = SQL.EXPLAIN_WRAPPER.format(sql=sql)
        plan_rows = con.execute(explain_sql).fetchall()
        explain_plan = "\n".join(r[1] for r in plan_rows if r[1])
        _debug_queries.append(
            {"sql": _compact_sql_for_debug(explain_sql), "time_ms": round((time.perf_counter() - t_exp) * 1000, 2)}
        )
        timer.mark("explain", t_exp)

    # Auto-apply LIMIT max_rows+1 when the query doesn't already have one.
    # Without this, `SELECT * FROM logs ORDER BY timestamp DESC` materializes
    # the entire 1.6M-row table before truncation — a 503 first-byte timeout
    # at the dashboard layer. With the +1 trick we can still report
    # ``truncated`` accurately and DuckDB's top-k optimizer kicks in on
    # ORDER BY ... LIMIT. Skip wrapping for non-SELECT statements (SUMMARIZE,
    # DESCRIBE, SHOW, PRAGMA, EXPLAIN) since they return small fixed-shape
    # result sets where the LIMIT semantics differ or aren't supported.
    exec_sql = sql
    # 015 / 026: Check if the statement is a simple SELECT using the AST-aware helper.
    # String-based startswith or regex checks match inside comments or string literals,
    # leading to bypasses. The AST-aware check ensures accuracy.
    is_simple_select = is_simple_select_statement(sql, parser_con=con)
    if is_simple_select:
        # Strip trailing semicolon so the wrapper LIMIT lands in the same statement.
        inner = sql.rstrip().rstrip(";")
        exec_sql = SQL.AUTO_LIMIT_WRAPPER.format(inner=inner, limit=max_rows + 1)

    t0 = time.perf_counter()
    result = con.execute(exec_sql)
    _t_fetch = time.perf_counter()
    timer.mark("execute", t0)
    df = result.fetchdf()
    timer.mark("fetchdf", _t_fetch)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    _debug_queries.append({"sql": _compact_sql_for_debug(exec_sql.strip()), "time_ms": elapsed_ms})

    fetched_rows = len(df)
    if is_simple_select:
        truncated = fetched_rows > max_rows
        if truncated:
            df = df.head(max_rows)
        # With the +1 trick we don't have an exact total. Report -1 as the
        # "unknown total" sentinel; frontend treats this as ``Showing N rows
        # (more available)``. Avoids the cost of re-running COUNT(*).
        total_rows = -1 if truncated else fetched_rows
    else:
        # Non-SELECT (SUMMARIZE, DESCRIBE, SHOW, PRAGMA): full result was
        # materialized and is small by construction. Apply the cap defensively.
        truncated = fetched_rows > max_rows
        if truncated:
            df = df.head(max_rows)
        total_rows = fetched_rows

    _t_serialize = time.perf_counter()
    columns = list(df.columns)
    records: list[dict[str, Any]] = json.loads(df.to_json(orient="records", date_format="iso"))
    timer.mark("serialize_json", _t_serialize)

    resp: dict[str, Any] = {
        "columns": columns,
        "data": records,
        "row_count": len(records),
        "total_rows": total_rows,
        "truncated": truncated,
        "elapsed_ms": int(elapsed_ms),
        "debug_queries": _debug_queries,
        "debug_calls": get_tracked_calls(),
        "section_timings": section_timings,
    }
    if explain_plan is not None:
        resp["explain_plan"] = explain_plan
    return resp


def get_presets(src: dict | None, con: duckdb.DuckDBPyConnection | None = None) -> list[dict]:
    if not src:
        return []
    table_name = _safe_table(src["name"])

    has_ts = False
    if con is not None:
        try:
            has_ts = "timestamp" in [col["name"] for col in _get_schema(con, src)]
        except Exception:
            pass

    _ = has_ts  # kept for future presets; dropped ORDER BY here to avoid
    # forcing a 1.6M-row sort on a "Sample rows" preview.
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
