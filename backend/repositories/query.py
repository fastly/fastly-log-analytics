"""Query repository — SQL execution helpers, no HTTP imports."""

from __future__ import annotations

import json
import re
import time
from typing import Any

import duckdb

from backend.repositories._base import _get_schema, _safe_table
from backend.utils.telemetry import get_tracked_calls

_BLOCKED_KEYWORDS = (
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "ATTACH",
    "COPY",
    "EXPORT",
    "IMPORT",
)


def execute_query(
    con: duckdb.DuckDBPyConnection,
    src: dict | None,
    sql: str,
    max_rows: int,
    want_explain: bool,
) -> dict:
    if src:
        table_name = _safe_table(src["name"])
        if table_name != "logs":
            sql = re.sub(r"\blogs\b", table_name, sql, flags=re.IGNORECASE)

    sql_upper = sql.upper()
    for kw in _BLOCKED_KEYWORDS:
        if re.search(rf"\b{kw}\b", sql_upper):
            raise PermissionError(f"Only read-only queries are allowed (blocked keyword: {kw})")

    _debug_queries: list[dict] = []
    if src:
        from backend.core.iceberg import inject_view_debug

        inject_view_debug(_debug_queries, src)

    explain_plan: str | None = None
    if want_explain:
        t_exp = time.monotonic()
        plan_rows = con.execute(f"EXPLAIN {sql}").fetchall()
        explain_plan = "\n".join(r[1] for r in plan_rows if r[1])
        _debug_queries.append({"sql": f"EXPLAIN {sql}", "time_ms": round((time.monotonic() - t_exp) * 1000, 2)})

    # Auto-apply LIMIT max_rows+1 when the query doesn't already have one.
    # Without this, `SELECT * FROM logs ORDER BY timestamp DESC` materializes
    # the entire 1.6M-row table before truncation — a 503 first-byte timeout
    # at the dashboard layer. With the +1 trick we can still report
    # ``truncated`` accurately and DuckDB's top-k optimizer kicks in on
    # ORDER BY ... LIMIT. Skip wrapping for non-SELECT statements (SUMMARIZE,
    # DESCRIBE, SHOW, PRAGMA, EXPLAIN) since they return small fixed-shape
    # result sets where the LIMIT semantics differ or aren't supported.
    exec_sql = sql
    sql_stripped_upper = sql.strip().upper().lstrip("(")
    is_simple_select = sql_stripped_upper.startswith(("SELECT", "WITH", "FROM", "VALUES", "TABLE")) and not re.search(
        r"\bLIMIT\b", sql_upper
    )
    if is_simple_select:
        # Strip trailing semicolon so the wrapper LIMIT lands in the same statement.
        inner = sql.rstrip().rstrip(";")
        exec_sql = f"SELECT * FROM ({inner}) AS _q LIMIT {max_rows + 1}"

    t0 = time.monotonic()
    result = con.execute(exec_sql)
    df = result.fetchdf()
    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)
    _debug_queries.append({"sql": exec_sql.strip(), "time_ms": elapsed_ms})

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

    columns = list(df.columns)
    records: list[dict[str, Any]] = json.loads(df.to_json(orient="records", date_format="iso"))

    resp: dict[str, Any] = {
        "columns": columns,
        "data": records,
        "row_count": len(records),
        "total_rows": total_rows,
        "truncated": truncated,
        "elapsed_ms": int(elapsed_ms),
        "debug_queries": _debug_queries,
        "debug_calls": get_tracked_calls(),
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
            "sql": f"SELECT * FROM {table_name} LIMIT 100",
        },
        {
            "name": "Row count",
            "description": "Total number of rows",
            "sql": f"SELECT count(*) AS total_rows FROM {table_name}",
        },
        {
            "name": "Column stats",
            "description": "Non-null counts and unique values per column",
            "sql": f"SUMMARIZE {table_name}",
        },
    ]
