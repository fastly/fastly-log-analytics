"""Query repository — SQL execution helpers, no HTTP imports."""

from __future__ import annotations

import json
import re
import time
from typing import Any

import duckdb

from backend.repositories._base import _compact_sql_for_debug, _get_schema, _safe_table
from backend.utils.sql_validator import (
    SQLValidationError,
    apply_user_query_limits,
    has_limit_clause,
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
    if src:
        table_name = _safe_table(src["name"])
        if table_name != "logs":
            sql = re.sub(r"\blogs\b", table_name, sql, flags=re.IGNORECASE)

    # Security (Decision B): run the user SQL through the
    # parse-tree validator. The previous regex-based ``_BLOCKED_KEYWORDS``
    # check missed:
    #   - read_csv_auto / read_parquet / iceberg_scan family (arbitrary
    #     file/S3 read via table functions)
    #   - getenv / current_setting / duckdb_secrets (env/secret exfil)
    #   - information_schema.* (introspection bypass via non-prefix name)
    #   - INSTALL / LOAD (which don't contain any blocked keyword)
    # The validator runs ``json_serialize_sql`` and walks the resulting
    # parse tree so every nested subquery / CTE / table-function is
    # inspected. See backend/utils/sql_validator.py for the policy.
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
        t_exp = time.monotonic()
        plan_rows = con.execute(f"EXPLAIN {sql}").fetchall()
        explain_plan = "\n".join(r[1] for r in plan_rows if r[1])
        _debug_queries.append(
            {"sql": _compact_sql_for_debug(f"EXPLAIN {sql}"), "time_ms": round((time.monotonic() - t_exp) * 1000, 2)}
        )

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
    # 026: ``re.search(r"\bLIMIT\b", sql)`` matches inside string
    # literals (``WHERE name = 'WITHOUT LIMIT'``) and inside SQL
    # comments — both false positives that cause the auto-wrap to
    # SKIP wrapping, leaving the query unbounded. The AST-aware
    # check inspects the parse tree so strings/comments are out of
    # scope.
    is_simple_select = sql_stripped_upper.startswith(
        ("SELECT", "WITH", "FROM", "VALUES", "TABLE")
    ) and not has_limit_clause(sql, parser_con=con)
    if is_simple_select:
        # Strip trailing semicolon so the wrapper LIMIT lands in the same statement.
        inner = sql.rstrip().rstrip(";")
        exec_sql = f"SELECT * FROM ({inner}) AS _q LIMIT {max_rows + 1}"

    t0 = time.monotonic()
    result = con.execute(exec_sql)
    df = result.fetchdf()
    elapsed_ms = round((time.monotonic() - t0) * 1000, 2)
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
