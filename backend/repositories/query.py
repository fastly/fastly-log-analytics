"""Query repository — SQL execution helpers, no HTTP imports."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import duckdb

from backend.core.share_db.validation import mask_ip_values
from backend.repositories._base import SectionTimer, _compact_sql_for_debug, _safe_table
from backend.repositories._sql import query as SQL
from backend.utils.date_utils import parse_iso_utc
from backend.utils.sql_validator import (
    SQLValidationError,
    apply_user_query_limits,
    is_simple_select_statement,
    validate_user_sql,
)
from backend.utils.telemetry import get_tracked_calls

logger = logging.getLogger(__name__)

# M1: hard ceiling on the number of rows /api/query will materialize, applied
# as a defense-in-depth re-clamp inside execute_query (the QueryRequest model
# bounds it too, but internal callers bypass the model). Mirrors the model's
# ``le=10_000``.
MAX_QUERY_ROWS = 10_000

# H1: reserved name for the per-request temp view that pins an analyst's query
# to their clamped [start, end) window. The user's references to the log table
# are rewritten to this name; the view itself is (re)created from the real
# per-service table on every analyst request and dropped in a finally block.
# Starts with "_" so it can never collide with a real service table (those are
# always ``logs`` / ``logs_<svc>`` via _safe_table_name), and the SQL validator
# rejects any user reference to it outright (not in the {logs, logs_<svc>}
# allowlist), so an analyst can't name it directly.
_ANALYST_WINDOW_VIEW = "_analyst_window_logs"


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
) -> dict:
    """Execute a validated user SQL statement.

    ``time_filter`` (H1): an ``(start_iso, end_iso)`` pair clamped to the
    analyst's allowed window by the router (``None`` / ``(None, None)`` for
    admin = full retained range). When both bounds are present the per-service
    log table is rebound to a temp view filtered to ``[start, end)`` so the
    window holds at the data source regardless of the user's projection /
    WHERE / aggregation — see ``_rebind_table_to_window_view``.

    ``mask_ips`` (H2): when True, every result cell that parses as an IP is
    masked by value (``mask_ip_values``) — robust against the column-aliasing
    that defeats the middleware's key-name masker on this free-form surface.
    """
    # Per-phase wall-clock timings — complements the existing
    # _debug_queries (per-SQL granularity) with a higher-level view of
    # where validate / explain / execute / serialize each contribute.
    timer = SectionTimer()
    section_timings = timer.entries

    # M1: defense-in-depth re-clamp. The QueryRequest model already bounds
    # max_rows, but internal callers construct the call directly.
    max_rows = max(1, min(int(max_rows), MAX_QUERY_ROWS))

    table_name: str | None = None
    if src:
        table_name = _safe_table(src["name"])
        if table_name != "logs":
            sql = re.sub(r"\blogs\b", table_name, sql, flags=re.IGNORECASE)
        # F-8/9/10: cross-tenant catalog leakage on /api/query. The
        # pooled DuckDB connection's catalog can contain foreign service
        # tables (logs_<other_sid>) left over from prior view rebinds.
        # Restrict user SQL to the canonical view name and the active
        # service's per-table name so SHOW TABLES + SELECT FROM a foreign
        # table both fail at parse time (the SHOW_REF reject in the
        # validator covers SHOW; this covers SELECT).
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
        # PermissionError is what the route handler maps to HTTP 403.
        raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t)

    # H1: bind the analyst's clamped window to the data source. Done AFTER
    # validation (the validator sees clean ``logs`` / ``logs_<svc>`` table
    # refs, not the rewritten view name) and the created view is dropped in
    # the finally below so it never lingers on the pooled connection.
    window_view = _rebind_table_to_window_view(con, table_name, time_filter)
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
                # Non-fatal: CREATE OR REPLACE on the next analyst request
                # refreshes it before use, so a leaked view can't serve stale
                # data — but log it so a recurring leak is visible.
                logger.debug("[query] failed to drop analyst window view", exc_info=True)


def _rebind_table_to_window_view(
    con: duckdb.DuckDBPyConnection,
    table_name: str | None,
    time_filter: tuple[str | None, str | None] | None,
) -> str | None:
    """Create the per-request window temp view, or return None when no filter.

    Returns the view name to substitute the table reference with, or ``None``
    for the admin / no-bounds path (caller leaves the SQL untouched = full
    retained range).

    The window literals are re-parsed from the caller's ISO strings and
    re-emitted via ``isoformat()`` so the interpolation is injection-safe even
    if ``time_filter`` ever originates from somewhere other than the router's
    ``clamp_or_400``. ``timestamp`` is the TIMESTAMPTZ ordering column on every
    log table in this app (mirrors the predicate the analytics repos build in
    ``_base.py``).
    """
    if time_filter is None or table_name is None:
        return None
    start_iso, end_iso = time_filter
    if not start_iso or not end_iso:
        return None
    # Fail closed: a bound that doesn't parse is a programming error, not a
    # reason to run unfiltered — raise rather than silently widening the window.
    start_dt = parse_iso_utc(start_iso)
    end_dt = parse_iso_utc(end_iso)
    if start_dt is None or end_dt is None:
        raise ValueError(f"invalid analyst window bounds: {start_iso!r}..{end_iso!r}")
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {_ANALYST_WINDOW_VIEW} AS "
        f"SELECT * FROM {table_name} "
        f"WHERE timestamp >= TIMESTAMPTZ '{start_dt.isoformat()}' "
        f"AND timestamp < TIMESTAMPTZ '{end_dt.isoformat()}'"
    )
    return _ANALYST_WINDOW_VIEW


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
    arrow_table = result.to_arrow_table()
    timer.mark("fetch_arrow", _t_fetch)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    _debug_queries.append({"sql": _compact_sql_for_debug(exec_sql.strip()), "time_ms": elapsed_ms})

    fetched_rows = arrow_table.num_rows
    if is_simple_select:
        truncated = fetched_rows > max_rows
        if truncated:
            arrow_table = arrow_table.slice(0, max_rows)
        # With the +1 trick we don't have an exact total. Report -1 as the
        # "unknown total" sentinel; frontend treats this as ``Showing N rows
        # (more available)``. Avoids the cost of re-running COUNT(*).
        total_rows = -1 if truncated else fetched_rows
    else:
        # Non-SELECT (SUMMARIZE, DESCRIBE, SHOW, PRAGMA): full result was
        # materialized and is small by construction. Apply the cap defensively.
        truncated = fetched_rows > max_rows
        if truncated:
            arrow_table = arrow_table.slice(0, max_rows)
        total_rows = fetched_rows

    # Arrow → Python natives in one pass, sidestepping the prior
    # ``df.to_json(...) → json.loads(...)`` round-trip (pandas serialised
    # the full result to a JSON string only for us to parse it back into
    # dicts before FastAPI re-serialised it for the wire). pyarrow's
    # ``to_pylist`` materialises ``datetime.datetime`` for timestamps and
    # ``None`` for nulls — both handled by the default JSON encoder.
    _t_serialize = time.perf_counter()
    columns = list(arrow_table.schema.names)
    records: list[dict[str, Any]] = arrow_table.to_pylist()
    # H2: value-shape PII masking. The analyst controls the output column
    # names on this free-form surface, so the middleware's key-name masker is
    # bypassable by aliasing (``SELECT ip AS addr``). Mask any cell that
    # parses as an IP, regardless of column name.
    if mask_ips:
        records = mask_ip_values(records)
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
    # `con` is unused — the presets are pure-template SQL keyed on the
    # service name. Parameter kept for the test fixture that passes a
    # connection positionally.
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
