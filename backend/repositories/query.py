"""Query repository — SQL execution helpers, no HTTP imports."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import duckdb

from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip_values
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
    dataset: str = "logs",
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
        if dataset != "logs":
            table_name = dataset
        else:
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
    #
    # The SAME source view redacts per-client PII columns for a mask_ips
    # analyst — session identifiers (SESSION_ID_KEYS, e.g. cookie_session) AND
    # the client IP family (IP_FAMILY_KEYS, e.g. ip). Redacting at the DATA
    # SOURCE is bypass-proof: no projection / alias / string-concat / CAST /
    # GROUP BY can recover the raw value because the view never emits it. This
    # is the ONLY robust control here — the value-shape masker (mask_ip_values,
    # below) is defeated by string ops (``'x' || ip`` yields a non-IP-shaped
    # cell) and a session hash has no IP shape at all. mask_ip_values is kept as
    # defense-in-depth for IP-shaped values that surface in a non-redacted
    # column. (adversarial audit 2026-07-06: closed a raw-IP enumeration leak
    # via ``SELECT 'x'||ip, count(*) FROM logs GROUP BY ip``.)
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
                # Non-fatal: CREATE OR REPLACE on the next analyst request
                # refreshes it before use, so a leaked view can't serve stale
                # data — but log it so a recurring leak is visible.
                logger.debug("[query] failed to drop analyst window view", exc_info=True)


from backend.utils.cache_registry import CacheRegistry as _CacheRegistry

_PII_COLS_CACHE: dict[str, tuple[float, frozenset[str]]] = {}
_PII_COLS_CACHE_TTL = 300.0  # 5 minutes
_CacheRegistry.register("query._PII_COLS_CACHE", _PII_COLS_CACHE)


def _pii_redact_cols(con: duckdb.DuckDBPyConnection, table_name: str | None) -> frozenset[str]:
    """Per-client PII columns (SESSION_ID_KEYS ∪ IP_FAMILY_KEYS) that actually
    exist on ``table_name``, for source-view redaction under mask_ips.

    Both families are redacted at the SOURCE (not just value-masked output-side)
    because value-shape masking is defeated by string ops: ``'x' || ip`` yields
    a non-IP-shaped cell and a session hash has no IP shape at all, so a mask_ips
    analyst could otherwise recover raw values via ``SELECT 'x'||ip`` /
    ``split_part(ip,'.',4)`` / ``CAST(ip AS BLOB)`` / ``GROUP BY ip``. ``oip``
    (origin IP) is intentionally NOT in IP_FAMILY_KEYS — it is not client PII and
    stays visible.

    Restricting to columns that genuinely exist keeps the ``SELECT * REPLACE
    (...)`` legal — a REPLACE naming an absent column is a Binder error that
    would fail the whole query. A DESCRIBE failure (e.g. a momentarily stale
    view) returns the empty set rather than raising: the user query would fail
    on the same stale view and the router's stale-view retry rebuilds it and
    re-runs this, so there is no window where a raw value slips through.
    """
    keys = SESSION_ID_KEYS | IP_FAMILY_KEYS
    if table_name is None or not keys:
        return frozenset()

    now = time.time()
    if table_name in _PII_COLS_CACHE:
        ts, cached_res = _PII_COLS_CACHE[table_name]
        if now - ts < _PII_COLS_CACHE_TTL:
            return cached_res

    try:
        cols = {row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()}
        res = frozenset(c for c in keys if c in cols)
        _PII_COLS_CACHE[table_name] = (now, res)
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
    """Create the per-request analyst source view, or None when nothing to bind.

    Two independent source-side controls share ONE temp view so the analyst's
    query reads from a single rebound table name:

      * H1 time window — when ``time_filter`` carries both bounds the view is
        filtered to ``[start, end)`` so the clamp holds regardless of the
        user's projection / WHERE / aggregation.
      * Phase-4 Track C session redaction — when ``redact_cols`` is non-empty
        each named column is rewritten to ``'[redacted]'`` (empty / NULL
        preserved) via ``SELECT * REPLACE (...)``, so a mask_ips analyst can
        never recover a raw session identifier by aliasing it or building it
        back out of the projection.

    Returns the view name to substitute the table reference with, or ``None``
    when neither control applies (the admin / no-mask, no-bounds path — caller
    leaves the SQL untouched = full retained range, raw columns).

    The window literals are re-parsed from the caller's ISO strings and
    re-emitted via ``isoformat()`` so the interpolation is injection-safe even
    if ``time_filter`` ever originates from somewhere other than the router's
    ``clamp_or_400``. ``timestamp`` is the TIMESTAMPTZ ordering column on every
    log table in this app (mirrors the predicate the analytics repos build in
    ``_base.py``).
    """
    if table_name is None:
        return None
    start_iso, end_iso = time_filter if time_filter is not None else (None, None)
    has_window = bool(start_iso and end_iso)
    if not has_window and not redact_cols:
        return None

    select_list = "*"
    if redact_cols:
        # Escape internal double-quotes so a hostile column name can't break out
        # of the quoted identifier (SESSION_ID_KEYS are static literals today,
        # but keep the guard for any future addition — mirrors optional_col in
        # _base.py). Sorted for a deterministic, testable view definition.
        repl_list = []
        for col in sorted(redact_cols):
            escaped_col = col.replace('"', '""')
            if col == "cid" and service_id:
                from backend.core.duckdb import get_or_generate_cid_salt

                salt = get_or_generate_cid_salt(service_id)
                expr = f'CASE WHEN "{escaped_col}" IS NULL OR "{escaped_col}" = \'\' THEN "{escaped_col}" ELSE sha256(CONCAT(\'{salt}\', "{escaped_col}")) END AS "{escaped_col}"'
            else:
                expr = f'CASE WHEN "{escaped_col}" IS NULL OR "{escaped_col}" = \'\' THEN "{escaped_col}" ELSE \'[redacted]\' END AS "{escaped_col}"'
            repl_list.append(expr)
        replacements = ", ".join(repl_list)
        select_list = f"* REPLACE ({replacements})"

    where_sql = ""
    if has_window:
        # Fail closed: a bound that doesn't parse is a programming error, not a
        # reason to run unfiltered — raise rather than silently widening the
        # window.
        start_dt = parse_iso_utc(start_iso)
        end_dt = parse_iso_utc(end_iso)
        if start_dt is None or end_dt is None:
            raise ValueError(f"invalid analyst window bounds: {start_iso!r}..{end_iso!r}")
        where_sql = (
            f" WHERE timestamp >= TIMESTAMPTZ '{start_dt.isoformat()}' "
            f"AND timestamp < TIMESTAMPTZ '{end_dt.isoformat()}'"
        )

    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {_ANALYST_WINDOW_VIEW} AS SELECT {select_list} FROM {table_name}{where_sql}"
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
