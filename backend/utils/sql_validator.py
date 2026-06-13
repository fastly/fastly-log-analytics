"""User-supplied SQL validator for DuckDB (security).

The audit-drafted fix for the three DuckDB file-read findings (set
``enable_external_access=false`` on the connection) was validated against
production DuckDB 1.5.3 and found to break the iceberg_scan view that every
dashboard query relies on (see security_remediation_final_v6.md Appendix A).
The alternatives — ``allowed_directories``, ``disabled_filesystems`` — either
don't enforce or also block S3 reads required for iceberg_scan.

This module implements Decision B: a statement-type whitelist + a recursive
parse-tree walker that runs ``json_serialize_sql`` on every user-supplied
SQL string before execution. The walker rejects:

  * Statement types other than SELECT / WITH / SHOW.
  * Catalog table references that match the dangerous-schema deny list
    (``duckdb_*`` / ``pg_*`` table-name prefixes, ``information_schema`` /
    ``pg_catalog`` / ``system`` schema names, any non-``main`` catalog).
  * Function calls in a fixed deny set (env-var / setting / secret
    exfiltration helpers, all ``read_*`` / ``glob`` / ``lsdir`` file-system
    helpers, external-DB scanners).
  * Multi-statement payloads.
  * Inputs larger than 64 KB (DoS guard on the parser itself).

Every rejection emits a structured audit log line so attack-shaped probing
(``getenv``, ``read_csv_auto``, ``duckdb_secrets``, etc.) shows up as a
rejection-rate spike per session / service. Operators can use the log
output to either tighten the policy or whitelist a legitimate query
pattern.

The execution-side defense-in-depth — per-connection memory cap, statement
timeout, auto-injected LIMIT — lives in ``apply_user_query_limits`` so it
can be applied to the connection separately (this module never opens a
connection itself).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, NoReturn

import duckdb

logger = logging.getLogger(__name__)
_audit_logger = logging.getLogger("backend.sql_validator.audit")


# ── Tunables ────────────────────────────────────────────────────────────────

# Reject inputs larger than this before invoking the parser. The DuckDB
# parser itself is a DoS surface on pathological inputs (deeply nested
# subqueries, very long IN lists, etc.), and no legitimate user query
# should approach 64 KB.
MAX_INPUT_BYTES = 64 * 1024

# Statement types accepted by the user-query path. ``SELECT_NODE`` covers
# the underlying expression of SELECT and CTE-wrapping WITH statements
# (``json_serialize_sql`` surfaces them via the statement-level
# ``"node":{"type":"SELECT_NODE"}``). ``SHOW`` is allowed because the
# dashboard's debug panel uses it to introspect the live schema.
ALLOWED_STATEMENT_TYPES = frozenset({"SELECT_NODE", "SET_OPERATION_NODE"})

# Table-name prefixes that should never appear in a user query. The
# ``duckdb_`` family enumerates internal state (``duckdb_secrets``,
# ``duckdb_settings``, ``duckdb_extensions``). The ``pg_`` family is the
# PostgreSQL catalog compatibility surface (``pg_settings``,
# ``pg_authid``).
_BLOCKED_TABLE_PREFIXES = ("duckdb_", "pg_")

# Schema names that bypass the table-name-prefix check. ``information_schema``
# is the SQL-standard introspection namespace and would otherwise slip
# through because ``information_schema.tables`` has ``table_name="tables"``
# (no blocked prefix).
_BLOCKED_SCHEMA_NAMES = frozenset({"information_schema", "pg_catalog", "system"})

# The app only uses the default ``main`` catalog. Any cross-catalog
# reference (e.g. ``system.information_schema.tables``) is rejected
# because the only way a user query reaches a non-``main`` catalog is
# via ``ATTACH`` (which is itself blocked at the statement-type level).
ALLOWED_CATALOG = "main"

# Function denylist organised by intent. Each name is matched
# case-insensitively against the ``function_name`` field in the parse
# tree's ``"class":"FUNCTION"`` nodes.
_BLOCKED_FUNCTIONS = frozenset(
    {
        # Environment / config exfiltration
        "getenv",
        "current_setting",
        "duckdb_secrets",
        "duckdb_settings",
        "duckdb_variables",
        "duckdb_extensions",
        # File system reads — all variants of read_* (CSV / Parquet / JSON /
        # text / blob / Avro / Iceberg-from-disk). No s3:// exception:
        # user SQL targets the materialized FOS view, never raw read_parquet.
        "read_csv",
        "read_csv_auto",
        "read_parquet",
        "read_parquet_metadata",
        "read_parquet_schema",
        "read_json",
        "read_json_auto",
        "read_json_objects",
        "read_json_objects_auto",
        "read_ndjson",
        "read_ndjson_auto",
        "read_ndjson_objects",
        "read_text",
        "read_text_auto",
        "read_blob",
        "read_blob_auto",
        "read_avro",
        "iceberg_scan",
        "iceberg_metadata",
        "iceberg_snapshots",
        "parquet_scan",
        "parquet_metadata",
        "parquet_schema",
        "parquet_kv_metadata",
        # File system discovery
        "glob",
        "lsdir",
        # External DB scanners
        "sqlite_scan",
        "sqlite_attach",
        "postgres_scan",
        "postgres_attach",
        "postgres_query",
        "mysql_scan",
        "mysql_attach",
        "mysql_query",
        "query",
        "query_table",
    }
)


# ── Public types ─────────────────────────────────────────────────────────────


class SQLValidationError(ValueError):
    """Raised by ``validate_user_sql`` when the input fails any check.

    The ``reason`` field is the structured rejection code; ``message`` is
    the human-readable explanation. Callers should surface ``message`` to
    the API caller but log ``reason`` for attack-detection alerting.
    """

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass
class ValidationResult:
    """Successful validation result.

    ``parse_tree`` is the raw json_serialize_sql output (kept for callers
    that want to inspect / transform). ``elapsed_ms`` is the parse + walk
    cost — useful for the perf-budget alert.
    """

    parse_tree: dict
    elapsed_ms: float


# ── Module entry point ──────────────────────────────────────────────────────


def validate_user_sql(
    sql: str,
    *,
    parser_con: duckdb.DuckDBPyConnection,
    session_id: str | None = None,
    service_id: str | None = None,
) -> ValidationResult:
    """Validate a user-supplied SQL string against the Decision B policy.

    On rejection: emits an audit log line AND raises ``SQLValidationError``.
    On success: returns a ``ValidationResult`` whose ``parse_tree`` is the
    parsed JSON representation (caller can ignore it; the parse itself is
    the side effect).

    ``parser_con`` is a DuckDB connection used to call ``json_serialize_sql``.
    Pass the same read-only connection the query will execute against —
    parsing is cheap (~ms) and uses no state.

    The ``session_id`` and ``service_id`` are stamped into the audit log
    for attack-pattern detection. Pass ``None`` for system-internal calls
    that bypass the user-query path (those should never invoke this
    function in the first place).
    """
    if not isinstance(sql, str):
        _reject(sql, "input_type", "SQL must be a string", session_id, service_id)

    if "\x00" in sql:
        _reject(sql, "nul_byte_injection", "query contains a NUL byte", session_id, service_id)

    # Size pre-check (cheap; bounds parser cost).
    encoded = sql.encode("utf-8", errors="replace")
    if len(encoded) > MAX_INPUT_BYTES:
        _reject(
            sql,
            "input_too_large",
            f"query exceeds {MAX_INPUT_BYTES} byte limit ({len(encoded)} bytes)",
            session_id,
            service_id,
        )

    t0 = time.monotonic()

    # Parse via json_serialize_sql. Any parser exception OR a returned
    # ``{"error": true, ...}`` envelope counts as a rejection (fail
    # closed). This forces the parser to see ALL whitespace and bracket
    # balance issues at validation time, not at execution time when a
    # malformed payload could land halfway through a statement.
    try:
        row = parser_con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    except Exception as exc:
        _reject(
            sql,
            "parse_error",
            f"SQL parse failed: {exc}",
            session_id,
            service_id,
        )

    if not row or row[0] is None:
        _reject(sql, "parse_empty", "SQL parse returned no output", session_id, service_id)

    try:
        parsed = json.loads(row[0])
    except json.JSONDecodeError as exc:
        _reject(sql, "parse_invalid_json", f"json_serialize_sql output invalid: {exc}", session_id, service_id)

    if not isinstance(parsed, dict):
        _reject(sql, "parse_unexpected_shape", "expected JSON object", session_id, service_id)

    # The parser surfaces malformed SQL as ``{"error": true, ...}``
    # rather than raising — fail closed on that branch.
    if parsed.get("error") is True:
        err_type = parsed.get("error_type", "")
        err_msg = parsed.get("error_message", "<unknown>")
        _reject(
            sql,
            f"parse_error:{err_type}",
            f"SQL parse error: {err_msg}",
            session_id,
            service_id,
        )

    statements = parsed.get("statements")
    if not isinstance(statements, list) or len(statements) == 0:
        _reject(sql, "no_statements", "no parseable statements", session_id, service_id)
    if len(statements) > 1:
        _reject(
            sql,
            "multi_statement",
            f"only one statement allowed, got {len(statements)}",
            session_id,
            service_id,
        )

    # Statement-type whitelist. SELECT and CTE-wrapping WITH both
    # surface as a ``SELECT_NODE`` inside ``node``. SET_OPERATION_NODE
    # is UNION/INTERSECT/EXCEPT — also legitimate.
    stmt = statements[0]
    node = stmt.get("node") if isinstance(stmt, dict) else None
    node_type = node.get("type") if isinstance(node, dict) else None
    if node_type not in ALLOWED_STATEMENT_TYPES:
        _reject(
            sql,
            f"statement_type:{node_type or '?'}",
            f"only SELECT / WITH / UNION statements allowed (got {node_type})",
            session_id,
            service_id,
        )

    # Recursive walk: catalog blocklist (table_name / schema_name /
    # catalog_name) + function denylist. The walker visits every dict and
    # list nested under ``parsed`` so a buried sub-select or CTE doesn't
    # slip through.
    _walk_and_validate(parsed, sql, session_id, service_id)

    elapsed_ms = (time.monotonic() - t0) * 1000

    # Perf budget tracking. The Decision B target is p99 < 10 ms on
    # representative queries. Above 50 ms is a yellow-flag, above 200 ms
    # means someone's sending pathological input that should be rejected
    # by the 64 KB cap (or there's a walker bug).
    if elapsed_ms > 50:
        logger.warning(
            "[sql_validator] slow parse+walk: %.1fms for %d-byte input session=%s service=%s",
            elapsed_ms,
            len(encoded),
            session_id,
            service_id,
        )

    return ValidationResult(parse_tree=parsed, elapsed_ms=elapsed_ms)


# ── Walker ──────────────────────────────────────────────────────────────────


def _walk_and_validate(
    node: Any,
    original_sql: str,
    session_id: str | None,
    service_id: str | None,
) -> None:
    """Recursively visit every dict/list in the parse tree."""
    if isinstance(node, dict):
        # BASE_TABLE: table reference. Check name + schema + catalog.
        if node.get("type") == "BASE_TABLE" or "table_name" in node:
            _check_table_reference(node, original_sql, session_id, service_id)

        # FUNCTION: function call. Check function_name.
        # DuckDB also tags table functions inside TABLE_FUNCTION wrappers
        # but the inner node is still {"class":"FUNCTION", "function_name":...}
        if node.get("class") == "FUNCTION":
            fname = node.get("function_name")
            if isinstance(fname, str) and fname.lower() in _BLOCKED_FUNCTIONS:
                _reject(
                    original_sql,
                    f"function_denylist:{fname.lower()}",
                    f"function '{fname}' is not allowed in user queries",
                    session_id,
                    service_id,
                )

        for value in node.values():
            _walk_and_validate(value, original_sql, session_id, service_id)
    elif isinstance(node, list):
        for item in node:
            _walk_and_validate(item, original_sql, session_id, service_id)


def _check_table_reference(
    node: dict,
    original_sql: str,
    session_id: str | None,
    service_id: str | None,
) -> None:
    """Validate a BASE_TABLE node's name / schema / catalog fields."""
    table_name = (node.get("table_name") or "").strip()
    schema_name = (node.get("schema_name") or "").strip().lower()
    catalog_name = (node.get("catalog_name") or "").strip().lower()

    # DuckDB replacement scans: a path-shaped string in a FROM clause
    # (``SELECT * FROM '/etc/passwd'`` or ``SELECT * FROM 's3://bucket/key'``)
    # is parsed as a BASE_TABLE with table_name=<path>, then resolved to an
    # implicit read_* function call at execution time — bypassing the
    # function denylist entirely. Reject any table name containing path
    # separators or dotted segments. Legitimate identifiers never need
    # them (schema-qualified names land in schema_name / catalog_name).
    if "/" in table_name or "\\" in table_name or "." in table_name:
        _reject(
            original_sql,
            "catalog_blocklist:table_name_path",
            f"table name '{table_name}' contains path-like characters",
            session_id,
            service_id,
        )

    # Reject blocked table-name prefixes (catches duckdb_secrets etc.
    # referenced without a schema qualifier).
    name_lower = table_name.lower()
    for prefix in _BLOCKED_TABLE_PREFIXES:
        if name_lower.startswith(prefix):
            _reject(
                original_sql,
                f"catalog_blocklist:table_name_prefix:{prefix}",
                f"table '{table_name}' uses blocked prefix '{prefix}'",
                session_id,
                service_id,
            )

    # Reject the introspection-schema bypass. information_schema.tables
    # has table_name="tables" which would otherwise pass the prefix check.
    if schema_name in _BLOCKED_SCHEMA_NAMES:
        _reject(
            original_sql,
            f"catalog_blocklist:schema_name:{schema_name}",
            f"schema '{schema_name}' is not allowed in user queries",
            session_id,
            service_id,
        )

    # Reject any non-default catalog. The app uses only 'main'.
    if catalog_name and catalog_name != ALLOWED_CATALOG:
        _reject(
            original_sql,
            f"catalog_blocklist:catalog_name:{catalog_name}",
            f"catalog '{catalog_name}' is not allowed (only '{ALLOWED_CATALOG}')",
            session_id,
            service_id,
        )


# ── Audit logging + raise ───────────────────────────────────────────────────


def _reject(
    sql: str,
    reason: str,
    message: str,
    session_id: str | None,
    service_id: str | None,
) -> NoReturn:
    """Emit a structured audit log line and raise SQLValidationError.

    Never returns — always raises. The log line is JSON-shaped so it can
    be aggregated by ``rejection_reason`` and alerted on per session/service.
    """
    sql_str = sql if isinstance(sql, str) else str(sql)
    query_hash = "sha256:" + hashlib.sha256(sql_str.encode("utf-8", errors="replace")).hexdigest()
    snippet = sql_str[:500]
    _audit_logger.warning(
        "sql_validator_reject reason=%s session=%s service=%s hash=%s len=%d snippet=%r",
        reason,
        session_id or "-",
        service_id or "-",
        query_hash,
        len(sql_str),
        snippet,
    )
    raise SQLValidationError(reason=reason, message=message)


# ── Execution-side defense in depth ─────────────────────────────────────────


def apply_user_query_limits(
    con: duckdb.DuckDBPyConnection,
    *,
    memory_limit: str = "2GB",
    timeout_seconds: int = 30,
) -> None:
    """Apply per-connection limits before executing a user-supplied query.

    These are independent of the parse-tree validation — they bound the
    blast radius of a query that passed the validator (e.g., a perfectly
    legal but unconstrained ``SELECT * FROM fos_view`` that scans 100M
    rows). Set on the user-query connection only — the cron sync /
    compaction paths bypass these.
    """
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    except duckdb.Error as exc:
        logger.warning("[sql_validator] failed to apply memory_limit=%s: %s", memory_limit, exc)
    # statement_timeout is DuckDB 0.10+; ms units.
    try:
        con.execute(f"SET statement_timeout = '{timeout_seconds * 1000}ms'")
    except duckdb.Error as exc:
        logger.debug("[sql_validator] statement_timeout not supported on this DuckDB build: %s", exc)


def escape_sql_literal(value: str) -> str:
    """Escape a string for safe inclusion inside a DuckDB single-quoted SQL literal.

    Security: ingest paths interpolate S3 object keys (attacker-controlled
    via uploads to the monitored bucket) into ``read_json_auto('{path}', ...)``
    calls. Without escaping, a key containing a single quote breaks out of
    the literal and the rest of the key is parsed as SQL.

    DuckDB follows the SQL standard rule: a single quote inside a
    single-quoted literal is escaped by DOUBLING it (``'O''Brien'``).
    Backslashes are NOT escape characters in standard-mode DuckDB literals
    (i.e., ``\\n`` is a 2-character sequence, not a newline) so we don't
    need to special-case them. NULL bytes are passed through — DuckDB
    accepts them inside literals as actual NUL characters, and the
    surrounding code already filters by S3-API-valid characters which
    excludes \\x00 from object keys.

    Returns the escaped value WITHOUT the surrounding quotes — caller is
    expected to wrap with f"'{escape_sql_literal(x)}'".

    Multi-byte UTF-8 sequences pass through unchanged: we operate on the
    Python str, so doubled-quote substitution only fires on actual U+0027
    code points, never on a UTF-8 continuation byte whose binary value
    happens to be 0x27 (those are always the second/third/fourth byte of
    a multi-byte sequence and can never decode as a quote in str form).
    """
    if not isinstance(value, str):
        raise TypeError(f"escape_sql_literal expected str, got {type(value).__name__}")
    return value.replace("'", "''")


def has_limit_clause(sql: str, *, parser_con: duckdb.DuckDBPyConnection) -> bool:
    """Return True iff ``sql`` parses as a statement with an explicit LIMIT
    modifier on the outermost statement.

    026: the previous ``\\bLIMIT\\b`` regex check matched ``LIMIT``
    inside string literals (``WHERE name = 'WITHOUT LIMIT'``) and
    inside SQL comments — both false positives that made the
    auto-wrap helper SKIP wrapping. A query with attacker-supplied
    text containing the word ``LIMIT`` then ran unbounded and could
    materialise the entire fact table (OOM / 503).

    We check the parse tree's modifiers list strictly on the top-level
    node of each statement, preventing nested LIMIT clauses (e.g. inside subqueries)
    from triggering false positives and bypassing the limit wrapper.
    """
    try:
        row = parser_con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    except Exception:
        return True
    if not row or row[0] is None:
        return True
    try:
        parsed = json.loads(row[0])
    except Exception:
        return True
    if not isinstance(parsed, dict) or parsed.get("error"):
        return True

    statements = parsed.get("statements")
    if not isinstance(statements, list):
        return False

    for stmt in statements:
        if not isinstance(stmt, dict):
            continue
        node = stmt.get("node")
        if not isinstance(node, dict):
            continue
        modifiers = node.get("modifiers")
        if not isinstance(modifiers, list):
            continue
        for mod in modifiers:
            if isinstance(mod, dict):
                mod_type = mod.get("type")
                if isinstance(mod_type, str) and mod_type.startswith("LIMIT"):
                    return True

    return False


def inject_default_limit(sql: str, *, default_limit: int = 100_000) -> str:
    """Wrap a user query in ``SELECT * FROM (<sql>) LIMIT N`` when the
    original lacks an explicit LIMIT clause.

    Belt-and-suspenders with the memory_limit setting: prevents accidental
    full-table scans from filling the result set even when memory is fine.
    Caller may pre-strip the trailing semicolon.

    Note: this helper still uses the regex check for backwards-compat
    with internal callers that don't have a parser connection handy.
    The route-handler path uses ``has_limit_clause`` directly so the
    AST-aware check covers the user-supplied-SQL surface (026).
    """
    import re

    if re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
        return sql
    inner = sql.rstrip().rstrip(";")
    return f"SELECT * FROM ({inner}) AS _user_q LIMIT {default_limit}"


def is_simple_select_statement(sql: str, *, parser_con: duckdb.DuckDBPyConnection) -> bool:
    """Return True iff ``sql`` parses as a SELECT-like statement that returns
    a result set (e.g. SELECT, WITH, VALUES, FROM, TABLE) and is not a
    SHOW/DESCRIBE/SUMMARIZE or other fixed-shape metadata statement.
    """
    try:
        row = parser_con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    except Exception:
        return False
    if not row or row[0] is None:
        return False
    try:
        parsed = json.loads(row[0])
    except Exception:
        return False
    if not isinstance(parsed, dict) or parsed.get("error"):
        return False

    statements = parsed.get("statements")
    if not isinstance(statements, list) or not statements:
        return False

    stmt = statements[0]
    node = stmt.get("node") if isinstance(stmt, dict) else None
    if not isinstance(node, dict):
        return False

    node_type = node.get("type")
    if node_type not in ("SELECT_NODE", "SET_OPERATION_NODE"):
        return False

    from_table = node.get("from_table")
    if isinstance(from_table, dict) and from_table.get("type") == "SHOW_REF":
        return False

    return True
