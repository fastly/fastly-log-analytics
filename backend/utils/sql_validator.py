"""User-supplied SQL validator for DuckDB (Decision B policy)."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, NoReturn

import duckdb

logger = logging.getLogger(__name__)
_audit_logger = logging.getLogger("backend.sql_validator.audit")

MAX_INPUT_BYTES = 64 * 1024
ALLOWED_STATEMENT_TYPES = frozenset({"SELECT_NODE", "SET_OPERATION_NODE"})
_BLOCKED_TABLE_PREFIXES = ("duckdb_", "pg_")
_BLOCKED_SCHEMA_NAMES = frozenset({"information_schema", "pg_catalog", "system"})
ALLOWED_CATALOG = "main"

_BLOCKED_FUNCTIONS = frozenset(
    {
        "getenv",
        "current_setting",
        "duckdb_secrets",
        "duckdb_settings",
        "duckdb_variables",
        "duckdb_extensions",
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
        "glob",
        "lsdir",
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

_BLOCKED_FUNCTION_PREFIXES = ("read_", "parquet_", "iceberg_", "duckdb_", "sniff_", "glob")

_ALLOWED_TABLE_FUNCTIONS = frozenset(
    {
        "generate_series",
        "range",
        "repeat",
        "repeat_row",
        "unnest",
        "json_each",
        "json_tree",
    }
)

_BASELINE_TABLE_FUNCTIONS = frozenset(
    {
        "arrow_scan",
        "arrow_scan_dumb",
        "checkpoint",
        "force_checkpoint",
        "disable_logging",
        "enable_logging",
        "disable_profiling",
        "enable_profiling",
        "truncate_duckdb_logs",
        "json_execute_serialized_sql",
        "pandas_scan",
        "python_map_function",
        "seq_scan",
        "summary",
        "which_secret",
        "icu_calendar_names",
        "pg_timezone_names",
        "query",
        "query_table",
        "sniff_csv",
        "read_duckdb",
        "parquet_file_metadata",
        "parquet_full_metadata",
        "parquet_bloom_probe",
        "pragma_collations",
        "pragma_database_size",
        "pragma_metadata_info",
        "pragma_platform",
        "pragma_show",
        "pragma_storage_info",
        "pragma_table_info",
        "pragma_user_agent",
        "pragma_version",
    }
)


def _enumerate_table_functions() -> frozenset[str]:
    """Return every table-typed function name in the live DuckDB build."""
    try:
        try:
            from backend.core.duckdb import get_memory_connection

            con = get_memory_connection()
        except ImportError:
            con = duckdb.connect(":memory:")
        try:
            rows = con.execute(
                "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_type = 'table'"
            ).fetchall()
        finally:
            con.close()
        names = {r[0].lower() for r in rows if r and isinstance(r[0], str)}
        if names:
            return frozenset(names | _BASELINE_TABLE_FUNCTIONS)
    except Exception:
        logger.exception("[sql_validator] table-function enumeration failed; using baseline set")
    return _BASELINE_TABLE_FUNCTIONS


_TABLE_FUNCTIONS = _enumerate_table_functions()


class SQLValidationError(ValueError):
    """Raised by ``validate_user_sql`` when the input fails any check."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass
class ValidationResult:
    """Successful validation result."""

    parse_tree: dict
    elapsed_ms: float


def validate_user_sql(
    sql: str,
    *,
    parser_con: duckdb.DuckDBPyConnection,
    session_id: str | None = None,
    service_id: str | None = None,
    allowed_tables: frozenset[str] | None = None,
) -> ValidationResult:
    """Validate a user-supplied SQL string against the Decision B policy."""
    if not isinstance(sql, str):
        _reject(sql, "input_type", "SQL must be a string", session_id, service_id)
    if "\x00" in sql:
        _reject(sql, "nul_byte_injection", "query contains a NUL byte", session_id, service_id)

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
    parsed, _node, err_code, err_msg = _parse_single_select_node(sql, parser_con)
    if err_code is not None or parsed is None:
        _reject(sql, err_code or "parse_error", err_msg or "SQL parse error", session_id, service_id)

    _walk_and_validate(parsed, sql, session_id, service_id, allowed_tables=allowed_tables)
    elapsed_ms = (time.monotonic() - t0) * 1000
    if elapsed_ms > 50:
        logger.warning(
            "[sql_validator] slow parse+walk: %.1fms for %d-byte input session=%s service=%s",
            elapsed_ms,
            len(encoded),
            session_id,
            service_id,
        )
    return ValidationResult(parse_tree=parsed, elapsed_ms=elapsed_ms)


def _check_function_node(
    node: dict[str, Any],
    original_sql: str,
    session_id: str | None,
    service_id: str | None,
) -> None:
    fname = node.get("function_name")
    if not isinstance(fname, str):
        return
    fl = fname.lower()
    if fl in _BLOCKED_FUNCTIONS:
        _reject(
            original_sql,
            f"function_denylist:{fl}",
            f"function '{fname}' is not allowed in user queries",
            session_id,
            service_id,
        )
    for prefix in _BLOCKED_FUNCTION_PREFIXES:
        if fl.startswith(prefix):
            _reject(
                original_sql,
                f"function_prefix:{prefix}",
                f"function '{fname}' uses blocked prefix '{prefix}'",
                session_id,
                service_id,
            )
    if (fl in _TABLE_FUNCTIONS or node.get("type") == "TABLE_FUNCTION") and fl not in _ALLOWED_TABLE_FUNCTIONS:
        _reject(
            original_sql,
            f"function_table_not_allowed:{fl}",
            f"table function '{fname}' is not allowed in user queries",
            session_id,
            service_id,
        )


def _walk_and_validate(
    node: Any,
    original_sql: str,
    session_id: str | None,
    service_id: str | None,
    *,
    allowed_tables: frozenset[str] | None = None,
    cte_aliases: frozenset[str] = frozenset(),
) -> None:
    """Recursively visit every dict/list in the parse tree."""
    if isinstance(node, dict):
        if allowed_tables is not None and "cte_map" in node:
            cte_map = node.get("cte_map")
            entries = cte_map.get("map") if isinstance(cte_map, dict) else None
            if isinstance(entries, list):
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    key = entry.get("key")
                    alias_fs = frozenset({key.strip().lower()}) if isinstance(key, str) and key else frozenset()
                    val = entry.get("value")
                    q = val.get("query") if isinstance(val, dict) else None
                    n = q.get("node") if isinstance(q, dict) else None
                    is_recursive = isinstance(n, dict) and n.get("type") == "RECURSIVE_CTE_NODE"
                    if val is not None:
                        _walk_and_validate(
                            val,
                            original_sql,
                            session_id,
                            service_id,
                            allowed_tables=allowed_tables,
                            cte_aliases=(cte_aliases | alias_fs) if is_recursive else cte_aliases,
                        )
                    cte_aliases = cte_aliases | alias_fs

        if node.get("type") == "BASE_TABLE" or "table_name" in node:
            _check_table_reference(
                node, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
            )

        if node.get("class") == "FUNCTION":
            _check_function_node(node, original_sql, session_id, service_id)

        for key, value in node.items():
            if key == "cte_map" and allowed_tables is not None:
                continue
            _walk_and_validate(
                value, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
            )
    elif isinstance(node, list):
        for item in node:
            _walk_and_validate(
                item, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
            )


def _check_table_reference(
    node: dict,
    original_sql: str,
    session_id: str | None,
    service_id: str | None,
    *,
    allowed_tables: frozenset[str] | None = None,
    cte_aliases: frozenset[str] = frozenset(),
) -> None:
    """Validate a BASE_TABLE node's name / schema / catalog fields."""
    table_name = (node.get("table_name") or "").strip()
    schema_name = (node.get("schema_name") or "").strip().lower()
    catalog_name = (node.get("catalog_name") or "").strip().lower()

    if "/" in table_name or "\\" in table_name or "." in table_name:
        _reject(
            original_sql,
            "catalog_blocklist:table_name_path",
            f"table name '{table_name}' contains path-like characters",
            session_id,
            service_id,
        )

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

    if schema_name in _BLOCKED_SCHEMA_NAMES:
        _reject(
            original_sql,
            f"catalog_blocklist:schema_name:{schema_name}",
            f"schema '{schema_name}' is not allowed in user queries",
            session_id,
            service_id,
        )

    if catalog_name and catalog_name != ALLOWED_CATALOG:
        _reject(
            original_sql,
            f"catalog_blocklist:catalog_name:{catalog_name}",
            f"catalog '{catalog_name}' is not allowed (only '{ALLOWED_CATALOG}')",
            session_id,
            service_id,
        )

    if allowed_tables is not None and name_lower and name_lower not in allowed_tables:
        is_cte_ref = name_lower in cte_aliases and not schema_name and not catalog_name
        if not is_cte_ref:
            _reject(
                original_sql,
                "catalog_blocklist:foreign_table",
                f"table '{table_name}' is not in the allowed set",
                session_id,
                service_id,
            )


def _reject(
    sql: str,
    reason: str,
    message: str,
    session_id: str | None,
    service_id: str | None,
) -> NoReturn:
    """Emit a structured audit log line and raise SQLValidationError."""
    sql_str = sql if isinstance(sql, str) else str(sql)
    query_hash = "sha256:" + hashlib.sha256(sql_str.encode("utf-8", errors="replace")).hexdigest()
    _audit_logger.warning(
        "sql_validator_reject reason=%s session=%s service=%s hash=%s len=%d snippet=%r",
        reason,
        session_id or "-",
        service_id or "-",
        query_hash,
        len(sql_str),
        sql_str[:500],
    )
    raise SQLValidationError(reason=reason, message=message)


def apply_user_query_limits(
    con: duckdb.DuckDBPyConnection,
    *,
    memory_limit: str = "2GB",
    timeout_seconds: int = 30,
) -> None:
    """Apply per-connection limits before executing a user-supplied query."""
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    except duckdb.Error as exc:
        logger.warning("[sql_validator] failed to apply memory_limit=%s: %s", memory_limit, exc)
    try:
        con.execute(f"SET statement_timeout = '{timeout_seconds * 1000}ms'")
    except duckdb.Error as exc:
        logger.debug("[sql_validator] statement_timeout not supported on this DuckDB build: %s", exc)


def escape_sql_literal(value: str) -> str:
    """Escape a string for safe inclusion inside a DuckDB single-quoted SQL literal."""
    if not isinstance(value, str):
        raise TypeError(f"escape_sql_literal expected str, got {type(value).__name__}")
    return value.replace("'", "''")


def _parse_single_select_node(
    sql: str,
    parser_con: duckdb.DuckDBPyConnection,
    *,
    allow_multi: bool = False,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None, str | None]:
    """Serialize ``sql`` via ``json_serialize_sql`` and verify the top-level SELECT/UNION node."""
    try:
        row = parser_con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    except Exception as exc:
        return None, None, "parse_error", f"SQL parse failed: {exc}"
    if not row or row[0] is None:
        return None, None, "parse_empty", "SQL parse returned no output"
    try:
        parsed = json.loads(row[0])
    except json.JSONDecodeError as exc:
        return None, None, "parse_invalid_json", f"json_serialize_sql output invalid: {exc}"
    if not isinstance(parsed, dict):
        return None, None, "parse_unexpected_shape", "expected JSON object"
    if parsed.get("error") is True:
        return (
            None,
            None,
            f"parse_error:{parsed.get('error_type', '')}",
            f"SQL parse error: {parsed.get('error_message', '<unknown>')}",
        )

    statements = parsed.get("statements")
    if not isinstance(statements, list) or len(statements) == 0:
        return None, None, "no_statements", "no parseable statements"
    if not allow_multi and len(statements) > 1:
        return None, None, "multi_statement", f"only one statement allowed, got {len(statements)}"

    stmt = statements[0]
    node = stmt.get("node") if isinstance(stmt, dict) else None
    node_type = node.get("type") if isinstance(node, dict) else None
    if not isinstance(node, dict) or node_type not in ALLOWED_STATEMENT_TYPES:
        return (
            None,
            None,
            f"statement_type:{node_type or '?'}",
            f"only SELECT / WITH / UNION statements allowed (got {node_type})",
        )

    from_table = node.get("from_table")
    if isinstance(from_table, dict) and from_table.get("type") == "SHOW_REF":
        return None, None, "show_ref", "SHOW / DESCRIBE / SUMMARIZE statements are not allowed via /api/query"

    return parsed, node, None, None


def is_simple_select_statement(sql: str, *, parser_con: duckdb.DuckDBPyConnection) -> bool:
    """Return True iff ``sql`` parses as a SELECT-like statement that returns a result set."""
    _parsed, node, err_code, _err_msg = _parse_single_select_node(sql, parser_con, allow_multi=True)
    return err_code is None and node is not None


SAFE_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def is_safe_ident(name: object) -> bool:
    """True when ``name`` is a non-empty ``[A-Za-z_][A-Za-z0-9_]*`` string."""
    return isinstance(name, str) and SAFE_IDENT_RE.fullmatch(name) is not None


def require_safe_ident(name: str, label: str) -> str:
    """Return ``name`` unchanged or raise ``ValueError(f"{label}: {name!r}")``."""
    if not is_safe_ident(name):
        raise ValueError(f"{label}: {name!r}")
    return name


sys.modules["backend.utils.sql_ident"] = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.sql_ident = sys.modules[__name__]  # type: ignore[attr-defined]
