"""Pure-Python SQL validation and identifier/literal escaping for user queries."""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import Any, NamedTuple, NoReturn


def escape_sql_literal(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"escape_sql_literal expected str, got {type(value).__name__}")
    return value.replace("'", "''")


SAFE_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def is_safe_ident(name: object) -> bool:
    return isinstance(name, str) and SAFE_IDENT_RE.fullmatch(name) is not None


def require_safe_ident(name: str, label: str) -> str:
    if not is_safe_ident(name):
        raise ValueError(f"{label}: {name!r}")
    return name


logger = logging.getLogger(__name__)
_audit_logger = logging.getLogger("backend.sql_validator.audit")

MAX_INPUT_BYTES = 64 * 1024
MAX_PAREN_DEPTH = 64
ALLOWED_STATEMENT_TYPES = frozenset({"SELECT_NODE", "SET_OPERATION_NODE"})
_BLOCKED_TABLE_PREFIXES = ("pg_",)
_BLOCKED_SCHEMA_NAMES = frozenset({"information_schema", "pg_catalog", "system"})
ALLOWED_CATALOG = "main"

_BLOCKED_FUNCTIONS = frozenset(
    {
        "getenv",
        "current_setting",
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
        "file",
        "filecluster",
        "url",
        "urlcluster",
        "s3",
        "s3cluster",
        "remote",
        "remotesecure",
        "cluster",
        "clusterallreplicas",
        "hdfs",
        "hdfscluster",
        "odbc",
        "jdbc",
        "mysql",
        "postgresql",
        "mongodb",
        "redis",
        "sqlite",
        "executable",
        "executablepool",
        "dictionary",
        "merge",
        "numbers",
        "numbers_mt",
        "zeros",
        "zeros_mt",
        "sleep",
        "sleepeachrow",
        "getsetting",
        "getmacro",
        "in",
        "notin",
        "globalin",
        "globalnotin",
        "nullin",
        "notnullin",
        "globalnullin",
        "globalnotnullin",
        "inignoreset",
        "notinignoreset",
        "globalinignoreset",
        "globalnotinignoreset",
        "nullinignoreset",
        "notnullinignoreset",
        "globalnullinignoreset",
        "globalnotnullinignoreset",
    }
)
_BLOCKED_FUNCTION_PREFIXES = ("read_", "parquet_", "iceberg_", "sniff_", "glob")
_ALLOWED_TABLE_FUNCTIONS = frozenset(
    {"generate_series", "range", "repeat", "repeat_row", "unnest", "json_each", "json_tree"}
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
    return _BASELINE_TABLE_FUNCTIONS


_TABLE_FUNCTIONS = _enumerate_table_functions()


class SQLValidationError(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason, self.message = reason, message


@dataclass
class ValidationResult:
    parse_tree: dict
    elapsed_ms: float


def _reject(sql: str, reason: str, message: str, session_id: str | None, service_id: str | None) -> NoReturn:
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


def validate_user_sql(
    sql: str,
    *,
    parser_con: Any | None = None,
    session_id: str | None = None,
    service_id: str | None = None,
    allowed_tables: AbstractSet[str] | None = None,
) -> ValidationResult:
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
    normalized_allowed = frozenset(t.lower() for t in allowed_tables) if allowed_tables is not None else None
    _walk_and_validate(parsed, sql, session_id, service_id, allowed_tables=normalized_allowed)
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
    node: dict[str, Any], original_sql: str, session_id: str | None, service_id: str | None
) -> None:
    if not isinstance(fname := node.get("function_name"), str):
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
    if blocked_pfx := next((p for p in _BLOCKED_FUNCTION_PREFIXES if fl.startswith(p)), None):
        _reject(
            original_sql,
            f"function_prefix:{blocked_pfx}",
            f"function '{fname}' uses blocked prefix '{blocked_pfx}'",
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


def _check_table_reference(
    node: dict,
    original_sql: str,
    session_id: str | None,
    service_id: str | None,
    *,
    allowed_tables: frozenset[str] | None = None,
    cte_aliases: frozenset[str] = frozenset(),
) -> None:
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
    if not name_lower:
        _reject(
            original_sql,
            "parse_error:SYNTAX",
            "SQL parse error: empty table reference",
            session_id,
            service_id,
        )
    if name_lower.startswith(_BLOCKED_TABLE_PREFIXES):
        blocked_pfx = next(p for p in _BLOCKED_TABLE_PREFIXES if name_lower.startswith(p))
        _reject(
            original_sql,
            f"catalog_blocklist:table_name_prefix:{blocked_pfx}",
            f"table '{table_name}' uses blocked prefix '{blocked_pfx}'",
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
    if (
        allowed_tables is not None
        and name_lower
        and name_lower not in allowed_tables
        and not (name_lower in cte_aliases and not schema_name and not catalog_name)
    ):
        _reject(
            original_sql,
            "catalog_blocklist:foreign_table",
            f"table '{table_name}' is not in the allowed set",
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
    if isinstance(node, dict):
        if allowed_tables is not None and isinstance(cte_map := node.get("cte_map"), dict):
            cte_entries = cte_map.get("map")
            for entry in cte_entries if isinstance(cte_entries, list) else ():
                if not isinstance(entry, dict):
                    continue
                key = entry.get("key")
                alias_fs = frozenset({key.strip().lower()}) if isinstance(key, str) and key else frozenset()
                val = entry.get("value")
                q = val.get("query") if isinstance(val, dict) else None
                n = q.get("node") if isinstance(q, dict) else None
                is_recursive = isinstance(n, dict) and n.get("type") == "RECURSIVE_CTE_NODE"
                if val is not None:
                    if is_recursive and isinstance(n, dict):
                        setop_target: dict[str, Any] | None = None
                        if "left" in n and "right" in n and str(n.get("setop_type", "UNION")).upper() == "UNION":
                            setop_target = n
                        else:
                            inner_q = n.get("query")
                            inner_n = inner_q.get("node") if isinstance(inner_q, dict) else None
                            if (
                                isinstance(inner_n, dict)
                                and inner_n.get("type") == "SET_OPERATION_NODE"
                                and "left" in inner_n
                                and "right" in inner_n
                                and str(inner_n.get("setop_type", "UNION")).upper() == "UNION"
                            ):
                                setop_target = inner_n
                        if (
                            setop_target is not None
                            and isinstance(setop_target.get("right"), dict)
                            and setop_target["right"].get("type") == "SET_OPERATION_NODE"
                        ):
                            setop_target = None
                        if setop_target is not None:
                            _walk_and_validate(
                                setop_target["left"],
                                original_sql,
                                session_id,
                                service_id,
                                allowed_tables=allowed_tables,
                                cte_aliases=cte_aliases,
                            )
                            _walk_and_validate(
                                setop_target["right"],
                                original_sql,
                                session_id,
                                service_id,
                                allowed_tables=allowed_tables,
                                cte_aliases=cte_aliases | alias_fs,
                            )
                            for k_rem, v_rem in n.items():
                                if k_rem not in ("left", "right", "query"):
                                    _walk_and_validate(
                                        v_rem,
                                        original_sql,
                                        session_id,
                                        service_id,
                                        allowed_tables=allowed_tables,
                                        cte_aliases=cte_aliases,
                                    )
                        else:
                            _walk_and_validate(
                                val,
                                original_sql,
                                session_id,
                                service_id,
                                allowed_tables=allowed_tables,
                                cte_aliases=cte_aliases,
                            )
                    else:
                        _walk_and_validate(
                            val,
                            original_sql,
                            session_id,
                            service_id,
                            allowed_tables=allowed_tables,
                            cte_aliases=cte_aliases,
                        )
                cte_aliases = cte_aliases | alias_fs
        if node.get("type") == "DISALLOWED_SUBQUERY_STATEMENT":
            _reject(
                original_sql,
                str(node.get("reason") or "statement_not_allowed"),
                str(node.get("message") or "statement is not allowed in user queries"),
                session_id,
                service_id,
            )
        if node.get("type") == "PARSE_ERROR_NODE":
            _reject(
                original_sql,
                "parse_error:SYNTAX",
                str(node.get("message") or "SQL parse error"),
                session_id,
                service_id,
            )
        if node.get("type") == "EXPR_CTE_TABLE_ALIAS":
            target_type = str(node.get("target_type") or "")
            alias_low = str(node.get("alias") or "").strip().lower()
            if target_type == "PARSE_ERROR_NODE":
                _reject(
                    original_sql,
                    "parse_error:SYNTAX",
                    str(node.get("target_message") or "SQL parse error"),
                    session_id,
                    service_id,
                )
            if target_type == "BASE_TABLE":
                target_node = {
                    "type": "BASE_TABLE",
                    "table_name": node.get("target_table") or "",
                    "schema_name": node.get("target_schema") or "",
                    "catalog_name": node.get("target_catalog") or "",
                }
                # Always enforce schema/catalog/prefix blocklist on expression-CTE table refs,
                # and enforce allowed_tables whenever an expression CTE shadows an allowed table.
                eff_allowed = allowed_tables if (allowed_tables is not None and alias_low in allowed_tables) else None
                _check_table_reference(
                    target_node,
                    original_sql,
                    session_id,
                    service_id,
                    allowed_tables=eff_allowed,
                    cte_aliases=frozenset(),
                )
            return
        if node.get("type") == "BASE_TABLE" or "table_name" in node:
            _check_table_reference(
                node, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
            )
        if node.get("class") == "FUNCTION":
            _check_function_node(node, original_sql, session_id, service_id)
        for key, value in node.items():
            if isinstance(value, (dict, list)) and not (key == "cte_map" and allowed_tables is not None):
                _walk_and_validate(
                    value, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
                )
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, (dict, list)):
                _walk_and_validate(
                    item, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
                )


# ── Pure-Python SQL tokenizer & AST builder ──────────────────────────────────

_NON_SELECT_STATEMENTS = frozenset(
    {
        "ATTACH",
        "DETACH",
        "INSTALL",
        "UNINSTALL",
        "LOAD",
        "PRAGMA",
        "SET",
        "RESET",
        "UPDATE",
        "INSERT",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "COPY",
        "EXPORT",
        "IMPORT",
        "CALL",
        "EXPLAIN",
        "VACUUM",
        "ANALYZE",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "TRANSACTION",
        "GRANT",
        "REVOKE",
        "TRUNCATE",
        "USE",
        "CHECKPOINT",
        "MERGE",
        "REPLACE",
        "PREPARE",
        "EXECUTE",
        "DEALLOCATE",
        "PIVOT",
        "UNPIVOT",
        "PIVOT_WIDER",
        "PIVOT_LONGER",
    }
)
_SHOW_REF_STATEMENTS = frozenset({"SHOW", "DESCRIBE", "SUMMARIZE", "DESC"})
_CLAUSE_STOP_KEYWORDS = frozenset(
    {
        "WHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "WINDOW",
        "QUALIFY",
        "SETTINGS",
        "FORMAT",
        "PREWHERE",
        "SAMPLE",
        "INTO",
        "FOR",
    }
)
_JOIN_MODIFIERS = frozenset(
    {
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "CROSS",
        "NATURAL",
        "GLOBAL",
        "ANY",
        "ALL",
        "ASOF",
        "SEMI",
        "ANTI",
        "POSITIONAL",
        "PASTE",
    }
)
_SPECIAL_CALL_KEYWORDS = frozenset(
    {"cast", "try_cast", "extract", "position", "substring", "trim", "btrim", "ltrim", "rtrim", "overlay"}
)
_NON_FUNCTION_KEYWORDS = _SPECIAL_CALL_KEYWORDS | frozenset(
    {
        "select",
        "from",
        "where",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "union",
        "intersect",
        "except",
        "with",
        "as",
        "on",
        "using",
        "in",
        "not",
        "and",
        "or",
        "case",
        "when",
        "then",
        "else",
        "end",
        "exists",
        "filter",
        "over",
        "within",
        "values",
        "table",
        "distinct",
        "all",
        "between",
        "like",
        "ilike",
    }
)
_SUBQUERY_HEAD_KEYWORDS = frozenset(
    {
        "SELECT",
        "WITH",
        "FROM",
        "TABLE",
        "VALUES",
        "SHOW",
        "DESCRIBE",
        "SUMMARIZE",
        "DESC",
        "PIVOT",
        "UNPIVOT",
        "PIVOT_WIDER",
        "PIVOT_LONGER",
    }
)
_ON_EXPR_PREV_WORDS = frozenset(
    {
        "ON",
        "AND",
        "OR",
        "XOR",
        "NOT",
        "IS",
        "IN",
        "LIKE",
        "ILIKE",
        "RLIKE",
        "REGEXP",
        "GLOB",
        "SIMILAR",
        "TO",
        "ESCAPE",
        "BETWEEN",
        "SYMMETRIC",
        "ASYMMETRIC",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "DIV",
        "MOD",
        "FROM",
        "COLLATE",
        "ZONE",
        "INTERVAL",
    }
)
_ON_EXPR_NEXT_WORDS = frozenset(
    {
        "NOT",
        "AT",
        "IS",
        "IN",
        "BETWEEN",
        "LIKE",
        "ILIKE",
        "RLIKE",
        "REGEXP",
        "GLOB",
        "SIMILAR",
        "DIV",
        "MOD",
        "COLLATE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AND",
        "OR",
        "XOR",
    }
)
_AS_CLAUSE_OPENING_WORDS = frozenset(
    {
        "SELECT",
        "WITH",
        "WHERE",
        "PREWHERE",
        "HAVING",
        "QUALIFY",
        "LIMIT",
        "OFFSET",
        "BY",
        "FROM",
        "JOIN",
        "TABLE",
        "USING",
    }
)
_SQL_INTERVAL_UNITS = frozenset(
    {
        "YEAR",
        "YEARS",
        "QUARTER",
        "QUARTERS",
        "MONTH",
        "MONTHS",
        "WEEK",
        "WEEKS",
        "DAY",
        "DAYS",
        "HOUR",
        "HOURS",
        "MINUTE",
        "MINUTES",
        "SECOND",
        "SECONDS",
        "MILLISECOND",
        "MILLISECONDS",
        "MICROSECOND",
        "MICROSECONDS",
        "NANOSECOND",
        "NANOSECONDS",
    }
)
_ON_OPERATOR_CONTEXT_WORDS = frozenset(
    {
        "SELECT",
        "DISTINCT",
        "ALL",
        "WHERE",
        "PREWHERE",
        "HAVING",
        "QUALIFY",
        "ON",
        "AND",
        "OR",
        "XOR",
        "NOT",
        "IS",
        "IN",
        "LIKE",
        "ILIKE",
        "RLIKE",
        "REGEXP",
        "GLOB",
        "SIMILAR",
        "TO",
        "BETWEEN",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "DIV",
        "MOD",
        "COLLATE",
    }
)
_FROM_FIRST_SELECT_FN_KEYWORDS = frozenset({"ANY", "ARRAY", "LEFT", "RIGHT", "FORMAT", "SAMPLE", "WINDOW"})
_DUCKDB_UNICODE_SPACES = frozenset(
    "\u00a0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b\u202f\u205f\u2060\u3000\ufeff"
)
_DISALLOWED_CTRL_OR_SEP_RE = re.compile(r"[\x00-\x08\x0b\x0e-\x1f\x7f-\x9f\u1680\u180e\u2028\u2029]")
_DOLQ_TAG_RE = re.compile(r"\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?[$]")


class _Tok(NamedTuple):
    kind: str  # "WORD" | "STRING" | "QIDENT" | "LPAREN" | "RPAREN" | "LBRACK" | "RBRACK" | "LBRACE" | "RBRACE" | "COMMA" | "DOT" | "SEMI" | "OTHER"
    val: str


def _is_sql_horiz_space(ch: str) -> bool:
    return ch in (" ", "\t", "\f") or ch in _DUCKDB_UNICODE_SPACES


def _is_sql_whitespace(ch: str) -> bool:
    return ch in ("\n", "\r") or _is_sql_horiz_space(ch)


def _is_ident_start_char(ch: str) -> bool:
    return (
        bool(ch)
        and not _is_sql_whitespace(ch)
        and _DISALLOWED_CTRL_OR_SEP_RE.match(ch) is None
        and (ch.isalpha() or ch == "_" or ord(ch) >= 0x80)
    )


def _is_ident_cont_char(ch: str) -> bool:
    return (
        bool(ch)
        and not _is_sql_whitespace(ch)
        and _DISALLOWED_CTRL_OR_SEP_RE.match(ch) is None
        and (ch.isalnum() or ch in ("_", "$") or ord(ch) >= 0x80)
    )


def _scan_sql_digits(sql: str, start: int) -> int:
    n = len(sql)
    i = start
    while i < n and sql[i].isascii() and sql[i].isdigit():
        i += 1
        if i + 1 < n and sql[i] == "_" and sql[i + 1].isascii() and sql[i + 1].isdigit():
            i += 1
    return i


def _scan_sql_number_end(sql: str, start: int) -> int:
    n = len(sql)
    if sql[start] == "0" and start + 2 < n and sql[start + 1] in ("x", "X", "b", "B", "o", "O"):
        radix = sql[start + 1]
        allowed = "0123456789abcdefABCDEF" if radix in ("x", "X") else ("01" if radix in ("b", "B") else "01234567")
        if sql[start + 2].isascii() and sql[start + 2] in allowed:
            i = start + 2
            while i < n and sql[i].isascii() and (sql[i] in allowed or sql[i] == "_"):
                i += 1
            if radix in ("x", "X"):
                if i < n and sql[i] == "." and not (i + 1 < n and sql[i + 1] == "."):
                    i += 1
                    while i < n and sql[i].isascii() and (sql[i] in allowed or sql[i] == "_"):
                        i += 1
                if i < n and sql[i] in ("p", "P"):
                    if i + 1 < n and sql[i + 1].isascii() and sql[i + 1].isdigit():
                        i = _scan_sql_digits(sql, i + 1)
                    elif i + 2 < n and sql[i + 1] in ("+", "-") and sql[i + 2].isascii() and sql[i + 2].isdigit():
                        i = _scan_sql_digits(sql, i + 2)
            return i
    i = _scan_sql_digits(sql, start)
    if i < n and sql[i] == "." and not (i + 1 < n and sql[i + 1] == "."):
        i += 1
        if i < n and sql[i].isascii() and sql[i].isdigit():
            i = _scan_sql_digits(sql, i)
    if i < n and sql[i] in ("e", "E"):
        if i + 1 < n and sql[i + 1].isascii() and sql[i + 1].isdigit():
            i = _scan_sql_digits(sql, i + 1)
        elif i + 2 < n and sql[i + 1] in ("+", "-") and sql[i + 2].isascii() and sql[i + 2].isdigit():
            i = _scan_sql_digits(sql, i + 2)
    return i


def _is_num_tok_val(val: str) -> bool:
    return bool(val) and (val[0].isdigit() or (len(val) >= 2 and val[0] == "." and val[1].isdigit()))


def _scan_sql_leading_dot_float_end(sql: str, dot_idx: int) -> int:
    n = len(sql)
    i = _scan_sql_digits(sql, dot_idx + 1)
    if i < n and sql[i] in ("e", "E"):
        if i + 1 < n and sql[i + 1].isascii() and sql[i + 1].isdigit():
            i = _scan_sql_digits(sql, i + 1)
        elif i + 2 < n and sql[i + 1] in ("+", "-") and sql[i + 2].isascii() and sql[i + 2].isdigit():
            i = _scan_sql_digits(sql, i + 2)
    return i


def _find_quote_continuation(sql: str, pos: int) -> int:
    n = len(sql)
    j = pos
    saw_newline = False
    while j < n:
        c = sql[j]
        if _is_sql_horiz_space(c):
            j += 1
            continue
        if c in ("\n", "\r"):
            saw_newline = True
            j += 1
            continue
        if c == "-" and j + 1 < n and sql[j + 1] == "-":
            j += 2
            while j < n and sql[j] not in ("\n", "\r"):
                j += 1
            continue
        break
    return j if (saw_newline and j < n and sql[j] == "'") else -1


_FAST_ASCII_STEP_RE = re.compile(
    r"(?P<WS>[ \t\n\r\f]+)|(?P<PUNCT>[()\[\]{},;])|(?P<WORD>[A-Za-z_][A-Za-z0-9_$]*)|(?P<DOT>\.)|(?P<NUM>[0-9]+)"
)
_FAST_PURE_ASCII_SPECIAL_RE = re.compile(r"['\"`$\\]|--|/\*|\.[0-9]|[0-9][.eExXbBoO_]|[\x00-\x08\x0b\x0e-\x1f\x7f]")
_FAST_PURE_ASCII_RE = re.compile(
    r"(?P<WS>[ \t\n\r\f]+)"
    r"|(?P<WORD>[A-Za-z_][A-Za-z0-9_]*)"
    r"|(?P<DOT>\.)"
    r"|(?P<PUNCT>[()\[\]{},;])"
    r"|(?P<NUM>[0-9]+)"
    r"|(?P<OTHER>[-/]|[^ \t\n\r\f()\[\]{},;.\-/$A-Za-z0-9_]+)"
)
_PUNCT_TO_KIND = {
    "(": "LPAREN",
    ")": "RPAREN",
    "[": "LBRACK",
    "]": "RBRACK",
    "{": "LBRACE",
    "}": "RBRACE",
    ",": "COMMA",
    ";": "SEMI",
}
_DOT_TOK = _Tok("DOT", ".")
_PUNCT_TOKS = {ch: _Tok(kind, ch) for ch, kind in _PUNCT_TO_KIND.items()}


def _tokenize_sql(sql: str) -> tuple[list[_Tok] | None, str | None]:
    punct_toks = _PUNCT_TOKS
    if sql.isascii() and _FAST_PURE_ASCII_SPECIAL_RE.search(sql) is None:
        tok_new = tuple.__new__
        tok_cls = _Tok
        dot_tok = _DOT_TOK
        tokens_fast: list[_Tok] = []
        append_f = tokens_fast.append
        word_cache: dict[str, _Tok] = {}
        other_cache: dict[str, _Tok] = {}
        for m in _FAST_PURE_ASCII_RE.finditer(sql):
            grp = m.lastgroup
            if grp == "WS":
                continue
            if grp == "DOT":
                append_f(dot_tok)
                continue
            val = m.group(0)
            if grp == "WORD":
                wtok = word_cache.get(val)
                if wtok is None:
                    if len(val) == 7 and val.lower() == "uescape":
                        return None, "UESCAPE is not allowed"
                    wtok = tok_new(tok_cls, ("WORD", val))
                    word_cache[val] = wtok
                append_f(wtok)
            elif grp == "PUNCT":
                append_f(punct_toks[val])
            else:
                otok = other_cache.get(val)
                if otok is None:
                    otok = tok_new(tok_cls, ("OTHER", val))
                    other_cache[val] = otok
                append_f(otok)
        return tokens_fast, None
    punct_to_kind = _PUNCT_TO_KIND
    tokens: list[_Tok] = []
    append_tok = tokens.append
    fast_match = _FAST_ASCII_STEP_RE.match
    i = 0
    n = len(sql)
    prev_was_ident_or_num = False
    while i < n:
        m_fast = fast_match(sql, i)
        if m_fast is not None:
            grp = m_fast.lastgroup
            end_i = m_fast.end()
            if grp == "WS":
                i = end_i
                prev_was_ident_or_num = False
                continue
            if grp == "PUNCT":
                ch_p = m_fast.group(0)
                append_tok(_Tok(punct_to_kind[ch_p], ch_p))
                i = end_i
                prev_was_ident_or_num = False
                continue
            if grp == "DOT":
                if not (
                    not prev_was_ident_or_num
                    and (not tokens or tokens[-1].kind not in ("RPAREN", "RBRACK", "RBRACE", "QIDENT"))
                    and end_i < n
                    and "0" <= sql[end_i] <= "9"
                ):
                    append_tok(_Tok("DOT", "."))
                    i = end_i
                    prev_was_ident_or_num = False
                    continue
            elif grp == "WORD":
                word = m_fast.group(0)
                if not (
                    (
                        not prev_was_ident_or_num
                        and len(word) == 1
                        and word in ("E", "e")
                        and end_i < n
                        and sql[end_i] == "'"
                    )
                    or (
                        not prev_was_ident_or_num
                        and len(word) == 1
                        and word in ("U", "u")
                        and end_i + 1 < n
                        and sql[end_i] == "&"
                        and sql[end_i + 1] in ("'", '"')
                    )
                    or (end_i < n and ord(sql[end_i]) >= 0x80)
                ):
                    if len(word) == 7 and word.lower() == "uescape":
                        return None, "UESCAPE is not allowed"
                    append_tok(_Tok("WORD", word))
                    i = end_i
                    prev_was_ident_or_num = True
                    continue
            elif grp == "NUM":
                if not (end_i < n and sql[end_i] in ".eExXbBoO_"):
                    append_tok(_Tok("OTHER", m_fast.group(0)))
                    i = end_i
                    prev_was_ident_or_num = True
                    continue
        ch = sql[i]
        at_word_start = not prev_was_ident_or_num
        if _is_sql_whitespace(ch):
            i += 1
            prev_was_ident_or_num = False
            continue
        if _DISALLOWED_CTRL_OR_SEP_RE.match(ch) is not None:
            return None, "unsupported control or separator character"
        if at_word_start and ch in ("U", "u") and i + 2 < n and sql[i + 1] == "&" and sql[i + 2] in ("'", '"'):
            return None, "unicode escape literals are not allowed"
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            j = i + 2
            while j < n and sql[j] not in ("\n", "\r"):
                j += 1
            i = j
            prev_was_ident_or_num = False
            continue
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            depth = 1
            j = i + 2
            while j < n and depth > 0:
                if sql[j] == "/" and j + 1 < n and sql[j + 1] == "*":
                    depth += 1
                    j += 2
                elif sql[j] == "*" and j + 1 < n and sql[j + 1] == "/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            if depth != 0:
                return None, "unterminated block comment"
            i = j
            prev_was_ident_or_num = False
            continue
        if (at_word_start and ch in ("E", "e") and i + 1 < n and sql[i + 1] == "'") or ch == "'":
            is_escape_str = ch in ("E", "e")
            j = i + 2 if is_escape_str else i + 1
            buf: list[str] = []
            while True:
                closed = False
                bs_run = 0
                while j < n:
                    if is_escape_str and sql[j] == "\\":
                        if j + 1 >= n:
                            return None, "unterminated escape string literal"
                        buf.append(sql[j + 1])
                        j += 2
                        continue
                    if not is_escape_str and sql[j] == "\\":
                        bs_run += 1
                        buf.append("\\")
                        j += 1
                        continue
                    if sql[j] == "'":
                        if not is_escape_str and (bs_run % 2) == 1:
                            return None, "ambiguous backslash-escaped quote in string literal"
                        bs_run = 0
                        if j + 1 < n and sql[j + 1] == "'":
                            buf.append("'")
                            j += 2
                            continue
                        closed = True
                        j += 1
                        break
                    bs_run = 0
                    buf.append(sql[j])
                    j += 1
                if not closed:
                    return None, "unterminated string literal"
                cont_quote = _find_quote_continuation(sql, j)
                if cont_quote == -1:
                    break
                j = cont_quote + 1
            tokens.append(_Tok("STRING", "".join(buf)))
            i = j
            prev_was_ident_or_num = False
            continue
        if ch == "$" and at_word_start:
            m_dolq = _DOLQ_TAG_RE.match(sql, i)
            if m_dolq is not None:
                tag = m_dolq.group(0)
                end_idx = sql.find(tag, i + len(tag))
                if end_idx == -1:
                    return None, "unterminated dollar-quoted string literal"
                tokens.append(_Tok("STRING", sql[i + len(tag) : end_idx]))
                i = end_idx + len(tag)
                prev_was_ident_or_num = False
                continue
        if ch == '"':
            j = i + 1
            buf = []
            closed = False
            while j < n:
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        buf.append('"')
                        j += 2
                        continue
                    closed = True
                    j += 1
                    break
                buf.append(sql[j])
                j += 1
            if not closed:
                return None, "unterminated quoted identifier"
            tokens.append(_Tok("QIDENT", "".join(buf)))
            i = j
            prev_was_ident_or_num = False
            continue
        if ch == "`":
            j = i + 1
            buf = []
            closed = False
            while j < n:
                if sql[j] == "`":
                    if j + 1 < n and sql[j + 1] == "`":
                        buf.append("`")
                        j += 2
                        continue
                    closed = True
                    j += 1
                    break
                buf.append(sql[j])
                j += 1
            if not closed:
                return None, "unterminated quoted identifier"
            tokens.append(_Tok("QIDENT", "".join(buf)))
            i = j
            prev_was_ident_or_num = False
            continue
        if ch == "(":
            tokens.append(_Tok("LPAREN", "("))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == ")":
            tokens.append(_Tok("RPAREN", ")"))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == "[":
            tokens.append(_Tok("LBRACK", "["))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == "]":
            tokens.append(_Tok("RBRACK", "]"))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == "{":
            tokens.append(_Tok("LBRACE", "{"))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == "}":
            tokens.append(_Tok("RBRACE", "}"))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == ",":
            tokens.append(_Tok("COMMA", ","))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == ".":
            if (
                not prev_was_ident_or_num
                and (not tokens or tokens[-1].kind not in ("RPAREN", "RBRACK", "RBRACE", "QIDENT"))
                and i + 1 < n
                and sql[i + 1].isascii()
                and sql[i + 1].isdigit()
            ):
                num_end = _scan_sql_leading_dot_float_end(sql, i)
                tokens.append(_Tok("OTHER", sql[i:num_end]))
                i = num_end
                prev_was_ident_or_num = True
                continue
            tokens.append(_Tok("DOT", "."))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == ";":
            tokens.append(_Tok("SEMI", ";"))
            i += 1
            prev_was_ident_or_num = False
            continue
        if _is_ident_start_char(ch):
            j = i + 1
            while j < n and _is_ident_cont_char(sql[j]):
                j += 1
            word = sql[i:j]
            if word.lower() == "uescape":
                return None, "UESCAPE is not allowed"
            tokens.append(_Tok("WORD", word))
            i = j
            prev_was_ident_or_num = True
            continue
        if ch.isdigit():
            num_end = _scan_sql_number_end(sql, i)
            tokens.append(_Tok("OTHER", sql[i:num_end]))
            i = num_end
            prev_was_ident_or_num = True
            continue
        j = i + 1
        while (
            j < n
            and not _is_sql_whitespace(sql[j])
            and _DISALLOWED_CTRL_OR_SEP_RE.match(sql[j]) is None
            and sql[j] not in ("'", '"', "`", "(", ")", "[", "]", "{", "}", ",", ".", ";", "-", "/", "$")
            and not _is_ident_start_char(sql[j])
            and not sql[j].isdigit()
        ):
            j += 1
        tokens.append(_Tok("OTHER", sql[i:j]))
        i = j
        prev_was_ident_or_num = False
    return tokens, None


_DELIM_PAIRS = {"LPAREN": "RPAREN", "LBRACK": "RBRACK", "LBRACE": "RBRACE"}
_CLOSING_DELIMS = frozenset(_DELIM_PAIRS.values())
_TOK_META_TLS = threading.local()


def _build_tok_meta(tokens: list[_Tok]) -> tuple[dict[int, int], list[int], dict[int, bool]]:
    n = len(tokens)
    match_map: dict[int, int] = {}
    stack: list[tuple[int, str]] = []
    as_run: list[int] = [0] * n
    run = 0
    delim_pairs = _DELIM_PAIRS
    closing_delims = _CLOSING_DELIMS
    for idx, tk in enumerate(tokens):
        if run:
            as_run[idx] = run
        k = tk.kind
        if k == "WORD":
            if len(tk.val) == 2 and tk.val.upper() == "AS":
                run += 1
            else:
                run = 0
        else:
            run = 0
            if k in delim_pairs:
                stack.append((idx, delim_pairs[k]))
            elif k in closing_delims:
                if stack and stack[-1][1] == k:
                    open_idx, _ = stack.pop()
                    match_map[open_idx] = idx
                    match_map[idx] = open_idx
                else:
                    stack.clear()
    return match_map, as_run, {}


def _get_tok_meta_entry(
    tokens: list[_Tok],
) -> tuple[dict[int, int], list[int], dict[int, bool], dict[str, Any] | None]:
    cache: dict[int, tuple[list[_Tok], dict[int, int], list[int], dict[int, bool], dict[str, Any]]] | None = getattr(
        _TOK_META_TLS, "cache", None
    )
    if cache is not None:
        last = getattr(_TOK_META_TLS, "last", None)
        if last is not None and last[0] is tokens:
            return last[1], last[2], last[3], last[4]
        entry = cache.get(id(tokens))
        if entry is not None and entry[0] is tokens:
            _TOK_META_TLS.last = entry
            return entry[1], entry[2], entry[3], entry[4]
        match_map, as_run, memo = _build_tok_meta(tokens)
        case_state: dict[str, Any] = {
            "ready": False,
            "building": False,
            "map": {},
            "on_op": {},
            "fact": {},
            "clause_open": {},
            "pos_in_ready": False,
            "pos_in_idx": None,
        }
        new_entry = (tokens, match_map, as_run, memo, case_state)
        cache[id(tokens)] = new_entry
        _TOK_META_TLS.last = new_entry
        return match_map, as_run, memo, case_state
    match_map, as_run, memo = _build_tok_meta(tokens)
    return match_map, as_run, memo, None


def _get_tok_meta(tokens: list[_Tok]) -> tuple[dict[int, int], list[int], dict[int, bool]]:
    match_map, as_run, memo, _ = _get_tok_meta_entry(tokens)
    return match_map, as_run, memo


def _find_matching_rdelim(tokens: list[_Tok], open_idx: int) -> int:
    if open_idx < 0 or open_idx >= len(tokens) or tokens[open_idx].kind not in _DELIM_PAIRS:
        return -1
    match_map, _, _ = _get_tok_meta(tokens)
    close_idx = match_map.get(open_idx, -1)
    if close_idx > open_idx:
        return close_idx
    return -1


def _find_matching_rparen(tokens: list[_Tok], lparen_idx: int) -> int:
    if lparen_idx < 0 or lparen_idx >= len(tokens) or tokens[lparen_idx].kind != "LPAREN":
        return -1
    return _find_matching_rdelim(tokens, lparen_idx)


def _find_matching_lparen(tokens: list[_Tok], rparen_idx: int) -> int:
    if rparen_idx < 0 or rparen_idx >= len(tokens) or tokens[rparen_idx].kind != "RPAREN":
        return -1
    match_map, _, _ = _get_tok_meta(tokens)
    open_idx = match_map.get(rparen_idx, -1)
    if 0 <= open_idx < rparen_idx and tokens[open_idx].kind == "LPAREN":
        return open_idx
    return -1


def _is_kw_word_tok(tokens: list[_Tok], idx: int, *words: str) -> bool:
    return (
        0 <= idx < len(tokens)
        and tokens[idx].kind == "WORD"
        and tokens[idx].val.upper() in words
        and not _is_non_keyword_ident_pos(tokens, idx)
    )


def _is_select_head_top_prefix_tok(tokens: list[_Tok], top_idx: int) -> bool:
    if top_idx < 1 or not _is_kw_word_tok(tokens, top_idx, "TOP"):
        return False
    if _is_kw_word_tok(tokens, top_idx - 1, "SELECT"):
        return True
    if top_idx >= 2 and _is_kw_word_tok(tokens, top_idx - 1, "ALL", "DISTINCT"):
        return _is_kw_word_tok(tokens, top_idx - 2, "SELECT")
    if tokens[top_idx - 1].kind == "RPAREN":
        lp = _find_matching_lparen(tokens, top_idx - 1)
        return (
            lp >= 3
            and _is_kw_word_tok(tokens, lp - 1, "ON")
            and _is_kw_word_tok(tokens, lp - 2, "DISTINCT")
            and _is_kw_word_tok(tokens, lp - 3, "SELECT")
        )
    return False


def _is_select_head_signed_top_end_tok(tokens: list[_Tok], pos: int) -> bool:
    if pos < 2 or pos >= len(tokens):
        return False
    tk = tokens[pos]
    return (
        tk.kind == "OTHER"
        and _is_num_tok_val(tk.val)
        and tokens[pos - 1].kind == "OTHER"
        and tokens[pos - 1].val in ("+", "-")
        and _is_select_head_top_prefix_tok(tokens, pos - 2)
    )


def _is_select_head_top_end_tok(tokens: list[_Tok], pos: int) -> bool:
    if pos < 1 or pos >= len(tokens):
        return False
    tk = tokens[pos]
    if tk.kind == "OTHER" and _is_num_tok_val(tk.val):
        prev_idx = pos - 1
        if prev_idx >= 1 and tokens[prev_idx].kind == "OTHER" and tokens[prev_idx].val in ("+", "-"):
            prev_idx -= 1
        return _is_select_head_top_prefix_tok(tokens, prev_idx)
    if tk.kind == "RPAREN":
        lp = _find_matching_lparen(tokens, pos)
        return lp >= 2 and _is_select_head_top_prefix_tok(tokens, lp - 1)
    return False


def _is_select_head_modifier_end_tok(tokens: list[_Tok], pos: int) -> bool:
    if pos < 1 or pos >= len(tokens):
        return False
    if _is_select_head_top_end_tok(tokens, pos):
        return True
    tk = tokens[pos]
    if tk.kind == "RPAREN":
        lp = _find_matching_lparen(tokens, pos)
        return (
            lp >= 3
            and _is_kw_word_tok(tokens, lp - 1, "ON")
            and _is_kw_word_tok(tokens, lp - 2, "DISTINCT")
            and _is_kw_word_tok(tokens, lp - 3, "SELECT")
        )
    if pos >= 3 and _is_kw_word_tok(tokens, pos, "TIES") and _is_kw_word_tok(tokens, pos - 1, "WITH"):
        return _is_select_head_top_end_tok(tokens, pos - 2)
    return False


def _is_bang_tok(tokens: list[_Tok], pos: int) -> bool:
    return (
        0 <= pos < len(tokens)
        and tokens[pos].kind == "OTHER"
        and bool(tokens[pos].val)
        and tokens[pos].val.strip("!") == ""
    )


def _is_postfix_factorial_tok(tokens: list[_Tok], pos: int) -> bool:
    if pos <= 0 or pos >= len(tokens) or not _is_bang_tok(tokens, pos):
        return False
    return _eval_on_op_and_fact(tokens, "fact", pos)


def _strip_outer_parens(tokens: list[_Tok]) -> list[_Tok]:
    n = len(tokens)
    if n < 2 or tokens[0].kind != "LPAREN":
        return tokens
    match_map, _, _ = _get_tok_meta(tokens)
    lo = 0
    hi = n - 1
    while lo < hi and tokens[lo].kind == "LPAREN" and match_map.get(lo) == hi:
        lo += 1
        hi -= 1
    return tokens if (lo == 0 and hi == n - 1) else tokens[lo : hi + 1]


def _is_dot_or_typecast_tok(tokens: list[_Tok], pos: int) -> bool:
    if pos < 0:
        return False
    tk = tokens[pos]
    return tk.kind == "DOT" or (tk.kind == "OTHER" and tk.val.endswith("::") and not tk.val.endswith(":::"))


def _cannot_end_aliasable_tok(tokens: list[_Tok], pos: int, r: int, after_as_idx: int) -> bool:
    if pos < 0 or _is_dot_or_typecast_tok(tokens, pos):
        return True
    tk = tokens[pos]
    if tk.kind in ("COMMA", "SEMI", "LPAREN", "LBRACK", "LBRACE"):
        return True
    if tk.kind == "OTHER" and not _is_num_tok_val(tk.val):
        if tk.val == "!" and (r % 2) == 0:
            return False
        return not _is_postfix_factorial_tok(tokens, pos)
    if (
        tk.kind in ("OTHER", "RPAREN") or (tk.kind == "WORD" and len(tk.val) == 4 and tk.val.upper() == "TIES")
    ) and _is_select_head_modifier_end_tok(tokens, pos):
        if (r % 2) == 0:
            return False
        if r == 1 and _is_select_head_signed_top_end_tok(tokens, pos):
            after_tok = tokens[after_as_idx] if after_as_idx < len(tokens) else None
            if after_tok is not None and after_tok.kind == "WORD" and after_tok.val.upper() in ("WHERE", "SELECT"):
                nxt_after = tokens[after_as_idx + 1] if after_as_idx + 1 < len(tokens) else None
                if (
                    nxt_after is None
                    or nxt_after.kind in ("COMMA", "RPAREN", "SEMI")
                    or (
                        nxt_after.kind == "WORD"
                        and nxt_after.val.upper()
                        in ("FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION", "INTERSECT", "EXCEPT")
                    )
                ):
                    return False
        return True
    if tk.kind != "WORD" or _is_non_keyword_ident_pos(tokens, pos):
        return False
    wu = tk.val.upper()
    after_tok = tokens[after_as_idx] if after_as_idx < len(tokens) else None
    if wu == "CASE":
        return after_tok is not None and after_tok.kind == "WORD" and after_tok.val.upper() == "WHEN"
    if wu == "NOT":
        if r == 2 and (
            after_tok is None
            or after_tok.kind in ("COMMA", "RPAREN", "SEMI")
            or (
                after_tok.kind == "WORD"
                and (after_tok.val.upper() in _CLAUSE_STOP_KEYWORDS or after_tok.val.upper() in ("FROM", "JOIN"))
            )
        ):
            return False
        return True
    if wu == "INTERVAL":
        return (
            (r % 2) == 1
            and after_tok is not None
            and after_tok.kind == "WORD"
            and after_tok.val.upper() in _SQL_INTERVAL_UNITS
        )
    if _is_prev_word_operator_at(tokens, pos, after_tok):
        return True
    if wu in ("DISTINCT", "ALL"):
        return (
            pos >= 1
            and tokens[pos - 1].kind == "WORD"
            and tokens[pos - 1].val.upper() in ("SELECT", "UNION", "INTERSECT", "EXCEPT")
            and not _is_non_keyword_ident_pos(tokens, pos - 1)
        )
    if wu in _AS_CLAUSE_OPENING_WORDS:
        return _is_clause_open_word_at(tokens, pos)
    return False


def _is_clause_open_word_at(tokens: list[_Tok], pos: int) -> bool:
    """Return True when ``tokens[pos]`` (in ``_AS_CLAUSE_OPENING_WORDS``) acts as a clause opener."""
    _, _, _, case_state = _get_tok_meta_entry(tokens)
    co_memo: dict[int, bool] | None = case_state["clause_open"] if case_state is not None else None
    if co_memo is not None and pos in co_memo:
        return co_memo[pos]
    cur = pos
    while cur > 0:
        if co_memo is not None and (cur - 1) in co_memo:
            break
        prev = tokens[cur - 1]
        if (
            prev.kind == "WORD"
            and prev.val.upper() in _AS_CLAUSE_OPENING_WORDS
            and not _is_non_keyword_ident_pos(tokens, cur - 1)
            and not (prev.val.upper() == "FROM" and _is_distinct_from_op(tokens, cur - 1))
        ):
            cur -= 1
            continue
        break
    if cur > 0 and co_memo is not None and (cur - 1) in co_memo:
        cur_val = not co_memo[cur - 1]
    elif cur == 0:
        cur_val = True
    else:
        prev = tokens[cur - 1]
        cur_val = True
        is_star_wildcard = (
            prev.kind == "OTHER"
            and prev.val == "*"
            and (
                cur == 1
                or _is_qualified_star_tok(tokens, cur - 1)
                or tokens[cur - 2].kind == "COMMA"
                or (
                    tokens[cur - 2].kind == "WORD"
                    and tokens[cur - 2].val.upper() in ("SELECT", "DISTINCT", "ALL")
                    and not _is_non_keyword_ident_pos(tokens, cur - 2)
                )
            )
        )
        if (
            prev.kind in ("OTHER", "RPAREN")
            or (prev.kind == "WORD" and len(prev.val) == 4 and prev.val.upper() == "TIES")
        ) and _is_select_head_modifier_end_tok(tokens, cur - 1):
            cur_val = False
        elif prev.kind in ("COMMA", "LPAREN", "LBRACK", "LBRACE", "DOT") or (
            prev.kind == "OTHER"
            and not is_star_wildcard
            and not _is_num_tok_val(prev.val)
            and not _is_postfix_factorial_tok(tokens, cur - 1)
        ):
            cur_val = False
        elif prev.kind == "WORD" and not _is_non_keyword_ident_pos(tokens, cur - 1):
            pw = prev.val.upper()
            if pw in ("IN", "GLOBAL"):
                cur_val = False
            elif pw in ("DISTINCT", "ALL"):
                is_setop_mod = (
                    cur >= 2
                    and tokens[cur - 2].kind == "WORD"
                    and tokens[cur - 2].val.upper() in ("UNION", "INTERSECT", "EXCEPT")
                    and not _is_non_keyword_ident_pos(tokens, cur - 2)
                )
                if not is_setop_mod:
                    cur_val = False
            elif _is_prev_word_operator_at(tokens, cur - 1, tokens[cur]):
                is_bare_table_alias = (
                    cur >= 3
                    and tokens[cur - 2].kind in ("WORD", "QIDENT")
                    and tokens[cur - 3].kind == "WORD"
                    and tokens[cur - 3].val.upper() in ("FROM", "JOIN")
                    and not _is_non_keyword_ident_pos(tokens, cur - 3)
                    and not _is_distinct_from_op(tokens, cur - 3)
                )
                if not is_bare_table_alias:
                    cur_val = False
    while True:
        if co_memo is not None:
            co_memo[cur] = cur_val
        if cur >= pos:
            return cur_val
        cur += 1
        cur_val = not cur_val


def _is_non_keyword_ident_pos(tokens: list[_Tok], idx: int) -> bool:
    if idx <= 0:
        return False
    prev = tokens[idx - 1]
    pk = prev.kind
    if pk == "DOT":
        return True
    if pk == "OTHER" and prev.val.endswith("::") and not prev.val.endswith(":::"):
        return True
    if pk != "WORD" or len(prev.val) != 2 or prev.val.upper() != "AS":
        return False
    _, as_run, memo, _ = _get_tok_meta_entry(tokens)
    cached = memo.get(idx)
    if cached is not None:
        return cached
    r = as_run[idx]
    if r == 0:
        memo[idx] = False
        return False
    p = idx - 1 - r
    if p > 0 and p not in memo and as_run[p] > 0:
        chain: list[int] = []
        cur = p
        while cur > 0 and cur not in memo and as_run[cur] > 0:
            chain.append(cur)
            cur = cur - 1 - as_run[cur]
        for c_idx in reversed(chain):
            c_r = as_run[c_idx]
            c_p = c_idx - 1 - c_r
            if _cannot_end_aliasable_tok(tokens, c_p, c_r, c_idx):
                c_r -= 1
            memo[c_idx] = (c_r % 2) == 1
    if _cannot_end_aliasable_tok(tokens, p, r, idx):
        r -= 1
    res = (r % 2) == 1
    memo[idx] = res
    return res


def _is_qualified_star_tok(tokens: list[_Tok], pos: int) -> bool:
    return (
        pos >= 1
        and pos < len(tokens)
        and tokens[pos].kind == "OTHER"
        and tokens[pos].val == "*"
        and tokens[pos - 1].kind == "DOT"
    )


def _is_star_except_clause(tokens: list[_Tok], idx: int) -> bool:
    return (
        idx > 0
        and idx + 1 < len(tokens)
        and tokens[idx].kind == "WORD"
        and tokens[idx].val.upper() == "EXCEPT"
        and tokens[idx - 1].kind == "OTHER"
        and tokens[idx - 1].val == "*"
        and tokens[idx + 1].kind == "LPAREN"
    )


def _is_dot_qualifier_at(tokens: list[_Tok], idx: int) -> bool:
    return (
        idx + 2 < len(tokens)
        and tokens[idx + 1].kind == "DOT"
        and (
            tokens[idx + 2].kind in ("WORD", "QIDENT", "STRING")
            or (
                tokens[idx + 2].kind == "OTHER"
                and (tokens[idx + 2].val.startswith("*") or tokens[idx + 2].val[0].isdigit())
            )
        )
    )


def _validate_select_syntax(tokens: list[_Tok]) -> str | None:
    """Catch malformed SELECT / WHERE clauses so invalid SQL fails closed."""
    n = len(tokens)
    match_map, _, _ = _get_tok_meta(tokens)
    for i, tok in enumerate(tokens):
        if tok.kind == "LPAREN":
            if match_map.get(i, -1) <= i:
                return "unbalanced parentheses"
            continue
        if tok.kind == "RPAREN":
            if match_map.get(i, -1) < 0:
                return "unmatched closing parenthesis"
            continue
        if tok.kind == "WORD":
            wu = tok.val.upper()
            if (
                wu in ("SELECT", "WHERE")
                and not _is_non_keyword_ident_pos(tokens, i)
                and not _is_dot_qualifier_at(tokens, i)
                and not (i > 0 and tokens[i - 1].kind == "OTHER" and not _is_num_tok_val(tokens[i - 1].val))
            ):
                if wu == "SELECT":
                    nxt = i + 1
                    if nxt < n and tokens[nxt].kind == "WORD" and tokens[nxt].val.upper() in ("DISTINCT", "ALL"):
                        nxt += 1
                    if (
                        nxt >= n
                        or tokens[nxt].kind == "RPAREN"
                        or (
                            tokens[nxt].kind == "WORD"
                            and not _is_dot_qualifier_at(tokens, nxt)
                            and tokens[nxt].val.upper()
                            in ("FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION")
                        )
                    ):
                        return "empty SELECT projection list"
                else:
                    nxt = i + 1
                    if (
                        nxt >= n
                        or tokens[nxt].kind == "RPAREN"
                        or (tokens[nxt].kind == "OTHER" and tokens[nxt].val.startswith("*"))
                    ):
                        return "invalid WHERE clause"
                    if (
                        tokens[nxt].kind == "WORD"
                        and not _is_dot_qualifier_at(tokens, nxt)
                        and tokens[nxt].val.upper()
                        in (
                            "FROM",
                            "WHERE",
                            "GROUP",
                            "HAVING",
                            "ORDER",
                            "LIMIT",
                        )
                    ):
                        return "empty WHERE clause"
    return None


def _has_top_level_setop(tokens: list[_Tok]) -> bool:
    unwrapped = _strip_outer_parens(tokens)
    k = 0
    m = len(unwrapped)
    while k < m:
        tk = unwrapped[k]
        if tk.kind in _DELIM_PAIRS:
            rp = _find_matching_rdelim(unwrapped, k)
            if rp == -1:
                return True
            k = rp + 1
            continue
        if (
            tk.kind == "WORD"
            and tk.val.upper() in ("UNION", "INTERSECT", "EXCEPT")
            and not _is_non_keyword_ident_pos(unwrapped, k)
            and not _is_star_except_clause(unwrapped, k)
        ):
            return True
        k += 1
    return False


def _is_distinct_from_op(tokens: list[_Tok], idx: int) -> bool:
    if idx < 2:
        return False
    p1 = tokens[idx - 1]
    if p1.kind != "WORD" or p1.val.upper() != "DISTINCT":
        return False
    p2 = tokens[idx - 2]
    if p2.kind != "WORD":
        return False
    if p2.val.upper() == "IS":
        return True
    if p2.val.upper() == "NOT" and idx >= 3:
        p3 = tokens[idx - 3]
        return p3.kind == "WORD" and p3.val.upper() == "IS"
    return False


def _parse_recursive_cte_tokens(
    tokens: list[_Tok], *, expr_cte_refs: dict[str, dict[str, Any]] | None = None
) -> dict[str, Any]:
    inner = _strip_outer_parens(tokens)
    k = 0
    m = len(inner)
    last_union_or_except_op = ""
    last_idx = -1
    while k < m:
        tk = inner[k]
        if tk.kind in _DELIM_PAIRS:
            rp = _find_matching_rdelim(inner, k)
            if rp == -1:
                last_union_or_except_op = ""
                last_idx = -1
                break
            k = rp + 1
            continue
        if (
            tk.kind == "WORD"
            and tk.val.upper() in ("UNION", "INTERSECT", "EXCEPT")
            and not _is_non_keyword_ident_pos(inner, k)
            and not _is_star_except_clause(inner, k)
        ):
            last_union_or_except_op = tk.val.upper()
            last_idx = k
        k += 1

    if last_union_or_except_op != "UNION" or last_idx == -1:
        body = _parse_query_tokens(inner, expr_cte_refs=expr_cte_refs)
        body["type"] = "RECURSIVE_CTE_NODE"
        return body

    after_union = last_idx + 1
    if after_union < m and inner[after_union].kind == "WORD" and inner[after_union].val.upper() in ("ALL", "DISTINCT"):
        after_union += 1

    right_tokens = inner[after_union:]
    if (
        not right_tokens
        or (right_tokens[0].kind == "WORD" and right_tokens[0].val.upper() == "BY")
        or _has_top_level_setop(right_tokens)
        or not _is_subquery_token_list(inner[:last_idx])
        or not _is_subquery_token_list(right_tokens)
    ):
        body = _parse_query_tokens(inner, expr_cte_refs=expr_cte_refs)
        body["type"] = "RECURSIVE_CTE_NODE"
        return body

    return {
        "type": "RECURSIVE_CTE_NODE",
        "setop_type": "UNION",
        "left": _parse_query_tokens(inner[:last_idx], expr_cte_refs=expr_cte_refs),
        "right": _parse_query_tokens(right_tokens, expr_cte_refs=expr_cte_refs),
    }


def _find_with_expr_cte_as_alias(tokens: list[_Tok], start_idx: int) -> tuple[int, int] | None:
    """Locate ``<expr> AS <ident>`` for a ClickHouse expression CTE item starting at ``start_idx``."""
    n = len(tokens)
    k = start_idx
    while k < n:
        tk = tokens[k]
        if tk.kind in _DELIM_PAIRS:
            rp = _find_matching_rdelim(tokens, k)
            if rp == -1:
                return None
            k = rp + 1
            continue
        if tk.kind in ("COMMA", "SEMI"):
            return None
        if tk.kind == "WORD":
            wu = tk.val.upper()
            if (
                wu == "AS"
                and k > start_idx
                and not _is_dot_or_typecast_tok(tokens, k - 1)
                and k + 1 < n
                and tokens[k + 1].kind in ("WORD", "QIDENT")
            ):
                after_idx = k + 2
                if after_idx < n:
                    after_tok = tokens[after_idx]
                    if after_tok.kind == "COMMA":
                        return k, k + 1
                    if after_tok.kind == "WORD" and after_tok.val.upper() in _SUBQUERY_HEAD_KEYWORDS:
                        return k, k + 1
                    if after_tok.kind == "LPAREN":
                        rp_sub = _find_matching_rparen(tokens, after_idx)
                        if rp_sub != -1 and _is_subquery_token_list(tokens[after_idx : rp_sub + 1]):
                            return k, k + 1
            elif (
                wu == "SELECT"
                and not _is_non_keyword_ident_pos(tokens, k)
                and not _is_dot_qualifier_at(tokens, k)
                and (k == start_idx or not _is_on_operator_tok_at(tokens, k - 1))
            ):
                return None
        k += 1
    return None


def _parse_with_prefix_tokens(
    tokens: list[_Tok],
    node: dict[str, Any],
    active_expr_ctes: dict[str, dict[str, Any]],
) -> int:
    """Parse a leading ``WITH [RECURSIVE] ...`` clause, returning the index of the body start."""
    n = len(tokens)
    i = 1
    is_recursive = False
    if i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() == "RECURSIVE":
        is_recursive = True
        i += 1
    cte_entries: list[dict[str, Any]] = []
    expecting_cte_item = True
    while i < n:
        entry_start = i
        matched_std_cte = False
        if tokens[i].kind in ("WORD", "QIDENT"):
            cte_name = tokens[i].val
            j = i + 1
            cols_ok = True
            if j < n and tokens[j].kind == "LPAREN":
                rp_cols = _find_matching_rparen(tokens, j)
                if rp_cols == -1 or not _is_valid_ident_list(tokens[j + 1 : rp_cols]):
                    cols_ok = False
                else:
                    j = rp_cols + 1
            if (
                cols_ok
                and j < n
                and tokens[j].kind == "WORD"
                and tokens[j].val.upper() == "AS"
                and not _is_non_keyword_ident_pos(tokens, j)
            ):
                j += 1
                while j < n and tokens[j].kind == "WORD" and tokens[j].val.upper() in ("MATERIALIZED", "NOT"):
                    j += 1
                if j < n and tokens[j].kind == "LPAREN":
                    rp = _find_matching_rparen(tokens, j)
                    if rp != -1 and _is_subquery_token_list(tokens[j + 1 : rp]):
                        if is_recursive:
                            cte_body = _parse_recursive_cte_tokens(tokens[j + 1 : rp], expr_cte_refs=active_expr_ctes)
                        else:
                            cte_body = _parse_query_tokens(tokens[j + 1 : rp], expr_cte_refs=active_expr_ctes)
                        cte_entries.append({"key": cte_name, "value": {"query": {"node": cte_body}}})
                        active_expr_ctes.pop(cte_name.strip().lower(), None)
                        i = rp + 1
                        matched_std_cte = True
        if matched_std_cte:
            if i < n and tokens[i].kind == "COMMA":
                i += 1
                expecting_cte_item = True
                continue
            expecting_cte_item = False
            break
        expr_cte_hit = _find_with_expr_cte_as_alias(tokens, entry_start)
        if expr_cte_hit is not None:
            as_idx, alias_idx = expr_cte_hit
            expr_toks = tokens[entry_start:as_idx]
            alias_low = tokens[alias_idx].val.strip().lower()
            _scan_expr_tokens(expr_toks, node, allow_direct_subquery=False, expr_cte_refs=active_expr_ctes)
            tbl_ref = _parse_in_rhs_table_ref(expr_toks)
            if tbl_ref is not None:
                if (
                    tbl_ref.get("type") == "BASE_TABLE"
                    and not tbl_ref.get("schema_name")
                    and not tbl_ref.get("catalog_name")
                ):
                    ref_low = str(tbl_ref.get("table_name") or "").strip().lower()
                    if ref_low in active_expr_ctes:
                        tbl_ref = active_expr_ctes[ref_low]
                active_expr_ctes[alias_low] = tbl_ref
                node["children"].append(
                    {
                        "type": "EXPR_CTE_TABLE_ALIAS",
                        "alias": alias_low,
                        "target_type": tbl_ref.get("type") or "",
                        "target_table": tbl_ref.get("table_name") or "",
                        "target_schema": tbl_ref.get("schema_name") or "",
                        "target_catalog": tbl_ref.get("catalog_name") or "",
                        "target_message": tbl_ref.get("message") or "",
                    }
                )
            else:
                active_expr_ctes.pop(alias_low, None)
            i = alias_idx + 1
            if i < n and tokens[i].kind == "COMMA":
                i += 1
                expecting_cte_item = True
                continue
            expecting_cte_item = False
            break
        node["children"].append({"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid WITH clause item"})
        expecting_cte_item = False
        i = entry_start
        break
    if expecting_cte_item:
        node["children"].append({"type": "PARSE_ERROR_NODE", "message": "SQL parse error: incomplete WITH clause"})
    node["cte_map"] = {"map": cte_entries}
    return i


def _parse_branch_with_tokens(
    tokens: list[_Tok], *, expr_cte_refs: dict[str, dict[str, Any]] | None = None
) -> tuple[dict[str, Any], int]:
    """Parse an unparenthesized set-op branch starting with ``WITH`` up to the next top-level set-op."""
    branch_node: dict[str, Any] = {"type": "SELECT_NODE", "children": []}
    active_expr_ctes: dict[str, dict[str, Any]] = dict(expr_cte_refs) if expr_cte_refs else {}
    after_with = _parse_with_prefix_tokens(tokens, branch_node, active_expr_ctes)
    body_consumed = _scan_body_tokens(
        tokens[after_with:], branch_node, expr_cte_refs=active_expr_ctes or None, stop_at_setop=True
    )
    return branch_node, after_with + body_consumed


def _parse_query_tokens(
    tokens: list[_Tok], *, expr_cte_refs: dict[str, dict[str, Any]] | None = None
) -> dict[str, Any]:
    tokens = _strip_outer_parens(tokens)
    node: dict[str, Any] = {"type": "SELECT_NODE", "children": []}
    if not tokens:
        return node

    if tokens[0].kind == "WORD":
        w0 = tokens[0].val.upper()
        if w0 in _SHOW_REF_STATEMENTS:
            node["children"].append(
                {
                    "type": "DISALLOWED_SUBQUERY_STATEMENT",
                    "reason": "show_ref",
                    "message": "SHOW / DESCRIBE / SUMMARIZE statements are not allowed via /api/query",
                }
            )
            return node
        if w0 in _NON_SELECT_STATEMENTS:
            node["children"].append(
                {
                    "type": "DISALLOWED_SUBQUERY_STATEMENT",
                    "reason": f"statement_type:{w0}_NODE",
                    "message": f"only SELECT / WITH / UNION statements allowed (got {w0}_NODE)",
                }
            )
            return node

    active_expr_ctes: dict[str, dict[str, Any]] = dict(expr_cte_refs) if expr_cte_refs else {}
    i = 0
    if tokens[0].kind == "WORD" and tokens[0].val.upper() == "WITH":
        i = _parse_with_prefix_tokens(tokens, node, active_expr_ctes)

    _scan_body_tokens(tokens if i == 0 else tokens[i:], node, expr_cte_refs=active_expr_ctes or None)
    return node


def _is_valid_ident_list(cols: list[_Tok]) -> bool:
    if not cols or len(cols) % 2 == 0:
        return False
    return all((tc.kind in ("WORD", "QIDENT") if idx % 2 == 0 else tc.kind == "COMMA") for idx, tc in enumerate(cols))


def _is_within_group_at(tokens: list[_Tok], idx: int) -> bool:
    return (
        idx > 0
        and idx + 1 < len(tokens)
        and tokens[idx].kind == "WORD"
        and tokens[idx].val.upper() == "GROUP"
        and not _is_non_keyword_ident_pos(tokens, idx)
        and tokens[idx - 1].kind == "WORD"
        and tokens[idx - 1].val.upper() == "WITHIN"
        and not _is_non_keyword_ident_pos(tokens, idx - 1)
        and tokens[idx + 1].kind == "LPAREN"
    )


_CASE_SCAN_GUARD = threading.local()


def _build_case_when_end_map(tokens: list[_Tok]) -> dict[int, bool]:
    match_map, _, _ = _get_tok_meta(tokens)
    case_map: dict[int, bool] = {}
    frames: list[list[list[Any]]] = [[]]
    for k, tk in enumerate(tokens):
        if tk.kind in _DELIM_PAIRS:
            if match_map.get(k, -1) <= k:
                frames[-1].clear()
            frames.append([])
            continue
        if tk.kind in _CLOSING_DELIMS:
            if len(frames) > 1:
                frames.pop()
            else:
                frames[0].clear()
            continue
        if tk.kind == "SEMI":
            frames[-1].clear()
            continue
        if tk.kind == "WORD" and len(tk.val) in (3, 4):
            wu = tk.val.upper()
            if (
                wu in ("CASE", "WHEN", "END")
                and not _is_non_keyword_ident_pos(tokens, k)
                and not _is_dot_qualifier_at(tokens, k)
            ):
                if wu == "CASE":
                    frames[-1].append([k, False])
                elif wu == "WHEN":
                    if frames[-1]:
                        frames[-1][-1][1] = True
                elif wu == "END":
                    if frames[-1]:
                        c_idx, saw_when = frames[-1].pop()
                        if saw_when:
                            case_map[c_idx] = True
    return case_map


def _has_matching_case_when_end_ahead(tokens: list[_Tok], case_idx: int) -> bool:
    if getattr(_TOK_META_TLS, "has_end", True) is False:
        return False
    _, _, _, case_state = _get_tok_meta_entry(tokens)
    if case_state is not None:
        if case_state["ready"]:
            return bool(case_state["map"].get(case_idx, False))
        if case_state["building"]:
            return False
        case_state["building"] = True
        try:
            case_map = _build_case_when_end_map(tokens)
            case_state["map"] = case_map
            case_state["ready"] = True
        finally:
            case_state["building"] = False
        return bool(case_state["map"].get(case_idx, False))

    active: set[int] | None = getattr(_CASE_SCAN_GUARD, "active", None)
    if active is None:
        active = set()
        _CASE_SCAN_GUARD.active = active
    tok_id = id(tokens)
    if tok_id in active:
        return False
    active.add(tok_id)
    try:
        return bool(_build_case_when_end_map(tokens).get(case_idx, False))
    finally:
        active.discard(tok_id)


_ON_OP_CANDIDATE_WORDS = _ON_EXPR_PREV_WORDS | frozenset(
    {"SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "DISTINCT", "ALL"}
)


def _prev_word_op_step(tokens: list[_Tok], prev_idx: int, nxt_tok: _Tok | None) -> tuple[bool | None, int]:
    """Return ``(result, -1)`` if resolved directly, or ``(None, dep_pos)`` meaning ``not on_op(dep_pos)``."""
    pw = tokens[prev_idx].val.upper()
    if pw not in _ON_EXPR_PREV_WORDS or _is_non_keyword_ident_pos(tokens, prev_idx):
        return False, -1
    if pw == "FROM":
        return _is_distinct_from_op(tokens, prev_idx), -1
    if pw == "TO":
        return (
            prev_idx >= 1
            and tokens[prev_idx - 1].kind == "WORD"
            and tokens[prev_idx - 1].val.upper() == "SIMILAR"
            and not _is_non_keyword_ident_pos(tokens, prev_idx - 1)
        ), -1
    if pw == "ZONE":
        return (
            prev_idx >= 2
            and tokens[prev_idx - 1].kind == "WORD"
            and tokens[prev_idx - 1].val.upper() == "TIME"
            and not _is_non_keyword_ident_pos(tokens, prev_idx - 1)
            and tokens[prev_idx - 2].kind == "WORD"
            and tokens[prev_idx - 2].val.upper() == "AT"
            and not _is_non_keyword_ident_pos(tokens, prev_idx - 2)
        ), -1
    if pw == "INTERVAL":
        return (
            nxt_tok is not None
            and nxt_tok.kind == "WORD"
            and nxt_tok.val.upper() in _SQL_INTERVAL_UNITS
            and not _is_non_keyword_ident_pos(tokens, prev_idx + 2)
        ), -1
    if pw == "ESCAPE":
        return (False, -1) if prev_idx < 1 else (None, prev_idx - 1)
    if pw in ("SYMMETRIC", "ASYMMETRIC"):
        return (
            prev_idx >= 1
            and tokens[prev_idx - 1].kind == "WORD"
            and tokens[prev_idx - 1].val.upper() == "BETWEEN"
            and not _is_non_keyword_ident_pos(tokens, prev_idx - 1)
        ), -1
    if pw == "NOT":
        return True, -1
    if pw == "CASE":
        if not _has_matching_case_when_end_ahead(tokens, prev_idx):
            return False, -1
        first_after = tokens[prev_idx + 1] if prev_idx + 1 < len(tokens) else None
        if first_after is None:
            return False, -1
        if (
            first_after.kind == "WORD"
            and first_after.val.upper() == "WHEN"
            and not _is_non_keyword_ident_pos(tokens, prev_idx + 1)
        ):
            return True, -1
        if first_after.kind != "WORD":
            return True, -1
        if nxt_tok is None:
            return False, -1
        if nxt_tok.kind in ("DOT", "LPAREN", "LBRACK") or (
            nxt_tok.kind == "OTHER" and not _is_num_tok_val(nxt_tok.val)
        ):
            return True, -1
        return (
            nxt_tok.kind == "WORD"
            and nxt_tok.val.upper() in _ON_EXPR_NEXT_WORDS
            and (prev_idx + 2 >= len(tokens) or not _is_non_keyword_ident_pos(tokens, prev_idx + 2))
        ), -1
    if pw == "ON":
        return (True, -1) if prev_idx == 0 else (None, prev_idx - 1)
    if pw == "WHEN":
        if prev_idx >= 1 and tokens[prev_idx - 1].kind == "WORD" and tokens[prev_idx - 1].val.upper() == "CASE":
            return (not _is_non_keyword_ident_pos(tokens, prev_idx - 1)), -1
        return (False, -1) if prev_idx < 1 else (None, prev_idx - 1)
    if (
        pw in ("IN", "LIKE", "ILIKE", "RLIKE", "REGEXP", "GLOB", "SIMILAR", "BETWEEN")
        and prev_idx >= 2
        and tokens[prev_idx - 1].kind == "WORD"
        and tokens[prev_idx - 1].val.upper() == "NOT"
        and not _is_non_keyword_ident_pos(tokens, prev_idx - 1)
    ):
        return None, prev_idx - 2
    return (False, -1) if prev_idx < 1 else (None, prev_idx - 1)


def _eval_on_op_and_fact(tokens: list[_Tok], start_kind: str, start_pos: int) -> bool:
    """Iterative, memoized evaluator for ``_is_on_operator_tok_at`` and ``_is_postfix_factorial_tok``."""
    _, as_run, non_kw_memo, case_state = _get_tok_meta_entry(tokens)
    if case_state is not None:
        on_op_memo: dict[int, bool] = case_state["on_op"]
        fact_memo: dict[int, bool] = case_state["fact"]
    else:
        on_op_memo = {}
        fact_memo = {}

    if start_kind == "on_op":
        if start_pos < 0:
            return True
        cached = on_op_memo.get(start_pos)
        if cached is not None:
            return cached
    else:
        cached = fact_memo.get(start_pos)
        if cached is not None:
            return cached

    n_tok = len(tokens)
    stack: list[tuple[str, int]] = [(start_kind, start_pos)]
    while stack:
        kind, pos = stack.pop()
        if kind == "fact":
            if pos in fact_memo:
                continue
            if pos <= 0 or pos >= n_tok or not _is_bang_tok(tokens, pos):
                fact_memo[pos] = False
                continue
            k = pos
            while k > 0 and _is_bang_tok(tokens, k) and k not in fact_memo:
                k -= 1
            if k in fact_memo:
                run_val = fact_memo[k]
                fill_start = k + 1
            elif _is_bang_tok(tokens, k):
                run_val = False
                fact_memo[0] = False
                on_op_memo[0] = True
                fill_start = 1
            elif _is_select_head_signed_top_end_tok(tokens, k):
                run_val = True
                fill_start = k + 1
            elif k in on_op_memo:
                run_val = not on_op_memo[k]
                fill_start = k + 1
            else:
                stack.append(("fact", pos))
                stack.append(("on_op", k))
                continue
            for j in range(fill_start, pos + 1):
                fact_memo[j] = run_val
                on_op_memo[j] = not run_val
            continue

        if pos < 0 or pos in on_op_memo:
            continue
        tk = tokens[pos]
        if (
            tk.kind in ("OTHER", "RPAREN") or (tk.kind == "WORD" and len(tk.val) == 4 and tk.val.upper() == "TIES")
        ) and _is_select_head_modifier_end_tok(tokens, pos):
            on_op_memo[pos] = True
            continue
        if tk.kind in ("COMMA", "SEMI", "LPAREN", "LBRACK", "LBRACE", "DOT"):
            on_op_memo[pos] = True
            continue
        if tk.kind == "OTHER" and not _is_num_tok_val(tk.val):
            if _is_bang_tok(tokens, pos):
                if pos not in fact_memo:
                    stack.append(("on_op", pos))
                    stack.append(("fact", pos))
                    continue
                on_op_memo[pos] = not fact_memo[pos]
            else:
                on_op_memo[pos] = not _is_qualified_star_tok(tokens, pos)
            continue
        if tk.kind != "WORD":
            on_op_memo[pos] = False
            continue
        wu = tk.val.upper()
        if wu not in _ON_OP_CANDIDATE_WORDS:
            on_op_memo[pos] = False
            continue
        if pos not in non_kw_memo and as_run[pos] > 0:
            cur_p = pos - 1 - as_run[pos]
            needed: list[int] = []
            while cur_p >= 0:
                if cur_p not in on_op_memo:
                    needed.append(cur_p)
                if cur_p > 0 and (cur_p - 1) not in on_op_memo and tokens[cur_p].kind == "WORD":
                    needed.append(cur_p - 1)
                if cur_p in non_kw_memo or as_run[cur_p] == 0:
                    break
                cur_p = cur_p - 1 - as_run[cur_p]
            if needed:
                stack.append(("on_op", pos))
                for np in needed:
                    stack.append(("on_op", np))
                continue
        if _is_non_keyword_ident_pos(tokens, pos):
            on_op_memo[pos] = False
            continue
        if wu in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY"):
            if pos == 0:
                on_op_memo[pos] = True
                continue
            prev = tokens[pos - 1]
            if prev.kind in ("COMMA", "LPAREN", "LBRACK", "LBRACE", "DOT"):
                on_op_memo[pos] = False
                continue
            if prev.kind == "OTHER" and not _is_num_tok_val(prev.val):
                if _is_bang_tok(tokens, pos - 1):
                    if (pos - 1) not in fact_memo:
                        stack.append(("on_op", pos))
                        stack.append(("fact", pos - 1))
                        continue
                    on_op_memo[pos] = fact_memo[pos - 1]
                else:
                    on_op_memo[pos] = False
                continue
            if prev.kind == "WORD":
                if (pos - 1) not in non_kw_memo and as_run[pos - 1] > 0:
                    p_prev = (pos - 1) - 1 - as_run[pos - 1]
                    if p_prev >= 0 and p_prev not in on_op_memo:
                        stack.append(("on_op", pos))
                        stack.append(("on_op", p_prev))
                        continue
                res_w, dep_w = _prev_word_op_step(tokens, pos - 1, tk)
                if res_w is None:
                    if dep_w not in on_op_memo:
                        stack.append(("on_op", pos))
                        stack.append(("on_op", dep_w))
                        continue
                    res_w = not on_op_memo[dep_w]
                on_op_memo[pos] = not res_w
                continue
            on_op_memo[pos] = True
            continue
        if wu in ("DISTINCT", "ALL"):
            on_op_memo[pos] = (
                pos >= 1
                and tokens[pos - 1].kind == "WORD"
                and tokens[pos - 1].val.upper() in ("SELECT", "UNION", "INTERSECT", "EXCEPT")
                and not _is_non_keyword_ident_pos(tokens, pos - 1)
            ) or (wu == "ALL" and pos + 1 < n_tok and tokens[pos + 1].kind == "LPAREN")
            continue
        after_operand = tokens[pos + 2] if pos + 2 < n_tok else None
        if pos >= 1 and (pos - 1) not in non_kw_memo and as_run[pos - 1] > 0:
            p_prev2 = (pos - 1) - 1 - as_run[pos - 1]
            if p_prev2 >= 0 and p_prev2 not in on_op_memo:
                stack.append(("on_op", pos))
                stack.append(("on_op", p_prev2))
                continue
        res_w, dep_w = _prev_word_op_step(tokens, pos, after_operand)
        if res_w is None:
            if dep_w not in on_op_memo:
                stack.append(("on_op", pos))
                stack.append(("on_op", dep_w))
                continue
            res_w = not on_op_memo[dep_w]
        on_op_memo[pos] = res_w

    return on_op_memo[start_pos] if start_kind == "on_op" else fact_memo[start_pos]


def _is_on_operator_tok_at(tokens: list[_Tok], pos: int) -> bool:
    if pos < 0:
        return True
    return _eval_on_op_and_fact(tokens, "on_op", pos)


def _is_prev_word_operator_at(tokens: list[_Tok], prev_idx: int, nxt_tok: _Tok | None) -> bool:
    res, dep = _prev_word_op_step(tokens, prev_idx, nxt_tok)
    if res is not None:
        return res
    return not _eval_on_op_and_fact(tokens, "on_op", dep)


def _is_multi_token_post_on_exit(tokens: list[_Tok], idx: int) -> bool:
    wu = tokens[idx].val.upper()
    if wu not in ("GROUP", "ORDER", "FETCH", "INTO", "UNION", "INTERSECT", "EXCEPT"):
        return False
    if wu == "GROUP" and _is_within_group_at(tokens, idx):
        return False
    nxt_tok = tokens[idx + 1] if idx + 1 < len(tokens) else None
    if nxt_tok is None:
        return False
    nxt_is_kw = nxt_tok.kind == "WORD" and not _is_non_keyword_ident_pos(tokens, idx + 1)
    if wu in ("GROUP", "ORDER"):
        return nxt_is_kw and nxt_tok.val.upper() in ("BY", "ALL")
    if wu == "FETCH":
        return nxt_is_kw and nxt_tok.val.upper() in ("FIRST", "NEXT")
    if wu == "INTO":
        return nxt_is_kw and nxt_tok.val.upper() == "OUTFILE"
    if nxt_tok.kind == "LPAREN":
        return True
    return (
        nxt_is_kw
        and not _is_dot_qualifier_at(tokens, idx + 1)
        and nxt_tok.val.upper() in ("ALL", "DISTINCT", "BY", "SELECT", "WITH", "FROM", "VALUES", "TABLE")
    )


def _is_on_expr_ident_at(tokens: list[_Tok], idx: int) -> bool:
    if _is_dot_qualifier_at(tokens, idx):
        return True
    wu = tokens[idx].val.upper()
    if wu == "GROUP" and _is_within_group_at(tokens, idx):
        return True
    if _is_multi_token_post_on_exit(tokens, idx):
        return False
    prev_tok = tokens[idx - 1] if idx > 0 else None
    nxt_tok = tokens[idx + 1] if idx + 1 < len(tokens) else None
    if prev_tok is not None:
        if prev_tok.kind == "DOT" or (
            prev_tok.kind == "OTHER"
            and not _is_num_tok_val(prev_tok.val)
            and not _is_postfix_factorial_tok(tokens, idx - 1)
            and not _is_qualified_star_tok(tokens, idx - 1)
        ):
            return True
        if prev_tok.kind == "WORD" and _is_prev_word_operator_at(tokens, idx - 1, nxt_tok):
            return True
    if nxt_tok is not None and nxt_tok.kind == "OTHER" and not _is_num_tok_val(nxt_tok.val):
        if wu == "SELECT" and nxt_tok.val in ("*", "+", "-", "~", "!"):
            return False
        if wu in ("WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET") and nxt_tok.val in (
            "+",
            "-",
            "~",
            "!",
        ):
            return False
        return True
    if (
        nxt_tok is not None
        and nxt_tok.kind == "LBRACK"
        and wu not in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET", "JOIN", "FROM", "TABLE")
    ):
        return True
    if (
        nxt_tok is not None
        and nxt_tok.kind == "WORD"
        and nxt_tok.val.upper() in _ON_EXPR_NEXT_WORDS
        and not _is_non_keyword_ident_pos(tokens, idx + 1)
        and wu not in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET", "JOIN", "FROM", "TABLE")
    ):
        return True
    return False


def _can_exit_from_on_stop_kw(tokens: list[_Tok], idx: int, *, in_on_clause: bool) -> bool:
    if _is_within_group_at(tokens, idx):
        return False
    if not in_on_clause:
        return True
    wu = tokens[idx].val.upper()
    nxt_tok = tokens[idx + 1] if idx + 1 < len(tokens) else None
    if wu in ("GROUP", "ORDER"):
        return nxt_tok is not None and nxt_tok.kind == "WORD" and nxt_tok.val.upper() in ("BY", "ALL")
    if wu == "FETCH":
        return nxt_tok is not None and nxt_tok.kind == "WORD" and nxt_tok.val.upper() in ("FIRST", "NEXT")
    return True


def _is_subquery_token_list(tokens: list[_Tok], *, enclosing_special_call: str | None = None) -> bool:
    unwrapped = _strip_outer_parens(tokens)
    if not unwrapped:
        return False
    if unwrapped[0].kind == "LPAREN":
        rp0 = _find_matching_rparen(unwrapped, 0)
        if (
            0 < rp0 < len(unwrapped) - 1
            and unwrapped[rp0 + 1].kind == "WORD"
            and unwrapped[rp0 + 1].val.upper() in ("UNION", "INTERSECT", "EXCEPT", "ORDER", "LIMIT", "OFFSET", "FETCH")
        ):
            return _is_subquery_token_list(unwrapped[1:rp0])
        return False
    if unwrapped[0].kind != "WORD":
        return False
    w0 = unwrapped[0].val.upper()
    if enclosing_special_call in {"trim", "btrim", "ltrim", "rtrim"} and len(unwrapped) == len(tokens) and w0 == "FROM":
        return False
    if w0 in _SUBQUERY_HEAD_KEYWORDS:
        return True
    return w0 in _NON_SELECT_STATEMENTS and not (len(unwrapped) > 1 and unwrapped[1].kind == "LPAREN")


_POSITION_FRAME_DISALLOWED_KEYWORDS = frozenset(
    {"AND", "OR", "XOR", "NOT", "IS", "LIKE", "ILIKE", "RLIKE", "REGEXP", "GLOB", "SIMILAR", "BETWEEN"}
)
_IN_LHS_ABSENT_EXTRA_WORDS = frozenset(
    {
        "BY",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "FIRST",
        "NEXT",
        "STEP",
        "STALENESS",
        "TO",
        "RETURN",
        "SET",
        "VALUES",
        "PARTITION",
        "USING",
        "OVER",
        "FILTER",
        "AS",
        "FROM",
        "JOIN",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "WITH",
        "WINDOW",
        "CASE",
    }
)


def _is_prefix_in_call(tokens: list[_Tok], in_idx: int) -> bool:
    """Return True when ``IN(...)`` at ``in_idx`` has no LHS operand (ClickHouse ``in(x, set)``)."""
    if in_idx + 1 >= len(tokens) or tokens[in_idx + 1].kind != "LPAREN":
        return False
    p = in_idx - 1
    if (
        p >= 0
        and tokens[p].kind == "WORD"
        and tokens[p].val.upper() == "NOT"
        and not _is_non_keyword_ident_pos(tokens, p)
    ):
        p -= 1
    if (
        p >= 0
        and tokens[p].kind == "WORD"
        and tokens[p].val.upper() == "GLOBAL"
        and not _is_non_keyword_ident_pos(tokens, p)
    ):
        p -= 1
    if p < 0:
        return True
    if _is_on_operator_tok_at(tokens, p):
        return True
    pt = tokens[p]
    return (
        pt.kind == "WORD" and pt.val.upper() in _IN_LHS_ABSENT_EXTRA_WORDS and not _is_non_keyword_ident_pos(tokens, p)
    )


def _parse_in_rhs_table_ref(rhs_tokens: list[_Tok]) -> dict[str, Any] | None:
    """Detect bare or schema/catalog-qualified table identifiers inside ``IN (...)``."""
    unwrapped = rhs_tokens
    while unwrapped:
        peeled = _strip_outer_parens(unwrapped)
        if len(peeled) != len(unwrapped):
            unwrapped = peeled
            continue
        if unwrapped[0].kind == "OTHER" and bool(unwrapped[0].val) and unwrapped[0].val.strip("+") == "":
            unwrapped = unwrapped[1:]
            continue
        if (
            len(unwrapped) >= 3
            and unwrapped[-2].kind == "WORD"
            and unwrapped[-2].val.upper() == "AS"
            and unwrapped[-1].kind in ("WORD", "QIDENT")
        ):
            unwrapped = unwrapped[:-2]
            continue
        break
    if not unwrapped or len(unwrapped) % 2 == 0:
        return None
    for idx, tk in enumerate(unwrapped):
        if idx % 2 == 0:
            if tk.kind not in ("WORD", "QIDENT"):
                return None
        elif tk.kind != "DOT":
            return None
    if len(unwrapped) == 1 and unwrapped[0].kind == "WORD" and unwrapped[0].val.upper() in ("TRUE", "FALSE", "NULL"):
        return None
    parts = [unwrapped[idx].val for idx in range(0, len(unwrapped), 2)]
    if len(parts) == 1:
        return {"type": "BASE_TABLE", "table_name": parts[0], "schema_name": "", "catalog_name": ""}
    if len(parts) == 2:
        return {"type": "BASE_TABLE", "table_name": parts[1], "schema_name": parts[0], "catalog_name": ""}
    if len(parts) == 3:
        return {"type": "BASE_TABLE", "table_name": parts[2], "schema_name": parts[1], "catalog_name": parts[0]}
    return {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid table reference in IN clause"}


def _resolve_in_rhs_table_ref(
    rhs_tokens: list[_Tok], expr_cte_refs: dict[str, dict[str, Any]] | None
) -> dict[str, Any] | None:
    tbl_ref = _parse_in_rhs_table_ref(rhs_tokens)
    if (
        tbl_ref is not None
        and expr_cte_refs
        and tbl_ref.get("type") == "BASE_TABLE"
        and not tbl_ref.get("schema_name")
        and not tbl_ref.get("catalog_name")
    ):
        resolved = expr_cte_refs.get(str(tbl_ref.get("table_name") or "").strip().lower())
        if resolved is not None:
            return resolved
    return tbl_ref


def _find_valid_position_in_idx(tokens: list[_Tok]) -> int | None:
    """Return the unique valid ``IN`` index in a ``POSITION(... IN ...)`` frame, or ``None``."""
    match_map, _, _, case_state = _get_tok_meta_entry(tokens)
    if case_state is not None and case_state["pos_in_ready"]:
        return case_state["pos_in_idx"]
    n = len(tokens)
    case_depth = 0
    top_in_count = 0
    valid_in_idx: int | None = None
    k = 0
    while k < n:
        tk = tokens[k]
        if tk.kind in _DELIM_PAIRS:
            rp = match_map.get(k, -1)
            if rp <= k:
                valid_in_idx = None
                top_in_count = 0
                break
            k = rp + 1
            continue
        if tk.kind == "COMMA":
            valid_in_idx = None
            top_in_count = 0
            break
        if case_depth == 0 and tk.kind == "OTHER" and any(ch in tk.val for ch in ("=", "<", ">", "!")):
            valid_in_idx = None
            top_in_count = 0
            break
        if tk.kind == "WORD" and not _is_non_keyword_ident_pos(tokens, k) and not _is_dot_qualifier_at(tokens, k):
            wu = tk.val.upper()
            if wu == "CASE":
                case_depth += 1
            elif wu == "END":
                case_depth = max(0, case_depth - 1)
            elif wu == "IN":
                top_in_count += 1
                if top_in_count > 1:
                    valid_in_idx = None
                    break
                if (
                    case_depth == 0
                    and 0 < k < n - 1
                    and not (tokens[k - 1].kind == "WORD" and tokens[k - 1].val.upper() in ("NOT", "GLOBAL"))
                ):
                    valid_in_idx = k
                else:
                    valid_in_idx = None
                    break
            elif case_depth == 0 and wu in _POSITION_FRAME_DISALLOWED_KEYWORDS:
                valid_in_idx = None
                top_in_count = 0
                break
        k += 1
    res = valid_in_idx if top_in_count == 1 else None
    if case_state is not None:
        case_state["pos_in_ready"] = True
        case_state["pos_in_idx"] = res
    return res


def _is_valid_position_in_frame(tokens: list[_Tok], in_idx: int) -> bool:
    if in_idx <= 0 or in_idx >= len(tokens) - 1:
        return False
    return _find_valid_position_in_idx(tokens) == in_idx


def _is_list_comp_in_at(tokens: list[_Tok], idx: int, *, in_bracket: bool) -> bool:
    return (
        in_bracket
        and idx + 1 < len(tokens)
        and tokens[idx + 1].kind == "LBRACK"
        and idx >= 2
        and tokens[idx - 2].kind == "WORD"
        and tokens[idx - 2].val.upper() == "FOR"
    )


def _scan_expr_tokens(
    tokens: list[_Tok],
    node: dict[str, Any],
    *,
    enclosing_special_call: str | None = None,
    allow_direct_subquery: bool = True,
    in_bracket: bool = False,
    expr_cte_refs: dict[str, dict[str, Any]] | None = None,
) -> None:
    work: list[tuple[list[_Tok], str | None, bool, bool]] = [
        (tokens, enclosing_special_call, allow_direct_subquery, in_bracket)
    ]
    while work:
        cur_tokens, cur_special, cur_allow_subq, cur_in_brack = work.pop()
        if cur_allow_subq and _is_subquery_token_list(cur_tokens, enclosing_special_call=cur_special):
            node["children"].append(_parse_query_tokens(_strip_outer_parens(cur_tokens), expr_cte_refs=expr_cte_refs))
            continue

        cur_match_map, _, _ = _get_tok_meta(cur_tokens)
        pos_in_cached: int | None = None
        pos_in_computed = False
        i = 0
        n = len(cur_tokens)
        while i < n:
            tok = cur_tokens[i]
            if (
                tok.kind == "WORD"
                and tok.val.upper() in ("PIVOT", "UNPIVOT", "PIVOT_WIDER", "PIVOT_LONGER")
                and not _is_non_keyword_ident_pos(cur_tokens, i)
            ):
                wu = tok.val.upper()
                node["children"].append(
                    {
                        "type": "DISALLOWED_SUBQUERY_STATEMENT",
                        "reason": f"statement_type:{wu}_NODE",
                        "message": f"only SELECT / WITH / UNION statements allowed (got {wu}_NODE)",
                    }
                )
                i += 1
                continue
            if (
                tok.kind == "WORD"
                and tok.val.upper() == "IN"
                and not _is_non_keyword_ident_pos(cur_tokens, i)
                and not _is_dot_qualifier_at(cur_tokens, i)
            ):
                if cur_special == "position":
                    if not pos_in_computed:
                        pos_in_cached = _find_valid_position_in_idx(cur_tokens)
                        pos_in_computed = True
                    is_valid_pos_in = pos_in_cached == i
                else:
                    is_valid_pos_in = False
                nxt_is_lparen = i + 1 < n and cur_tokens[i + 1].kind == "LPAREN"
                if nxt_is_lparen:
                    if not is_valid_pos_in:
                        rp = _find_matching_rparen(cur_tokens, i + 1)
                        if _is_prefix_in_call(cur_tokens, i):
                            node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                            if rp != -1:
                                work.append((cur_tokens[i + 2 : rp], None, True, False))
                                i = rp + 1
                                continue
                        elif rp != -1:
                            rhs = cur_tokens[i + 2 : rp]
                            if not _is_subquery_token_list(rhs):
                                tbl_ref = _resolve_in_rhs_table_ref(rhs, expr_cte_refs)
                                if tbl_ref is not None:
                                    node["children"].append(tbl_ref)
                elif not (is_valid_pos_in or _is_list_comp_in_at(cur_tokens, i, in_bracket=cur_in_brack)):
                    node["children"].append(
                        {
                            "type": "PARSE_ERROR_NODE",
                            "message": "SQL parse error: IN must be followed by a parenthesized list or subquery",
                        }
                    )
                i += 1
                continue
            if tok.kind in ("WORD", "QIDENT") and i + 1 < n and cur_tokens[i + 1].kind == "LPAREN":
                rp = _find_matching_rparen(cur_tokens, i + 1)
                if rp != -1:
                    if tok.kind == "WORD" and tok.val.lower() in _NON_FUNCTION_KEYWORDS:
                        if tok.val.lower() in _SPECIAL_CALL_KEYWORDS:
                            work.append((cur_tokens[i + 2 : rp], tok.val.lower(), True, False))
                            i = rp + 1
                            continue
                        i += 1
                        continue
                    node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                    work.append((cur_tokens[i + 2 : rp], None, True, False))
                    i = rp + 1
                    continue
            if tok.kind == "LPAREN":
                rp = cur_match_map.get(i, -1)
                if rp > i:
                    lo_p, hi_p = i + 1, rp - 1
                    while lo_p < hi_p and cur_tokens[lo_p].kind == "LPAREN" and cur_match_map.get(lo_p) == hi_p:
                        lo_p += 1
                        hi_p -= 1
                    work.append((cur_tokens[lo_p : hi_p + 1], None, True, False))
                    i = rp + 1
                    continue
            if tok.kind in ("LBRACK", "LBRACE"):
                rb = _find_matching_rdelim(cur_tokens, i)
                if rb != -1:
                    work.append((cur_tokens[i + 1 : rb], None, False, tok.kind == "LBRACK"))
                    i = rb + 1
                    continue
            i += 1


_DISALLOWED_UNQUOTED_AS_TABLE_ALIASES = (
    (_CLAUSE_STOP_KEYWORDS - {"SAMPLE", "UNION"})
    | _JOIN_MODIFIERS
    | frozenset(
        {
            "FROM",
            "ON",
            "USING",
            "VALUES",
            "SET",
            "WITH",
            "TABLE",
            "AS",
            "FINAL",
            "LATERAL",
            "ONLY",
            "PIVOT",
            "UNPIVOT",
            "PIVOT_WIDER",
            "PIVOT_LONGER",
            "TABLESAMPLE",
        }
    )
)
_DISALLOWED_BARE_TABLE_ALIASES = _DISALLOWED_UNQUOTED_AS_TABLE_ALIASES | frozenset({"SELECT", "UNION", "SAMPLE"})
_OUTSIDE_FROM_KEYWORDS = (
    frozenset(
        {
            "FROM",
            "TABLE",
            "JOIN",
            "IN",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "PIVOT",
            "UNPIVOT",
            "PIVOT_WIDER",
            "PIVOT_LONGER",
        }
    )
    | _JOIN_MODIFIERS
)
_SCAN_BODY_WORD_KEYWORDS = (
    _OUTSIDE_FROM_KEYWORDS
    | _CLAUSE_STOP_KEYWORDS
    | _SHOW_REF_STATEMENTS
    | _NON_SELECT_STATEMENTS
    | frozenset({"SELECT", "ARRAY", "FINAL", "TABLESAMPLE", "ON", "USING", "SET", "ONLY", "LATERAL", "WITH", "VALUES"})
)


def _scan_body_tokens(
    tokens: list[_Tok],
    node: dict[str, Any],
    *,
    in_from: bool = False,
    expect_table: bool = False,
    expr_cte_refs: dict[str, dict[str, Any]] | None = None,
    stop_at_setop: bool = False,
) -> int:
    i = 0
    n = len(tokens)
    at_branch_start = not in_from
    in_sample_clause = False
    in_on_clause = False
    in_array_join = False
    allow_table_alias = False
    saw_final = False
    in_join_mod = False
    saw_join = False
    outside_from_kws = _OUTSIDE_FROM_KEYWORDS
    join_mod_only = _JOIN_MODIFIERS - {"JOIN"}
    jm_cache_start = -1
    jm_cache_end = -1
    jm_cache_ok = False

    def _starts_join_modifier_seq(idx: int) -> bool:
        nonlocal jm_cache_start, jm_cache_end, jm_cache_ok
        if jm_cache_start <= idx < jm_cache_end:
            return jm_cache_ok
        k = idx
        while k < n and tokens[k].kind == "WORD" and tokens[k].val.upper() in join_mod_only:
            k += 1
        ok = False
        if k < n and tokens[k].kind == "WORD":
            ku = tokens[k].val.upper()
            if ku == "JOIN":
                ok = True
            elif ku == "ARRAY" and k + 1 < n and tokens[k + 1].kind == "WORD" and tokens[k + 1].val.upper() == "JOIN":
                ok = True
        jm_cache_start = idx
        jm_cache_end = k
        jm_cache_ok = ok
        return ok

    while i < n:
        tok = tokens[i]
        if not in_from:
            tk_q = tok.kind
            if tk_q not in ("WORD", "QIDENT", "LPAREN", "LBRACK", "LBRACE"):
                at_branch_start = False
                i += 1
                continue
            if tk_q == "WORD" and not at_branch_start and (i + 1 >= n or tokens[i + 1].kind != "LPAREN"):
                if tok.val.upper() not in outside_from_kws:
                    i += 1
                    continue
        if (
            in_from
            and not expect_table
            and not in_on_clause
            and not in_sample_clause
            and not in_array_join
            and tok.kind == "WORD"
            and tok.val.upper() == "AS"
            and not _is_dot_or_typecast_tok(tokens, i - 1)
        ):
            if not allow_table_alias or in_join_mod or i + 1 >= n or tokens[i + 1].kind not in ("WORD", "QIDENT"):
                node["children"].append(
                    {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid table alias after AS"}
                )
                in_from = False
                allow_table_alias = False
                in_join_mod = False
                i += 1
                continue
            alias_tok = tokens[i + 1]
            if alias_tok.kind == "WORD":
                au = alias_tok.val.upper()
                if au in _DISALLOWED_UNQUOTED_AS_TABLE_ALIASES:
                    node["children"].append(
                        {
                            "type": "PARSE_ERROR_NODE",
                            "message": "SQL parse error: reserved keyword cannot be unquoted table alias",
                        }
                    )
                    in_from = False
                    allow_table_alias = False
                    in_join_mod = False
                    i += 2
                    continue
                if au in ("SELECT", "UNION"):
                    nxt2 = tokens[i + 2] if i + 2 < n else None
                    if not (
                        nxt2 is not None
                        and nxt2.kind == "WORD"
                        and nxt2.val.upper() in ("WHERE", "ON")
                        and not _is_dot_qualifier_at(tokens, i + 2)
                    ):
                        node["children"].append(
                            {
                                "type": "PARSE_ERROR_NODE",
                                "message": "SQL parse error: invalid clause after AS alias",
                            }
                        )
                        in_from = False
                        allow_table_alias = False
                        in_join_mod = False
                        i += 2
                        continue
            allow_table_alias = False
            at_branch_start = False
            i += 2
            if i < n and tokens[i].kind == "LPAREN":
                rp_alias = _find_matching_rparen(tokens, i)
                if rp_alias == -1:
                    node["children"].append(
                        {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid column alias list"}
                    )
                    in_from = False
                    continue
                alias_cols = tokens[i + 1 : rp_alias]
                if not (alias_tok.kind == "WORD" and alias_tok.val.lower() in _NON_FUNCTION_KEYWORDS):
                    node["children"].append({"class": "FUNCTION", "function_name": alias_tok.val})
                _scan_expr_tokens(alias_cols, node)
                if not _is_valid_ident_list(alias_cols):
                    node["children"].append(
                        {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid column alias list"}
                    )
                    in_from = False
                i = rp_alias + 1
            continue

        if (
            tok.kind == "WORD"
            and len(tok.val) >= 2
            and tok.val.upper() in _SCAN_BODY_WORD_KEYWORDS
            and not _is_non_keyword_ident_pos(tokens, i)
        ):
            wu = tok.val.upper()
            if wu in ("PIVOT", "UNPIVOT", "PIVOT_WIDER", "PIVOT_LONGER"):
                node["children"].append(
                    {
                        "type": "DISALLOWED_SUBQUERY_STATEMENT",
                        "reason": f"statement_type:{wu}_NODE",
                        "message": f"only SELECT / WITH / UNION statements allowed (got {wu}_NODE)",
                    }
                )
                i += 1
                continue
            if at_branch_start:
                if wu in _SHOW_REF_STATEMENTS:
                    node["children"].append(
                        {
                            "type": "DISALLOWED_SUBQUERY_STATEMENT",
                            "reason": "show_ref",
                            "message": "SHOW / DESCRIBE / SUMMARIZE statements are not allowed via /api/query",
                        }
                    )
                    return i
                if wu in _NON_SELECT_STATEMENTS and not (
                    wu in ("REPLACE", "TRUNCATE") and i + 1 < n and tokens[i + 1].kind == "LPAREN"
                ):
                    node["children"].append(
                        {
                            "type": "DISALLOWED_SUBQUERY_STATEMENT",
                            "reason": f"statement_type:{wu}_NODE",
                            "message": f"only SELECT / WITH / UNION statements allowed (got {wu}_NODE)",
                        }
                    )
                    return i
            is_dot_qual = i + 2 < n and tokens[i + 1].kind == "DOT" and _is_dot_qualifier_at(tokens, i)
            is_on_ident = (
                in_on_clause
                and not expect_table
                and (wu in _CLAUSE_STOP_KEYWORDS or wu in ("SELECT", "FROM", "JOIN", "TABLE"))
                and _is_on_expr_ident_at(tokens, i)
            )
            if is_dot_qual or is_on_ident:
                in_sample_clause = False
            else:
                if at_branch_start and wu == "WITH":
                    if i == 0:
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: unexpected WITH clause"}
                        )
                        i += 1
                        at_branch_start = False
                        continue
                    branch_node, consumed = _parse_branch_with_tokens(tokens[i:], expr_cte_refs=expr_cte_refs)
                    node["children"].append(branch_node)
                    i += consumed
                    at_branch_start = False
                    continue
                if wu in ("UNION", "INTERSECT", "EXCEPT") and not _is_star_except_clause(tokens, i):
                    if expect_table or in_join_mod:
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                        )
                    if stop_at_setop:
                        return i
                    node["type"] = "SET_OPERATION_NODE"
                    in_from = False
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = False
                    in_array_join = False
                    allow_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    saw_join = False
                    i += 1
                    while (
                        i < n
                        and tokens[i].kind == "WORD"
                        and tokens[i].val.upper() in ("ALL", "DISTINCT", "BY", "NAME")
                    ):
                        i += 1
                    at_branch_start = True
                    continue
                if wu == "SELECT":
                    if (
                        in_from
                        and not expect_table
                        and not in_on_clause
                        and not in_sample_clause
                        and not in_array_join
                        and allow_table_alias
                        and not in_join_mod
                        and not saw_final
                    ):
                        nxt_tok = tokens[i + 1] if i + 1 < n else None
                        is_fn_projection = (
                            nxt_tok is not None
                            and nxt_tok.kind == "WORD"
                            and nxt_tok.val.upper() in _FROM_FIRST_SELECT_FN_KEYWORDS
                            and i + 2 < n
                            and tokens[i + 2].kind == "LPAREN"
                        )
                        is_bare_select_alias = not is_fn_projection and (
                            nxt_tok is None
                            or nxt_tok.kind == "COMMA"
                            or (
                                nxt_tok.kind == "WORD"
                                and not _is_dot_qualifier_at(tokens, i + 1)
                                and nxt_tok.val.upper()
                                in (
                                    (_JOIN_MODIFIERS - {"ALL"})
                                    | _CLAUSE_STOP_KEYWORDS
                                    | {"ON", "USING", "FINAL", "SELECT"}
                                )
                            )
                        )
                        if is_bare_select_alias:
                            allow_table_alias = False
                            in_sample_clause = False
                            at_branch_start = False
                            i += 1
                            continue
                    if expect_table or in_join_mod:
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                        )
                    in_from = False
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = False
                    in_array_join = False
                    allow_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    saw_join = False
                    at_branch_start = False
                    i += 1
                    continue
                if (wu == "FROM" and not _is_distinct_from_op(tokens, i)) or wu == "TABLE":
                    in_from = True
                    expect_table = True
                    in_sample_clause = False
                    in_on_clause = False
                    in_array_join = False
                    allow_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    saw_join = False
                    at_branch_start = False
                    i += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_sample_clause
                    and not (in_on_clause and _is_on_expr_ident_at(tokens, i))
                    and wu == "ARRAY"
                    and i + 1 < n
                    and tokens[i + 1].kind == "WORD"
                    and tokens[i + 1].val.upper() == "JOIN"
                ):
                    in_array_join = True
                    in_on_clause = False
                    allow_table_alias = False
                    in_join_mod = False
                    at_branch_start = False
                    i += 2
                    continue
                if wu == "JOIN":
                    in_from = True
                    in_array_join = False
                    expect_table = True
                    saw_join = True
                    in_sample_clause = False
                    in_on_clause = False
                    allow_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    at_branch_start = False
                    i += 1
                    continue
                if wu in (_JOIN_MODIFIERS - {"JOIN"}):
                    if in_from and not expect_table:
                        if _starts_join_modifier_seq(i):
                            in_sample_clause = False
                            in_on_clause = False
                            in_array_join = False
                            allow_table_alias = False
                            in_join_mod = True
                            at_branch_start = False
                            i += 1
                            continue
                        if not in_on_clause and not in_sample_clause and not in_array_join:
                            node["children"].append(
                                {
                                    "type": "PARSE_ERROR_NODE",
                                    "message": "SQL parse error: incomplete JOIN modifier in FROM clause",
                                }
                            )
                            in_from = False
                            allow_table_alias = False
                            in_join_mod = False
                            i += 1
                            continue
                    elif not in_from:
                        in_sample_clause = False
                        at_branch_start = False
                        i += 1
                        continue
                if (
                    in_from
                    and not expect_table
                    and not in_on_clause
                    and not in_sample_clause
                    and not in_array_join
                    and not in_join_mod
                    and not saw_final
                    and wu == "FINAL"
                ):
                    saw_final = True
                    allow_table_alias = False
                    at_branch_start = False
                    i += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_on_clause
                    and not in_array_join
                    and not in_join_mod
                    and wu == "TABLESAMPLE"
                ):
                    in_sample_clause = True
                    allow_table_alias = False
                    at_branch_start = False
                    i += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_on_clause
                    and not in_sample_clause
                    and not in_array_join
                    and not in_join_mod
                    and wu in ("SETTINGS", "FORMAT", "PREWHERE", "SAMPLE", "FOR", "SET")
                ):
                    nxt_tok = tokens[i + 1] if i + 1 < n else None
                    is_lparen_alias = False
                    if nxt_tok is not None and nxt_tok.kind == "LPAREN":
                        if wu == "PREWHERE":
                            rp_pw = _find_matching_rparen(tokens, i + 1)
                            after_pw = tokens[rp_pw + 1] if (rp_pw != -1 and rp_pw + 1 < n) else None
                            is_lparen_alias = (
                                rp_pw > i + 1
                                and _is_valid_ident_list(tokens[i + 2 : rp_pw])
                                and after_pw is not None
                                and (
                                    after_pw.kind == "COMMA"
                                    or (
                                        after_pw.kind == "WORD"
                                        and not _is_dot_qualifier_at(tokens, rp_pw + 1)
                                        and after_pw.val.upper() in (_JOIN_MODIFIERS | {"FINAL"})
                                    )
                                )
                            )
                        else:
                            is_lparen_alias = True
                    is_bare_table_alias = (
                        allow_table_alias
                        and not saw_final
                        and (
                            nxt_tok is None
                            or nxt_tok.kind == "COMMA"
                            or is_lparen_alias
                            or (
                                nxt_tok.kind == "WORD"
                                and not _is_dot_qualifier_at(tokens, i + 1)
                                and nxt_tok.val.upper()
                                in (_JOIN_MODIFIERS | _CLAUSE_STOP_KEYWORDS | {"ON", "USING", "FINAL", "SELECT"})
                            )
                        )
                    )
                    if is_bare_table_alias or wu == "SAMPLE":
                        in_sample_clause = wu == "SAMPLE" and not is_bare_table_alias
                        allow_table_alias = False
                        at_branch_start = False
                        i += 1
                        if is_bare_table_alias and i < n and tokens[i].kind == "LPAREN":
                            rp_alias = _find_matching_rparen(tokens, i)
                            if rp_alias != -1:
                                alias_cols = tokens[i + 1 : rp_alias]
                                if wu.lower() not in _NON_FUNCTION_KEYWORDS:
                                    node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                                _scan_expr_tokens(alias_cols, node)
                                if not _is_valid_ident_list(alias_cols):
                                    node["children"].append(
                                        {
                                            "type": "PARSE_ERROR_NODE",
                                            "message": "SQL parse error: invalid column alias list",
                                        }
                                    )
                                    in_from = False
                                i = rp_alias + 1
                        continue
                if in_from and in_sample_clause and wu == "OFFSET":
                    at_branch_start = False
                    i += 1
                    continue
                if (
                    wu in _CLAUSE_STOP_KEYWORDS
                    and not _is_star_except_clause(tokens, i)
                    and _can_exit_from_on_stop_kw(tokens, i, in_on_clause=in_on_clause)
                ):
                    if expect_table or in_join_mod:
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                        )
                    in_from = False
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = False
                    in_array_join = False
                    allow_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    saw_join = False
                    at_branch_start = False
                    i += 1
                    continue
                if in_from and (wu == "ON" or (wu == "USING" and not in_on_clause)):
                    is_using_sample = (
                        wu == "USING"
                        and not expect_table
                        and not in_join_mod
                        and i + 1 < n
                        and tokens[i + 1].kind == "WORD"
                        and tokens[i + 1].val.upper() == "SAMPLE"
                    )
                    if is_using_sample:
                        in_sample_clause = True
                        allow_table_alias = False
                        at_branch_start = False
                        i += 2
                        continue
                    if expect_table or in_join_mod or not saw_join:
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid ON/USING in FROM clause"}
                        )
                        in_from = False
                        expect_table = False
                        in_sample_clause = False
                        in_on_clause = False
                        in_array_join = False
                        allow_table_alias = False
                        in_join_mod = False
                        saw_join = False
                        i += 1
                        continue
                    expect_table = False
                    in_sample_clause = False
                    allow_table_alias = False
                    at_branch_start = False
                    if wu == "ON":
                        in_on_clause = True
                        i += 1
                        continue
                    # USING (<valid_ident_list>)
                    if i + 1 >= n or tokens[i + 1].kind != "LPAREN":
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected column list after USING"}
                        )
                        in_from = False
                        i += 1
                        continue
                    rp_using = _find_matching_rparen(tokens, i + 1)
                    if rp_using == -1 or not _is_valid_ident_list(tokens[i + 2 : rp_using]):
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid USING column list"}
                        )
                        in_from = False
                        i += 1
                        continue
                    _scan_expr_tokens(tokens[i + 2 : rp_using], node)
                    in_on_clause = False
                    i = rp_using + 1
                    continue

        if in_from and tok.kind == "COMMA":
            if expect_table or in_join_mod:
                node["children"].append(
                    {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                )
            expect_table = not in_array_join
            in_sample_clause = False
            in_on_clause = False
            allow_table_alias = False
            saw_final = False
            in_join_mod = False
            saw_join = False
            at_branch_start = False
            i += 1
            continue

        if in_from and expect_table:
            if tok.kind == "WORD" and tok.val.upper() in ("LATERAL", "ONLY"):
                i += 1
                continue
            if tok.kind == "LPAREN":
                rp = _find_matching_rparen(tokens, i)
                if rp != -1:
                    inner = _strip_outer_parens(tokens[i + 1 : rp])
                    if _is_subquery_token_list(inner):
                        node["children"].append(_parse_query_tokens(inner, expr_cte_refs=expr_cte_refs))
                    else:
                        _scan_body_tokens(inner, node, in_from=True, expect_table=True, expr_cte_refs=expr_cte_refs)
                    expect_table = False
                    allow_table_alias = True
                    saw_final = False
                    in_join_mod = False
                    at_branch_start = False
                    i = rp + 1
                    continue
            if tok.kind == "STRING":
                node["children"].append(
                    {
                        "type": "BASE_TABLE",
                        "table_name": tok.val,
                        "schema_name": "",
                        "catalog_name": "",
                    }
                )
                expect_table = False
                allow_table_alias = True
                saw_final = False
                in_join_mod = False
                at_branch_start = False
                i += 1
                continue
            if tok.kind in ("WORD", "QIDENT"):
                parts = [tok.val]
                j = i + 1
                while j + 1 < n and tokens[j].kind == "DOT" and tokens[j + 1].kind in ("WORD", "QIDENT", "STRING"):
                    parts.append(tokens[j + 1].val)
                    j += 2
                if j < n and tokens[j].kind == "LPAREN":
                    rp = _find_matching_rparen(tokens, j)
                    if rp != -1:
                        node["children"].append(
                            {
                                "class": "FUNCTION",
                                "type": "TABLE_FUNCTION",
                                "function_name": parts[-1],
                            }
                        )
                        _scan_expr_tokens(tokens[j + 1 : rp], node, expr_cte_refs=expr_cte_refs)
                        i = rp + 1
                        # Skip optional table alias and column alias list: e.g. `range(3) t(n)` or `AS t(n)`
                        used_as_tf = False
                        if i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() == "AS":
                            used_as_tf = True
                            i += 1
                        if i < n and tokens[i].kind in ("WORD", "QIDENT"):
                            tf_au = tokens[i].val.upper() if tokens[i].kind == "WORD" else ""
                            tf_bad_as = used_as_tf and (
                                tf_au in _DISALLOWED_UNQUOTED_AS_TABLE_ALIASES
                                or (
                                    tf_au in ("SELECT", "UNION")
                                    and not (
                                        i + 1 < n
                                        and tokens[i + 1].kind == "WORD"
                                        and tokens[i + 1].val.upper() in ("WHERE", "ON")
                                        and not _is_dot_qualifier_at(tokens, i + 1)
                                    )
                                )
                            )
                            if tf_bad_as:
                                node["children"].append(
                                    {
                                        "type": "PARSE_ERROR_NODE",
                                        "message": "SQL parse error: reserved keyword cannot be unquoted table alias",
                                    }
                                )
                                in_from = False
                            elif used_as_tf or tf_au not in _DISALLOWED_BARE_TABLE_ALIASES:
                                i += 1
                                if i < n and tokens[i].kind == "LPAREN":
                                    rp_alias = _find_matching_rparen(tokens, i)
                                    if rp_alias != -1:
                                        alias_cols = tokens[i + 1 : rp_alias]
                                        _scan_expr_tokens(alias_cols, node)
                                        if not _is_valid_ident_list(alias_cols):
                                            node["children"].append(
                                                {
                                                    "type": "PARSE_ERROR_NODE",
                                                    "message": "SQL parse error: invalid column alias list",
                                                }
                                            )
                                            in_from = False
                                        i = rp_alias + 1
                        elif used_as_tf:
                            node["children"].append(
                                {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid table alias after AS"}
                            )
                            in_from = False
                        expect_table = False
                        allow_table_alias = False
                        saw_final = False
                        in_join_mod = False
                        at_branch_start = False
                        continue
                else:
                    if len(parts) == 1:
                        cat_name, sch_name, tbl_name = "", "", parts[0]
                    elif len(parts) == 2:
                        cat_name, sch_name, tbl_name = "", parts[0], parts[1]
                    elif len(parts) == 3:
                        cat_name, sch_name, tbl_name = parts[0], parts[1], parts[2]
                    else:
                        node["children"].append(
                            {
                                "type": "PARSE_ERROR_NODE",
                                "message": "SQL parse error: table identifier has too many qualifier parts",
                            }
                        )
                        expect_table = False
                        allow_table_alias = False
                        saw_final = False
                        in_join_mod = False
                        at_branch_start = False
                        i = j
                        continue
                    node["children"].append(
                        {
                            "type": "BASE_TABLE",
                            "table_name": tbl_name,
                            "schema_name": sch_name,
                            "catalog_name": cat_name,
                        }
                    )
                    expect_table = False
                    allow_table_alias = True
                    saw_final = False
                    in_join_mod = False
                    at_branch_start = False
                    i = j
                    continue
            node["children"].append(
                {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
            )
            expect_table = False
            allow_table_alias = False
            in_join_mod = False
            at_branch_start = False
            i += 1
            continue

        at_branch_start = False

        if in_from and not expect_table and not in_on_clause and not in_sample_clause and not in_array_join:
            if (
                allow_table_alias
                and not in_join_mod
                and not saw_final
                and tok.kind in ("WORD", "QIDENT")
                and not (tok.kind == "WORD" and tok.val.upper() in _DISALLOWED_BARE_TABLE_ALIASES)
            ):
                allow_table_alias = False
                if i + 1 < n and tokens[i + 1].kind == "LPAREN":
                    rp_alias = _find_matching_rparen(tokens, i + 1)
                    if rp_alias == -1:
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid column alias list"}
                        )
                        in_from = False
                        i += 1
                        continue
                    alias_cols = tokens[i + 2 : rp_alias]
                    if not (tok.kind == "WORD" and tok.val.lower() in _NON_FUNCTION_KEYWORDS):
                        node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                    _scan_expr_tokens(alias_cols, node)
                    if not _is_valid_ident_list(alias_cols):
                        node["children"].append(
                            {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: invalid column alias list"}
                        )
                        in_from = False
                    i = rp_alias + 1
                    continue
                i += 1
                continue
            node["children"].append(
                {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: unexpected token in FROM clause"}
            )
            in_from = False
            allow_table_alias = False
            in_join_mod = False

        # Scalar expression / subquery outside table-reference position
        if (
            tok.kind == "WORD"
            and tok.val.upper() == "IN"
            and not _is_non_keyword_ident_pos(tokens, i)
            and not _is_dot_qualifier_at(tokens, i)
        ):
            if i + 1 < n and tokens[i + 1].kind == "LPAREN":
                rp = _find_matching_rparen(tokens, i + 1)
                if _is_prefix_in_call(tokens, i):
                    node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                    if rp != -1:
                        _scan_expr_tokens(tokens[i + 2 : rp], node, expr_cte_refs=expr_cte_refs)
                        i = rp + 1
                        continue
                elif rp != -1:
                    rhs = tokens[i + 2 : rp]
                    if not _is_subquery_token_list(rhs):
                        tbl_ref = _resolve_in_rhs_table_ref(rhs, expr_cte_refs)
                        if tbl_ref is not None:
                            node["children"].append(tbl_ref)
            else:
                node["children"].append(
                    {
                        "type": "PARSE_ERROR_NODE",
                        "message": "SQL parse error: IN must be followed by a parenthesized list or subquery",
                    }
                )
            i += 1
            continue
        if tok.kind in ("WORD", "QIDENT") and i + 1 < n and tokens[i + 1].kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i + 1)
            if rp != -1:
                if tok.kind == "WORD" and tok.val.lower() in _NON_FUNCTION_KEYWORDS:
                    if tok.val.lower() in _SPECIAL_CALL_KEYWORDS:
                        _scan_expr_tokens(
                            tokens[i + 2 : rp],
                            node,
                            enclosing_special_call=tok.val.lower(),
                            expr_cte_refs=expr_cte_refs,
                        )
                        i = rp + 1
                        continue
                    i += 1
                    continue
                node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                _scan_expr_tokens(tokens[i + 2 : rp], node, expr_cte_refs=expr_cte_refs)
                i = rp + 1
                continue
        if tok.kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i)
            if rp != -1:
                body_match_map, _, _ = _get_tok_meta(tokens)
                lo_p, hi_p = i + 1, rp - 1
                while lo_p < hi_p and tokens[lo_p].kind == "LPAREN" and body_match_map.get(lo_p) == hi_p:
                    lo_p += 1
                    hi_p -= 1
                _scan_expr_tokens(tokens[lo_p : hi_p + 1], node, expr_cte_refs=expr_cte_refs)
                i = rp + 1
                continue
        if tok.kind in ("LBRACK", "LBRACE"):
            rb = _find_matching_rdelim(tokens, i)
            if rb != -1:
                _scan_expr_tokens(
                    tokens[i + 1 : rb],
                    node,
                    allow_direct_subquery=False,
                    in_bracket=(tok.kind == "LBRACK"),
                    expr_cte_refs=expr_cte_refs,
                )
                i = rb + 1
                continue

        i += 1

    if expect_table or in_join_mod:
        node["children"].append({"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"})
    return n


def _parse_single_select_node(
    sql: str, parser_con: Any | None = None, *, allow_multi: bool = False
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None, str | None]:
    del parser_con
    tokens, tok_err = _tokenize_sql(sql)
    if tok_err is not None or tokens is None:
        return None, None, "parse_error:SYNTAX", f"SQL parse error: {tok_err}"

    # Split into statements on top-level semicolons and enforce matched ()/[]/{} delimiters
    stmts: list[list[_Tok]] = []
    if not any(ch in sql for ch in ";()[]{}"):
        if tokens:
            stmts.append(tokens)
    else:
        cur: list[_Tok] = []
        delim_stack: list[str] = []
        for tok in tokens:
            if tok.kind in _DELIM_PAIRS:
                delim_stack.append(_DELIM_PAIRS[tok.kind])
                if len(delim_stack) > MAX_PAREN_DEPTH:
                    return None, None, "nesting_depth_exceeded", "SQL nesting depth limit exceeded"
                cur.append(tok)
            elif tok.kind in _CLOSING_DELIMS:
                if not delim_stack or delim_stack[-1] != tok.kind:
                    return None, None, "parse_error:SYNTAX", "SQL parse error: unmatched closing delimiter"
                delim_stack.pop()
                cur.append(tok)
            elif tok.kind == "SEMI" and not delim_stack:
                if cur:
                    stmts.append(cur)
                    cur = []
            else:
                cur.append(tok)
        if delim_stack:
            return None, None, "parse_error:SYNTAX", "SQL parse error: unbalanced delimiters"
        if cur:
            stmts.append(cur)

    if not stmts:
        return None, None, "no_statements", "no parseable statements"
    if not allow_multi and len(stmts) > 1:
        return None, None, "multi_statement", f"only one statement allowed, got {len(stmts)}"

    prev_cache = getattr(_TOK_META_TLS, "cache", None)
    prev_last = getattr(_TOK_META_TLS, "last", None)
    prev_has_end = getattr(_TOK_META_TLS, "has_end", True)
    _TOK_META_TLS.cache = {}
    _TOK_META_TLS.last = None
    _TOK_META_TLS.has_end = "END" in sql.upper()
    try:
        stmt_tokens = _strip_outer_parens(stmts[0])
        if not stmt_tokens:
            return None, None, "no_statements", "no parseable statements"

        first = stmt_tokens[0]
        if first.kind != "WORD":
            return None, None, "parse_error:SYNTAX", "SQL parse error: expected statement keyword"

        first_upper = first.val.upper()
        if first_upper in _SHOW_REF_STATEMENTS:
            return None, None, "show_ref", "SHOW / DESCRIBE / SUMMARIZE statements are not allowed via /api/query"
        if first_upper in _NON_SELECT_STATEMENTS:
            node_type = f"{first_upper}_NODE"
            return (
                None,
                None,
                f"statement_type:{node_type}",
                f"only SELECT / WITH / UNION statements allowed (got {node_type})",
            )
        if first_upper not in ("SELECT", "WITH", "VALUES", "FROM", "TABLE"):
            return None, None, "parse_error:SYNTAX", f"SQL parse error near {first.val!r}"

        syntax_err = _validate_select_syntax(stmt_tokens)
        if syntax_err is not None:
            return None, None, "parse_error:SYNTAX", f"SQL parse error: {syntax_err}"

        node = _parse_query_tokens(stmt_tokens)
        parsed = {"error": False, "statements": [{"node": node}]}
        return parsed, node, None, None
    finally:
        _TOK_META_TLS.cache = prev_cache
        _TOK_META_TLS.last = prev_last
        _TOK_META_TLS.has_end = prev_has_end


def is_simple_select_statement(sql: str, *, parser_con: Any | None = None) -> bool:
    _parsed, node, err_code, _err_msg = _parse_single_select_node(sql, parser_con, allow_multi=True)
    return err_code is None and node is not None


__all__ = [
    "ALLOWED_CATALOG",
    "ALLOWED_STATEMENT_TYPES",
    "MAX_INPUT_BYTES",
    "SAFE_IDENT_RE",
    "SQLValidationError",
    "ValidationResult",
    "_ALLOWED_TABLE_FUNCTIONS",
    "_BASELINE_TABLE_FUNCTIONS",
    "_BLOCKED_FUNCTIONS",
    "_BLOCKED_FUNCTION_PREFIXES",
    "_BLOCKED_SCHEMA_NAMES",
    "_BLOCKED_TABLE_PREFIXES",
    "_TABLE_FUNCTIONS",
    "_audit_logger",
    "_check_function_node",
    "_check_table_reference",
    "_enumerate_table_functions",
    "_parse_single_select_node",
    "_reject",
    "_walk_and_validate",
    "escape_sql_literal",
    "is_safe_ident",
    "is_simple_select_statement",
    "require_safe_ident",
    "validate_user_sql",
]
