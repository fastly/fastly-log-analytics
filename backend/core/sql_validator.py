"""Pure-Python SQL validation and identifier/literal escaping for user queries."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import Any, NoReturn


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
    if any(ch in table_name for ch in ("/", "\\", ".")):
        _reject(
            original_sql,
            "catalog_blocklist:table_name_path",
            f"table name '{table_name}' contains path-like characters",
            session_id,
            service_id,
        )
    name_lower = table_name.lower()
    if blocked_pfx := next((p for p in _BLOCKED_TABLE_PREFIXES if name_lower.startswith(p)), None):
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
        if node.get("type") == "BASE_TABLE" or "table_name" in node:
            _check_table_reference(
                node, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
            )
        if node.get("class") == "FUNCTION":
            _check_function_node(node, original_sql, session_id, service_id)
        for key, value in node.items():
            if not (key == "cte_map" and allowed_tables is not None):
                _walk_and_validate(
                    value, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
                )
    elif isinstance(node, list):
        for item in node:
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
_DUCKDB_UNICODE_SPACES = frozenset(
    "\u00a0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b\u202f\u205f\u2060\u3000\ufeff"
)
_DISALLOWED_CTRL_OR_SEP_RE = re.compile(r"[\x00-\x08\x0b\x0e-\x1f\x7f-\x9f\u1680\u180e\u2028\u2029]")
_DOLQ_TAG_RE = re.compile(r"\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$")


@dataclass(slots=True)
class _Tok:
    kind: str  # "WORD" | "STRING" | "QIDENT" | "LPAREN" | "RPAREN" | "COMMA" | "DOT" | "SEMI" | "OTHER"
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


def _scan_sql_number_end(sql: str, start: int) -> int:
    n = len(sql)
    i = start + 1
    if sql[start] == "0" and i + 1 < n:
        radix = sql[i]
        if radix in ("x", "X") and (sql[i + 1].isdigit() or sql[i + 1] in "abcdefABCDEF_"):
            i += 2
            while i < n and (sql[i].isdigit() or sql[i] in "abcdefABCDEF_"):
                i += 1
            return i
        if radix in ("b", "B") and sql[i + 1] in "01_":
            i += 2
            while i < n and sql[i] in "01_":
                i += 1
            return i
        if radix in ("o", "O") and sql[i + 1] in "01234567_":
            i += 2
            while i < n and sql[i] in "01234567_":
                i += 1
            return i
    while i < n:
        c = sql[i]
        if c.isdigit() or c in "._":
            i += 1
        elif c in ("e", "E") and i + 1 < n and sql[i + 1].isdigit():
            i += 2
        elif c in ("e", "E") and i + 2 < n and sql[i + 1] in ("+", "-") and sql[i + 2].isdigit():
            i += 3
        else:
            break
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


def _tokenize_sql(sql: str) -> tuple[list[_Tok] | None, str | None]:
    tokens: list[_Tok] = []
    i = 0
    n = len(sql)
    prev_was_ident_or_num = False
    while i < n:
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
                while j < n:
                    if is_escape_str and sql[j] == "\\":
                        if j + 1 >= n:
                            return None, "unterminated escape string literal"
                        buf.append(sql[j + 1])
                        j += 2
                        continue
                    if sql[j] == "'":
                        if j + 1 < n and sql[j + 1] == "'":
                            buf.append("'")
                            j += 2
                            continue
                        closed = True
                        j += 1
                        break
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
        if ch == ",":
            tokens.append(_Tok("COMMA", ","))
            i += 1
            prev_was_ident_or_num = False
            continue
        if ch == ".":
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
            and sql[j] not in ("'", '"', "(", ")", ",", ".", ";", "-", "/", "$")
            and not _is_ident_start_char(sql[j])
            and not sql[j].isdigit()
        ):
            j += 1
        tokens.append(_Tok("OTHER", sql[i:j]))
        i = j
        prev_was_ident_or_num = False
    return tokens, None


def _find_matching_rparen(tokens: list[_Tok], lparen_idx: int) -> int:
    depth = 0
    for idx in range(lparen_idx, len(tokens)):
        if tokens[idx].kind == "LPAREN":
            depth += 1
        elif tokens[idx].kind == "RPAREN":
            depth -= 1
            if depth == 0:
                return idx
    return -1


def _strip_outer_parens(tokens: list[_Tok]) -> list[_Tok]:
    while len(tokens) >= 2 and tokens[0].kind == "LPAREN":
        rp = _find_matching_rparen(tokens, 0)
        if rp == len(tokens) - 1:
            tokens = tokens[1:-1]
        else:
            break
    return tokens


def _validate_select_syntax(tokens: list[_Tok]) -> str | None:
    """Catch malformed SELECT / WHERE clauses so invalid SQL fails closed."""
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok.kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i)
            if rp == -1:
                return "unbalanced parentheses"
            err = _validate_select_syntax(tokens[i + 1 : rp])
            if err is not None:
                return err
            i = rp + 1
            continue
        if tok.kind == "RPAREN":
            return "unmatched closing parenthesis"
        if tok.kind == "WORD":
            wu = tok.val.upper()
            if wu == "SELECT":
                nxt = i + 1
                if nxt < n and tokens[nxt].kind == "WORD" and tokens[nxt].val.upper() in ("DISTINCT", "ALL"):
                    nxt += 1
                if nxt >= n or (
                    tokens[nxt].kind == "WORD"
                    and tokens[nxt].val.upper() in ("FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "UNION")
                ):
                    return "empty SELECT projection list"
            elif wu == "WHERE":
                nxt = i + 1
                if nxt >= n or (tokens[nxt].kind == "OTHER" and tokens[nxt].val.startswith("*")):
                    return "invalid WHERE clause"
                if tokens[nxt].kind == "WORD" and tokens[nxt].val.upper() in (
                    "FROM",
                    "WHERE",
                    "GROUP",
                    "HAVING",
                    "ORDER",
                    "LIMIT",
                ):
                    return "empty WHERE clause"
        i += 1
    return None


def _has_top_level_setop(tokens: list[_Tok]) -> bool:
    unwrapped = _strip_outer_parens(tokens)
    k = 0
    m = len(unwrapped)
    while k < m:
        tk = unwrapped[k]
        if tk.kind == "LPAREN":
            rp = _find_matching_rparen(unwrapped, k)
            if rp == -1:
                return True
            k = rp + 1
            continue
        if tk.kind == "WORD" and tk.val.upper() in ("UNION", "INTERSECT", "EXCEPT"):
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


def _parse_recursive_cte_tokens(tokens: list[_Tok]) -> dict[str, Any]:
    inner = _strip_outer_parens(tokens)
    k = 0
    m = len(inner)
    last_union_or_except_op = ""
    last_idx = -1
    while k < m:
        tk = inner[k]
        if tk.kind == "LPAREN":
            rp = _find_matching_rparen(inner, k)
            if rp == -1:
                last_union_or_except_op = ""
                last_idx = -1
                break
            k = rp + 1
            continue
        if tk.kind == "WORD" and tk.val.upper() in ("UNION", "EXCEPT"):
            last_union_or_except_op = tk.val.upper()
            last_idx = k
        k += 1

    if last_union_or_except_op != "UNION" or last_idx == -1:
        body = _parse_query_tokens(inner)
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
    ):
        body = _parse_query_tokens(inner)
        body["type"] = "RECURSIVE_CTE_NODE"
        return body

    return {
        "type": "RECURSIVE_CTE_NODE",
        "setop_type": "UNION",
        "left": _parse_query_tokens(inner[:last_idx]),
        "right": _parse_query_tokens(right_tokens),
    }


def _parse_query_tokens(tokens: list[_Tok]) -> dict[str, Any]:
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
        if w0 in _NON_SELECT_STATEMENTS and not (len(tokens) > 1 and tokens[1].kind == "LPAREN"):
            node["children"].append(
                {
                    "type": "DISALLOWED_SUBQUERY_STATEMENT",
                    "reason": f"statement_type:{w0}_NODE",
                    "message": f"only SELECT / WITH / UNION statements allowed (got {w0}_NODE)",
                }
            )
            return node

    i = 0
    n = len(tokens)
    if tokens[0].kind == "WORD" and tokens[0].val.upper() == "WITH":
        i = 1
        is_recursive = False
        if i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() == "RECURSIVE":
            is_recursive = True
            i += 1
        cte_entries: list[dict[str, Any]] = []
        while i < n:
            if tokens[i].kind not in ("WORD", "QIDENT"):
                break
            cte_name = tokens[i].val
            i += 1
            if i < n and tokens[i].kind == "LPAREN":
                rp = _find_matching_rparen(tokens, i)
                if rp == -1:
                    break
                i = rp + 1
            if i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() == "AS":
                i += 1
            while i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() in ("MATERIALIZED", "NOT"):
                i += 1
            if i >= n or tokens[i].kind != "LPAREN":
                break
            rp = _find_matching_rparen(tokens, i)
            if rp == -1:
                break
            if is_recursive:
                cte_body = _parse_recursive_cte_tokens(tokens[i + 1 : rp])
            else:
                cte_body = _parse_query_tokens(tokens[i + 1 : rp])
            cte_entries.append({"key": cte_name, "value": {"query": {"node": cte_body}}})
            i = rp + 1
            if i < n and tokens[i].kind == "COMMA":
                i += 1
                continue
            break
        node["cte_map"] = {"map": cte_entries}

    _scan_body_tokens(tokens[i:], node)
    return node


def _is_subquery_token_list(tokens: list[_Tok], *, enclosing_special_call: str | None = None) -> bool:
    unwrapped = _strip_outer_parens(tokens)
    if not unwrapped or unwrapped[0].kind != "WORD":
        return False
    w0 = unwrapped[0].val.upper()
    if enclosing_special_call in {"trim", "btrim", "ltrim", "rtrim"} and len(unwrapped) == len(tokens) and w0 == "FROM":
        return False
    if w0 in _SUBQUERY_HEAD_KEYWORDS:
        return True
    return w0 in _NON_SELECT_STATEMENTS and not (len(unwrapped) > 1 and unwrapped[1].kind == "LPAREN")


def _scan_expr_tokens(tokens: list[_Tok], node: dict[str, Any], *, enclosing_special_call: str | None = None) -> None:
    if _is_subquery_token_list(tokens, enclosing_special_call=enclosing_special_call):
        node["children"].append(_parse_query_tokens(_strip_outer_parens(tokens)))
        return

    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if (
            tok.kind == "WORD"
            and tok.val.upper() in ("PIVOT", "UNPIVOT", "PIVOT_WIDER", "PIVOT_LONGER")
            and not (i > 0 and tokens[i - 1].kind == "DOT")
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
        if tok.kind in ("WORD", "QIDENT") and i + 1 < n and tokens[i + 1].kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i + 1)
            if rp != -1:
                if tok.kind == "WORD" and tok.val.lower() in _NON_FUNCTION_KEYWORDS:
                    if tok.val.lower() in _SPECIAL_CALL_KEYWORDS:
                        _scan_expr_tokens(tokens[i + 2 : rp], node, enclosing_special_call=tok.val.lower())
                        i = rp + 1
                        continue
                    i += 1
                    continue
                node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                _scan_expr_tokens(tokens[i + 2 : rp], node)
                i = rp + 1
                continue
        if tok.kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i)
            if rp != -1:
                _scan_expr_tokens(tokens[i + 1 : rp], node)
                i = rp + 1
                continue
        i += 1


def _scan_body_tokens(
    tokens: list[_Tok],
    node: dict[str, Any],
    *,
    in_from: bool = False,
    expect_table: bool = False,
) -> None:
    i = 0
    n = len(tokens)
    at_branch_start = not in_from

    while i < n:
        tok = tokens[i]
        if tok.kind == "WORD":
            wu = tok.val.upper()
            prev_is_dot = i > 0 and tokens[i - 1].kind == "DOT"
            if not prev_is_dot and wu in ("PIVOT", "UNPIVOT", "PIVOT_WIDER", "PIVOT_LONGER"):
                node["children"].append(
                    {
                        "type": "DISALLOWED_SUBQUERY_STATEMENT",
                        "reason": f"statement_type:{wu}_NODE",
                        "message": f"only SELECT / WITH / UNION statements allowed (got {wu}_NODE)",
                    }
                )
                i += 1
                continue
            if at_branch_start and not prev_is_dot and not (i + 1 < n and tokens[i + 1].kind == "LPAREN"):
                if wu in _SHOW_REF_STATEMENTS:
                    node["children"].append(
                        {
                            "type": "DISALLOWED_SUBQUERY_STATEMENT",
                            "reason": "show_ref",
                            "message": "SHOW / DESCRIBE / SUMMARIZE statements are not allowed via /api/query",
                        }
                    )
                    return
                if wu in _NON_SELECT_STATEMENTS:
                    node["children"].append(
                        {
                            "type": "DISALLOWED_SUBQUERY_STATEMENT",
                            "reason": f"statement_type:{wu}_NODE",
                            "message": f"only SELECT / WITH / UNION statements allowed (got {wu}_NODE)",
                        }
                    )
                    return
            if wu in ("UNION", "INTERSECT", "EXCEPT"):
                if expect_table:
                    node["children"].append(
                        {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                    )
                node["type"] = "SET_OPERATION_NODE"
                in_from = False
                expect_table = False
                i += 1
                while i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() in ("ALL", "DISTINCT", "BY", "NAME"):
                    i += 1
                at_branch_start = True
                continue
            if wu == "SELECT":
                if expect_table:
                    node["children"].append(
                        {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                    )
                in_from = False
                expect_table = False
                at_branch_start = False
                i += 1
                continue
            if (wu == "FROM" and not _is_distinct_from_op(tokens, i)) or (wu == "TABLE" and not prev_is_dot):
                in_from = True
                expect_table = True
                at_branch_start = False
                i += 1
                continue
            if wu in _JOIN_MODIFIERS:
                if wu == "JOIN":
                    in_from = True
                    expect_table = True
                at_branch_start = False
                i += 1
                continue
            if wu in _CLAUSE_STOP_KEYWORDS:
                if expect_table:
                    node["children"].append(
                        {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                    )
                in_from = False
                expect_table = False
                at_branch_start = False
                i += 1
                continue
            if in_from and wu in ("ON", "USING"):
                if expect_table:
                    node["children"].append(
                        {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                    )
                expect_table = False
                at_branch_start = False
                i += 1
                continue

        if in_from and tok.kind == "COMMA":
            if expect_table:
                node["children"].append(
                    {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
                )
            expect_table = True
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
                        node["children"].append(_parse_query_tokens(inner))
                    else:
                        _scan_body_tokens(inner, node, in_from=True, expect_table=True)
                    expect_table = False
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
                at_branch_start = False
                i += 1
                continue
            if tok.kind in ("WORD", "QIDENT"):
                parts = [tok.val]
                j = i + 1
                while j + 1 < n and tokens[j].kind == "DOT" and tokens[j + 1].kind in ("WORD", "QIDENT"):
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
                        _scan_expr_tokens(tokens[j + 1 : rp], node)
                        i = rp + 1
                        # Skip optional table alias and column alias list: e.g. `range(3) t(n)` or `AS t(n)`
                        if i < n and tokens[i].kind == "WORD" and tokens[i].val.upper() == "AS":
                            i += 1
                        if (
                            i < n
                            and tokens[i].kind in ("WORD", "QIDENT")
                            and tokens[i].val.upper()
                            not in (_CLAUSE_STOP_KEYWORDS | _JOIN_MODIFIERS | {"ON", "USING", "SELECT"})
                        ):
                            i += 1
                            if i < n and tokens[i].kind == "LPAREN":
                                rp_alias = _find_matching_rparen(tokens, i)
                                if rp_alias != -1:
                                    i = rp_alias + 1
                        expect_table = False
                        at_branch_start = False
                        continue
                else:
                    if len(parts) == 1:
                        cat_name, sch_name, tbl_name = "", "", parts[0]
                    elif len(parts) == 2:
                        cat_name, sch_name, tbl_name = "", parts[0], parts[1]
                    else:
                        cat_name, sch_name, tbl_name = parts[0], parts[1], parts[-1]
                    node["children"].append(
                        {
                            "type": "BASE_TABLE",
                            "table_name": tbl_name,
                            "schema_name": sch_name,
                            "catalog_name": cat_name,
                        }
                    )
                    expect_table = False
                    at_branch_start = False
                    i = j
                    continue
            node["children"].append(
                {"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"}
            )
            expect_table = False
            at_branch_start = False
            i += 1
            continue

        at_branch_start = False

        # Scalar expression / subquery outside table-reference position
        if tok.kind in ("WORD", "QIDENT") and i + 1 < n and tokens[i + 1].kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i + 1)
            if rp != -1:
                if tok.kind == "WORD" and tok.val.lower() in _NON_FUNCTION_KEYWORDS:
                    if tok.val.lower() in _SPECIAL_CALL_KEYWORDS:
                        _scan_expr_tokens(tokens[i + 2 : rp], node, enclosing_special_call=tok.val.lower())
                        i = rp + 1
                        continue
                    i += 1
                    continue
                node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                _scan_expr_tokens(tokens[i + 2 : rp], node)
                i = rp + 1
                continue
        if tok.kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i)
            if rp != -1:
                _scan_expr_tokens(tokens[i + 1 : rp], node)
                i = rp + 1
                continue

        i += 1

    if expect_table:
        node["children"].append({"type": "PARSE_ERROR_NODE", "message": "SQL parse error: expected table reference"})


def _parse_single_select_node(
    sql: str, parser_con: Any | None = None, *, allow_multi: bool = False
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None, str | None]:
    del parser_con
    tokens, tok_err = _tokenize_sql(sql)
    if tok_err is not None or tokens is None:
        return None, None, "parse_error:SYNTAX", f"SQL parse error: {tok_err}"

    # Split into statements on top-level semicolons
    stmts: list[list[_Tok]] = []
    cur: list[_Tok] = []
    depth = 0
    for tok in tokens:
        if tok.kind == "LPAREN":
            depth += 1
            cur.append(tok)
        elif tok.kind == "RPAREN":
            depth -= 1
            if depth < 0:
                return None, None, "parse_error:SYNTAX", "SQL parse error: unmatched closing parenthesis"
            cur.append(tok)
        elif tok.kind == "SEMI" and depth == 0:
            if cur:
                stmts.append(cur)
                cur = []
        else:
            cur.append(tok)
    if depth != 0:
        return None, None, "parse_error:SYNTAX", "SQL parse error: unbalanced parentheses"
    if cur:
        stmts.append(cur)

    if not stmts:
        return None, None, "no_statements", "no parseable statements"
    if not allow_multi and len(stmts) > 1:
        return None, None, "multi_statement", f"only one statement allowed, got {len(stmts)}"

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
