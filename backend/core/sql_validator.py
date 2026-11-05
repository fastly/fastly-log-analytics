"""Pure-Python SQL validation and identifier/literal escaping for user queries."""

from __future__ import annotations

import hashlib
import logging
import re
import time
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
    allowed_tables: frozenset[str] | None = None,
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
            if not (key == "cte_map" and allowed_tables is not None):
                _walk_and_validate(
                    value, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
                )
    elif isinstance(node, list):
        for item in node:
            _walk_and_validate(
                item, original_sql, session_id, service_id, allowed_tables=allowed_tables, cte_aliases=cte_aliases
            )


def apply_user_query_limits(con: Any, *, memory_limit: str = "2GB", timeout_seconds: int = 30) -> None:
    try:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    except Exception as exc:
        logger.warning("[sql_validator] failed to apply memory_limit=%s: %s", memory_limit, exc)
    try:
        con.execute(f"SET statement_timeout = '{timeout_seconds * 1000}ms'")
    except Exception as exc:
        logger.debug("[sql_validator] statement_timeout not supported: %s", exc)


# ── Pure-Python SQL tokenizer & AST builder ──────────────────────────────────

_NON_SELECT_STATEMENTS = frozenset(
    {
        "ATTACH",
        "DETACH",
        "INSTALL",
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
        "GRANT",
        "REVOKE",
        "TRUNCATE",
        "USE",
        "CHECKPOINT",
        "MERGE",
        "REPLACE",
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
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "WINDOW",
        "QUALIFY",
        "SETTINGS",
        "FORMAT",
        "PREWHERE",
        "SAMPLE",
    }
)
_JOIN_MODIFIERS = frozenset(
    {"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL", "GLOBAL", "ANY", "ALL", "ASOF"}
)
_NON_FUNCTION_KEYWORDS = frozenset(
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
        "cast",
        "extract",
        "position",
        "substring",
        "trim",
        "overlay",
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


@dataclass(slots=True)
class _Tok:
    kind: str  # "WORD" | "STRING" | "QIDENT" | "LPAREN" | "RPAREN" | "COMMA" | "DOT" | "SEMI" | "OTHER"
    val: str


def _tokenize_sql(sql: str) -> tuple[list[_Tok] | None, str | None]:
    tokens: list[_Tok] = []
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            nl = sql.find("\n", i + 2)
            i = n if nl == -1 else nl + 1
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
            continue
        if ch == "'":
            j = i + 1
            buf: list[str] = []
            closed = False
            while j < n:
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
            tokens.append(_Tok("STRING", "".join(buf)))
            i = j
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
            continue
        if ch == "(":
            tokens.append(_Tok("LPAREN", "("))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Tok("RPAREN", ")"))
            i += 1
            continue
        if ch == ",":
            tokens.append(_Tok("COMMA", ","))
            i += 1
            continue
        if ch == ".":
            tokens.append(_Tok("DOT", "."))
            i += 1
            continue
        if ch == ";":
            tokens.append(_Tok("SEMI", ";"))
            i += 1
            continue
        if ch.isalpha() or ch == "_":
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] in ("_", "$")):
                j += 1
            tokens.append(_Tok("WORD", sql[i:j]))
            i = j
            continue
        j = i + 1
        while (
            j < n
            and not sql[j].isspace()
            and sql[j] not in ("'", '"', "(", ")", ",", ".", ";", "-", "/")
            and not (sql[j].isalpha() or sql[j] == "_")
        ):
            j += 1
        tokens.append(_Tok("OTHER", sql[i:j]))
        i = j
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


def _parse_query_tokens(tokens: list[_Tok]) -> dict[str, Any]:
    tokens = _strip_outer_parens(tokens)
    node: dict[str, Any] = {"type": "SELECT_NODE", "children": []}
    if not tokens:
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
            cte_body = _parse_query_tokens(tokens[i + 1 : rp])
            if is_recursive:
                cte_body["type"] = "RECURSIVE_CTE_NODE"
            cte_entries.append({"key": cte_name, "value": {"query": {"node": cte_body}}})
            i = rp + 1
            if i < n and tokens[i].kind == "COMMA":
                i += 1
                continue
            break
        node["cte_map"] = {"map": cte_entries}

    _scan_body_tokens(tokens[i:], node)
    return node


def _scan_expr_tokens(tokens: list[_Tok], node: dict[str, Any]) -> None:
    i = 0
    n = len(tokens)
    while i < n:
        tok = tokens[i]
        if tok.kind in ("WORD", "QIDENT") and i + 1 < n and tokens[i + 1].kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i + 1)
            if rp != -1:
                if tok.kind == "QIDENT" or tok.val.lower() not in _NON_FUNCTION_KEYWORDS:
                    node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                _scan_expr_tokens(tokens[i + 2 : rp], node)
                i = rp + 1
                continue
        elif tok.kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i)
            if rp != -1:
                inner = _strip_outer_parens(tokens[i + 1 : rp])
                if inner and inner[0].kind == "WORD" and inner[0].val.upper() in ("SELECT", "WITH"):
                    node["children"].append(_parse_query_tokens(inner))
                else:
                    _scan_expr_tokens(inner, node)
                i = rp + 1
                continue
        i += 1


def _scan_body_tokens(tokens: list[_Tok], node: dict[str, Any]) -> None:
    i = 0
    n = len(tokens)
    in_from = False
    expect_table = False

    while i < n:
        tok = tokens[i]
        if tok.kind == "WORD":
            wu = tok.val.upper()
            if wu in ("UNION", "INTERSECT", "EXCEPT"):
                node["type"] = "SET_OPERATION_NODE"
                in_from = False
                expect_table = False
                i += 1
                continue
            if wu == "FROM":
                in_from = True
                expect_table = True
                i += 1
                continue
            if wu in _JOIN_MODIFIERS:
                if wu == "JOIN":
                    in_from = True
                    expect_table = True
                i += 1
                continue
            if wu in _CLAUSE_STOP_KEYWORDS:
                in_from = False
                expect_table = False
                i += 1
                continue
            if in_from and wu in ("ON", "USING"):
                expect_table = False
                i += 1
                continue

        if in_from and tok.kind == "COMMA":
            expect_table = True
            i += 1
            continue

        if in_from and expect_table:
            if tok.kind == "WORD" and tok.val.upper() == "LATERAL":
                i += 1
                continue
            if tok.kind == "LPAREN":
                rp = _find_matching_rparen(tokens, i)
                if rp != -1:
                    inner = _strip_outer_parens(tokens[i + 1 : rp])
                    if inner and inner[0].kind == "WORD" and inner[0].val.upper() in ("SELECT", "WITH"):
                        node["children"].append(_parse_query_tokens(inner))
                    else:
                        _scan_body_tokens(inner, node)
                    expect_table = False
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
                            and tokens[i].val.upper() not in (_CLAUSE_STOP_KEYWORDS | _JOIN_MODIFIERS | {"ON", "USING"})
                        ):
                            i += 1
                            if i < n and tokens[i].kind == "LPAREN":
                                rp_alias = _find_matching_rparen(tokens, i)
                                if rp_alias != -1:
                                    i = rp_alias + 1
                        expect_table = False
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
                    i = j
                    continue

        # Scalar expression / subquery outside table-reference position
        if tok.kind in ("WORD", "QIDENT") and i + 1 < n and tokens[i + 1].kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i + 1)
            if rp != -1:
                if tok.kind == "QIDENT" or tok.val.lower() not in _NON_FUNCTION_KEYWORDS:
                    node["children"].append({"class": "FUNCTION", "function_name": tok.val})
                _scan_expr_tokens(tokens[i + 2 : rp], node)
                i = rp + 1
                continue
        if tok.kind == "LPAREN":
            rp = _find_matching_rparen(tokens, i)
            if rp != -1:
                inner = _strip_outer_parens(tokens[i + 1 : rp])
                if inner and inner[0].kind == "WORD" and inner[0].val.upper() in ("SELECT", "WITH"):
                    node["children"].append(_parse_query_tokens(inner))
                else:
                    _scan_expr_tokens(inner, node)
                i = rp + 1
                continue

        i += 1


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
    if first_upper not in ("SELECT", "WITH", "VALUES"):
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
    "apply_user_query_limits",
    "escape_sql_literal",
    "is_safe_ident",
    "is_simple_select_statement",
    "require_safe_ident",
    "validate_user_sql",
]
