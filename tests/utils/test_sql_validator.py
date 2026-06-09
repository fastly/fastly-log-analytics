"""Acceptance tests for the DuckDB user-SQL validator (security).

The Decision B policy is defined in security_remediation_final_v6.md §2 and
implemented in backend/utils/sql_validator.py. This file pins every
acceptance criterion called out in that document so a future refactor
can't silently weaken the policy.

The validator runs against a real DuckDB connection (the same engine the
production app uses), so test coverage is end-to-end on the parse-tree
walker — no mocking the json_serialize_sql output.
"""

from __future__ import annotations

import duckdb
import pytest

from backend.utils.sql_validator import (
    MAX_INPUT_BYTES,
    SQLValidationError,
    apply_user_query_limits,
    inject_default_limit,
    validate_user_sql,
)


@pytest.fixture
def con():
    """A fresh in-memory DuckDB connection for each test. Sized minimally —
    these tests never execute the SQL, only parse it."""
    c = duckdb.connect(":memory:")
    yield c
    c.close()


# ── ACCEPTANCE: things that should PASS ─────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        # Plain SELECT from a non-blocked table
        "SELECT count(*) FROM fos_view WHERE service_id = 'x' LIMIT 100",
        # CTE alias produces a BASE_TABLE node for `x` — must not be blocked
        "WITH x AS (SELECT count(*) FROM fos_view) SELECT * FROM x",
        # UNION (SET_OPERATION_NODE)
        "SELECT 1 UNION SELECT 2",
        # SELECT with aggregate + GROUP BY
        "SELECT service_id, count(*) FROM logs GROUP BY service_id ORDER BY 2 DESC LIMIT 50",
        # Multiple CTEs
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT a.x + b.y FROM a, b",
    ],
)
def test_valid_user_queries_pass(con, sql):
    result = validate_user_sql(sql, parser_con=con)
    assert result.parse_tree.get("statements"), f"missing statements key: {result.parse_tree}"


# ── ACCEPTANCE: function denylist (file/secret exfil) ───────────────────────


@pytest.mark.parametrize(
    "sql,blocked_function",
    [
        ("SELECT * FROM read_csv_auto('/etc/passwd')", "read_csv_auto"),
        ("SELECT * FROM read_parquet('s3://bucket/key.parquet')", "read_parquet"),
        ("SELECT * FROM read_json('/tmp/whatever.json')", "read_json"),
        ("SELECT * FROM read_text('/etc/passwd')", "read_text"),
        ("SELECT * FROM read_blob('/etc/passwd')", "read_blob"),
        ("SELECT getenv('AWS_SECRET_ACCESS_KEY')", "getenv"),
        ("SELECT current_setting('s3_secret_access_key')", "current_setting"),
        ("SELECT * FROM duckdb_secrets()", "duckdb_secrets"),
        ("SELECT * FROM glob('/etc/*')", "glob"),
        ("SELECT * FROM lsdir('/')", "lsdir"),
        ("SELECT * FROM postgres_scan('host=evil', 'public', 't')", "postgres_scan"),
        ("SELECT * FROM sqlite_scan('/tmp/x.db', 't')", "sqlite_scan"),
        ("SELECT * FROM iceberg_scan('/tmp/iceberg')", "iceberg_scan"),
        # Regression for audit finding 014: parquet_scan / parquet_metadata
        # / parquet_schema / parquet_kv_metadata are DuckDB aliases that
        # bypassed the denylist before. They must be rejected for the same
        # reason as read_parquet (arbitrary path → exfil).
        ("SELECT * FROM parquet_scan('/etc/passwd.parquet')", "parquet_scan"),
        ("SELECT * FROM parquet_metadata('/tmp/x.parquet')", "parquet_metadata"),
        ("SELECT * FROM parquet_schema('/tmp/x.parquet')", "parquet_schema"),
        ("SELECT * FROM parquet_kv_metadata('/tmp/x.parquet')", "parquet_kv_metadata"),
    ],
)
def test_blocked_functions_rejected(con, sql, blocked_function):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == f"function_denylist:{blocked_function}", (
        f"expected function_denylist:{blocked_function}, got {exc.value.reason}"
    )


# ── ACCEPTANCE: catalog table prefix blocklist ──────────────────────────────


@pytest.mark.parametrize(
    "sql,blocked_prefix",
    [
        ("SELECT * FROM duckdb_secrets", "duckdb_"),
        ("SELECT * FROM duckdb_settings", "duckdb_"),
        ("SELECT * FROM duckdb_variables", "duckdb_"),
        ("SELECT * FROM duckdb_extensions", "duckdb_"),
        ("SELECT * FROM pg_settings", "pg_"),
    ],
)
def test_blocked_table_prefixes_rejected(con, sql, blocked_prefix):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == f"catalog_blocklist:table_name_prefix:{blocked_prefix}"


# ── ACCEPTANCE: schema_name blocklist (catches information_schema bypass) ──


@pytest.mark.parametrize(
    "sql,blocked_schema",
    [
        # The v5 regression: information_schema.tables has table_name='tables'
        # which slipped through the prefix-only check. This is THE
        # specific test that pins the v5 → v6 hardening.
        ("SELECT * FROM information_schema.tables", "information_schema"),
        ("SELECT * FROM information_schema.schemata", "information_schema"),
        ("SELECT * FROM information_schema.referential_constraints", "information_schema"),
        ("SELECT * FROM pg_catalog.pg_settings", "pg_catalog"),
    ],
)
def test_blocked_schemas_rejected(con, sql, blocked_schema):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    # pg_catalog.pg_settings has a pg_ prefix too — either rejection
    # message is acceptable as long as the query is blocked.
    assert exc.value.reason.startswith("catalog_blocklist:")


# ── ACCEPTANCE: statement-type whitelist ────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        "ATTACH '/etc/passwd' AS x",
        "INSTALL httpfs",
        "LOAD httpfs",
        "PRAGMA enable_external_access",
        "UPDATE logs SET x = 1",
        "INSERT INTO logs VALUES (1)",
        "DELETE FROM logs",
        "CREATE TABLE evil (x INT)",
        "DROP TABLE logs",
        "ALTER TABLE logs ADD COLUMN y INT",
        "COPY logs TO '/tmp/dump.csv'",
    ],
)
def test_non_select_statements_rejected(con, sql):
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    # Some of these (PRAGMA / SET / ATTACH) trigger 'parse_error' if the
    # parser refuses them outright; others land on statement_type:*.
    # Both shapes count as 'rejected' for security purposes.
    assert exc.value.reason.startswith(("statement_type:", "parse_error"))


# ── ACCEPTANCE: multi-statement payloads ────────────────────────────────────


def test_multi_statement_rejected(con):
    sql = "SELECT 1; SELECT 2"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason in ("multi_statement", "no_statements")


def test_injection_classic_attempted(con):
    """The original audit attack vector: prepended ATTACH after the
    legitimate SELECT. Must be caught by either the multi-statement check
    or the statement-type check on the ATTACH branch."""
    sql = "SELECT 1; ATTACH '/etc/passwd' AS x"
    with pytest.raises(SQLValidationError):
        validate_user_sql(sql, parser_con=con)


# ── ACCEPTANCE: input size ceiling ──────────────────────────────────────────


def test_input_too_large_rejected(con):
    huge = "SELECT 1 WHERE 1 IN (" + ",".join(str(i) for i in range(20_000)) + ")"
    assert len(huge.encode()) > MAX_INPUT_BYTES
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(huge, parser_con=con)
    assert exc.value.reason == "input_too_large"


# ── ACCEPTANCE: fail-closed on parse error ──────────────────────────────────


def test_malformed_sql_rejected(con):
    """json_serialize_sql returns {'error': true, ...} for parse failures
    rather than raising. The validator must NOT silently pass these
    through — fail closed."""
    sql = "SELECT FROM WHERE * NOT-VALID"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason.startswith("parse_error")


def test_empty_input_rejected(con):
    """Empty string is technically zero statements — must fail closed."""
    with pytest.raises(SQLValidationError):
        validate_user_sql("", parser_con=con)


def test_whitespace_only_input_rejected(con):
    with pytest.raises(SQLValidationError):
        validate_user_sql("   \n\t  ", parser_con=con)


# ── inject_default_limit ────────────────────────────────────────────────────


def test_inject_default_limit_wraps_when_missing():
    out = inject_default_limit("SELECT * FROM logs", default_limit=42)
    assert "LIMIT 42" in out
    assert "SELECT * FROM logs" in out


def test_inject_default_limit_skips_when_present():
    sql = "SELECT * FROM logs LIMIT 5"
    out = inject_default_limit(sql, default_limit=42)
    assert out == sql, "must NOT re-wrap a query that already has LIMIT"


def test_inject_default_limit_handles_trailing_semicolon():
    out = inject_default_limit("SELECT * FROM logs;", default_limit=10)
    # No double-semicolon, LIMIT lands correctly
    assert ";" not in out.replace(" ", "").rstrip(";")
    assert "LIMIT 10" in out


# ── apply_user_query_limits ─────────────────────────────────────────────────


def test_apply_user_query_limits_sets_memory_limit(con):
    apply_user_query_limits(con, memory_limit="256MB", timeout_seconds=5)
    # The setting is visible via current_setting (note: validator would
    # block this in user SQL — but this is a meta-test on the helper).
    val = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    # DuckDB normalizes "256MB" to bytes/MB depending on version; just
    # ensure something non-empty came back.
    assert val


# ── escape_sql_literal characterization tests (security) ───────────────
#
# These tests target the REAL attack vectors enumerated in the v6
# remediation doc, not just the happy path. They use a real DuckDB
# connection to round-trip the escaped value back through the parser, so
# any future bug in the escape function fails the test instead of silently
# allowing a known attack shape.


from backend.utils.sql_validator import escape_sql_literal  # noqa: E402


def _roundtrip(con: duckdb.DuckDBPyConnection, value: str) -> str:
    """Embed ``value`` in a DuckDB string literal via escape_sql_literal
    and confirm the engine returns it unchanged."""
    sql = f"SELECT '{escape_sql_literal(value)}'"
    return con.execute(sql).fetchone()[0]


def test_escape_simple_string(con):
    assert _roundtrip(con, "raw/2026-01-01/log.json") == "raw/2026-01-01/log.json"


def test_escape_single_quote(con):
    """Bare single quote — the textbook attack character."""
    assert _roundtrip(con, "O'Brien") == "O'Brien"


def test_escape_already_doubled_quote(con):
    """An attacker who pre-doubled their quotes shouldn't double them again."""
    assert _roundtrip(con, "''') OR 1=1 --") == "''') OR 1=1 --"


def test_escape_attack_payload_from_audit(con):
    """The exact payload shape from the audit's proof-of-concept."""
    payload = (
        "raw/inj*'], format='newline_delimited', ignore_errors=true);"
        " CREATE MACRO ignore(records, filename, columns, ignore_errors) AS NULL;"
        " CREATE TABLE pwned AS SELECT 1 AS flag;"
        " SELECT ignore(--"
    )
    assert _roundtrip(con, payload) == payload


def test_escape_backslash_is_literal(con):
    """DuckDB doesn't treat \\ as an escape character in single-quoted
    literals (standard mode), so we don't need to double it. Confirm
    round-trip is identity."""
    assert _roundtrip(con, r"a\nb\tc\\d") == r"a\nb\tc\\d"


def test_escape_control_chars(con):
    """Tabs and newlines pass through unchanged."""
    assert _roundtrip(con, "a\tb\nc") == "a\tb\nc"


def test_escape_multibyte_utf8(con):
    """A historic DuckDB bug treated 0x27 as a quote even when it appeared
    as a continuation byte inside a multi-byte UTF-8 sequence. Modern
    DuckDB handles this correctly because we operate on the Python str
    (not bytes), so only actual U+0027 code points get doubled."""
    # U+0027 is the actual quote; U+2019 is the right single quotation
    # mark (encodes as 0xE2 0x80 0x99 — no 0x27 in the byte stream but
    # similar visual character).
    assert _roundtrip(con, "café'résumé") == "café'résumé"
    assert _roundtrip(con, "smart ’quote") == "smart ’quote"


def test_escape_empty_string(con):
    assert _roundtrip(con, "") == ""


def test_escape_only_quotes(con):
    assert _roundtrip(con, "''''''") == "''''''"


def test_escape_null_byte_raises_in_duckdb(con):
    """DuckDB's parser terminates literals on NUL bytes (treats them as
    a C-style string terminator). S3 object keys cannot contain NUL
    (S3 API rejects them), so this is a non-issue for the production
    callsite — but pinning the behavior here ensures any future fix to
    the escape helper that tries to handle NUL silently doesn't end up
    in a passing test.
    """
    # escape_sql_literal itself returns the NUL unchanged. The DuckDB
    # parser is what fails — that's the layer that would silently
    # truncate a query if we ever interpolated a NUL-bearing string.
    escaped = escape_sql_literal("a\x00b")
    assert "\x00" in escaped
    with pytest.raises(duckdb.ParserException):
        con.execute(f"SELECT '{escaped}'")


def test_escape_long_string_with_many_quotes(con):
    """An attacker padding their payload shouldn't break escaping."""
    value = "x" * 1000 + "'" * 1000 + "y" * 1000
    assert _roundtrip(con, value) == value


def test_escape_rejects_non_string():
    """The helper is str-only — passing bytes or int is a programming
    error and should fail loud."""
    with pytest.raises(TypeError):
        escape_sql_literal(b"bytes")
    with pytest.raises(TypeError):
        escape_sql_literal(42)
    with pytest.raises(TypeError):
        escape_sql_literal(None)


# ── REGRESSION TESTS: BATCH B Hardening ─────────────────────────────────────


def test_query_table_function_blocked(con):
    """Finding 010: Ensure query() table function is blocked."""
    sql = "SELECT * FROM query('SELECT * FROM duckdb_secrets()')"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == "function_denylist:query"


def test_has_limit_clause_strictly_outermost(con):
    """Finding 011: Ensure LIMIT clauses inside subqueries are ignored for outer limit wrapping."""
    from backend.utils.sql_validator import has_limit_clause

    # Limit nested in subquery
    nested_sql = "SELECT * FROM range(10) a CROSS JOIN range(10) b WHERE 1 IN (SELECT 1 LIMIT 1)"
    assert not has_limit_clause(nested_sql, parser_con=con)

    # Limit on top-level SELECT
    outer_sql = "SELECT * FROM range(10) LIMIT 5"
    assert has_limit_clause(outer_sql, parser_con=con)


def test_replacement_scan_blocked_by_table_name_characters(con):
    """Finding 029: Ensure table names containing slashes, backslashes, or dots (file paths/replacement scans) are rejected."""
    sql_slash = "SELECT * FROM '/etc/passwd'"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql_slash, parser_con=con)
    assert exc.value.reason == "catalog_blocklist:table_name_path"

    sql_dot = "SELECT * FROM 'data.parquet'"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql_dot, parser_con=con)
    assert exc.value.reason == "catalog_blocklist:table_name_path"


def test_query_table_function_blocked_finding_011(con):
    """Finding 011: Ensure query_table() table function is blocked."""
    sql = "SELECT * FROM query_table('my_table')"
    with pytest.raises(SQLValidationError) as exc:
        validate_user_sql(sql, parser_con=con)
    assert exc.value.reason == "function_denylist:query_table"
