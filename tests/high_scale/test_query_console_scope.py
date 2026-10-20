"""The SQL console never rewrites a user's WHERE: scope travels as ClickHouse settings, results are capped outside the SQL."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from backend.high_scale import query as console
from backend.high_scale.query import _reject_unsafe_functions, _table_filters, query_endpoint
from backend.models.query import QueryRequest

pytestmark = pytest.mark.security_regression

START = "2026-09-15T00:00:00+00:00"
END = "2026-09-15T01:00:00+00:00"


class _Recorder:
    def __init__(self, probe_rows: int = 0) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []
        self._probe_rows = probe_rows

    def execute(self, sql: str, params: dict | None = None, *, settings: dict[str, str] | None = None) -> list[dict]:
        self.calls.append((sql, dict(settings or {})))
        if "system.one" in sql:
            return [{"n": self._probe_rows}]
        return []


def _run(sql: str, *, max_rows: int = 10, client: _Recorder | None = None) -> tuple[str, dict[str, str]]:
    client = client or _Recorder()
    query_endpoint(
        SimpleNamespace(service_id="svc", client=client),  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=max_rows),
        START,
        END,
        False,
    )
    return client.calls[-1]


def test_every_referenced_fact_table_is_pinned_by_a_setting_not_by_sql_text() -> None:
    sql, settings = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")

    assert "additional_table_filters" not in sql and "SETTINGS" not in sql
    filters = settings["additional_table_filters"]
    assert filters.startswith("{'request_facts': ")
    assert "service_id = \\'svc\\'" in filters
    assert "high_scale_batch_publications FINAL" in filters and "domain = \\'request\\'" in filters
    assert "publication_state = \\'visible\\'" in filters
    assert "toDateTime64(\\'2026-09-15 00:00:00.000000\\', 6, \\'UTC\\')" in filters
    assert "toDateTime64(\\'2026-09-15 01:00:00.000000\\', 6, \\'UTC\\')" in filters


def test_the_users_own_where_clause_is_left_untouched_not_spliced_with_the_scope() -> None:
    sql, _ = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")

    assert "WHERE url = '/x' OR 1 = 1" in sql
    assert "service_id" not in sql


@pytest.mark.parametrize("comment", ["-- swallow the rest", "--", "-- LIMIT 999999999 SETTINGS x = 1"])
def test_a_trailing_line_comment_cannot_swallow_the_row_cap(comment: str) -> None:
    sql, settings = _run(f"SELECT * FROM logs {comment}", max_rows=7)

    assert sql.endswith("\n) LIMIT 8"), "the cap must start on its own line, outside the user's comment"
    assert settings["max_result_rows"] == "8" and settings["result_overflow_mode"] == "break"
    assert "additional_table_filters" in settings


def test_an_explicit_user_limit_is_bounded_by_the_global_ceiling_not_trusted() -> None:
    sql, settings = _run("SELECT url FROM logs LIMIT 999999999", max_rows=5)

    assert sql.endswith(f"\n) LIMIT {console.MAX_QUERY_ROWS + 1}")
    assert settings["max_result_rows"] == str(console.MAX_QUERY_ROWS + 1)


def test_each_rum_table_gets_its_own_domain_fence() -> None:
    filters = _table_filters("svc", START, END, ["rum_vitals_facts", "rum_error_facts"])

    assert "'rum_vitals_facts': " in filters and "domain = \\'rum_vitals\\'" in filters
    assert "'rum_error_facts': " in filters and "domain = \\'rum_errors\\'" in filters


def test_a_hostile_service_id_cannot_break_out_of_the_filter_literal() -> None:
    filters = _table_filters("x' OR 1=1 --", None, None, ["request_facts"])

    # Escaped once for the inner filter literal and again for the outer map-value literal.
    assert "x" + "\\" * 3 + "' OR 1=1 --" in filters
    assert "x' OR" not in filters


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM remote('other-host', default.request_facts)",
        "SELECT * FROM url('http://169.254.169.254/latest/meta-data', 'CSV', 'x String')",
        "SELECT * FROM file('/etc/passwd', 'CSV', 'x String')",
        "SELECT * FROM numbers(1000000000)",
        "SELECT getSetting('max_memory_usage') FROM logs",
        "SELECT hostName(), version(), currentUser() FROM logs",
        "SELECT * FROM \"remote\"('h', db.t)",
        "SELECT hex(ip) FROM logs",
        "SELECT dictGet('d', 'a', 1) FROM logs",
    ],
)
def test_clickhouse_table_and_server_functions_are_rejected(sql: str) -> None:
    with pytest.raises(PermissionError):
        _reject_unsafe_functions(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) AS n, quantile(0.95)(elapsed) AS p95 FROM logs GROUP BY toStartOfMinute(timestamp)",
        "SELECT url FROM logs WHERE country IN ('US', 'CA') AND (status = '200' OR status = '404')",
        "SELECT row_number() OVER (PARTITION BY country ORDER BY n DESC) AS rk FROM (SELECT 1 AS n) AS t",
        "SELECT 'remote(' AS s, '-- not a comment' AS c FROM logs",
        "SELECT count(*) FROM logs -- remote('x')",
        "SELECT count(*) FROM logs /* url('x') */",
    ],
)
def test_ordinary_analytics_sql_is_not_rejected(sql: str) -> None:
    _reject_unsafe_functions(sql)


def test_the_console_refuses_to_serve_when_the_server_does_not_enforce_table_filters() -> None:
    ignoring = _Recorder(probe_rows=1)

    with pytest.raises(RuntimeError, match="not enforcing"):
        _run("SELECT count(*) FROM logs", client=ignoring)

    assert all("FROM (" not in sql for sql, _ in ignoring.calls), "no user query may run after a failed probe"


def test_a_passing_probe_is_remembered_per_client() -> None:
    client: Any = _Recorder()

    _run("SELECT 1 AS a FROM logs", client=client)
    _run("SELECT 2 AS a FROM logs", client=client)

    assert sum("system.one" in sql for sql, _ in client.calls) == 1


@pytest.mark.parametrize("decoy", ["-- LIMIT 1", "/* LIMIT 1 */", "'LIMIT'", "'it''s a LIMIT'"])
def test_the_word_limit_in_a_comment_or_literal_does_not_raise_the_users_row_cap(decoy: str) -> None:
    sql, settings = _run(
        f"SELECT {decoy if decoy.startswith(chr(39)) else '1'} AS a FROM logs {decoy if not decoy.startswith(chr(39)) else ''}",
        max_rows=5,
    )

    assert sql.endswith("\n) LIMIT 6")
    assert settings["max_result_rows"] == "6"
