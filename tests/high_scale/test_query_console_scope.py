"""The SQL console pins fact tables with ClickHouse's ``additional_table_filters`` instead of rewriting user SQL."""

from __future__ import annotations

from types import SimpleNamespace

from backend.high_scale.query import _scope_settings, query_endpoint
from backend.models.query import QueryRequest

START = "2026-09-15T00:00:00+00:00"
END = "2026-09-15T01:00:00+00:00"


class _Recorder:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.sql.append(sql)
        return []


def _run(sql: str) -> str:
    client = _Recorder()
    query_endpoint(
        SimpleNamespace(service_id="svc", client=client),  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=10),
        START,
        END,
        False,
    )
    return client.sql[-1]


def test_every_referenced_fact_table_is_pinned_to_the_service_the_fence_and_the_window() -> None:
    sql = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")

    assert sql.rstrip().endswith("}")
    settings = sql[sql.index(" SETTINGS ") :]
    assert settings.startswith(" SETTINGS additional_table_filters = {'request_facts': ")
    assert "service_id = \\'svc\\'" in settings
    assert "high_scale_batch_publications FINAL" in settings and "domain = \\'request\\'" in settings
    assert "publication_state = \\'visible\\'" in settings
    assert "toDateTime64(\\'2026-09-15 00:00:00.000000\\', 6, \\'UTC\\')" in settings
    assert "toDateTime64(\\'2026-09-15 01:00:00.000000\\', 6, \\'UTC\\')" in settings


def test_the_users_own_where_clause_is_left_untouched_not_spliced_with_the_scope() -> None:
    sql = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")

    user_part = sql[: sql.index(" SETTINGS ")]
    assert "WHERE url = '/x' OR 1 = 1" in user_part
    assert "service_id" not in user_part


def test_each_rum_table_gets_its_own_domain_fence() -> None:
    settings = _scope_settings("svc", START, END, ["rum_vitals_facts", "rum_error_facts"])

    assert "'rum_vitals_facts': " in settings and "domain = \\'rum_vitals\\'" in settings
    assert "'rum_error_facts': " in settings and "domain = \\'rum_errors\\'" in settings


def test_a_hostile_service_id_cannot_break_out_of_the_filter_literal() -> None:
    settings = _scope_settings("x' OR 1=1 --", None, None, ["request_facts"])

    # Escaped once for the inner filter literal and again for the outer map-value literal.
    assert "x" + "\\" * 3 + "' OR 1=1 --" in settings
    assert "x' OR" not in settings
