"""The cid salt must never appear in any recorded or returned SQL text.

A mask_ips analyst's ``/api/query`` rebinds the log table to a temp view that replaces ``cid`` with
``sha256(CONCAT(<salt>, cid))``. The salt is a secret: with it, a pseudonymous client id can be
dictionary-reversed. Embedding it as a SQL literal put it in every channel that records statement
text (the live query registry, persisted ``slow_queries``, DuckDB error excerpts). The salt now travels
as a bound parameter, and the hashes must stay identical.
"""

from __future__ import annotations

import hashlib
import json
from unittest.mock import patch

import duckdb
import pytest

from backend.core.query_instrumentation import InstrumentedDuckDBConnection
from backend.core.query_registry import query_registry
from backend.repositories import query as q

SALT = "SECRETSALT-9f3a1c7e5b"
SVC = "svc"
WINDOW = ("2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z")
SRC = {"name": SVC, "service_id": SVC}


@pytest.fixture
def raw_con():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR, status INT)")
    con.execute(
        "INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4', 200), (now(), 'client-b', '5.6.7.8', 500),"
        " (now(), '', '9.9.9.9', 200), (now(), NULL, '8.8.8.8', 200)"
    )
    yield con
    con.close()


def _analyst_query(con, sql: str, *, want_explain: bool = False) -> dict:
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        return q.execute_query(
            con,
            SRC,
            sql,
            100,
            want_explain,
            service_id=SVC,
            mask_ips=True,
            time_filter=WINDOW,
        )


def _expected_hash(cid: str) -> str:
    return hashlib.sha256((SALT + cid).encode()).hexdigest()


def test_hashes_are_identical_to_sha256_of_salt_plus_cid(raw_con):
    res = _analyst_query(raw_con, "SELECT cid FROM logs")
    assert sorted((r["cid"] or "") for r in res["data"]) == sorted(
        ["", "", _expected_hash("client-a"), _expected_hash("client-b")]
    )
    assert sum(r["cid"] is None for r in res["data"]) == 1, "NULL cid stays NULL (not hashed)"


def test_hashes_match_the_previous_inline_literal_expression(raw_con):
    legacy = raw_con.execute(
        f"SELECT sha256(CONCAT('{SALT}', cid)) FROM logs_svc WHERE status = 500"  # old shape, for parity only
    ).fetchone()[0]
    res = _analyst_query(raw_con, "SELECT cid FROM logs WHERE status = 500")
    assert res["data"] == [{"cid": legacy}]


def test_salt_is_absent_from_the_live_registry_and_history(raw_con):
    con = InstrumentedDuckDBConnection(raw_con, service_id=SVC)
    _analyst_query(con, "SELECT cid, count(*) AS n FROM logs GROUP BY cid")

    snap = query_registry.snapshot(since_seq=0, full_sql=True, include_completed=True, all_active=True)
    texts = json.dumps(snap, default=str)
    assert "_analyst_window_logs" in texts, "sanity: the view DDL really went through the instrumented connection"
    assert SALT not in texts


def test_salt_is_absent_from_persisted_slow_queries(raw_con):
    con = InstrumentedDuckDBConnection(raw_con, service_id=SVC)
    persisted: list[tuple] = []

    def capture(service_id, row):
        persisted.append((service_id, row))

    with (
        patch("backend.core.query_registry._SLOW_QUERY_PERSIST_THRESHOLD_MS", 0.0),
        patch("backend.core.query_registry._SLOW_QUERY_PERSIST_DISABLED", False),
        patch("backend.core.metadata.insert_slow_query", side_effect=capture),
    ):
        _analyst_query(con, "SELECT cid FROM logs")

    assert persisted, "sanity: slow-query persistence ran for the rebind DDL"
    assert SALT not in json.dumps(persisted, default=str)


def test_salt_is_absent_from_every_field_returned_to_the_analyst(raw_con):
    con = InstrumentedDuckDBConnection(raw_con, service_id=SVC)
    res = _analyst_query(con, "SELECT cid FROM logs", want_explain=True)
    assert SALT not in json.dumps(res, default=str)


def test_salt_is_absent_from_the_error_when_the_rebind_ddl_itself_fails(raw_con):
    # A column the DDL references but the table lacks => the DDL (not the user query) raises, and DuckDB
    # echoes an excerpt of the failing statement in its message.
    raw_con.execute("CREATE TABLE logs_nots (cid VARCHAR, ip VARCHAR)")
    src = {"name": "nots", "service_id": SVC}
    con = InstrumentedDuckDBConnection(raw_con, service_id=SVC)
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(duckdb.Error) as exc:
            q.execute_query(
                con, src, "SELECT cid FROM logs", 100, False, service_id=SVC, mask_ips=True, time_filter=WINDOW
            )
    assert SALT not in str(exc.value)


def test_salt_holder_is_dropped_with_the_view_and_unreachable_by_analyst_sql(raw_con):
    _analyst_query(raw_con, "SELECT cid FROM logs")
    leftovers = raw_con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE temporary UNION ALL "
        "SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal"
    ).fetchall()
    assert leftovers == [], f"temp objects leaked onto the pooled connection: {leftovers}"

    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(PermissionError):
            q.execute_query(
                raw_con,
                SRC,
                f"SELECT * FROM {q._ANALYST_CID_SALT_TABLE}",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )


def test_non_cid_columns_still_redact_to_the_literal_marker(raw_con):
    res = _analyst_query(raw_con, "SELECT ip FROM logs ORDER BY ip")
    assert {r["ip"] for r in res["data"]} == {"[redacted]"}


def test_read_only_database_connections_can_still_hold_the_salt(tmp_path):
    path = str(tmp_path / "ro.duckdb")
    seed = duckdb.connect(path)
    seed.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR, status INT)")
    seed.execute("INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4', 200)")
    seed.close()

    ro = duckdb.connect(path, read_only=True)
    try:
        res = _analyst_query(ro, "SELECT cid FROM logs")
    finally:
        ro.close()
    assert res["data"] == [{"cid": _expected_hash("client-a")}]


def test_invalid_window_bounds_fail_before_any_salt_is_bound(raw_con):
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(ValueError, match="invalid analyst window bounds"):
            q.execute_query(
                raw_con,
                SRC,
                "SELECT cid FROM logs",
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=("not-a-date", "also-not"),
            )
    assert raw_con.execute("SELECT count(*) FROM duckdb_tables() WHERE temporary").fetchone()[0] == 0


def test_salt_table_is_dropped_even_when_dropping_the_view_fails(raw_con):
    class _FailViewDrop:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            if sql.startswith("DROP VIEW"):
                raise RuntimeError("interrupted")
            return self._inner.execute(sql, *a, **k)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    _analyst_query(_FailViewDrop(raw_con), "SELECT cid FROM logs")
    assert (
        raw_con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = '_analyst_cid_salt'").fetchone()[0]
        == 0
    )
