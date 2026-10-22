"""Tests for the metadata query profiler."""

from __future__ import annotations

import pytest

from backend.utils import sqlite_profiler


@pytest.fixture(autouse=True)
def _clear_buffer():
    """Every test starts with an empty ring buffer."""
    sqlite_profiler.clear()
    yield
    sqlite_profiler.clear()


# ── Statement recording ──────────────────────────────────────────────────────


def test_execute_captures_a_statement():
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.5, 0, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["sql"] == "CREATE TABLE t (x INTEGER)"
    assert snap["queries"][0]["op"] == "execute"
    assert snap["queries"][0]["time_ms"] >= 0
    assert snap["queries"][0]["seq"] >= 1


def test_executemany_captures_one_entry_with_seq_length():
    sqlite_profiler._record("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)], 1.2, 3, "executemany")
    snap = sqlite_profiler.get_recent()
    assert len(snap["queries"]) == 1
    assert snap["queries"][0]["op"] == "executemany"
    assert snap["queries"][0]["params_kind"] == "seq[3]"


def test_params_are_not_captured_as_values():
    """PII safety: params must never appear in the captured SQL or anywhere
    in the entry body. Only shape metadata."""
    secret = "user-pii-do-not-capture@example.com"
    sqlite_profiler._record("INSERT INTO t VALUES (?)", (secret,), 0.4, 1, "execute")
    snap = sqlite_profiler.get_recent()
    entry = snap["queries"][0]
    assert secret not in entry["sql"]
    assert all(secret not in str(v) for v in entry.values())


# ── Ring buffer mechanics ────────────────────────────────────────────────────


def test_ring_buffer_caps_at_max(monkeypatch):
    """Push past cap, verify buffer_size never exceeds cap and dropped grows."""
    monkeypatch.setattr(sqlite_profiler, "_buffer", type(sqlite_profiler._buffer)(maxlen=10))
    monkeypatch.setattr(sqlite_profiler, "_dropped", 0)
    for _ in range(25):
        sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 10
    assert snap["dropped"] >= 15


def test_since_seq_filters():
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    midpoint = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler._record("SELECT 3", None, 0.1, 1, "execute")
    sqlite_profiler._record("SELECT 4", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent(since_seq=midpoint)
    sqls = [q["sql"] for q in snap["queries"]]
    assert "SELECT 3" in sqls
    assert "SELECT 4" in sqls
    assert "SELECT 1" not in sqls
    assert "SELECT 2" not in sqls


def test_limit_returns_most_recent():
    for i in range(20):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent(limit=5)
    assert len(snap["queries"]) == 5
    sqls = [q["sql"] for q in snap["queries"]]
    assert sqls == [f"SELECT {i}" for i in range(15, 20)]


def test_clear_resets_buffer_but_not_seq():
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    last_seq_before = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler.clear()
    snap_after_clear = sqlite_profiler.get_recent()
    assert snap_after_clear["buffer_size"] == 0
    assert snap_after_clear["last_seq"] == 0
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["last_seq"] > last_seq_before


# ── End-to-end via get_con() ─────────────────────────────────────────────────


def test_get_con_returns_instrumented_connection():
    """metadata_db.get_con() must hand out PgConnectionWrapper under Postgres."""
    from backend.core.metadata import base, pg_connection

    con = base.get_con("test-svc")
    assert isinstance(con, pg_connection.PgConnectionWrapper)


def test_metadata_db_traffic_appears_in_buffer():
    """Sanity end-to-end: a real metadata_db call populates the ring buffer."""
    from backend.core.metadata import base

    con = base.get_con("test-svc-traffic")
    con.execute("SELECT 1")
    snap = sqlite_profiler.get_recent()
    sqls = [q["sql"] for q in snap["queries"]]
    assert any("SELECT 1" in s for s in sqls)


# ── Helper-function direct tests (cover the small surface) ──────────────────


def test_summarize_sql_truncates_long_strings():
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(1000))
    out = sqlite_profiler._summarize_sql(long_sql)
    assert "chars]" in out
    assert len(out) < len(long_sql)


def test_summarize_sql_coerces_non_strings():
    """Some callers pass bytes / int; the profiler must not raise."""
    out = sqlite_profiler._summarize_sql(b"SELECT 1")
    assert isinstance(out, str)
    assert "SELECT 1" in out
    assert sqlite_profiler._summarize_sql(42) == "42"


def test_describe_params_covers_each_shape():
    assert sqlite_profiler._describe_params(None) == "none"
    assert sqlite_profiler._describe_params([1, 2, 3]) == "seq[3]"
    assert sqlite_profiler._describe_params((1,)) == "seq[1]"
    assert sqlite_profiler._describe_params({"a": 1, "b": 2}) == "map[2]"
    assert sqlite_profiler._describe_params(42) == "int"


def test_record_swallows_internal_exceptions(monkeypatch):
    """The profiler's hard contract: any internal failure must never
    reach the caller's SQL path."""
    from unittest.mock import MagicMock

    bad = MagicMock()
    bad.append.side_effect = RuntimeError("buffer borked")
    monkeypatch.setattr(sqlite_profiler, "_buffer", bad)
    sqlite_profiler._record("SELECT 1", None, 0.5, 1, "execute")


def test_live_register_returns_sentinel_on_registry_failure(monkeypatch):
    """When the registry import / call raises, _live_register returns -1
    (a sentinel that _live_deregister treats as a no-op)."""
    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "register",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("registry down")),
    )
    qid = sqlite_profiler._live_register("SQLite", "SELECT 1", con=None)
    assert qid == -1


def test_live_deregister_is_noop_for_negative_qid():
    sqlite_profiler._live_deregister(-1, error=None)


def test_live_deregister_swallows_registry_errors(monkeypatch):
    from unittest.mock import MagicMock

    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "deregister",
        MagicMock(side_effect=RuntimeError("dereg failed")),
    )
    sqlite_profiler._live_deregister(42, error=None)


# ── Per-request collector (Debug Panel "This page" scoping) ──────────────────


@pytest.fixture()
def _request_collector():
    """Isolate the telemetry request-scoped SQLite collector per test —
    start_call_tracking() sets a ContextVar that would otherwise leak
    into subsequent tests running in the same context."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    token = _SQLITE_QUERIES.set(None)
    yield
    _SQLITE_QUERIES.reset(token)


def test_statement_lands_in_request_collector_when_tracking_active(_request_collector):
    """The page-scoped Debug Panel view: a statement executed while a
    request is being tracked must land in BOTH the process-global ring
    buffer AND the request's contextvar collector."""
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.2, 0, "execute")

    entries = telemetry.get_sqlite_queries()
    assert len(entries) == 1
    assert entries[0]["sql"] == "CREATE TABLE t (x INTEGER)"
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["seq"] == entries[0]["seq"]


def test_statement_skips_request_collector_outside_tracking(_request_collector):
    """Cron/startup statements (no start_call_tracking) must NOT create or
    populate a request collector — only the ring buffer sees them."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")

    assert _SQLITE_QUERIES.get() is None
    assert sqlite_profiler.get_recent()["buffer_size"] == 1


def test_request_collector_failure_does_not_break_sql_path(_request_collector, monkeypatch):
    """The profiler contract: observability failures never propagate into
    the calling SQL path. A broken record_sqlite_query must not raise."""
    from backend.utils import telemetry

    monkeypatch.setattr(
        telemetry,
        "record_sqlite_query",
        lambda entry: (_ for _ in ()).throw(RuntimeError("collector broken")),
    )
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
