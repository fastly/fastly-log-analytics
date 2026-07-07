"""Tests for the SQLite query profiler."""

from __future__ import annotations

import sqlite3

import pytest

from backend.utils import sqlite_profiler


@pytest.fixture(autouse=True)
def _clear_buffer():
    """Every test starts with an empty ring buffer."""
    sqlite_profiler.clear()
    yield
    sqlite_profiler.clear()


def _open_instrumented(path: str) -> sqlite3.Connection:
    return sqlite3.connect(path, factory=sqlite_profiler.InstrumentedConnection)


# ── InstrumentedConnection / InstrumentedCursor ──────────────────────────────


def test_execute_captures_a_statement(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("CREATE TABLE t (x INTEGER)")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["sql"] == "CREATE TABLE t (x INTEGER)"
    assert snap["queries"][0]["op"] == "execute"
    assert snap["queries"][0]["time_ms"] >= 0
    assert snap["queries"][0]["seq"] >= 1


def test_executemany_captures_one_entry_with_seq_length(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("CREATE TABLE t (x INTEGER)")
    sqlite_profiler.clear()
    con.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
    snap = sqlite_profiler.get_recent()
    assert len(snap["queries"]) == 1
    assert snap["queries"][0]["op"] == "executemany"
    assert snap["queries"][0]["params_kind"] == "seq[3]"


def test_cursor_execute_path_captured(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("CREATE TABLE t (x INTEGER)")
    sqlite_profiler.clear()
    cur = con.cursor()
    cur.execute("INSERT INTO t VALUES (1)")
    cur.execute("INSERT INTO t VALUES (2)")
    snap = sqlite_profiler.get_recent()
    assert len(snap["queries"]) == 2
    assert all(q["op"] == "execute" for q in snap["queries"])


def test_failing_statement_is_still_recorded(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    with pytest.raises(sqlite3.OperationalError):
        con.execute("SELECT * FROM nonexistent_table")
    snap = sqlite_profiler.get_recent()
    assert len(snap["queries"]) == 1
    assert "nonexistent_table" in snap["queries"][0]["sql"]


def test_params_are_not_captured_as_values(tmp_path):
    """PII safety: params must never appear in the captured SQL or anywhere
    in the entry body. Only shape metadata."""
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("CREATE TABLE t (x TEXT)")
    sqlite_profiler.clear()
    secret = "user-pii-do-not-capture@example.com"
    con.execute("INSERT INTO t VALUES (?)", (secret,))
    snap = sqlite_profiler.get_recent()
    entry = snap["queries"][0]
    assert secret not in entry["sql"]
    assert all(secret not in str(v) for v in entry.values())


# ── Ring buffer mechanics ────────────────────────────────────────────────────


def test_ring_buffer_caps_at_max(tmp_path, monkeypatch):
    """Push past cap, verify buffer_size never exceeds cap and dropped grows."""
    # Reduce cap to keep the test fast/deterministic.
    monkeypatch.setattr(sqlite_profiler, "_buffer", type(sqlite_profiler._buffer)(maxlen=10))
    monkeypatch.setattr(sqlite_profiler, "_dropped", 0)
    con = _open_instrumented(str(tmp_path / "t.db"))
    for _ in range(25):
        con.execute("SELECT 1")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 10
    # 25 issued, 10 retained, so >= 15 dropped (first call to CREATE/PRAGMA
    # may have happened depending on factory init; just enforce the floor).
    assert snap["dropped"] >= 15


def test_since_seq_filters(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("SELECT 1")
    con.execute("SELECT 2")
    midpoint = sqlite_profiler.get_recent()["last_seq"]
    con.execute("SELECT 3")
    con.execute("SELECT 4")
    snap = sqlite_profiler.get_recent(since_seq=midpoint)
    sqls = [q["sql"] for q in snap["queries"]]
    assert "SELECT 3" in sqls
    assert "SELECT 4" in sqls
    assert "SELECT 1" not in sqls
    assert "SELECT 2" not in sqls


def test_limit_returns_most_recent(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    for i in range(20):
        con.execute(f"SELECT {i}")
    snap = sqlite_profiler.get_recent(limit=5)
    assert len(snap["queries"]) == 5
    # Most recent statements — values 15..19.
    sqls = [q["sql"] for q in snap["queries"]]
    assert sqls == [f"SELECT {i}" for i in range(15, 20)]


def test_clear_resets_buffer_but_not_seq(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("SELECT 1")
    last_seq_before = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler.clear()
    snap_after_clear = sqlite_profiler.get_recent()
    assert snap_after_clear["buffer_size"] == 0
    assert snap_after_clear["last_seq"] == 0  # empty buffer
    con.execute("SELECT 2")
    snap = sqlite_profiler.get_recent()
    assert snap["last_seq"] > last_seq_before  # seq counter is monotonic


# ── End-to-end via get_con() ─────────────────────────────────────────────────


def test_get_con_returns_instrumented_connection():
    """metadata_db.get_con() must hand out InstrumentedConnection so all
    real production SQLite traffic flows through the profiler."""
    from backend.core import metadata as metadata_db

    con = metadata_db.get_con("test-svc")
    assert isinstance(con, sqlite_profiler.InstrumentedConnection)


def test_metadata_db_traffic_appears_in_buffer():
    """Sanity end-to-end: a real metadata_db call populates the ring buffer."""
    from backend.core import metadata as metadata_db

    metadata_db.get_con("test-svc-traffic")  # init + PRAGMAs
    snap = sqlite_profiler.get_recent()
    sqls = [q["sql"] for q in snap["queries"]]
    # PRAGMA journal_mode=WAL is the canonical first statement after connect.
    assert any("PRAGMA journal_mode" in s for s in sqls)


# ── Helper-function direct tests (cover the small surface) ──────────────────


def test_summarize_sql_truncates_long_strings():
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(1000))
    out = sqlite_profiler._summarize_sql(long_sql)
    assert "chars]" in out
    assert len(out) < len(long_sql)


def test_summarize_sql_coerces_non_strings():
    """Some callers pass bytes / int; the profiler must not raise."""
    # bytes → str(bytes) gives "b'...'" which is acceptable; just verify
    # no exception and the result is a string.
    out = sqlite_profiler._summarize_sql(b"SELECT 1")
    assert isinstance(out, str)
    assert "SELECT 1" in out
    assert sqlite_profiler._summarize_sql(42) == "42"


def test_describe_params_covers_each_shape():
    assert sqlite_profiler._describe_params(None) == "none"
    assert sqlite_profiler._describe_params([1, 2, 3]) == "seq[3]"
    assert sqlite_profiler._describe_params((1,)) == "seq[1]"
    assert sqlite_profiler._describe_params({"a": 1, "b": 2}) == "map[2]"
    # Other types fall through to the type-name branch.
    assert sqlite_profiler._describe_params(42) == "int"


def test_record_swallows_internal_exceptions(monkeypatch):
    """The profiler's hard contract: any internal failure must never
    reach the caller's SQL path."""
    from unittest.mock import MagicMock

    bad = MagicMock()
    bad.append.side_effect = RuntimeError("buffer borked")
    monkeypatch.setattr(sqlite_profiler, "_buffer", bad)
    # Must not raise — exception is logged at DEBUG and swallowed.
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
    # Must not raise even though we never registered.
    sqlite_profiler._live_deregister(-1, error=None)


def test_live_deregister_swallows_registry_errors(monkeypatch):
    from unittest.mock import MagicMock

    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "deregister",
        MagicMock(side_effect=RuntimeError("dereg failed")),
    )
    # Must not raise even though deregister blew up.
    sqlite_profiler._live_deregister(42, error=None)


def test_connection_cursor_returns_instrumented_by_default():
    con = sqlite3.connect(":memory:", factory=sqlite_profiler.InstrumentedConnection)
    cur = con.cursor()
    assert isinstance(cur, sqlite_profiler.InstrumentedCursor)


def test_connection_cursor_respects_explicit_factory():
    """Callers can opt out by passing factory=sqlite3.Cursor; the
    instrumentation shouldn't force its subclass on them."""
    con = sqlite3.connect(":memory:", factory=sqlite_profiler.InstrumentedConnection)
    cur = con.cursor(factory=sqlite3.Cursor)
    assert type(cur) is sqlite3.Cursor


def test_executescript_failure_is_still_recorded(tmp_path):
    con = _open_instrumented(str(tmp_path / "t.db"))
    with pytest.raises(sqlite3.OperationalError):
        con.executescript("CREATE TABLE BAD SQL BLAH;")
    snap = sqlite_profiler.get_recent()
    assert any("BAD SQL" in q["sql"] for q in snap["queries"])


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


def test_statement_lands_in_request_collector_when_tracking_active(tmp_path, _request_collector):
    """The page-scoped Debug Panel view: a statement executed while a
    request is being tracked must land in BOTH the process-global ring
    buffer AND the request's contextvar collector."""
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("CREATE TABLE t (x INTEGER)")

    entries = telemetry.get_sqlite_queries()
    assert len(entries) == 1
    assert entries[0]["sql"] == "CREATE TABLE t (x INTEGER)"
    # Identical entry object also sits in the ring buffer (cron view).
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["seq"] == entries[0]["seq"]


def test_statement_skips_request_collector_outside_tracking(tmp_path, _request_collector):
    """Cron/startup statements (no start_call_tracking) must NOT create or
    populate a request collector — only the ring buffer sees them. This is
    what keeps the Debug Panel's 'This page' view free of background noise."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("SELECT 1")

    assert _SQLITE_QUERIES.get() is None  # record path never initialises it
    assert sqlite_profiler.get_recent()["buffer_size"] == 1


def test_request_collector_failure_does_not_break_sql_path(tmp_path, _request_collector, monkeypatch):
    """The profiler contract: observability failures never propagate into
    the calling SQL path. A broken record_sqlite_query must not raise."""
    from backend.utils import telemetry

    monkeypatch.setattr(
        telemetry,
        "record_sqlite_query",
        lambda entry: (_ for _ in ()).throw(RuntimeError("collector broken")),
    )
    con = _open_instrumented(str(tmp_path / "t.db"))
    con.execute("SELECT 1")  # must not raise
