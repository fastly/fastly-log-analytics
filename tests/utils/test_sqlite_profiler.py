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
    from backend.core import metadata_db

    con = metadata_db.get_con("test-svc")
    assert isinstance(con, sqlite_profiler.InstrumentedConnection)


def test_metadata_db_traffic_appears_in_buffer():
    """Sanity end-to-end: a real metadata_db call populates the ring buffer."""
    from backend.core import metadata_db

    metadata_db.get_con("test-svc-traffic")  # init + PRAGMAs
    snap = sqlite_profiler.get_recent()
    sqls = [q["sql"] for q in snap["queries"]]
    # PRAGMA journal_mode=WAL is the canonical first statement after connect.
    assert any("PRAGMA journal_mode" in s for s in sqls)
