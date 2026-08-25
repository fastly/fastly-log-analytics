"""DuckDB lock-contention retry contract.

The locked contract (TESTING_PLAN_3 §7):

* On a lock error from ``duckdb.connect``, the application retries with
  exponential backoff (50 ms → 100 ms → 200 ms → 400 ms → 500 ms cap).
* The whole loop is bounded by ``max_wait``. When the deadline is hit,
  the caller sees a ``DBBusyError`` — never a raw DuckDB lock string.
* Every retry attempt increments a process-wide counter so contention
  is observable in operational dashboards (and pinnable in tests).

This file pins all three.

DuckDB's single-writer-per-file model means a cron job holding a writer
connection blocks a dashboard reader (or vice versa). Without retry the
user-facing request 500s; with retry the contention is invisible at the
HTTP layer except in the long-tail latency histogram + the counter.
"""

from __future__ import annotations

import threading
import time

import duckdb
import pytest

from backend.core import duckdb as duckdb_mod
from backend.core.duckdb import (
    DBBusyError,
    _reset_lock_retry_count,
    get_connection,
    get_lock_retry_count,
)


@pytest.fixture(autouse=True)
def _reset_counter():
    """Each test starts from zero so assertions are local, not cumulative."""
    _reset_lock_retry_count()
    yield
    _reset_lock_retry_count()


def _src(db_path: str) -> dict:
    """Minimal source dict the connection setup will accept.

    ``_configure_fos`` runs unconditionally and reads several keys; supply
    empty strings to match the default-source pattern in
    [_build_default_source](../../backend/core/duckdb.py#L93).
    """
    return {
        "name": "test",
        "duckdb_path": db_path,
        "endpoint": "",
        "access_key_id": "",
        "secret_access_key": "",
        "region": "us-east-1",
        "bucket": "",
        "prefix": "",
        "cdn_url": "",
        "cdn_secret": "",
    }


# ── Unit: counter + backoff behavior via monkeypatched duckdb.connect ──────


def _make_failing_connect(fail_count: int, real_connect):
    """Return a duckdb.connect stand-in that fails ``fail_count`` times
    with a lock error, then delegates to the real connect.
    """
    state = {"calls": 0}

    def _fake(db_path, read_only=False):
        state["calls"] += 1
        if state["calls"] <= fail_count:
            # The substring must match one of the patterns get_connection
            # treats as a lock error: "conflict", "locked", or
            # "different configuration". Use "locked" — most representative.
            raise duckdb.Error("Could not set lock on file: database is locked")
        return real_connect(db_path, read_only=read_only)

    return _fake, state


def test_succeeds_after_transient_lock_errors(tmp_path, monkeypatch):
    """Three lock failures, then success. Counter should be 3."""
    db_path = str(tmp_path / "retry.duckdb")
    real_connect = duckdb.connect
    fake, state = _make_failing_connect(fail_count=3, real_connect=real_connect)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    src = _src(db_path)
    con = get_connection(src, max_wait=5.0)
    try:
        assert con.execute("SELECT 1").fetchone() == (1,)
    finally:
        con.close()

    assert state["calls"] == 4, "should have retried 3 times then succeeded on the 4th call"
    assert get_lock_retry_count() == 3, f"counter must increment once per retry; got {get_lock_retry_count()}"


def test_uses_exponential_backoff(tmp_path, monkeypatch):
    """Sleep durations should roughly double up to the cap. We capture the
    sleep arguments rather than measuring wall time so the test isn't flaky
    on slow CI."""
    db_path = str(tmp_path / "backoff.duckdb")
    real_connect = duckdb.connect
    fake, _ = _make_failing_connect(fail_count=5, real_connect=real_connect)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    sleeps: list[float] = []
    real_sleep = time.sleep

    def _capture_sleep(seconds):
        if threading.current_thread() is threading.main_thread():
            sleeps.append(seconds)
        # Don't actually sleep — keep the test fast.
        real_sleep(0)

    monkeypatch.setattr(duckdb_mod.time, "sleep", _capture_sleep)

    # Trigger the False branch of _capture_sleep from a background thread
    bg_sleeps = []

    def bg_thread():
        duckdb_mod.time.sleep(1.23)
        bg_sleeps.append(1.23)

    t = threading.Thread(target=bg_thread)
    t.start()
    t.join()

    src = _src(db_path)
    con = get_connection(src, max_wait=30.0)
    con.close()

    # Five retries → five sleeps.
    assert len(sleeps) == 5
    assert bg_sleeps == [1.23]
    # 50ms, 100ms, 200ms, 400ms, then capped at 500ms.
    # Allow tiny floating-point fuzz.
    expected = [0.05, 0.10, 0.20, 0.40, 0.50]
    for i, (got, want) in enumerate(zip(sleeps, expected)):
        assert abs(got - want) < 1e-6, f"sleep #{i + 1}: expected {want}s, got {got}s. Full sequence: {sleeps}"


def test_deadline_exceeded_raises_dbbusyerror(tmp_path, monkeypatch):
    """When the deadline is reached, the caller sees DBBusyError — not a
    raw DuckDB exception. This is the contract dashboards rely on to
    translate to a clean 'system busy, retry' response."""
    db_path = str(tmp_path / "busy.duckdb")

    def _always_locked(db_path, read_only=False):
        raise duckdb.Error("Could not set lock on file: database is locked")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _always_locked)

    # Patch sleep to a no-op so the test runs fast — the deadline still
    # advances via time.monotonic().
    monkeypatch.setattr(duckdb_mod.time, "sleep", lambda s: None)

    src = _src(db_path)
    with pytest.raises(DBBusyError) as excinfo:
        get_connection(src, max_wait=0.2)

    assert "locked by another process" in str(excinfo.value)
    # __cause__ preserves the original DuckDB error for forensic debugging.
    assert excinfo.value.__cause__ is not None
    # Multiple retries should have been recorded — the exact count varies
    # by scheduler, but it must be > 0 (we hit the retry path before
    # giving up).
    assert get_lock_retry_count() > 0


def test_non_lock_error_is_not_retried(tmp_path, monkeypatch):
    """Random errors must propagate immediately — retry would mask bugs."""
    db_path = str(tmp_path / "boom.duckdb")
    calls = {"n": 0}

    def _boom(db_path, read_only=False):
        calls["n"] += 1
        raise duckdb.Error("Catastrophic failure: something unrelated to locking")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _boom)

    src = _src(db_path)
    with pytest.raises(duckdb.Error, match="Catastrophic failure"):
        get_connection(src, max_wait=5.0)

    assert calls["n"] == 1, "non-lock errors must not be retried"
    assert get_lock_retry_count() == 0


# ── Integration: real concurrent writer + readers ──────────────────────────


def test_concurrent_readers_against_held_writer(tmp_path):
    """All connections open with read_only=False (get_connection forces this),
    so concurrent readers coexist with a held writer within the same process
    without contention — DuckDB shares the database instance internally.

    Contract: every reader succeeds; no retries needed."""
    db_path = str(tmp_path / "stress.duckdb")

    # Bootstrap the file with a table so readers have something to query.
    boot = duckdb.connect(db_path)
    boot.execute("CREATE TABLE t(x INTEGER)")
    boot.execute("INSERT INTO t VALUES (1), (2), (3)")
    boot.close()

    # Hold a long-running writer for the entire test.
    writer = duckdb.connect(db_path, read_only=False)
    try:
        results: list[str] = []
        errors: list[Exception] = []

        def reader():
            try:
                src = _src(db_path)
                con = get_connection(src, max_wait=0.3, read_only=True)
                try:
                    rows = con.execute("SELECT count(*) FROM t").fetchone()
                    results.append(f"ok:{rows[0]}")
                finally:
                    con.close()
            except DBBusyError:
                results.append("busy")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)
    finally:
        writer.close()

    assert not errors, f"raw exceptions leaked from get_connection: {errors!r}"
    assert len(results) == 8, f"expected 8 results, got {results!r}"
    for r in results:
        assert r.startswith("ok:"), f"unexpected result: {r!r}"


def test_writer_then_reader_release_path(tmp_path):
    """Sanity: open a writer, close it, then a reader succeeds without
    retries. Ensures the retry path is contention-only — no false positives
    in the no-contention case."""
    db_path = str(tmp_path / "release.duckdb")

    boot = duckdb.connect(db_path)
    boot.execute("CREATE TABLE t(x INTEGER)")
    boot.close()

    src = _src(db_path)

    # Open and immediately close a writer.
    w = get_connection(src, read_only=False)
    w.close()
    assert get_lock_retry_count() == 0, "no contention; counter must be 0"

    # Reader should sail through.
    r = get_connection(src, read_only=True)
    try:
        r.execute("SELECT count(*) FROM t").fetchone()
    finally:
        r.close()

    assert get_lock_retry_count() == 0, (
        f"reader after closed writer hit the retry path "
        f"({get_lock_retry_count()} retries); regression in lock detection?"
    )
