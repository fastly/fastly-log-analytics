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

import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

from backend.core import duckdb as duckdb_mod
from backend.core.duckdb import (
    DBBusyError,
    get_connection,
)


@pytest.fixture
def retries(monkeypatch):
    """Observe this thread's retries without suppressing background telemetry."""
    owner = threading.current_thread()
    record = duckdb_mod._record_lock_retry
    count = 0

    def tracked_record():
        nonlocal count
        record()
        if threading.current_thread() is owner:
            count += 1

    monkeypatch.setattr(duckdb_mod, "_record_lock_retry", tracked_record)
    return lambda: count


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


def test_process_wide_retry_counter_records_every_thread_and_resets():
    # A fresh interpreter excludes other tests' delayed background retries
    # while exercising the real process-global counter, lock, and public getter.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
from concurrent.futures import ThreadPoolExecutor
from backend.core.duckdb import _record_lock_retry, _reset_lock_retry_count, get_lock_retry_count

_reset_lock_retry_count()
assert get_lock_retry_count() == 0
with ThreadPoolExecutor(max_workers=4) as executor:
    list(executor.map(lambda _: _record_lock_retry(), range(800)))
assert get_lock_retry_count() == 800
_reset_lock_retry_count()
assert get_lock_retry_count() == 0
""",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def _make_failing_connect(fail_count: int, real_connect, target_path: str):
    """Return a duckdb.connect stand-in that fails ``fail_count`` times
    with a lock error, then delegates to the real connect.
    """
    state = {"calls": 0}

    def _fake(db_path, read_only=False):
        if db_path != target_path:
            return real_connect(db_path, read_only=read_only)
        state["calls"] += 1
        if state["calls"] <= fail_count:
            # The substring must match one of the patterns get_connection
            # treats as a lock error: "conflict", "locked", or
            # "different configuration". Use "locked" — most representative.
            raise duckdb.Error("Could not set lock on file: database is locked")
        return real_connect(db_path, read_only=read_only)

    return _fake, state


def test_succeeds_after_transient_lock_errors(tmp_path, monkeypatch, retries):
    """Three lock failures, then success. Counter should be 3."""
    db_path = str(tmp_path / "retry.duckdb")
    # Pre-create the file so the `if not os.path.exists` branch in get_connection doesn't run and confuse the call counts
    duckdb.connect(db_path).close()

    real_connect = duckdb.connect
    fake, state = _make_failing_connect(fail_count=3, real_connect=real_connect, target_path=db_path)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    # Reproduce an earlier test's background work while these patches are live.
    def unrelated_work():
        duckdb.connect(str(tmp_path / "unrelated.duckdb")).close()
        duckdb_mod._record_lock_retry()

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(unrelated_work).result()
    assert state["calls"] == 0
    assert retries() == 0

    src = _src(db_path)
    con = get_connection(src, max_wait=5.0, read_only=False)
    try:
        assert con.execute("SELECT 1").fetchone() == (1,)
    finally:
        con.close()

    assert state["calls"] == 4, "should have retried 3 times then succeeded on the 4th call"
    assert retries() == 3, f"counter must increment once per retry; got {retries()}"


def test_uses_exponential_backoff(tmp_path, monkeypatch):
    """Sleep durations should roughly double up to the cap. We capture the
    sleep arguments rather than measuring wall time so the test isn't flaky
    on slow CI."""
    db_path = str(tmp_path / "backoff.duckdb")
    duckdb.connect(db_path).close()

    real_connect = duckdb.connect
    fake, _ = _make_failing_connect(fail_count=5, real_connect=real_connect, target_path=db_path)
    monkeypatch.setattr(duckdb_mod.duckdb, "connect", fake)

    sleeps: list[float] = []
    real_sleep = time.sleep

    def _capture_sleep(seconds):
        if threading.current_thread() is threading.main_thread():
            sleeps.append(seconds)
            real_sleep(0)
        else:
            real_sleep(seconds)

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
    con = get_connection(src, max_wait=30.0, read_only=False)
    con.close()

    # Five retries → five sleeps.
    assert len(sleeps) == 5
    assert bg_sleeps == [1.23]
    # 50ms, 100ms, 200ms, 400ms, then capped at 500ms.
    # Allow tiny floating-point fuzz.
    expected = [0.05, 0.10, 0.20, 0.40, 0.50]
    for i, (got, want) in enumerate(zip(sleeps, expected)):
        assert abs(got - want) < 1e-6, f"sleep #{i + 1}: expected {want}s, got {got}s. Full sequence: {sleeps}"


def test_deadline_exceeded_raises_dbbusyerror(tmp_path, monkeypatch, retries):
    """When the deadline is reached, the caller sees DBBusyError — not a
    raw DuckDB exception. This is the contract dashboards rely on to
    translate to a clean 'system busy, retry' response."""
    db_path = str(tmp_path / "busy.duckdb")

    real_connect = duckdb.connect

    def _always_locked(path, read_only=False):
        if path != db_path:
            return real_connect(path, read_only=read_only)
        raise duckdb.Error("Could not set lock on file: database is locked")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _always_locked)

    # Patch sleep to a no-op so the test runs fast — the deadline still
    # advances via time.monotonic().
    real_sleep = time.sleep
    owner = threading.current_thread()
    monkeypatch.setattr(
        duckdb_mod.time, "sleep", lambda s: None if threading.current_thread() is owner else real_sleep(s)
    )

    src = _src(db_path)
    with pytest.raises(DBBusyError) as excinfo:
        get_connection(src, max_wait=0.2)

    assert "locked by another process" in str(excinfo.value)
    # __cause__ preserves the original DuckDB error for forensic debugging.
    assert excinfo.value.__cause__ is not None
    # Multiple retries should have been recorded — the exact count varies
    # by scheduler, but it must be > 0 (we hit the retry path before
    # giving up).
    assert retries() > 0


def test_non_lock_error_is_not_retried(tmp_path, monkeypatch, retries):
    """Random errors must propagate immediately — retry would mask bugs."""
    db_path = str(tmp_path / "boom.duckdb")
    calls = {"n": 0}
    real_connect = duckdb.connect

    def _boom(path, read_only=False):
        if path != db_path:
            return real_connect(path, read_only=read_only)
        calls["n"] += 1
        raise duckdb.Error("Catastrophic failure: something unrelated to locking")

    monkeypatch.setattr(duckdb_mod.duckdb, "connect", _boom)

    src = _src(db_path)
    with pytest.raises(duckdb.Error, match="Catastrophic failure"):
        get_connection(src, max_wait=5.0, read_only=False)

    assert calls["n"] == 1, "non-lock errors must not be retried"
    assert retries() == 0


# ── Integration: real concurrent writer + readers ──────────────────────────


def test_writer_then_reader_release_path(tmp_path, retries):
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
    assert retries() == 0, "no contention; counter must not advance"

    # Reader should sail through.
    r = get_connection(src, read_only=True)
    try:
        r.execute("SELECT count(*) FROM t").fetchone()
    finally:
        r.close()

    assert retries() == 0, (
        f"reader after closed writer hit the retry path ({retries()} retries); regression in lock detection?"
    )
