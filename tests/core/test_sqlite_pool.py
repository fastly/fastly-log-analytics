"""Direct tests for :class:`backend.core.sqlite_pool.ThreadLocalPool`.

The three pool consumers (``metadata.base``, ``metadata.usage_log_db``,
``share_db.connection``) have their own behavioral test suites that pin
the surface they expose. These tests pin the shared abstraction itself:
the per-thread cache, the registry, the init lock, the schema-init gate,
the on-borrow hook, the connect override, and the read-only path.
"""

from __future__ import annotations

import os
import sqlite3
import threading

import pytest

from backend.core.sqlite_pool import DEFAULT_PRAGMAS, ThreadLocalPool

_pools: list[ThreadLocalPool] = []


@pytest.fixture(autouse=True)
def _cleanup_pools():
    _pools.clear()
    yield
    for pool in _pools:
        try:
            pool.close_all()
        except Exception:
            pass
    _pools.clear()


def _schema_users(con: sqlite3.Connection) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT)")
    con.commit()


def _make_pool(tmp_path, **overrides) -> ThreadLocalPool:
    def path_fn(key):
        return os.path.join(str(tmp_path), f"{key}.db")

    defaults = dict(
        name="testpool",
        path_fn=path_fn,
        schema_fn=_schema_users,
    )
    defaults.update(overrides)
    pool = ThreadLocalPool(**defaults)
    _pools.append(pool)
    return pool


def test_returns_thread_local_connection(tmp_path):
    pool = _make_pool(tmp_path)
    a = pool.get("svc1")
    b = pool.get("svc1")
    assert a is b


def test_distinct_keys_get_distinct_connections(tmp_path):
    pool = _make_pool(tmp_path)
    a = pool.get("svc1")
    b = pool.get("svc2")
    assert a is not b


def test_distinct_threads_get_distinct_connections(tmp_path):
    pool = _make_pool(tmp_path)
    seen: list[sqlite3.Connection] = []

    def _worker() -> None:
        seen.append(pool.get("svc1"))

    t1 = threading.Thread(target=_worker)
    t2 = threading.Thread(target=_worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert len(seen) == 2
    assert seen[0] is not seen[1]


def test_pragmas_applied_in_order(tmp_path):
    pool = _make_pool(tmp_path)
    con = pool.get("svc1")
    journal_mode = con.execute("PRAGMA journal_mode").fetchone()[0]
    assert journal_mode == "wal"
    assert con.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_schema_fn_runs_once_per_path(tmp_path):
    calls = {"n": 0}

    def schema_fn(con):
        calls["n"] += 1
        con.execute("CREATE TABLE IF NOT EXISTS t(x INTEGER)")
        con.commit()

    pool = _make_pool(tmp_path, schema_fn=schema_fn)
    pool.get("svc1")

    def _worker():
        pool.get("svc1")

    t = threading.Thread(target=_worker)
    t.start()
    t.join()
    assert calls["n"] == 1


def test_close_all_drains_cross_thread_connections(tmp_path):
    pool = _make_pool(tmp_path)
    seen: list[sqlite3.Connection] = []

    def _worker():
        seen.append(pool.get("svc1"))

    t = threading.Thread(target=_worker)
    t.start()
    t.join()

    pool.close_all()
    # The cross-thread connection is closed; cursor operations raise.
    with pytest.raises(sqlite3.ProgrammingError):
        seen[0].execute("SELECT 1")


def test_teardown_closes_cross_thread_connections(tmp_path):
    """Finding 008: ``teardown(key)`` previously only closed the calling
    thread's cached connection. Connections opened by *other* worker threads
    for the same key were left in ``_all_connections`` — and once the
    caller subsequently deleted the underlying SQLite file, those lingering
    file descriptors leaked (SQLite holds the file open, so the OS-level
    delete succeeds but the inode stays around until the FD is closed).
    The fix walks ``_all_connections`` under the lock and closes every
    handle stamped with the matching ``_service_id``."""
    pool = _make_pool(tmp_path)
    seen: list[sqlite3.Connection] = []

    def _worker():
        seen.append(pool.get("svc1"))

    # Open one connection on the main thread and one on a worker thread.
    main_con = pool.get("svc1")
    t = threading.Thread(target=_worker)
    t.start()
    t.join()
    assert len(seen) == 1
    worker_con = seen[0]
    assert main_con is not worker_con, "thread-local cache: distinct connections per thread"

    pool.teardown("svc1")

    # Both connections must be closed — using either raises ProgrammingError.
    with pytest.raises(sqlite3.ProgrammingError):
        main_con.execute("SELECT 1")
    with pytest.raises(sqlite3.ProgrammingError):
        worker_con.execute("SELECT 1")


def test_reaps_dead_thread_connections(tmp_path):
    """A connection opened on a now-exited thread must be closed + dropped from
    ``_all_connections`` on the next cold-open.

    This is the 2026-06-22 OOM fix: cron_task formerly spawned a fresh executor
    thread per tick, each opening per-thread metadata/usage_log connections that
    lingered forever (file + up-to-64MB page cache). ``check_same_thread=True``
    means a dead thread's connection can never be used again, so reaping is safe.
    """
    pool = _make_pool(tmp_path)
    seen: list[sqlite3.Connection] = []

    def _worker():
        seen.append(pool.get("svc1"))

    t = threading.Thread(target=_worker)
    t.start()
    t.join()  # worker dead → its connection is orphaned in _all_connections
    worker_con = seen[0]
    assert len(pool._all_connections) == 1

    # A cold-open on the (live) main thread triggers the reaper.
    main_con = pool.get("svc2")

    with pytest.raises(sqlite3.ProgrammingError):
        worker_con.execute("SELECT 1")  # closed by the reaper
    remaining = [c for c, _owner in pool._all_connections]
    assert worker_con not in remaining
    assert main_con in remaining
    assert len(pool._all_connections) == 1


def test_get_reopens_when_cached_connection_closed_elsewhere(tmp_path):
    """If a cached connection is closed out-of-band (close_all/teardown on
    another thread), the next ``get`` on the owning thread must transparently
    reopen rather than hand back the dead handle. Guards the de-churn change
    where cron bodies share long-lived watchdog threads that can outlive a
    pool reset."""
    pool = _make_pool(tmp_path)
    first = pool.get("svc1")
    first.close()  # simulate close_all/teardown closing it from elsewhere
    second = pool.get("svc1")
    assert second is not first
    assert second.execute("SELECT 1").fetchone()[0] == 1


def test_teardown_drops_init_marker(tmp_path):
    calls = {"n": 0}

    def schema_fn(con):
        calls["n"] += 1
        con.execute("CREATE TABLE IF NOT EXISTS t(x INTEGER)")
        con.commit()

    pool = _make_pool(tmp_path, schema_fn=schema_fn)
    pool.get("svc1")
    pool.teardown("svc1")
    pool.get("svc1")
    assert calls["n"] == 2


def test_on_borrow_returning_none_evicts_and_reopens(tmp_path):
    state = {"call": 0}

    def on_borrow(con):
        state["call"] += 1
        return None if state["call"] == 1 else con

    pool = _make_pool(tmp_path, on_borrow_fn=on_borrow)
    pool.get("svc1")  # warms the cache
    second = pool.get("svc1")  # on_borrow returns None -> reopen
    third = pool.get("svc1")  # on_borrow returns con -> reuse
    assert second is third


def test_init_lock_provider_invoked_per_call(tmp_path):
    real_lock = threading.Lock()
    calls = {"n": 0}

    def provider():
        calls["n"] += 1
        return real_lock

    pool = _make_pool(tmp_path, init_lock_provider=provider)
    pool.get("svc1")  # cold
    pool.get("svc1")  # cache hit — no init_lock needed
    pool.get("svc2")  # cold again
    assert calls["n"] == 2


def test_init_lock_acquire_timeout_raises_named(tmp_path):
    held = threading.Lock()
    held.acquire()

    pool = _make_pool(
        tmp_path,
        init_lock_provider=lambda: held,
        init_lock_timeout=0.05,
    )
    with pytest.raises(sqlite3.OperationalError) as exc:
        pool.get("svc1")
    assert "testpool" in str(exc.value)
    assert "contended" in str(exc.value)
    held.release()


def test_connect_fn_override_intercepts(tmp_path):
    captured = {"paths": []}

    def custom_connect(path):
        captured["paths"].append(path)
        return sqlite3.connect(path, timeout=5.0)

    pool = _make_pool(tmp_path, connect_fn=custom_connect, stamp_service_id=False)
    pool.get("svc1")
    assert captured["paths"] == [os.path.join(str(tmp_path), "svc1.db")]


def test_service_id_stamped_on_default_connection(tmp_path):
    pool = _make_pool(tmp_path)
    con = pool.get("svc1")
    assert getattr(con, "_service_id", None) == "svc1"


def test_open_readonly_does_not_register_or_pragma(tmp_path):
    pool = _make_pool(tmp_path)
    # Create the file first
    rw = pool.get("svc1")
    rw.execute("INSERT INTO users(name) VALUES ('a')")
    rw.commit()

    before = len(pool._all_connections)
    ro = pool.open_readonly("svc1")
    assert len(pool._all_connections) == before
    rows = ro.execute("SELECT name FROM users").fetchall()
    assert rows[0][0] == "a"
    # Read-only — writes raise
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO users(name) VALUES ('b')")
    ro.close()


def test_open_readonly_missing_file_raises(tmp_path):
    pool = _make_pool(tmp_path)
    with pytest.raises(sqlite3.OperationalError):
        pool.open_readonly("nosuch")


def test_reset_clears_initialized_and_closes(tmp_path):
    calls = {"n": 0}

    def schema_fn(con):
        calls["n"] += 1
        con.execute("CREATE TABLE IF NOT EXISTS t(x INTEGER)")
        con.commit()

    pool = _make_pool(tmp_path, schema_fn=schema_fn)
    pool.get("svc1")
    pool.reset()
    pool.get("svc1")
    assert calls["n"] == 2


def test_default_pragmas_constant_unchanged():
    # Pinned so a downstream pool that imports DEFAULT_PRAGMAS for
    # comparison or composition keeps the same ordering contract.
    assert DEFAULT_PRAGMAS == (
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=NORMAL",
        "PRAGMA foreign_keys=ON",
        "PRAGMA cache_size=-64000",
        "PRAGMA busy_timeout=30000",
    )
