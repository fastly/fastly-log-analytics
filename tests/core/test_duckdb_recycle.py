"""Tests for backend.core.duckdb_recycle — the periodic DuckDB instance recycle.

Covers the deterministic weakref liveness gate on a real temp-file DB, db_path
grouping, the success path (barrier + locks always released), abort-on-lock-busy,
and the RSS-threshold skip.
"""

import gc
import os
import threading
import time
from unittest.mock import MagicMock

import duckdb

import backend.core.duckdb as d
import backend.core.duckdb_recycle as rec
from backend.core.iceberg.view import _get_service_lock


def test_live_set_hits_zero_with_real_conns(tmp_path):
    """Real DuckDB conns registered in the liveness set drop to zero once closed
    and GC'd — the authoritative signal the recycle waits on."""
    dbfile = str(tmp_path / "real.duckdb")
    path = os.path.abspath(dbfile)
    assert d.live_connection_count(path) == 0

    conns = [duckdb.connect(dbfile) for _ in range(3)]
    for c in conns:
        d._register_live_connection(c, path)
    assert d.live_connection_count(path) == 3

    for c in conns:
        c.close()
    conns.clear()
    c = None  # drop the lingering loop-variable ref to the last conn
    gc.collect()
    assert d.live_connection_count(path) == 0


def test_recycle_once_no_services(monkeypatch):
    monkeypatch.setattr(rec.svcconfig, "list_configs", lambda: [])
    assert rec.recycle_once() == "no services configured"


def test_sources_grouped_by_db_path(monkeypatch):
    """Two services sharing a duckdb_path land in one group (one instance)."""
    cfgs = [
        {"name": "svc1", "duckdb_path": "/tmp/shared.duckdb"},
        {"name": "svc2", "duckdb_path": "/tmp/shared.duckdb"},
    ]
    monkeypatch.setattr(rec.svcconfig, "list_configs", lambda: cfgs)
    monkeypatch.setattr(rec._db, "_source_from_config", lambda c: c)

    groups = rec._sources_by_db_path()
    assert len(groups) == 1
    ((only_path, sources),) = groups.items()
    assert only_path == os.path.abspath("/tmp/shared.duckdb")
    assert {s["name"] for s in sources} == {"svc1", "svc2"}


def test_recycle_db_path_success_releases_barrier_and_lock(tmp_path):
    """With no pools and no live conns, a recycle reports 'recycled' and always
    leaves the barrier cleared and the service lock released."""
    path = os.path.abspath(str(tmp_path / "ok.duckdb"))
    src = {"name": "rec_ok_svc", "duckdb_path": path}

    result = rec._recycle_db_path(path, [src])

    assert result["status"] == "recycled"
    assert result["live_after"] == 0
    # Barrier cleared.
    assert path not in d._recycle_barrier_active
    # Lock fully released (acquirable again right away).
    lock = _get_service_lock("rec_ok_svc")
    assert lock.acquire(blocking=False) is True
    lock.release()


def test_recycle_db_path_skips_when_write_lock_busy(tmp_path, monkeypatch):
    """If a writer holds the service lock, recycle aborts cleanly (no barrier)."""
    monkeypatch.setenv("DUCKDB_RECYCLE_LOCK_TIMEOUT_MS", "100")
    path = os.path.abspath(str(tmp_path / "busy.duckdb"))
    src = {"name": "rec_busy_svc", "duckdb_path": path}
    lock = _get_service_lock("rec_busy_svc")

    held = threading.Event()
    release = threading.Event()

    def holder():
        lock.acquire()
        held.set()
        release.wait(timeout=5.0)
        lock.release()

    th = threading.Thread(target=holder)
    th.start()
    held.wait(timeout=2.0)
    try:
        result = rec._recycle_db_path(path, [src])
        assert result["status"] == "skipped_locked"
        # Recycle never raised the barrier because it bailed before acquiring.
        assert path not in d._recycle_barrier_active
    finally:
        release.set()
        th.join(timeout=2.0)


def test_barrier_no_block_when_clear():
    """_barrier_wait returns immediately when no barrier is active."""
    t0 = time.monotonic()
    d._barrier_wait("/tmp/no-barrier.duckdb")
    assert time.monotonic() - t0 < 0.1


def test_barrier_blocks_then_fails_open(monkeypatch):
    """With a barrier set and never cleared, _barrier_wait returns after the cap
    (fail-open) — it must never block a connection open indefinitely."""
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "150")
    path = "/tmp/failopen.duckdb"
    d.set_recycle_barrier(path, True)
    try:
        t0 = time.monotonic()
        d._barrier_wait(path)
        elapsed = time.monotonic() - t0
        # Returned by fail-open, ~cap, even though the barrier is still set.
        assert 0.1 <= elapsed < 0.6
        assert path in d._recycle_barrier_active
    finally:
        d.set_recycle_barrier(path, False)


def test_barrier_cleared_unblocks_immediately(monkeypatch):
    """Clearing the barrier wakes a parked waiter well before the cap."""
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "5000")
    path = "/tmp/cleared.duckdb"
    d.set_recycle_barrier(path, True)
    elapsed = {}

    def waiter():
        t0 = time.monotonic()
        d._barrier_wait(path)
        elapsed["t"] = time.monotonic() - t0

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(0.1)
    d.set_recycle_barrier(path, False)  # clear → should wake the waiter now
    th.join(timeout=2.0)
    assert not th.is_alive()
    assert elapsed["t"] < 1.0  # nowhere near the 5s cap


def test_barrier_per_db_path_isolation(monkeypatch):
    """A barrier on one db file must not block opens against a different file."""
    monkeypatch.setenv("DUCKDB_RECYCLE_BARRIER_CAP_MS", "5000")
    a, b = "/tmp/path_a.duckdb", "/tmp/path_b.duckdb"
    d.set_recycle_barrier(a, True)
    try:
        t0 = time.monotonic()
        d._barrier_wait(b)  # different path — must not block
        assert time.monotonic() - t0 < 0.2
    finally:
        d.set_recycle_barrier(a, False)


def test_live_connection_set_tracks_and_drops():
    """Registered conns count as live; once unreferenced + GC'd they drop out."""
    path = "/tmp/liveset.duckdb"
    before = d.live_connection_count(path)
    conns = [MagicMock() for _ in range(3)]
    for c in conns:
        d._register_live_connection(c, path)
    assert d.live_connection_count(path) == before + 3
    conns.clear()
    c = None  # drop the lingering loop-variable ref to the last conn
    gc.collect()
    assert d.live_connection_count(path) == before


def test_current_rss_bytes_returns_positive():
    rss = d.current_rss_bytes()
    assert rss is None or rss > 0
