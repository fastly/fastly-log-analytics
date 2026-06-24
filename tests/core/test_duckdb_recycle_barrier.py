"""Tests for the fail-open recycle barrier + weakref liveness set in duckdb.py.

The barrier briefly pauses new connection opens for a db file so a recycle can
drain it; it MUST fail open (never block an open longer than its cap) and MUST
be per-db_path. The liveness set tracks live raw conns so the recycle can confirm
zero before destroying the instance.
"""

import gc
import threading
import time
from unittest.mock import MagicMock

import backend.core.duckdb as d


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
