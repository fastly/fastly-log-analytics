"""Tests for the _Pool draining epoch used by the DuckDB instance recycle.

begin_drain closes idle conns + rejects new acquires; release closes (not idles)
while draining; wait_drained blocks until _in_use==0 or times out; end_drain
resumes. None of it may deadlock.
"""

import threading
import time
from unittest.mock import MagicMock

import duckdb
import pytest

from backend.core.duckdb_pool import _Pool, _PoolBusy


def _mock_conn():
    return MagicMock(spec=duckdb.DuckDBPyConnection)


def test_begin_drain_closes_idle_and_zeroes_in_use():
    pool = _Pool(service_key="drain_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3

    pool.begin_drain()

    assert pool._draining is True
    assert pool._in_use == 0
    assert pool._idle.empty()
    for c in conns:
        c.close.assert_called_once()


def test_acquire_rejected_while_draining():
    pool = _Pool(service_key="drain_reject", max_size=2)
    pool.begin_drain()
    with pytest.raises(_PoolBusy):
        pool.acquire(src={"name": "drain_reject", "bucket": "b"}, max_wait=0.1)


def test_release_closes_when_draining():
    """A checked-out conn returned during drain is closed, not idled."""
    pool = _Pool(service_key="drain_release", max_size=2)
    pool._in_use = 1  # simulate one checked out, none idle
    pool.begin_drain()  # no idle to close; _in_use stays 1
    assert pool._in_use == 1

    con = _mock_conn()
    pool.release(con)  # should discard (close), not idle

    con.close.assert_called_once()
    assert pool._in_use == 0
    assert pool._idle.empty()


def test_wait_drained_reaches_zero_when_conn_returned():
    pool = _Pool(service_key="drain_wait", max_size=2)
    pool._in_use = 1
    pool.begin_drain()

    def returner():
        time.sleep(0.1)
        pool.release(_mock_conn())

    th = threading.Thread(target=returner)
    th.start()
    ok = pool.wait_drained(timeout=2.0)
    th.join(timeout=2.0)

    assert ok is True
    assert pool._in_use == 0


def test_wait_drained_times_out_without_deadlock():
    pool = _Pool(service_key="drain_timeout", max_size=2)
    pool._in_use = 1  # never released
    pool.begin_drain()

    result = {}

    def waiter():
        result["ok"] = pool.wait_drained(timeout=0.2)

    th = threading.Thread(target=waiter)
    th.start()
    th.join(timeout=2.0)

    assert not th.is_alive(), "wait_drained deadlocked"
    assert result["ok"] is False


def test_acquire_queues_during_drain_then_resumes_after_end_drain():
    """Near-zero-downtime contract: an acquire arriving during a recycle drain
    QUEUES (it must NOT 503) and resumes against a fresh build once end_drain
    runs. Pre-fix this raised _PoolBusy immediately, which is what turned every
    recycle into user-visible 503s and (with the too-short barrier cap) let opens
    leak back in so the instance never reached zero live connections."""
    from unittest.mock import patch

    pool = _Pool(service_key="drain_queue", max_size=1)
    pool.begin_drain()  # draining → a fresh acquire must queue, not reject

    outcome = {}
    fake = _mock_conn()

    def queued():
        with patch("backend.core.duckdb.get_connection", return_value=fake):
            try:
                outcome["got"] = pool.acquire(src={"name": "drain_queue", "bucket": "b"}, max_wait=5.0)
            except _PoolBusy:
                outcome["got"] = "poolbusy"

    th = threading.Thread(target=queued)
    th.start()
    time.sleep(0.2)  # let it park in the draining wait
    assert th.is_alive(), "acquire should QUEUE during drain, not return or reject"

    pool.end_drain()  # clears _draining + notify_all → waiter resumes, builds fresh
    th.join(timeout=2.0)

    assert not th.is_alive()
    assert outcome["got"] is fake


def test_acquire_during_drain_times_out_to_poolbusy_as_failsafe():
    """If a recycle overruns the caller's max_wait, a queued acquire falls back
    to _PoolBusy rather than blocking forever — fail-safe for a hung recycle."""
    pool = _Pool(service_key="drain_failsafe", max_size=2)
    pool.begin_drain()  # never ended → the queued acquire must time out
    with pytest.raises(_PoolBusy):
        pool.acquire(src={"name": "drain_failsafe", "bucket": "b"}, max_wait=0.1)


def test_end_drain_resumes_acquire():
    pool = _Pool(service_key="drain_resume", max_size=1)
    pool.begin_drain()
    pool.end_drain()
    assert pool._draining is False
    # A fresh acquire now proceeds to the build path (which we don't exercise
    # here) rather than raising _PoolBusy. Verify the gate is lifted by checking
    # the flag is clear and a build attempt would be made: patch get_connection.
    from unittest.mock import patch

    fake = _mock_conn()
    with patch("backend.core.duckdb.get_connection", return_value=fake):
        got = pool.acquire(src={"name": "drain_resume", "bucket": "b"}, max_wait=0.5)
        assert got is fake


def test_warm_idle_is_noop_while_draining():
    pool = _Pool(service_key="drain_warm", max_size=2)
    conns = [_mock_conn() for _ in range(2)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 2
    pool._draining = True

    pool.warm_idle(src={"name": "drain_warm", "bucket": "b"})

    # Idle queue untouched, no conn closed by warm_idle.
    assert pool._idle.qsize() == 2
    for c in conns:
        c.close.assert_not_called()


def test_module_drain_helpers_only_touch_existing_pools(monkeypatch):
    """begin/wait/end_drain_pools resolve only existing pools (never create)."""
    import backend.core.duckdb_pool as pool_mod

    pool = _Pool(service_key="grp_a", max_size=1)
    pool._in_use = 0
    monkeypatch.setitem(pool_mod._pools, "grp_a", pool)

    pool_mod.begin_drain_pools(["grp_a", "does_not_exist"])
    assert pool._draining is True
    assert pool_mod.wait_pools_drained(["grp_a", "does_not_exist"], timeout=1.0) is True
    pool_mod.end_drain_pools(["grp_a", "does_not_exist"])
    assert pool._draining is False
