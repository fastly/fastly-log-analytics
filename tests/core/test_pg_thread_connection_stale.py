"""A thread's cached connection must not outlive a close_all_pg_connections() issued from another thread."""

from __future__ import annotations

import threading
import time

from backend.core.metadata import pg_connection


def test_stale_thread_wrapper_is_replaced_after_close_all_from_another_thread():
    first_ready = threading.Event()
    closed = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        results["first"] = pg_connection.get_pg_thread_connection()
        first_ready.set()
        closed.wait(10)
        # Before the fix this returned the same wrapper, whose connection had been returned
        # to (and, in tests, the pool then closed under) the pool: PoolClosed / use-after-return.
        second = pg_connection.get_pg_thread_connection()
        results["second"] = second
        results["value"] = second.execute("SELECT 1").fetchone()[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert first_ready.wait(10)
    pg_connection.close_all_pg_connections()
    closed.set()
    t.join(15)

    assert results["second"] is not results["first"]
    assert results["value"] == 1


def test_close_all_leaves_a_connection_with_a_query_in_flight_to_its_owner():
    """Returning a busy connection from another thread freed it under the running query (worker segfault)."""
    started = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        con = pg_connection.get_pg_thread_connection()
        results["wrapper"] = con
        started.set()
        results["value"] = con.execute("SELECT 1 FROM pg_sleep(1)").fetchone()[0]
        results["returned_after_query"] = con._returned[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert started.wait(10)
    wrapper = results["wrapper"]
    assert isinstance(wrapper, pg_connection.PgConnectionWrapper)
    # Wait until the worker's statement is actually in flight (its busy lock is held).
    for _ in range(500):
        if not wrapper._busy.acquire(blocking=False):
            break
        wrapper._busy.release()
        time.sleep(0.01)
    else:
        raise AssertionError("worker never started its query")
    pg_connection.close_all_pg_connections()
    t.join(15)

    assert results["value"] == 1
    assert results["returned_after_query"] is False


def test_return_to_pool_once_is_atomic_under_contention():
    """Two threads racing to return the same connection must putconn it exactly once."""
    from unittest.mock import MagicMock

    pool = MagicMock()
    returned = [False]
    start = threading.Barrier(8)

    def go() -> None:
        start.wait(5)
        pg_connection._return_to_pool_once(object(), returned, pool)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert pool.putconn.call_count == 1
