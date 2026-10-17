"""A thread's cached connection must not outlive a close_all_pg_connections() issued from another thread."""

from __future__ import annotations

import threading

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
