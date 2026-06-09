import threading
from unittest.mock import MagicMock, patch

import duckdb

from backend.core.duckdb_pool import _Pool


def test_pool_does_not_deadlock_on_checkout_exception():
    """Verify that if an exception occurs during _prepare_checkout on a connection checkout,

    the pool does not deadlock itself and correctly discards the failed connection.
    """
    pool = _Pool(service_key="test_deadlock_service", max_size=2)

    # 1. Prepare a mock connection and put it into the idle pool
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    # 2. Mock iceberg's view cache and update_iceberg_view to raise an exception
    # so that _prepare_checkout fails and triggers the _discard path.
    with (
        patch("backend.core.iceberg._view_cache", {}),
        patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("Mock view rebind failed")),
    ):
        # 3. Call acquire. Since _prepare_checkout fails, it should discard the connection
        # and raise the exception, but it must NOT deadlock. We set a timeout to be safe.
        completed_without_deadlock = False
        try:
            # We run it with a timeout using threading to guarantee we don't hang the test suite if there's a deadlock
            def run_acquire():
                nonlocal completed_without_deadlock
                try:
                    pool.acquire(src={"name": "test_deadlock_service", "bucket": "b"}, max_wait=0.1)
                except RuntimeError as e:
                    if str(e) == "Mock view rebind failed":
                        completed_without_deadlock = True

            t = threading.Thread(target=run_acquire)
            t.start()
            t.join(timeout=2.0)

            assert not t.is_alive(), "The acquire call deadlocked!"
            assert completed_without_deadlock, "The acquire call did not raise the expected error"
        finally:
            # 4. Clean up
            try:
                mock_conn.close()
            except Exception:
                pass

    # Ensure that pool state has been correctly updated
    assert pool._in_use == 0
    assert pool._discarded_total == 1
    assert pool._idle.empty()
