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


# ── warm_idle ────────────────────────────────────────────────────────────────


def test_warm_idle_binds_every_idle_connection():
    """warm_idle calls _try_fast_path_view on each idle conn and returns it."""
    pool = _Pool(service_key="test_warm", max_size=3)
    conns = [MagicMock(spec=duckdb.DuckDBPyConnection) for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3

    with patch("backend.core.iceberg._try_fast_path_view", return_value=True) as mock_fp:
        pool.warm_idle(src={"name": "test_warm", "bucket": "b"})

    # Every idle conn was warmed
    assert mock_fp.call_count == 3
    warmed = {call.args[0] for call in mock_fp.call_args_list}
    assert warmed == set(conns)
    # Pool bookkeeping unchanged
    assert pool._in_use == 3
    assert pool._idle.qsize() == 3


def test_warm_idle_empty_pool_is_noop():
    """warm_idle on an empty pool returns immediately without error."""
    pool = _Pool(service_key="test_warm_empty", max_size=2)

    with patch("backend.core.iceberg._try_fast_path_view") as mock_fp:
        pool.warm_idle(src={"name": "test_warm_empty", "bucket": "b"})

    assert mock_fp.call_count == 0
    assert pool._in_use == 0
    assert pool._idle.qsize() == 0


def test_warm_idle_returns_conn_on_bind_failure():
    """If _try_fast_path_view raises, the conn goes back to idle unwarmed —
    next checkout will rebind via _prepare_checkout."""
    pool = _Pool(service_key="test_warm_fail", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    with patch("backend.core.iceberg._try_fast_path_view", side_effect=RuntimeError("bind boom")):
        pool.warm_idle(src={"name": "test_warm_fail", "bucket": "b"})

    # Connection is still in idle — not discarded
    assert pool._in_use == 1
    assert pool._idle.qsize() == 1
    mock_conn.close.assert_not_called()


def test_warm_idle_bounded_by_max_size():
    """warm_idle stops after max_size iterations even if conns keep returning."""
    pool = _Pool(service_key="test_warm_bound", max_size=2)
    conns = [MagicMock(spec=duckdb.DuckDBPyConnection) for _ in range(2)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 2

    with patch("backend.core.iceberg._try_fast_path_view", return_value=True) as mock_fp:
        pool.warm_idle(src={"name": "test_warm_bound", "bucket": "b"})

    # Hit max_size iterations exactly — no infinite loop
    assert mock_fp.call_count == 2


def test_warm_pool_for_service_noop_when_no_pool():
    """warm_pool_for_service is a no-op if no pool exists for the service."""
    from backend.core.duckdb_pool import _pools, _pools_lock, warm_pool_for_service

    # Make sure the service has no pool entry
    with _pools_lock:
        _pools.pop("nonexistent_service", None)

    # Should not raise
    warm_pool_for_service("nonexistent_service", {"name": "nonexistent_service"})
