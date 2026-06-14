import threading
import time
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
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=RuntimeError("Mock view rebind failed")),
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

    with patch("backend.core.iceberg.view._try_fast_path_view", return_value=True) as mock_fp:
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

    with patch("backend.core.iceberg.view._try_fast_path_view") as mock_fp:
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

    with patch("backend.core.iceberg.view._try_fast_path_view", side_effect=RuntimeError("bind boom")):
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

    with patch("backend.core.iceberg.view._try_fast_path_view", return_value=True) as mock_fp:
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


# ── saturation + wait-stats telemetry ────────────────────────────────────────


def test_pool_saturation_raises_poolbusy_after_max_wait():
    """When every slot is in use, acquire() must time out at max_wait and
    raise _PoolBusy rather than waiting forever. Pinned because losing the
    deadline check would freeze every FastAPI worker behind whatever held
    the last connection (cron compact, slow query) — the symptom the
    Phase 6 telemetry sampling was added to detect."""
    from backend.core.duckdb_pool import _Pool, _PoolBusy

    pool = _Pool(service_key="test_saturation", max_size=1)
    pool._in_use = 1  # simulate the single slot being held by someone else

    t0 = time.monotonic()
    try:
        pool.acquire(src={"name": "test_saturation", "bucket": "b"}, max_wait=0.05)
    except _PoolBusy as e:
        elapsed = time.monotonic() - t0
        assert "saturated" in str(e)
        # Should have waited ~max_wait, with generous upper bound for CI jitter
        assert 0.04 <= elapsed < 0.5, f"acquire timed out at {elapsed:.3f}s, expected ~0.05s"
    else:
        raise AssertionError("acquire on saturated pool must raise _PoolBusy")

    # The timeout path also records a wait sample so admin UI percentiles
    # account for saturation events, not just successful checkouts.
    stats = pool.stats()
    assert stats["wait"]["count"] >= 1, "saturation timeout must record a wait sample"


def test_wait_stats_empty_buffer_returns_stable_zero_shape():
    """_wait_stats on an empty sample buffer must return the same key shape
    as a populated one, with zero values. Admin UI binds to these keys
    directly and a missing key would surface as ``undefined`` in the
    rendered percentile cells."""
    from backend.core.duckdb_pool import _Pool

    pool = _Pool(service_key="test_wait_empty", max_size=1)
    stats = pool._wait_stats()
    assert stats == {"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}


def test_wait_stats_percentiles_track_ring_contents():
    """Percentile keys must reflect the samples in the deque. Pinned
    because losing this would let a regression in _pct silently flatten
    p95 to p50 (or NaN out) — the admin UI would still render, just
    with wrong numbers, and ADR-03's cron-isolation decision is read
    directly off this output."""
    from backend.core.duckdb_pool import _Pool

    pool = _Pool(service_key="test_wait_populated", max_size=1)
    for sample_ms in [1.0, 2.0, 3.0, 4.0, 100.0]:
        pool._record_wait_sample(sample_ms)
    stats = pool._wait_stats()
    assert stats["count"] == 5
    assert stats["max_ms"] == 100.0
    # p95 of 5 samples (nearest-rank) lands at index round(.95*4)=4 → 100.0
    assert stats["p95_ms"] == 100.0
    # p50 of 5 samples → index 2 → 3.0
    assert stats["p50_ms"] == 3.0


# ── rebind-wait telemetry + short API lock timeout ───────────────────────────


def _checkout_idle_conn_with_rebind(pool: _Pool, mock_conn: MagicMock) -> dict:
    """Helper: put ``mock_conn`` idle, run acquire(), return the captured
    update_iceberg_view kwargs. Patches the iceberg view module so the
    rebind path runs without touching real DuckDB or S3."""
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    captured: dict = {}

    def _capture(con, src, **kwargs):
        captured["con"] = con
        captured["src"] = src
        captured["kwargs"] = kwargs

    with (
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=_capture) as mock_uiv,
    ):
        pool.acquire(src={"name": pool.service_key, "bucket": "b"}, max_wait=0.5)
        captured["call_count"] = mock_uiv.call_count
    return captured


def test_prepare_checkout_passes_short_lock_timeout_by_default():
    """API pool checkouts must pass a sub-second ``lock_timeout`` to
    update_iceberg_view so a leaked cron worker holding the per-service
    rebind RLock can't cascade pool waits into a 503 storm. Pinned because
    losing the short-timeout would re-open the failure-mode from the
    2026-06-14 incident where cron_sync exceeded the 300s hard cap and
    dashboard endpoints went 503 until the backend was restarted."""
    pool = _Pool(service_key="test_rebind_timeout", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    captured = _checkout_idle_conn_with_rebind(pool, mock_conn)
    assert captured["call_count"] == 1
    # 500ms is the default (matches _pool_api_rebind_lock_timeout_s); upper
    # bound here pins "sub-second" so any future tweak that pushes it
    # back over 1s trips the test before it ships.
    assert "lock_timeout" in captured["kwargs"], (
        "update_iceberg_view called without lock_timeout — pool would inherit "
        "the 5s default and re-open the 503-cascade window"
    )
    assert 0 < captured["kwargs"]["lock_timeout"] < 1.0


def test_prepare_checkout_lock_timeout_honors_env_override(monkeypatch):
    """DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS overrides the default. Lets
    operators dial up the timeout temporarily (e.g. during a cron-tuning
    push) without a redeploy."""
    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "1750")
    pool = _Pool(service_key="test_rebind_env", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    captured = _checkout_idle_conn_with_rebind(pool, mock_conn)
    assert captured["kwargs"]["lock_timeout"] == 1.75


def test_prepare_checkout_records_rebind_wait_sample():
    """Every checkout that traverses the rebind path records a sample so
    the admin pool-stats UI can attribute pool latency to "cron is
    holding the view lock" vs. just "no idle slot available"."""
    pool = _Pool(service_key="test_rebind_sample", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    _checkout_idle_conn_with_rebind(pool, mock_conn)
    stats = pool.stats()
    assert "rebind_wait" in stats, "stats() must expose rebind_wait alongside wait"
    assert stats["rebind_wait"]["count"] == 1
    assert stats["rebind_wait"]["max_ms"] >= 0.0


def test_rebind_wait_stats_empty_buffer_returns_stable_zero_shape():
    """Empty rebind_wait buffer returns the same key shape as wait so admin
    UI binds the same template to both panels."""
    pool = _Pool(service_key="test_rebind_empty", max_size=1)
    rebind = pool._rebind_wait_stats()
    assert rebind == {"count": 0, "p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0, "mean_ms": 0.0}


def test_rebind_wait_sample_failure_still_records_sample():
    """When the rebind raises, the sample is still recorded — operators
    need to see contention duration even when it ends in a discard, not
    only when it ends in a successful checkout."""
    pool = _Pool(service_key="test_rebind_fail", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    with (
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=RuntimeError("boom")),
    ):
        try:
            pool.acquire(src={"name": "test_rebind_fail", "bucket": "b"}, max_wait=0.1)
        except RuntimeError:
            pass

    stats = pool.stats()
    assert stats["rebind_wait"]["count"] == 1
