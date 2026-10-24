from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core import duckdb_pool
from backend.core import duckdb_pool as _pool
from backend.core.duckdb_pool import (
    _Pool,
    _pool_heavy_cap,
    _PoolBusy,
    _set_conn_state,
    checkout_connection,
    is_heavy_route,
)

# ── From test_duckdb_pool.py ──


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


def test_pool_enabled_default_and_override(monkeypatch):
    """_pool_enabled defaults to True and respects falsy env values."""
    from backend.core.duckdb_pool import _pool_enabled

    monkeypatch.delenv("DUCKDB_CONNECTION_POOL", raising=False)
    assert _pool_enabled() is True
    for falsy in ("0", "false", "no", "off", "FALSE"):
        monkeypatch.setenv("DUCKDB_CONNECTION_POOL", falsy)
        assert _pool_enabled() is False
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    assert _pool_enabled() is True


def test_pool_max_size_parses_and_falls_back(monkeypatch):
    """_pool_max_size returns max(1, int(env)) and falls back to 12 on garbage."""
    from backend.core.duckdb_pool import _pool_max_size

    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "16")
    assert _pool_max_size() == 16
    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "0")
    assert _pool_max_size() == 1  # clamped to >= 1
    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "not-an-int")
    assert _pool_max_size() == 12  # fallback default


def test_pool_conn_memory_limit_passthrough(monkeypatch):
    """_pool_conn_memory_limit returns env value verbatim or None."""
    from backend.core.duckdb_pool import _pool_conn_memory_limit

    monkeypatch.delenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", raising=False)
    assert _pool_conn_memory_limit() is None
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "256MB")
    assert _pool_conn_memory_limit() == "256MB"


def test_pool_sweep_enabled_default_and_truthy(monkeypatch):
    """_pool_sweep_enabled is False by default; truthy env values enable it."""
    from backend.core.duckdb_pool import _pool_sweep_enabled

    monkeypatch.delenv("DUCKDB_POOL_SWEEP", raising=False)
    assert _pool_sweep_enabled() is False
    for truthy in ("1", "true", "yes", "on", "TRUE"):
        monkeypatch.setenv("DUCKDB_POOL_SWEEP", truthy)
        assert _pool_sweep_enabled() is True


def test_pool_warm_at_boot_enabled_default_and_truthy(monkeypatch):
    """_pool_warm_at_boot_enabled is False by default; truthy env enables.

    Pinned because the default has operational consequences — flipping it
    to True silently would add ``count * ~150 ms per service`` to every
    cold-start, surprising the operator. The conservative default mirrors
    DUCKDB_POOL_SWEEP."""
    from backend.core.duckdb_pool import _pool_warm_at_boot_enabled

    monkeypatch.delenv("DUCKDB_POOL_WARM_AT_BOOT", raising=False)
    assert _pool_warm_at_boot_enabled() is False
    for truthy in ("1", "true", "yes", "on", "TRUE"):
        monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT", truthy)
        assert _pool_warm_at_boot_enabled() is True


def test_pool_warm_at_boot_count_parses_and_falls_back(monkeypatch):
    """_pool_warm_at_boot_count returns max(1, int(env)) and falls back to
    4 on garbage. Default 4 matches the origin composite's footprint
    (ctx.con + 3 extras)."""
    from backend.core.duckdb_pool import _pool_warm_at_boot_count

    monkeypatch.delenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", raising=False)
    assert _pool_warm_at_boot_count() == 4
    monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", "8")
    assert _pool_warm_at_boot_count() == 8
    monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", "0")
    assert _pool_warm_at_boot_count() == 1  # clamped to >= 1
    monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", "garbage")
    assert _pool_warm_at_boot_count() == 4  # fallback default


def test_pool_api_rebind_lock_timeout_default_and_invalid(monkeypatch):
    """Default 500ms = 0.5s, garbage env falls back to 0.5s, valid env parses."""
    from backend.core.duckdb_pool import _pool_api_rebind_lock_timeout_s

    monkeypatch.delenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", raising=False)
    assert _pool_api_rebind_lock_timeout_s() == 0.5
    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "garbage")
    assert _pool_api_rebind_lock_timeout_s() == 0.5  # fallback
    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "-50")
    assert _pool_api_rebind_lock_timeout_s() == 0.0  # clamped to >= 0


def test_conn_state_register_lookup_forget():
    """_set_conn_state stores key/values, _get_conn_state retrieves with
    default fallback, _forget_conn clears the entry."""
    from backend.core.duckdb_pool import _forget_conn, _get_conn_state, _set_conn_state

    mock = MagicMock(spec=duckdb.DuckDBPyConnection)
    _set_conn_state(mock, service_key="svc", view_fingerprint=("a",))
    assert _get_conn_state(mock, "service_key") == "svc"
    assert _get_conn_state(mock, "view_fingerprint") == ("a",)
    assert _get_conn_state(mock, "missing", default="dflt") == "dflt"
    _forget_conn(mock)
    # After forget, returns default
    assert _get_conn_state(mock, "service_key", default=None) is None


def test_safe_buffer_mtime_returns_none_on_error():
    """_safe_buffer_mtime swallows exceptions from _buffer_dir / os.path."""
    from backend.core.duckdb_pool import _safe_buffer_mtime

    assert _safe_buffer_mtime(None) is None
    # src that triggers _buffer_dir to fail
    with patch("backend.core.iceberg._core._buffer_dir", side_effect=RuntimeError("boom")):
        assert _safe_buffer_mtime({"name": "x"}) is None


def test_safe_buffer_mtime_happy_path(tmp_path):
    """_safe_buffer_mtime returns the mtime when _buffer_dir resolves a real path."""
    from backend.core.duckdb_pool import _safe_buffer_mtime

    p = tmp_path / "buffer"
    p.mkdir()
    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(p)):
        mt = _safe_buffer_mtime({"name": "x"})
    assert isinstance(mt, float)


def test_prepare_checkout_cache_hit_skips_rebind():
    """When the stamped view fingerprint matches the current _view_cache
    entry AND buffer_mtime matches, _prepare_checkout returns without
    calling update_iceberg_view — the few-µs dict-lookup fast path."""
    from backend.core.duckdb_pool import _set_conn_state

    pool = _Pool(service_key="test_cache_hit", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    sentinel_tuple = ("v1",)
    # Pre-stamp the conn with the same identity tuple that view_cache returns
    _set_conn_state(mock_conn, view_fingerprint=sentinel_tuple, buffer_mtime=None)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_cache_hit": sentinel_tuple}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=None),
    ):
        result = pool.acquire(src={"name": "test_cache_hit"}, max_wait=0.5)

    assert result is mock_conn
    mock_uiv.assert_not_called()  # the fast path was taken
    assert pool._reused_total == 1


def test_acquire_skip_view_update_bypasses_prepare_checkout():
    """With ``skip_view_update=True`` an idle-conn reuse returns the conn
    directly — no fingerprint check, no update_iceberg_view call, no
    fingerprint re-stamp. This is the in-request "extras" fast path used
    by /api/origin/aggregates' parallel branches, where the caller has
    just validated a sibling ctx.con and the per-service view is known
    fresh for the duration of the request. Pinned so a future refactor
    can't reintroduce the rebind probe on extras (the entire point of
    the flag) without tripping CI."""
    from backend.core.duckdb_pool import _get_conn_state

    pool = _Pool(service_key="test_skip_view", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    # NOTE: deliberately stamp a DIFFERENT fingerprint than what view_cache
    # holds so the normal-path fast-cache would NOT fire — the only thing
    # that should keep update_iceberg_view from being called is the
    # skip_view_update flag itself.
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_skip_view": ("fresh",)}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=12345.0),
    ):
        result = pool.acquire(
            src={"name": "test_skip_view"},
            max_wait=0.5,
            skip_view_update=True,
        )

    assert result is mock_conn
    mock_uiv.assert_not_called()
    # No re-stamp either — the flag's contract is "trust me, leave the
    # conn alone". Stamped state remains whatever it was before.
    assert _get_conn_state(mock_conn, "view_fingerprint") is None
    assert pool._reused_total == 1


def test_acquire_skip_view_update_default_false_still_rebinds():
    """The new flag MUST default to False so existing callers keep the
    fingerprint-check + rebind safety. Pinned so a future signature
    tweak that flips the default doesn't silently strip the safety net
    from every checkout in the codebase."""
    pool = _Pool(service_key="test_skip_view_default", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    # Force a fingerprint mismatch so the rebind path fires when not skipped.
    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_skip_view_default": ("v1",)}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=None),
    ):
        pool.acquire(src={"name": "test_skip_view_default"}, max_wait=0.5)

    mock_uiv.assert_called_once()


def test_checkout_connection_passes_skip_view_update_through(monkeypatch):
    """The contextmanager must forward ``skip_view_update`` to pool.acquire
    so callers like /api/origin/aggregates' extras actually benefit. Pinned
    because the flag traverses two layers (contextmanager → _Pool.acquire)
    and a silent drop at the boundary would manifest as a perf regression,
    not a test failure on any of the existing checkout assertions."""
    from backend.core.duckdb_pool import _pools, _pools_lock, checkout_connection

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    key = "test_skip_view_passthrough"
    with _pools_lock:
        _pools.pop(key, None)

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
    ):
        # First checkout builds a fresh conn (fresh-build always binds the
        # view — flag is silently ignored on that branch by design).
        with checkout_connection(src={"name": key}, max_wait=1.0):
            pass
        baseline_calls = mock_uiv.call_count
        # Second checkout reuses the same idle conn — without skip would
        # hit _prepare_checkout's rebind probe; with skip must NOT.
        with checkout_connection(src={"name": key}, max_wait=1.0, skip_view_update=True):
            pass
        assert mock_uiv.call_count == baseline_calls, (
            "skip_view_update=True did not propagate from checkout_connection to "
            "pool.acquire — extras would still pay the rebind probe"
        )

    with _pools_lock:
        _pools.pop(key, None)


def test_fresh_build_applies_memory_limit(monkeypatch):
    """When the idle queue is empty and capacity is available, acquire()
    builds via get_connection and applies the optional SET memory_limit."""
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "512MB")
    pool = _Pool(service_key="test_fresh", max_size=2)

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=mock_conn),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        result = pool.acquire(src={"name": "test_fresh"}, max_wait=0.5)

    assert result is mock_conn
    # Resource limits applied before the connection enters the pool.
    executed = [c.args[0] for c in mock_conn.execute.call_args_list]
    assert any("memory_limit" in s and "512MB" in s for s in executed)
    assert pool._in_use == 1
    assert pool._created_total == 1


def test_fresh_build_swallows_pragma_errors(monkeypatch):
    """SET memory_limit failures are logged-and-swallowed —
    a slightly mis-applied pragma must not block a checkout."""
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "invalid-unit")
    pool = _Pool(service_key="test_fresh_err", max_size=1)

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.side_effect = RuntimeError("pragma rejected")
    with (
        patch("backend.core.duckdb.get_connection", return_value=mock_conn),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        result = pool.acquire(src={"name": "test_fresh_err"}, max_wait=0.5)
    # Still returns the connection despite the pragma failures
    assert result is mock_conn


def test_fresh_build_failure_decrements_in_use():
    """When get_connection raises, acquire() must decrement _in_use and
    notify so a waiter doesn't deadlock against a phantom in-use slot."""
    pool = _Pool(service_key="test_fresh_fail", max_size=1)

    with patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("conn build failed")):
        with pytest.raises(RuntimeError, match="conn build failed"):
            pool.acquire(src={"name": "test_fresh_fail"}, max_wait=0.1)

    assert pool._in_use == 0  # released the optimistically-incremented slot


def test_release_happy_path_returns_to_idle():
    """Non-errored release puts the conn back on the idle queue and leaves
    in_use unchanged (the slot is still accounted for; just idle now)."""
    pool = _Pool(service_key="test_release_happy", max_size=2)
    pool._in_use = 1
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    pool.release(mock_conn, errored=False)
    assert pool._idle.qsize() == 1
    assert pool._in_use == 1


def test_release_errored_discards_and_frees_slot():
    """errored=True takes the discard branch — closes the conn, drops
    state, decrements in_use, increments discarded_total."""
    pool = _Pool(service_key="test_release_err", max_size=2)
    pool._in_use = 2
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    pool.release(mock_conn, errored=True)
    assert pool._idle.qsize() == 0
    assert pool._in_use == 1
    assert pool._discarded_total == 1
    mock_conn.close.assert_called_once()


def test_release_when_idle_queue_full_closes_conn():
    """Defensive path: if _idle.put_nowait raises Full (shouldn't normally
    happen), release closes the conn and decrements in_use so the slot frees."""
    pool = _Pool(service_key="test_release_full", max_size=1)
    # Pre-fill the idle queue to force put_nowait -> Full on next release
    filler = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(filler)
    pool._in_use = 2  # over-allocated to exercise the close + decrement branch

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool.release(mock_conn, errored=False)

    # The release-time conn was closed (could not fit in queue) and slot freed
    mock_conn.close.assert_called_once()
    assert pool._in_use == 1


def test_release_sweep_runs_when_enabled(monkeypatch):
    """With DUCKDB_POOL_SWEEP=1, release queries DuckDB for temp tables and
    issues DROP for each. Errors in DROP are swallowed."""
    monkeypatch.setenv("DUCKDB_POOL_SWEEP", "1")
    pool = _Pool(service_key="test_sweep_on", max_size=2)
    pool._in_use = 1
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    # First execute returns a result with two leftover temp tables; subsequent
    # execute calls (the DROPs) get bare MagicMocks.
    sweep_result = MagicMock()
    sweep_result.fetchall.return_value = [("t_abc",), ("t_def",)]
    mock_conn.execute.return_value = sweep_result

    pool.release(mock_conn, errored=False)

    # Sweep SELECT + two DROPs
    sql_calls = [c.args[0] for c in mock_conn.execute.call_args_list]
    assert any("duckdb_tables" in s for s in sql_calls)
    assert any('DROP TABLE IF EXISTS "t_abc"' in s for s in sql_calls)
    assert any('DROP TABLE IF EXISTS "t_def"' in s for s in sql_calls)


def test_release_sweep_failure_does_not_block(monkeypatch):
    """A failing sweep query is logged and release proceeds normally."""
    monkeypatch.setenv("DUCKDB_POOL_SWEEP", "1")
    pool = _Pool(service_key="test_sweep_fail", max_size=2)
    pool._in_use = 1
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.execute.side_effect = RuntimeError("sweep blew up")

    pool.release(mock_conn, errored=False)
    # Conn still goes back to idle
    assert pool._idle.qsize() == 1


def test_discard_swallows_close_exception():
    """_discard must tolerate con.close() raising — the slot still frees."""
    pool = _Pool(service_key="test_discard_close", max_size=2)
    pool._in_use = 1
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.close.side_effect = RuntimeError("close exploded")

    pool._discard(mock_conn)
    assert pool._in_use == 0
    assert pool._discarded_total == 1


def test_get_pool_lazy_creation_and_reuse(monkeypatch):
    """_get_pool creates a pool on first call for a service_key, reuses on
    subsequent calls. Uses a unique key to avoid cross-test contamination."""
    from backend.core.duckdb_pool import _get_pool, _pools, _pools_lock

    key = "test_get_pool_unique_key_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    p1 = _get_pool(key, max_size=3)
    p2 = _get_pool(key, max_size=99)  # max_size only honored on first call
    assert p1 is p2
    assert p1.max_size == 3

    # Default max_size path (no argument)
    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "5")
    key2 = "test_get_pool_default_key"
    with _pools_lock:
        _pools.pop(key2, None)
    p3 = _get_pool(key2)
    assert p3.max_size == 5

    # Cleanup
    with _pools_lock:
        _pools.pop(key, None)
        _pools.pop(key2, None)


def test_checkout_connection_disabled_falls_back_to_legacy(monkeypatch):
    """When DUCKDB_CONNECTION_POOL=0, checkout_connection bypasses the pool
    and yields a fresh connection from get_connection, closing on exit."""
    from backend.core.duckdb_pool import checkout_connection

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with patch("backend.core.duckdb.get_connection", return_value=raw) as mock_get:
        with checkout_connection(src={"name": "test_disabled"}, max_wait=1.0) as con:
            assert con is not None  # may be wrapped or raw
        mock_get.assert_called_once()
    raw.close.assert_called_once()


def test_checkout_connection_pooled_happy_path(monkeypatch):
    """Pool-enabled path: acquire from the pool, yield wrapped conn, release
    on clean exit (errored=False)."""
    from backend.core.duckdb_pool import _pools, _pools_lock, checkout_connection

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    key = "test_checkout_pooled_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        with checkout_connection(src={"name": key}, max_wait=1.0) as con:
            assert con is not None

    # Pool should now be tracking the conn (back on idle)
    with _pools_lock:
        pool = _pools.get(key)
    assert pool is not None
    assert pool._idle.qsize() == 1

    # Cleanup
    with _pools_lock:
        _pools.pop(key, None)


def test_checkout_connection_exception_marks_errored(monkeypatch):
    """If the with-block raises, pool.release is called with errored=True
    (conn is discarded, not returned to idle)."""
    from backend.core.duckdb_pool import _pools, _pools_lock, checkout_connection

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    key = "test_checkout_errored_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        with pytest.raises(ValueError, match="boom"):
            with checkout_connection(src={"name": key}, max_wait=1.0):
                raise ValueError("boom")

    # Conn discarded, not idle
    with _pools_lock:
        pool = _pools.get(key)
    assert pool is not None
    assert pool._idle.qsize() == 0
    assert pool._discarded_total == 1

    with _pools_lock:
        _pools.pop(key, None)


def test_checkout_connection_interrupt_discards_connection(monkeypatch):
    """POOL-INT-03: a connection whose in-flight query was INTERRUPTED (an admin
    cancel calls ``con.interrupt()``, which makes the query raise
    ``duckdb.InterruptException``) must be DISCARDED on release — never silently
    returned to the pool, never double-returned.

    ``InterruptException`` is an ``Exception`` (NOT a ``BaseException`` like the
    client-cancel ``GeneratorExit`` that ``test_request_context`` covers), so
    checkout_connection's ``except Exception`` sets ``errored=True`` and
    release() takes the discard branch. Using the REAL
    ``duckdb.InterruptException`` — not a generic exception — is what makes this
    the interrupt-origin case rather than a duplicate of
    test_checkout_connection_exception_marks_errored: a refactor that special-
    cased the interrupt type into a BaseException-only catch (and so re-pooled a
    connection still carrying a half-cancelled query) would slip past the generic
    test but fail this one. ``_discarded_total == 1`` doubles as the
    no-double-return guard."""
    from backend.core.duckdb_pool import _pools, _pools_lock, checkout_connection

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    key = "test_checkout_interrupt_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        with pytest.raises(duckdb.InterruptException):
            with checkout_connection(src={"name": key}, max_wait=1.0):
                # An admin cancel interrupted the in-flight query; the
                # instrumentation proxy re-raises this to checkout_connection.
                raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")

    # Discarded exactly once (not re-pooled, not double-returned); the in-use
    # slot freed and the underlying connection closed by the discard.
    with _pools_lock:
        pool = _pools.get(key)
    assert pool is not None
    assert pool._idle.qsize() == 0, "an interrupted connection must NOT be re-pooled"
    assert pool._discarded_total == 1, "interrupted connection must be discarded exactly once"
    assert pool._in_use == 0, "the in-use slot must be freed"
    raw.close.assert_called_once()

    with _pools_lock:
        _pools.pop(key, None)


def test_instrument_returns_wrapper_on_success():
    """_instrument wraps the raw conn in InstrumentedDuckDBConnection."""
    from backend.core.duckdb_pool import _instrument

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    wrapped = _instrument(raw, service_key="svc")
    # The wrapper exposes the raw conn through delegation; identity may differ
    assert wrapped is not None


def test_instrument_falls_back_to_raw_on_error():
    """If InstrumentedDuckDBConnection construction raises, _instrument
    returns the raw conn unchanged — instrumentation must never block."""
    from backend.core.duckdb_pool import _instrument

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with patch(
        "backend.core.query_instrumentation.InstrumentedDuckDBConnection",
        side_effect=RuntimeError("wrap failed"),
    ):
        result = _instrument(raw, service_key="svc")
    assert result is raw


def test_warm_pool_for_service_calls_warm_idle_when_pool_exists():
    """When a pool exists for the service, warm_pool_for_service delegates
    to pool.warm_idle."""
    from backend.core.duckdb_pool import _pools, _pools_lock, warm_pool_for_service

    key = "test_warm_for_service_xyz"
    pool = _Pool(service_key=key, max_size=1)
    with _pools_lock:
        _pools[key] = pool

    with patch.object(pool, "warm_idle") as mock_warm:
        warm_pool_for_service(key, src={"name": key})
    mock_warm.assert_called_once()

    with _pools_lock:
        _pools.pop(key, None)


def test_warm_pool_at_startup_builds_count_idle_conns():
    """warm_pool_at_startup acquires ``count`` conns and releases them
    back, leaving ``count`` idle entries ready for the first request.
    Pinned so a future refactor that drops the release-back step won't
    silently break the warm-up promise (the conns would be checked-out
    forever and the first request would still cold-build)."""
    from backend.core.duckdb_pool import _pools, _pools_lock, warm_pool_at_startup

    key = "test_warm_at_startup_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        patch("backend.core.duckdb.get_connection", return_value=raw),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        built = warm_pool_at_startup(key, src={"name": key}, count=4)

    assert built == 4
    with _pools_lock:
        pool = _pools.get(key)
    assert pool is not None
    # All 4 conns built and returned to idle (in_use == count, qsize == count
    # because release() leaves the slot accounted for; that's the invariant
    # _Pool.acquire's saturation math depends on).
    assert pool._idle.qsize() == 4
    assert pool._created_total == 4

    with _pools_lock:
        _pools.pop(key, None)


def test_warm_pool_at_startup_clamped_by_max_size():
    """Requesting more conns than max_size yields max_size, never more.
    Excess would only burn build time without making it into idle (the
    pool's in_use cap stops new builds at max_size)."""
    from backend.core.duckdb_pool import _pools, _pools_lock, warm_pool_at_startup

    key = "test_warm_clamp_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    # Pre-create the pool with max_size=2 so warm_pool_at_startup gets the
    # bounded instance (subsequent _get_pool calls return the same pool).
    from backend.core.duckdb_pool import _get_pool

    _get_pool(key, max_size=2)

    with (
        patch("backend.core.duckdb.get_connection", return_value=raw),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        built = warm_pool_at_startup(key, src={"name": key}, count=10)

    assert built == 2  # clamped to max_size
    with _pools_lock:
        pool = _pools.get(key)
    assert pool is not None
    assert pool._idle.qsize() == 2

    with _pools_lock:
        _pools.pop(key, None)


def test_warm_pool_at_startup_swallows_per_conn_build_error():
    """A failing build during warm-up logs and returns ``built`` so far —
    a single bad service must not abort startup for the rest. Pinned
    because the scheduler's loop relies on this contract (it catches
    a top-level exception too, but per-conn swallow keeps partial
    progress visible in the log)."""
    from backend.core.duckdb_pool import _pools, _pools_lock, warm_pool_at_startup

    key = "test_warm_err_xyz"
    with _pools_lock:
        _pools.pop(key, None)

    call_count = {"n": 0}

    def _flaky_get(**_kwargs):
        call_count["n"] += 1
        if call_count["n"] >= 2:
            raise RuntimeError("simulated build failure")
        return MagicMock(spec=duckdb.DuckDBPyConnection)

    with (
        patch("backend.core.duckdb.get_connection", side_effect=_flaky_get),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        built = warm_pool_at_startup(key, src={"name": key}, count=4)

    # First conn built successfully; second raised, loop bailed.
    assert built == 1
    with _pools_lock:
        _pools.pop(key, None)


def test_get_all_stats_returns_per_pool_dicts():
    """get_all_stats returns a list of pool.stats() dicts, one per registered pool."""
    from backend.core.duckdb_pool import _pools, _pools_lock, get_all_stats

    key = "test_get_all_stats_xyz"
    pool = _Pool(service_key=key, max_size=2)
    with _pools_lock:
        _pools[key] = pool

    stats = get_all_stats()
    matching = [s for s in stats if s["service"] == key]
    assert len(matching) == 1
    s = matching[0]
    assert s["max_size"] == 2
    assert "wait" in s
    assert "rebind_wait" in s

    with _pools_lock:
        _pools.pop(key, None)


def test_shutdown_all_closes_idle_conns_and_clears_registry():
    """shutdown_all drains every pool's idle queue and clears the registry."""
    from backend.core.duckdb_pool import _pools, _pools_lock, shutdown_all

    key = "test_shutdown_all_xyz"
    pool = _Pool(service_key=key, max_size=2)
    mock_a = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_b = MagicMock(spec=duckdb.DuckDBPyConnection)
    # mock_b.close raises to exercise the swallow branch
    mock_b.close.side_effect = RuntimeError("close kaboom")
    pool._idle.put_nowait(mock_a)
    pool._idle.put_nowait(mock_b)
    with _pools_lock:
        _pools[key] = pool

    shutdown_all()

    mock_a.close.assert_called_once()
    mock_b.close.assert_called_once()
    with _pools_lock:
        assert key not in _pools


def test_stamp_fingerprint_swallows_exception():
    """If iceberg view import or lookup fails, _stamp_fingerprint sets None
    sentinels rather than propagating — must never break a checkout."""
    from backend.core.duckdb_pool import _get_conn_state

    pool = _Pool(service_key="test_stamp_err", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    # Force the inner _view_cache.get to raise to exercise the except branch
    bad_cache = MagicMock()
    bad_cache.get.side_effect = RuntimeError("cache blew up")
    with patch("backend.core.iceberg.view._view_cache", bad_cache):
        pool._stamp_fingerprint(mock_conn, src={"name": "test_stamp_err"})

    # State was written with None sentinels
    assert _get_conn_state(mock_conn, "view_fingerprint", default="NOT_SET") is None
    assert _get_conn_state(mock_conn, "buffer_mtime", default="NOT_SET") is None


def test_pool_exhaustion_one_waiter_raises_poolbusy():
    """With max_size=1 and a held connection, two more waiters compete for
    the slot via max_wait=0.05 — at least one must surface _PoolBusy. The
    other(s) may also time out (CI jitter); the only assertion is that
    NONE deadlock and at least one raises _PoolBusy."""
    from backend.core.duckdb_pool import _Pool, _PoolBusy

    pool = _Pool(service_key="test_exhaust", max_size=1)
    pool._in_use = 1  # hold the single slot

    results: list = []
    errors: list = []

    def _worker():
        try:
            pool.acquire(src={"name": "test_exhaust"}, max_wait=0.05)
            results.append("acquired")
        except _PoolBusy:
            errors.append("busy")
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=_worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2.0)
        assert not t.is_alive(), "worker deadlocked waiting for pool slot"

    # All three should have timed out as _PoolBusy since the slot is permanently held
    assert errors.count("busy") == 3
    assert results == []


def test_release_sweep_swallows_individual_drop_failures(monkeypatch):
    """When one DROP TABLE raises, the sweep logs and continues with the rest —
    a single stuck temp table can't poison the whole release path."""
    monkeypatch.setenv("DUCKDB_POOL_SWEEP", "1")
    pool = _Pool(service_key="test_sweep_drop_err", max_size=2)
    pool._in_use = 1
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    # First execute() returns rows; the next two are the DROPs (first raises, second OK)
    sweep_result = MagicMock()
    sweep_result.fetchall.return_value = [("t_bad",), ("t_ok",)]

    call_count = {"n": 0}

    def _execute(sql):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return sweep_result  # SELECT
        if call_count["n"] == 2:
            raise RuntimeError("can't drop t_bad")  # First DROP
        return MagicMock()  # Second DROP succeeds

    mock_conn.execute.side_effect = _execute
    pool.release(mock_conn, errored=False)
    # Conn returned to idle despite the per-DROP failure
    assert pool._idle.qsize() == 1


def test_release_close_swallowed_when_queue_full():
    """If put_nowait raises Full AND the subsequent close() raises, the
    pool still frees the slot — the close failure is swallowed."""
    pool = _Pool(service_key="test_release_close_err", max_size=1)
    filler = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(filler)
    pool._in_use = 2

    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_conn.close.side_effect = RuntimeError("close went bang")
    pool.release(mock_conn, errored=False)
    assert pool._in_use == 1  # slot freed even though close raised


def test_checkout_connection_disabled_path_swallows_close_error(monkeypatch):
    """Legacy disabled-pool path swallows raw_con.close() failures in
    finally — a poisoned close must not surface to the caller."""
    from backend.core.duckdb_pool import checkout_connection

    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    raw = MagicMock(spec=duckdb.DuckDBPyConnection)
    raw.close.side_effect = RuntimeError("close failed")
    with patch("backend.core.duckdb.get_connection", return_value=raw):
        # Must NOT raise on exit
        with checkout_connection(src={"name": "test_disabled_close"}, max_wait=1.0):
            pass
    raw.close.assert_called_once()


def test_percentile_summary_single_sample():
    """Single sample → all percentiles equal that sample."""
    import collections as _c

    from backend.core.duckdb_pool import _percentile_summary

    samples = _c.deque([42.5])
    summary = _percentile_summary(samples, threading.Lock())
    assert summary["count"] == 1
    assert summary["p50_ms"] == 42.5
    assert summary["p95_ms"] == 42.5
    assert summary["p99_ms"] == 42.5
    assert summary["max_ms"] == 42.5
    assert summary["mean_ms"] == 42.5


def test_pool_recovers_after_forced_discard():
    """After ``_prepare_checkout`` fails and ``_discard`` runs, a follow-up
    ``acquire`` with a working rebind must succeed against a freshly-built
    connection. Pinned because the discard path mutates ``_in_use`` and
    drains ``_idle`` — if recovery were broken the pool would silently
    refuse all subsequent checkouts.
    """
    pool = _Pool(service_key="test_recover_after_discard", max_size=2)

    # 1. Seed an idle connection; first acquire will see a rebind failure
    #    and discard it.
    bad_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(bad_conn)
    pool._in_use = 1

    with (
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=RuntimeError("rebind 1 boom")),
    ):
        with pytest.raises(RuntimeError, match="rebind 1 boom"):
            pool.acquire(src={"name": "test_recover_after_discard", "bucket": "b"}, max_wait=0.1)

    # Pool returned the failed conn's slot — _in_use back to 0, idle empty.
    assert pool._in_use == 0
    assert pool._idle.empty()
    assert pool._discarded_total == 1

    # 2. Next acquire must build a fresh connection and succeed.
    fresh_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    with (
        # Pool's fresh-build path calls backend.core.duckdb.get_connection().
        patch("backend.core.duckdb.get_connection", return_value=fresh_conn) as build_mock,
        patch("backend.core.iceberg.view.update_iceberg_view", return_value=None),
        patch("backend.core.iceberg.view._try_fast_path_view", return_value=True),
        # _set_conn_state + per-conn pragmas are setup overhead — short-
        # circuit them so the test doesn't depend on a real DuckDB session.
        patch("backend.core.duckdb_pool._set_conn_state"),
        patch("backend.core.duckdb_pool._pool_conn_memory_limit", return_value=None),
    ):
        got = pool.acquire(src={"name": "test_recover_after_discard", "bucket": "b"}, max_wait=0.5)

    assert got is fresh_conn, "second acquire must hand out the freshly-built connection"
    assert build_mock.call_count == 1, "fresh connection must be built post-discard"
    assert pool._in_use == 1
    # discarded counter still 1 — fresh acquire is not itself a discard.
    assert pool._discarded_total == 1


def test_prepare_checkout_rebinds_on_stale_view_fingerprint():
    """When the ``_view_cache`` tuple changes between checkouts (cron
    committed new data), ``_prepare_checkout`` must call
    ``update_iceberg_view`` to rebind the connection. The identity check
    (``current is stamped_view``) on the cache tuple is the fast path;
    this test verifies a different tuple triggers the rebind."""
    pool = _Pool(service_key="test_stale_fp", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    old_tuple = ("old_view_sql", "old_hash")
    new_tuple = ("new_view_sql", "new_hash")

    _set_conn_state(mock_conn, view_fingerprint=old_tuple, buffer_mtime=1.0)

    with (
        patch("backend.core.iceberg.view._view_cache", {"test_stale_fp": new_tuple}),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=1.0),
    ):
        got = pool.acquire(src={"name": "test_stale_fp", "bucket": "b"}, max_wait=0.5)

    assert got is mock_conn
    mock_uiv.assert_called_once()
    assert mock_uiv.call_args[0][0] is mock_conn


def test_prepare_checkout_skips_rebind_when_fingerprint_matches():
    """When the ``_view_cache`` tuple AND buffer mtime are identical to
    what was stamped at last checkout, ``update_iceberg_view`` is NOT
    called — the connection is returned as-is."""
    pool = _Pool(service_key="test_fresh_fp", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    same_tuple = ("same_sql", "same_hash")
    _set_conn_state(mock_conn, view_fingerprint=same_tuple, buffer_mtime=42.0)

    with (
        patch("backend.core.iceberg.view._view_cache", {"test_fresh_fp": same_tuple}),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=42.0),
    ):
        got = pool.acquire(src={"name": "test_fresh_fp", "bucket": "b"}, max_wait=0.5)

    assert got is mock_conn
    mock_uiv.assert_not_called()


def test_prepare_checkout_rebinds_on_stale_buffer_mtime():
    """When the ``_view_cache`` tuple is the same object but the buffer
    directory mtime changed (cron deleted buffer files), the rebind must
    still fire — the view SQL is the same but the files it references
    are gone."""
    pool = _Pool(service_key="test_stale_buf", max_size=1)
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    same_tuple = ("same_sql", "same_hash")
    _set_conn_state(mock_conn, view_fingerprint=same_tuple, buffer_mtime=100.0)

    with (
        patch("backend.core.iceberg.view._view_cache", {"test_stale_buf": same_tuple}),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=200.0),
    ):
        got = pool.acquire(src={"name": "test_stale_buf", "bucket": "b"}, max_wait=0.5)

    assert got is mock_conn
    mock_uiv.assert_called_once()


# ── From test_duckdb_pool_drain.py ──


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


# ── From test_duckdb_pool_heavy.py ──


def test_heavy_cap_leaves_one_connection_for_light_requests(monkeypatch):
    monkeypatch.delenv("DUCKDB_POOL_HEAVY_MAX", raising=False)
    assert _pool_heavy_cap(4) == 3
    assert _pool_heavy_cap(12) == 11
    assert _pool_heavy_cap(2) == 1
    assert _pool_heavy_cap(1) == 1  # nothing to reserve in a pool of one


def test_heavy_cap_env_override_is_clamped(monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_HEAVY_MAX", "2")
    assert _pool_heavy_cap(4) == 2
    monkeypatch.setenv("DUCKDB_POOL_HEAVY_MAX", "99")
    assert _pool_heavy_cap(4) == 4
    monkeypatch.setenv("DUCKDB_POOL_HEAVY_MAX", "0")
    assert _pool_heavy_cap(4) == 1
    monkeypatch.setenv("DUCKDB_POOL_HEAVY_MAX", "not-a-number")
    assert _pool_heavy_cap(4) == 3


@pytest.mark.parametrize(
    "route, expected",
    [
        ("/api/cmcd/aggregates", True),
        ("/api/security/aggregates", True),
        ("/api/network/bundle", True),
        ("/api/network-health", True),
        ("/api/insights", True),
        ("/api/origin/slow-urls", True),
        ("/api/cron-runs", False),
        ("/api/admin/share/banner", False),
        ("/api/web-vitals", False),
        ("", False),
        (None, False),
    ],
)
def test_is_heavy_route(route, expected, monkeypatch):
    monkeypatch.delenv("DUCKDB_POOL_HEAVY_ROUTES", raising=False)
    assert is_heavy_route(route) is expected


def test_heavy_routes_env_override(monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_HEAVY_ROUTES", "/custom, /other")
    assert is_heavy_route("/api/x/custom") is True
    assert is_heavy_route("/api/cmcd/aggregates") is False


def test_heavy_slots_cap_concurrency_and_release_wakes_a_waiter():
    pool = _Pool("heavy-cap", max_size=4)  # cap 3
    for _ in range(3):
        pool.acquire_heavy_slot(max_wait=1.0)

    got_slot = threading.Event()

    def fourth():
        pool.acquire_heavy_slot(max_wait=5.0)
        got_slot.set()

    t = threading.Thread(target=fourth)
    t.start()
    assert not got_slot.wait(0.2), "a 4th heavy request must wait while 3 hold slots"

    pool.release_heavy_slot()
    assert got_slot.wait(2.0), "releasing a slot must let the waiter in"
    t.join(2.0)
    assert pool.stats()["heavy"]["in_use"] == 3


def test_heavy_slot_timeout_raises_pool_busy_and_counts_a_reject():
    pool = _Pool("heavy-timeout", max_size=2)  # cap 1
    pool.acquire_heavy_slot(max_wait=1.0)
    with pytest.raises(_PoolBusy, match="heavy-request slots exhausted"):
        pool.acquire_heavy_slot(max_wait=0.05)
    assert pool.stats()["saturated_rejects_total"] == 1


def _pool_with_fake_connections(max_size: int) -> _Pool:
    pool = _Pool("heavy-checkout", max_size=max_size)
    pool.acquire = MagicMock(side_effect=lambda *a, **k: MagicMock())  # type: ignore[method-assign]
    pool.release = MagicMock()  # type: ignore[method-assign]
    return pool


def test_light_checkout_is_not_blocked_by_saturated_heavy_requests():
    pool = _pool_with_fake_connections(4)  # cap 3
    release_heavy = threading.Event()
    inside = threading.Barrier(4)  # 3 holders + this test thread

    def hold_heavy():
        with checkout_connection({"name": "heavy-checkout"}, max_wait=5.0, heavy=True):
            inside.wait(2.0)
            release_heavy.wait(5.0)

    fourth_done = threading.Event()

    def fourth_heavy():
        with checkout_connection({"name": "heavy-checkout"}, max_wait=5.0, heavy=True):
            fourth_done.set()

    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
    ):
        holders = [threading.Thread(target=hold_heavy) for _ in range(3)]
        for h in holders:
            h.start()
        inside.wait(2.0)  # all three heavy slots are held

        extra = threading.Thread(target=fourth_heavy)
        extra.start()
        assert not fourth_done.wait(0.2), "the 4th heavy request must queue"

        with checkout_connection({"name": "heavy-checkout"}, max_wait=0.5):  # light: must not wait
            pass

        release_heavy.set()
        assert fourth_done.wait(2.0)
        for t in (*holders, extra):
            t.join(2.0)

    assert pool.stats()["heavy"]["in_use"] == 0


def test_heavy_slot_is_released_when_the_connection_acquire_fails_or_the_body_raises():
    pool = _pool_with_fake_connections(2)  # cap 1
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
    ):
        pool.acquire = MagicMock(side_effect=_PoolBusy("saturated"))  # type: ignore[method-assign]
        with pytest.raises(_PoolBusy):
            with checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
                pass
        assert pool.stats()["heavy"]["in_use"] == 0

        pool.acquire = MagicMock(side_effect=lambda *a, **k: MagicMock())  # type: ignore[method-assign]
        with pytest.raises(RuntimeError):
            with checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
                raise RuntimeError("query failed")
        assert pool.stats()["heavy"]["in_use"] == 0
        pool.release.assert_called_once()
        assert pool.release.call_args.kwargs["errored"] is True

        # the single heavy slot is free again
        with checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
            pass


def test_stats_report_the_heavy_section():
    pool = _Pool("heavy-stats", max_size=4)
    pool.acquire_heavy_slot(max_wait=1.0)
    heavy = pool.stats()["heavy"]
    assert heavy["cap"] == 3
    assert heavy["in_use"] == 1
    assert heavy["wait"]["count"] == 1


def test_slow_checkout_is_logged_with_the_route_and_where_it_waited(caplog, monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "50")
    pool = _pool_with_fake_connections(4)

    def slow_acquire(*_a, **_k):
        time.sleep(0.12)
        return MagicMock()

    pool.acquire = MagicMock(side_effect=slow_acquire)  # type: ignore[method-assign]
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
        caplog.at_level("WARNING", logger="backend.core.duckdb_pool"),
    ):
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0, heavy=True, label="/api/cmcd/aggregates"):
            pass

    messages = [r.getMessage() for r in caplog.records if "slow checkout" in r.getMessage()]
    assert len(messages) == 1
    assert "route=/api/cmcd/aggregates" in messages[0]
    assert "heavy=True" in messages[0]
    assert "slot_wait_ms=0" in messages[0]
    assert "pool_wait_ms=1" in messages[0]  # 120 ms in acquire


def test_fast_checkout_is_not_logged(caplog, monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "500")
    pool = _pool_with_fake_connections(4)
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
        caplog.at_level("WARNING", logger="backend.core.duckdb_pool"),
    ):
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0, label="/api/cron-runs"):
            pass
    assert not [r for r in caplog.records if "slow checkout" in r.getMessage()]


def test_slow_checkout_logging_can_be_disabled(caplog, monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "0")
    pool = _pool_with_fake_connections(4)
    pool.acquire = MagicMock(side_effect=lambda *a, **k: (time.sleep(0.05), MagicMock())[1])  # type: ignore[method-assign]
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
        caplog.at_level("WARNING", logger="backend.core.duckdb_pool"),
    ):
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0):
            pass
    assert not [r for r in caplog.records if "slow checkout" in r.getMessage()]


# ── From test_duckdb_pool_reset.py ──


def test_discard_idle_closes_idle_and_zeroes_in_use():
    pool = _Pool(service_key="discard_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3

    closed = pool.discard_idle()

    assert closed == 3
    assert pool._in_use == 0
    assert pool._idle.empty()
    for c in conns:
        c.close.assert_called_once()
    # Unlike begin_drain, discard_idle must NOT enter draining mode — new
    # acquires keep flowing and build fresh conns with the new credentials.
    assert pool._draining is False


def test_discard_idle_leaves_checked_out_conns():
    """In-flight (checked-out) conns aren't touched — only idle ones close.
    One checked out (in_use=1, none idle) → nothing closed, in_use unchanged."""
    pool = _Pool(service_key="discard_inflight", max_size=2)
    pool._in_use = 1
    assert pool.discard_idle() == 0
    assert pool._in_use == 1


def test_reset_pool_for_service_noop_without_pool():
    assert _pool.reset_pool_for_service("never-queried-service-xyz") == 0


def test_reset_pool_for_service_closes_idle():
    pool = _pool._get_pool("reset-svc-xyz", max_size=2)
    conns = [_mock_conn() for _ in range(2)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 2
    try:
        closed = _pool.reset_pool_for_service("reset-svc-xyz")
        assert closed == 2
        assert pool._idle.empty()
        for c in conns:
            c.close.assert_called_once()
    finally:
        # Don't leak the test pool into the module-global registry.
        with _pool._pools_lock:
            _pool._pools.pop("reset-svc-xyz", None)


# ── From test_duckdb_pool_reuse_only.py ──


def test_reuse_only_rejects_when_no_idle_even_with_free_capacity():
    """idle empty + capacity available ⇒ reuse_only must reject, not build.

    This is the exact wedge condition: ``_in_use < max_size`` so the normal
    path would build fresh, but a reuse_only acquire must raise ``_PoolBusy``
    immediately without entering the build path or touching ``_in_use`` /
    ``_created_total``.
    """
    pool = _Pool(service_key="reuse_only_cap", max_size=4)
    assert pool._idle.empty()
    assert pool._in_use == 0

    with pytest.raises(_PoolBusy):
        pool.acquire(
            src={"name": "reuse_only_cap", "bucket": "b"},
            max_wait=0.2,
            reuse_only=True,
        )

    # Never built: counters untouched, no slot leaked.
    assert pool._in_use == 0
    assert pool._created_total == 0
    assert pool._idle.empty()


def test_reuse_only_rejects_immediately_without_blocking():
    """A saturated pool + reuse_only must not wait out ``max_wait``."""
    pool = _Pool(service_key="reuse_only_sat", max_size=1)
    pool._in_use = 1  # fully checked out, nothing idle

    done = threading.Event()

    def _try():
        with pytest.raises(_PoolBusy):
            # Large max_wait: if reuse_only honored, this returns at once;
            # if it fell through to the saturated wait it would block ~2s.
            pool.acquire(
                src={"name": "reuse_only_sat", "bucket": "b"},
                max_wait=2.0,
                reuse_only=True,
            )
        done.set()

    t = threading.Thread(target=_try)
    t.start()
    t.join(timeout=0.5)
    assert done.is_set(), "reuse_only acquire blocked instead of rejecting fast"
    # Reuse-only rejections are best-effort skips, not real saturation — they
    # must not inflate the saturated-reject counter the operator watches.
    assert pool._saturated_rejects_total == 0
