from __future__ import annotations

import collections
import threading
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core import duckdb_pool
from backend.core import duckdb_pool as _pool
from backend.core.duckdb_pool import (
    _forget_conn,
    _get_conn_state,
    _get_pool,
    _instrument,
    _percentile_summary,
    _Pool,
    _pool_api_rebind_lock_timeout_s,
    _pool_conn_memory_limit,
    _pool_enabled,
    _pool_heavy_cap,
    _pool_max_size,
    _pool_sweep_enabled,
    _pool_warm_at_boot_count,
    _pool_warm_at_boot_enabled,
    _PoolBusy,
    _pools,
    _pools_lock,
    _safe_buffer_mtime,
    _set_conn_state,
    checkout_connection,
    get_all_stats,
    is_heavy_route,
    shutdown_all,
    warm_pool_at_startup,
    warm_pool_for_service,
)


def _mock_conn() -> MagicMock:
    return MagicMock(spec=duckdb.DuckDBPyConnection)


@contextmanager
def _clean_pool_keys(*keys: str):
    with _pools_lock:
        for k in keys:
            _pools.pop(k, None)
    try:
        yield
    finally:
        with _pools_lock:
            for k in keys:
                _pools.pop(k, None)


def _checkout_idle_conn_with_rebind(pool: _Pool, mock_conn: MagicMock) -> dict:
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1
    captured: dict = {}

    def _capture(con, src, **kwargs):
        captured.update({"con": con, "src": src, "kwargs": kwargs})

    with (
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=_capture) as mock_uiv,
    ):
        pool.acquire(src={"name": pool.service_key, "bucket": "b"}, max_wait=0.5)
        captured["call_count"] = mock_uiv.call_count
    return captured


def _pool_with_fake_connections(max_size: int) -> _Pool:
    pool = _Pool("heavy-checkout", max_size=max_size)
    pool.acquire = MagicMock(side_effect=lambda *a, **k: MagicMock())  # type: ignore[method-assign]
    pool.release = MagicMock()  # type: ignore[method-assign]
    return pool


# ── Checkout deadlock & recovery ─────────────────────────────────────


def test_pool_deadlock_safety_and_recovery_on_checkout_exception():
    pool = _Pool(service_key="test_deadlock_service", max_size=2)
    mock_conn = _mock_conn()
    pool._idle.put_nowait(mock_conn)
    pool._in_use = 1

    with (
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=RuntimeError("Mock view rebind failed")),
    ):
        completed = False

        def run_acquire():
            nonlocal completed
            try:
                pool.acquire(src={"name": "test_deadlock_service", "bucket": "b"}, max_wait=0.1)
            except RuntimeError as e:
                if str(e) == "Mock view rebind failed":
                    completed = True

        t = threading.Thread(target=run_acquire)
        t.start()
        t.join(timeout=2.0)
        assert not t.is_alive() and completed

    assert (pool._in_use, pool._discarded_total, pool._idle.empty()) == (0, 1, True)

    # Recovery: next acquire builds a fresh connection and succeeds
    fresh_conn = _mock_conn()
    with (
        patch("backend.core.duckdb.get_connection", return_value=fresh_conn) as build_mock,
        patch("backend.core.iceberg.view.update_iceberg_view", return_value=None),
        patch("backend.core.iceberg.view._try_fast_path_view", return_value=True),
        patch("backend.core.duckdb_pool._set_conn_state"),
        patch("backend.core.duckdb_pool._pool_conn_memory_limit", return_value=None),
    ):
        got = pool.acquire(src={"name": "test_deadlock_service", "bucket": "b"}, max_wait=0.5)

    assert got is fresh_conn and build_mock.call_count == 1
    assert (pool._in_use, pool._discarded_total) == (1, 1)


# ── Warm idle & startup warming ──────────────────────────────────────


def test_warm_idle_and_warm_pool_for_service():
    # 1. Binds every idle connection bounded by max_size
    pool = _Pool(service_key="test_warm", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3

    with patch("backend.core.iceberg.view._try_fast_path_view", return_value=True) as mock_fp:
        pool.warm_idle(src={"name": "test_warm", "bucket": "b"})
    assert mock_fp.call_count == 3
    assert {call.args[0] for call in mock_fp.call_args_list} == set(conns)
    assert (pool._in_use, pool._idle.qsize()) == (3, 3)

    # 2. Empty pool is noop
    empty_pool = _Pool(service_key="test_warm_empty", max_size=2)
    with patch("backend.core.iceberg.view._try_fast_path_view") as mock_fp2:
        empty_pool.warm_idle(src={"name": "test_warm_empty", "bucket": "b"})
    assert (mock_fp2.call_count, empty_pool._in_use, empty_pool._idle.qsize()) == (0, 0, 0)

    # 3. Bind failure returns conn to idle without closing
    fail_pool = _Pool(service_key="test_warm_fail", max_size=1)
    mc = _mock_conn()
    fail_pool._idle.put_nowait(mc)
    fail_pool._in_use = 1
    with patch("backend.core.iceberg.view._try_fast_path_view", side_effect=RuntimeError("bind boom")):
        fail_pool.warm_idle(src={"name": "test_warm_fail", "bucket": "b"})
    assert (fail_pool._in_use, fail_pool._idle.qsize()) == (1, 1)
    mc.close.assert_not_called()

    # 4. warm_pool_for_service noop when absent, delegates when present
    key = "test_warm_for_service_xyz"
    with _clean_pool_keys("nonexistent_service", key):
        warm_pool_for_service("nonexistent_service", {"name": "nonexistent_service"})
        with _pools_lock:
            _pools[key] = fail_pool
        with patch.object(fail_pool, "warm_idle") as mock_warm:
            warm_pool_for_service(key, src={"name": key})
        mock_warm.assert_called_once()


def test_warm_pool_at_startup_builds_clamps_and_swallows_errors():
    k1, k2, k3 = "test_warm_startup_1", "test_warm_startup_2", "test_warm_startup_3"
    with _clean_pool_keys(k1, k2, k3):
        raw = _mock_conn()
        with (
            patch("backend.core.duckdb.get_connection", return_value=raw),
            patch("backend.core.iceberg.view._view_cache", {}),
            patch("backend.core.iceberg.view.update_iceberg_view"),
        ):
            assert warm_pool_at_startup(k1, src={"name": k1}, count=4) == 4
            _get_pool(k2, max_size=2)
            assert warm_pool_at_startup(k2, src={"name": k2}, count=10) == 2

        with _pools_lock:
            p1, p2 = _pools[k1], _pools[k2]
        assert (p1._idle.qsize(), p1._created_total) == (4, 4)
        assert p2._idle.qsize() == 2

        call_count = {"n": 0}

        def _flaky_get(**_kw):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise RuntimeError("simulated build failure")
            return _mock_conn()

        with (
            patch("backend.core.duckdb.get_connection", side_effect=_flaky_get),
            patch("backend.core.iceberg.view._view_cache", {}),
            patch("backend.core.iceberg.view.update_iceberg_view"),
        ):
            assert warm_pool_at_startup(k3, src={"name": k3}, count=4) == 1


# ── Saturation, wait stats & percentiles ─────────────────────────────


def test_pool_saturation_and_wait_stats():
    pool = _Pool(service_key="test_saturation", max_size=1)
    assert pool._wait_stats() == {
        "count": 0,
        "p50_ms": 0.0,
        "p95_ms": 0.0,
        "p99_ms": 0.0,
        "max_ms": 0.0,
        "mean_ms": 0.0,
    }
    assert pool._rebind_wait_stats() == {
        "count": 0,
        "p50_ms": 0.0,
        "p95_ms": 0.0,
        "p99_ms": 0.0,
        "max_ms": 0.0,
        "mean_ms": 0.0,
    }

    pool._in_use = 1
    t0 = time.monotonic()
    with pytest.raises(_PoolBusy, match="saturated"):
        pool.acquire(src={"name": "test_saturation", "bucket": "b"}, max_wait=0.05)
    assert 0.04 <= (time.monotonic() - t0) < 0.5
    assert pool.stats()["wait"]["count"] >= 1

    # Multiple concurrent waiters all time out with _PoolBusy without deadlock
    errors: list = []

    def _worker():
        try:
            pool.acquire(src={"name": "test_saturation"}, max_wait=0.05)
        except _PoolBusy:
            errors.append("busy")

    threads = [threading.Thread(target=_worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2.0)
        assert not t.is_alive()
    assert errors == ["busy", "busy", "busy"]

    # Percentile ring tracking + single sample summary
    pop_pool = _Pool(service_key="test_wait_populated", max_size=1)
    for sample_ms in [1.0, 2.0, 3.0, 4.0, 100.0]:
        pop_pool._record_wait_sample(sample_ms)
    st = pop_pool._wait_stats()
    assert (st["count"], st["p50_ms"], st["p95_ms"], st["max_ms"]) == (5, 3.0, 100.0, 100.0)

    single = _percentile_summary(collections.deque([42.5]), threading.Lock())
    assert single == {"count": 1, "p50_ms": 42.5, "p95_ms": 42.5, "p99_ms": 42.5, "max_ms": 42.5, "mean_ms": 42.5}


# ── _prepare_checkout, fingerprints & skip_view_update ───────────────


def test_prepare_checkout_lock_timeout_and_rebind_wait_samples(monkeypatch):
    pool = _Pool(service_key="test_rebind_timeout", max_size=1)
    captured = _checkout_idle_conn_with_rebind(pool, _mock_conn())
    assert captured["call_count"] == 1
    assert 0 < captured["kwargs"]["lock_timeout"] < 1.0
    st = pool.stats()
    assert st["rebind_wait"]["count"] == 1 and st["rebind_wait"]["max_ms"] >= 0.0

    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "1750")
    pool_env = _Pool(service_key="test_rebind_env", max_size=1)
    assert _checkout_idle_conn_with_rebind(pool_env, _mock_conn())["kwargs"]["lock_timeout"] == 1.75

    # Rebind failure still records rebind_wait sample
    pool_fail = _Pool(service_key="test_rebind_fail", max_size=1)
    pool_fail._idle.put_nowait(_mock_conn())
    pool_fail._in_use = 1
    with (
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError),
    ):
        pool_fail.acquire(src={"name": "test_rebind_fail", "bucket": "b"}, max_wait=0.1)
    assert pool_fail.stats()["rebind_wait"]["count"] == 1


def test_prepare_checkout_fingerprints_and_skip_view_update(monkeypatch):
    # 1. Matching view tuple + buffer mtime skips rebind
    pool = _Pool(service_key="test_fp", max_size=1)
    mc = _mock_conn()
    same_tuple = ("same_sql", "same_hash")
    _set_conn_state(mc, view_fingerprint=same_tuple, buffer_mtime=42.0)
    pool._idle.put_nowait(mc)
    pool._in_use = 1
    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_fp": same_tuple}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=42.0),
    ):
        assert pool.acquire(src={"name": "test_fp", "bucket": "b"}, max_wait=0.5) is mc
    mock_uiv.assert_not_called()
    assert pool._reused_total == 1

    # 2. Stale view fingerprint triggers rebind
    pool._idle.put_nowait(mc)
    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_fp": ("new_sql", "new_hash")}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv2,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=42.0),
    ):
        assert pool.acquire(src={"name": "test_fp", "bucket": "b"}, max_wait=0.5) is mc
    mock_uiv2.assert_called_once()

    # 3. Stale buffer mtime triggers rebind even when view tuple matches
    _set_conn_state(mc, view_fingerprint=same_tuple, buffer_mtime=100.0)
    pool._idle.put_nowait(mc)
    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_fp": same_tuple}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv3,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=200.0),
    ):
        assert pool.acquire(src={"name": "test_fp", "bucket": "b"}, max_wait=0.5) is mc
    mock_uiv3.assert_called_once()

    # 4. skip_view_update=True bypasses _prepare_checkout; default False still rebinds
    pool_skip = _Pool(service_key="test_skip_view", max_size=1)
    mc_skip = _mock_conn()
    pool_skip._idle.put_nowait(mc_skip)
    pool_skip._in_use = 1
    with (
        patch.dict("backend.core.iceberg.view._view_cache", {"test_skip_view": ("fresh",)}, clear=False),
        patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv_skip,
        patch("backend.core.duckdb_pool._safe_buffer_mtime", return_value=12345.0),
    ):
        assert pool_skip.acquire(src={"name": "test_skip_view"}, max_wait=0.5, skip_view_update=True) is mc_skip
        mock_uiv_skip.assert_not_called()
        assert _get_conn_state(mc_skip, "view_fingerprint") is None
        pool_skip._idle.put_nowait(mc_skip)
        pool_skip.acquire(src={"name": "test_skip_view"}, max_wait=0.5)
        mock_uiv_skip.assert_called_once()

    # 5. checkout_connection forwards skip_view_update
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    key = "test_skip_view_passthrough"
    with _clean_pool_keys(key):
        with (
            patch("backend.core.duckdb.get_connection", return_value=_mock_conn()),
            patch("backend.core.iceberg.view._view_cache", {}),
            patch("backend.core.iceberg.view.update_iceberg_view") as mock_uiv_pass,
        ):
            with checkout_connection(src={"name": key}, max_wait=1.0):
                pass
            baseline = mock_uiv_pass.call_count
            with checkout_connection(src={"name": key}, max_wait=1.0, skip_view_update=True):
                pass
            assert mock_uiv_pass.call_count == baseline


# ── Env parsers & state helpers ──────────────────────────────────────


def test_env_parsers_and_conn_state_helpers(monkeypatch, tmp_path):
    monkeypatch.delenv("DUCKDB_CONNECTION_POOL", raising=False)
    assert _pool_enabled() is True
    for falsy in ("0", "false", "no", "off", "FALSE"):
        monkeypatch.setenv("DUCKDB_CONNECTION_POOL", falsy)
        assert _pool_enabled() is False
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    assert _pool_enabled() is True

    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "16")
    assert _pool_max_size() == 16
    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "0")
    assert _pool_max_size() == 1
    monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "not-an-int")
    assert _pool_max_size() == 12

    monkeypatch.delenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", raising=False)
    assert _pool_conn_memory_limit() is None
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "256MB")
    assert _pool_conn_memory_limit() == "256MB"

    for env_var, fn in (
        ("DUCKDB_POOL_SWEEP", _pool_sweep_enabled),
        ("DUCKDB_POOL_WARM_AT_BOOT", _pool_warm_at_boot_enabled),
    ):
        monkeypatch.delenv(env_var, raising=False)
        assert fn() is False
        for truthy in ("1", "true", "yes", "on", "TRUE"):
            monkeypatch.setenv(env_var, truthy)
            assert fn() is True

    monkeypatch.delenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", raising=False)
    assert _pool_warm_at_boot_count() == 4
    monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", "8")
    assert _pool_warm_at_boot_count() == 8
    monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", "0")
    assert _pool_warm_at_boot_count() == 1
    monkeypatch.setenv("DUCKDB_POOL_WARM_AT_BOOT_COUNT", "garbage")
    assert _pool_warm_at_boot_count() == 4

    monkeypatch.delenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", raising=False)
    assert _pool_api_rebind_lock_timeout_s() == 0.5
    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "garbage")
    assert _pool_api_rebind_lock_timeout_s() == 0.5
    monkeypatch.setenv("DUCKDB_POOL_API_REBIND_LOCK_TIMEOUT_MS", "-50")
    assert _pool_api_rebind_lock_timeout_s() == 0.0

    # _set_conn_state / _get_conn_state / _forget_conn
    mc = _mock_conn()
    _set_conn_state(mc, service_key="svc", view_fingerprint=("a",))
    assert _get_conn_state(mc, "service_key") == "svc"
    assert _get_conn_state(mc, "view_fingerprint") == ("a",)
    assert _get_conn_state(mc, "missing", default="dflt") == "dflt"
    _forget_conn(mc)
    assert _get_conn_state(mc, "service_key", default=None) is None

    # _safe_buffer_mtime
    assert _safe_buffer_mtime(None) is None
    with patch("backend.core.iceberg._core._buffer_dir", side_effect=RuntimeError("boom")):
        assert _safe_buffer_mtime({"name": "x"}) is None
    p = tmp_path / "buffer"
    p.mkdir()
    with patch("backend.core.iceberg._core._buffer_dir", return_value=str(p)):
        assert isinstance(_safe_buffer_mtime({"name": "x"}), float)

    # _stamp_fingerprint swallows errors
    pool = _Pool(service_key="test_stamp_err", max_size=1)
    bad_cache = MagicMock()
    bad_cache.get.side_effect = RuntimeError("cache blew up")
    with patch("backend.core.iceberg.view._view_cache", bad_cache):
        pool._stamp_fingerprint(mc, src={"name": "test_stamp_err"})
    assert _get_conn_state(mc, "view_fingerprint", default="NOT_SET") is None
    assert _get_conn_state(mc, "buffer_mtime", default="NOT_SET") is None

    # _instrument wrapper & fallback
    assert _instrument(mc, service_key="svc") is not None
    with patch(
        "backend.core.query_instrumentation.InstrumentedDuckDBConnection", side_effect=RuntimeError("wrap failed")
    ):
        assert _instrument(mc, service_key="svc") is mc


# ── Fresh build, release, sweep & registry ───────────────────────────


def test_fresh_build_memory_limit_pragma_and_failure(monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_CONN_MEMORY_LIMIT", "512MB")
    pool = _Pool(service_key="test_fresh", max_size=2)
    mc = _mock_conn()
    with (
        patch("backend.core.duckdb.get_connection", return_value=mc),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        assert pool.acquire(src={"name": "test_fresh"}, max_wait=0.5) is mc
    assert any("memory_limit" in c.args[0] and "512MB" in c.args[0] for c in mc.execute.call_args_list)
    assert (pool._in_use, pool._created_total) == (1, 1)

    # Pragma error swallowed
    mc_err = _mock_conn()
    mc_err.execute.side_effect = RuntimeError("pragma rejected")
    pool_err = _Pool(service_key="test_fresh_err", max_size=1)
    with (
        patch("backend.core.duckdb.get_connection", return_value=mc_err),
        patch("backend.core.iceberg.view._view_cache", {}),
        patch("backend.core.iceberg.view.update_iceberg_view"),
    ):
        assert pool_err.acquire(src={"name": "test_fresh_err"}, max_wait=0.5) is mc_err

    # Build failure decrements _in_use
    pool_fail = _Pool(service_key="test_fresh_fail", max_size=1)
    with (
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("conn build failed")),
        pytest.raises(RuntimeError, match="conn build failed"),
    ):
        pool_fail.acquire(src={"name": "test_fresh_fail"}, max_wait=0.1)
    assert pool_fail._in_use == 0


def test_release_discard_and_sweep_branches(monkeypatch):
    # Happy release vs errored release
    pool = _Pool(service_key="test_rel", max_size=2)
    pool._in_use = 2
    c1, c2 = _mock_conn(), _mock_conn()
    pool.release(c1, errored=False)
    assert (pool._idle.qsize(), pool._in_use) == (1, 2)
    pool.release(c2, errored=True)
    assert (pool._idle.qsize(), pool._in_use, pool._discarded_total) == (1, 1, 1)
    c2.close.assert_called_once()

    # _discard swallows close error
    c_boom = _mock_conn()
    c_boom.close.side_effect = RuntimeError("close exploded")
    pool._discard(c_boom)
    assert (pool._in_use, pool._discarded_total) == (0, 2)

    # Full idle queue closes conn and swallows close error
    full_pool = _Pool(service_key="test_full", max_size=1)
    full_pool._idle.put_nowait(_mock_conn())
    full_pool._in_use = 3
    c_extra1, c_extra2 = _mock_conn(), _mock_conn()
    c_extra2.close.side_effect = RuntimeError("close went bang")
    full_pool.release(c_extra1, errored=False)
    c_extra1.close.assert_called_once()
    assert full_pool._in_use == 2
    full_pool.release(c_extra2, errored=False)
    assert full_pool._in_use == 1

    # Sweep enabled: runs DROPs, swallows per-DROP failure and SELECT failure
    monkeypatch.setenv("DUCKDB_POOL_SWEEP", "1")
    sweep_pool = _Pool(service_key="test_sweep", max_size=3)
    sweep_pool._in_use = 3

    c_sweep = _mock_conn()
    res = MagicMock()
    res.fetchall.return_value = [("t_abc",), ("t_def",)]
    c_sweep.execute.return_value = res
    sweep_pool.release(c_sweep, errored=False)
    sql_calls = [c.args[0] for c in c_sweep.execute.call_args_list]
    assert any("duckdb_tables" in s for s in sql_calls)
    assert any('DROP TABLE IF EXISTS "t_abc"' in s for s in sql_calls)
    assert any('DROP TABLE IF EXISTS "t_def"' in s for s in sql_calls)

    c_drop_err = _mock_conn()
    calls = {"n": 0}

    def _exec_drop_err(_sql):
        calls["n"] += 1
        if calls["n"] == 1:
            return res
        if calls["n"] == 2:
            raise RuntimeError("can't drop")
        return MagicMock()

    c_drop_err.execute.side_effect = _exec_drop_err
    sweep_pool.release(c_drop_err, errored=False)
    assert sweep_pool._idle.qsize() == 2

    c_sel_err = _mock_conn()
    c_sel_err.execute.side_effect = RuntimeError("sweep blew up")
    sweep_pool.release(c_sel_err, errored=False)
    assert sweep_pool._idle.qsize() == 3


def test_registry_get_pool_stats_and_shutdown(monkeypatch):
    k1, k2 = "test_get_pool_1", "test_get_pool_2"
    with _clean_pool_keys(k1, k2):
        p1 = _get_pool(k1, max_size=3)
        assert _get_pool(k1, max_size=99) is p1 and p1.max_size == 3
        monkeypatch.setenv("DUCKDB_POOL_MAX_SIZE", "5")
        assert _get_pool(k2).max_size == 5

        matching = [s for s in get_all_stats() if s["service"] == k1]
        assert len(matching) == 1 and matching[0]["max_size"] == 3
        assert "wait" in matching[0] and "rebind_wait" in matching[0]

        ma, mb = _mock_conn(), _mock_conn()
        mb.close.side_effect = RuntimeError("close kaboom")
        p1._idle.put_nowait(ma)
        p1._idle.put_nowait(mb)
        shutdown_all()
        ma.close.assert_called_once()
        mb.close.assert_called_once()
        with _pools_lock:
            assert k1 not in _pools


def test_checkout_connection_disabled_pooled_errored_and_interrupted(monkeypatch):
    # Disabled legacy path + close error swallow
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    raw = _mock_conn()
    with patch("backend.core.duckdb.get_connection", return_value=raw) as mock_get:
        with checkout_connection(src={"name": "test_disabled"}, max_wait=1.0) as con:
            assert con is not None
        mock_get.assert_called_once()
    raw.close.assert_called_once()

    raw_err = _mock_conn()
    raw_err.close.side_effect = RuntimeError("close failed")
    with patch("backend.core.duckdb.get_connection", return_value=raw_err):
        with checkout_connection(src={"name": "test_disabled_close"}, max_wait=1.0):
            pass
    raw_err.close.assert_called_once()

    # Pooled happy path, generic exception discard, and duckdb.InterruptException discard
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "1")
    k_ok, k_err, k_int = "test_co_ok", "test_co_err", "test_co_int"
    with _clean_pool_keys(k_ok, k_err, k_int):
        raw_int = _mock_conn()
        with (
            patch("backend.core.duckdb.get_connection", side_effect=[_mock_conn(), _mock_conn(), raw_int]),
            patch("backend.core.iceberg.view._view_cache", {}),
            patch("backend.core.iceberg.view.update_iceberg_view"),
        ):
            with checkout_connection(src={"name": k_ok}, max_wait=1.0) as con:
                assert con is not None
            with pytest.raises(ValueError, match="boom"):
                with checkout_connection(src={"name": k_err}, max_wait=1.0):
                    raise ValueError("boom")
            with pytest.raises(duckdb.InterruptException):
                with checkout_connection(src={"name": k_int}, max_wait=1.0):
                    raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")

        with _pools_lock:
            p_ok, p_err, p_int = _pools[k_ok], _pools[k_err], _pools[k_int]
        assert p_ok._idle.qsize() == 1
        assert (p_err._idle.qsize(), p_err._discarded_total) == (0, 1)
        assert (p_int._idle.qsize(), p_int._discarded_total, p_int._in_use) == (0, 1, 0)
        raw_int.close.assert_called_once()


# ── Drain lifecycle ──────────────────────────────────────────────────


def test_pool_drain_lifecycle_and_module_helpers(monkeypatch):
    # begin_drain closes idle and zeroes in_use
    pool = _Pool(service_key="drain_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3
    pool.begin_drain()
    assert (pool._draining, pool._in_use, pool._idle.empty()) == (True, 0, True)
    for c in conns:
        c.close.assert_called_once()

    # acquire during drain times out to _PoolBusy if drain doesn't end
    with pytest.raises(_PoolBusy):
        pool.acquire(src={"name": "drain_idle", "bucket": "b"}, max_wait=0.05)

    # release during drain closes conn instead of idling
    pool_rel = _Pool(service_key="drain_release", max_size=2)
    pool_rel._in_use = 1
    pool_rel.begin_drain()
    c_rel = _mock_conn()
    pool_rel.release(c_rel)
    c_rel.close.assert_called_once()
    assert (pool_rel._in_use, pool_rel._idle.empty()) == (0, True)

    # wait_drained reaches zero when returned, or times out without deadlock
    pool_wait = _Pool(service_key="drain_wait", max_size=2)
    pool_wait._in_use = 1
    pool_wait.begin_drain()
    assert pool_wait.wait_drained(timeout=0.05) is False
    threading.Thread(target=lambda: (time.sleep(0.05), pool_wait.release(_mock_conn()))).start()
    assert pool_wait.wait_drained(timeout=2.0) is True and pool_wait._in_use == 0

    # acquire queues during drain then resumes after end_drain
    pool_q = _Pool(service_key="drain_queue", max_size=1)
    pool_q.begin_drain()
    outcome: dict = {}
    fake = _mock_conn()

    def _queued():
        with patch("backend.core.duckdb.get_connection", return_value=fake):
            outcome["got"] = pool_q.acquire(src={"name": "drain_queue", "bucket": "b"}, max_wait=5.0)

    th = threading.Thread(target=_queued)
    th.start()
    time.sleep(0.1)
    assert th.is_alive()
    pool_q.end_drain()
    th.join(timeout=2.0)
    assert not th.is_alive() and outcome["got"] is fake

    # warm_idle is noop while draining
    pool_w = _Pool(service_key="drain_warm", max_size=2)
    wc = _mock_conn()
    pool_w._idle.put_nowait(wc)
    pool_w._in_use = 1
    pool_w._draining = True
    pool_w.warm_idle(src={"name": "drain_warm", "bucket": "b"})
    assert pool_w._idle.qsize() == 1
    wc.close.assert_not_called()

    # Module drain helpers only touch existing pools
    grp_pool = _Pool(service_key="grp_a", max_size=1)
    monkeypatch.setitem(duckdb_pool._pools, "grp_a", grp_pool)
    duckdb_pool.begin_drain_pools(["grp_a", "does_not_exist"])
    assert grp_pool._draining is True
    assert duckdb_pool.wait_pools_drained(["grp_a", "does_not_exist"], timeout=1.0) is True
    duckdb_pool.end_drain_pools(["grp_a", "does_not_exist"])
    assert grp_pool._draining is False


# ── Heavy slots & slow checkout logging ──────────────────────────────


def test_heavy_cap_and_is_heavy_route(monkeypatch):
    monkeypatch.delenv("DUCKDB_POOL_HEAVY_MAX", raising=False)
    assert (_pool_heavy_cap(4), _pool_heavy_cap(12), _pool_heavy_cap(2), _pool_heavy_cap(1)) == (3, 11, 1, 1)

    for val, expected in (("2", 2), ("99", 4), ("0", 1), ("not-a-number", 3)):
        monkeypatch.setenv("DUCKDB_POOL_HEAVY_MAX", val)
        assert _pool_heavy_cap(4) == expected

    monkeypatch.delenv("DUCKDB_POOL_HEAVY_ROUTES", raising=False)
    for route in (
        "/api/cmcd/aggregates",
        "/api/security/aggregates",
        "/api/network/bundle",
        "/api/network-health",
        "/api/insights",
        "/api/origin/slow-urls",
    ):
        assert is_heavy_route(route) is True
    for route in ("/api/cron-runs", "/api/admin/share/banner", "/api/web-vitals", "", None):
        assert is_heavy_route(route) is False

    monkeypatch.setenv("DUCKDB_POOL_HEAVY_ROUTES", "/custom, /other")
    assert is_heavy_route("/api/x/custom") is True and is_heavy_route("/api/cmcd/aggregates") is False


def test_heavy_slots_concurrency_timeout_and_light_bypass():
    # Cap concurrency + wake waiter + stats section
    pool = _Pool("heavy-cap", max_size=4)
    for _ in range(3):
        pool.acquire_heavy_slot(max_wait=1.0)
    assert pool.stats()["heavy"]["cap"] == 3 and pool.stats()["heavy"]["in_use"] == 3

    got_slot = threading.Event()
    t = threading.Thread(target=lambda: (pool.acquire_heavy_slot(max_wait=5.0), got_slot.set()))
    t.start()
    assert not got_slot.wait(0.1)
    pool.release_heavy_slot()
    assert got_slot.wait(2.0)
    t.join(2.0)
    assert pool.stats()["heavy"]["in_use"] == 3

    # Timeout raises _PoolBusy and increments saturated_rejects_total
    pool_to = _Pool("heavy-timeout", max_size=2)
    pool_to.acquire_heavy_slot(max_wait=1.0)
    with pytest.raises(_PoolBusy, match="heavy-request slots exhausted"):
        pool_to.acquire_heavy_slot(max_wait=0.05)
    assert pool_to.stats()["saturated_rejects_total"] == 1

    # Light checkout not blocked by saturated heavy requests
    fpool = _pool_with_fake_connections(4)
    release_heavy = threading.Event()
    inside = threading.Barrier(4)
    fourth_done = threading.Event()

    def hold_heavy():
        with checkout_connection({"name": "heavy-checkout"}, max_wait=5.0, heavy=True):
            inside.wait(2.0)
            release_heavy.wait(5.0)

    def fourth_heavy():
        with checkout_connection({"name": "heavy-checkout"}, max_wait=5.0, heavy=True):
            fourth_done.set()

    with (
        patch.object(duckdb_pool, "_get_pool", return_value=fpool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
    ):
        holders = [threading.Thread(target=hold_heavy) for _ in range(3)]
        for h in holders:
            h.start()
        inside.wait(2.0)

        extra = threading.Thread(target=fourth_heavy)
        extra.start()
        assert not fourth_done.wait(0.1)

        with checkout_connection({"name": "heavy-checkout"}, max_wait=0.5):
            pass

        release_heavy.set()
        assert fourth_done.wait(2.0)
        for th in (*holders, extra):
            th.join(2.0)
    assert fpool.stats()["heavy"]["in_use"] == 0

    # Heavy slot released when acquire fails or body raises
    err_pool = _pool_with_fake_connections(2)
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=err_pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
    ):
        err_pool.acquire = MagicMock(side_effect=_PoolBusy("saturated"))  # type: ignore[method-assign]
        with pytest.raises(_PoolBusy):
            with checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
                pass
        assert err_pool.stats()["heavy"]["in_use"] == 0

        err_pool.acquire = MagicMock(side_effect=lambda *a, **k: MagicMock())  # type: ignore[method-assign]
        with pytest.raises(RuntimeError):
            with checkout_connection({"name": "heavy-checkout"}, max_wait=0.2, heavy=True):
                raise RuntimeError("query failed")
        assert err_pool.stats()["heavy"]["in_use"] == 0
        assert err_pool.release.call_args.kwargs["errored"] is True


def test_slow_checkout_logging(caplog, monkeypatch):
    pool = _pool_with_fake_connections(4)
    with (
        patch.object(duckdb_pool, "_get_pool", return_value=pool),
        patch.object(duckdb_pool, "_instrument", side_effect=lambda con, **_: con),
        caplog.at_level("WARNING", logger="backend.core.duckdb_pool"),
    ):
        # 1. Slow checkout logged
        monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "50")
        pool.acquire = MagicMock(side_effect=lambda *a, **k: (time.sleep(0.12), MagicMock())[1])  # type: ignore[method-assign]
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0, heavy=True, label="/api/cmcd/aggregates"):
            pass
        msgs = [r.getMessage() for r in caplog.records if "slow checkout" in r.getMessage()]
        assert len(msgs) == 1 and "route=/api/cmcd/aggregates" in msgs[0] and "heavy=True" in msgs[0]

        # 2. Fast checkout not logged
        caplog.clear()
        monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "500")
        pool.acquire = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0, label="/api/cron-runs"):
            pass
        assert not [r for r in caplog.records if "slow checkout" in r.getMessage()]

        # 3. Disabled when threshold=0
        caplog.clear()
        monkeypatch.setenv("DUCKDB_POOL_SLOW_CHECKOUT_MS", "0")
        pool.acquire = MagicMock(side_effect=lambda *a, **k: (time.sleep(0.05), MagicMock())[1])  # type: ignore[method-assign]
        with checkout_connection({"name": "heavy-checkout"}, max_wait=2.0):
            pass
        assert not [r for r in caplog.records if "slow checkout" in r.getMessage()]


# ── discard_idle, reset_pool_for_service & reuse_only ────────────────


def test_discard_idle_reset_and_reuse_only():
    # discard_idle closes idle, zeroes in_use, does not enter draining mode
    pool = _Pool(service_key="discard_idle", max_size=3)
    conns = [_mock_conn() for _ in range(3)]
    for c in conns:
        pool._idle.put_nowait(c)
    pool._in_use = 3
    assert pool.discard_idle() == 3
    assert (pool._in_use, pool._idle.empty(), pool._draining) == (0, True, False)
    for c in conns:
        c.close.assert_called_once()

    # In-flight checked-out conns left alone
    pool_inf = _Pool(service_key="discard_inflight", max_size=2)
    pool_inf._in_use = 1
    assert (pool_inf.discard_idle(), pool_inf._in_use) == (0, 1)

    # reset_pool_for_service noop when absent, closes idle when present
    assert _pool.reset_pool_for_service("never-queried-service-xyz") == 0
    key = "reset-svc-xyz"
    with _clean_pool_keys(key):
        rpool = _pool._get_pool(key, max_size=2)
        rconns = [_mock_conn() for _ in range(2)]
        for c in rconns:
            rpool._idle.put_nowait(c)
        rpool._in_use = 2
        assert _pool.reset_pool_for_service(key) == 2 and rpool._idle.empty()
        for c in rconns:
            c.close.assert_called_once()

    # reuse_only rejects immediately when no idle (even with free capacity or when saturated)
    ro_cap = _Pool(service_key="reuse_only_cap", max_size=4)
    with pytest.raises(_PoolBusy):
        ro_cap.acquire(src={"name": "reuse_only_cap", "bucket": "b"}, max_wait=0.2, reuse_only=True)
    assert (ro_cap._in_use, ro_cap._created_total, ro_cap._idle.empty()) == (0, 0, True)

    ro_sat = _Pool(service_key="reuse_only_sat", max_size=1)
    ro_sat._in_use = 1
    t0 = time.monotonic()
    with pytest.raises(_PoolBusy):
        ro_sat.acquire(src={"name": "reuse_only_sat", "bucket": "b"}, max_wait=2.0, reuse_only=True)
    assert (time.monotonic() - t0) < 0.5
    assert ro_sat._saturated_rejects_total == 0


# ── Merged from tests/perf/test_perf_optimizations.py ──

import multiprocessing
import os

from backend.utils.hll import HyperLogLog

from backend.core.duckdb import _safe_table_name
from backend.utils.sql_validator import escape_sql_literal


def _fake_src(service_id="svc-perf"):
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "b",
        "endpoint": "e",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }


def _minimal_cfg(service_id="svc-perf", commit_interval_mins=5):
    return {
        "service_id": service_id,
        "log_period": 60,
        "access_level": "read_write",
        "commit_interval_mins": commit_interval_mins,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": False}},
    }


def test_commit_job_misfire_grace_time_equals_interval():
    from backend.cron.scheduler import Scheduler

    cfg = _minimal_cfg(commit_interval_mins=5)
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src()),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job = s._sched.get_job("commit_svc-perf")
    assert job is not None, "commit job must be registered"
    assert job.misfire_grace_time == 5 * 60


def test_commit_job_misfire_grace_time_scales_with_longer_interval():
    from backend.cron.scheduler import Scheduler

    cfg = _minimal_cfg(service_id="svc-perf-10")
    cfg["provisioning"]["cron_sync"]["commit_interval_mins"] = 10
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-perf-10")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job = s._sched.get_job("commit_svc-perf-10")
    assert job is not None
    assert job.misfire_grace_time == 10 * 60


def test_insights_prewarmer_grace_time_exceeds_interval():
    from backend.cron.scheduler import Scheduler

    cfg = _minimal_cfg(service_id="svc-prewarm")
    cfg["provisioning"]["cron_compact"] = {"enabled": True}
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-prewarm")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job = s._sched.get_job("insights_prewarmer_svc-prewarm")
    assert job is not None, "insights_prewarmer job must be registered"
    assert job.misfire_grace_time > 240


def test_duckdb_threads_env_var_respected(monkeypatch):
    import backend.core.duckdb as duckdb_mod

    monkeypatch.setenv("DUCKDB_THREADS", "2")
    duckdb_mod._cached_n_threads = None
    cached = (
        int(os.environ["DUCKDB_THREADS"]) if os.environ.get("DUCKDB_THREADS") else min(multiprocessing.cpu_count(), 8)
    )
    assert cached == 2
    duckdb_mod._cached_n_threads = None


def test_duckdb_threads_falls_back_to_cpu_count_when_unset(monkeypatch):
    monkeypatch.delenv("DUCKDB_THREADS", raising=False)
    expected = min(multiprocessing.cpu_count(), 8)
    actual = (
        int(os.environ["DUCKDB_THREADS"]) if os.environ.get("DUCKDB_THREADS") else min(multiprocessing.cpu_count(), 8)
    )
    assert actual == expected


def test_duckdb_threads_capped_at_8(monkeypatch):
    monkeypatch.delenv("DUCKDB_THREADS", raising=False)
    with patch("multiprocessing.cpu_count", return_value=32):
        actual = (
            int(os.environ["DUCKDB_THREADS"])
            if os.environ.get("DUCKDB_THREADS")
            else min(multiprocessing.cpu_count(), 8)
        )
    assert actual == 8


def _needs_prewarm(pathname: str) -> bool:
    return (
        pathname.startswith("/dashboard")
        or pathname.startswith("/network")
        or pathname.startswith("/origin")
        or pathname.startswith("/performance")
        or pathname.startswith("/security")
        or pathname.startswith("/charts")
        or pathname.startswith("/insights")
        or pathname.startswith("/fastly-value")
        or pathname.startswith("/usage")
        or pathname.startswith("/streaming")
        or pathname.startswith("/control-room")
        or pathname.startswith("/sessions/stream")
    )


def test_prewarm_covers_usage():
    assert _needs_prewarm("/usage") is True
    assert _needs_prewarm("/usage/") is True


def test_prewarm_covers_streaming():
    assert _needs_prewarm("/streaming") is True


def test_prewarm_covers_control_room():
    assert _needs_prewarm("/control-room") is True


def test_prewarm_covers_sessions_stream():
    assert _needs_prewarm("/sessions/stream") is True
    assert _needs_prewarm("/sessions/stream/abc123") is True


def test_prewarm_does_not_fire_for_sessions_list():
    assert _needs_prewarm("/sessions") is False


def test_prewarm_does_not_fire_for_query():
    assert _needs_prewarm("/query") is False


def test_prewarm_fires_for_existing_chart_routes():
    for route in ["/dashboard", "/network", "/origin", "/performance", "/security", "/insights", "/fastly-value"]:
        assert _needs_prewarm(route) is True, f"{route} should trigger prewarm"


@pytest.mark.benchmark
def test_bench_hll_add_1k_items(benchmark):
    items = [f"203.0.113.{i % 256}" for i in range(1000)]

    def _go():
        sketch = HyperLogLog()
        for item in items:
            sketch.add(item)
        return sketch

    result = benchmark(_go)
    assert result.count() > 0


@pytest.mark.benchmark
def test_bench_hll_count_after_100k_adds(benchmark):
    sketch = HyperLogLog()
    for i in range(100_000):
        sketch.add(f"203.0.113.{i % 256}.{i // 256}")

    estimate = benchmark(sketch.count)
    assert estimate > 0


@pytest.mark.benchmark
def test_bench_hll_merge_24_hours(benchmark):
    sketches = []
    for hour in range(24):
        s = HyperLogLog()
        for i in range(5_000):
            s.add(f"hour{hour}:198.51.100.{i % 256}.{i // 256}")
        sketches.append(s)

    def _go():
        merged = HyperLogLog()
        for s in sketches:
            merged.merge(s)
        return merged

    merged = benchmark(_go)
    assert merged is not None
    assert merged.count() > 0


@pytest.mark.benchmark
def test_bench_hll_serialize_roundtrip(benchmark):
    sketch = HyperLogLog()
    for i in range(10_000):
        sketch.add(f"192.0.2.{i % 256}.{i // 256}")
    blob = sketch.to_bytes()

    def _roundtrip():
        return HyperLogLog.from_bytes(blob).to_bytes()

    out = benchmark(_roundtrip)
    assert out == blob


@pytest.mark.benchmark
def test_bench_safe_table_name(benchmark):
    name = "svc-test-account-name-with-dashes-and-some-numbers-42"
    result = benchmark(_safe_table_name, name)
    assert result.startswith("logs_")


@pytest.mark.benchmark
def test_bench_escape_sql_literal_typical(benchmark):
    key = "raw/2026-05-15/12/2026-05-15T12-34-56.svc-name.gz"
    result = benchmark(escape_sql_literal, key)
    assert result == key


@pytest.mark.benchmark
def test_bench_escape_sql_literal_with_quotes(benchmark):
    bad_key = "raw/2026-05-15/O'Brien's-bucket/file.gz"
    result = benchmark(escape_sql_literal, bad_key)
    assert "''" in result
