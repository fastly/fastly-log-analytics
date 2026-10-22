"""Heavy/light bulkhead: heavy (aggregate) checkouts cannot take every pool slot."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from backend.core import duckdb_pool
from backend.core.duckdb_pool import _Pool, _pool_heavy_cap, _PoolBusy, checkout_connection, is_heavy_route


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
