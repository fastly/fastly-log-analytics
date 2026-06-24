"""Tests for backend.utils.active_requests.

Covers the in-flight counter (thread-safe inc/dec/read) and the
``should_defer_cron`` gate's four logical branches:

  * no in-flight requests -> proceed (False) AND clear any prior window
  * in-flight + no prior window -> defer (True) and START the window
  * in-flight + prior window still within max_defer_secs -> defer (True)
  * in-flight + prior window exceeds max_defer_secs -> proceed (False),
    WARN log, and clear the window
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from backend.utils import active_requests
from backend.utils.active_requests import (
    _reset_for_tests,
    active_request_count,
    decrement_active_requests,
    increment_active_requests,
    should_defer_cron,
    yield_to_api,
)


@pytest.fixture(autouse=True)
def _clean_state():
    """Reset module-level counter + defer window between every test."""
    _reset_for_tests()
    yield
    _reset_for_tests()


# ---------------------------------------------------------------------------
# Counter primitives
# ---------------------------------------------------------------------------


def test_initial_count_is_zero_after_reset():
    assert active_request_count() == 0


def test_increment_bumps_count():
    increment_active_requests()
    assert active_request_count() == 1
    increment_active_requests()
    assert active_request_count() == 2


def test_decrement_reduces_count():
    increment_active_requests()
    increment_active_requests()
    decrement_active_requests()
    assert active_request_count() == 1


def test_decrement_clamps_at_zero():
    """Belt-and-suspenders: a stray decrement must NOT take the counter
    negative, otherwise should_defer_cron's `_active_count == 0` branch
    would never fire again."""
    decrement_active_requests()
    decrement_active_requests()
    assert active_request_count() == 0


def test_increment_then_decrement_back_to_zero():
    for _ in range(5):
        increment_active_requests()
    assert active_request_count() == 5
    for _ in range(5):
        decrement_active_requests()
    assert active_request_count() == 0


def test_reset_for_tests_clears_counter_and_window():
    increment_active_requests()
    increment_active_requests()
    # Seed the defer-window dict via a deferral
    assert should_defer_cron("sync", "svc-A") is True
    assert active_requests._defer_started_at  # populated

    _reset_for_tests()

    assert active_request_count() == 0
    assert active_requests._defer_started_at == {}


# ---------------------------------------------------------------------------
# Thread safety (light coverage — `with _lock` is the contract here)
# ---------------------------------------------------------------------------


def test_concurrent_increments_are_atomic():
    """Spawn 50 threads, each calls increment_active_requests once.
    Without the lock, the read-modify-write would race and lose updates."""
    workers = []
    barrier = threading.Barrier(50)

    def _worker():
        barrier.wait()  # maximize contention
        increment_active_requests()

    for _ in range(50):
        t = threading.Thread(target=_worker)
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

    assert active_request_count() == 50


def test_concurrent_decrements_clamp_at_zero():
    """Over-decrementing across threads must still bottom out at 0."""
    for _ in range(10):
        increment_active_requests()

    workers = []
    barrier = threading.Barrier(30)

    def _worker():
        barrier.wait()
        decrement_active_requests()

    for _ in range(30):
        t = threading.Thread(target=_worker)
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

    assert active_request_count() == 0


# ---------------------------------------------------------------------------
# should_defer_cron — branch coverage
# ---------------------------------------------------------------------------


def test_should_defer_returns_false_when_no_active_requests():
    assert active_request_count() == 0
    assert should_defer_cron("sync", "svc-A") is False


def test_should_defer_clears_prior_window_when_quiet():
    """If a deferral window was set during a busy period but the system
    has since gone idle, the next call must clear that window so the
    NEXT contention round starts a fresh max_defer_secs clock."""
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A") is True
    assert ("sync", "svc-A") in active_requests._defer_started_at

    # Drain in-flight requests.
    decrement_active_requests()
    assert active_request_count() == 0

    assert should_defer_cron("sync", "svc-A") is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at


def test_should_defer_returns_true_and_seeds_window_when_busy():
    increment_active_requests()
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    assert should_defer_cron("sync", "svc-A") is True

    # Window must now exist with a monotonic timestamp.
    started = active_requests._defer_started_at[("sync", "svc-A")]
    assert isinstance(started, float)
    assert started > 0


def test_should_defer_returns_true_while_window_within_budget():
    """Second call within max_defer_secs of the first must still defer,
    and the window must NOT be reset (otherwise starvation safeguard
    would never fire on a sustained-busy service)."""
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    first_started = active_requests._defer_started_at[("sync", "svc-A")]

    # A subsequent call still well within the 30 s budget should defer.
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    second_started = active_requests._defer_started_at[("sync", "svc-A")]
    assert second_started == first_started  # window preserved


def test_should_defer_returns_false_after_starvation_window_expires(caplog):
    """Force the window to age past max_defer_secs by passing a tiny
    budget + sleeping. The third call must return False, clear the
    window, and emit a WARN."""
    increment_active_requests()

    # Seed window.
    assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is True
    assert ("sync", "svc-A") in active_requests._defer_started_at

    time.sleep(0.02)

    with caplog.at_level(logging.WARNING, logger="backend.utils.active_requests"):
        result = should_defer_cron("sync", "svc-A", max_defer_secs=0.01)

    assert result is False
    # Starvation path must clear the window so the next round starts fresh.
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    warn_records = [
        r for r in caplog.records if r.levelno == logging.WARNING and r.name == "backend.utils.active_requests"
    ]
    assert len(warn_records) == 1
    msg = warn_records[0].getMessage()
    assert "active-request-gate" in msg
    assert "sync" in msg
    assert "svc-A" in msg


def test_should_defer_starvation_via_monkeypatched_monotonic(monkeypatch):
    """Belt-and-suspenders: drive the starvation branch deterministically
    by stubbing time.monotonic — avoids any flakiness from wall-clock
    jitter on a loaded CI box."""
    increment_active_requests()

    fake_now = [1000.0]

    def _fake_monotonic():
        return fake_now[0]

    monkeypatch.setattr(active_requests.time, "monotonic", _fake_monotonic)

    # t=1000.0 — seed window
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    assert active_requests._defer_started_at[("optimize", "svc-Z")] == 1000.0

    # t=1010.0 — within budget, still defers
    fake_now[0] = 1010.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True

    # t=1040.0 — exceeds 30 s budget, runs anyway
    fake_now[0] = 1040.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is False
    assert ("optimize", "svc-Z") not in active_requests._defer_started_at


def test_should_defer_tracks_windows_per_key_independently():
    """Concurrent services must not share a starvation clock — deferring
    job=sync for svc-A must NOT mark job=sync for svc-B as deferred."""
    increment_active_requests()

    assert should_defer_cron("sync", "svc-A") is True
    assert ("sync", "svc-A") in active_requests._defer_started_at
    assert ("sync", "svc-B") not in active_requests._defer_started_at

    assert should_defer_cron("sync", "svc-B") is True
    assert ("sync", "svc-B") in active_requests._defer_started_at

    # Distinct jobs on the same service are also independent.
    assert should_defer_cron("optimize", "svc-A") is True
    assert ("optimize", "svc-A") in active_requests._defer_started_at

    assert len(active_requests._defer_started_at) == 3


# ---------------------------------------------------------------------------
# yield_to_api — cooperative mid-tick yield
# ---------------------------------------------------------------------------


def test_yield_returns_immediately_when_no_active_requests():
    """Hot path: zero in-flight means zero sleep — must NOT call time.sleep."""
    assert active_request_count() == 0
    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=1.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    assert slept == 0.0
    assert elapsed < 0.02  # generous; the function should be essentially free


def test_yield_returns_early_when_requests_drain_during_sleep():
    """Spawn a thread that decrements after a short delay. yield_to_api must
    notice the count dropped and return before max_wait_secs expires."""
    increment_active_requests()

    def _drain_soon():
        time.sleep(0.1)
        decrement_active_requests()

    t = threading.Thread(target=_drain_soon)
    t.start()

    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=2.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    t.join()

    # Drained after ~100ms; helper should have returned shortly after that
    # — well under the 2 s max.
    assert elapsed < 0.5, f"helper waited {elapsed:.3f}s after drain"
    assert 0.05 <= slept < 0.5


def test_yield_caps_at_max_wait_secs_under_sustained_load():
    """Sustained in-flight requests must not stall sync forever. The helper
    returns after max_wait_secs even if count stays > 0."""
    increment_active_requests()
    try:
        t0 = time.monotonic()
        slept = yield_to_api(max_wait_secs=0.2, poll_interval=0.05)
        elapsed = time.monotonic() - t0
        # Bounded by max_wait_secs + one extra poll_interval slack.
        assert 0.15 <= elapsed < 0.4, f"helper waited {elapsed:.3f}s"
        # slept counter tracks the budget consumed; allow one extra tick.
        assert 0.15 <= slept <= 0.3
        # Counter was NOT touched — yield observes, doesn't mutate state.
        assert active_request_count() == 1
    finally:
        decrement_active_requests()


def test_yield_polls_at_configured_interval(monkeypatch):
    """Verify the helper actually polls — i.e. uses small sleeps in a loop
    so an early drain is observed promptly, rather than one big sleep that
    blocks past the drain moment."""
    increment_active_requests()
    sleep_calls: list[float] = []
    real_sleep = time.sleep

    def _tracking_sleep(secs: float) -> None:
        sleep_calls.append(secs)
        real_sleep(secs)

    monkeypatch.setattr(active_requests.time, "sleep", _tracking_sleep)
    try:
        yield_to_api(max_wait_secs=0.2, poll_interval=0.05)
    finally:
        decrement_active_requests()

    assert len(sleep_calls) >= 2, f"expected multiple polls, got {sleep_calls}"
    assert all(s == 0.05 for s in sleep_calls), f"unexpected sleep durations: {sleep_calls}"
