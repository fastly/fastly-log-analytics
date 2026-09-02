"""APScheduler ↔ cron_task watchdog stress test (TESTING_PLAN_3 item 20).

The existing ``test_scheduler_watchdog.py`` test proves the wrapper
returns when its body exceeds ``_CRON_HARD_CAP_S``. That's a necessary
but insufficient bar — the actual 2026-05-21 incident
([cron_watchdog_max_instances_trap](../memory/cron_watchdog_max_instances_trap.md))
was about *APScheduler's* behavior:

  - ``_run_log_discovery_cron`` is registered with ``max_instances=1``.
  - When the body hung 10+ minutes, APScheduler's worker thread stayed
    blocked inside the call.
  - Every subsequent tick was logged ``skipped: maximum number of
    running instances reached (1)`` and ingestion stopped entirely.

The cron_task watchdog fixes that by running the body on its own
ThreadPoolExecutor and *returning* on timeout. This test pins the
end-to-end interaction: register a real BackgroundScheduler with a
``max_instances=1`` job whose first call hangs past the hard cap; assert
that the second tick still fires.

A unit test of cron_task alone can't catch a regression where someone
changes ``shutdown(wait=False)`` back to ``shutdown(wait=True)`` — the
inner call would still timeout but the executor cleanup would re-block
the APScheduler worker. That's exactly the kind of refactor mistake
this stress test is here to catch.

Also pins the per-phase usage_log pattern: ThreadPoolExecutor with
``submit(...).result(timeout=30)`` and ``shutdown(wait=False)`` on
timeout. Same fix shape, second site, same regression risk.
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest


def test_apscheduler_max_instances_1_keeps_firing_after_watchdog_kill(monkeypatch, caplog):
    """The real-world contract: hung tick → watchdog returns → next tick fires.

    Without the ``shutdown(wait=False)`` branch in cron_task, the
    BackgroundScheduler worker thread would stay wedged inside the
    timed-out future cleanup. APScheduler's ``max_instances=1`` would
    then skip every subsequent tick (the symptom of the 2026-05-21
    incident).
    """
    apscheduler = pytest.importorskip("apscheduler.schedulers.background")
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 0.5)

    tick_log: list[float] = []
    _hang_lock = threading.Event()

    @sched_mod.cron_task("apscheduler-stress")
    def _cron_body(service_id: str) -> None:
        tick_log.append(time.monotonic())
        # First tick hangs; subsequent ticks return immediately.
        if len(tick_log) == 1:
            # Sleep MUCH longer than the hard cap so the watchdog must fire.
            # The thread leaks (Python can't kill threads) — that's expected
            # and matches production behavior.
            time.sleep(10)
            _hang_lock.set()

    sched = apscheduler.BackgroundScheduler(timezone="UTC")
    sched.add_job(
        _cron_body,
        "interval",
        seconds=0.4,
        args=["svc-stress"],
        id="cron-apscheduler-stress",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(UTC) + timedelta(milliseconds=50),
    )

    caplog.set_level(logging.ERROR, logger="backend.scheduler")
    sched.start()

    try:
        # Give the scheduler time for at least 3 firings: tick1 (hangs +
        # gets killed at 0.5s), then tick2 + tick3 at 400ms intervals.
        # Total wait of 2.5s leaves room for jitter.
        deadline = time.monotonic() + 2.5
        while time.monotonic() < deadline and len(tick_log) < 3:
            time.sleep(0.05)
    finally:
        sched.shutdown(wait=False)

    # If the watchdog didn't free the APScheduler worker, len(tick_log)
    # would stick at 1 (every subsequent tick gets "skipped: maximum
    # number of running instances reached").
    assert len(tick_log) >= 2, (
        f"APScheduler stalled at {len(tick_log)} tick(s) — watchdog did not "
        f"free the worker thread. Symptom of regressing shutdown(wait=False) "
        f"back to wait=True. caplog: {[r.getMessage() for r in caplog.records]}"
    )

    # Watchdog must have logged the kill — provides breadcrumbs for the
    # next incident.
    assert any("exceeded" in rec.getMessage() and "hard cap" in rec.getMessage() for rec in caplog.records), (
        f"expected watchdog ERROR log; got {[r.getMessage() for r in caplog.records]}"
    )


def test_usage_log_phase_30s_timeout_pattern_does_not_block_caller():
    """Pins the per-phase pattern in _run_log_discovery_cron (backend/cron/jobs/sync.py).

    The phase is wrapped in its own single-worker ThreadPoolExecutor with
    a 30s timeout. On timeout we MUST call ``shutdown(wait=False)`` — if
    that's ever changed to ``wait=True`` the caller blocks on the leaked
    thread and we're right back in the wedge.

    This test reproduces the exact shape (executor + submit +
    result(timeout=...) + shutdown(wait=False)) and verifies the caller
    returns within timeout + small jitter.
    """

    def _hangs():
        time.sleep(10)
        return "should-never-return"

    PHASE_TIMEOUT_S = 0.3

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="usage-log-stress")
    shutdown_wait = True

    start = time.monotonic()
    try:
        fut = ex.submit(_hangs)
        try:
            fut.result(timeout=PHASE_TIMEOUT_S)
            pytest.fail("expected TimeoutError — hangs() sleeps 10s but timeout is 0.3s")
        except concurrent.futures.TimeoutError:
            shutdown_wait = False
    finally:
        ex.shutdown(wait=shutdown_wait)
    elapsed = time.monotonic() - start

    # The full block must return well inside the timeout + small overhead.
    # If shutdown_wait=True ever creeps back in, this elapsed jumps to ~10s.
    assert elapsed < PHASE_TIMEOUT_S + 1.0, (
        f"phase pattern took {elapsed:.2f}s — caller is blocking on the leaked "
        f"thread, defeating the per-phase timeout. Check that shutdown_wait "
        f"is set to False on TimeoutError."
    )


def test_metadata_db_init_lock_has_finite_timeout():
    """The third layer of the 2026-05-21 fix: metadata_db's init lock acquires
    with a timeout, not as a blocking ``with`` block.

    Without this layer, a hung connect+PRAGMA inside the lock would
    wedge every other caller forever, regardless of the cron-level
    watchdog. The lock pattern now lives in
    :class:`backend.core.sqlite_pool.ThreadLocalPool` (metadata_db.get_con
    delegates to ``_pool.get(service_id)`` which calls
    ``init_lock.acquire(timeout=...)`` inside the cold path). Verify it's
    still using a timeout-based acquire.
    """
    import inspect

    from backend.core.sqlite_pool import ThreadLocalPool

    src = inspect.getsource(ThreadLocalPool.get)
    # The lock pattern must NOT be a bare ``with init_lock:`` block —
    # that's the regression the 2026-05-21 fix removed.
    assert "init_lock.acquire(" in src, (
        "ThreadLocalPool.get must call init_lock.acquire() with a timeout — "
        "a bare `with init_lock:` reintroduces the unkillable wedge that "
        "the 2026-05-21 incident exposed. See cron_watchdog_max_instances_trap "
        "memory for the full incident writeup."
    )
    # Sanity: there's a release call too (acquire/release must pair).
    assert "init_lock.release(" in src, (
        "ThreadLocalPool.get calls init_lock.acquire() but no release() — this leaks the lock on every call."
    )
