"""Tests for the hard-cap watchdog inside ``backend.scheduler.cron_task``.

Pins the deadlock fix landed for the 2026-05-21 incident where
``_run_service_cron`` hung 10+ minutes on the usage-log step and APScheduler's
``max_instances=1`` wedged every subsequent tick. The watchdog runs the cron
body on a single-worker ThreadPoolExecutor with a hard cap; on timeout it
returns control to APScheduler so the next tick can fire, leaking the stuck
inner thread (Python can't cleanly kill threads).
"""

from __future__ import annotations

import logging
import time


def test_cron_task_returns_when_inner_function_exceeds_hard_cap(monkeypatch, caplog):
    """A cron body that runs past the hard cap must NOT block the wrapper.

    Otherwise APScheduler's worker thread is wedged and ``max_instances=1``
    skips every subsequent tick — exactly the failure mode this fix exists
    for. The wrapper returns None and an ERROR log line names the cron +
    service so the next incident leaves a breadcrumb.
    """
    from backend import scheduler as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 0.5)

    @sched_mod.cron_task("hang-test")
    def _hangs_forever(service_id: str) -> None:
        time.sleep(30)

    caplog.set_level(logging.ERROR, logger="backend.scheduler")

    start = time.monotonic()
    result = _hangs_forever("svc-watchdog")
    elapsed = time.monotonic() - start

    assert result is None, "watchdog should return None on timeout"
    assert elapsed < 5, (
        f"wrapper returned in {elapsed:.2f}s — expected <5s (hard cap was 0.5s). "
        "If this fails, the executor is being shut down with wait=True and the "
        "watchdog is defeated."
    )
    assert any(
        "exceeded" in rec.getMessage() and "hard cap" in rec.getMessage() and "svc-watchdog" in rec.getMessage()
        for rec in caplog.records
        if rec.levelno >= logging.ERROR
    ), f"expected ERROR log naming the service; got {[r.getMessage() for r in caplog.records]}"


def test_cron_task_propagates_normal_return(monkeypatch):
    """Fast inner function returns a sentinel; wrapper returns the same value.

    Pins that the watchdog is transparent on the happy path — refactoring
    it must not silently swallow the return value or wrap it in a Future.
    """
    from backend import scheduler as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 60)

    sentinel = object()

    @sched_mod.cron_task("happy-path")
    def _quick(service_id: str):
        return sentinel

    start = time.monotonic()
    result = _quick("svc-happy")
    elapsed = time.monotonic() - start

    assert result is sentinel
    assert elapsed < 5, f"happy path took {elapsed:.2f}s — should be near-instant"


def test_cron_task_preserves_telemetry_context(monkeypatch):
    """The wrapper's process_context_scope must apply inside the worker thread.

    Pins that wrapping in ThreadPoolExecutor didn't break telemetry attribution.
    The 2026-05-21 incident left this contract intact and we don't want a
    silent regression where every cron's usage rows land with NULL context.
    """
    from backend import scheduler as sched_mod
    from backend.utils.telemetry import get_process_context

    captured = {}

    @sched_mod.cron_task("ctx-test")
    def _check_ctx(service_id: str) -> None:
        captured["ctx"] = get_process_context()

    _check_ctx("svc-ctx")

    assert captured.get("ctx") == "ctx-test", (
        f"expected process_context='ctx-test' inside cron body; got {captured.get('ctx')!r}. "
        "If None, the process_context_scope is being applied in the wrong thread."
    )
