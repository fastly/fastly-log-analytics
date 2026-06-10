"""Decorator that wraps every cron handler with telemetry + a hard watchdog.

The ``cron_task`` factory used to live at the top of ``backend/scheduler.py``
alongside the APScheduler lifecycle and every cron body. This module isolates
the decorator so the job modules can import it without dragging the whole
scheduler module in.

The hard-cap is module-level so tests can monkeypatch
``backend.cron.decorators._CRON_HARD_CAP_S`` (or the shim alias
``backend.scheduler._CRON_HARD_CAP_S``) without modifying the decorator
itself.
"""

from __future__ import annotations

import concurrent.futures
import logging
from functools import wraps

# We intentionally bind to the shim's logger name so caplog filters that
# historically read ``logger="backend.scheduler"`` still receive watchdog
# error lines after the carve.
logger = logging.getLogger("backend.scheduler")

# Hard upper bound on any single cron invocation. Ingest is already capped at
# max_seconds=240 inside _run_service_cron; this leaves ~60s for the post-ingest
# phases (refresh_config_status, usage-log block, update_cron_duration). If the
# inner thread runs past this, the APScheduler worker thread returns anyway so
# max_instances=1 cannot stay wedged across ticks. The leaked inner thread is
# accepted — Python cannot cleanly kill a thread, but it will eventually unblock
# (SQLite timeouts are 30s) and flush its own usage log on exit.
_CRON_HARD_CAP_S = 300


def cron_task(name: str):
    """Wraps a cron handler with telemetry + usage-log flush + a hard watchdog.

    The process_context_scope wrapper resets both the ContextVar and the
    process-global mirror (CAS-style) on exit. Otherwise APScheduler's
    worker threads carry the stale ContextVar into the next job, and the
    fsspec iothread keeps reading the stale global — misattributing every
    subsequent cron's I/O to whichever job ran last.

    Watchdog: runs the wrapped function on a single-worker ThreadPoolExecutor
    bounded by _CRON_HARD_CAP_S. On timeout, the executor is shut down with
    wait=False so this wrapper returns and the APScheduler worker thread is
    freed for the next tick.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(service_id: str, *args, **kwargs):
            def _body():
                from backend.utils.telemetry import process_context_scope, start_call_tracking
                from backend.utils.usage_logger import flush_usage_log

                with process_context_scope(name):
                    start_call_tracking()
                    try:
                        return func(service_id, *args, **kwargs)
                    finally:
                        flush_usage_log(service_id)

            # Read the cap at call time so tests can monkeypatch it without
            # re-decorating the function under test. Resolve through the
            # ``backend.scheduler`` shim so existing tests that do
            # ``monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", ...)``
            # continue to take effect, while still falling back to the
            # value defined in this module.
            try:
                import backend.scheduler as _shim

                cap = getattr(_shim, "_CRON_HARD_CAP_S", _CRON_HARD_CAP_S)
            except Exception:
                cap = _CRON_HARD_CAP_S
            ex = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"cron-{name}-{service_id}")
            shutdown_wait = True
            try:
                fut = ex.submit(_body)
                try:
                    return fut.result(timeout=cap)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        "[scheduler] %s/%s exceeded %ds hard cap — abandoning worker "
                        "thread so APScheduler max_instances=1 doesn't wedge ingestion",
                        name,
                        service_id,
                        cap,
                    )
                    shutdown_wait = False
                    return None
            finally:
                ex.shutdown(wait=shutdown_wait)

        return wrapper

    return decorator


__all__ = ["_CRON_HARD_CAP_S", "cron_task"]
