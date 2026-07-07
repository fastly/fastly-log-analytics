"""Decorator that wraps every cron handler with telemetry + a hard watchdog.

The ``cron_task`` factory used to live at the top of the old monolithic scheduler module
alongside the APScheduler lifecycle and every cron body. This module isolates
the decorator so the job modules can import it without dragging the whole
scheduler module in.

The hard-cap is module-level so tests can monkeypatch
``backend.cron.decorators._CRON_HARD_CAP_S`` without modifying the
decorator itself (the wrapper reads it from module globals at call time).
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
from functools import wraps

# We intentionally keep the historical logger NAME (a plain string, not a
# module reference) so caplog filters and log-shipping rules that read
# ``logger="backend.scheduler"`` keep receiving watchdog error lines.
logger = logging.getLogger("backend.scheduler")

# Hard upper bound on any single cron invocation. Ingest is already capped at
# max_seconds=240 inside _run_service_cron; this leaves ~60s for the post-ingest
# phases (refresh_config_status, usage-log block, update_cron_duration). If the
# inner thread runs past this, the APScheduler worker thread returns anyway so
# max_instances=1 cannot stay wedged across ticks. The leaked inner thread is
# accepted — Python cannot cleanly kill a thread, but it will eventually unblock
# (SQLite timeouts are 30s) and flush its own usage log on exit.
_CRON_HARD_CAP_S = 300

# Shared watchdog executor (see _watchdog_executor below). Sized >= APScheduler's
# default pool (10) so concurrent cron bodies never queue behind each other here;
# each job is max_instances=1 and the global_job crons don't use this path.
_WATCHDOG_MAX_WORKERS = 12
_watchdog_lock = threading.Lock()
_watchdog_executor: concurrent.futures.ThreadPoolExecutor | None = None


def _get_watchdog_executor() -> concurrent.futures.ThreadPoolExecutor:
    """Lazily build the process-shared cron watchdog executor.

    cron_task runs each job body on a watchdog thread so a wedged job can be
    abandoned (``fut.result(timeout=...)``) without holding APScheduler's
    ``max_instances=1`` slot. This pool is REUSED across ticks. Previously
    cron_task created a fresh ``ThreadPoolExecutor`` PER TICK; each tick's
    throwaway thread opened per-thread SQLite connections (metadata.db +
    usage_log.db) that ``ThreadLocalPool`` pins in ``_all_connections`` and
    never closes on thread death → ~2 leaked connections/tick, each with a
    page cache → the 2026-06-22 OOM. Reusing the threads keeps their
    thread-local connections alive and reused instead of orphaned. See
    [[backend-oom-restart-loop]].
    """
    global _watchdog_executor
    with _watchdog_lock:
        if _watchdog_executor is None:
            _watchdog_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=_WATCHDOG_MAX_WORKERS, thread_name_prefix="cron-watchdog"
            )
        return _watchdog_executor


def _abandon_watchdog_executor() -> None:
    """Drop the shared executor after a hard-cap timeout.

    A timed-out body's thread cannot be killed and is stuck (typically on a 30s
    SQLite lock); keeping the shared pool would lose that worker slot forever.
    Shut it down with ``wait=False`` (abandon the wedged thread) and null it so
    the next call rebuilds a fresh pool. The abandoned thread's thread-local
    SQLite connections become dead-owner orphans, which ThreadLocalPool's
    cold-open reaper then closes. Rare — error path only.
    """
    global _watchdog_executor
    with _watchdog_lock:
        ex, _watchdog_executor = _watchdog_executor, None
    if ex is not None:
        ex.shutdown(wait=False)


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

            # Read the cap from module globals at call time so tests that
            # ``monkeypatch.setattr(backend.cron.decorators,
            # "_CRON_HARD_CAP_S", ...)`` take effect per-invocation.
            cap = _CRON_HARD_CAP_S
            # Submit to the SHARED watchdog pool — do NOT create (or shut down)
            # an executor per call; that churn leaked a SQLite connection per
            # tick (the 2026-06-22 OOM). On the happy path the pool is reused;
            # only a hard-cap timeout tears it down (the wedged thread can't be
            # cancelled, so we abandon the whole pool and rebuild it next call).
            ex = _get_watchdog_executor()
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
                _abandon_watchdog_executor()
                return None

        return wrapper

    return decorator


def global_job(job_id: str, *, color: str, tag: str, label: str):
    """Wrap a global (non-service) cron callable with the start/end-log +
    record_job_run boilerplate the three util jobs (bot_data_refresh /
    rdns_enrichment / share_audit_purge) share.

    The wrapped function should return the ``detail`` string that
    record_job_run records on the success row. Exceptions are caught,
    recorded as ``status="error"``, and logged via ``logger.error``.
    The wrapped function may still emit its own success log line; the
    decorator only owns the start log, end log, wall-clock timing, and
    record_job_run wrapper.

    ``tag`` and ``color`` are the bracketed log prefix shown to the
    operator (e.g. ``[bots]`` in cyan); ``job_id`` is the stable name
    recorded in cron_runs.
    """
    import time

    def decorator(fn):
        @wraps(fn)
        def wrapper() -> None:
            # Import on each call so tests that patch
            # ``backend.utils.system_jobs.record_job_run`` see their stub
            # — a module-scope import bound the original reference into
            # the decorator's closure at decorator-application time
            # (well before the patch ran), defeating the mock.
            from backend.utils.system_jobs import record_job_run
            from backend.utils.telemetry import process_context_scope

            prefix = f"\x1b[{color}m[{tag}]\x1b[0m"
            logger.info("▶️  %s %s job started.", prefix, label)
            start = time.monotonic()
            # SRE-09: enter the cron attribution scope (mirrors @cron_task) so
            # any SQLite this global job runs against ``__global_share__``
            # (e.g. share_audit_purge's DELETE) registers in the Live Query
            # Monitor as kind="cron" / "Cron: <job_id>" instead of falling
            # back to "System: thread:<generic APScheduler worker>". Without
            # this an operator triaging a runaway purge at 2am can't tell it
            # apart from boot/pool-warmer work.
            try:
                with process_context_scope(f"cron:{job_id}"):
                    detail = fn()
                record_job_run(job_id, "success", time.monotonic() - start, detail)
            except Exception as e:
                record_job_run(job_id, "error", time.monotonic() - start, str(e))
                logger.error("[%s] Failed: %s", job_id, e)
            logger.info("⏹️  %s %s job finished.", prefix, label)

        return wrapper

    return decorator


__all__ = ["_CRON_HARD_CAP_S", "cron_task", "global_job"]
