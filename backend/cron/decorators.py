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


def cron_task(name: str, job_name: str | None = None):
    """Wraps a cron handler with telemetry + usage-log flush + a hard watchdog.

    ``job_name`` is the ``job_runs.job_name`` the wrapped body registers via
    ``start_cron_run`` (it often differs from the telemetry ``name``). The
    heartbeat loop scopes its lease refresh to that job — an unscoped
    refresh would keep EVERY running lease for the service alive, so a
    wedged job whose own heartbeat died would never be reaped while any
    healthy job kept ticking (re-creating the documented orphaned-sync-row
    ingestion stall).

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
        from backend.celery_app import app

        @wraps(func)
        def wrapper(service_id: str, *args, **kwargs):
            def heartbeat_loop(stop_event: threading.Event):
                import time

                from backend.core.metadata.base import get_con

                while not stop_event.is_set():
                    if stop_event.wait(10.0):
                        break
                    try:
                        con = get_con(service_id)
                        if job_name:
                            con.execute(
                                "UPDATE job_runs SET heartbeat_at = ? "
                                "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                                (time.time(), service_id, job_name),
                            )
                        else:
                            # No job_name declared: refresh nothing rather than
                            # everything — an unscoped refresh keeps other jobs'
                            # leaked leases alive forever (frozen-ingestion trap).
                            pass
                        con.commit()
                    except Exception:
                        pass

            def _body():

                from backend.utils.telemetry import process_context_scope, start_call_tracking
                from backend.utils.usage_logger import flush_usage_log

                with process_context_scope(name):
                    start_call_tracking()
                    try:
                        return func(service_id, *args, **kwargs)
                    finally:
                        flush_usage_log(service_id)

            cap = _CRON_HARD_CAP_S

            stop_event = threading.Event()
            hb_thread = threading.Thread(target=heartbeat_loop, args=(stop_event,), daemon=True)
            hb_thread.start()

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
            finally:
                stop_event.set()
                hb_thread.join(timeout=1.0)

        # Register celery task
        task_name = f"{func.__module__}.{func.__name__}_celery"

        @app.task(name=task_name, bind=True)
        @wraps(func)
        def celery_wrapper(self, service_id: str, *args, **kwargs):
            return wrapper(service_id, *args, **kwargs)

        wrapper.celery_task = celery_wrapper
        wrapper.delay = celery_wrapper.delay

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
        from backend.celery_app import app

        @wraps(fn)
        def wrapper() -> None:
            from backend.utils.system_jobs import record_job_run
            from backend.utils.telemetry import process_context_scope

            prefix = f"\x1b[{color}m[{tag}]\x1b[0m"
            logger.info("🏎️  %s %s job started.", prefix, label)
            start = time.monotonic()
            try:
                with process_context_scope(f"cron:{job_id}"):
                    detail = fn()
                record_job_run(job_id, "success", time.monotonic() - start, detail)
            except Exception as e:
                record_job_run(job_id, "error", time.monotonic() - start, str(e))
                logger.error("[%s] Failed: %s", job_id, e)
            logger.info("🏁  %s %s job finished.", prefix, label)

        task_name = f"{fn.__module__}.{fn.__name__}_celery"

        @app.task(name=task_name, bind=True)
        @wraps(fn)
        def celery_wrapper(self, *args, **kwargs):
            return wrapper(*args, **kwargs)

        wrapper.celery_task = celery_wrapper
        wrapper.delay = celery_wrapper.delay

        return wrapper

    return decorator


__all__ = ["_CRON_HARD_CAP_S", "cron_task", "global_job"]
