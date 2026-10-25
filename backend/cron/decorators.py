"""Decorator that wraps every cron handler with telemetry + a watchdog.

The ``cron_task`` factory used to live at the top of the old monolithic scheduler module
alongside the APScheduler lifecycle and every cron body. This module isolates
the decorator so the job modules can import it without dragging the whole
scheduler module in.

The watchdog cap is module-level so tests can monkeypatch
``backend.cron.decorators._CRON_HARD_CAP_S`` without modifying the
decorator itself (the wrapper reads it from module globals at call time).
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from functools import wraps
from types import TracebackType

# We intentionally keep the historical logger NAME (a plain string, not a
# module reference) so caplog filters and log-shipping rules that read
# ``logger="backend.scheduler"`` keep receiving watchdog error lines.
logger = logging.getLogger("backend.scheduler")

DEFAULT_INTERVAL_S = 10.0
_JOIN_TIMEOUT_S = 1.0


class CronHeartbeat:
    """Refresh ``job_runs.heartbeat_at`` for ``(service_id, job_name)`` while the block runs.

    Only the job's own lease is refreshed: an unscoped refresh would keep every running lease for
    the service alive, so a wedged job whose own heartbeat died would never be reaped while any
    healthy job kept ticking (the orphaned-sync-row ingestion stall). With no ``job_name`` the
    loop refreshes nothing.
    """

    def __init__(self, service_id: str, job_name: str | None, *, interval_s: float | None = None) -> None:
        self._service_id = service_id
        self._job_name = job_name
        self._interval_s = DEFAULT_INTERVAL_S if interval_s is None else interval_s
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"cron-heartbeat-{service_id}")

    def __enter__(self) -> CronHeartbeat:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._stop.set()
        self._thread.join(timeout=_JOIN_TIMEOUT_S)

    def _run(self) -> None:
        from backend.core.metadata.base import get_con, release_thread_connection

        try:
            while self._job_name and not self._stop.wait(self._interval_s):
                try:
                    con = get_con(self._service_id)
                    con.execute(
                        "UPDATE job_runs SET heartbeat_at = ? "
                        "WHERE service_id = ? AND job_name = ? AND status = 'running'",
                        (time.time(), self._service_id, self._job_name),
                    )
                    con.commit()
                except Exception:
                    logger.debug("cron heartbeat refresh failed for %s", self._service_id, exc_info=True)
        finally:
            release_thread_connection()


def dev_mode_no_crons() -> bool:
    """Kill switch for local-dev runs: never schedule or execute cron work.

    Evaluates FLA_DEV_NO_CRONS environment variable.
    """
    import os

    return os.environ.get("FLA_DEV_NO_CRONS", "").lower() in ("1", "true", "yes")


# Watchdog threshold for reporting an overlong invocation. The wrapped body is
# still allowed to finish before the APScheduler worker is released: Python
# cannot safely kill a thread that owns DuckDB/FOS resources, and abandoning it
# leaves those resources active while the next tick starts.
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

    Watchdog: runs the wrapped function on a shared ThreadPoolExecutor and logs
    when it exceeds ``_CRON_HARD_CAP_S``. The future is then awaited to ensure
    DuckDB, FOS, and thread-local metadata resources are released before the
    APScheduler worker is freed; Python cannot safely cancel the body.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(service_id: str, *args, **kwargs):
            def _body():

                from backend.utils.telemetry import (
                    flush_usage_log,
                    process_context_scope,
                    start_call_tracking,
                )

                with process_context_scope(name):
                    start_call_tracking()
                    try:
                        return func(service_id, *args, **kwargs)
                    finally:
                        flush_usage_log(service_id)

            cap = _CRON_HARD_CAP_S

            with CronHeartbeat(service_id, job_name):
                fut = _get_watchdog_executor().submit(_body)
                try:
                    return fut.result(timeout=cap)
                except concurrent.futures.TimeoutError:
                    logger.error(
                        "[scheduler] %s/%s exceeded %ds watchdog threshold — waiting "
                        "for worker cleanup before releasing the scheduler slot",
                        name,
                        service_id,
                        cap,
                    )
                    return fut.result()

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
            from backend.utils.telemetry import process_context_scope

            prefix = f"\x1b[{color}m[{tag}]\x1b[0m"
            logger.info("🏎️  %s %s job started.", prefix, label)
            start = time.monotonic()
            try:
                with process_context_scope(f"cron:{job_id}"):
                    result = fn()
                if isinstance(result, tuple) and len(result) == 2:
                    status, detail = result
                else:
                    status, detail = "success", str(result or "")
                record_job_run(job_id, status, time.monotonic() - start, detail)
            except Exception as e:
                record_job_run(job_id, "error", time.monotonic() - start, str(e))
                logger.error("[%s] Failed: %s", job_id, e)
            logger.info("🏁  %s %s job finished.", prefix, label)

        return wrapper

    return decorator


_status_lock = threading.Lock()
_status: dict[str, dict] = {}


from backend.utils.date_utils import iso_z_now


def record_job_run(job_id: str, status: str, duration_s: float, detail: str = "") -> None:
    """Record the outcome of a completed global job run."""
    with _status_lock:
        _status[job_id] = {
            "last_run_at": iso_z_now(),
            "status": status,
            "duration_s": round(duration_s, 2),
            "detail": detail,
        }


def get_system_job_status() -> dict[str, dict]:
    """Return a snapshot of all recorded job statuses."""
    with _status_lock:
        return dict(_status)


# R-1: drain the job-status dict between tests so an earlier test's
# record_job_run doesn't bleed into the next test's snapshot read.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.system_jobs._status", _status)

__all__ = [
    "_CRON_HARD_CAP_S",
    "cron_task",
    "get_system_job_status",
    "global_job",
    "record_job_run",
]
