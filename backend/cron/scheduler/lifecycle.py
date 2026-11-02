"""Cron job lifecycle context manager, decorators, heartbeat, and preflight checks."""

from __future__ import annotations

import concurrent.futures
import logging
import os
import sys
import threading
import time
from collections.abc import Callable
from functools import wraps
from types import TracebackType
from typing import Any

from backend.cron.scheduler.progress import (
    add_progress,
    cleanup_progress_and_reap,
    end_progress,
    get_progress,
    start_progress,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import iso_z_now

logger = logging.getLogger("backend.scheduler")

DEFAULT_INTERVAL_S = 10.0
_JOIN_TIMEOUT_S = 1.0


class CronHeartbeat:
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
    return os.environ.get("FLA_DEV_NO_CRONS", "").lower() in ("1", "true", "yes")


_CRON_HARD_CAP_S = 300
_WATCHDOG_MAX_WORKERS = 12
_watchdog_lock = threading.Lock()
_watchdog_executor: concurrent.futures.ThreadPoolExecutor | None = None


def _get_watchdog_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _watchdog_executor
    with _watchdog_lock:
        if _watchdog_executor is None:
            _watchdog_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=_WATCHDOG_MAX_WORKERS, thread_name_prefix="cron-watchdog"
            )
        return _watchdog_executor


def cron_task(name: str, job_name: str | None = None):
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


def record_job_run(job_id: str, status: str, duration_s: float, detail: str = "") -> None:
    with _status_lock:
        _status[job_id] = {
            "last_run_at": iso_z_now(),
            "status": status,
            "duration_s": round(duration_s, 2),
            "detail": detail,
        }


def get_system_job_status() -> dict[str, dict]:
    with _status_lock:
        return dict(_status)


_CacheRegistry.register("utils.system_jobs._status", _status)


class cron_run:  # noqa: N801 — context-manager factory, lowercase by convention
    def __init__(
        self,
        service_id: str,
        task: str,
        *,
        source: dict | None = None,
        run_id: int | None = None,
        error_summary: str | Callable[[BaseException], str] | None = None,
        error_result: Any | Callable[[BaseException], Any] | None = None,
        finalize_duration: bool = False,
        finalize_running_backstop: bool = False,
        suppress_exceptions: bool = True,
        include_log_output: bool = True,
        skip_log_prefix: str | None = None,
        progress_task: str | None = None,
        backstop_summary: str | None = None,
        backstop_error_message: str | None = None,
    ) -> None:
        self.service_id = service_id
        self.task = task
        self._source = source
        self._run_id = run_id
        self.error_summary = error_summary
        self.error_result = error_result
        self.finalize_duration = finalize_duration
        self.finalize_running_backstop = finalize_running_backstop
        self.suppress_exceptions = suppress_exceptions
        self.include_log_output = include_log_output
        self.skip_log_prefix = skip_log_prefix
        self.progress_task = progress_task or task
        self.backstop_summary = backstop_summary
        self.backstop_error_message = backstop_error_message
        self.result: Any = None
        self.skipped: bool = False
        self.skip_reason: str | None = None
        self.aborted: bool = False
        self._recorded: bool = False
        self._start_time: float | None = None

    @property
    def source(self) -> dict | None:
        return self._source

    @property
    def run_id(self) -> int | None:
        return self._run_id

    @property
    def ok(self) -> bool:
        return not self.skipped and not self.aborted

    def elapsed(self) -> float:
        return 0.0 if self._start_time is None else time.time() - self._start_time

    def __enter__(self) -> cron_run:
        from backend.core.duckdb import get_source_for_service, start_cron_run
        from backend.core.metadata.usage_log import CronLeaseHeldError

        if self._source is None:
            self._source = get_source_for_service(self.service_id)
            if self._source is None:
                self.skipped = True
                self.skip_reason = "source not found"
                return self
        if self._run_id is None:
            try:
                self._run_id = start_cron_run(self._source, self.task)
            except CronLeaseHeldError as e:
                self.skipped = True
                self.skip_reason = str(e)
                self._log_skip(e)
                return self
        self._start_time = time.time()
        try:
            cleanup_progress_and_reap()
            start_progress(self._run_id, service_id=self.service_id, task=self.progress_task)
        except Exception as exc:
            self._record_exception(exc)
            self.aborted = True
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if self.skipped:
            return False
        if exc is not None and not self._recorded:
            self._record_exception(exc)
        self._run_finally()
        return self.suppress_exceptions if exc is not None else False

    def record(
        self,
        status: str,
        *,
        summary: str | None = None,
        error_message: str | None = None,
        **log_kwargs: Any,
    ) -> None:
        from backend.core.duckdb import log_cron_run

        if self._source is None:
            return
        log_output = _extract_log_text(self._run_id) if (self.include_log_output and self._run_id is not None) else None
        log_cron_run(
            self._source,
            self.task,
            self.elapsed(),
            status,
            summary=summary,
            error_message=error_message,
            run_id=self._run_id,
            log_output=log_output,
            **log_kwargs,
        )
        self._recorded = True

    def success(self, summary: str | None = None, **log_kwargs: Any) -> None:
        self.record("success", summary=summary, **log_kwargs)

    def warn(self, summary: str | None = None, *, error_message: str | None = None, **log_kwargs: Any) -> None:
        self.record("warning", summary=summary, error_message=error_message, **log_kwargs)

    def fail(self, summary: str | None = None, *, error_message: str | None = None, **log_kwargs: Any) -> None:
        self.record("error", summary=summary, error_message=error_message, **log_kwargs)

    def log_progress(self, event: dict, *, job_name: str | None = None) -> None:
        _log_and_add_progress(self._run_id, self.service_id, event, job_name=job_name or self.task)

    def _log_skip(self, exc: BaseException) -> None:
        prefix = self.skip_log_prefix or self.task
        logger.info("⏭️  [%s] %s: skipping — %s", prefix, self.service_id, str(exc))

    def _record_exception(self, exc: BaseException) -> None:
        summary = (
            self.error_summary(exc) if callable(self.error_summary) else (self.error_summary or f"{self.task} failed")
        )
        self.record("error", summary=summary, error_message=str(exc))
        self.log_progress({"type": "error", "message": str(exc)})
        logger.exception("[scheduler] %s: %s failed: %s", self.service_id, self.task, exc)
        if self.error_result is not None:
            self.result = self.error_result(exc) if callable(self.error_result) else self.error_result

    def _run_finally(self) -> None:
        def _finish_progress_and_duration() -> None:
            try:
                end_progress(self._run_id)
            finally:
                if self.finalize_duration:
                    self._finalize_duration()

        if self.finalize_running_backstop and self._source is not None:
            from backend.core.duckdb import finalize_cron_run_if_running

            backstop_kwargs: dict[str, Any] = {}
            if self.backstop_summary is not None or self.backstop_error_message is not None:
                backstop_kwargs = {
                    "duration_s": self.elapsed(),
                    "summary": self.backstop_summary,
                    "error_message": self.backstop_error_message,
                }
            try:
                finalize_cron_run_if_running(self._source, self.task, self._run_id, **backstop_kwargs)
            finally:
                _finish_progress_and_duration()
        else:
            _finish_progress_and_duration()

    def _finalize_duration(self) -> None:
        if self._source is None or self._start_time is None:
            return
        from backend.cron.jobs.sync import finalize_cron_duration

        finalize_cron_duration(self._source, self._run_id, self._start_time)


def record_skip_row(
    source: dict,
    task: str,
    summary: str,
    *,
    status: str = "skipped",
    duration_s: float = 0.0,
    error_message: str | None = None,
    run_id: int | None = None,
) -> None:
    from backend.core.duckdb import log_cron_run

    log_cron_run(source, task, duration_s, status, summary=summary, error_message=error_message, run_id=run_id)


def partial_hour_merge_enabled() -> bool:
    return os.environ.get("PARTIAL_HOUR_MERGE_ENABLED", "true").lower() in ("1", "true", "yes")


def partial_hour_merge_interval_sec() -> int:
    try:
        val = int(os.environ.get("PARTIAL_HOUR_MERGE_INTERVAL_SEC", "30"))
        return max(10, min(120, val))
    except (ValueError, TypeError):
        return 30


def dev_local_crons_enabled() -> bool:
    return os.environ.get("FLA_DEV_LOCAL_CRONS", "true").lower() in ("1", "true", "yes")


def _resolve_rum_sync_interval(rum_cfg: dict[str, Any], cfg: dict[str, Any], default_request_interval: int) -> int:
    if "sync_interval_seconds" in rum_cfg and rum_cfg["sync_interval_seconds"] is not None:
        try:
            return max(5, int(rum_cfg["sync_interval_seconds"]))
        except (ValueError, TypeError):
            pass
    rum_log_period = rum_cfg.get("log_period")
    if rum_log_period is None:
        rum_log_period = cfg.get("rum_log_period")
    if rum_log_period is not None:
        try:
            period_val = int(rum_log_period)
            if period_val >= 60:
                return max(5, period_val // 2)
            return max(5, period_val)
        except (ValueError, TypeError):
            pass
    return max(5, int(default_request_interval))


def _display_label(src: dict, service_id: str) -> str:
    from backend.cron.jobs.sync import get_service_display_label

    return get_service_display_label(service_id, src)


_HEAVY_REFRESH_INTERVAL_SEC = 60.0
_last_heavy_refresh: dict[str, float] = {}
_last_heavy_refresh_lock = threading.Lock()


def _claim_heavy_refresh(service_id: str) -> bool:
    now = time.time()
    with _last_heavy_refresh_lock:
        last = _last_heavy_refresh.get(service_id, 0.0)
        if (now - last) >= _HEAVY_REFRESH_INTERVAL_SEC:
            _last_heavy_refresh[service_id] = now
            return True
    return False


def _elapsed_since(start: float) -> str:
    s = time.time() - start
    return f"{int(s // 60)}m{int(s % 60):02d}s" if s >= 60 else f"{s:.1f}s"


def _service_has_alerts(service_id: str) -> bool:
    from backend.core import metadata as metadata_db

    try:
        return metadata_db.count_alerts(service_id) > 0
    except Exception:
        return True


_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _extract_log_text(run_id: int | None) -> str:
    if run_id is None:
        return ""
    evs = get_progress(run_id)
    if not evs:
        return ""
    return "\n".join(
        f"[{e.get('type', 'info').upper()}] {e['message']}"
        for e in evs
        if "message" in e and e.get("type") in ("error", "status", "done", "warning")
    )


JOB_COLORS = {
    "sync": "\x1b[94m",  # Bright Blue
    "commit": "\x1b[95m",  # Bright Magenta
    "metadata_cleanup": "\x1b[35m",  # Magenta
    "alerts": "\x1b[93m",  # Bright Yellow
    "optimize": "\x1b[92m",  # Bright Green
    "expire": "\x1b[90m",  # Gray
    "ngwaf_sync": "\x1b[36m",  # Cyan
    "usage_log": "\x1b[32m",  # Green
}
RESET_COLOR = "\x1b[0m"
TYPE_ICONS = {
    "error": "❌ ",  # Added trailing space to prevent terminal width collision
    "warning": "⚠️ ",
    "done": "✅ ",
    "status": "ℹ️ ",
    "progress": "⏳ ",
    "sync": "⬇️  ",
    "commit": "💾 ",
    "optimize": "🔨 ",
    "expire": "🗑️ ",
    "alerts": "🔔 ",
    "ngwaf_sync": "👾 ",
    "usage_log": "📊 ",
}


def _log_and_add_progress(
    run_id: int | None, service_id: str, event: dict, job_name: str = "scheduler", service_name: str | None = None
) -> None:
    from backend.cron.jobs.sync import get_service_display_label

    add_progress(run_id, event)
    msg = event.get("message")
    if msg:
        display = get_service_display_label(service_id)
        t = event.get("type", "info")
        if t == "status":
            return
        c = JOB_COLORS.get(job_name, "")
        c_end = RESET_COLOR if c else ""
        if t == "info" and job_name in TYPE_ICONS:
            icon = TYPE_ICONS[job_name]
        else:
            icon = TYPE_ICONS.get(t, "ℹ️ ")
        display_job_name = job_name
        if display_job_name == "sync":
            display_job_name = "log_discovery"
        prefix = f"{icon}{c}[{display_job_name}]{c_end}"
        log = logger
        if t == "error":
            log.error("%s %s: %s", prefix, display, msg)
        elif t == "warning":
            log.warning("%s %s: %s", prefix, display, msg)
        else:
            log.info("%s %s: %s", prefix, display, msg)


_DISK_FREE_HARD_FLOOR_BYTES = 500 * 1024 * 1024
_DISK_FREE_HARD_FLOOR_PCT = 0.005  # 0.5 %


def _check_disk_space(cache_dir: str, service_id: str, job_name: str) -> tuple[bool, str]:
    import shutil

    try:
        usage = shutil.disk_usage(cache_dir if os.path.isdir(cache_dir) else ".")
    except OSError as e:
        logger.warning("[scheduler] %s: disk-space probe failed for %s: %s", service_id, cache_dir, e)
        return True, ""  # don't block on probe failure — let the job try and fail naturally
    free_pct = usage.free / usage.total if usage.total else 1.0
    if usage.free < _DISK_FREE_HARD_FLOOR_BYTES or free_pct < _DISK_FREE_HARD_FLOOR_PCT:
        free_mb = usage.free // (1024 * 1024)
        total_gb = usage.total / (1024 * 1024 * 1024)
        msg = f"disk almost full: {free_mb} MB free ({free_pct * 100:.1f}% of {total_gb:.1f} GiB)"
        logger.error("💾 \x1b[31m[disk]\x1b[0m %s [%s]: refusing to start — %s", service_id, job_name, msg)
        return False, msg
    return True, ""


_BACKLOG_FILE_COUNT_WARN = 200
_BACKLOG_AGE_MULTIPLIER = 3
_BACKLOG_BYTES_WARN = 1 * 1024 * 1024 * 1024


def _check_buffer_backlog(src: dict, service_id: str, commit_interval_mins: int) -> str:
    try:
        from backend.core import storage as db_storage

        stats = db_storage.buffer_backlog_stats(src)
    except Exception as e:
        logger.warning("[scheduler] %s: buffer backlog probe failed: %s", service_id, e)
        return ""
    file_count = int(stats.get("file_count", 0) or 0)
    total_bytes = int(stats.get("total_bytes", 0) or 0)
    oldest_age_s = int(stats.get("oldest_age_seconds", 0) or 0)
    if file_count == 0:
        return ""
    max_oldest_age_s = max(60, commit_interval_mins * 60 * _BACKLOG_AGE_MULTIPLIER)
    problems: list[str] = []
    if file_count > _BACKLOG_FILE_COUNT_WARN:
        problems.append(f"{file_count} files")
    if oldest_age_s > max_oldest_age_s:
        problems.append(f"oldest {oldest_age_s // 60}m old")
    if total_bytes > _BACKLOG_BYTES_WARN:
        problems.append(f"{total_bytes // (1024 * 1024)}MB on disk")
    if not problems:
        return ""
    msg = "buffer backlog: " + ", ".join(problems)
    logger.warning(
        "🪣 \x1b[33m[backlog]\x1b[0m %s: %s — commits may be failing silently or ingest is outrunning commit",
        service_id,
        msg,
    )
    return f" ⚠ {msg}"


_CacheRegistry.register("cron.scheduler._last_heavy_refresh", _last_heavy_refresh)

__all__ = [
    "CronHeartbeat",
    "DEFAULT_INTERVAL_S",
    "JOB_COLORS",
    "RESET_COLOR",
    "TYPE_ICONS",
    "_BACKLOG_AGE_MULTIPLIER",
    "_BACKLOG_BYTES_WARN",
    "_BACKLOG_FILE_COUNT_WARN",
    "_CRON_HARD_CAP_S",
    "_DISK_FREE_HARD_FLOOR_BYTES",
    "_DISK_FREE_HARD_FLOOR_PCT",
    "_HEAVY_REFRESH_INTERVAL_SEC",
    "_JOIN_TIMEOUT_S",
    "_ROOT",
    "_WATCHDOG_MAX_WORKERS",
    "_check_buffer_backlog",
    "_check_disk_space",
    "_claim_heavy_refresh",
    "_display_label",
    "_elapsed_since",
    "_extract_log_text",
    "_get_watchdog_executor",
    "_last_heavy_refresh",
    "_last_heavy_refresh_lock",
    "_log_and_add_progress",
    "_resolve_rum_sync_interval",
    "_service_has_alerts",
    "_status",
    "_status_lock",
    "_watchdog_executor",
    "_watchdog_lock",
    "add_progress",
    "cleanup_progress_and_reap",
    "cron_run",
    "cron_task",
    "dev_local_crons_enabled",
    "dev_mode_no_crons",
    "end_progress",
    "get_progress",
    "get_system_job_status",
    "global_job",
    "iso_z_now",
    "logger",
    "partial_hour_merge_enabled",
    "partial_hour_merge_interval_sec",
    "record_job_run",
    "record_skip_row",
    "start_progress",
    "time",
]
