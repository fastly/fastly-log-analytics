from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger("backend.scheduler")
import concurrent.futures  # noqa: E402
from functools import wraps  # noqa: E402
from types import TracebackType  # noqa: E402

from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402
from backend.utils.date_utils import iso_z_now  # noqa: E402

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
        from backend.core.metadata.cron_log import CronLeaseHeldError

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
            from backend.cron_progress import cleanup_progress_and_reap, start_progress

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
        from backend.cron_progress import end_progress

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


sys.modules["backend.cron.decorators"] = sys.modules[__name__]
sys.modules["backend.cron.lifecycle"] = sys.modules[__name__]
_cron_pkg = sys.modules.get("backend.cron")
if _cron_pkg is not None:
    setattr(_cron_pkg, "decorators", sys.modules[__name__])
    setattr(_cron_pkg, "lifecycle", sys.modules[__name__])
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES["backend.cron.decorators"] = "backend.cron.scheduler"
    _backend_pkg._MODULE_ALIASES["backend.cron.lifecycle"] = "backend.cron.scheduler"


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


_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _extract_log_text(run_id: int | None) -> str:
    from backend.cron_progress import get_progress

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
    from backend.cron_progress import add_progress

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
        from backend.core import iceberg as db_iceberg

        stats = db_iceberg.buffer_backlog_stats(src)
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


class Scheduler:
    def __init__(self) -> None:
        from apscheduler.executors.pool import ThreadPoolExecutor as APSThreadPoolExecutor
        from apscheduler.schedulers.background import BackgroundScheduler

        executors = {
            "default": APSThreadPoolExecutor(10),
            "streaming": APSThreadPoolExecutor(10),
            "maintenance": APSThreadPoolExecutor(3),
        }
        self._sched = BackgroundScheduler(timezone=UTC, executors=executors)
        self._job_ids: dict[str, str] = {}  # job_id -> job_id

    def start(self) -> None:
        if dev_mode_no_crons():
            logger.warning(
                "🚫 [scheduler] FLA_DEV_NO_CRONS=1 — skipping all FOS-writing / ingest / outbound crons "
                "(sync/full_sweep/gap_heal/commit/optimize/expire/ngwaf_sync/metadata_cleanup/"
                "bot_data_refresh/rdns/alerts). Registering ONLY local-safe jobs (local_compact, "
                "rollup_compact, insights_prewarmer) that touch the local cache only. "
                "HTTP API calls (including /api/admin/rebuild-local-view) remain functional."
            )
            self._register_dev_local_safe_jobs()
            if self._job_ids:
                self._sched.start()
            logger.info(
                "🟢 [scheduler] Started in dev-local-safe mode (pid: %d). %d local-only job(s) registered.",
                os.getpid(),
                len(self._job_ids),
            )
            return
        self._sync_jobs()
        self._sched.start()
        logger.info("🟢 [scheduler] Started (pid: %d). %d job(s) registered.", os.getpid(), len(self._job_ids))
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.core.duckdb_pool import (
            _pool_warm_at_boot_count,
            _pool_warm_at_boot_enabled,
            warm_pool_at_startup,
        )

        warm_enabled = _pool_warm_at_boot_enabled()
        warm_count = _pool_warm_at_boot_count() if warm_enabled else 0
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id")
            if not service_id:
                continue
            if warm_enabled:
                src = get_source_for_service(service_id)
                if src and is_configured(src):
                    try:
                        built = warm_pool_at_startup(service_id, src, count=warm_count)
                        logger.info(
                            "🔥 [pool] %s: warm_pool_at_startup built %d/%d idle conns",
                            service_id,
                            built,
                            warm_count,
                        )
                    except Exception as e:
                        logger.warning(
                            "[pool] %s: warm_pool_at_startup raised (continuing startup): %s",
                            service_id,
                            e,
                        )

    def shutdown(self, *, wait: bool = False) -> None:
        try:
            self._sched.shutdown(wait=wait)
        except Exception:
            pass
        logger.info("[scheduler] Stopped.")

    def _register_once_job(
        self,
        job_id: str,
        func,
        trigger: str,
        seen_ids: set[str] | None = None,
        log_msg: str | None = None,
        *log_args,
        **kwargs,
    ) -> bool:
        if seen_ids is not None:
            seen_ids.add(job_id)
        if job_id in self._job_ids:
            return False
        self._add_job(func, trigger, id=job_id, max_instances=1, coalesce=True, **kwargs)
        self._job_ids[job_id] = job_id
        if log_msg:
            logger.info(log_msg, *log_args)
        return True

    def _register_reschedulable_job(
        self,
        job_id: str,
        func,
        trigger: str,
        seen_ids: set[str] | None = None,
        log_msg: str | None = None,
        *log_args,
        reschedule_kwargs: dict | None = None,
        max_instances: int = 1,
        **kwargs,
    ) -> bool:
        if seen_ids is not None:
            seen_ids.add(job_id)
        add_kw = {"id": job_id, "coalesce": True, **kwargs}
        if max_instances:
            add_kw["max_instances"] = max_instances
        added = self._reschedule_or_add_job(func, trigger, reschedule_kwargs=reschedule_kwargs, **add_kw)
        if added and log_msg:
            logger.info(log_msg, *log_args)
        return added

    def _register_alerts_evaluation_job(
        self, service_id: str, seconds: int, seen_ids: set[str], enabled: bool = True
    ) -> None:
        if not enabled or not _service_has_alerts(service_id):
            return
        from backend.cron.jobs.metadata import _run_service_alerts_evaluation

        alert_job_id = f"alerts_evaluation_{service_id}"
        self._register_reschedulable_job(
            alert_job_id,
            _run_service_alerts_evaluation,
            "interval",
            seen_ids,
            "🔔 [scheduler] Registered alerts evaluation job %s (every %ds).",
            alert_job_id,
            seconds,
            seconds=seconds,
            args=[service_id],
            misfire_grace_time=60,
        )

    def _register_insights_prewarmer_job(self, service_id: str, seen_ids: set[str], prov: dict) -> None:
        from backend.cron.jobs.insights_prewarmer import _run_insights_prewarmer

        cron_ip = prov.get("cron_insights_prewarmer", {})
        if not cron_ip.get("enabled", True):
            return
        interval_seconds = int(cron_ip.get("interval_seconds", 240))
        ip_job_id = f"insights_prewarmer_{service_id}"
        self._register_reschedulable_job(
            ip_job_id,
            _run_insights_prewarmer,
            "interval",
            seen_ids,
            "🔥 [scheduler] Registered insights_prewarmer job %s (every %ds).",
            ip_job_id,
            interval_seconds,
            seconds=interval_seconds,
            jitter=15,
            reschedule_kwargs={"seconds": interval_seconds, "jitter": 15},
            args=[service_id],
            misfire_grace_time=360,
        )

    def _register_recycle_job(self, seen_ids: set[str] | None = None) -> None:
        from backend.core.duckdb_recycle import recycle_interval_min
        from backend.cron.jobs.metadata import run_duckdb_recycle

        recycle_interval = recycle_interval_min()
        if recycle_interval <= 0:
            return
        self._register_once_job(
            "duckdb_recycle",
            run_duckdb_recycle,
            "interval",
            seen_ids,
            "♻️  \x1b[35m[recycle]\x1b[0m Registered DuckDB instance recycle job (every %gm, executor=maintenance).",
            recycle_interval,
            minutes=recycle_interval,
            misfire_grace_time=120,
            executor="maintenance",
        )

    def _register_metric_snapshot_job(self, seen_ids: set[str] | None = None) -> None:
        from backend.cron.jobs.metric_snapshot import _run_metric_snapshot

        self._register_once_job(
            "metric_snapshot",
            _run_metric_snapshot,
            "interval",
            seen_ids,
            "📈 \x1b[34m[metric_snapshot]\x1b[0m Registered metric snapshot job (every 60s).",
            seconds=60,
            jitter=5,
            misfire_grace_time=60,
        )

    def _sync_local_compaction_jobs(
        self,
        service_id: str,
        cfg: dict,
        seen_ids: set[str] | None = None,
        *,
        dev_local: bool = False,
    ) -> None:
        from backend.cron.jobs.metadata import (
            _run_local_compact,
            _run_partial_hour_merge,
            _run_rollup_compact_daily,
            _run_rollup_hour_heal,
        )

        prov = cfg.get("provisioning", {})
        compact_cfg = prov.get("cron_compact", {})
        tag = "(dev-local) " if dev_local else ""
        lc_job_id = f"local_compact_{service_id}"
        lc_interval_mins = max(1, int(os.environ.get("LOCAL_COMPACT_INTERVAL_MIN", "2")))
        self._register_once_job(
            lc_job_id,
            _run_local_compact,
            "interval",
            seen_ids,
            "⚙️  [scheduler] %sRegistered %s%s (every %d min, local-only).",
            tag,
            "" if dev_local else "local_compact job ",
            lc_job_id,
            lc_interval_mins,
            minutes=lc_interval_mins,
            jitter=10,
            args=[service_id],
            misfire_grace_time=60,
        )
        if partial_hour_merge_enabled():
            ph_job_id = f"partial_hour_merge_{service_id}"
            ph_interval_sec = partial_hour_merge_interval_sec()
            self._register_once_job(
                ph_job_id,
                _run_partial_hour_merge,
                "interval",
                seen_ids,
                "⚡ [scheduler] %sRegistered %s%s (every %ds, local-only).",
                tag,
                "" if dev_local else "partial_hour_merge job ",
                ph_job_id,
                ph_interval_sec,
                seconds=ph_interval_sec,
                jitter=5,
                args=[service_id],
                misfire_grace_time=60,
            )
        if compact_cfg.get("enabled", True):
            rc_job_id = f"rollup_compact_{service_id}"
            self._register_once_job(
                rc_job_id,
                _run_rollup_compact_daily,
                "cron",
                seen_ids,
                "📦 [scheduler] %sRegistered %s%s (daily 02:00 UTC).",
                tag,
                "" if dev_local else "rollup compaction job ",
                rc_job_id,
                hour=2,
                minute=0,
                args=[service_id],
                misfire_grace_time=3600,
            )
            rh_job_id = f"rollup_heal_{service_id}"
            self._register_once_job(
                rh_job_id,
                _run_rollup_hour_heal,
                "cron",
                seen_ids,
                "🩹 [scheduler] %sRegistered %s%s (hourly at :05).",
                tag,
                "" if dev_local else "rollup hour-heal job ",
                rh_job_id,
                minute=5,
                next_run_time=datetime.now(UTC) + timedelta(seconds=5),
                args=[service_id],
                misfire_grace_time=900,
            )

    def _register_dev_local_safe_jobs(self) -> None:
        if not dev_local_crons_enabled():
            logger.warning("🚫 [scheduler] FLA_DEV_LOCAL_CRONS=0 — no dev-local-safe jobs registered.")
            return
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

        seen_ids: set[str] = set()
        for cfg in svcconfig.list_configs():
            service_id = cfg.get("service_id", "")
            if not service_id:
                continue
            src = get_source_for_service(service_id)
            if not src or not is_configured(src):
                continue
            if get_high_scale_service_registry().resolve(service_id) is not None:
                logger.info(
                    "⏭️  [scheduler] (dev-local) Skipping legacy local jobs for high-scale service %s.", service_id
                )
                continue
            prov = cfg.get("provisioning", {})
            self._sync_local_compaction_jobs(service_id, cfg, dev_local=True)
            self._register_insights_prewarmer_job(service_id, seen_ids, prov)
        self._register_metric_snapshot_job()
        self._register_recycle_job()

    def _sync_jobs(self) -> None:
        from backend import config as svcconfig
        from backend.core.duckdb import get_source_for_service, is_configured
        from backend.cron.jobs.metadata import (
            _run_bot_data_refresh,
            _run_expire_snapshots,
            _run_metadata_cleanup,
            _run_ngwaf_bot_sync,
            _run_optimize,
            _run_rdns_enrichment,
            _run_share_audit_purge,
        )
        from backend.cron.jobs.sync import _run_commit, _run_full_sweep, _run_gap_heal, _run_rum_commit, _run_rum_sync
        from backend.cron.jobs.sync import _run_log_discovery_cron as _run_service_cron

        configs = svcconfig.list_configs()
        seen_ids: set[str] = set()
        for cfg in configs:
            service_id = cfg.get("service_id", "")
            if not service_id:
                continue
            src = get_source_for_service(service_id)
            if not src or not is_configured(src):
                logger.warning("[scheduler] %s: service not fully configured, skipping jobs.", service_id)
                continue
            prov = cfg.get("provisioning", {})
            sync_cfg = prov.get("cron_sync", {})
            if not sync_cfg.get("enabled", True):
                continue
            log_period = int(cfg.get("log_period", 60))
            if sync_cfg.get("interval_mins"):
                interval_seconds = max(5, int(sync_cfg["interval_mins"]) * 60)
            elif sync_cfg.get("interval_seconds"):
                interval_seconds = max(5, int(sync_cfg["interval_seconds"]))
            else:
                interval_seconds = max(5, log_period // 2 if log_period >= 60 else log_period)
            commit_interval_mins = max(1, int(sync_cfg.get("commit_interval_mins", 5)))
            job_id = f"log_discovery_{service_id}"
            seen_ids.add(job_id)
            if job_id in self._job_ids:
                try:
                    job = self._sched.get_job(job_id)
                    if job:
                        job.reschedule("interval", seconds=interval_seconds)
                        logger.info(
                            "[scheduler] Rescheduled log_discovery job %s to every %ds.", job_id, interval_seconds
                        )
                except Exception as e:
                    logger.error("[scheduler] Failed to reschedule sync job %s: %s", job_id, e)
            else:
                self._add_job(
                    _run_service_cron,
                    "interval",
                    seconds=interval_seconds,
                    jitter=2 if interval_seconds >= 5 else 1,
                    start_date=None,
                    args=[service_id],
                    id=job_id,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                self._job_ids[job_id] = job_id
                logger.info("🔄 [scheduler] Registered log_discovery job %s (every %ds).", job_id, interval_seconds)
            commit_job_id = f"commit_{service_id}"
            self._register_reschedulable_job(
                commit_job_id,
                _run_commit,
                "interval",
                seen_ids,
                "📦 [scheduler] Registered commit job %s (every %dm).",
                commit_job_id,
                commit_interval_mins,
                minutes=commit_interval_mins,
                jitter=30,
                args=[service_id],
                misfire_grace_time=commit_interval_mins * 60,
            )
            if svcconfig.is_high_scale_mode(src):
                from backend.cron.jobs.sync import _run_ledger_sweep

                sweep_cfg = prov.get("cron_ledger_sweep", {})
                if sweep_cfg.get("enabled", True):
                    sweep_interval_mins = max(1, int(sweep_cfg.get("interval_minutes", 15)))
                    sweep_job_id = f"ledger_sweep_{service_id}"
                    self._register_reschedulable_job(
                        sweep_job_id,
                        _run_ledger_sweep,
                        "interval",
                        seen_ids,
                        "🧹 [scheduler] Registered ledger_sweep job %s (every %dm).",
                        sweep_job_id,
                        sweep_interval_mins,
                        minutes=sweep_interval_mins,
                        args=[service_id],
                        misfire_grace_time=300,
                    )
            rum_cfg = cfg.get("rum")
            rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
            rum_enabled = svcconfig.is_rum_enabled(cfg)
            if rum_enabled and svcconfig.is_high_scale_mode(src):
                from backend.cron.jobs.sync import _run_rum_discovery_cron, _run_rum_ledger_sweep

                rum_disc_interval_secs = _resolve_rum_sync_interval(rum_cfg, cfg, interval_seconds)
                rum_disc_job_id = f"rum_discovery_{service_id}"
                self._register_reschedulable_job(
                    rum_disc_job_id,
                    _run_rum_discovery_cron,
                    "interval",
                    seen_ids,
                    "[scheduler] Registered RUM discovery job %s (every %ds).",
                    rum_disc_job_id,
                    rum_disc_interval_secs,
                    seconds=rum_disc_interval_secs,
                    args=[service_id],
                    misfire_grace_time=60,
                )
                rum_sweep_cfg = prov.get("cron_ledger_rum_sweep") or prov.get("cron_rum_ledger_sweep") or {}
                if rum_sweep_cfg.get("enabled", True):
                    rum_sweep_interval_mins = max(1, int(rum_sweep_cfg.get("interval_minutes", 15)))
                    rum_sweep_job_id = f"ledger_rum_sweep_{service_id}"
                    self._register_reschedulable_job(
                        rum_sweep_job_id,
                        _run_rum_ledger_sweep,
                        "interval",
                        seen_ids,
                        "🧹 [scheduler] Registered ledger_rum_sweep job %s (every %dm).",
                        rum_sweep_job_id,
                        rum_sweep_interval_mins,
                        minutes=rum_sweep_interval_mins,
                        args=[service_id],
                        misfire_grace_time=300,
                    )
            elif rum_enabled:
                rum_sync_interval_secs = _resolve_rum_sync_interval(rum_cfg, cfg, interval_seconds)
                rum_sync_job_id = f"rum_sync_{service_id}"
                self._register_reschedulable_job(
                    rum_sync_job_id,
                    _run_rum_sync,
                    "interval",
                    seen_ids,
                    "[scheduler] Registered RUM sync job %s (every %ds).",
                    rum_sync_job_id,
                    rum_sync_interval_secs,
                    seconds=rum_sync_interval_secs,
                    args=[service_id],
                    misfire_grace_time=60,
                )
                rum_commit_interval_mins = max(1, int(rum_cfg.get("commit_interval_mins", 1)))
                rum_commit_job_id = f"rum_commit_{service_id}"
                self._register_reschedulable_job(
                    rum_commit_job_id,
                    _run_rum_commit,
                    "interval",
                    seen_ids,
                    "[scheduler] Registered RUM commit job %s (every %dm).",
                    rum_commit_job_id,
                    rum_commit_interval_mins,
                    minutes=rum_commit_interval_mins,
                    args=[service_id],
                    misfire_grace_time=60,
                )
            alerts_cfg = prov.get("cron_alerts", {})
            alerts_enabled = alerts_cfg.get("enabled", True)
            alerts_seconds = int(alerts_cfg.get("interval_seconds") or log_period)
            self._register_alerts_evaluation_job(service_id, alerts_seconds, seen_ids, enabled=alerts_enabled)
            full_sweep_cfg = prov.get("cron_full_sweep", {})
            if full_sweep_cfg.get("enabled", True):
                full_job_id = f"full_sync_{service_id}"
                cron_hours = str(full_sweep_cfg.get("cron_hours", "3,9,15,21"))
                cron_minute = int(full_sweep_cfg.get("cron_minute", 30))
                self._register_reschedulable_job(
                    full_job_id,
                    _run_full_sweep,
                    "cron",
                    seen_ids,
                    "🔍 [scheduler] Registered full-sweep job %s (hours: %s at :%02d UTC).",
                    full_job_id,
                    cron_hours,
                    cron_minute,
                    hour=cron_hours,
                    minute=cron_minute,
                    args=[service_id],
                    misfire_grace_time=3600,
                )
            heal_cfg = prov.get("cron_gap_heal", {})
            has_logging_svc = bool(cfg.get("logging_service_id") or cfg.get("service_id"))
            if heal_cfg.get("enabled", True) and has_logging_svc:
                heal_job_id = f"gap_heal_{service_id}"
                interval_mins = int(heal_cfg.get("interval_minutes", 30))
                self._register_reschedulable_job(
                    heal_job_id,
                    _run_gap_heal,
                    "interval",
                    seen_ids,
                    "🩹 [scheduler] Registered gap-heal job %s (every %d min).",
                    heal_job_id,
                    interval_mins,
                    minutes=interval_mins,
                    args=[service_id],
                    misfire_grace_time=600,
                )
            compact_cfg = prov.get("cron_compact", {})
            if compact_cfg.get("enabled", True):
                opt_job_id = f"optimize_{service_id}"
                self._register_once_job(
                    opt_job_id,
                    _run_optimize,
                    "cron",
                    seen_ids,
                    "⚙️  [scheduler] Registered optimize job %s (daily 04:00 UTC). Local compact handles ongoing dashboard perf — this is just FOS-side housekeeping.",
                    opt_job_id,
                    hour=4,
                    minute=0,
                    args=[service_id],
                    misfire_grace_time=3600,
                )
            self._sync_local_compaction_jobs(service_id, cfg, seen_ids=seen_ids, dev_local=False)
            self._register_insights_prewarmer_job(service_id, seen_ids, prov)
            if compact_cfg.get("enabled", True) and not dev_mode_no_crons():
                exp_job_id = f"expire_{service_id}"
                seen_ids.add(exp_job_id)
                expire_interval_mins = max(5, int(sync_cfg.get("expire_interval_mins", 60)))
                if exp_job_id not in self._job_ids:
                    self._add_job(
                        _run_expire_snapshots,
                        "interval",
                        minutes=expire_interval_mins,
                        jitter=60,
                        args=[service_id],
                        id=exp_job_id,
                        max_instances=1,
                        coalesce=True,
                        misfire_grace_time=3600,
                    )
                    self._job_ids[exp_job_id] = exp_job_id
                    logger.info(
                        "🗑️  [scheduler] Registered expire-snapshots job %s (every %dm).",
                        exp_job_id,
                        expire_interval_mins,
                    )
                else:
                    job = self._sched.get_job(exp_job_id)
                    if job is not None:
                        existing = getattr(getattr(job, "trigger", None), "interval", None)
                        if existing is None or int(existing.total_seconds()) != expire_interval_mins * 60:
                            job.reschedule("interval", minutes=expire_interval_mins)
                            logger.info(
                                "🗑️  [scheduler] Rescheduled expire-snapshots job %s to every %dm.",
                                exp_job_id,
                                expire_interval_mins,
                            )
            if svcconfig.get_ngwaf_workspace_id(service_id):
                ngwaf_interval_mins = max(1, int(prov.get("cron_ngwaf", {}).get("interval_mins", 5)))
                ngwaf_job_id = f"ngwaf_sync_{service_id}"
                self._register_reschedulable_job(
                    ngwaf_job_id,
                    _run_ngwaf_bot_sync,
                    "interval",
                    seen_ids,
                    "👾 \x1b[36m[ngwaf_sync]\x1b[0m Registered NGWAF bot sync job %s (every %dm, executor=maintenance).",
                    ngwaf_job_id,
                    ngwaf_interval_mins,
                    minutes=ngwaf_interval_mins,
                    jitter=15,
                    args=[service_id],
                    misfire_grace_time=300,
                    executor="maintenance",
                )
            cleanup_cfg = prov.get("cron_metadata_cleanup", {})
            if cleanup_cfg.get("enabled", True) and not dev_mode_no_crons():
                cleanup_job_id = f"metadata_cleanup_{service_id}"
                cron_hour = int(cleanup_cfg.get("cron_hour", 3))
                cron_minute = int(cleanup_cfg.get("cron_minute", 15))
                self._register_reschedulable_job(
                    cleanup_job_id,
                    _run_metadata_cleanup,
                    "cron",
                    seen_ids,
                    "🧹 \x1b[35m[metadata_cleanup]\x1b[0m Registered metadata cleanup job %s (daily %02d:%02d UTC).",
                    cleanup_job_id,
                    cron_hour,
                    cron_minute,
                    hour=cron_hour,
                    minute=cron_minute,
                    args=[service_id],
                    misfire_grace_time=3600,
                )
        self._register_once_job(
            "bot_data_refresh",
            _run_bot_data_refresh,
            "cron",
            seen_ids,
            "👾 \x1b[36m[bots]\x1b[0m Registered bot data refresh job (daily 02:00 UTC).",
            hour=2,
            minute=0,
            misfire_grace_time=3600,
        )
        self._register_once_job(
            "rdns_enrichment",
            _run_rdns_enrichment,
            "interval",
            seen_ids,
            "🌐 \x1b[34m[rdns]\x1b[0m Registered rDNS enrichment job (every 5m).",
            minutes=5,
            jitter=15,
            misfire_grace_time=300,
        )
        self._register_once_job(
            "share_audit_purge",
            _run_share_audit_purge,
            "cron",
            seen_ids,
            "🧹 \x1b[35m[share_audit_purge]\x1b[0m Registered share audit purge job (daily 03:45 UTC, executor=maintenance).",
            hour=3,
            minute=45,
            misfire_grace_time=3600,
        )
        self._register_metric_snapshot_job(seen_ids)
        self._register_recycle_job(seen_ids)
        for jid in list(self._job_ids.keys()):
            if jid in seen_ids:
                continue
            try:
                self._sched.remove_job(jid)
            except Exception:
                pass
            logger.info("[scheduler] Removed stale job %s.", jid)
            del self._job_ids[jid]

    def _reschedule_or_add_job(self, func, trigger=None, *, reschedule_kwargs: dict | None = None, **kwargs) -> bool:
        job_id = kwargs["id"]
        if job_id in self._job_ids:
            if reschedule_kwargs is None:
                keys = ("seconds", "minutes", "hours") if trigger == "interval" else ("hour", "minute")
                reschedule_kwargs = {k: kwargs[k] for k in keys if k in kwargs}
            try:
                job = self._sched.get_job(job_id)
                if job:
                    job.reschedule(trigger, **reschedule_kwargs)
            except Exception:
                pass
            return False
        self._add_job(func, trigger, **kwargs)
        self._job_ids[job_id] = job_id
        return True

    def _add_job(self, func, trigger=None, **kwargs):
        job_id = kwargs.pop("id", None)
        args = kwargs.pop("args", [])
        if "executor" not in kwargs and job_id:
            jid = str(job_id)
            if jid.startswith(("log_discovery_", "rum_discovery_", "rum_sync_", "commit_", "sync_")):
                kwargs["executor"] = "streaming"
            elif jid.startswith(
                (
                    "insights_prewarmer_",
                    "local_compact_",
                    "rollup_compact_",
                    "rollup_heal_",
                    "metric_snapshot",
                    "reconcile_fastly_",
                    "metadata_cleanup",
                    "rdns_enrichment",
                    "rdns_refresh",
                    "bot_data_refresh",
                    "optimize_",
                    "ngwaf_sync_",
                    "share_audit_purge",
                    "duckdb_recycle",
                )
            ):
                kwargs["executor"] = "maintenance"
            else:
                kwargs["executor"] = "default"
        self._sched.add_job(func, trigger, id=job_id, args=args, **kwargs)
        self._job_ids[job_id] = job_id

    def reload(self) -> None:
        if dev_mode_no_crons():
            logger.warning("🚫 [scheduler] FLA_DEV_NO_CRONS=1 — Scheduler.reload() ignored.")
            return
        self._sync_jobs()

    def get_job(self, job_id: str):
        return self._sched.get_job(job_id)


_scheduler: Scheduler | None = None


def get_scheduler() -> Scheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = Scheduler()
    return _scheduler


from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("cron.scheduler._last_heavy_refresh", _last_heavy_refresh)
TASK_MAP = {
    "log_discovery": "log_discovery",
    "rum_sync": "rum_sync",
    "full_sync": "full_sync",
    "gap_heal": "gap_heal",
    "commit": "commit",
    "rum_commit": "rum_commit",
    "optimize": "optimize",
    "local_compact": "local_compact",
    "expire": "expire_snapshots",
    "alerts_evaluation": "alerts",
    "ngwaf_sync": "ngwaf_sync",
    "metadata_cleanup": "metadata_cleanup",
    "insights_prewarmer": "insights_prewarmer",
    "rollup_heal": "rollup_hour_heal",
    "rollup_compact": "rollup_compact_daily",
    "ledger_sweep": "ledger_sweep",
    "partial_hour_merge": "partial_hour_merge",
    "rum_discovery": "rum_discovery",
    "ledger_rum_sweep": "ledger_rum_sweep",
}


def build_cron_schedule_payload(source: dict) -> dict:
    from backend.core import metadata as metadata_db

    sched = get_scheduler()
    service_id = source["name"]
    last_runs: dict[str, dict] = {}
    try:
        per_task = metadata_db.latest_cron_per_task(service_id)
        for task, info in per_task.items():
            last_runs[task] = {
                "last_run_time": info["started_at"],
                "last_run_status": info["status"],
                "last_run_duration_s": info["duration_s"],
                "last_run_summary": info["summary"],
            }
    except Exception:
        pass
    schedules = []
    for job in sched._sched.get_jobs():
        job_id = getattr(job, "id", "")
        if not job_id.endswith(f"_{service_id}"):
            continue
        task_name = job_id[: -len(f"_{service_id}")]
        db_task = TASK_MAP.get(task_name)
        if db_task is None:
            continue
        from backend.utils.date_utils import iso_z

        next_run = iso_z(job.next_run_time) if job.next_run_time else None  # type: ignore
        schedules.append({"task": db_task, "next_run_time": next_run, **last_runs.get(db_task, {})})
    existing = {s["task"] for s in schedules}
    for task, info in last_runs.items():
        if task not in existing and task in TASK_MAP.values():
            schedules.append({"task": task, "next_run_time": None, **info})
    try:
        if metadata_db.count_alerts(service_id) == 0:
            alerts_entry = next((s for s in schedules if s["task"] == "alerts"), None)
            if alerts_entry is None:
                schedules.append(
                    {
                        "task": "alerts",
                        "next_run_time": None,
                        "disabled_reason": "no_alerts_configured",
                    }
                )
            elif alerts_entry.get("next_run_time") is None:
                alerts_entry["disabled_reason"] = "no_alerts_configured"
    except Exception:
        pass
    return {"schedules": schedules}


from backend.celery_app import _InProcessPublisher  # noqa: E402
from backend.config import SSE_BACKPLANE  # noqa: E402
from backend.utils.dir_size import _get_dir_size  # noqa: E402

_progress: dict[int, list[dict]] = {}
_last_update: dict[int, float] = {}
_run_metadata: dict[int, dict] = {}
_lock = threading.Lock()
_terminal_run_ids: set[int] = set()
_TERMINAL_CACHE_CAP = 4096


def start_progress(run_id: int | None, service_id: str | None = None, task: str | None = None):
    if run_id is None:
        return
    with _lock:
        if run_id not in _progress:
            now = time.time()
            _progress[run_id] = []
            _last_update[run_id] = now
            _run_metadata[run_id] = {
                "service_id": service_id,
                "task": task,
                "started_at": now,
            }


_STALE_AFTER_SECONDS = 300  # 5 min — covers slow syncs, kills zombie entries


def list_active_runs() -> list[dict]:
    now = time.time()
    with _lock:
        candidates = []
        for run_id, meta in _run_metadata.items():
            events = _progress.get(run_id) or []
            if events and events[-1].get("type") in ("done", "error"):
                continue
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                continue
            candidates.append((run_id, meta))
    out = []
    for run_id, meta in candidates:
        if run_id in _terminal_run_ids:
            continue
        if _db_status_is_terminal(meta.get("service_id"), run_id):
            if len(_terminal_run_ids) < _TERMINAL_CACHE_CAP:
                _terminal_run_ids.add(run_id)
            continue
        entry = {"run_id": run_id}
        entry.update(meta)
        out.append(entry)
    return out


def active_task_for_service(service_id: str, tasks: tuple[str, ...]) -> str | None:
    now = time.time()
    with _lock:
        for run_id, meta in _run_metadata.items():
            if meta.get("service_id") != service_id or meta.get("task") not in tasks:
                continue
            events = _progress.get(run_id) or []
            if events and events[-1].get("type") in ("done", "error"):
                continue
            if now - _last_update.get(run_id, now) > _STALE_AFTER_SECONDS:
                continue
            return meta["task"]
    return None


def _db_status_is_terminal(service_id: str | None, run_id: int) -> bool:
    if not service_id:
        return False
    try:
        from backend.core import metadata as metadata_db

        status = metadata_db.get_cron_run_status(service_id, run_id)
        return status in ("success", "error")
    except Exception:
        return False


def reap_zombie_runs() -> int:
    now = time.time()
    evicted = 0
    with _lock:
        for run_id in list(_run_metadata.keys()):
            last_update = _last_update.get(run_id, now)
            if now - last_update > _STALE_AFTER_SECONDS:
                events = _progress.get(run_id) or []
                if not events or events[-1].get("type") not in ("done", "error"):
                    _progress.setdefault(run_id, []).append(
                        {"type": "error", "message": "scheduler reaped zombie cron (no progress in 5m)"}
                    )
                _run_metadata.pop(run_id, None)
                evicted += 1
        return evicted


def add_progress(run_id: int | None, event: dict):
    if run_id is None:
        return
    with _lock:
        if run_id in _progress:
            _progress[run_id].append(event)
            _last_update[run_id] = time.time()


def get_progress(run_id: int | None, start_idx: int = 0, service_id: str | None = None) -> list[dict] | None:
    if run_id is None:
        return None
    with _lock:
        if run_id not in _progress:
            return None
        if service_id and _run_metadata.get(run_id, {}).get("service_id") != service_id:
            return None
        return list(_progress[run_id][start_idx:])


def get_latest_progress_for_service(service_id: str) -> dict | None:
    with _lock:
        active_runs = []
        for run_id, meta in _run_metadata.items():
            if meta.get("service_id") == service_id and run_id in _progress:
                events = _progress[run_id]
                if events and events[-1].get("type") in ("done", "error"):
                    continue
                active_runs.append(run_id)
        if not active_runs:
            return None
        run_id = max(active_runs)
        if _progress[run_id]:
            latest = _progress[run_id][-1].copy()
            latest["task"] = _run_metadata[run_id].get("task")
            return latest
        return {"task": _run_metadata[run_id].get("task")}


def end_progress(run_id: int | None, final_event: dict | None = None):
    if run_id is None:
        return
    with _lock:
        if run_id in _progress:
            events = _progress[run_id]
            last_type = events[-1].get("type") if events else None
            if final_event:
                _progress[run_id].append(final_event)
            elif last_type not in ("done", "error"):
                _progress[run_id].append({"type": "done"})
            _last_update[run_id] = time.time()


def cleanup_progress_and_reap():
    cleanup_progress()
    return reap_zombie_runs()


def cleanup_progress():
    now = time.time()
    with _lock:
        expired = [k for k, v in _last_update.items() if now - v > 3600]  # 1 hour TTL
        for k in expired:
            _progress.pop(k, None)
            _last_update.pop(k, None)
            _run_metadata.pop(k, None)


_CacheRegistry.register("cron_progress._progress", _progress)
_CacheRegistry.register("cron_progress._last_update", _last_update)
_CacheRegistry.register("cron_progress._run_metadata", _run_metadata)
_CacheRegistry.register("cron_progress._terminal_run_ids", _terminal_run_ids)


class CronRunsPublisher(_InProcessPublisher):
    def __init__(self) -> None:
        super().__init__(replay_size=0)


publisher: Any
if SSE_BACKPLANE == "valkey":
    from backend.utils.telemetry import ValkeyPublisher

    publisher = ValkeyPublisher("cron-runs")
else:
    publisher = CronRunsPublisher()


def compute_sync_status_cached(service_id: str | None) -> dict | None:
    from backend import config as svcconfig
    from backend.core import duckdb as _db

    if not service_id:
        return None
    src = _db.get_source_for_service(service_id)
    if not src:
        return None
    from backend.high_scale.dashboard import header_metrics
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    high_scale_service = get_high_scale_service_registry().resolve(service_id)
    if high_scale_service is not None:
        metrics = header_metrics(high_scale_service)
        try:
            from backend.core.metadata.cron_log import latest_cron_per_task

            latest_crons = latest_cron_per_task(service_id)
            req_sync = None
            for t in ("log_discovery", "commit", "ledger_sweep"):
                if t in latest_crons and latest_crons[t].get("started_at"):
                    ts = latest_crons[t]["started_at"]
                    if req_sync is None or ts > req_sync:
                        req_sync = ts
            if req_sync and metrics.get("request"):
                metrics["request"]["last_sync_at"] = req_sync
            rum_sync = None
            for t in ("rum_discovery", "rum_sync", "ledger_rum_sweep"):
                if t in latest_crons and latest_crons[t].get("started_at"):
                    ts = latest_crons[t]["started_at"]
                    if rum_sync is None or ts > rum_sync:
                        rum_sync = ts
            if rum_sync and metrics.get("rum"):
                metrics["rum"]["last_sync_at"] = rum_sync
        except Exception:
            pass
        return {
            "configured": True,
            **metrics,
            "storage_mode": src.get("storage_mode", "cloud"),
        }
    lookup_key = service_id or src.get("service_id") or src.get("name", "")
    cached_status = svcconfig.get_status(lookup_key) or (svcconfig.get_status(src["name"]) if "name" in src else {})
    if not cached_status:
        return None  # fall through to dedicated endpoint
    request_status = cached_status.get("request")
    if isinstance(request_status, dict):
        cached_status["latest_log_at"] = request_status.get("latest_log_at")
    try:
        import re

        from backend.core import metadata as metadata_db

        summary = metadata_db.get_ingested_files_status_summary(lookup_key)
        if not summary.get("latest_file_name") and "name" in src:
            summary = metadata_db.get_ingested_files_status_summary(src["name"]) or summary
        latest_file_name = summary.get("latest_file_name")
        total_rows = summary.get("total_rows") or 0
        try:
            from backend.core.metadata.base import get_con as _get_meta_con

            lrow = (
                _get_meta_con(service_id)
                .execute(
                    "SELECT object_key FROM ingest_ledger WHERE service_id = ? AND status = 'committed' "
                    "ORDER BY object_key DESC LIMIT 1",
                    (service_id,),
                )
                .fetchone()
            )
            ledger_latest = lrow[0] if lrow else None
            if ledger_latest and (
                not latest_file_name or ledger_latest.split("/")[-1] > latest_file_name.split("/")[-1]
            ):
                latest_file_name = ledger_latest
        except Exception:
            pass
        if latest_file_name:
            fname = latest_file_name.split("/")[-1]
            m = re.search(r"(\d{4}-\d{2}-\d{2})[T-](\d{2}[:.-]\d{2}[:.-]\d{2})", fname)
            if m:
                latest_ingested_file_at = f"{m.group(1)} {m.group(2).replace('-', ':').replace('.', ':')}"
                cached_status["latest_ingested_file_at"] = latest_ingested_file_at
                cached_status["latest_available_file_at"] = latest_ingested_file_at
        if request_status is None and total_rows > 0 and cached_status.get("local_rows") is None:
            cached_status["local_rows"] = total_rows
    except Exception as e:
        logger.debug("[compute_sync_status_cached] failed to overlay real-time SQLite metadata: %s", e)
    cached_status["storage_mode"] = src.get("storage_mode", "cloud")
    cached_status["configured"] = True
    db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
    db_exists = os.path.exists(db_path)
    db_size = os.path.getsize(db_path) if db_exists else 0
    cache_size = _get_dir_size(_db._cache_dir(src))
    cached_status["duckdb_size_bytes"] = db_size + cache_size
    cached_status["duckdb_exists"] = db_exists
    active_run = get_latest_progress_for_service(service_id)
    if active_run:
        cached_status["active_run"] = active_run
        cached_status["busy"] = True
    cfg = svcconfig.load_config(service_id) or {}
    cached_status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")
    return cached_status


_self_mod = sys.modules[__name__]
for _alias in (
    "backend.cron_progress",
    "backend.cron_runs_publisher",
    "backend.sync_status_snapshot",
):
    sys.modules[_alias] = _self_mod
if "backend" in sys.modules:
    _backend_pkg = sys.modules["backend"]
    _backend_pkg.cron_progress = _self_mod  # type: ignore[attr-defined]
    _backend_pkg.cron_runs_publisher = _self_mod  # type: ignore[attr-defined]
    _backend_pkg.sync_status_snapshot = _self_mod  # type: ignore[attr-defined]
