"""Cron scheduler package — re-exports progress tracking, job lifecycle, and APScheduler runner."""

from __future__ import annotations

from backend.cron.scheduler import lifecycle, progress, runner
from backend.cron.scheduler.lifecycle import (  # noqa: F401
    _BACKLOG_AGE_MULTIPLIER,
    _BACKLOG_BYTES_WARN,
    _BACKLOG_FILE_COUNT_WARN,
    _CRON_HARD_CAP_S,
    _DISK_FREE_HARD_FLOOR_BYTES,
    _DISK_FREE_HARD_FLOOR_PCT,
    _HEAVY_REFRESH_INTERVAL_SEC,
    _JOIN_TIMEOUT_S,
    _ROOT,
    _WATCHDOG_MAX_WORKERS,
    DEFAULT_INTERVAL_S,
    JOB_COLORS,
    RESET_COLOR,
    TYPE_ICONS,
    CronHeartbeat,
    _check_buffer_backlog,
    _check_disk_space,
    _claim_heavy_refresh,
    _display_label,
    _elapsed_since,
    _extract_log_text,
    _get_watchdog_executor,
    _last_heavy_refresh,
    _last_heavy_refresh_lock,
    _log_and_add_progress,
    _resolve_rum_sync_interval,
    _service_has_alerts,
    _status,
    _status_lock,
    _watchdog_executor,
    _watchdog_lock,
    cron_run,
    cron_task,
    dev_local_crons_enabled,
    dev_mode_no_crons,
    get_system_job_status,
    global_job,
    iso_z_now,
    logger,
    partial_hour_merge_enabled,
    partial_hour_merge_interval_sec,
    record_job_run,
    record_skip_row,
)
from backend.cron.scheduler.progress import (  # noqa: F401
    _STALE_AFTER_SECONDS,
    _TERMINAL_CACHE_CAP,
    SSE_BACKPLANE,
    CronRunsPublisher,
    _CacheRegistry,
    _db_status_is_terminal,
    _get_dir_size,
    _InProcessPublisher,
    _last_update,
    _lock,
    _progress,
    _run_metadata,
    _terminal_run_ids,
    active_task_for_service,
    add_progress,
    cleanup_progress,
    cleanup_progress_and_reap,
    compute_sync_status_cached,
    end_progress,
    get_latest_progress_for_service,
    get_progress,
    list_active_runs,
    publisher,
    reap_zombie_runs,
    start_progress,
    time,
)
from backend.cron.scheduler.runner import (  # noqa: F401
    TASK_MAP,
    Scheduler,
    _scheduler,
    build_cron_schedule_payload,
    get_scheduler,
)
from backend.utils.module_shim import install_shim_module

_SYNC_MODULES = (
    progress,
    lifecycle,
    runner,
)

_DELEGATED_ATTRS = {
    "_scheduler": runner,
    "_watchdog_executor": lifecycle,
}

_ShimModule = install_shim_module(
    __name__,
    _SYNC_MODULES,
    only_existing_attrs=True,
    delegated_attrs=_DELEGATED_ATTRS,
)
