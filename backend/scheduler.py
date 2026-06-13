"""Backward-compat shim for the legacy ``backend.scheduler`` module surface.

The implementation moved into the :mod:`backend.cron` package — see
:mod:`backend.cron.scheduler` for the APScheduler lifecycle,
:mod:`backend.cron.decorators` for the watchdog wrapper, and
``backend.cron.jobs.*`` for the individual cron bodies. This file exists
so every historical ``from backend.scheduler import ...`` import keeps
working without callers needing to know about the carve.

Usage (called from main.py lifespan):

    from backend.scheduler import get_scheduler
    scheduler = get_scheduler()
    scheduler.start()
    ...
    scheduler.shutdown()
"""

from __future__ import annotations

from backend.cron.decorators import _CRON_HARD_CAP_S, cron_task
from backend.cron.jobs.commit import _run_commit
from backend.cron.jobs.compaction import _run_local_compact, _run_rollup_compact_daily
from backend.cron.jobs.expire import _run_expire_snapshots
from backend.cron.jobs.metadata import (
    _run_bot_data_refresh,
    _run_metadata_cleanup,
    _run_metadata_sync,
    _run_ngwaf_bot_sync,
    _run_rdns_enrichment,
    _run_service_alerts_evaluation,
    _run_share_audit_purge,
)
from backend.cron.jobs.optimize import _run_optimize
from backend.cron.jobs.sync import (
    GAP_HEAL_THROTTLE_HOURS,
    _last_successful_gap_heal_trigger,
    _mark_gap_heal_triggered,
    _run_full_sweep,
    _run_gap_heal,
    _run_service_cron,
)
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    TYPE_ICONS,
    Scheduler,
    _check_buffer_backlog,
    _check_disk_space,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
    get_scheduler,
    logger,
)

__all__ = [
    "GAP_HEAL_THROTTLE_HOURS",
    "JOB_COLORS",
    "RESET_COLOR",
    "Scheduler",
    "TYPE_ICONS",
    "_CRON_HARD_CAP_S",
    "_check_buffer_backlog",
    "_check_disk_space",
    "_elapsed_since",
    "_extract_log_text",
    "_last_successful_gap_heal_trigger",
    "_log_and_add_progress",
    "_mark_gap_heal_triggered",
    "_run_bot_data_refresh",
    "_run_commit",
    "_run_expire_snapshots",
    "_run_full_sweep",
    "_run_gap_heal",
    "_run_local_compact",
    "_run_metadata_cleanup",
    "_run_metadata_sync",
    "_run_ngwaf_bot_sync",
    "_run_optimize",
    "_run_rdns_enrichment",
    "_run_rollup_compact_daily",
    "_run_service_alerts_evaluation",
    "_run_service_cron",
    "_run_share_audit_purge",
    "cron_task",
    "get_scheduler",
    "logger",
]
