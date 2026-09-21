"""Scheduler entry for the periodic DuckDB instance recycle.

The recycle *mechanism* lives in :mod:`backend.core.duckdb_recycle` (pure core:
drain the pools, drop the per-file DuckDB instance, free the leaked
``enable_object_cache`` parquet metadata). This module is the thin cron-layer
wrapper the scheduler registers — it keeps the ``@global_job`` decorator (and
therefore the ``backend.cron`` dependency) out of the core layer so the
"Core does not depend on routers" import contract holds (core → cron → routers
would otherwise be a transitive violation). See backend/core/duckdb_recycle.py
and [[backend-oom-restart-loop]].

This tick also runs the process-level OOM stopgap (memory_guard): the cache
recycle alone does NOT bound RSS (the 12GB isn't the object cache), so when
RSS crosses the restart threshold we exit cleanly and let docker restart us
before the kernel OOM-SIGKILLs the process. The guard rides this job's
cadence; it's a no-op unless BACKEND_GRACEFUL_RESTART_RSS_MB is set.
"""

from __future__ import annotations

import logging

from backend.core import duckdb as _db
from backend.core.duckdb_recycle import _recycle_rss_threshold_bytes, recycle_once
from backend.core.memory_guard import maybe_graceful_restart
from backend.cron.decorators import global_job

logger = logging.getLogger(__name__)


def _maybe_adjust_recycle_schedule(*, expedite: bool) -> None:
    """Dynamically adjust recycle interval under memory pressure (e.g. tighten to 2m, or relax back to baseline)."""
    try:
        from backend.core.duckdb_recycle import recycle_interval_min
        from backend.cron.scheduler import get_scheduler

        sched = get_scheduler()
        job = sched.get_job("duckdb_recycle")
        if not job:
            return

        baseline_mins = max(1.0, recycle_interval_min() or 60.0)
        target_mins = min(2.0, baseline_mins) if expedite else baseline_mins

        current_trigger = getattr(job, "trigger", None)
        current_interval_s = getattr(getattr(current_trigger, "interval", None), "total_seconds", lambda: 0)()

        if int(current_interval_s) != int(target_mins * 60):
            job.reschedule("interval", minutes=target_mins)
            logger.info(
                "⚡ [duckdb_recycle] Dynamically adjusted recycle interval to %.1fm (expedite=%s)",
                target_mins,
                expedite,
            )
    except Exception as e:
        logger.debug("[duckdb_recycle] Could not adjust recycle schedule: %s", e)


@global_job("duckdb_recycle", color="35", tag="recycle", label="DuckDB recycle")
def run_duckdb_recycle() -> str:
    """Scheduler entry: recycle on the interval, gated by the optional RSS
    threshold (skip when the process is still small — avoids needless cold
    caches). Wrapped by ``global_job`` for start/end logging + record_job_run.
    """
    # Process-level OOM stopgap FIRST and independent of the recycle threshold:
    # if RSS is already over the restart threshold, a clean self-restart is the
    # only thing that reliably reclaims it (recycle frees ~0 of the real leak).
    if maybe_graceful_restart():
        return "graceful restart triggered (RSS over restart threshold)"
    threshold = _recycle_rss_threshold_bytes()
    rss = _db.current_rss_bytes()
    if threshold > 0 and rss is not None:
        if rss < threshold:
            if rss >= int(threshold * 0.8):
                _maybe_adjust_recycle_schedule(expedite=True)
            elif rss < int(threshold * 0.5):
                _maybe_adjust_recycle_schedule(expedite=False)
            return f"skipped: RSS {rss / 1e6:.0f}MB < threshold {threshold / 1e6:.0f}MB"

    res = recycle_once(reason="interval")

    # Evaluate post-recycle memory to adapt schedule for next tick
    rss_after = _db.current_rss_bytes()
    if threshold > 0 and rss_after is not None:
        if rss_after >= int(threshold * 0.7) or "incomplete" in res:
            _maybe_adjust_recycle_schedule(expedite=True)
        elif rss_after < int(threshold * 0.5):
            _maybe_adjust_recycle_schedule(expedite=False)

    return res

