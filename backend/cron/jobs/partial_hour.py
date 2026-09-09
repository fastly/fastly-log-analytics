"""Partial-hour merge cron — the fast (30s) tick that folds newly-landed
buffer/active-hour-partition files into the pod-local partial-hour rollup.

Structurally mirrors ``backend/cron/jobs/compaction.py::_run_local_compact``
(same active-request defer gate, same start_cron_run/log_cron_run lease
contract) but on its own faster cadence — see
docs/superpowers/specs/2026-09-09-partial-hour-speed-layer-design.md Part 2
for why this isn't just piggybacked onto local_compact's 2 min tick.
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import _display_label, _extract_log_text, _log_and_add_progress

logger = logging.getLogger("backend.scheduler")


@cron_task("partial_hour_merge", job_name="partial_hour_merge")
def _run_partial_hour_merge(service_id: str) -> None:
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.rollup_readiness import mark_rollup_coverage_ready
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories.dashboard import FIELDS
    from backend.utils.active_requests import should_defer_cron

    if should_defer_cron("partial_hour_merge", service_id):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "partial_hour_merge")
    except RuntimeError as e:
        logger.info("⏭️  [partial-hour] %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="partial_hour_merge")
    _display = _display_label(src, service_id)

    start_time = time.time()
    try:
        stats = merge_partial_hour(service_id, src, FIELDS)
        removed = gc_stale_partial_hours(src)
        duration = time.time() - start_time
        # A successful tick (even a no-new-files no-op) proves this pod's
        # partial-hour writer path is alive, which is a reasonable proxy
        # for "the closed-hour self-heal path is alive too" — both are
        # pod-local APScheduler jobs registered together (Task 6). This is
        # the self-heal fallback the Task 1/2 startup catch-up relies on
        # when the one-time backfill times out or crashes.
        mark_rollup_coverage_ready(service_id)
        summary = f"Merged {stats['new_files']} new file(s) into hour={stats['hour']}; GC'd {removed} stale hour(s)"
        log_cron_run(
            src,
            "partial_hour_merge",
            duration,
            "success",
            summary=summary,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        if stats["new_files"]:
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="partial_hour_merge",
                event={"type": "status", "message": summary},
            )
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "partial_hour_merge",
            duration,
            "error",
            error_message=str(e),
            summary="partial-hour merge failed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(
            run_id, service_id, job_name="partial_hour_merge", event={"type": "error", "message": str(e)}
        )
        logger.exception("[scheduler] %s: partial_hour_merge failed: %s", service_id, e)
    finally:
        end_progress(run_id)
