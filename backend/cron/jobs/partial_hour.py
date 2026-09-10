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
    from backend.core.rollups.partial_hour import gc_stale_partial_hours, merge_partial_hour
    from backend.repositories._base import _LIVE_TOPN_SKIP_FIELDS
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

    logger.info("🕐  [partial-hour] %s: partial-hour merge started.", _display)
    # Only fields execute_top_n_rollups's own live top-up would bother
    # aggregating (M1, final whole-branch review) — the full FIELDS list
    # includes high-cardinality identifier/raw-measurement columns
    # (_LIVE_TOPN_SKIP_FIELDS) with no top-N panel, so aggregating them
    # every 30s bought nothing any reader consumes.
    merge_fields = [f for f in FIELDS if f not in _LIVE_TOPN_SKIP_FIELDS]

    start_time = time.time()
    try:
        stats = merge_partial_hour(service_id, src, merge_fields)
        removed = gc_stale_partial_hours(src)
        duration = time.time() - start_time
        # NOTE (C2, final whole-branch review): this tick deliberately does
        # NOT call mark_rollup_coverage_ready. A successful tick — even a
        # real one — only proves the partial-hour writer path is alive, not
        # that this pod's local closed-hour rollup tree reflects a genuine
        # backfill pass; the 6 durable-mode trust gates need the latter.
        # That flag is now only raised by main.py's startup catch-up and by
        # _run_rollup_hour_heal's success path (both call
        # backfill_missing_hour_bundles, the real coverage-establishing
        # function).
        summary = f"Merged {stats['new_files']} new file(s) into hour={stats['hour']}; GC'd {removed} stale hour(s)"
        # Always finalize through log_cron_run first — it releases the
        # job_runs lease the same way a real tick does, which the NEXT
        # tick's start_cron_run depends on to avoid spuriously seeing this
        # one as still "running".
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
