"""Local + rollup compaction crons.

* ``_run_local_compact`` — frequent merge of small parquet files in the
  LOCAL CACHE only (does NOT touch FOS). Free in terms of cloud cost, so
  we run it on a 2 min interval.
* ``_run_rollup_compact_daily`` — consolidates per-hour rollup parquet
  into per-day files for closed days, slashing file-open overhead on
  7-day dashboard queries.
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    _display_name,
    _extract_log_text,
    _log_and_add_progress,
)

logger = logging.getLogger("backend.scheduler")


@cron_task("local_compact")
def _run_local_compact(service_id: str) -> None:
    """Frequent job: merge small parquet files in the LOCAL CACHE only.

    Does NOT touch FOS — only rewrites files inside cache/<bucket>/data/
    so DuckDB's view-glob picks up fewer files at query time. Free in
    terms of FOS cost (no 30-day-minimum penalty), so we can run it
    aggressively (every 10 min) without billing impact.

    Distinct from ``_run_optimize`` which writes through PyIceberg and
    DOES update FOS.
    """
    from backend.core import local_compaction as _lc
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "local_compact")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[96m[local-compact]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="local_compact")
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[96m[local-compact]\x1b[0m %s: Local compaction started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="local_compact",
        event={"type": "status", "message": "Scanning local cache partitions..."},
    )

    start_time = time.time()
    try:
        result = _lc.compact_local_partitions(src)
        duration = time.time() - start_time
        errors = result.get("errors") or []
        merged = result.get("files_merged", 0)
        removed = result.get("files_removed", 0)
        partitions = result.get("partitions_compacted", 0)
        summary = (
            f"Compacted {partitions} partition(s): merged {merged} small file(s) into "
            f"{partitions} (removed {removed} originals)"
        )
        if errors:
            err_preview = "\n".join(errors[:3])
            if len(errors) > 3:
                err_preview += f"\n... ({len(errors) - 3} more)"
            status = "warning"
            summary += f" — {len(errors)} partition error(s)"
        else:
            err_preview = None
            status = "success"
        log_cron_run(
            src,
            "local_compact",
            duration,
            status,
            summary=summary,
            error_message=err_preview,
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="local_compact",
            event={"type": "status", "message": summary},
        )
        logger.info("⏹️  \x1b[96m[local-compact]\x1b[0m %s: %s in %.2fs", _display, summary, duration)
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "local_compact",
            duration,
            "error",
            error_message=str(e),
            summary="local compaction failed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="local_compact", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: local_compact failed: %s", service_id, e)
    finally:
        end_progress(run_id)


@cron_task("rollup_compact_daily")
def _run_rollup_compact_daily(service_id: str) -> None:
    """Daily job: consolidate closed-day per-hour rollup parquet into per-day files.

    Reduces file-open overhead on 7-day dashboard queries from ~1500 files
    to ~30. Reader automatically falls back to per-hour when per-day is
    missing, so this is purely additive.
    """
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.rollups import backfill_day_bundles, compact_closed_days_to_daily

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "rollup_compact_daily")
    except RuntimeError as e:
        logger.info("⏭️  [rollup-compact] %s: skipping — %s", service_id, str(e))
        return

    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  [rollup-compact] %s: Daily rollup compaction started.", _display)

    start_time = time.time()
    try:
        rebuilt = compact_closed_days_to_daily(service_id, src)
        # After per-field per-day files are fresh, bundle them across
        # fields so the dashboard reader opens 1 file per day instead
        # of ~40. backfill_day_bundles is idempotent (skips up-to-date
        # bundles via mtime) so running it on every compact tick is
        # cheap when no new per-field days landed. Best-effort —
        # bundle failure degrades to per-field reading, which still
        # works correctly.
        try:
            bundled = backfill_day_bundles(service_id, src)
        except Exception as e:
            logger.warning(
                "[rollup-compact] %s: day-bundle backfill failed (per-field still serves): %s",
                _display,
                e,
            )
            bundled = 0
        duration = time.time() - start_time
        # Pass run_id so log_cron_run UPDATEs the 'running' row that
        # start_cron_run inserted (instead of orphaning it and inserting
        # a fresh terminal row). The same fix applies to the error
        # branch below — without run_id pass-through both branches
        # leave the original 'running' row stuck forever.
        log_cron_run(
            src,
            "rollup_compact_daily",
            duration,
            "success",
            summary=f"Rebuilt {rebuilt} (field, day) file(s); bundled {bundled} day(s).",
            run_id=run_id,
        )
        logger.info(
            "⏹️  [rollup-compact] %s: Compacted %d (field, day) file(s), bundled %d day(s) in %.1fs.",
            _display,
            rebuilt,
            bundled,
            duration,
        )
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "rollup_compact_daily",
            duration,
            "error",
            error_message=str(e),
            run_id=run_id,
        )
        logger.exception("[rollup-compact] %s: Daily rollup compaction failed: %s", _display, e)
