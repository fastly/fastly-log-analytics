"""Iceberg optimize cron — daily small-file compaction.

Distinct from :mod:`backend.cron.jobs.compaction`'s local-only compactor: this
job writes through PyIceberg and DOES update FOS. Pinned to 03:00 UTC by
:meth:`backend.cron.scheduler.Scheduler._sync_jobs`.
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


@cron_task("optimize_iceberg")
def _run_optimize(service_id: str) -> None:
    """Daily job: compact small Iceberg data files into target-sized ones."""
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "optimize")
    except RuntimeError as e:
        logger.info("⏭️  \x1b[92m[optimize]\x1b[0m %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="optimize")
    _svc_name = _display_name(src, service_id)
    _display = f"{_svc_name} ({service_id})" if _svc_name != service_id else service_id
    logger.info("▶️  \x1b[92m[optimize]\x1b[0m %s: Optimize job started.", _display)
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="optimize",
        event={"type": "status", "message": "Scanning Iceberg table for small files to compact..."},
    )

    start_time = time.time()
    try:
        # Pin the cron's threshold to the conservative original (>10 files
        # per partition) so the daily FOS-touching pass stays cheap. The
        # auto-derive heuristic stays available for the admin endpoint
        # (`/admin/optimize-now`) when you want to force aggressive cleanup.
        result = db_iceberg.optimize_table(src, min_files_per_partition=10)
        duration = time.time() - start_time
        if "error" in result:
            log_cron_run(
                src,
                "optimize",
                duration,
                "error",
                error_message=result["error"],
                summary="Iceberg optimize failed",
                run_id=run_id,
                log_output=_extract_log_text(run_id),
            )
            _log_and_add_progress(
                run_id, service_id, job_name="optimize", event={"type": "error", "message": result["error"]}
            )
            _log_and_add_progress(
                run_id, service_id, job_name="optimize", event={"type": "warning", "message": result["error"]}
            )
        else:
            summary = f"Rewrote {result.get('files_rewritten', 0)} files into {result.get('files_added', 0)} files"
            partition_errors = result.get("partition_errors") or []
            if partition_errors:
                eligible = result.get("eligible_partitions", 0)
                summary += f" — {len(partition_errors)}/{eligible} partitions failed"
                # First 3 errors give enough signal for triage without exploding log size.
                err_preview = "\n".join(partition_errors[:3])
                if len(partition_errors) > 3:
                    err_preview += f"\n... ({len(partition_errors) - 3} more)"
                status = "error" if result.get("files_added", 0) == 0 else "warning"
            else:
                err_preview = None
                status = "success"
            log_cron_run(
                src,
                "optimize",
                duration,
                status,
                run_id=run_id,
                parquet_files_optimized=result.get("files_rewritten", 0),
                parquet_files_created=result.get("files_added", 0),
                summary=summary,
                error_message=err_preview,
                log_output=_extract_log_text(run_id),
            )
            event_type = "done" if status == "success" else status
            _log_and_add_progress(
                run_id, service_id, job_name="optimize", event={"type": event_type, "message": summary}
            )
            logger.info(
                "[scheduler] %s: optimize complete — %s",
                service_id,
                summary,
            )
    except Exception as e:
        duration = time.time() - start_time
        log_cron_run(
            src,
            "optimize",
            duration,
            "error",
            error_message=str(e),
            summary="Iceberg optimize failed",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        _log_and_add_progress(run_id, service_id, job_name="optimize", event={"type": "error", "message": str(e)})
        logger.exception("[scheduler] %s: optimize failed: %s", service_id, e)
    finally:
        end_progress(run_id)

    from backend.cron.jobs._common import finalize_cron_duration

    finalize_cron_duration(src, run_id, start_time)

    logger.info("⏹️  \x1b[92m[optimize]\x1b[0m %s: Optimize job finished.", _display)
