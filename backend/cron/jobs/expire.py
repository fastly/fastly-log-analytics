"""Weekly Iceberg snapshot-expiry / cloud maintenance cron."""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_label,
)

logger = logging.getLogger("backend.scheduler")


@cron_task("cron:expire_snapshots", job_name="expire_snapshots")
def _run_expire_snapshots(service_id: str, *, manual: bool = False) -> dict:
    """Maintenance job: perform cloud maintenance including data deletion, cache cleanup, and snapshot expiry."""
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import (
        finalize_cron_run_if_running,
        get_source_for_service,
        log_cron_run,
        start_cron_run,
    )
    from backend.cron.jobs._common import finalize_cron_duration
    from backend.cron.scheduler import _extract_log_text, _log_and_add_progress, dev_mode_no_crons
    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress
    from backend.utils.active_requests import should_defer_cron

    if dev_mode_no_crons():
        return {"status": "skipped", "service_id": service_id, "summary": "FLA_DEV_NO_CRONS=1"}
    if not manual and should_defer_cron("expire_snapshots", service_id):
        return {"status": "deferred", "service_id": service_id, "summary": "active requests"}

    src = get_source_for_service(service_id)
    if src is None:
        return {"status": "skipped", "service_id": service_id, "summary": "service not found"}
    if src.get("access_level") == "read_only":
        return {"status": "skipped", "service_id": service_id, "summary": "read-only services cannot run maintenance"}

    try:
        run_id = start_cron_run(src, "expire_snapshots")
    except RuntimeError as e:
        logger.info("⏭️  [expire] %s: skipping — %s", service_id, str(e))
        return {"status": "skipped", "service_id": service_id, "summary": str(e)}

    svc_id = src.get("service_id", "unknown")
    display_name = svc_id
    start_time = time.time()
    outcome: dict = {}
    try:
        display_name = _display_label(src, svc_id)
        cleanup_progress_and_reap()
        start_progress(run_id, service_id=service_id, task="expire_snapshots")
        logger.info("🏎️  \x1b[90m[expire]\x1b[0m %s: Maintenance job started.", display_name)
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="expire_snapshots",
            event={"type": "status", "message": "Starting snapshot expiry and retention cleanup..."},
        )
        result = db_iceberg.run_cloud_maintenance(src)
        duration = time.time() - start_time
        if "error" in result:
            logger.warning("%s %s: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, result["error"])
            log_cron_run(
                src,
                "expire_snapshots",
                duration,
                "error",
                error_message=str(result["error"]),
                summary="Maintenance failed at catalog load",
                run_id=run_id,
                log_output=_extract_log_text(run_id),
            )
            outcome = {
                **result,
                "status": "error",
                "service_id": service_id,
                "summary": "Maintenance failed at catalog load",
            }
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="expire_snapshots",
                event={"type": "error", "message": str(result["error"])},
            )
        else:
            result = {
                **result,
                "retention_deleted_rows": int(
                    result.get("retention_deleted_rows", 0)
                    or sum(
                        int(result.get(key, 0) or 0)
                        for key in ("data_rows_deleted", "rum_log_rows_deleted", "rum_beacon_rows_deleted")
                    )
                ),
                "snapshots_expired": int(
                    result.get("snapshots_expired", result.get("snapshots_expired_count", 0)) or 0
                ),
                "files_unlinked": int(result.get("files_unlinked", result.get("data_files_cleaned", 0)) or 0),
            }
            summary_parts = []
            sub_errors = []
            for k, v in result.items():
                if k.endswith("_error"):
                    sub_errors.append(f"{k}={v}")
                else:
                    summary_parts.append(f"{k}={v}")
            summary = ", ".join(summary_parts) if summary_parts else "no work to do"
            status = "warning" if sub_errors else "success"
            error_message = "; ".join(sub_errors) if sub_errors else None
            files_unlinked = result["files_unlinked"]
            logger.info("🗑️ \x1b[90m[expire]\x1b[0m %s: Maintenance completed. %s", display_name, result)
            log_cron_run(
                src,
                "expire_snapshots",
                duration,
                status,
                error_message=error_message,
                summary=summary,
                run_id=run_id,
                log_output=_extract_log_text(run_id),
                files_deleted_fos=files_unlinked,
            )
            outcome = {**result, "status": status, "service_id": service_id, "summary": summary}
            event_type = "done" if status == "success" else status
            _log_and_add_progress(
                run_id,
                service_id,
                job_name="expire_snapshots",
                event={"type": event_type, "message": summary},
            )
    except Exception as e:
        duration = time.time() - start_time
        logger.exception(
            "%s %s: Maintenance failed: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, e
        )
        log_cron_run(
            src,
            "expire_snapshots",
            duration,
            "error",
            error_message=str(e),
            summary="Maintenance raised an uncaught exception",
            run_id=run_id,
            log_output=_extract_log_text(run_id),
        )
        outcome = {
            "status": "error",
            "service_id": service_id,
            "error": str(e),
            "summary": "Maintenance raised an uncaught exception",
        }
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="expire_snapshots",
            event={"type": "error", "message": str(e)},
        )
    finally:
        try:
            finalize_cron_run_if_running(src, "expire_snapshots", run_id)
        finally:
            try:
                end_progress(run_id)
            finally:
                finalize_cron_duration(src, run_id, start_time)

    logger.info("🏁  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)
    return outcome
