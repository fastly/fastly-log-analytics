"""Weekly Iceberg snapshot-expiry / cloud maintenance cron."""

from __future__ import annotations

import logging

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run
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
    from backend.core.duckdb import get_source_for_service
    from backend.cron.scheduler import dev_mode_no_crons
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

    svc_id = src.get("service_id", "unknown")
    display_name = _display_label(src, svc_id)

    with cron_run(
        service_id,
        "expire_snapshots",
        source=src,
        skip_log_prefix="expire",
        finalize_duration=True,
        finalize_running_backstop=True,
        error_summary="Maintenance raised an uncaught exception",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error": str(exc),
            "summary": "Maintenance raised an uncaught exception",
        },
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        if run.ok:
            logger.info("🏎️  \x1b[90m[expire]\x1b[0m %s: Maintenance job started.", display_name)
            run.log_progress({"type": "status", "message": "Starting snapshot expiry and retention cleanup..."})
            result = db_iceberg.run_cloud_maintenance(src)
            if "error" in result:
                logger.warning(
                    "%s %s: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, result["error"]
                )
                run.record(
                    "error",
                    error_message=str(result["error"]),
                    summary="Maintenance failed at catalog load",
                )
                run.result = {
                    **result,
                    "status": "error",
                    "service_id": service_id,
                    "summary": "Maintenance failed at catalog load",
                }
                run.log_progress({"type": "error", "message": str(result["error"])})
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
                run.record(
                    status,
                    error_message=error_message,
                    summary=summary,
                    files_deleted_fos=files_unlinked,
                )
                run.result = {**result, "status": status, "service_id": service_id, "summary": summary}
                event_type = "done" if status == "success" else status
                run.log_progress({"type": event_type, "message": summary})

    logger.info("🏁  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)
    return run.result
