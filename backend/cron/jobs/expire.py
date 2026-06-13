"""Weekly Iceberg snapshot-expiry / cloud maintenance cron."""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task
from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_name,
)

logger = logging.getLogger("backend.scheduler")


@cron_task("expire_snapshots")
def _run_expire_snapshots(service_id: str) -> None:
    """Weekly job: perform cloud maintenance including data deletion, cache cleanup, and snapshot expiry."""
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run

    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "expire_snapshots")
    except RuntimeError as e:
        logger.info("⏭️  [expire] %s: skipping — %s", service_id, str(e))
        return

    svc_id = src.get("service_id", "unknown")
    svc_name = _display_name(src, svc_id)
    display_name = f"{svc_name} ({svc_id})" if svc_name != svc_id else svc_id
    logger.info("▶️  \x1b[90m[expire]\x1b[0m %s: Maintenance job started.", display_name)

    start_time = time.time()
    try:
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
            )
        else:
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
            logger.info("🗑️ \x1b[90m[expire]\x1b[0m %s: Maintenance completed. %s", display_name, result)
            log_cron_run(
                src,
                "expire_snapshots",
                duration,
                status,
                error_message=error_message,
                summary=summary,
                run_id=run_id,
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
        )

    logger.info("⏹️  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)
