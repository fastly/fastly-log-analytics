"""Metadata cleanup cron job."""

from __future__ import annotations

import logging

from backend.cron.scheduler import (
    JOB_COLORS,
    RESET_COLOR,
    _display_label,
    _log_and_add_progress,
    cron_run,
    cron_task,
)

logger = logging.getLogger("backend.scheduler")


@cron_task("metadata_cleanup", job_name="metadata_cleanup")
def _run_metadata_cleanup(service_id: str) -> None:
    from backend import config as svcconfig
    from backend.core.fos_client import get_source_for_service
    from backend.core.metadata import cleanup_metadata
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.utils.telemetry import should_defer_cron

    label = f"{JOB_COLORS.get('metadata_cleanup', '')}[metadata_cleanup]{RESET_COLOR}"
    if dev_mode_no_crons():
        logger.info("⏸️  %s %s: dev_mode_no_crons active, skipping metadata cleanup.", label, service_id)
        return
    src = get_source_for_service(service_id)
    if src is None or should_defer_cron("metadata_cleanup", service_id):
        return
    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}
    _display = _display_label(src, service_id)
    with cron_run(
        service_id,
        "metadata_cleanup",
        source=src,
        error_summary=lambda exc: f"cleanup failed: {exc}",
        finalize_duration=True,
        include_log_output=False,
        skip_log_prefix="metadata_cleanup",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id
        logger.info("🏎️  %s %s: Starting metadata cleanup.", label, _display)
        result = cleanup_metadata(
            service_id,
            retention,
            on_event=lambda ev: _log_and_add_progress(run_id, service_id, job_name="metadata_cleanup", event=ev),
        )
        total_deleted = sum(result["deleted"].values())
        summary_parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
        if total_deleted:
            vac_str = "yes" if result["vacuumed"] else "skipped (no deletions)"
            summary = f"Trimmed {total_deleted:,} rows ({', '.join(summary_parts)}). VACUUM={vac_str}."
            logger.info(
                "🧹 %s %s: deleted %d rows (%s) vacuumed=%s in %.2fs",
                label,
                _display,
                total_deleted,
                ", ".join(summary_parts),
                result["vacuumed"],
                result["duration_s"],
            )
        else:
            summary = "No rows older than retention windows."
            logger.info("🏁  %s %s: no rows to trim (took %.2fs)", label, _display, result["duration_s"])
        run.success(summary, rows_ingested=total_deleted)
        _log_and_add_progress(
            run_id, service_id, job_name="metadata_cleanup", event={"type": "done", "message": summary}
        )


__all__ = [
    "_run_metadata_cleanup",
]
