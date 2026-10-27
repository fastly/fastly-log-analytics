"""Metadata cleanup, DuckLake snapshot expiry, and table compaction cron jobs."""

from __future__ import annotations

import logging
import sys as _sys

from backend.core.storage.backup import schedule_cloud_push
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
    from backend.core.duckdb import get_source_for_service
    from backend.core.metadata import cleanup_metadata
    from backend.cron.decorators import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

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
        try:
            from backend.cron.jobs import metric_snapshots as _ms

            _ms.purge_old(retention_days=30)
        except Exception as e:
            logger.debug("[metadata_cleanup] metric_snapshots purge failed: %s", e)
        run.success(summary, rows_ingested=total_deleted)
        _log_and_add_progress(
            run_id, service_id, job_name="metadata_cleanup", event={"type": "done", "message": summary}
        )


@cron_task("cron:expire_snapshots", job_name="expire_snapshots")
def _run_expire_snapshots(service_id: str, *, manual: bool = False) -> dict:
    from backend.core import storage as db_storage
    from backend.core.duckdb import get_source_for_service
    from backend.cron.scheduler import dev_mode_no_crons
    from backend.utils.active_requests import should_defer_cron

    if dev_mode_no_crons():
        return {"status": "skipped", "service_id": service_id, "summary": "FLA_DEV_NO_CRONS=1"}
    if not manual and should_defer_cron("expire_snapshots", service_id):
        return {"status": "deferred", "service_id": service_id, "summary": "active requests"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "skipped", "service_id": service_id, "summary": "service not found"}
    display_name = _display_label(src, str(src.get("service_id", "unknown")))
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
            result = db_storage.run_cloud_maintenance(src)
            if "error" in result:
                logger.warning(
                    "%s %s: %s", JOB_COLORS["expire"] + "[expire]" + RESET_COLOR, display_name, result["error"]
                )
                run.record("error", error_message=str(result["error"]), summary="Maintenance failed at catalog load")
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
                            int(result.get(k, 0) or 0)
                            for k in ("data_rows_deleted", "rum_log_rows_deleted", "rum_beacon_rows_deleted")
                        )
                    ),
                    "snapshots_expired": int(
                        result.get("snapshots_expired", result.get("snapshots_expired_count", 0)) or 0
                    ),
                    "files_unlinked": int(result.get("files_unlinked", result.get("data_files_cleaned", 0)) or 0),
                }
                summary_parts = [f"{k}={v}" for k, v in result.items() if not k.endswith("_error")]
                sub_errors = [f"{k}={v}" for k, v in result.items() if k.endswith("_error")]
                summary = ", ".join(summary_parts) if summary_parts else "no work to do"
                status = "warning" if sub_errors else "success"
                logger.info("🗑️ \x1b[90m[expire]\x1b[0m %s: Maintenance completed. %s", display_name, result)
                run.record(
                    status,
                    error_message="; ".join(sub_errors) if sub_errors else None,
                    summary=summary,
                    files_deleted_fos=result["files_unlinked"],
                )
                run.result = {**result, "status": status, "service_id": service_id, "summary": summary}
                run.log_progress({"type": "done" if status == "success" else status, "message": summary})
                schedule_cloud_push(src)
    logger.info("🏁  \x1b[90m[expire]\x1b[0m %s: Maintenance job finished.", display_name)
    return dict(run.result) if isinstance(run.result, dict) else {}


@cron_task("cron.optimize", job_name="optimize")
def _run_optimize(service_id: str, manual: bool = False) -> dict | None:
    from backend.core import storage as db_storage
    from backend.core.duckdb import get_source_for_service
    from backend.cron.decorators import dev_mode_no_crons
    from backend.cron.jobs.sync import publish_sync_status_safe
    from backend.utils.active_requests import should_defer_cron

    if not manual and dev_mode_no_crons():
        logger.warning("🚫 [scheduler] %s: FLA_DEV_NO_CRONS=1 — optimize refused.", service_id)
        return {"status": "skipped", "service_id": service_id, "summary": "Skipped due to FLA_DEV_NO_CRONS=1"}
    if not manual and should_defer_cron("optimize", service_id):
        logger.info("⏸️  [optimize] %s: deferring due to active user queries", service_id)
        return {"status": "deferred", "service_id": service_id, "summary": "Deferred due to active queries"}
    if (src := get_source_for_service(service_id)) is None:
        return {"status": "error", "service_id": service_id, "summary": "Source not found"}
    _display = _display_label(src, service_id)
    outcome: dict | None = None
    with cron_run(
        service_id,
        "optimize",
        source=src,
        error_summary="DuckLake optimize failed",
        error_result=lambda exc: {
            "status": "error",
            "service_id": service_id,
            "error": str(exc),
            "files_rewritten": 0,
            "files_added": 0,
            "summary": "DuckLake optimize failed",
        },
        finalize_duration=True,
        skip_log_prefix="optimize",
    ) as run:
        if run.skipped:
            return {"status": "skipped", "service_id": service_id, "summary": run.skip_reason}
        logger.info("🏎️  \x1b[92m[optimize]\x1b[0m %s: Optimize job started.", _display)
        run.log_progress({"type": "status", "message": "Scanning DuckLake table for small files to compact..."})
        result = db_storage.optimize_table(src, min_files_per_partition=10)
        if "error" in result:
            run.fail("DuckLake optimize failed", error_message=result["error"])
            run.log_progress({"type": "error", "message": result["error"]})
            run.log_progress({"type": "warning", "message": result["error"]})
            outcome = {
                "status": "error",
                "service_id": service_id,
                "error": result["error"],
                "files_rewritten": 0,
                "files_added": 0,
                "summary": "DuckLake optimize failed",
            }
        else:
            summary = f"Rewrote {result.get('files_rewritten', 0)} files into {result.get('files_added', 0)} files"
            partition_errors = result.get("partition_errors") or []
            if partition_errors:
                summary += f" — {len(partition_errors)}/{result.get('eligible_partitions', 0)} partitions failed"
                err_preview: str | None = "\n".join(partition_errors[:3]) + (
                    f"\n... ({len(partition_errors) - 3} more)" if len(partition_errors) > 3 else ""
                )
                status = "error" if result.get("files_added", 0) == 0 else "warning"
            else:
                err_preview, status = None, "success"
            run.record(
                status,
                summary=summary,
                error_message=err_preview,
                parquet_files_optimized=result.get("files_rewritten", 0),
                parquet_files_created=result.get("files_added", 0),
            )
            run.log_progress({"type": "done" if status == "success" else status, "message": summary})
            logger.info("[scheduler] %s: optimize complete — %s", service_id, summary)
            schedule_cloud_push(src)
            outcome = {
                "status": status,
                "service_id": service_id,
                "files_rewritten": result.get("files_rewritten", 0),
                "files_added": result.get("files_added", 0),
                "eligible_partitions": result.get("eligible_partitions", 0),
                "partition_errors": partition_errors,
                "summary": summary,
            }
    if run.result is not None:
        outcome = run.result
    publish_sync_status_safe(service_id, "scheduler")
    logger.info("🏁  \x1b[92m[optimize]\x1b[0m %s: Optimize job finished.", _display)
    return outcome


_self_mod = _sys.modules[__name__]
_sys.modules["backend.cron.jobs.optimize"] = _self_mod
_sys.modules["backend.cron.jobs.expire"] = _self_mod
if (_jobs_pkg := _sys.modules.get("backend.cron.jobs")) is not None:
    _jobs_pkg.optimize = _self_mod  # type: ignore[attr-defined]
    _jobs_pkg.expire = _self_mod  # type: ignore[attr-defined]

__all__ = [
    "_run_expire_snapshots",
    "_run_metadata_cleanup",
    "_run_optimize",
    "schedule_cloud_push",
]
