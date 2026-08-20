"""RUM commit cron job — compact local DuckDB tables to Iceberg.

Registered in scheduler as cron_rum_commit.
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task

logger = logging.getLogger(__name__)


@cron_task("cron_rum_commit")
def _run_rum_commit(service_id: str, force: bool = False, run_id: int | None = None, **kwargs) -> None:
    """Compact RUM tables from DuckDB cache to Iceberg/FOS."""
    from backend import config as svcconfig
    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import (
        finalize_cron_run_if_running,
        get_source_for_service,
        log_cron_run,
        start_cron_run,
    )
    from backend.utils.telemetry_proxy import _BOTO3_CALLER_HINT

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return

    if src.get("access_level") == "read_only" and not force:
        return

    prov = cfg.get("provisioning", {})
    sync_cfg = prov.get("cron_sync", {})
    if not sync_cfg.get("enabled", True) and not force:
        return

    try:
        if run_id is None:
            run_id = start_cron_run(src, "rum_commit")
    except RuntimeError as e:
        logger.info("[rum_commit] %s: skipping — %s", service_id, str(e))
        return

    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="rum_commit")

    start_time = time.time()
    boto3_token = _BOTO3_CALLER_HINT.set("rum_commit")

    try:
        logger.info(f"RUM commit starting for {service_id}")

        total_committed_vitals = 0
        total_committed_errors = 0

        # Commit client_vitals
        vitals_res = db_iceberg.commit_buffer(src, table_name="client_vitals")
        if vitals_res.get("files_committed", 0) > 0:
            total_committed_vitals = vitals_res.get("rows_committed", 0)
            # Sync client_vitals view/metadata
            db_iceberg.sync_data(src, table_name="client_vitals")

        # Commit client_errors
        errors_res = db_iceberg.commit_buffer(src, table_name="client_errors")
        if errors_res.get("files_committed", 0) > 0:
            total_committed_errors = errors_res.get("rows_committed", 0)
            # Sync client_errors view/metadata
            db_iceberg.sync_data(src, table_name="client_errors")

        # Also launch local compaction for BOTH tables
        try:
            import threading as _t

            from backend.core import local_compaction as _lc

            _t.Thread(
                target=lambda: _lc.compact_local_partitions(src, table_name="client_vitals"),
                name=f"local-compact-rum-vitals:{service_id}",
                daemon=True,
            ).start()
            _t.Thread(
                target=lambda: _lc.compact_local_partitions(src, table_name="client_errors"),
                name=f"local-compact-rum-errors:{service_id}",
                daemon=True,
            ).start()
        except Exception as lc_err:
            logger.warning("[rum_commit] %s: post-sync local compaction failed to launch: %s", service_id, lc_err)

        duration = time.time() - start_time
        summary = (
            f"Committed {vitals_res.get('files_committed', 0)} vitals files ({total_committed_vitals} rows) "
            f"and {errors_res.get('files_committed', 0)} errors files ({total_committed_errors} rows)"
        )
        log_cron_run(
            src,
            "rum_commit",
            duration,
            "success",
            run_id=run_id,
            rows_ingested=total_committed_vitals + total_committed_errors,
            summary=summary,
        )
        logger.info("RUM commit complete")

    except Exception as e:
        logger.error(f"RUM commit failed: {e}", exc_info=True)
        duration = time.time() - start_time
        log_cron_run(
            src,
            "rum_commit",
            duration,
            "error",
            error_message=str(e),
            run_id=run_id,
        )
        raise
    finally:
        _BOTO3_CALLER_HINT.reset(boto3_token)
        end_progress(run_id)
        if run_id is not None:
            finalize_cron_run_if_running(src, "rum_commit", run_id)
