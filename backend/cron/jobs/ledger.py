"""Celery/ledger-mode crash recovery and dead-letter maintenance cron job.

Acts as the automated crash-net for High-Scale (DEPLOYMENT_MODE=high_throughput)
distributed ingestion. Reclaims stuck worker claims in PostgreSQL ingest_ledger,
re-dispatches stranded tasks with queue-depth guards, records permanently failing
objects as dead_letter/quarantined, and diffs FOS to catch up on unrecorded keys.
"""

from __future__ import annotations

import logging
import time

from backend.cron.decorators import cron_task

logger = logging.getLogger("backend.scheduler")


@cron_task("ledger_sweep", job_name="ledger_sweep")
def _run_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net: reclaim stale ledger claims, re-dispatch stuck
    rows, and diff a lookback FOS LIST against the ledger. Registered by the
    scheduler only when high-throughput mode."""
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — ledger sweep refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.ingest import sweep_ledger_once

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_throughput_mode(src):
        return
    if src.get("access_level") == "read_only":
        return

    if run_id is None:
        try:
            run_id = start_cron_run(src, "ledger_sweep")
        except RuntimeError as e:
            logger.info("[ledger_sweep] %s: skipping — %s", service_id, str(e))
            return

    from backend.cron.jobs._common import finalize_cron_duration
    from backend.cron.jobs.metadata import _log_and_add_progress
    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="ledger_sweep")
    _log_and_add_progress(
        run_id,
        service_id,
        job_name="ledger_sweep",
        event={"type": "status", "message": f"Starting ledger sweep for {service_id}..."},
    )

    started = time.time()
    try:
        summary = sweep_ledger_once(service_id, run_id=run_id)
        run_status = "success"
        warnings = []
        if not summary.get("broker_ok", True):
            warnings.append("Celery broker/queue depth probe failed")
        if summary.get("dead_letter", 0) > 0:
            warnings.append(f"{summary['dead_letter']} dead-letter/quarantined row(s)")
        if warnings:
            run_status = "warning"

        summary_msg = (
            f"reclaimed={summary.get('reclaimed', 0)} redispatched={summary.get('redispatched', 0)} "
            f"discovered={summary.get('discovered', 0)}"
        )
        if warnings:
            summary_msg += f" ({'; '.join(warnings)})"

        log_cron_run(
            src,
            "ledger_sweep",
            time.time() - started,
            run_status,
            run_id=run_id,
            files_downloaded=summary.get("discovered", 0),
            summary=summary_msg,
        )
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="ledger_sweep",
            event={"type": "done", "message": summary_msg},
        )
    except Exception as e:
        log_cron_run(
            src,
            "ledger_sweep",
            time.time() - started,
            "error",
            run_id=run_id,
            error_message=str(e),
            summary="Ledger sweep failed",
        )
        _log_and_add_progress(
            run_id,
            service_id,
            job_name="ledger_sweep",
            event={"type": "error", "message": str(e)},
        )
        logger.exception("[ledger_sweep] %s: sweep failed", service_id)
    finally:
        end_progress(run_id)
        finalize_cron_duration(src, run_id, started)
