"""Celery/ledger-mode crash recovery and dead-letter maintenance cron job.

Acts as the automated crash-net for High-Scale (DEPLOYMENT_MODE=high_scale)
distributed ingestion. Reclaims stuck worker claims in PostgreSQL ingest_ledger,
re-dispatches stranded tasks with queue-depth guards, records permanently failing
objects as dead_letter/quarantined, and diffs FOS to catch up on unrecorded keys.
"""

from __future__ import annotations

import logging

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run

logger = logging.getLogger("backend.scheduler")


@cron_task("ledger_sweep", job_name="ledger_sweep")
def _run_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net: reclaim stale ledger claims, re-dispatch stuck
    rows, and diff a lookback FOS LIST against the ledger. Registered by the
    scheduler only when high-scale mode."""
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — ledger sweep refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.ingest import sweep_ledger_once

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_scale_mode(src):
        return
    if src.get("access_level") == "read_only":
        return

    with cron_run(
        service_id,
        "ledger_sweep",
        source=src,
        run_id=run_id,
        error_summary="Ledger sweep failed",
        finalize_duration=True,
        include_log_output=False,
    ) as run:
        if not run.ok:
            return
        run.log_progress({"type": "status", "message": f"Starting ledger sweep for {service_id}..."})

        summary = sweep_ledger_once(service_id, run_id=run.run_id)
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

        run.record(run_status, summary=summary_msg, files_downloaded=summary.get("discovered", 0))
        run.log_progress({"type": "done", "message": summary_msg})
