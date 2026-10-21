"""Celery/ledger-mode RUM ingest cron jobs.

RUM counterpart of the regular-log ledger jobs in ``backend.cron.jobs.sync``
(``_run_log_discovery_cron``'s celery branch, ``_run_ledger_sweep``): fans
per-file discovery + convert work out across the Celery worker fleet instead
of running RUM beacon ingest as one big per-service in-process job.

The pre-existing v2 (non-celery) RUM path — ``backend.core.rum_ingest``,
``backend.cron.jobs.rum_commit``, and the ``rum_sync_{id}``/``rum_commit_{id}``
APScheduler jobs registered in ``backend.cron.scheduler`` — is untouched and
keeps running unchanged for non-celery deployments. The scheduler only
registers THIS module's jobs in high-scale mode, so the two
pipelines never run concurrently against the same service (which would
double-ingest: both write into the same DuckLake ``client_vitals``/
``client_errors`` tables via independent dedup registries that don't know
about each other).

Raw-file deletion needs no RUM-specific counterpart here: the existing,
unmodified ``finalize_committed_raw`` (called from ``backend.cron.jobs.commit``'s
celery branch on every ``commit_{id}`` tick) already gates deletion on
``ingest_ledger.status='committed'`` regardless of which prefix produced the
commit — it deletes by exact object_key, which is destination-only and can't
be misrouted the way a content-parsing dispatch could. Likewise DuckLake
small-file compaction (``merge_lake_files`` / ``ducklake_merge_adjacent_files``)
is catalog-wide and already covers the RUM tables once they exist, so no
RUM-specific commit/compaction job is needed either.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from backend.cron.decorators import cron_task
from backend.cron.lifecycle import cron_run

logger = logging.getLogger("backend.scheduler")


@cron_task("cron_rum_discovery", job_name="rum_discovery")
def _run_rum_discovery_cron(service_id: str, run_id: int | None = None) -> None:
    """Discover new ``raw/rum/`` beacon files and dispatch batched
    ``convert_batch_rum_files`` calls covering them. Runs inline so the
    cron_runs row carries the real per-tick outcome instead of a fake instant
    success.

    Also drives Faro bundle integrity/upstream-drift reconciliation — the
    same side effect the old ``rum_sync_{id}`` job provided via
    ``_reconcile_faro_bundle`` — by importing and calling that existing
    helper directly (read-only reuse; ``rum_sync.py`` itself is untouched).
    Without this, celery-mode RUM-enabled services would silently stop
    self-healing a wiped/corrupt Faro bundle or picking up an upstream
    re-release, since that helper otherwise only fires from the
    now-superseded ``rum_sync_{id}`` job.
    """
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — RUM discovery refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.cron.jobs.rum_sync import _reconcile_faro_bundle

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    rum_cfg = cfg.get("rum")
    rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
    rum_enabled = svcconfig.is_rum_enabled(cfg)
    if not rum_enabled:
        return

    src = get_source_for_service(service_id)
    if src is None:
        return
    if src.get("access_level") == "read_only":
        return
    if not svcconfig.is_high_scale_mode(src):
        return

    with cron_run(
        service_id,
        "rum_discovery",
        source=src,
        run_id=run_id,
        error_summary="RUM discovery failed",
        finalize_running_backstop=True,
        include_log_output=False,
        skip_log_prefix="rum_discovery",
    ) as run:
        if not run.ok:
            return
        run_id = run.run_id

        faro_ok = True
        faro_err = None
        try:
            faro_ok = _reconcile_faro_bundle(service_id, run_id)
        except Exception as fe:
            faro_ok = False
            faro_err = str(fe)
            logger.warning(
                "[rum_discovery] %s: Faro bundle reconcile failed (non-fatal): %s", service_id, fe, exc_info=True
            )

        if not svcconfig.CELERY_BROKER_URL:
            run.fail(
                "Celery RUM ingest misconfigured: no broker URL",
                error_message="DEPLOYMENT_MODE=high_scale requires CELERY_BROKER_URL",
            )
            return

        from backend.core.ingest import discover_rum_prefix
        from backend.provision.log_paths import rum_minute_list_prefix

        now = datetime.now(UTC)
        discovered = 0
        for i in range(5):
            prefix = rum_minute_list_prefix(now - timedelta(minutes=i))
            discovered += discover_rum_prefix(service_id, prefix_subpath=prefix, run_id=run_id)

        if not faro_ok:
            status = "warning"
            warn_msg = f"Faro bundle reconcile issue ({faro_err or 'drift/restore warning'})"
            summary = (
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers ({warn_msg})"
                if discovered
                else f"No new RUM files ({warn_msg})"
            )
            error_message = faro_err or "Faro bundle reconcile failed"
        else:
            status = "success"
            summary = (
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers"
                if discovered
                else "No new RUM files"
            )
            error_message = None

        run.record(status, summary=summary, error_message=error_message, files_downloaded=discovered)


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net for the RUM ledger pipeline — RUM counterpart
    of ``backend.cron.jobs.sync._run_ledger_sweep``. Registered by the
    scheduler only when high-scale mode and RUM is enabled for this
    service."""
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — RUM ledger sweep refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service
    from backend.core.ingest import sweep_rum_ledger_once

    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_scale_mode(src):
        return
    if src.get("access_level") == "read_only":
        return
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    rum_cfg = cfg.get("rum")
    rum_cfg = rum_cfg if isinstance(rum_cfg, dict) else {}
    rum_enabled = svcconfig.is_rum_enabled(cfg)
    if not rum_enabled:
        return

    with cron_run(
        service_id,
        "ledger_rum_sweep",
        source=src,
        run_id=run_id,
        error_summary="RUM ledger sweep failed",
        finalize_duration=True,
        finalize_running_backstop=True,
        include_log_output=False,
    ) as run:
        if not run.ok:
            return
        run.log_progress({"type": "status", "message": f"Starting RUM ledger sweep for {service_id}..."})

        summary = sweep_rum_ledger_once(service_id, run_id=run.run_id)
        run_status = "success"
        warnings = []
        if not summary.get("broker_ok", True):
            warnings.append("Celery broker/queue depth probe failed")
        if summary.get("dead_letter", 0) > 0:
            warnings.append(f"{summary['dead_letter']} dead-letter/quarantined RUM row(s)")
        if warnings:
            run_status = "warning"

        summary_msg = (
            f"reclaimed={summary.get('reclaimed', 0)} redispatched={summary.get('redispatched', 0)} "
            f"discovered={summary.get('discovered', 0)}"
        )
        if warnings:
            summary_msg += f" ({'; '.join(warnings)})"

        run.record(
            run_status,
            summary=summary_msg,
            error_message="; ".join(warnings) if warnings else None,
            files_downloaded=summary.get("discovered", 0),
        )
        run.log_progress({"type": "done", "message": summary_msg})
