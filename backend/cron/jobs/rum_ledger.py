"""Celery/ledger-mode RUM ingest cron jobs.

RUM counterpart of the regular-log ledger jobs in ``backend.cron.jobs.sync``
(``_run_log_discovery_cron``'s celery branch, ``_run_ledger_sweep``): fans
per-file discovery + convert work out across the Celery worker fleet instead
of running RUM beacon ingest as one big per-service in-process job.

The pre-existing v2 (non-celery) RUM path — ``backend.core.rum_ingest``,
``backend.cron.jobs.rum_commit``, and the ``rum_sync_{id}``/``rum_commit_{id}``
APScheduler jobs registered in ``backend.cron.scheduler`` — is untouched and
keeps running unchanged for non-celery deployments. The scheduler only
registers THIS module's jobs in high-throughput mode, so the two
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
import time
from datetime import UTC, datetime, timedelta

from backend.cron.decorators import cron_task

logger = logging.getLogger("backend.scheduler")


@cron_task("cron_rum_discovery", job_name="rum_discovery")
def _run_rum_discovery_cron(service_id: str, run_id: int | None = None) -> None:
    """Discover new ``raw/rum/`` beacon files and dispatch batched
    ``convert_batch_rum_files`` calls covering them. Runs inline (this job
    already executes on a worker via
    RedBeat in external mode) so the cron_runs row carries the real
    per-tick outcome instead of a fake instant success.

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
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.cron.jobs.rum_sync import _reconcile_faro_bundle

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    rum_cfg = cfg.get("rum") or {}
    if not (cfg.get("rum_enabled") or rum_cfg.get("enabled")):
        return

    src = get_source_for_service(service_id)
    if src is None:
        return
    if src.get("access_level") == "read_only":
        return
    if not svcconfig.is_high_throughput_mode(src):
        return

    try:
        if run_id is None:
            run_id = start_cron_run(src, "rum_discovery")
    except RuntimeError as e:
        logger.info("[rum_discovery] %s: skipping — %s", service_id, str(e))
        return

    from backend.core.duckdb import finalize_cron_run_if_running
    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="rum_discovery")

    faro_ok = True
    faro_err = None
    try:
        faro_ok = _reconcile_faro_bundle(service_id, run_id)
    except Exception as fe:
        faro_ok = False
        faro_err = str(fe)
        logger.warning("[rum_discovery] %s: Faro bundle reconcile failed (non-fatal): %s", service_id, fe, exc_info=True)

    if not svcconfig.CELERY_BROKER_URL:
        log_cron_run(
            src,
            "rum_discovery",
            0.0,
            "error",
            run_id=run_id,
            error_message="DEPLOYMENT_MODE=high_throughput requires CELERY_BROKER_URL",
            summary="Celery RUM ingest misconfigured: no broker URL",
        )
        end_progress(run_id)
        if run_id is not None:
            finalize_cron_run_if_running(src, "rum_discovery", run_id)
        return

    from backend.core.ingest import discover_rum_prefix
    from backend.provision.log_paths import rum_minute_list_prefix

    started = time.time()
    now = datetime.now(UTC)
    try:
        discovered = 0
        for i in range(5):
            prefix = rum_minute_list_prefix(now - timedelta(minutes=i))
            discovered += discover_rum_prefix(service_id, prefix_subpath=prefix)

        duration = time.time() - started
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

        log_cron_run(
            src,
            "rum_discovery",
            duration,
            status,
            run_id=run_id,
            files_downloaded=discovered,
            summary=summary,
            error_message=error_message,
        )
    except Exception as e:
        log_cron_run(
            src,
            "rum_discovery",
            time.time() - started,
            "error",
            run_id=run_id,
            error_message=str(e),
            summary="RUM discovery failed",
        )
        logger.exception("[ledger] %s: RUM discovery failed: %s", service_id, e)
    finally:
        end_progress(run_id)
        if run_id is not None:
            finalize_cron_run_if_running(src, "rum_discovery", run_id)


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str) -> None:
    """Celery-mode crash net for the RUM ledger pipeline — RUM counterpart
    of ``backend.cron.jobs.sync._run_ledger_sweep``. Registered by the
    scheduler only when high-throughput mode and RUM is enabled for this
    service."""
    from backend.cron.scheduler import dev_mode_no_crons

    if dev_mode_no_crons():
        logger.warning("[scheduler] %s: FLA_DEV_NO_CRONS=1 — RUM ledger sweep refused.", service_id)
        return

    from backend import config as svcconfig
    from backend.core.duckdb import finalize_cron_run_if_running, get_source_for_service, log_cron_run, start_cron_run
    from backend.core.ingest import sweep_rum_ledger_once
    from backend.cron_progress import cleanup_progress_and_reap, end_progress, start_progress

    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_throughput_mode(src):
        return
    if src.get("access_level") == "read_only":
        return
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return

    rum_cfg = cfg.get("rum") or {}
    if not (cfg.get("rum_enabled") or rum_cfg.get("enabled")):
        return

    run_id = None
    try:
        run_id = start_cron_run(src, "ledger_rum_sweep")
    except RuntimeError as e:
        logger.info("[ledger_rum_sweep] %s: skipping — %s", service_id, str(e))
        return

    cleanup_progress_and_reap()
    start_progress(run_id, service_id=service_id, task="ledger_rum_sweep")

    started = time.time()
    try:
        summary = sweep_rum_ledger_once(service_id)
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

        log_cron_run(
            src,
            "ledger_rum_sweep",
            time.time() - started,
            run_status,
            run_id=run_id,
            files_downloaded=summary.get("discovered", 0),
            summary=summary_msg,
            error_message="; ".join(warnings) if warnings else None,
        )
    except Exception as e:
        log_cron_run(
            src,
            "ledger_rum_sweep",
            time.time() - started,
            "error",
            run_id=run_id,
            error_message=str(e),
            summary="RUM ledger sweep failed",
        )
        logger.exception("[ledger_rum_sweep] %s: sweep failed", service_id)
    finally:
        end_progress(run_id)
        if run_id is not None:
            finalize_cron_run_if_running(src, "ledger_rum_sweep", run_id)
