"""Celery/ledger-mode RUM ingest cron jobs.

RUM counterpart of the regular-log ledger jobs in ``backend.cron.jobs.sync``
(``_run_log_discovery_cron``'s celery branch, ``_run_ledger_sweep``): fans
per-file discovery + convert work out across the Celery worker fleet instead
of running RUM beacon ingest as one big per-service in-process job.

The pre-existing v2 (non-celery) RUM path — ``backend.core.rum_ingest``,
``backend.cron.jobs.rum_commit``, and the ``rum_sync_{id}``/``rum_commit_{id}``
APScheduler jobs registered in ``backend.cron.scheduler`` — is untouched and
keeps running unchanged for non-celery deployments. The scheduler only
registers THIS module's jobs when ``INGEST_MODE == "celery"``, so the two
pipelines never run concurrently against the same service (which would
double-ingest: both write into the same DuckLake ``client_vitals``/
``client_errors`` tables via independent dedup registries that don't know
about each other).

Raw-file deletion needs no RUM-specific counterpart here: the existing,
unmodified ``finalize_committed_raw`` (called from ``backend.cron.jobs.commit``'s
celery branch on every ``log_ingest_{id}`` tick) already gates deletion on
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
    """Discover new ``rum/raw/`` beacon files and dispatch one ``convert_rum``
    per file. Runs inline (this job already executes on a worker via
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

    try:
        if run_id is None:
            run_id = start_cron_run(src, "rum_discovery")
    except RuntimeError as e:
        logger.info("[rum_discovery] %s: skipping — %s", service_id, str(e))
        return

    try:
        _reconcile_faro_bundle(service_id, run_id)
    except Exception:
        logger.warning("[rum_discovery] %s: Faro bundle reconcile failed (non-fatal)", service_id, exc_info=True)

    if not svcconfig.CELERY_BROKER_URL:
        log_cron_run(
            src,
            "rum_discovery",
            0.0,
            "error",
            run_id=run_id,
            error_message="INGEST_MODE=celery requires CELERY_BROKER_URL",
            summary="Celery RUM ingest misconfigured: no broker URL",
        )
        return

    from backend.core.ingest import discover_rum_prefix
    from backend.provision.log_paths import rum_minute_list_prefix

    started = time.time()
    now = datetime.now(UTC)
    try:
        discovered = 0
        for i in range(5):
            prefix = rum_minute_list_prefix(src.get("prefix", ""), now - timedelta(minutes=i))
            discovered += discover_rum_prefix(service_id, prefix_subpath=prefix)
        log_cron_run(
            src,
            "rum_discovery",
            time.time() - started,
            "success",
            run_id=run_id,
            files_downloaded=discovered,
            summary=(
                f"Discovered {discovered} new RUM file(s); dispatched to ingest workers"
                if discovered
                else "No new RUM files"
            ),
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


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str) -> None:
    """Celery-mode crash net for the RUM ledger pipeline — RUM counterpart
    of ``backend.cron.jobs.sync._run_ledger_sweep``. Registered by the
    scheduler only when INGEST_MODE=celery and RUM is enabled for this
    service."""
    from backend import config as svcconfig
    from backend.core.duckdb import get_source_for_service, log_cron_run, start_cron_run
    from backend.core.ingest import sweep_rum_ledger_once

    if svcconfig.INGEST_MODE != "celery":
        return
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        return
    src = get_source_for_service(service_id)
    if src is None:
        return

    try:
        run_id = start_cron_run(src, "ledger_rum_sweep")
    except RuntimeError as e:
        logger.info("[ledger_rum_sweep] %s: skipping — %s", service_id, str(e))
        return

    started = time.time()
    try:
        summary = sweep_rum_ledger_once(service_id)
        log_cron_run(
            src,
            "ledger_rum_sweep",
            time.time() - started,
            "success",
            run_id=run_id,
            files_downloaded=summary.get("discovered", 0),
            summary=(
                f"reclaimed={summary['reclaimed']} redispatched={summary['redispatched']} "
                f"discovered={summary['discovered']}"
            ),
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
