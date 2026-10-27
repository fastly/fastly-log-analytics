"""Celery/ledger-mode RUM and request ingest discovery & sweep cron jobs."""

from __future__ import annotations

import logging
import sys as _sys
from datetime import UTC, datetime, timedelta

from backend import config as svcconfig
from backend.cron.jobs._common import _refuse_if_dev_no_crons
from backend.cron.scheduler import cron_run, cron_task

logger = logging.getLogger("backend.scheduler")


def _resolve_reconcile_faro_bundle():
    from backend.cron.jobs import rum_sync as _rs_mod

    orig = _rs_mod._orig_reconcile_faro_bundle
    for mod_name in (
        "backend.cron.jobs.rum_ledger",
        "backend.cron.jobs.rum_sync",
        "backend.cron.jobs.sync",
    ):
        mod = _sys.modules.get(mod_name)
        if mod is not None and "_reconcile_faro_bundle" in mod.__dict__:
            candidate = mod.__dict__["_reconcile_faro_bundle"]
            if candidate is not orig:
                return candidate
    return _rs_mod._reconcile_faro_bundle


@cron_task("cron_rum_discovery", job_name="rum_discovery")
def _run_rum_discovery_cron(service_id: str, run_id: int | None = None) -> None:
    """Discover new ``raw/rum/`` beacon files and dispatch batched
    ``convert_batch_rum_files`` calls covering them. Runs inline so the
    cron_runs row carries the real per-tick outcome instead of a fake instant
    success.
    """
    if _refuse_if_dev_no_crons(service_id, "RUM discovery"):
        return

    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg or not svcconfig.is_rum_enabled(cfg):
        return
    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_scale_mode(src):
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
        faro_ok, faro_err = True, None
        try:
            reconcile_fn = _resolve_reconcile_faro_bundle()
            faro_ok = reconcile_fn(service_id, run_id)
        except Exception as fe:
            faro_ok, faro_err = False, str(fe)
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
        from backend.provision.cli import rum_minute_list_prefix

        now = datetime.now(UTC)
        discovered = sum(
            discover_rum_prefix(
                service_id, prefix_subpath=rum_minute_list_prefix(now - timedelta(minutes=i)), run_id=run_id
            )
            for i in range(5)
        )
        base_summary = (
            f"Discovered {discovered} new RUM file(s); dispatched to ingest workers"
            if discovered
            else "No new RUM files"
        )
        if not faro_ok:
            warn_msg = f"Faro bundle reconcile issue ({faro_err or 'drift/restore warning'})"
            run.record(
                "warning",
                summary=f"{base_summary} ({warn_msg})",
                error_message=faro_err or "Faro bundle reconcile failed",
                files_downloaded=discovered,
            )
        else:
            run.record("success", summary=base_summary, error_message=None, files_downloaded=discovered)


def _run_ledger_sweep_impl(
    service_id: str,
    run_id: int | None,
    *,
    job_name: str,
    sweep_fn,
    label: str,
    require_rum: bool = False,
    finalize_running_backstop: bool = False,
    include_warning_error_message: bool = False,
) -> None:
    if _refuse_if_dev_no_crons(service_id, label):
        return

    from backend.core.duckdb import get_source_for_service

    cfg = svcconfig.load_config(service_id)
    if not cfg or (require_rum and not svcconfig.is_rum_enabled(cfg)):
        return
    src = get_source_for_service(service_id)
    if src is None or not svcconfig.is_high_scale_mode(src):
        return

    row_prefix = "RUM " if require_rum else ""
    with cron_run(
        service_id,
        job_name,
        source=src,
        run_id=run_id,
        error_summary=f"{label[0].upper()}{label[1:]} failed",
        finalize_duration=True,
        finalize_running_backstop=finalize_running_backstop,
        include_log_output=False,
    ) as run:
        if not run.ok:
            return
        run.log_progress({"type": "status", "message": f"Starting {label} for {service_id}..."})

        summary = sweep_fn(service_id, run_id=run.run_id)
        warnings = []
        if not summary.get("broker_ok", True):
            warnings.append("Celery broker/queue depth probe failed")
        if summary.get("dead_letter", 0) > 0:
            warnings.append(f"{summary['dead_letter']} dead-letter/quarantined {row_prefix}row(s)")

        summary_msg = (
            f"reclaimed={summary.get('reclaimed', 0)} redispatched={summary.get('redispatched', 0)} "
            f"discovered={summary.get('discovered', 0)}" + (f" ({'; '.join(warnings)})" if warnings else "")
        )
        record_kwargs: dict = {"summary": summary_msg, "files_downloaded": summary.get("discovered", 0)}
        if include_warning_error_message:
            record_kwargs["error_message"] = "; ".join(warnings) if warnings else None
        run.record("warning" if warnings else "success", **record_kwargs)
        run.log_progress({"type": "done", "message": summary_msg})


@cron_task("ledger_sweep", job_name="ledger_sweep")
def _run_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net: reclaim stale ledger claims, re-dispatch stuck
    rows, and diff a lookback FOS LIST against the ledger."""
    from backend.core.ingest import sweep_ledger_once

    _run_ledger_sweep_impl(
        service_id, run_id, job_name="ledger_sweep", sweep_fn=sweep_ledger_once, label="ledger sweep"
    )


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net for the RUM ledger pipeline."""
    from backend.core.ingest import sweep_rum_ledger_once

    _run_ledger_sweep_impl(
        service_id,
        run_id,
        job_name="ledger_rum_sweep",
        sweep_fn=sweep_rum_ledger_once,
        label="RUM ledger sweep",
        require_rum=True,
        finalize_running_backstop=True,
        include_warning_error_message=True,
    )
