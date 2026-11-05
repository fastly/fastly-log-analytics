"""Celery/ledger-mode RUM and request ingest discovery & sweep cron jobs."""

from __future__ import annotations

import logging
import sys as _sys
from datetime import UTC, datetime, timedelta
from typing import Any

from backend import config as svcconfig
from backend.core.fos_client import _get_fos_client, get_source_for_service
from backend.cron.jobs._common import _refuse_if_dev_no_crons
from backend.cron.scheduler import cron_run, cron_task
from backend.high_scale.ingest_controller import (
    INITIAL_SOURCE_CURSOR,
    MAX_SOURCE_PAGE_SIZE,
    S3SourceObjectLister,
)
from backend.high_scale.postgres_control import PostgresControlPlane

logger = logging.getLogger("backend.scheduler")


def _discover_high_scale_prefix(
    service_id: str,
    *,
    domain: str,
    prefix_subpath: str | None = None,
) -> int:
    src = get_source_for_service(service_id)
    if src is None or not src.get("bucket"):
        return 0
    fos_client = _get_fos_client(src)
    lister = S3SourceObjectLister(
        fos_client,
        bucket=str(src["bucket"]),
        prefix=str(src.get("prefix") or ""),
    )
    sub_prefix = ""
    if prefix_subpath:
        clean = prefix_subpath.lstrip("/")
        for base in ("raw/request/", "raw/rum/"):
            if clean.startswith(base):
                clean = clean[len(base) :]
                break
        sub_prefix = clean
    page = lister.list_source_objects(
        service_id,
        domain,
        page_size=MAX_SOURCE_PAGE_SIZE,
        cursor=None,
        partition_prefix=sub_prefix,
    )
    if not page.objects:
        return 0
    control = PostgresControlPlane()
    owner = control.initialize_owner(service_id, owner="high_scale", source_cursor=INITIAL_SOURCE_CURSOR)
    by_key = control.discover_sources_batch(
        service_id,
        domain,
        page.objects,
        expected_owner=owner.current_owner,
        expected_owner_epoch=owner.owner_epoch,
    )
    return sum(1 for rec in by_key.values() if getattr(rec, "status", None) == "discovered")


def discover_rum_prefix(
    service_id: str,
    prefix_subpath: str | None = None,
    run_id: int | None = None,
) -> int:
    del run_id
    return _discover_high_scale_prefix(service_id, domain="rum_vitals", prefix_subpath=prefix_subpath)


def _sweep_high_scale_ledger(service_id: str, *, domain: str, run_id: int | None = None) -> dict[str, Any]:
    del run_id
    broker_ok = True
    if svcconfig.CELERY_BROKER_URL:
        try:
            from backend.celery_app import celery_queue_depths

            _, broker_ok = celery_queue_depths()
        except Exception:
            broker_ok = False
    control = PostgresControlPlane()
    control.initialize_owner(service_id, owner="high_scale", source_cursor=INITIAL_SOURCE_CURSOR)
    expired = control.expired_claims(service_id, domain, limit=MAX_SOURCE_PAGE_SIZE)
    discovered = _discover_high_scale_prefix(service_id, domain=domain)
    snap = control.operational_snapshot(service_id)
    dead_letter = sum(
        int(row.get("count", 0))
        for row in snap.get("source_objects", [])
        if row.get("domain") == domain and row.get("status") == "source_missing"
    )
    return {
        "reclaimed": len(expired),
        "redispatched": len(expired),
        "discovered": discovered,
        "dead_letter": dead_letter,
        "broker_ok": broker_ok,
    }


def sweep_ledger_once(service_id: str, lookback_hours: int = 4, run_id: int | None = None) -> dict[str, Any]:
    del lookback_hours
    return _sweep_high_scale_ledger(service_id, domain="request", run_id=run_id)


def sweep_rum_ledger_once(service_id: str, lookback_hours: int = 4, run_id: int | None = None) -> dict[str, Any]:
    del lookback_hours
    return _sweep_high_scale_ledger(service_id, domain="rum_vitals", run_id=run_id)


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
    """Discover new ``raw/rum/`` beacon files and register them in the high-scale control plane."""
    if _refuse_if_dev_no_crons(service_id, "RUM discovery"):
        return

    cfg = svcconfig.load_config(service_id)
    if not cfg or not svcconfig.is_rum_enabled(cfg):
        return
    src = get_source_for_service(service_id)
    if src is None:
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

    cfg = svcconfig.load_config(service_id)
    if not cfg or (require_rum and not svcconfig.is_rum_enabled(cfg)):
        return
    src = get_source_for_service(service_id)
    if src is None:
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
    _run_ledger_sweep_impl(
        service_id, run_id, job_name="ledger_sweep", sweep_fn=sweep_ledger_once, label="ledger sweep"
    )


@cron_task("ledger_rum_sweep", job_name="ledger_rum_sweep")
def _run_rum_ledger_sweep(service_id: str, run_id: int | None = None) -> None:
    """Celery-mode crash net for the RUM ledger pipeline."""
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
