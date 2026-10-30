"""Manual ingest trigger, full-sweep, gap-heal, and ledger sweep admin endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Depends, HTTPException, Query

from backend.core.request_context import get_source, require_admin
from backend.models.admin import SyncStartResponse
from backend.utils import router_utils as _ru
from backend.utils.router_utils import (
    load_service_config,
    make_error,
)

from ._router import router

logger = logging.getLogger(__name__)


@router.post(
    "/admin/sync/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def sync_service(
    service_id: str,
    force: bool = Query(default=True, description="Force sync even if throttled or intervals not elapsed"),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_log_discovery_cron

    res = _ru.run_admin_cron_trigger(
        source,
        "log_discovery",
        _run_log_discovery_cron,
        target_kwargs={"force": force, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/full-sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def full_sweep_service(
    service_id: str,
    force: bool = Query(default=True, description="Force full sweep even if active requests or throttled"),
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_full_sweep

    res = _ru.run_admin_cron_trigger(
        source,
        "full_sync",
        _run_full_sweep,
        target_kwargs={"force": force},
        success_msg="Full sweep started.",
        in_progress_msg="Full sweep already running.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/gap-heal/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def gap_heal_service(
    service_id: str,
    force: bool = Query(default=True, description="Force gap heal evaluation even if throttled or active requests"),
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_gap_heal

    res = _ru.run_admin_cron_trigger(
        source,
        "gap_heal",
        _run_gap_heal,
        target_kwargs={"force": force},
        success_msg="Gap heal evaluation started.",
        in_progress_msg="Gap heal already running.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/ledger/sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def ledger_sweep_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_ledger_sweep

    res = _ru.run_admin_cron_trigger(
        source,
        "ledger_sweep",
        _run_ledger_sweep,
        success_msg="Ledger sweep started.",
        in_progress_msg="Ledger sweep already running.",
        require_high_scale=True,
        high_scale_detail="Ledger sweep is only supported under high-scale mode.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/rum/sync/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_sync_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_rum_sync

    res = _ru.run_admin_cron_trigger(
        source,
        "rum_sync",
        _run_rum_sync,
        success_msg="RUM sync started.",
        in_progress_msg="RUM sync already running.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/rum/discovery/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_discovery_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_rum_discovery_cron

    res = _ru.run_admin_cron_trigger(
        source,
        "rum_discovery",
        _run_rum_discovery_cron,
        success_msg="RUM discovery started.",
        in_progress_msg="RUM discovery already running.",
        require_high_scale=True,
        high_scale_detail="RUM discovery is only supported under high-scale mode.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post(
    "/admin/rum/ledger/sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_ledger_sweep_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend import config as svcconfig
    from backend.cron.jobs.sync import _run_rum_ledger_sweep

    def _precheck() -> None:
        try:
            cfg = load_service_config(service_id)
        except HTTPException:
            raise HTTPException(
                status_code=400,
                detail=make_error("service_not_found", "Service configuration not found."),
            ) from None
        if not svcconfig.is_rum_enabled(cfg):
            raise HTTPException(
                status_code=400,
                detail=make_error("rum_not_enabled", "RUM is not enabled for this service."),
            )

    res = _ru.run_admin_cron_trigger(
        source,
        "ledger_rum_sweep",
        _run_rum_ledger_sweep,
        success_msg="RUM ledger sweep started.",
        in_progress_msg="RUM ledger sweep already running.",
        require_high_scale=True,
        high_scale_detail=make_error(
            "high_scale_required", "RUM ledger sweep is only supported under high-scale mode."
        ),
        precheck=_precheck,
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)


@router.post("/admin/ingest-logs", response_model=SyncStartResponse, response_model_exclude_unset=True)
def ingest_endpoint(
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    source: dict[str, Any] = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.sync import _run_log_discovery_cron
    from backend.repositories.dashboard import invalidate_service

    src = source
    invalidate_service(src["name"])
    res = _ru.start_or_resume_cron(
        src,
        "log_discovery",
        _run_log_discovery_cron,
        target_kwargs={"force": True, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
    )
    if isinstance(res, SyncStartResponse):
        return res
    return SyncStartResponse(**res)
