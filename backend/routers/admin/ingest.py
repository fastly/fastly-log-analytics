"""Manual ingest trigger endpoint."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Query

from backend.deps import get_source, require_admin
from backend.models.admin import SyncStartResponse
from backend.utils.router_utils import make_error

from ._router import router


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
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    if source.get("access_level") == "read_only":
        raise HTTPException(
            status_code=403,
            detail=make_error("read_only", "Read-only services cannot run log discovery."),
        )

    from backend.cron.jobs.sync import _run_log_discovery_cron as _run_service_cron
    from backend.repositories.dashboard import invalidate_service
    from backend.utils.router_utils import start_or_resume_cron

    invalidate_service(source["name"])
    res = start_or_resume_cron(
        source,
        "log_discovery",
        _run_service_cron,
        target_kwargs={"force": force, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
    )
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
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    if source.get("access_level") == "read_only":
        raise HTTPException(
            status_code=403,
            detail=make_error("read_only", "Read-only services cannot run full sweep."),
        )

    from backend.cron.jobs.sync import _run_full_sweep
    from backend.repositories.dashboard import invalidate_service
    from backend.utils.router_utils import start_or_resume_cron

    invalidate_service(source["name"])
    res = start_or_resume_cron(
        source,
        "full_sync",
        _run_full_sweep,
        target_kwargs={"force": force},
        success_msg="Full sweep started.",
        in_progress_msg="Full sweep already running.",
    )
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
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    if source.get("access_level") == "read_only":
        raise HTTPException(
            status_code=403,
            detail=make_error("read_only", "Read-only services cannot run gap heal."),
        )

    from backend.cron.jobs.sync import _run_gap_heal
    from backend.repositories.dashboard import invalidate_service
    from backend.utils.router_utils import start_or_resume_cron

    invalidate_service(source["name"])
    res = start_or_resume_cron(
        source,
        "gap_heal",
        _run_gap_heal,
        target_kwargs={"force": force},
        success_msg="Gap heal evaluation started.",
        in_progress_msg="Gap heal already running.",
    )
    return SyncStartResponse(**res)


@router.post("/admin/ingest-logs")
def ingest_endpoint(
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    source: dict = Depends(get_source),
) -> dict:
    from backend.cron.jobs.metadata import _run_metadata_sync
    from backend.cron.jobs.sync import _run_log_discovery_cron as _run_service_cron
    from backend.repositories.dashboard import invalidate_service
    from backend.utils.router_utils import start_or_resume_cron

    src = source
    invalidate_service(src["name"])
    if source.get("access_level") == "read_only":
        return start_or_resume_cron(
            source,
            "metadata_sync",
            _run_metadata_sync,
            target_kwargs={"start_time": start_time, "end_time": end_time},
            success_msg="Metadata sync started.",
            in_progress_msg="Metadata sync already running.",
        )
    return start_or_resume_cron(
        src,
        "log_discovery",
        _run_service_cron,
        target_kwargs={"force": True, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
    )
