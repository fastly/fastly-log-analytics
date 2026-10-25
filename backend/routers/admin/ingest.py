"""Manual ingest trigger + ingest quarantine evidence endpoints."""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.core import quarantine
from backend.core.metadata.quarantine import (
    count_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    list_quarantine_evidence,
)
from backend.deps import get_source, require_admin
from backend.models.admin import (
    QuarantineDeleteResponse,
    QuarantineEvidenceItem,
    QuarantineListResponse,
    QuarantineSummary,
    SyncStartResponse,
)
from backend.utils.router_utils import load_service_config, make_error, require_service_id, run_admin_cron_trigger

from ._helpers import router


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
    from backend.cron.jobs.sync import _run_log_discovery_cron

    return run_admin_cron_trigger(
        source,
        "log_discovery",
        _run_log_discovery_cron,
        target_kwargs={"force": force, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
        readonly_detail="Read-only services cannot run log discovery.",
    )


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
    from backend.cron.jobs.sync import _run_full_sweep

    return run_admin_cron_trigger(
        source,
        "full_sync",
        _run_full_sweep,
        target_kwargs={"force": force},
        success_msg="Full sweep started.",
        in_progress_msg="Full sweep already running.",
        readonly_detail="Read-only services cannot run full sweep.",
    )


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
    from backend.cron.jobs.sync import _run_gap_heal

    return run_admin_cron_trigger(
        source,
        "gap_heal",
        _run_gap_heal,
        target_kwargs={"force": force},
        success_msg="Gap heal evaluation started.",
        in_progress_msg="Gap heal already running.",
        readonly_detail="Read-only services cannot run gap heal.",
    )


@router.post(
    "/admin/ledger/sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def ledger_sweep_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.rum_ledger import _run_ledger_sweep

    return run_admin_cron_trigger(
        source,
        "ledger_sweep",
        _run_ledger_sweep,
        success_msg="Ledger sweep started.",
        in_progress_msg="Ledger sweep already running.",
        readonly_detail="Read-only services cannot run ledger sweep.",
        require_high_scale=True,
        high_scale_detail="Ledger sweep is only supported under high-scale mode.",
    )


@router.post(
    "/admin/rum/sync/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_sync_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.rum_sync import _run_rum_sync

    return run_admin_cron_trigger(
        source,
        "rum_sync",
        _run_rum_sync,
        success_msg="RUM sync started.",
        in_progress_msg="RUM sync already running.",
        readonly_detail="Read-only services cannot run RUM sync.",
    )


@router.post(
    "/admin/rum/discovery/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_discovery_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend.cron.jobs.rum_ledger import _run_rum_discovery_cron

    return run_admin_cron_trigger(
        source,
        "rum_discovery",
        _run_rum_discovery_cron,
        success_msg="RUM discovery started.",
        in_progress_msg="RUM discovery already running.",
        readonly_detail="Read-only services cannot run RUM discovery.",
        require_high_scale=True,
        high_scale_detail="RUM discovery is only supported under high-scale mode.",
    )


@router.post(
    "/admin/rum/ledger/sweep/{service_id}",
    response_model=SyncStartResponse,
    response_model_exclude_unset=True,
)
def rum_ledger_sweep_service(
    service_id: str,
    _admin: None = Depends(require_admin),
    source: dict = Depends(get_source),
) -> SyncStartResponse:
    from backend import config as svcconfig
    from backend.cron.jobs.rum_ledger import _run_rum_ledger_sweep

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

    return run_admin_cron_trigger(
        source,
        "ledger_rum_sweep",
        _run_rum_ledger_sweep,
        success_msg="RUM ledger sweep started.",
        in_progress_msg="RUM ledger sweep already running.",
        readonly_detail=make_error("admin_only", "Read-only services cannot run RUM ledger sweep."),
        require_high_scale=True,
        high_scale_detail=make_error(
            "high_scale_required", "RUM ledger sweep is only supported under high-scale mode."
        ),
        precheck=_precheck,
    )


@router.post("/admin/ingest-logs", response_model=SyncStartResponse, response_model_exclude_unset=True)
def ingest_endpoint(
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    source: dict = Depends(get_source),
) -> dict:
    from backend.cron.jobs.metadata import _run_metadata_sync
    from backend.cron.jobs.sync import _run_log_discovery_cron
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
        _run_log_discovery_cron,
        target_kwargs={"force": True, "start_time": start_time, "end_time": end_time},
        success_msg="Ingestion started.",
        in_progress_msg="Ingestion already running.",
    )


# ── Ingest quarantine evidence endpoints (folded from quarantine.py) ──

_MAX_EVIDENCE_DOWNLOAD_BYTES = 50 * 1024 * 1024


def _service_id_from_source(source: dict) -> str:
    return str(source.get("service_id") or source.get("name", ""))


def _require_read_write_source(source: dict) -> None:
    if source.get("access_level", "read_write") != "read_write":
        raise HTTPException(
            status_code=403,
            detail=make_error("admin_only", "This operation requires admin access."),
        )


def _fetch_quarantine_list_payload(
    service_id: str,
    *,
    limit: int,
    offset: int,
    error_category: str | None,
    source_type: str | None,
) -> dict:
    items = list_quarantine_evidence(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )
    for item in items:
        item["quarantined_at"] = str(item["quarantined_at"])
    total = count_quarantine_evidence(
        service_id,
        error_category=error_category,
        source_type=source_type,
    )
    return {
        "items": [QuarantineEvidenceItem(**item) for item in items],
        "total": total,
    }


@router.get("/admin/quarantine", response_model=QuarantineListResponse)
def list_quarantine(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    error_category: str | None = Query(default=None, min_length=1, max_length=64),
    source_type: str | None = Query(default=None, min_length=1, max_length=32),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    return _fetch_quarantine_list_payload(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )


@router.get("/admin/ledger/quarantine", response_model=QuarantineListResponse)
def list_ledger_quarantine(
    service_id: str | None = Query(default=None, description="Service ID to inspect quarantine for"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    error_category: str | None = Query(default=None, min_length=1, max_length=64),
    source_type: str | None = Query(default=None, min_length=1, max_length=32),
    _admin: None = Depends(require_admin),
) -> dict:
    from backend.core.duckdb import get_source_for_service

    service_id = require_service_id(service_id)
    src = get_source_for_service(service_id)
    if not src:
        raise HTTPException(
            status_code=404,
            detail=make_error("service_not_found", f"Service {service_id} not found."),
        )
    _require_read_write_source(src)
    return _fetch_quarantine_list_payload(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )


@router.get("/admin/quarantine/summary", response_model=QuarantineSummary)
def quarantine_summary(
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    return get_quarantine_evidence_summary(_service_id_from_source(source))


@router.get("/admin/quarantine/download/{item_id}")
def download_quarantine_evidence(
    item_id: int,
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> StreamingResponse:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    try:
        result = quarantine.read_evidence(service_id, item_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail=make_error("quarantine_evidence_invalid", "Stored quarantine evidence has an invalid path."),
        ) from exc
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("quarantine_not_found", f"Quarantine item {item_id} not found."),
        )
    item, evidence_file = result
    actual_size = evidence_file.seek(0, 2)
    evidence_file.seek(0)
    if item["byte_length"] > _MAX_EVIDENCE_DOWNLOAD_BYTES or actual_size > _MAX_EVIDENCE_DOWNLOAD_BYTES:
        evidence_file.close()
        raise HTTPException(
            status_code=413,
            detail=make_error(
                "quarantine_evidence_too_large", "Quarantine evidence exceeds the 50 MiB download limit."
            ),
        )

    def _stream() -> Iterator[bytes]:
        try:
            while chunk := evidence_file.read(65536):
                yield chunk
        finally:
            evidence_file.close()

    return StreamingResponse(
        _stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="quarantine-item-{item_id}.dat"'},
    )


@router.post("/admin/quarantine/purge", response_model=QuarantineDeleteResponse)
def purge_quarantine_evidence(
    item_id: int | None = Query(default=None, gt=0),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict[str, int]:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    if item_id is not None and get_quarantine_evidence(service_id, item_id) is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("quarantine_not_found", f"Quarantine item {item_id} not found."),
        )
    return quarantine.purge_evidence(service_id, item_id)
