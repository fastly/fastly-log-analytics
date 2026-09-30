"""Admin-only access to per-item exact-byte ingest quarantine evidence."""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.core import quarantine
from backend.core.metadata.quarantine import (
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    list_quarantine_evidence,
)
from backend.deps import get_source, require_admin
from backend.models.admin import QuarantineEvidenceItem, QuarantineListResponse, QuarantineSummary
from backend.utils.router_utils import make_error

from ._router import router

_MAX_EVIDENCE_DOWNLOAD_BYTES = 50 * 1024 * 1024


def _service_id_from_source(source: dict) -> str:
    return source.get("service_id") or source.get("name", "")


def _require_read_write_source(source: dict) -> None:
    if source.get("access_level", "read_write") != "read_write":
        raise HTTPException(
            status_code=403,
            detail=make_error("admin_only", "This operation requires admin access."),
        )


@router.get("/admin/quarantine", response_model=QuarantineListResponse)
def list_quarantine(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    error_category: str | None = Query(default=None, min_length=1, max_length=64),
    source: dict = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> dict:
    _require_read_write_source(source)
    service_id = _service_id_from_source(source)
    items = list_quarantine_evidence(
        service_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
    )
    for item in items:
        item["quarantined_at"] = str(item["quarantined_at"])
    summary = get_quarantine_evidence_summary(service_id)
    return {
        "items": [QuarantineEvidenceItem(**item) for item in items],
        "total": summary["category_counts"].get(error_category, 0) if error_category else summary["total_items"],
    }


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


@router.post("/admin/quarantine/purge")
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
