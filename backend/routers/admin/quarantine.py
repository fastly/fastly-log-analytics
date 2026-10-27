"""Ingest quarantine evidence listing, summary, download, and purge admin endpoints."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.core.metadata import ingest_log as _quarantine_store
from backend.core.metadata.ingest_log import (
    count_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    list_quarantine_evidence,
)
from backend.core.request_context import get_source, require_admin
from backend.models.admin import (
    QuarantineDeleteResponse,
    QuarantineEvidenceItem,
    QuarantineListResponse,
    QuarantineSummary,
)
from backend.utils.router_utils import make_error, require_service_id

from ._router import router

_MAX_EVIDENCE_DOWNLOAD_BYTES = 50 * 1024 * 1024


def _service_id_from_source(source: dict[str, Any]) -> str:
    return str(source.get("service_id") or source.get("name", ""))


def _fetch_quarantine_list_payload(
    service_id: str,
    *,
    limit: int,
    offset: int,
    error_category: str | None,
    source_type: str | None,
) -> QuarantineListResponse:
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
    return QuarantineListResponse(
        items=[QuarantineEvidenceItem(**item) for item in items],
        total=total,
    )


@router.get("/admin/quarantine", response_model=QuarantineListResponse)
def list_quarantine(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    error_category: str | None = Query(default=None, min_length=1, max_length=64),
    source_type: str | None = Query(default=None, min_length=1, max_length=32),
    source: dict[str, Any] = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> QuarantineListResponse:
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
) -> QuarantineListResponse:
    from backend.core.duckdb import get_source_for_service

    resolved_id = require_service_id(service_id)
    src = get_source_for_service(resolved_id)
    if not src:
        raise HTTPException(
            status_code=404,
            detail=make_error("service_not_found", f"Service {resolved_id} not found."),
        )
    return _fetch_quarantine_list_payload(
        resolved_id,
        limit=limit,
        offset=offset,
        error_category=error_category,
        source_type=source_type,
    )


@router.get("/admin/quarantine/summary", response_model=QuarantineSummary)
def quarantine_summary(
    source: dict[str, Any] = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> QuarantineSummary:
    res = get_quarantine_evidence_summary(_service_id_from_source(source))
    return QuarantineSummary(**res)


@router.get("/admin/quarantine/download/{item_id}")
def download_quarantine_evidence(
    item_id: int,
    source: dict[str, Any] = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> StreamingResponse:
    service_id = _service_id_from_source(source)
    try:
        result = _quarantine_store.read_evidence(service_id, item_id)
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
    source: dict[str, Any] = Depends(get_source),
    _admin: None = Depends(require_admin),
) -> QuarantineDeleteResponse:
    service_id = _service_id_from_source(source)
    if item_id is not None and get_quarantine_evidence(service_id, item_id) is None:
        raise HTTPException(
            status_code=404,
            detail=make_error("quarantine_not_found", f"Quarantine item {item_id} not found."),
        )
    res = _quarantine_store.purge_evidence(service_id, item_id)
    return QuarantineDeleteResponse(**res)
