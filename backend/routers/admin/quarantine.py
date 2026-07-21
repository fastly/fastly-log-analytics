"""Quarantined ingest files — list, summary, download, and manual purge."""

from __future__ import annotations

import json

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.core import metadata as metadata_db
from backend.deps import get_source
from backend.models.admin import (
    QuarantinedFile,
    QuarantineListResponse,
    QuarantineSummary,
)
from backend.utils.router_utils import make_error

from ._router import router


def _service_id_from_source(source: dict) -> str:
    return source.get("service_id") or source.get("name", "")


@router.get("/admin/quarantine", response_model=QuarantineListResponse)
def list_quarantine(
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    source: dict = Depends(get_source),
) -> dict:
    service_id = _service_id_from_source(source)
    files = metadata_db.list_quarantined_files(service_id, limit=limit, offset=offset)
    summary = metadata_db.get_quarantine_summary(service_id)
    return {
        "files": [
            QuarantinedFile(
                id=f["id"],
                file_name=f["file_name"],
                error_key=f["error_key"],
                valid_rows=f["valid_rows"],
                corrupt_rows=f["corrupt_rows"],
                file_size_bytes=f["file_size_bytes"],
                corrupt_samples=f["corrupt_samples"],
                reason_counts=f.get("reason_counts", {}),
                quarantined_at=f["quarantined_at"],
            )
            for f in files
        ],
        "total": summary["total_files"],
        "summary": QuarantineSummary(**summary),
    }


@router.get("/admin/quarantine/summary", response_model=QuarantineSummary)
def quarantine_summary(source: dict = Depends(get_source)) -> dict:
    service_id = _service_id_from_source(source)
    return metadata_db.get_quarantine_summary(service_id)


@router.get("/admin/quarantine/export")
def export_quarantine(source: dict = Depends(get_source)) -> StreamingResponse:
    """Export all quarantine metadata as JSONL (no FOS fetch — SQLite only)."""
    service_id = _service_id_from_source(source)
    files = metadata_db.list_quarantined_files(service_id, limit=10_000, offset=0)

    def _generate():
        for f in files:
            yield json.dumps(f, default=str) + "\n"

    return StreamingResponse(
        _generate(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": "attachment; filename=quarantine-export.jsonl"},
    )


@router.get("/admin/quarantine/{quarantine_id}/download")
def download_quarantined_file(
    quarantine_id: int,
    source: dict = Depends(get_source),
) -> StreamingResponse:
    """Stream the ``.bad.jsonl`` for a single quarantined file from FOS."""
    from backend.core.duckdb import _get_fos_client

    service_id = _service_id_from_source(source)
    record = metadata_db.get_quarantined_file_by_id(service_id, quarantine_id)
    if not record:
        raise HTTPException(
            status_code=404, detail=make_error("quarantine_not_found", f"Quarantine record {quarantine_id} not found")
        )

    fos_client = _get_fos_client(source)
    obj = fos_client.get_object(Bucket=source["bucket"], Key=record["error_key"])
    body = obj["Body"]

    return StreamingResponse(
        body.iter_chunks(chunk_size=65536) if hasattr(body, "iter_chunks") else iter([body.read()]),
        media_type="application/x-ndjson",
        headers={
            "Content-Disposition": f'attachment; filename="{record["file_name"]}.bad.jsonl"',
        },
    )


@router.post("/admin/quarantine/purge")
def purge_quarantine(
    retention_days: int = Query(default=0, ge=0),
    source: dict = Depends(get_source),
) -> dict:
    from backend.core.duckdb import _get_fos_client
    from backend.core.ingest import _delete_objects_robust

    service_id = _service_id_from_source(source)
    expired = metadata_db.get_expired_quarantined_files(service_id, retention_days=retention_days)
    if not expired:
        return {"purged_fos": 0, "purged_metadata": 0}

    keys_to_delete = []
    ids_to_delete = []
    for row in expired:
        keys_to_delete.append(row["error_key"])
        keys_to_delete.append(row["meta_key"])
        ids_to_delete.append(row["id"])

    purged_fos = 0
    try:
        fos_client = _get_fos_client(source)
        purged_fos = _delete_objects_robust(fos_client, source["bucket"], keys_to_delete)
    except Exception:
        pass

    purged_meta = metadata_db.delete_quarantined_rows(service_id, ids_to_delete)
    return {"purged_fos": purged_fos, "purged_metadata": purged_meta}
