"""Manual ingest trigger + ingest quarantine evidence + download endpoints."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from typing import Any

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.config import path_within_dir
from backend.core.metadata import ingest_log as quarantine
from backend.core.metadata.ingest_log import (
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
    TreeResponse,
)
from backend.utils.router_utils import (
    load_service_config,
    make_error,
    query_errors,
    raise_internal,
    require_service_id,
    run_admin_cron_trigger,
)

from .high_scale import _fetch_file_to_zip, router, stream_zip

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
    from backend.cron.jobs.rum_sync import _run_ledger_sweep

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
    from backend.cron.jobs.rum_sync import _run_rum_discovery_cron

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
    from backend.cron.jobs.rum_sync import _run_rum_ledger_sweep

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


# ── Download endpoints (folded from downloads.py) ─────────────────────────────


@router.get("/admin/raw-tree", response_model=TreeResponse)
def raw_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import duckdb as _db_mod

    _db: Any = _db_mod
    result = _db.get_raw_tree_node(source, prefix, root="raw")
    resp: TreeResponse = TreeResponse.with_telemetry(nodes=result.get("children", []))
    return resp


@router.get("/admin/iceberg-tree", response_model=TreeResponse)
def iceberg_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import duckdb as _db_mod

    _db: Any = _db_mod
    result = _db.get_raw_tree_node(source, prefix, root="iceberg")
    resp: TreeResponse = TreeResponse.with_telemetry(nodes=result.get("children", []))
    return resp


@router.get("/download-folder")
def download_folder(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
    root: str = Query(default="raw"),
) -> StreamingResponse:
    from backend.core import duckdb as _db

    prefix = prefix.strip("/")
    base_prefix = source.get("prefix", "").strip().rstrip("/")
    if base_prefix:
        target_prefix = f"{base_prefix}/{root}/{prefix}" if prefix else f"{base_prefix}/{root}/"
    else:
        target_prefix = f"{root}/{prefix}" if prefix else f"{root}/"

    if not target_prefix.endswith("/"):
        target_prefix += "/"

    def populate(zf: Any) -> None:
        cdn = source.get("cdn_url", "").rstrip("/")
        fos_client = _db._get_fos_client(source)
        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_zip")
        pages = paginator.paginate(Bucket=source["bucket"], Prefix=target_prefix)

        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj["Key"]
                if key.endswith("/"):  # Skip directory markers
                    continue

                top_folder = os.path.basename(prefix) if prefix else root
                rel_path = key[len(target_prefix) :]
                arcname = f"{top_folder}/{rel_path}" if rel_path else os.path.basename(key)

                _fetch_file_to_zip(source, fos_client, cdn, key, arcname, zf, "download_zip")

    safe_name = prefix.replace("/", "_") or root
    headers = {
        "Content-Disposition": f'attachment; filename="{safe_name}.zip"',
    }

    return StreamingResponse(
        stream_zip(f"api:GET /admin/download-zip:{root}", source.get("name", ""), populate),
        media_type="application/zip",
        headers=headers,
    )


@router.get("/download")
@query_errors(status_code=500)
def download_file(
    source: dict[str, Any] = Depends(get_source),
    key: str = Query(default=""),
) -> Any:
    import posixpath
    import urllib.parse

    from fastapi.responses import FileResponse

    from backend.core.duckdb import _cache_dir, _get_fos_client

    if not key:
        raise HTTPException(status_code=400, detail={"error": "Missing key parameter"})

    key = posixpath.normpath(key)

    src_prefix = source.get("prefix", "")
    if src_prefix:
        if not src_prefix.endswith("/"):
            src_prefix += "/"
        if not key.startswith(src_prefix):
            raise HTTPException(status_code=400, detail={"error": "invalid_key"})

    cache_dir = os.path.realpath(_cache_dir(source))
    candidate = os.path.realpath(os.path.join(cache_dir, key))
    if not path_within_dir(cache_dir, candidate):
        raise HTTPException(status_code=400, detail={"error": "invalid_key"})
    local_path = candidate
    if os.path.exists(local_path):
        return FileResponse(local_path, filename=os.path.basename(local_path))

    from backend.utils.telemetry import record_call as _record_call

    cdn = source.get("cdn_url", "").rstrip("/")
    if cdn:
        import time as _time
        import urllib.request

        from backend.utils.telemetry import record_cdn_call as _rcdn

        url = f"{cdn}/{urllib.parse.quote(key)}"
        req = urllib.request.Request(url)
        if source.get("cdn_secret"):
            req.add_header("x-fastly-key", source["cdn_secret"])
        try:
            cdn_resp = urllib.request.urlopen(req, timeout=30)
        except Exception as exc:
            raise_internal(logger, exc, code="cdn_fetch_failed", status=502)

        try:
            content_type = cdn_resp.headers.get("Content-Type") or "application/octet-stream"
            content_length = cdn_resp.headers.get("Content-Length")
            filename = os.path.basename(key) or "download"
        except Exception:
            try:
                cdn_resp.close()
            except Exception:
                logger.debug("download_file: cdn_resp.close failed (ignored)", exc_info=True)
            raise

        def _iter_cdn(chunk_size: int = 65536) -> Iterator[bytes]:
            bytes_read = 0
            t0 = _time.time()
            cdn_headers = cdn_resp.headers
            try:
                while True:
                    chunk = cdn_resp.read(chunk_size)
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    yield chunk
            finally:
                try:
                    cdn_resp.close()
                except Exception:
                    logger.debug("_iter_cdn: cdn_resp.close failed (ignored)", exc_info=True)
                try:
                    _rcdn(
                        "GET",
                        key,
                        round((_time.time() - t0) * 1000, 2),
                        headers=cdn_headers,
                        bytes_count=bytes_read,
                        caller="api:/download",
                    )
                except Exception:
                    logger.debug("_iter_cdn: _rcdn failed (ignored)", exc_info=True)

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "private, no-store",
        }
        if content_length:
            headers["Content-Length"] = content_length
        return StreamingResponse(_iter_cdn(), media_type=content_type, headers=headers)

    fos_client = _get_fos_client(source)
    import time as _time

    try:
        t0 = _time.time()
        obj = fos_client.get_object(Bucket=source["bucket"], Key=key)
        _record_call(
            "GET_OBJECT",
            f"{source['bucket']}/{key}",
            round((_time.time() - t0) * 1000, 2),
            status="SUCCESS",
            service="FOS",
            details="download stream · Class B",
            caller="api:/download",
        )
    except Exception as exc:
        raise_internal(logger, exc, code="fos_fetch_failed", status=502)

    body = obj["Body"]
    content_type = obj.get("ContentType") or "application/octet-stream"
    content_length = obj.get("ContentLength")
    filename = os.path.basename(key) or "download"

    def _iter_fos(chunk_size: int = 65536) -> Iterator[bytes]:
        try:
            yield from body.iter_chunks(chunk_size)
        finally:
            try:
                body.close()
            except Exception:
                logger.debug("_iter_fos: body.close failed (ignored)", exc_info=True)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "private, no-store",
    }
    if content_length:
        headers["Content-Length"] = str(content_length)

    return StreamingResponse(_iter_fos(), media_type=content_type, headers=headers)


@router.get("/download-all")
def download_all_files(
    source: dict[str, Any] = Depends(get_source),
    include: str = Query(default="all"),
) -> StreamingResponse:
    from backend.core import duckdb as _db

    src = source
    service_id = require_service_id(src.get("name"))

    def populate(zf: Any) -> None:
        if include == "local":
            db_path = src.get("duckdb_path")
            if not db_path:
                from backend import config as svcconfig

                db_path = svcconfig.duckdb_path(service_id)
            if db_path and os.path.exists(db_path):
                zf.write(db_path, os.path.basename(db_path))

            cache_dir = os.path.realpath(_db._cache_dir(src))
            walk_dir = os.path.realpath(
                os.path.join(cache_dir, src.get("prefix", "").lstrip("/")) if src.get("prefix") else cache_dir
            )
            if not path_within_dir(cache_dir, walk_dir):
                raise ValueError("invalid prefix")

            if os.path.exists(walk_dir):
                for root_dir, _, files in os.walk(walk_dir):
                    for file in files:
                        file_path = os.path.join(root_dir, file)
                        arcname = os.path.relpath(file_path, cache_dir)
                        zf.write(file_path, arcname)
        else:
            cdn = src.get("cdn_url", "").rstrip("/")
            fos_client = _db._get_fos_client(src)
            paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_all")
            pages = paginator.paginate(Bucket=src["bucket"], Prefix=src.get("prefix", ""))

            for page in pages:
                if "Contents" not in page:
                    continue
                for obj in page["Contents"]:
                    key = obj["Key"]
                    _fetch_file_to_zip(src, fos_client, cdn, key, key, zf, "download_all")

    headers = {
        "Content-Disposition": f'attachment; filename="fastly_logs_{service_id}.zip"',
    }

    return StreamingResponse(
        stream_zip(f"api:GET /download-all:{include}", service_id, populate),
        media_type="application/zip",
        headers=headers,
    )


sys.modules["backend.routers.admin.downloads"] = sys.modules[__name__]
