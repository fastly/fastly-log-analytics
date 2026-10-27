"""Tree browsing and ZIP/file download admin endpoints."""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator
from typing import Any

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.config import path_within_dir
from backend.core.request_context import get_source
from backend.models.admin import TreeResponse
from backend.utils.router_utils import query_errors, raise_internal, require_service_id

from ._helpers import _fetch_file_to_zip, _resolve_source, stream_zip
from ._router import router

logger = logging.getLogger(__name__)


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


@router.get("/admin/storage-tree", response_model=TreeResponse)
def storage_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import duckdb as _db_mod

    _db: Any = _db_mod
    result = _db.get_storage_tree_node(source, prefix)
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

                from backend.routers import admin as _admin_pkg

                _fn = _fetch_file_to_zip
                _orig = getattr(sys.modules.get("backend.routers.admin.health"), "_fetch_file_to_zip", None)
                if _fn is _orig:
                    _fn = getattr(_admin_pkg, "_fetch_file_to_zip", _fn)
                _fn(source, fos_client, cdn, key, arcname, zf, "download_zip")

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
                    from backend.routers import admin as _admin_pkg

                    _fn = _fetch_file_to_zip
                    _orig = getattr(sys.modules.get("backend.routers.admin.health"), "_fetch_file_to_zip", None)
                    if _fn is _orig:
                        _fn = getattr(_admin_pkg, "_fetch_file_to_zip", _fn)
                    _fn(src, fos_client, cdn, key, key, zf, "download_all")

    headers = {
        "Content-Disposition": f'attachment; filename="fastly_logs_{service_id}.zip"',
    }

    return StreamingResponse(
        stream_zip(f"api:GET /download-all:{include}", service_id, populate),
        media_type="application/zip",
        headers=headers,
    )


__all__ = [
    "_fetch_file_to_zip",
    "_resolve_source",
    "download_all_files",
    "download_file",
    "download_folder",
    "raw_tree_endpoint",
    "storage_tree_endpoint",
]
