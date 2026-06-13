"""Download endpoints: single-file, single-folder ZIP, and full-service ZIP."""

from __future__ import annotations

import logging
import os
import queue
import zipfile
from typing import Any, cast

from fastapi import Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend.deps import get_source
from backend.utils.router_utils import query_errors

from ._helpers import _fetch_file_to_zip, _QueueFile, _stream_from_worker
from ._router import router

logger = logging.getLogger(__name__)


@router.get("/download-folder")
def download_folder(
    source: dict = Depends(get_source),
    prefix: str = Query(default=""),
    root: str = Query(default="raw"),
):
    from backend.core import duckdb as _db

    prefix = prefix.strip("/")
    base_prefix = source.get("prefix", "").strip().rstrip("/")
    if base_prefix:
        target_prefix = f"{base_prefix}/{root}/{prefix}" if prefix else f"{base_prefix}/{root}/"
    else:
        target_prefix = f"{root}/{prefix}" if prefix else f"{root}/"

    if not target_prefix.endswith("/"):
        target_prefix += "/"

    def zip_worker(q: queue.Queue):
        # Independent call-tracking scope: we run on a thread after the API
        # middleware has already flushed, so we own a fresh _CALLS list and
        # flush it ourselves when done. process_context_scope (the context
        # manager) so the fsspec iothread fallback isn't wiped out by a
        # concurrent scope exit on another thread.
        from backend.utils.telemetry import (
            process_context_scope as _pcs,
        )
        from backend.utils.telemetry import (
            start_call_tracking as _sct,
        )
        from backend.utils.usage_logger import flush_usage_log as _flush

        _sct()
        with _pcs(f"api:GET /admin/download-zip:{root}"):
            try:
                # _QueueFile is a stream-shaped duck type the zipfile stubs
                # don't recognise; cast keeps the call site type-safe at the
                # boundary without touching the runtime behaviour.
                with zipfile.ZipFile(cast(Any, _QueueFile(q)), "w", compression=zipfile.ZIP_DEFLATED) as zf:
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
            except Exception:
                logger.error("Error in ZIP generation", exc_info=True)
            finally:
                try:
                    _flush(source.get("name", ""))
                except Exception:
                    pass
                q.put(None)

    safe_name = prefix.replace("/", "_") or root
    headers = {
        "Content-Disposition": f'attachment; filename="{safe_name}.zip"',
    }

    return StreamingResponse(_stream_from_worker(zip_worker), media_type="application/zip", headers=headers)


@router.get("/download")
@query_errors(status_code=500)
def download_file(
    source: dict = Depends(get_source),
    key: str = Query(default=""),
):
    import posixpath
    import urllib.parse

    from fastapi.responses import FileResponse

    from backend.core.duckdb import _cache_dir, _get_fos_client

    if not key:
        raise HTTPException(status_code=400, detail={"error": "Missing key parameter"})

    key = posixpath.normpath(key)

    # Cross-tenant guard: a single FOS bucket can host multiple services
    # separated by per-source prefixes. The path-traversal cage below
    # bounds local cache reads, but a sibling-tenant key like
    # ``other_tenant/file.log`` would still mint a presigned URL or CDN
    # redirect for that object. Require the key to live under this
    # service's prefix before any FOS / CDN URL minting.
    src_prefix = source.get("prefix", "")
    if src_prefix:
        if not src_prefix.endswith("/"):
            src_prefix += "/"
        if not key.startswith(src_prefix):
            raise HTTPException(status_code=400, detail={"error": "invalid_key"})

    # Security: ``os.path.join(base, key)`` returns ``key`` when
    # ``key`` is absolute, which a malicious caller exploits by passing
    # ``key=/etc/passwd``. Resolve both paths and require commonpath ==
    # cache_dir so a path-traversal payload (absolute path or
    # ``../../../etc/passwd``) is rejected at the boundary.
    cache_dir = os.path.realpath(_cache_dir(source))
    candidate = os.path.realpath(os.path.join(cache_dir, key))
    try:
        common = os.path.commonpath([cache_dir, candidate])
    except ValueError:
        # commonpath raises ValueError when paths have different drives /
        # mixed absolute/relative. Treat as path-escape and reject.
        raise HTTPException(status_code=400, detail={"error": "invalid_key"})
    if common != cache_dir:
        raise HTTPException(status_code=400, detail={"error": "invalid_key"})
    local_path = candidate
    if os.path.exists(local_path):
        return FileResponse(local_path, filename=os.path.basename(local_path))

    from backend.utils.telemetry import record_call as _record_call

    cdn = source.get("cdn_url", "").rstrip("/")
    if cdn:
        # Stream the CDN response through this server rather than 307-ing the
        # browser to ``{cdn}/{key}?key={cdn_secret}``. The static cdn_secret
        # is a shared bearer token; embedding it in the redirect Location
        # leaks it into browser history, the address bar, the Referer header
        # of any subsequent navigation, and any HTTP intermediaries. By
        # fetching server-side with the ``x-fastly-key`` header (which the
        # CDN VCL accepts equivalently — see backend/core/fastly/utils.py)
        # the secret never leaves the trust boundary. See audit finding 009.
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
            raise HTTPException(
                status_code=502,
                detail={"error": f"cdn fetch failed: {exc}"},
            )

        content_type = cdn_resp.headers.get("Content-Type") or "application/octet-stream"
        content_length = cdn_resp.headers.get("Content-Length")
        filename = os.path.basename(key) or "download"

        def _iter_cdn(chunk_size: int = 65536):
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
                    pass
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
                    pass

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
        raise HTTPException(
            status_code=502,
            detail={"error": f"FOS fetch failed: {exc}"},
        )

    body = obj["Body"]
    content_type = obj.get("ContentType") or "application/octet-stream"
    content_length = obj.get("ContentLength")
    filename = os.path.basename(key) or "download"

    def _iter_fos(chunk_size: int = 65536):
        try:
            yield from body.iter_chunks(chunk_size)
        finally:
            try:
                body.close()
            except Exception:
                pass

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "private, no-store",
    }
    if content_length:
        headers["Content-Length"] = str(content_length)

    return StreamingResponse(_iter_fos(), media_type=content_type, headers=headers)


@router.get("/download-all")
def download_all_files(
    source: dict = Depends(get_source),
    include: str = Query(default="all"),
):

    from backend.core import duckdb as _db

    src = source
    service_id = src.get("name", "")
    if not service_id:
        raise HTTPException(status_code=400, detail={"error": "service_id required"})

    def zip_worker(q: queue.Queue):
        # process_context_scope (the context manager) so the fsspec iothread
        # fallback isn't wiped out by a concurrent scope exit on another
        # thread — see _initialize_service for context.
        from backend.utils.telemetry import (
            process_context_scope as _pcs,
        )
        from backend.utils.telemetry import (
            start_call_tracking as _sct,
        )
        from backend.utils.usage_logger import flush_usage_log as _flush

        _sct()
        with _pcs(f"api:GET /download-all:{include}"):
            try:
                with zipfile.ZipFile(cast(Any, _QueueFile(q)), "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    if include == "local":
                        db_path = src.get("duckdb_path")
                        if not db_path:
                            from backend import config as svcconfig

                            db_path = svcconfig.duckdb_path(service_id)
                        if db_path and os.path.exists(db_path):
                            zf.write(db_path, os.path.basename(db_path))

                        cache_dir = _db._cache_dir(src)
                        walk_dir = (
                            os.path.join(cache_dir, src.get("prefix", "").lstrip("/"))
                            if src.get("prefix")
                            else cache_dir
                        )
                        if os.path.exists(walk_dir):
                            for root, _, files in os.walk(walk_dir):
                                for file in files:
                                    file_path = os.path.join(root, file)
                                    arcname = os.path.relpath(file_path, cache_dir)
                                    zf.write(file_path, arcname)
                    else:
                        cdn = src.get("cdn_url", "").rstrip("/")
                        fos_client = _db._get_fos_client(src)
                        paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_all")
                        # Cross-tenant guard: scope to this service's prefix
                        # so a shared bucket with multiple services doesn't
                        # leak sibling data into the zip.
                        pages = paginator.paginate(Bucket=src["bucket"], Prefix=src.get("prefix", ""))

                        for page in pages:
                            if "Contents" not in page:
                                continue
                            for obj in page["Contents"]:
                                key = obj["Key"]
                                _fetch_file_to_zip(src, fos_client, cdn, key, key, zf, "download_all")
            except Exception:
                logger.error("Error in ZIP generation", exc_info=True)
            finally:
                try:
                    _flush(service_id)
                except Exception:
                    pass
                q.put(None)

    headers = {
        "Content-Disposition": f'attachment; filename="fastly_logs_{service_id}.zip"',
    }

    return StreamingResponse(_stream_from_worker(zip_worker), media_type="application/zip", headers=headers)
