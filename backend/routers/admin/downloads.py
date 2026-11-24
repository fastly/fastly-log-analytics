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
from backend.core.request_context import get_source, require_admin
from backend.models.admin import TreeResponse
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.router_utils import make_error, query_errors, raise_internal, require_service_id

from ._helpers import (
    _fetch_file_to_zip,
    _open_safe_cdn,
    _resolve_source,
    _sanitize_download_filename,
    _validate_storage_rel_path,
    _validated_admin_cdn_url,
    stream_zip,
)
from ._router import router

logger = logging.getLogger(__name__)

_RAW_TREE_CACHE: BoundedTTLCache[tuple[str, str], dict[str, Any]] = BoundedTTLCache(maxsize=256, ttl_seconds=30.0)
_STORAGE_TREE_CACHE: BoundedTTLCache[tuple[str, str], dict[str, Any]] = BoundedTTLCache(maxsize=256, ttl_seconds=30.0)
CacheRegistry.register("downloads._RAW_TREE_CACHE", _RAW_TREE_CACHE)
CacheRegistry.register("downloads._STORAGE_TREE_CACHE", _STORAGE_TREE_CACHE)


@router.get("/admin/raw-tree", response_model=TreeResponse, dependencies=[Depends(require_admin)])
def raw_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import fos_client as _db_mod

    try:
        prefix = _validate_storage_rel_path(prefix, allow_empty=True)
    except ValueError:
        raise HTTPException(status_code=400, detail=make_error("invalid_prefix", "Invalid storage prefix"))

    _db: Any = _db_mod
    cache_key = (str(source.get("name", "")), prefix)
    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    result = _RAW_TREE_CACHE.get(cache_key) if use_cache else None
    if result is None:
        result = _db.get_raw_tree_node(source, prefix, root="raw")
        if use_cache and isinstance(result, dict):
            _RAW_TREE_CACHE.set(cache_key, result)
    resp: TreeResponse = TreeResponse.with_telemetry(nodes=result.get("children", []))
    return resp


@router.get("/admin/storage-tree", response_model=TreeResponse, dependencies=[Depends(require_admin)])
def storage_tree_endpoint(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
) -> TreeResponse:
    from backend.core import fos_client as _db_mod

    try:
        prefix = _validate_storage_rel_path(prefix, allow_empty=True)
    except ValueError:
        raise HTTPException(status_code=400, detail=make_error("invalid_prefix", "Invalid storage prefix"))

    _db: Any = _db_mod
    cache_key = (str(source.get("name", "")), prefix)
    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    result = _STORAGE_TREE_CACHE.get(cache_key) if use_cache else None
    if result is None:
        result = _db.get_storage_tree_node(source, prefix)
        if use_cache and isinstance(result, dict):
            _STORAGE_TREE_CACHE.set(cache_key, result)
    resp: TreeResponse = TreeResponse.with_telemetry(nodes=result.get("children", []))
    return resp


@router.get("/download-folder", dependencies=[Depends(require_admin)])
def download_folder(
    source: dict[str, Any] = Depends(get_source),
    prefix: str = Query(default=""),
    root: str = Query(default="raw"),
) -> StreamingResponse:
    from backend.core import fos_client as _db

    if root not in ("raw", "processed"):
        raise HTTPException(status_code=400, detail=make_error("invalid_root", "root must be 'raw' or 'processed'"))
    try:
        prefix = _validate_storage_rel_path(prefix, allow_empty=True)
    except ValueError:
        raise HTTPException(status_code=400, detail=make_error("invalid_prefix", "Invalid folder prefix"))
    base_prefix = source.get("prefix", "").strip().rstrip("/")
    if base_prefix:
        target_prefix = f"{base_prefix}/{root}/{prefix}" if prefix else f"{base_prefix}/{root}/"
    else:
        target_prefix = f"{root}/{prefix}" if prefix else f"{root}/"

    if not target_prefix.endswith("/"):
        target_prefix += "/"

    raw_cdn = (source.get("cdn_url") or "").strip().rstrip("/")
    if raw_cdn and _validated_admin_cdn_url(raw_cdn) is None:
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_cdn_url", "Configured CDN URL is not on the Fastly HTTPS allowlist"),
        )
    safe_cdn = _validated_admin_cdn_url(raw_cdn) or ""

    def populate(zf: Any) -> None:
        cdn = safe_cdn
        fos_client = _db._get_fos_client(source)
        try:
            paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_zip")
        except TypeError:
            paginator = fos_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=source["bucket"], Prefix=target_prefix)

        for page in pages:
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                key = obj.get("Key", "")
                if not key or key.endswith("/") or not key.startswith(target_prefix):
                    continue
                try:
                    clean_key = _validate_storage_rel_path(key, allow_empty=False)
                except ValueError:
                    logger.warning("Skipping unsafe S3 key in download_folder: %r", key)
                    continue

                top_folder = os.path.basename(prefix) if prefix else root
                rel_path = clean_key[len(target_prefix) :]
                raw_arc = f"{top_folder}/{rel_path}" if rel_path else os.path.basename(clean_key)
                try:
                    arcname = _validate_storage_rel_path(raw_arc, allow_empty=False)
                except ValueError:
                    logger.warning("Skipping unsafe ZIP arcname in download_folder: %r", raw_arc)
                    continue

                from backend.routers import admin as _admin_pkg

                _fn = _fetch_file_to_zip
                _orig = getattr(sys.modules.get("backend.routers.admin.health"), "_fetch_file_to_zip", None)
                if _fn is _orig:
                    _fn = getattr(_admin_pkg, "_fetch_file_to_zip", _fn)
                _fn(source, fos_client, cdn, clean_key, arcname, zf, "download_zip")

    safe_name = _sanitize_download_filename(prefix.replace("/", "_") or root, fallback=root)
    headers = {
        "Content-Disposition": f'attachment; filename="{safe_name}.zip"',
    }

    return StreamingResponse(
        stream_zip(f"api:GET /admin/download-zip:{root}", source.get("name", ""), populate),
        media_type="application/zip",
        headers=headers,
    )


@router.get("/download", dependencies=[Depends(require_admin)])
@query_errors(status_code=500)
def download_file(
    source: dict[str, Any] = Depends(get_source),
    key: str = Query(default=""),
) -> Any:
    import urllib.parse

    from fastapi.responses import FileResponse

    from backend.core.fos_client import _cache_dir, _get_fos_client

    if not key:
        raise HTTPException(status_code=400, detail={"error": "Missing key parameter"})
    if key.startswith("/"):
        raise HTTPException(status_code=400, detail=make_error("invalid_key", "Invalid key path"))
    try:
        key = _validate_storage_rel_path(key, allow_empty=False)
    except ValueError:
        raise HTTPException(status_code=400, detail=make_error("invalid_key", "Invalid key path"))

    src_prefix = (source.get("prefix") or "").strip("/")
    if src_prefix:
        req_prefix = f"{src_prefix}/"
        if not key.startswith(req_prefix):
            raise HTTPException(status_code=400, detail=make_error("invalid_key", "Key outside configured prefix"))
    elif not key.startswith(("raw/", "processed/")):
        raise HTTPException(
            status_code=400, detail=make_error("invalid_key", "Key must reside under raw/ or processed/")
        )

    cache_dir = os.path.realpath(_cache_dir(source))
    raw_local_path = os.path.join(cache_dir, key)
    if os.path.islink(raw_local_path):
        raise HTTPException(status_code=400, detail=make_error("invalid_key", "Symlinks are not allowed"))
    candidate = os.path.realpath(raw_local_path)
    if not path_within_dir(cache_dir, candidate):
        raise HTTPException(status_code=400, detail=make_error("invalid_key", "Invalid key path"))
    local_path = candidate
    if os.path.isfile(local_path):
        return FileResponse(
            local_path,
            filename=_sanitize_download_filename(os.path.basename(local_path), fallback="download"),
        )

    from backend.utils.telemetry import record_call as _record_call

    cdn = (source.get("cdn_url") or "").strip().rstrip("/")
    if cdn:
        safe_cdn = _validated_admin_cdn_url(cdn)
        if safe_cdn is None:
            raise HTTPException(
                status_code=400,
                detail=make_error("invalid_cdn_url", "Configured CDN URL is not on the Fastly HTTPS allowlist"),
            )
        import time as _time
        import urllib.request

        from backend.utils.telemetry import record_cdn_call as _rcdn

        url = f"{safe_cdn}/{urllib.parse.quote(key)}"
        req = urllib.request.Request(url)
        if source.get("cdn_secret"):
            req.add_header("x-fastly-key", source["cdn_secret"])
        try:
            cdn_resp = _open_safe_cdn(req, timeout=30)
        except Exception as exc:
            raise_internal(logger, exc, code="cdn_fetch_failed", status=502)

        try:
            content_type = cdn_resp.headers.get("Content-Type") or "application/octet-stream"
            content_length = cdn_resp.headers.get("Content-Length")
            filename = _sanitize_download_filename(os.path.basename(key), fallback="download")
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
    filename = _sanitize_download_filename(os.path.basename(key), fallback="download")

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


@router.get("/download-all", dependencies=[Depends(require_admin)])
def download_all_files(
    source: dict[str, Any] = Depends(get_source),
    include: str = Query(default="all"),
) -> StreamingResponse:
    from backend.core import fos_client as _db

    src = source
    service_id = require_service_id(src.get("name"))
    raw_prefix = (src.get("prefix") or "").strip()
    try:
        clean_prefix = _validate_storage_rel_path(raw_prefix, allow_empty=True)
    except ValueError:
        raise HTTPException(status_code=400, detail=make_error("invalid_prefix", "Invalid storage prefix")) from None

    raw_cdn = (src.get("cdn_url") or "").strip().rstrip("/")
    if include != "local" and raw_cdn and _validated_admin_cdn_url(raw_cdn) is None:
        raise HTTPException(
            status_code=400,
            detail=make_error("invalid_cdn_url", "Configured CDN URL is not on the Fastly HTTPS allowlist"),
        )

    def populate(zf: Any) -> None:
        if include == "local":
            cache_dir = os.path.realpath(_db._cache_dir(src))
            walk_roots = (
                [os.path.join(cache_dir, clean_prefix)]
                if clean_prefix
                else [os.path.join(cache_dir, "raw"), os.path.join(cache_dir, "processed")]
            )
            for walk_root in walk_roots:
                if os.path.islink(walk_root):
                    continue
                real_walk = os.path.realpath(walk_root)
                if not path_within_dir(cache_dir, real_walk) or not os.path.isdir(real_walk):
                    continue
                for root_dir, dirnames, files in os.walk(real_walk, followlinks=False):
                    dirnames[:] = [
                        d
                        for d in dirnames
                        if not os.path.islink(os.path.join(root_dir, d))
                        and path_within_dir(cache_dir, os.path.realpath(os.path.join(root_dir, d)))
                    ]
                    for file in files:
                        file_path = os.path.join(root_dir, file)
                        if os.path.islink(file_path):
                            continue
                        real_file = os.path.realpath(file_path)
                        if not path_within_dir(cache_dir, real_file) or not os.path.isfile(real_file):
                            continue
                        rel_arc = os.path.relpath(real_file, cache_dir).replace(os.sep, "/")
                        try:
                            arcname = _validate_storage_rel_path(rel_arc, allow_empty=False)
                        except ValueError:
                            continue
                        if not clean_prefix and not arcname.startswith(("raw/", "processed/")):
                            continue
                        zf.write(real_file, arcname)
        else:
            if not src.get("bucket"):
                raise ValueError("Storage bucket is not configured for cloud download")
            cdn = _validated_admin_cdn_url(raw_cdn) or ""
            fos_client = _db._get_fos_client(src)
            try:
                paginator = fos_client.get_paginator("list_objects_v2", caller_hint="download_all")
            except TypeError:
                paginator = fos_client.get_paginator("list_objects_v2")
            list_prefixes = (f"{clean_prefix}/",) if clean_prefix else ("raw/", "processed/")
            for list_pfx in list_prefixes:
                for page in paginator.paginate(Bucket=src["bucket"], Prefix=list_pfx):
                    for obj in page.get("Contents", []):
                        raw_key = obj.get("Key", "")
                        if not raw_key or raw_key.endswith("/") or raw_key.startswith("/"):
                            continue
                        try:
                            clean_key = _validate_storage_rel_path(raw_key, allow_empty=False)
                        except ValueError:
                            logger.warning("Skipping unsafe S3 key in download_all: %r", raw_key)
                            continue
                        if not clean_key.startswith(list_pfx):
                            continue
                        from backend.routers import admin as _admin_pkg

                        _fn = _fetch_file_to_zip
                        _orig = getattr(sys.modules.get("backend.routers.admin.health"), "_fetch_file_to_zip", None)
                        if _fn is _orig:
                            _fn = getattr(_admin_pkg, "_fetch_file_to_zip", _fn)
                        _fn(src, fos_client, cdn, clean_key, clean_key, zf, "download_all")

    safe_sid = _sanitize_download_filename(service_id, fallback="logs")
    headers = {
        "Content-Disposition": f'attachment; filename="fastly_logs_{safe_sid}.zip"',
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
