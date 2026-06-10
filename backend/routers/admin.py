"""Admin router — ingest, sync status, raw file tree, download."""

from __future__ import annotations

import os
import queue
import zipfile

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.deps import get_service_id, get_source
from backend.models.admin import (
    BotSourcesResponse,
    IcebergTableInfoResponse,
    IngestedFilesResponse,
    LogAccountingBucket,
    LogAccountingResponse,
    LogAccountingTotals,
    PopLocationsResponse,
    SustainedLossAlert,
    SyncStatusResponse,
    SystemJobsResponse,
    TreeResponse,
    UsageLogAggregate,
    UsageLogEntry,
    UsageLogResponse,
)
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["admin"])


class _QueueFile:
    """File-like wrapper around a queue.Queue for streaming ZIP generation."""

    def __init__(self, q: queue.Queue):
        self.q = q
        self.offset = 0

    def write(self, b: bytes) -> int:
        self.q.put(b)
        n = len(b)
        self.offset += n
        return n

    def flush(self):
        pass

    def tell(self):
        return self.offset


class ClientDisconnected(Exception):
    """Raised when the client disconnects during a streaming response."""

    pass


class _AbortableQueue(queue.Queue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self.aborted = False

    def put(self, item, block=True, timeout=None):
        if self.aborted:
            if item is None:
                return
            raise ClientDisconnected("Client disconnected during streaming")
        super().put(item, block, timeout)


def _stream_from_worker(worker):
    """Run *worker(q)* in a daemon thread and yield the bytes it puts into the queue."""
    import contextvars
    import threading

    q: _AbortableQueue = _AbortableQueue(maxsize=10)
    # Copy the request's context (process_context, _CALLS list) so any
    # record_call() inside the worker thread lands in the same _usage_log batch.
    ctx = contextvars.copy_context()
    thread = threading.Thread(target=lambda: ctx.run(worker, q), daemon=True)
    thread.start()
    try:
        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk
    finally:
        q.aborted = True
        while True:
            try:
                q.get_nowait()
            except queue.Empty:
                break


def _fetch_file_to_zip(
    source: dict,
    fos_client,
    cdn: str,
    key: str,
    arcname: str,
    zf: zipfile.ZipFile,
    caller: str,
) -> bool:
    """Fetch a single S3 key into the zip via CDN with fallback to direct FOS.

    Returns True on success. Failures are printed and return False so the
    caller can decide whether to abort or continue with the next file.
    """
    import time as _t
    import urllib.parse
    import urllib.request

    from backend.utils.telemetry import record_cdn_call as _rcdn

    if cdn:
        url = f"{cdn}/{urllib.parse.quote(key)}"
        try:
            req = urllib.request.Request(url)
            if source.get("cdn_secret"):
                req.add_header("x-fastly-key", source["cdn_secret"])
            t0 = _t.time()
            bytes_read = 0
            cdn_headers = None
            with urllib.request.urlopen(req, timeout=30) as response:
                cdn_headers = response.headers
                with zf.open(arcname, "w", force_zip64=True) as dest:
                    while True:
                        chunk = response.read(65536)
                        if not chunk:
                            break
                        bytes_read += len(chunk)
                        dest.write(chunk)
            _rcdn(
                "GET",
                key,
                round((_t.time() - t0) * 1000, 2),
                headers=cdn_headers,
                bytes_count=bytes_read,
                caller=caller,
            )
            return True
        except Exception as cdn_err:
            print(f"CDN fetch failed for {key}, falling back to FOS: {cdn_err}")

    try:
        # fos_client MUST be from _get_fos_client() so the telemetry proxy
        # captures this read. Don't swap in a raw boto3.client(...) — that
        # silently drops the usage_log row.
        resp = fos_client.get_object(Bucket=source["bucket"], Key=key)
        with zf.open(arcname, "w", force_zip64=True) as dest:
            body = resp["Body"]
            while True:
                chunk = body.read(65536)
                if not chunk:
                    break
                dest.write(chunk)
        return True
    except Exception as fos_err:
        print(f"Error fetching {key} from FOS: {fos_err}")
        return False


@router.get("/admin/pop-locations", response_model=PopLocationsResponse)
def get_pop_locations():
    """Return the cached POP locations (code, name, coordinates)."""
    from backend.utils.pop_utils import get_pop_locations

    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


class RefreshPopLocationsRequest(BaseModel):
    token: str = Field(..., description="Fastly API key")


@router.post("/admin/pop-locations/refresh", response_model=PopLocationsResponse)
def refresh_pop_locations(req: RefreshPopLocationsRequest | None = None, token: str | None = Query(default=None)):
    """Refresh the POP locations cache from the Fastly API."""
    api_key = ""
    if req is not None:
        api_key = req.token.strip()

    if not api_key:
        if token is None:
            raise HTTPException(status_code=422, detail="token is required")
        api_key = token.strip()
        if not api_key:
            raise HTTPException(status_code=400, detail={"error": "api_key is required"})

    from backend.utils.pop_utils import fetch_pop_locations, get_pop_locations

    ok = fetch_pop_locations(api_key)
    if not ok:
        raise HTTPException(
            status_code=502, detail={"error": "Failed to fetch POP data from Fastly API. Check your API key."}
        )
    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


def _resolve_source(source_name: str) -> dict:
    from backend import config as svcconfig
    from backend.core.duckdb import _DEFAULT_SOURCE

    if source_name == "default":
        return _DEFAULT_SOURCE
    cfg = svcconfig.load_config(source_name)
    if cfg:
        from backend import config as _sc

        return {**_DEFAULT_SOURCE, **_sc.config_to_source(cfg)}
    return _DEFAULT_SOURCE


@router.post("/admin/ingest-logs")
def ingest_endpoint(
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    source: dict = Depends(get_source),
):
    import threading

    from fastapi import HTTPException

    from backend.core.duckdb import start_cron_run
    from backend.cron_progress import list_active_runs, start_progress
    from backend.repositories.dashboard import _dashboard_cache
    from backend.scheduler import _run_metadata_sync, _run_service_cron

    src = source
    _dashboard_cache.pop(src["name"], None)
    is_readonly = source.get("access_level") == "read_only"

    if is_readonly:
        try:
            run_id = start_cron_run(source, "metadata_sync")
            start_progress(run_id, service_id=source["name"], task="metadata_sync")
            t = threading.Thread(
                target=_run_metadata_sync,
                args=(source["name"],),
                kwargs={"run_id": run_id, "start_time": start_time, "end_time": end_time},
                daemon=True,
            )
            t.start()
        except RuntimeError as e:
            run_id = None
            for entry in list_active_runs():
                if entry.get("service_id") == source["name"] and entry.get("task") == "metadata_sync":
                    run_id = entry["run_id"]
                    break
            if run_id is None:
                raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
            return {"ok": True, "message": "Metadata sync already running.", "run_id": run_id}

        return {"ok": True, "message": "Metadata sync started.", "run_id": run_id}

    else:
        try:
            run_id = start_cron_run(src, "sync")
            start_progress(run_id, service_id=src["name"], task="sync")
            t = threading.Thread(
                target=_run_service_cron,
                args=(src["name"],),
                kwargs={
                    "force": True,
                    "run_id": run_id,
                    "start_time": start_time,
                    "end_time": end_time,
                },
                daemon=True,
            )
            t.start()
        except RuntimeError as e:
            run_id = None
            for entry in list_active_runs():
                if entry.get("service_id") == src["name"] and entry.get("task") == "sync":
                    run_id = entry["run_id"]
                    break
            if run_id is None:
                raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
            return {"ok": True, "message": "Ingestion already running.", "run_id": run_id}

        return {"ok": True, "message": "Ingestion started.", "run_id": run_id}


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
                with zipfile.ZipFile(_QueueFile(q), "w", compression=zipfile.ZIP_DEFLATED) as zf:
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
            except Exception as e:
                print(f"Error in ZIP generation: {e}")
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


@router.get("/admin/raw-tree", response_model=TreeResponse)
def raw_tree_endpoint(
    source: dict = Depends(get_source),
    prefix: str = Query(default=""),
):
    from backend.core.duckdb import get_raw_tree_node

    result = get_raw_tree_node(source, prefix, root="raw")
    return TreeResponse.with_telemetry(nodes=result.get("children", []))


@router.get("/admin/iceberg-tree", response_model=TreeResponse)
def iceberg_tree_endpoint(
    source: dict = Depends(get_source),
    prefix: str = Query(default=""),
):
    from backend.core.duckdb import get_raw_tree_node

    result = get_raw_tree_node(source, prefix, root="iceberg")
    return TreeResponse.with_telemetry(nodes=result.get("children", []))


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
                with zipfile.ZipFile(_QueueFile(q), "w", compression=zipfile.ZIP_DEFLATED) as zf:
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
            except Exception as e:
                print(f"Error in ZIP generation: {e}")
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


_DIR_SIZE_CACHE: dict[str, tuple[float, int]] = {}
_DIR_SIZE_TTL_S = 30.0


def _get_dir_size(path: str) -> int:
    # Cache results per-path with a 30s TTL. The cache walk is O(files-in-tree)
    # and the per-service cache grew from ~300 files to ~19k after the rollups
    # backfill (one parquet per field × hour). At ~700ms per uncached walk,
    # SyncStatusBadge's 15s poll was paying that cost on every refresh; the
    # cache turns it into a single getsize_sum sweep per minute.
    #
    # Files only grow incrementally (ingest + rollup-recompute) so a 30s
    # staleness window means the dashboard's reported disk usage can lag by
    # at most that window. Worth it for the perf vs measuring exact-to-the-
    # millisecond size on a poll endpoint.
    import time as _t

    now = _t.monotonic()
    cached = _DIR_SIZE_CACHE.get(path)
    if cached is not None and (now - cached[0]) < _DIR_SIZE_TTL_S:
        return cached[1]
    total = _scan_dir_size(path)
    _DIR_SIZE_CACHE[path] = (now, total)
    return total


def _scan_dir_size(path: str) -> int:
    total = 0
    if not os.path.exists(path):
        return 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += _scan_dir_size(entry.path)
    except Exception:
        pass
    return total


# Moved out of /admin/ so analysts can also see sync status / time bounds
# for their scoped service. The endpoint returns per-service timestamps and
# row counts — no admin-specific info. Service-scope is still enforced by
# RemoteAccessMiddleware via the x-service-id check on the request.
@router.get("/sync-status", response_model=SyncStatusResponse)
def sync_status(
    service_id: str | None = Depends(get_service_id),
    skip_fos: bool = Query(default=False),
    force: bool = Query(default=False),
):
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core.duckdb import get_sync_status
    from backend.utils.telemetry import clear_queries

    clear_queries()

    src: dict | None = None
    if service_id:
        src = _db.get_source_for_service(service_id)
    if not src:
        return SyncStatusResponse.with_telemetry(configured=False)

    try:
        # Fast path: skip_fos=true callers (FilterBar polling, badge in
        # the page header, etc.) only need the cached snapshot that the
        # sync cron refreshes every minute. Return it without grabbing a
        # DuckDB connection, so that a busy dashboard load — agg/raw/
        # bots all racing for connections — doesn't starve sync-status
        # and trigger 503s when its max_wait expires.
        cached_status = svcconfig.get_status(src["name"]) if skip_fos and not force else None
        # get_status returns {} (not None) when no status has been
        # persisted yet — fall through to the DB path in that case.
        if cached_status:
            cached_status["access_level"] = src.get("access_level", "read_write")
            cached_status["storage_mode"] = _db.STORAGE_MODE
            cached_status["configured"] = True
            status = cached_status
        else:
            from backend.core.duckdb import get_connection

            _con = get_connection(source=src, max_wait=5, skip_view_update=True)
            try:
                status = get_sync_status(_con, src, skip_fos=skip_fos, force=force)
            finally:
                _con.close()

        db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
        db_exists = os.path.exists(db_path)
        db_size = os.path.getsize(db_path) if db_exists else 0

        cache_size = _get_dir_size(_db._cache_dir(src))

        status["duckdb_size_bytes"] = db_size + cache_size
        status["duckdb_exists"] = db_exists

        from backend.cron_progress import get_latest_progress_for_service

        active_run = get_latest_progress_for_service(service_id)
        if active_run:
            status["active_run"] = active_run
            status["busy"] = True

        cfg = svcconfig.load_config(service_id) or {}
        status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")

        return SyncStatusResponse.with_telemetry(**status)
    except _db.DBBusyError as e:
        raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": str(e)})


@router.get("/admin/ingested-files", response_model=IngestedFilesResponse)
@query_errors(status_code=500)
def ingested_files(source: dict = Depends(get_source)):
    from backend.core.duckdb import get_ingested_files

    res = get_ingested_files(None, source)
    return IngestedFilesResponse.with_telemetry(files=res)


@router.post("/admin/optimize-now")
def optimize_now(
    source: dict = Depends(get_source),
    min_files: int | None = Query(
        default=None, description="Override auto-derived threshold. Pass 1 for max-aggressive cleanup."
    ),
):
    """Trigger an immediate Iceberg table optimize (compaction) pass.
    Bypasses the nightly cron schedule for ad-hoc cleanup. Returns the
    optimize_table result dict (files_rewritten / files_added / etc).
    Writes through to FOS — use ``/admin/local-compact-now`` for the
    free local-only equivalent.
    """
    from backend.core import iceberg as _ice

    return _ice.optimize_table(source, min_files_per_partition=min_files)


@router.post("/admin/local-compact-now")
def local_compact_now(
    source: dict = Depends(get_source),
    min_files: int = Query(default=3, ge=1, description="Compact partitions with strictly more files than this."),
    dry_run: bool = Query(default=False, description="Report what would happen without writing."),
):
    """Trigger an immediate local-only parquet compaction pass.

    Does NOT touch FOS — only rewrites files inside the local cache, so
    no 30-day-minimum billing penalty. Safe to call as often as needed.
    The 2-minute cron does this automatically; this endpoint is for
    ad-hoc cleanup.
    """
    from backend.core import local_compaction as _lc

    return _lc.compact_local_partitions(source, min_files_per_partition=min_files, dry_run=dry_run)


@router.get("/admin/compaction-stats")
def compaction_stats(source: dict = Depends(get_source)):
    """Snapshot of file-count distribution across local cache partitions.

    Useful for monitoring: rising partitions_above_3 means the local
    compaction cron has stopped keeping up; rising avg_files_per_partition
    correlates with slow dashboard scans.
    """
    from backend.core import local_compaction as _lc

    return _lc.compaction_stats(source)


@router.patch("/admin/metadata-retention")
def update_metadata_retention(body: dict, source: dict = Depends(get_source)):
    """Update the per-service ``metadata_retention`` config block.

    Body shape: any subset of ``{usage_log_days, ingested_files_days,
    cron_runs_days}``. Each value is coerced to int; negative / non-numeric
    inputs are clamped to 0 (which disables cleanup for that table per
    cleanup_metadata's semantics). Missing keys preserve their current
    value. Returns the resolved retention (defaults merged with cfg) so the
    UI can confirm what was saved.
    """
    from backend import config as svcconfig
    from backend.core import metadata_db as _mdb
    from backend.core.metadata_db import DEFAULT_METADATA_RETENTION

    service_id = source["name"]
    cfg = svcconfig.load_config(service_id)
    if cfg is None:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})

    from backend.core.metadata_db import is_ingested_files_dedup_active

    current = dict(cfg.get("metadata_retention") or {})
    for key in ("usage_log_days", "ingested_files_days", "cron_runs_days"):
        if key in body:
            try:
                v = int(body[key])
            except (TypeError, ValueError):
                v = 0
            current[key] = max(0, v)

    # Mirror the cleanup helper's safety override at the write layer:
    # if delete_after=false on this service, refuse to persist a non-zero
    # ingested_files_days. Storing it would mislead the operator into
    # thinking the value will be honored when the cleanup ignores it.
    if not is_ingested_files_dedup_active(service_id) and int(current.get("ingested_files_days") or 0) > 0:
        current["ingested_files_days"] = 0

    cfg["metadata_retention"] = current
    svcconfig.save_config(service_id, cfg)
    try:
        _mdb.record_audit(
            service_id=service_id,
            event_type="metadata_retention_update",
            details=current,
        )
    except Exception:
        pass

    return {"retention": {**DEFAULT_METADATA_RETENTION, **current}}


@router.get("/admin/metadata-storage")
def metadata_storage(source: dict = Depends(get_source)):
    """Per-table row count + estimated bytes for this service's metadata.db.

    Includes the resolved retention policy (per-service cfg merged with
    defaults). The UI uses this to render the Metadata Storage card on
    the admin page — table sizes, bytes, and a Cleanup-now button.
    """
    from backend import config as svcconfig
    from backend.core.metadata_db import (
        DEFAULT_METADATA_RETENTION,
        get_metadata_storage_stats,
        is_ingested_files_dedup_active,
    )

    service_id = source["name"]
    stats = get_metadata_storage_stats(service_id)
    cfg = svcconfig.load_config(service_id) or {}
    retention = {**DEFAULT_METADATA_RETENTION, **(cfg.get("metadata_retention") or {})}
    # ingested_files_locked surfaces the safety override: when
    # cron_sync.delete_after=False the ingested_files table is the
    # dedup gate, so the cleanup helper force-disables its trimming
    # regardless of the configured retention. UI uses this to disable
    # the input + show a tooltip explaining the override.
    ingested_files_locked = not is_ingested_files_dedup_active(service_id)
    return {**stats, "retention": retention, "ingested_files_locked": ingested_files_locked}


@router.post("/admin/metadata-cleanup")
def metadata_cleanup_now(source: dict = Depends(get_source)):
    """Trigger an immediate metadata cleanup, streaming progress as SSE.

    Equivalent to the daily ``metadata_cleanup`` cron at 03:15 UTC but
    on-demand. The DELETE phase is fast; VACUUM rewrites the whole file
    and on a multi-GB metadata.db can take minutes. Streaming gives the
    operator real-time feedback instead of a 5-minute hang behind a
    spinning button.

    Event shapes (between SSE ``data:`` lines):

        {"type": "status",   "message": str}
        {"type": "progress", "current": int, "total": int, "message": str}
        {"type": "done",     "message": str, "result": {...}}
        {"type": "error",    "message": str}

    Writes a row to ``cron_runs`` with task=``metadata_cleanup`` so the
    manual run shows up on the Data Management schedule + history grid
    alongside the scheduled cron's runs.
    """
    import json as _json
    import queue as _queue
    import threading
    import time as _t

    from backend import config as svcconfig
    from backend.core.duckdb import log_cron_run, start_cron_run
    from backend.core.metadata_db import cleanup_metadata

    service_id = source["name"]
    cfg = svcconfig.load_config(service_id) or {}
    retention = cfg.get("metadata_retention") or {}

    # Bridge cleanup_metadata's on_event callback to the SSE generator via
    # a thread-safe queue. The worker thread runs the cleanup synchronously
    # (DELETE then VACUUM — both block the SQLite writer) and pushes events
    # as they happen; the streaming generator consumes them and yields SSE
    # frames. Sentinel ``None`` marks end-of-stream.
    events: _queue.Queue = _queue.Queue()

    def worker():
        started = _t.time()
        run_id = start_cron_run(source, "metadata_cleanup")
        try:
            result = cleanup_metadata(service_id, retention, on_event=events.put)
        except Exception as e:
            err = str(e)
            events.put({"type": "error", "message": f"Cleanup failed: {err}"})
            try:
                log_cron_run(
                    source,
                    "metadata_cleanup",
                    _t.time() - started,
                    "error",
                    error_message=err,
                    summary=f"cleanup failed: {err}",
                    run_id=run_id,
                )
            finally:
                events.put(None)
            return

        total_deleted = sum(result["deleted"].values())
        if total_deleted:
            parts = [f"{t}={n}" for t, n in result["deleted"].items() if n]
            summary = (
                f"Trimmed {total_deleted:,} rows ({', '.join(parts)}). "
                f"VACUUM={'yes' if result['vacuumed'] else 'skipped'}."
            )
        else:
            summary = "No rows older than retention windows."
        try:
            log_cron_run(
                source,
                "metadata_cleanup",
                _t.time() - started,
                "success",
                summary=summary,
                rows_ingested=total_deleted,
                run_id=run_id,
            )
        finally:
            events.put({"type": "done", "message": summary, "result": result})
            events.put(None)

    threading.Thread(target=worker, daemon=True, name=f"metadata-cleanup-{service_id}").start()

    def stream():
        # Pre-pad to defeat any reverse-proxy / browser buffering; SSE
        # clients flush on the first blank-line delimiter.
        yield ":" + " " * 2048 + "\n\n"
        while True:
            event = events.get()
            if event is None:
                break
            yield f"data: {_json.dumps(event)}\n\n"

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.get("/admin/health-snapshot")
def health_snapshot():
    """One-shot health snapshot for the admin page system health card.

    Returns CPU load averages, memory, disk usage of the data mount,
    docker container CPU/memory (if reachable), and the count of
    in-flight cron runs. Uses only stdlib (no psutil dep).
    """
    import shutil

    out: dict = {}

    # ── Load + uptime ─────────────────────────────────────────────────
    try:
        load1, load5, load15 = os.getloadavg()
        out["load"] = {"avg_1m": round(load1, 2), "avg_5m": round(load5, 2), "avg_15m": round(load15, 2)}
    except Exception:
        out["load"] = None

    # vCPU count to interpret load (load > vCPU = backlog).
    try:
        import multiprocessing as _mp

        out["vcpus"] = _mp.cpu_count()
    except Exception:
        out["vcpus"] = None

    # ── Memory (Linux /proc/meminfo) ─────────────────────────────────
    try:
        meminfo: dict[str, int] = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, rest = line.partition(":")
                v = rest.strip().split()
                if v and v[0].isdigit():
                    meminfo[k.strip()] = int(v[0]) * 1024  # kB → bytes
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", 0)
        out["memory"] = {
            "total_mb": round(total / 1024 / 1024),
            "available_mb": round(avail / 1024 / 1024),
            "used_pct": round((1 - avail / total) * 100, 1) if total else None,
        }
    except Exception:
        out["memory"] = None

    # ── Data-mount disk usage ────────────────────────────────────────
    for path, label in (("/app/data", "data_mount"), ("/", "root_disk")):
        try:
            d = shutil.disk_usage(path)
            out[label] = {
                "total_gb": round(d.total / 1024 / 1024 / 1024, 1),
                "used_gb": round(d.used / 1024 / 1024 / 1024, 1),
                "free_gb": round(d.free / 1024 / 1024 / 1024, 1),
                "used_pct": round(d.used / d.total * 100, 1) if d.total else None,
            }
        except Exception:
            out[label] = None

    # ── In-flight cron runs ──────────────────────────────────────────
    # Use list_active_runs() (which filters out runs whose last event is
    # done/error) instead of iterating _run_metadata directly. The dict
    # holds entries for an hour after completion (the cleanup TTL), so the
    # raw iteration was showing dozens of stale "sync" entries in the
    # System Health card.
    try:
        from backend.cron_progress import list_active_runs

        in_flight = []
        for entry in list_active_runs():
            in_flight.append(
                {
                    "run_id": entry["run_id"],
                    "service_id": entry.get("service_id"),
                    "task": entry.get("task"),
                    "started_at": entry.get("started_at"),
                }
            )
        out["in_flight_runs"] = in_flight
    except Exception:
        out["in_flight_runs"] = []

    # ── Per-service compaction stats ─────────────────────────────────
    try:
        from backend import config as _svcconfig
        from backend.core import local_compaction as _lc

        stats_by_svc: dict = {}
        for cfg in _svcconfig.list_configs():
            sid = cfg.get("service_id") or cfg.get("name")
            try:
                src = _svcconfig.config_to_source(cfg)
                stats_by_svc[sid] = _lc.compaction_stats(src)
            except Exception:
                stats_by_svc[sid] = None
        out["compaction"] = stats_by_svc
    except Exception:
        out["compaction"] = {}

    # ── DuckDB connection-pool wait stats (Phase 6 in-process sampler) ──
    # Backs the Pool Wait card in the admin SystemHealthCard. The same
    # samples also stream to the OTel ``app.thread_wait_ms`` histogram for
    # off-box analysis; this in-process projection is for the UI's 1s poll.
    try:
        from backend.core import duckdb_pool as _pool_mod

        out["pool_wait"] = _pool_mod.get_all_stats()
    except Exception:
        out["pool_wait"] = []

    return out


@router.post("/admin/backfill-window")
def backfill_window(
    start_time: str = Query(..., description="ISO 8601 UTC start, e.g. '2026-05-31T23:00:00Z'"),
    end_time: str = Query(..., description="ISO 8601 UTC end, e.g. '2026-06-01T01:00:00Z'"),
    source: dict = Depends(get_source),
):
    """Force-sync a specific time window from FOS into local cache.

    Use to fill gaps left by ingestion outages (the normal cron pulls
    'since last sync' and won't reach back past its pointer once recovered).
    Idempotent — files already present in the local cache are skipped.
    """
    from backend.core import iceberg as _ice

    return _ice.sync_data(source, start_time=start_time, end_time=end_time)


from backend.core.fastly.utils import FASTLY_LOG_FIELDS as _FASTLY_LOG_FIELDS


def _fetch_fastly_log_counts(
    logging_svc_id: str, api_key: str, from_ts: int, to_ts: int, by: str
) -> tuple[dict[str, int], str | None]:
    """Return (bucket_iso → log_count, field_name_used or None).

    Bucket key is the UTC ISO string at the same width the local SQL bucket
    uses (`YYYY-MM-DDTHH` for hour, `YYYY-MM-DD` for day) so the outer-join
    in api_log_accounting can key on string equality directly.
    """
    import logging
    from datetime import UTC, datetime

    from backend.core.fastly.client import fastly

    payload = fastly(
        "GET",
        f"/stats/service/{logging_svc_id}?by={by}&from={from_ts}&to={to_ts}",
        token=api_key,
    )

    width = 13 if by == "hour" else 10
    records = payload.get("data", []) or []
    out: dict[str, int] = {}
    field_used: str | None = None
    missing_logged = False
    for r in records:
        ts = r.get("start_time")
        if ts is None:
            continue
        bucket = datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")[:width]
        chosen = 0
        for fname in _FASTLY_LOG_FIELDS:
            v = r.get(fname)
            if v:
                chosen = int(v)
                field_used = fname
                break
        if chosen == 0 and field_used is None and not missing_logged:
            logging.getLogger("admin.log_accounting").warning(
                "Fastly /stats/service response has no log-count field; keys present=%s",
                sorted(r.keys()),
            )
            missing_logged = True
        out[bucket] = out.get(bucket, 0) + chosen
    return out, field_used


# Sustained-loss thresholds — referenced by both api_log_accounting (so the
# UI callout matches the heal trigger) and the gap-heal cron in scheduler.py.
LOG_ACCOUNTING_LOSS_THRESHOLD = 0.05
LOG_ACCOUNTING_MIN_RUN = 2


def compute_log_accounting(source: dict, hours: int = 24, by: str = "hour") -> dict:
    """Pure compute path for log-line accounting.

    Returns a dict with all the fields api_log_accounting surfaces:
    ``buckets``, ``totals``, ``sustained_loss``, ``fastly_field_used``,
    ``from_ts``, ``to_ts``. Raises HTTPException on configuration error
    (no logging_service_id / no api_key) or on Fastly Stats API failure.

    Extracted so the gap-heal cron can reuse the same Fastly fetch + SQL +
    sustained-loss detection without duplicating the math — drift between
    the two would mean the heal trigger and the UI callout disagree.
    """
    from datetime import UTC, datetime, timedelta

    from backend import config as svcconfig
    from backend.core import metadata_db

    service_id = source.get("name", "")
    logging_svc_id = source.get("logging_service_id") or svcconfig.get_fastly_logging_service_id(service_id)
    if not logging_svc_id:
        raise HTTPException(
            status_code=400,
            detail={"error": "no logging_service_id configured for this service"},
        )
    api_key = svcconfig.get_fastly_api_key(service_id)
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail={"error": "no fastly_api_key configured for this service"},
        )

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    if by == "day":
        now = now.replace(hour=0)
    start = now - timedelta(hours=hours)
    from_ts = int(start.timestamp())
    to_ts = int((now + timedelta(hours=1 if by == "hour" else 24)).timestamp())

    try:
        fastly_counts, field_used = _fetch_fastly_log_counts(logging_svc_id, api_key, from_ts, to_ts, by)
    except Exception as e:
        raise HTTPException(status_code=502, detail={"error": f"Fastly Stats API call failed: {e}"})

    width = 13 if by == "hour" else 10
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    # Upper bound spans the END of the current (in-flight) bucket so newly
    # ingested files in that bucket are included — same span as the Fastly
    # request. Without this, an hour-aligned clamp drops every file ingested
    # after :00 and the latest bucket shows our_rows=0.
    end_clamp = now + timedelta(hours=1 if by == "hour" else 24)
    end_iso = end_clamp.strftime("%Y-%m-%dT%H:%M:%S")
    # We bucket by emission time (from the filename) but the SQL window is on
    # ingested_at, so widen it ±2h to catch files emitted near the window
    # boundary but ingested outside it. Python-side filter trims to the
    # requested emission window afterwards.
    sql_window_pad = timedelta(hours=2)
    sql_start_iso = (start - sql_window_pad).strftime("%Y-%m-%dT%H:%M:%S")
    sql_end_iso = (end_clamp + sql_window_pad).strftime("%Y-%m-%dT%H:%M:%S")
    # ingested_at is stored with a space separator (datetime('now')) while
    # start/end are ISO-T strings, so a raw string comparison silently
    # filters out everything — wrap both sides with datetime() to compare
    # as actual timestamps. See memory: usage_log timestamp formats.
    # Bucket by emission time parsed from the filename (falls back to
    # ingested_at for legacy/test files without an ISO prefix).
    start_bucket = start_iso[:width]
    end_bucket = end_iso[:width]
    local_counts = metadata_db.get_log_accounting_counts(
        service_id, sql_start_iso, sql_end_iso, width, start_bucket, end_bucket
    )

    all_buckets = sorted(set(fastly_counts.keys()) | set(local_counts.keys()))
    buckets: list[LogAccountingBucket] = []
    total_fastly = 0
    total_ours = 0
    worst_ts: str | None = None
    worst_gap_pct: float | None = None
    for b in all_buckets:
        fastly = int(fastly_counts.get(b, 0))
        ours, fcount = local_counts.get(b, (0, 0))
        gap = fastly - ours
        denom = fastly if fastly > 0 else ours
        gap_pct = (gap / denom) if denom > 0 else 0.0
        ts_iso = f"{b}:00:00Z" if by == "hour" else f"{b}T00:00:00Z"
        buckets.append(
            LogAccountingBucket(
                ts=ts_iso,
                fastly_logs=fastly,
                our_rows=ours,
                file_count=fcount,
                gap=gap,
                gap_pct=round(gap_pct, 6),
            )
        )
        total_fastly += fastly
        total_ours += ours
        # Rank by positive gap only — negative gaps are bucket-edge drift
        # where one side's emission/ingest straddled the boundary. The user
        # cares about "Fastly emitted more than we ingested" (real loss),
        # not "we ingested more than Fastly reports yet" (timing artifact).
        if gap_pct > (worst_gap_pct or 0.0):
            worst_ts = ts_iso
            worst_gap_pct = gap_pct

    total_gap = total_fastly - total_ours
    total_denom = total_fastly if total_fastly > 0 else total_ours
    total_pct = round((total_gap / total_denom), 6) if total_denom > 0 else 0.0
    totals = LogAccountingTotals(
        fastly_logs=total_fastly,
        our_rows=total_ours,
        gap=total_gap,
        gap_pct=total_pct,
        worst_bucket_ts=worst_ts,
        worst_bucket_gap_pct=(round(worst_gap_pct, 6) if worst_gap_pct is not None else None),
    )

    # Sustained-loss detection: only flag runs of ≥MIN_RUN consecutive completed
    # buckets with one-sided positive gap ≥LOSS_THRESHOLD (Fastly emitted more
    # than we ingested). Bucket-edge drift is bidirectional and stays under
    # 2.5%; the in-flight bucket is noisy because Fastly Stats lags our ingest,
    # so we exclude it from the scan. Returns the longest qualifying run.
    in_flight_bucket = now.strftime("%Y-%m-%dT%H") if by == "hour" else now.strftime("%Y-%m-%d")
    in_flight_ts = f"{in_flight_bucket}:00:00Z" if by == "hour" else f"{in_flight_bucket}T00:00:00Z"
    completed = [b for b in buckets if b.ts != in_flight_ts]
    sustained: SustainedLossAlert | None = None
    run_start = None
    for i, b in enumerate(completed + [None]):
        is_loss = b is not None and b.gap_pct >= LOG_ACCOUNTING_LOSS_THRESHOLD
        if is_loss and run_start is None:
            run_start = i
        elif not is_loss and run_start is not None:
            run = completed[run_start:i]
            if len(run) >= LOG_ACCOUNTING_MIN_RUN and (sustained is None or len(run) > sustained.n_buckets):
                sustained = SustainedLossAlert(
                    started_at=run[0].ts,
                    n_buckets=len(run),
                    max_gap_pct=round(max(rb.gap_pct for rb in run), 6),
                    total_lost_lines=sum(rb.gap for rb in run if rb.gap > 0),
                )
            run_start = None

    # Catch-up indicator: derived from the most recent successful ingest
    # (max(ingested_at) on ingested_files). Lag = now - that. The status
    # thresholds match the Fastly delivery promise — typical drop interval
    # is 60s, so >300s lag means we're at least 5 cycles behind. Stalled
    # means >1h (the operator should look at it).
    latest_ingest_str = metadata_db.get_latest_ingest_ts(service_id)
    catchup: dict | None
    if latest_ingest_str:
        latest_dt = datetime.fromisoformat(latest_ingest_str.replace(" ", "T")).replace(tzinfo=UTC)
        lag_seconds = max(0, int((datetime.now(UTC) - latest_dt).total_seconds()))
        if lag_seconds <= 300:
            status_str = "caught_up"
        elif lag_seconds <= 3600:
            status_str = "backfilling"
        else:
            status_str = "stalled"
        catchup = {
            "latest_ingest_ts": latest_dt.isoformat().replace("+00:00", "Z"),
            "lag_seconds": lag_seconds,
            "status": status_str,
        }
    else:
        catchup = {"latest_ingest_ts": None, "lag_seconds": None, "status": "no_data"}

    return {
        "by": by,
        "from_ts": start_iso + "Z",
        "to_ts": end_iso + "Z",
        "fastly_field_used": field_used,
        "buckets": buckets,
        "totals": totals,
        "sustained_loss": sustained,
        "catchup": catchup,
    }


@router.get("/admin/log-accounting", response_model=LogAccountingResponse)
def api_log_accounting(
    source: dict = Depends(get_source),
    hours: int = Query(24, ge=1, le=720),
    by: str = Query("hour", pattern="^(hour|day)$"),
) -> LogAccountingResponse:
    """Reconcile Fastly's authoritative log-line emission count against our
    locally-ingested row counts to surface any gap between emission and ingest.

    Per-bucket gap is the actionable signal — totals smooth over burst losses.
    """
    result = compute_log_accounting(source, hours=hours, by=by)
    return LogAccountingResponse.with_telemetry(**result)


@router.get("/admin/iceberg-info", response_model=IcebergTableInfoResponse)
@query_errors(status_code=500)
def iceberg_info_endpoint(source: dict = Depends(get_source)):
    """Return Iceberg table metadata: snapshots, data files, size, buffer status."""
    from backend.core import iceberg as db_iceberg

    result = db_iceberg.get_table_info(source)
    return IcebergTableInfoResponse.with_telemetry(**result)


@router.get("/admin/iceberg-calendar")
@query_errors(status_code=500)
def iceberg_calendar_endpoint(source: dict = Depends(get_source)):
    """Return per-date data file counts from Iceberg partition metadata."""
    from backend.core import iceberg as db_iceberg
    from backend.utils.telemetry import get_tracked_calls

    result = db_iceberg.get_snapshot_calendar(source)
    return {**result, "_debug_calls": get_tracked_calls()}


@router.post("/admin/commit-iceberg")
def iceberg_commit_endpoint(source: dict = Depends(get_source)):
    """Manually flush the local buffer to the Iceberg table."""
    import threading

    from backend.core.duckdb import start_cron_run
    from backend.scheduler import _run_commit

    try:
        run_id = start_cron_run(source, "commit")
        from backend.cron_progress import start_progress

        start_progress(run_id, service_id=source["name"], task="commit")
        t = threading.Thread(
            target=_run_commit, args=(source["name"],), kwargs={"force": True, "run_id": run_id}, daemon=True
        )
        t.start()
        return {"ok": True, "message": "Commit started.", "run_id": run_id}

    except RuntimeError as e:
        from backend.cron_progress import list_active_runs

        run_id = None
        for entry in list_active_runs():
            if entry.get("service_id") == source["name"] and entry.get("task") == "commit":
                run_id = entry["run_id"]
                break
        if run_id is None:
            raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
        return {"ok": True, "message": "Commit already running.", "run_id": run_id}


@router.post("/admin/rebuild-local-view")
def rebuild_local_view_endpoint(source: dict = Depends(get_source)):
    """One-button "fix it" for a stuck or stale local DuckDB view.

    Clears the in-memory + on-disk caches that drive view SQL generation,
    then triggers a metadata_sync that re-pulls the catalog from the cloud
    and rebuilds the view. The local raw buffer is NOT touched —
    un-committed data is safe.

    When to use: after manually editing parquet files, after a catalog
    schema-mapping desync, or when "Sync All" already ran and the view
    still looks wrong. This is the nuclear-option version of refresh.
    """
    import threading

    from backend.core import iceberg as db_iceberg
    from backend.core.duckdb import _cache_dir, start_cron_run
    from backend.cron_progress import start_progress
    from backend.scheduler import _run_metadata_sync

    service_id = source["name"]

    db_iceberg.clear_source_caches(service_id)
    # The persistent cache file lives at cache/{bucket}/snapshot_files_cache.json
    # — deleting it forces sync_data to call tbl.scan().plan_files() against
    # the freshly-loaded catalog instead of trusting the previous snapshot's
    # cached file list.
    persistent_cache = os.path.join(_cache_dir(source), "snapshot_files_cache.json")
    if os.path.exists(persistent_cache):
        try:
            os.remove(persistent_cache)
        except OSError as e:
            raise HTTPException(status_code=500, detail={"error": f"failed to remove snapshot cache: {e}"}) from e

    try:
        run_id = start_cron_run(source, "metadata_sync")
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail={"error": str(e), "busy": True}) from e

    start_progress(run_id, service_id=service_id, task="metadata_sync")
    t = threading.Thread(target=_run_metadata_sync, args=(service_id,), kwargs={"run_id": run_id}, daemon=True)
    t.start()
    return {"ok": True, "message": "Local view rebuild started.", "run_id": run_id}


@router.get("/admin/bot-sources", response_model=BotSourcesResponse)
def get_bot_sources_endpoint():
    """Return metadata for all bot sources plus rDNS cache stats."""
    from backend.utils.bot_sources import get_all_sources_meta
    from backend.utils.rdns_cache import get_stats as rdns_stats

    return BotSourcesResponse.with_telemetry(sources=get_all_sources_meta(), rdns=rdns_stats())


@router.post("/admin/bot-sources/{source_id}/refresh")
def refresh_bot_source_endpoint(source_id: str):
    """Fetch and re-cache a single bot source."""
    from backend.utils.bot_sources import fetch_and_cache_source

    try:
        meta = fetch_and_cache_source(source_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch bot source: {e}")
    return {"ok": True, "source": meta}




# ── Usage-logging endpoints (carved out for file-size budget) ──────────────
#
# Imported for side effects: registers the usage-log endpoints on
# ``router`` via decorators. Must be at the BOTTOM of this file so
# the shared router + helpers are bound before the sidecar pulls them.
from backend.routers import admin_usage  # noqa: F401,E402
