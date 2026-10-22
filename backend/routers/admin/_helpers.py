"""Shared helpers for the admin router package.

Houses the streaming-zip plumbing (`_QueueFile`, `_AbortableQueue`,
`ClientDisconnected`, `_stream_from_worker`, `_fetch_file_to_zip`) and
the source resolver (`_resolve_source`) used by ingest + download
endpoints.

Re-exported from ``backend.routers.admin`` for external test compat.
"""

from __future__ import annotations

import logging
import queue
import zipfile
from typing import Any

from fastapi import HTTPException

from backend.models.admin import SyncStartResponse
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
    stream_sse_from_worker,
)

logger = logging.getLogger(__name__)


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


def run_admin_cron_trigger(
    source: dict[str, Any],
    task_name: str,
    target_fn: Any,
    *,
    target_kwargs: dict[str, Any] | None = None,
    success_msg: str,
    in_progress_msg: str,
    readonly_detail: Any,
    invalidate_cache: bool = True,
    require_high_scale: bool = False,
    high_scale_detail: Any = None,
) -> SyncStartResponse:
    """Shared execution wrapper for manual admin cron trigger routes."""
    if source.get("access_level") == "read_only":
        raise HTTPException(status_code=403, detail=readonly_detail)

    if require_high_scale:
        from backend import config as svcconfig

        if not svcconfig.is_high_scale_mode(source):
            raise HTTPException(
                status_code=400,
                detail=high_scale_detail or f"{task_name} is only supported under high-scale mode.",
            )

    if invalidate_cache:
        from backend.repositories.dashboard import invalidate_service

        invalidate_service(source["name"])

    from backend.utils.router_utils import start_or_resume_cron

    res = start_or_resume_cron(
        source,
        task_name,
        target_fn,
        target_kwargs=target_kwargs,
        success_msg=success_msg,
        in_progress_msg=in_progress_msg,
    )
    return SyncStartResponse(**res)


def stream_zip(
    context_label: str,
    service_name: str,
    populate: Any,
) -> Any:
    """Run a streaming ZIP producer in a daemon thread.

    Shared envelope previously inlined as ``zip_worker(q)`` in every
    download endpoint:
      - start_call_tracking() so this worker thread owns a fresh _CALLS
        list (the request middleware has already flushed by the time the
        worker runs);
      - process_context_scope(context_label) so the fsspec iothread
        fallback isn't wiped by a concurrent scope exit on another
        thread (see _initialize_service for context);
      - try/except around ``populate(zf)`` catches Exception (which the
        existing handlers RELY on to swallow ClientDisconnected and
        keep the response stream sane);
      - flush_usage_log(service_name) on the way out so the worker
        thread's recorded calls land in the usage_log batch.

    ``populate(zf)`` is the caller's content callback — receives an
    already-open zipfile.ZipFile and writes entries into it. Caller
    does not need to handle the queue or the lifecycle.
    """

    def zip_worker(q: queue.Queue):
        from backend.utils.telemetry import (
            process_context_scope as _pcs,
        )
        from backend.utils.telemetry import (
            start_call_tracking as _sct,
        )
        from backend.utils.usage_logger import flush_usage_log as _flush

        _sct()
        with _pcs(context_label):
            try:
                # _QueueFile is a stream-shaped duck type the zipfile
                # stubs don't recognise; cast keeps the call site
                # type-safe at the boundary without changing runtime.
                from typing import cast as _cast

                with zipfile.ZipFile(_cast(Any, _QueueFile(q)), "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    populate(zf)
            except Exception:
                logger.error("Error in ZIP generation", exc_info=True)
            finally:
                try:
                    _flush(service_name)
                except Exception:
                    logger.debug("zip_worker: _flush failed (ignored)", exc_info=True)
                q.put(None)

    return _stream_from_worker(zip_worker)


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
        except ClientDisconnected:
            raise
        except Exception:
            logger.warning("CDN fetch failed for %s, falling back to FOS", key, exc_info=True)

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
    except ClientDisconnected:
        raise
    except Exception:
        logger.error("Error fetching %s from FOS", key, exc_info=True)
        return False


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
