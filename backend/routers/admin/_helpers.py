"""Shared helper utilities for admin router submodules."""

from __future__ import annotations

import logging
import queue
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from typing import Any

from backend.core.fos_client import _safe_cdn_url
from backend.utils.sse import (  # noqa: F401
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
)

logger = logging.getLogger(__name__)
_STDLIB_URLOPEN = urllib.request.urlopen


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject all HTTP redirects on CDN fetches to prevent SSRF and header leakage."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        raise urllib.error.URLError(f"HTTP redirects are not allowed for CDN fetches (status={code})")


def _open_safe_cdn(req: urllib.request.Request, *, timeout: float = 30.0) -> Any:
    if urllib.request.urlopen is not _STDLIB_URLOPEN:
        return urllib.request.urlopen(req, timeout=timeout)
    opener = urllib.request.build_opener(SafeRedirectHandler())
    return opener.open(req, timeout=timeout)


def _sanitize_download_filename(name: str, fallback: str = "download") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", (name or "").strip()).strip("._")
    return cleaned[:128] if cleaned else fallback


def _validate_storage_rel_path(raw: str, *, allow_empty: bool = False) -> str:
    if raw is None:
        if allow_empty:
            return ""
        raise ValueError("missing path")
    if any(ch in raw for ch in ("\x00", "\\", "\r", "\n")):
        raise ValueError("invalid characters in path")
    trimmed = raw.strip("/")
    if not trimmed:
        if allow_empty:
            return ""
        raise ValueError("empty path")
    segments = trimmed.split("/")
    if any(seg in ("", ".", "..") for seg in segments):
        raise ValueError("path traversal segment not allowed")
    return "/".join(segments)


def _validated_admin_cdn_url(cdn_url: str | None) -> str | None:
    """Validate an admin download CDN base URL (strict Fastly HTTPS origin/path only).

    Wraps ``fos_client._safe_cdn_url`` without modifying ``fos_client.py`` and
    additionally rejects userinfo credentials, non-443 ports, query strings,
    fragments, control/backslash chars, and ``..`` path segments.
    """
    if not cdn_url or any(ch in cdn_url for ch in ("\x00", "\\", "\r", "\n")):
        return None
    cleaned = cdn_url.strip().rstrip("/")
    base_ok = _safe_cdn_url(cleaned)
    if base_ok is None:
        return None
    try:
        parsed = urllib.parse.urlsplit(base_ok)
        port = parsed.port
    except ValueError:
        return None
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        return None
    if port not in (None, 443):
        return None
    if parsed.query or parsed.fragment:
        return None
    if parsed.path and parsed.path != "/":
        try:
            _validate_storage_rel_path(parsed.path, allow_empty=True)
        except ValueError:
            return None
    return base_ok


class _QueueFile:
    """File-like wrapper around a queue.Queue for streaming ZIP generation."""

    def __init__(self, q: queue.Queue[bytes | None]) -> None:
        self.q = q
        self.offset = 0

    def write(self, b: bytes) -> int:
        self.q.put(b)
        n = len(b)
        self.offset += n
        return n

    def flush(self) -> None:
        pass

    def tell(self) -> int:
        return self.offset


def stream_zip(
    context_label: str,
    service_name: str,
    populate: Any,
) -> Any:
    """Run a streaming ZIP producer in a daemon thread."""

    def zip_worker(q: queue.Queue[bytes | None]) -> None:
        from backend.utils.telemetry import (
            flush_usage_log as _flush,
        )
        from backend.utils.telemetry import (
            process_context_scope as _pcs,
        )
        from backend.utils.telemetry import (
            start_call_tracking as _sct,
        )

        _sct()
        with _pcs(context_label):
            try:
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
    source: dict[str, Any],
    fos_client: Any,
    cdn: str,
    key: str,
    arcname: str,
    zf: zipfile.ZipFile,
    caller: str,
) -> bool:
    """Fetch a single S3 key into the zip via CDN with fallback to direct FOS."""
    import time as _t
    import urllib.parse
    import urllib.request

    from backend.utils.telemetry import record_cdn_call as _rcdn

    safe_cdn = _validated_admin_cdn_url(cdn) if cdn else None
    if safe_cdn:
        url = f"{safe_cdn}/{urllib.parse.quote(key)}"
        try:
            req = urllib.request.Request(url)
            if source.get("cdn_secret"):
                req.add_header("x-fastly-key", source["cdn_secret"])
            t0 = _t.time()
            bytes_read = 0
            cdn_headers = None
            with _open_safe_cdn(req, timeout=30) as response:
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


def attach_debug_db_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach ``_debug_sqlite`` and ``_debug_postgres`` when debug responses are enabled."""
    from backend.models.common import _debug_responses_enabled

    if _debug_responses_enabled():
        from backend.utils.telemetry import get_sqlite_queries

        ds = list(get_sqlite_queries())
        payload["_debug_sqlite"] = ds
        payload["_debug_postgres"] = ds
    return payload


def service_id_from_source(
    source: dict[str, Any],
    service_id: str | None = None,
    *,
    prefer_name: bool = False,
) -> str:
    """Resolve ``service_id`` from an explicit override or the ``source`` dict."""
    if service_id:
        return service_id
    if prefer_name:
        return str(source.get("name") or source.get("service_id") or "")
    return str(source.get("service_id") or source.get("name") or "")


from backend.core.request_context import _resolve_source as _resolve_source
