from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import queue
import sys
import threading
import time
from typing import Any

import aiohttp
import yarl
from aiohttp import web
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from backend.utils.telemetry import log_flusher as _log_flusher
from backend.utils.telemetry.log_flusher import (
    _IN_FLIGHT_LOCK as _IN_FLIGHT_LOCK,
)
from backend.utils.telemetry.log_flusher import (
    _IN_FLIGHT_REQUESTS as _IN_FLIGHT_REQUESTS,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_BATCH_MAX_INTERVAL_S as _LOG_BATCH_MAX_INTERVAL_S,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_BATCH_MAX_ROWS as _LOG_BATCH_MAX_ROWS,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_FLUSHER_LOCK as _LOG_FLUSHER_LOCK,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_FLUSHER_STOP as _LOG_FLUSHER_STOP,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_FLUSHER_THREAD as _LOG_FLUSHER_THREAD,
)
from backend.utils.telemetry.log_flusher import (
    _LOG_QUEUE as _LOG_QUEUE,
)
from backend.utils.telemetry.log_flusher import (
    _ensure_log_flusher_running as _ensure_log_flusher_running,
)
from backend.utils.telemetry.log_flusher import (
    _flush_batch as _flush_batch,
)
from backend.utils.telemetry.log_flusher import (
    _flush_log_writes_for_tests as _flush_log_writes_for_tests,
)
from backend.utils.telemetry.log_flusher import (
    _FlushBarrier as _FlushBarrier,
)
from backend.utils.telemetry.log_flusher import (
    _log_flusher_loop as _log_flusher_loop,
)
from backend.utils.telemetry.log_flusher import (
    _submit_log_write as _submit_log_write,
)
from backend.utils.telemetry.system_stats import _reset_active_requests_for_tests

logger = logging.getLogger("backend.utils.telemetry_proxy")

# Per-call caller-hint override. When set, the boto3 before-send hook
# (install_boto3_proxy_hook) writes this value into X-Telemetry-Caller
# instead of the default ``boto3.<op>``. Callers that want their hint to
# survive across the boto3 paginator's internal threading should keep the
# token in scope for the entire paginate() iteration — see
# backend/core/duckdb.py:_ProxyPaginatorShim.
_BOTO3_CALLER_HINT: contextvars.ContextVar[str | None] = contextvars.ContextVar("fos_boto3_caller_hint", default=None)

# Module-level state (intentionally process-singleton; reset in tests via
# _reset_for_tests()).
_PORT: int | None = None
_SERVER_THREAD: threading.Thread | None = None
_RUNNER: web.AppRunner | None = None
_LOOP: asyncio.AbstractEventLoop | None = None
_SESSION: aiohttp.ClientSession | None = None
_READY = threading.Event()
_START_LOCK = threading.Lock()

_UPSTREAM_TIMEOUT_TOTAL_S = float(os.getenv("FOS_PROXY_UPSTREAM_TIMEOUT_S", "90"))
_UPSTREAM_TIMEOUT = aiohttp.ClientTimeout(
    total=_UPSTREAM_TIMEOUT_TOTAL_S,
    sock_connect=5,
    sock_read=60,
)

_POOL_PER_HOST = 128

_UPSTREAM_KEEPALIVE_S = float(os.getenv("FOS_PROXY_KEEPALIVE_S", "4"))

_RETRYABLE_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientConnectionResetError,
    aiohttp.ClientOSError,
)

# RFC 7230 §6.1 hop-by-hop headers — must not be forwarded by a proxy.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)
# Our own telemetry headers — strip so they don't propagate to upstream.
_TELEMETRY_HEADERS = frozenset(
    {
        "x-fos-target",
        "x-telemetry-context",
        "x-telemetry-caller",
        "x-telemetry-service-id",
    }
)


def _load_config_cached(service_id: str) -> dict | None:
    from backend import config as _config

    return _config.load_config(service_id)


def _bust_config_cache(service_id: str | None = None) -> None:
    from backend import config as _config

    with _config._config_cache_lock:
        if service_id is None:
            _config._config_cache.clear()
        else:
            _config._config_cache.pop(service_id, None)


def _scheme_host(value: str) -> str:
    return value.replace("https://", "").replace("http://", "").split("/", 1)[0].lower()


def _cdn_host_for(cfg: dict) -> str | None:
    cdn_url = (cfg.get("cdn_url") or "").strip()
    if not cdn_url:
        return None
    return _scheme_host(cdn_url)


_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "0.0.0.0"})


def _strip_port_for_loopback_check(target_host: str) -> str:
    if target_host.startswith("[") and "]" in target_host:
        return target_host[1 : target_host.index("]")]
    if target_host.count(":") == 1:
        return target_host.split(":", 1)[0]
    return target_host


def _is_target_host_allowed(target_host: str, cdn_host: str | None, fos_native: str | None) -> bool:
    t = target_host.lower()
    if _strip_port_for_loopback_check(t) in _LOOPBACK_HOSTS:
        return True
    if cdn_host and t == cdn_host:
        return True
    if fos_native and t == fos_native.lower():
        return True
    return False


def _sign_request(method: str, url: str, headers: dict, body: bytes, service_id: str) -> dict:
    cfg = _load_config_cached(service_id)
    if not cfg:
        logger.warning(
            "[telemetry-proxy] cannot sign %s %s: no config for service_id=%r — forwarding UNSIGNED (will 403)",
            method,
            url,
            service_id,
        )
        return headers

    target_host = _scheme_host(url)
    cdn_host = _cdn_host_for(cfg)
    if cdn_host and target_host == cdn_host:
        if cfg.get("cdn_secret"):
            headers["x-fastly-key"] = cfg["cdn_secret"]
        return headers

    access_key = cfg.get("fos_access_key_id")
    secret_key = cfg.get("fos_secret_access_key")
    region = cfg.get("fos_region", "us-east-1")
    if not access_key or not secret_key:
        logger.warning(
            "[telemetry-proxy] cannot sign %s %s: service_id=%r is missing fos_access_key_id/fos_secret_access_key"
            " — forwarding UNSIGNED (will 403)",
            method,
            url,
            service_id,
        )
        return headers

    if (
        ("?delete" in url or b"<Delete" in body)
        and body
        and "Content-MD5" not in headers
        and "content-md5" not in headers
    ):
        import base64
        import hashlib

        headers["Content-MD5"] = base64.b64encode(
            hashlib.md5(body, usedforsecurity=False).digest()  # nosec B324
        ).decode("ascii")

    credentials = Credentials(access_key, secret_key)
    aws_req = AWSRequest(method=method, url=url, headers=headers, data=body)
    S3SigV4Auth(credentials, "s3", region).add_auth(aws_req)
    headers.update(dict(aws_req.headers))
    return headers


def _service_for_target(target_host: str, cdn_host: str | None) -> str:
    return "CDN" if cdn_host and target_host == cdn_host else "FOS"


def _build_details(x_cache: str | None, caller: str) -> str:
    parts: list[str] = []
    if x_cache:
        parts.append(x_cache)
    parts.append(caller)
    return " · ".join(parts)


def _build_upstream_headers(client_headers) -> dict:
    out: dict[str, str] = {}
    for k, v in client_headers.items():
        kl = k.lower()
        if kl in _HOP_BY_HOP or kl in _TELEMETRY_HEADERS or kl == "host":
            continue
        out[k] = v
    return out


async def handle_healthz(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def handle_request(request: web.Request) -> web.StreamResponse:
    global _IN_FLIGHT_REQUESTS
    with _IN_FLIGHT_LOCK:
        _IN_FLIGHT_REQUESTS += 1
        _log_flusher._IN_FLIGHT_REQUESTS = _IN_FLIGHT_REQUESTS
    try:
        return await _handle_request_inner(request)
    finally:
        with _IN_FLIGHT_LOCK:
            _IN_FLIGHT_REQUESTS -= 1
            _log_flusher._IN_FLIGHT_REQUESTS = _IN_FLIGHT_REQUESTS


async def _handle_request_inner(request: web.Request) -> web.StreamResponse:
    target_host = request.headers.get("X-Fos-Target")
    if not target_host:
        return web.Response(status=400, text="Missing X-Fos-Target header")

    service_id = request.headers.get("X-Telemetry-Service-Id")
    is_mutation = request.method in ("PUT", "POST", "DELETE")
    if service_id and is_mutation:
        _cfg = _load_config_cached(service_id)
        if _cfg:
            _fos_native = (_cfg.get("fos_native_endpoint") or _cfg.get("fos_endpoint") or "").strip()
            if _fos_native:
                target_host = _fos_native.replace("https://", "").replace("http://", "").rstrip("/")

    if target_host.startswith(("http://", "https://")):
        upstream_url = f"{target_host.rstrip('/')}{request.path_qs}"
        target_host_for_classify = target_host.replace("https://", "").replace("http://", "").rstrip("/").lower()
    else:
        upstream_url = f"https://{target_host}{request.path_qs}"
        target_host_for_classify = target_host.lower()
    out_headers = _build_upstream_headers(request.headers)
    service_id = request.headers.get("X-Telemetry-Service-Id")
    caller = request.headers.get("X-Telemetry-Caller", "telemetry-proxy")
    process_context = request.headers.get("X-Telemetry-Context")
    cdn_host: str | None = None
    fos_native: str | None = None
    cfg: dict | None = None
    if service_id:
        cfg = _load_config_cached(service_id)
        if cfg:
            cdn_host = _cdn_host_for(cfg)
            fos_native = (cfg.get("fos_native_endpoint") or cfg.get("fos_endpoint") or "").strip() or None

    if service_id and cfg is not None and not _is_target_host_allowed(target_host_for_classify, cdn_host, fos_native):
        logger.warning(
            "[telemetry-proxy] rejected disallowed X-Fos-Target=%r for service_id=%r (cdn=%r fos=%r)",
            target_host_for_classify,
            service_id,
            cdn_host,
            fos_native,
        )
        return web.Response(status=400, text="X-Fos-Target not allowed for service")

    data: Any
    if service_id and request.can_read_body:
        body = await request.read()
        data = body
        out_headers = _sign_request(request.method, upstream_url, out_headers, body, service_id)
    elif service_id:
        out_headers = _sign_request(request.method, upstream_url, out_headers, b"", service_id)
        data = None
    else:
        logger.warning(
            "[telemetry-proxy] forwarding UNSIGNED %s %s: no X-Telemetry-Service-Id header (caller=%s, will 403 against FOS)",
            request.method,
            upstream_url,
            caller,
        )
        data = request.content if request.can_read_body else None

    t0 = time.time()
    bytes_received = 0
    upstream_status: int | None = None
    x_cache: str | None = None
    proxy_resp: web.StreamResponse | web.Response
    client_response_started = False
    _IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")
    attempt = 0

    try:
        while True:
            try:
                wire_url = yarl.URL(upstream_url, encoded=True)
                if _SESSION is None:
                    raise RuntimeError("telemetry-proxy session not initialised")
                async with _SESSION.request(
                    method=request.method,
                    url=wire_url,
                    headers=out_headers,
                    data=data,
                    allow_redirects=False,
                ) as upstream_resp:
                    upstream_status = upstream_resp.status
                    x_cache = upstream_resp.headers.get("X-Cache")
                    proxy_resp = web.StreamResponse(
                        status=upstream_resp.status,
                        headers={k: v for k, v in upstream_resp.headers.items() if k.lower() not in _HOP_BY_HOP},
                    )
                    await proxy_resp.prepare(request)
                    client_response_started = True
                    is_head = request.method == "HEAD"
                    try:
                        async for chunk in upstream_resp.content.iter_chunked(65536):
                            bytes_received += len(chunk)
                            if not is_head:
                                await proxy_resp.write(chunk)
                        await proxy_resp.write_eof()
                    except ConnectionResetError as ce:
                        logger.warning(
                            "[telemetry-proxy] client disconnect mid-stream for %s %s after %d bytes: %s: %s",
                            request.method,
                            upstream_url,
                            bytes_received,
                            type(ce).__name__,
                            ce,
                        )
                    return proxy_resp
            except TimeoutError:
                upstream_status = 504
                logger.error("[telemetry-proxy] upstream timeout for %s %s", request.method, upstream_url)
                return web.Response(status=504, text="Upstream Timeout")
            except _RETRYABLE_CONNECTION_ERRORS as e:
                if attempt == 0 and not client_response_started and request.method in _IDEMPOTENT_METHODS:
                    attempt += 1
                    bytes_received = 0
                    logger.debug(
                        "[telemetry-proxy] retrying %s %s after connection error: %s",
                        request.method,
                        upstream_url,
                        e,
                    )
                    continue
                upstream_status = 502
                logger.error("[telemetry-proxy] upstream request failed: %s", e)
                return web.Response(status=502, text="Bad Gateway")
            except Exception as e:
                upstream_status = 502
                logger.error("[telemetry-proxy] upstream request failed: %s", e)
                return web.Response(status=502, text="Bad Gateway")
    finally:
        if service_id:
            elapsed_ms = round((time.time() - t0) * 1000, 2)
            service = _service_for_target(target_host_for_classify, cdn_host)
            status_str = "OK" if (upstream_status and 200 <= upstream_status < 300) else f"HTTP {upstream_status}"
            billing_method = request.method
            if service == "FOS" and request.method == "GET" and "list-type=" in request.query_string:
                billing_method = "LIST_OBJECTS_V2"
            row = {
                "method": billing_method,
                "path": request.path_qs,
                "bytes": bytes_received,
                "status": status_str,
                "service": service,
                "details": _build_details(x_cache, caller),
                "caller": caller,
                "time_ms": elapsed_ms,
            }
            _submit_log_write(service_id, row, process_context)

            if (
                service == "FOS"
                and request.method in ("GET", "HEAD")
                and (process_context or "").startswith("api:")
                and not any(h in (process_context or "") for h in ("/api/admin/health", "/api/health"))
            ):
                logger.warning(
                    "[telemetry-proxy] dashboard context hitting FOS: %s %s ctx=%s",
                    request.method,
                    request.path_qs,
                    process_context,
                )

            from backend.utils.telemetry import build_cdn_miss_synth_details, is_full_miss

            if service == "CDN" and is_full_miss(x_cache):
                synth_row = {
                    "method": "GET_OBJECT",
                    "path": request.path_qs,
                    "bytes": bytes_received,
                    "status": status_str,
                    "service": "FOS",
                    "details": build_cdn_miss_synth_details(bytes_received or None),
                    "caller": "cdn.miss",
                    "time_ms": elapsed_ms,
                }
                _submit_log_write(service_id, synth_row, process_context)


async def _create_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=256,
            limit_per_host=_POOL_PER_HOST,
            enable_cleanup_closed=True,
            keepalive_timeout=_UPSTREAM_KEEPALIVE_S,
        ),
        timeout=_UPSTREAM_TIMEOUT,
    )


def _run_server() -> None:
    global _PORT, _RUNNER, _LOOP, _SESSION

    _LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_LOOP)

    _SESSION = _LOOP.run_until_complete(_create_session())

    app = web.Application(client_max_size=4 * 1024 * 1024 * 1024)
    app.router.add_get("/healthz", handle_healthz)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    _RUNNER = web.AppRunner(app, access_log=None)
    _LOOP.run_until_complete(_RUNNER.setup())

    site = web.TCPSite(_RUNNER, "127.0.0.1", 0)
    _LOOP.run_until_complete(site.start())

    _server = site._server
    if _server is None:
        raise RuntimeError("_server is unexpectedly None")
    _PORT = _server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    _self = sys.modules[__name__]
    setattr(_self, "_LOOP", _LOOP)
    setattr(_self, "_SESSION", _SESSION)
    setattr(_self, "_RUNNER", _RUNNER)
    setattr(_self, "_PORT", _PORT)
    _READY.set()

    _LOOP.run_forever()


def start_proxy_server() -> None:
    global _SERVER_THREAD
    if _PORT is not None and _SERVER_THREAD is not None and _SERVER_THREAD.is_alive():
        return
    with _START_LOCK:
        if _SERVER_THREAD is None or not _SERVER_THREAD.is_alive():
            _READY.clear()
            thread = threading.Thread(target=_run_server, daemon=True, name="telemetry-proxy")
            _SERVER_THREAD = thread
            setattr(sys.modules[__name__], "_SERVER_THREAD", thread)
            thread.start()
    if not _READY.wait(timeout=2.0):
        raise RuntimeError("telemetry proxy failed to start within 2s")


def stop_proxy_server() -> None:
    global _RUNNER, _LOOP, _SERVER_THREAD, _SESSION, _PORT
    if _LOOP is not None:
        if _SESSION is not None:
            try:
                asyncio.run_coroutine_threadsafe(_SESSION.close(), _LOOP).result(timeout=2.0)
            except Exception:
                logger.debug("stop_proxy_server: asyncio.run_coroutine_threadsafe failed (ignored)", exc_info=True)
        if _RUNNER is not None:
            try:
                asyncio.run_coroutine_threadsafe(_RUNNER.cleanup(), _LOOP).result(timeout=2.0)
            except Exception:
                logger.debug("stop_proxy_server: asyncio.run_coroutine_threadsafe failed (ignored)", exc_info=True)
        _LOOP.call_soon_threadsafe(_LOOP.stop)
    if _SERVER_THREAD is not None:
        _SERVER_THREAD.join(timeout=2.0)


def proxy_endpoint() -> str:
    if _PORT is None:
        raise RuntimeError("proxy server is not running")
    return f"http://127.0.0.1:{_PORT}"


def install_boto3_proxy_hook(client, source: dict) -> None:
    native_target = source.get("fos_native_endpoint") or source["endpoint"]
    cdn_url = (source.get("cdn_url") or "").strip()
    edge_only = source.get("provisioning", {}).get("edge_only")
    if edge_only is None:
        edge_only = source.get("edge_only")
    cdn_target = _scheme_host(cdn_url) if (cdn_url and edge_only is not False) else None
    cdn_secret = source.get("cdn_secret") or ""
    service_id = source.get("service_id") or source.get("name", "default")
    _CDN_OPS = {"getobject", "headobject"}

    def _inject(request, event_name: str = "", **_kwargs):
        hint = _BOTO3_CALLER_HINT.get()
        try:
            from backend.utils.telemetry import get_process_context, get_process_context_with_fallback

            ctx = get_process_context() or ""
            if not ctx and hint not in ("ingest_download", "ingest_scan"):
                ctx = get_process_context_with_fallback() or ""
        except Exception:
            ctx = ""
        op = event_name.rsplit(".", 1)[-1].lower() if event_name else "unknown"

        if cdn_target and op in _CDN_OPS:
            request.headers["X-Fos-Target"] = cdn_target
            if cdn_secret:
                request.headers["x-fastly-key"] = cdn_secret
        else:
            request.headers["X-Fos-Target"] = native_target

        request.headers["X-Telemetry-Service-Id"] = service_id
        request.headers["X-Telemetry-Caller"] = hint if hint else f"boto3.{op}"
        if not ctx:
            import threading as _threading

            ctx = f"untagged:{_threading.current_thread().name}"
        request.headers["X-Telemetry-Context"] = ctx

    client.meta.events.register("before-send.s3.*", _inject)


def _reset_for_tests() -> None:
    global _PORT, _SERVER_THREAD, _RUNNER, _LOOP, _SESSION, _LOG_FLUSHER_THREAD
    _reset_active_requests_for_tests()
    try:
        _flush_log_writes_for_tests(timeout=1.0)
    except Exception:
        logger.debug("_reset_for_tests: _flush_log_writes_for_tests failed (ignored)", exc_info=True)
    while True:
        try:
            _LOG_QUEUE.get_nowait()
        except queue.Empty:
            break
    flusher = _log_flusher._LOG_FLUSHER_THREAD or _LOG_FLUSHER_THREAD
    if flusher is not None and flusher.is_alive():
        _LOG_FLUSHER_STOP.set()
        barrier = _FlushBarrier()
        _LOG_QUEUE.put(barrier)
        if not barrier.event.wait(timeout=1.0):
            raise RuntimeError("telemetry log flusher did not drain during test reset")
        flusher.join(timeout=1.0)
        if flusher.is_alive():
            raise RuntimeError("telemetry log flusher did not stop during test reset")
    _LOG_FLUSHER_THREAD = None
    _LOG_FLUSHER_STOP.clear()
    _PORT = None
    _SERVER_THREAD = None
    _RUNNER = None
    _LOOP = None
    _SESSION = None
    _READY.clear()
    _self = sys.modules[__name__]
    setattr(_log_flusher, "_LOG_FLUSHER_THREAD", None)
    setattr(_self, "_LOG_FLUSHER_THREAD", None)
    setattr(_self, "_PORT", None)
    setattr(_self, "_SERVER_THREAD", None)
    setattr(_self, "_RUNNER", None)
    setattr(_self, "_LOOP", None)
    setattr(_self, "_SESSION", None)
