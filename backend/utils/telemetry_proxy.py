"""Local HTTP proxy that intercepts S3/CDN calls from boto3, DuckDB httpfs,
and PyIceberg so we can centrally capture FOS/CDN telemetry, re-sign SigV4
on behalf of unsigned clients, and apply policy guardrails (e.g. dashboard
reads MUST NOT hit cloud).

Design spec: docs/superpowers/specs/2026-05-19-telemetry-proxy-design.md
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import queue
import threading
import time
from collections import defaultdict
from typing import Any

import aiohttp
import yarl
from aiohttp import web
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

# Per-call caller-hint override. When set, the boto3 before-send hook
# (install_boto3_proxy_hook) writes this value into X-Telemetry-Caller
# instead of the default ``boto3.<op>``. Callers that want their hint to
# survive across the boto3 paginator's internal threading should keep the
# token in scope for the entire paginate() iteration — see
# backend/core/duckdb.py:_ProxyPaginatorShim.
_BOTO3_CALLER_HINT: contextvars.ContextVar[str | None] = contextvars.ContextVar("fos_boto3_caller_hint", default=None)

logger = logging.getLogger(__name__)

# Module-level state (intentionally process-singleton; reset in tests via
# _reset_for_tests()).
_PORT: int | None = None
_SERVER_THREAD: threading.Thread | None = None
_RUNNER: web.AppRunner | None = None
_LOOP: asyncio.AbstractEventLoop | None = None
_SESSION: aiohttp.ClientSession | None = None
_READY = threading.Event()
# Serialises the "is the server already up / do we need to start it"
# decision in :func:`start_proxy_server`. Concurrent first-callers used
# to race: thread A would see ``_SERVER_THREAD is None``, spawn the
# server, and start waiting on _READY; thread B would see the just-
# spawned thread alive, early-return without waiting, then hit
# ``proxy_endpoint()`` while ``_PORT`` was still None — surfacing as
# "proxy server is not running" on every concurrent first-caller after
# the first.
_START_LOCK = threading.Lock()

# Upstream call timeouts. The wall-clock `total` is the safety net for
# requests that get wedged past Fastly's 60s first_byte_timeout (a stuck
# proxy used to hold a 300s connection open on 2026-05-20). 90s default
# leaves room for one full Fastly TTFB attempt plus byte streaming; tune
# via FOS_PROXY_UPSTREAM_TIMEOUT_S if you regularly fetch very large
# parquets through the proxy. sock_read=60 catches mid-stream stalls.
_UPSTREAM_TIMEOUT_TOTAL_S = float(os.getenv("FOS_PROXY_UPSTREAM_TIMEOUT_S", "90"))
_UPSTREAM_TIMEOUT = aiohttp.ClientTimeout(
    total=_UPSTREAM_TIMEOUT_TOTAL_S,
    sock_connect=5,
    sock_read=60,
)

# Connection-pool cap per upstream host. Boto3 sticks to ~10; DuckDB does
# many parallel Range GETs against the same parquet host so 128 gives some
# headroom without exhausting ephemeral ports.
_POOL_PER_HOST = 128

# Idle keep-alive cap on pooled upstream connections. aiohttp's default is
# 15s; Fastly's edge keep-alive is typically 5-10s. The mismatch caused the
# 2026-05-20 "Cannot write to closing transport" storm — the pool handed out
# a connection that Fastly had already half-closed, the write failed, and we
# returned 502. boto3's legacy-retry then re-fetched (97% of those retries
# succeeded), doubling the wire-byte cost on every affected GET. 4s keeps us
# below any plausible upstream keep-alive while still letting bursty
# back-to-back GETs reuse a connection. Pair this with enable_cleanup_closed
# on the connector to reap half-dead sockets the pool can't detect.
_UPSTREAM_KEEPALIVE_S = float(os.getenv("FOS_PROXY_KEEPALIVE_S", "4"))

# Connection-error class set we treat as eligible for one internal retry.
# Empirically (2026-05-20 telemetry): ServerDisconnectedError dominates;
# ClientConnectionResetError shows up under load; ClientOSError covers
# generic socket errors (broken pipe, ECONNRESET) that aren't already a
# ServerDisconnectedError. We do NOT retry on ClientResponseError (the
# upstream actually responded — propagate verbatim) or on auth errors.
_RETRYABLE_CONNECTION_ERRORS: tuple[type[BaseException], ...] = (
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientConnectionResetError,
    aiohttp.ClientOSError,
)

# usage_log write coalescer. Every proxied FOS/CDN call used to submit a
# single-row INSERT to a ThreadPoolExecutor — at 200 cdn ops/cron-tick that
# was 200 separate INSERTs (~10ms each, ~2s SQLite write time and 200 ring-
# buffer entries that drowned out genuine page SQLite). A single background
# thread now drains a queue and batches rows into executemany() per
# (service_id, process_context) group. Flush whenever the batch hits
# _LOG_BATCH_MAX_ROWS OR _LOG_BATCH_MAX_INTERVAL_S elapses since the last
# flush — whichever comes first.
_LOG_BATCH_MAX_ROWS = 50
_LOG_BATCH_MAX_INTERVAL_S = 0.1
_LOG_QUEUE: queue.Queue = queue.Queue()
_LOG_FLUSHER_STOP = threading.Event()
_LOG_FLUSHER_THREAD: threading.Thread | None = None
_LOG_FLUSHER_LOCK = threading.Lock()
# Pair sentinel ↔ event. _flush_log_writes_for_tests pushes an event-wrapping
# sentinel; the flusher drains everything up to (and including) it, then signals
# the event so the test can return deterministically — no sleep guesswork.


class _FlushBarrier:
    __slots__ = ("event",)

    def __init__(self) -> None:
        self.event = threading.Event()


_IN_FLIGHT_REQUESTS = 0
_IN_FLIGHT_LOCK = threading.Lock()


def _flush_batch(items: list[tuple[str, dict, str | None]]) -> None:
    """Group queued rows by (service_id, process_context) and call
    log_usage_calls once per group so each group is a single executemany."""
    if not items:
        return
    grouped: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
    for service_id, row, ctx in items:
        grouped[(service_id, ctx)].append(row)
    for (service_id, ctx), rows in grouped.items():
        try:
            from backend.core import metadata as metadata_db

            metadata_db.log_usage_calls(service_id, rows, process_context=ctx)
        except Exception as e:
            # Telemetry must NEVER break the request path it's tracking.
            logger.debug("[telemetry-proxy] batched usage log failed (%d rows): %s", len(rows), e)


def _log_flusher_loop(log_queue: queue.Queue, stop_event: threading.Event) -> None:
    # Queue + stop event are passed as parameters (not read from module
    # globals) so a future importlib.reload(telemetry_proxy) — done by the
    # FOS_PROXY_UPSTREAM_TIMEOUT_S env-override test — rebinds the module's
    # _LOG_QUEUE without leaving this still-alive daemon thread racing the
    # post-reload flusher for items from the SAME (new) queue. Each thread
    # owns its queue reference for life; the post-reload thread starts fresh
    # against its new queue, and this zombie just waits on its now-orphaned
    # one until the process exits.
    pending: list[tuple[str, dict, str | None]] = []
    deadline = time.monotonic() + _LOG_BATCH_MAX_INTERVAL_S
    while True:
        timeout = max(0.001, deadline - time.monotonic())
        try:
            item = log_queue.get(timeout=timeout)
        except queue.Empty:
            item = None  # interval timed out, no new row

        if isinstance(item, _FlushBarrier):
            _flush_batch(pending)
            pending = []
            deadline = time.monotonic() + _LOG_BATCH_MAX_INTERVAL_S
            item.event.set()
            if stop_event.is_set():
                # Drain anything left and exit.
                while True:
                    try:
                        leftover = log_queue.get_nowait()
                    except queue.Empty:
                        break
                    if not isinstance(leftover, _FlushBarrier):
                        pending.append(leftover)
                    else:
                        leftover.event.set()
                _flush_batch(pending)
                return
            continue

        if item is not None:
            pending.append(item)

        if pending and (len(pending) >= _LOG_BATCH_MAX_ROWS or time.monotonic() >= deadline):
            _flush_batch(pending)
            pending = []
            deadline = time.monotonic() + _LOG_BATCH_MAX_INTERVAL_S


def _ensure_log_flusher_running() -> None:
    global _LOG_FLUSHER_THREAD
    if _LOG_FLUSHER_THREAD is not None and _LOG_FLUSHER_THREAD.is_alive():
        return
    with _LOG_FLUSHER_LOCK:
        if _LOG_FLUSHER_THREAD is not None and _LOG_FLUSHER_THREAD.is_alive():
            return
        _LOG_FLUSHER_STOP.clear()
        _LOG_FLUSHER_THREAD = threading.Thread(
            target=_log_flusher_loop,
            args=(_LOG_QUEUE, _LOG_FLUSHER_STOP),
            name="proxy-log-flusher",
            daemon=True,
        )
        _LOG_FLUSHER_THREAD.start()


def _submit_log_write(service_id: str, row: dict, process_context: str | None) -> None:
    _ensure_log_flusher_running()
    _LOG_QUEUE.put((service_id, row, process_context))


def _flush_log_writes_for_tests(timeout: float = 15.0) -> None:
    """Block until all in-flight proxy requests finish their handler AND
    all queued log rows are written (or `timeout`).

    Tests that assert on mock-captured log writes (or caplog records from
    the proxy's warning path) must call this BEFORE exiting the
    `with patch(...)` / `with caplog.at_level(...)` context, otherwise:
      - the handler's finally block may run AFTER the patch is torn down
        and invoke the un-patched real log_usage_calls (capture empty).
      - the warning may be emitted AFTER caplog stops capturing.
    """
    # Two separate timeout budgets: in-flight wait gets the full `timeout`,
    # barrier wait gets its own `timeout` budget. Sharing them caused
    # intermittent test flakes — when in-flight polling consumed most of the
    # budget, the barrier wait shrank to ~10ms and timed out before the
    # flusher's executemany returned, leaving captured_rows empty.
    deadline = time.time() + timeout
    while time.time() < deadline:
        with _IN_FLIGHT_LOCK:
            in_flight = _IN_FLIGHT_REQUESTS
        if in_flight == 0:
            break
        time.sleep(0.005)
    # Push a barrier; the flusher signals its event AFTER draining every row
    # queued before the barrier. Deterministic — no sleep guesswork.
    if _LOG_FLUSHER_THREAD is not None and _LOG_FLUSHER_THREAD.is_alive():
        barrier = _FlushBarrier()
        _LOG_QUEUE.put(barrier)
        barrier.event.wait(timeout=timeout)


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
    # Delegates to backend.config.load_config, which has its own
    # mtime-revalidated cache. The wrapper exists to localize the import
    # (avoids circular import at module load) and to give tests a single
    # patch point.
    from backend import config as _config

    return _config.load_config(service_id)


def _bust_config_cache(service_id: str | None = None) -> None:
    # Clears the central cache in backend.config so tests that patch
    # load_config() see the patched return value immediately. Production
    # callers don't need this — load_config revalidates via st_mtime_ns
    # on every call, so credential rotations pick up automatically.
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


# Hosts treated as machine-local — connections to any of these from the
# backend itself reach loopback and so cannot escalate privilege via the
# proxy. ``0.0.0.0`` is the wildcard-bind address that ``moto`` and other
# in-process test servers commonly use; in production it would never be a
# legitimate X-Fos-Target value.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "0.0.0.0"})


def _strip_port_for_loopback_check(target_host: str) -> str:
    """Return the bare host portion of ``target_host`` for comparison
    against the loopback set. Handles three forms commonly seen as
    X-Fos-Target values:
      * ``[::1]:9999`` → ``::1`` (IPv6 literal with port, RFC 3986)
      * ``host:port`` / ``127.0.0.1:9999`` → host portion before the colon
      * ``::1`` / bare hostname → returned unchanged
    """
    if target_host.startswith("[") and "]" in target_host:
        return target_host[1 : target_host.index("]")]
    # A single colon means host:port. Multiple colons (and no brackets) is
    # a bare IPv6 literal that must not be split.
    if target_host.count(":") == 1:
        return target_host.split(":", 1)[0]
    return target_host


def _is_target_host_allowed(target_host: str, cdn_host: str | None, fos_native: str | None) -> bool:
    """Defense-in-depth gate on the value of X-Fos-Target.

    Without this gate, anyone who can reach the proxy AND supplies a
    valid X-Telemetry-Service-Id can use the proxy as an AWS-signed
    forwarder to arbitrary internet hosts (SSRF + service-credential
    exposure). The proxy binds to loopback so the realistic attacker
    today already has admin, but defense-in-depth wants the request to
    be rejected before signing rather than relying on the network
    binding.

    Allowed per-request:
      * the service's configured ``cdn_host`` (from ``cfg.cdn_url``),
        for CDN reads that use ``?key=`` auth and skip SigV4;
      * the service's configured ``fos_native_endpoint`` (defaults to
        ``{region}.object.fastlystorage.app``), for FOS reads/writes
        that the proxy SigV4-signs;
      * 127.0.0.1 / localhost / ::1 / 0.0.0.0 (loopback / wildcard-bind),
        because the proxy runs co-located with the backend and routing a
        loopback request *through* the proxy adds telemetry but not
        privilege.

    Region isolation is enforced — a service whose ``fos_endpoint`` is
    ``us-east-1.object.fastlystorage.app`` cannot use the proxy to
    target ``eu-west-1.object.fastlystorage.app`` (different region's
    credentials would not work anyway, but the gate cuts off the probe).
    """
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
        # Silent unsigned forwarding here was the root cause of the unexplained
        # 'early-startup 403 on LIST /<bucket>/?prefix=raw/' — the proxy
        # received the request, couldn't resolve the service config, dropped
        # signing, and FOS rejected the unsigned request. Log loudly so the
        # caller is identifiable next time.
        logger.warning(
            "[telemetry-proxy] cannot sign %s %s: no config for service_id=%r — forwarding UNSIGNED (will 403)",
            method,
            url,
            service_id,
        )
        return headers

    target_host = _scheme_host(url)
    cdn_host = _cdn_host_for(cfg)
    # CDN routing uses ?key= auth — do NOT inject SigV4 there.
    if cdn_host and target_host == cdn_host:
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

    credentials = Credentials(access_key, secret_key)
    aws_req = AWSRequest(method=method, url=url, headers=headers, data=body)
    # S3SigV4Auth (not bare SigV4Auth) adds X-Amz-Content-SHA256, which
    # S3 requires for every signed request.
    S3SigV4Auth(credentials, "s3", region).add_auth(aws_req)
    headers.update(dict(aws_req.headers))
    return headers


def _service_for_target(target_host: str, cdn_host: str | None) -> str:
    # 'CDN' if the target matches the service's cdn_url, else 'FOS'.
    # op_class is derived inside metadata_db.log_usage_calls from this
    # + method, so we don't pass it ourselves.
    return "CDN" if cdn_host and target_host == cdn_host else "FOS"


def _build_details(x_cache: str | None, caller: str) -> str:
    # X-Cache MUST be the first `· `-separated chunk so the shield-egress
    # doubling logic at backend.core.metadata.usage_log.log_usage_calls picks it up correctly.
    # Caller goes second.
    parts: list[str] = []
    if x_cache:
        parts.append(x_cache)
    parts.append(caller)
    return " · ".join(parts)


def _build_upstream_headers(client_headers) -> dict:
    # Strip hop-by-hop headers (RFC 7230 §6.1), the synthetic Host of
    # 127.0.0.1:<port>, and our internal telemetry routing headers.
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
    try:
        return await _handle_request_inner(request)
    finally:
        with _IN_FLIGHT_LOCK:
            _IN_FLIGHT_REQUESTS -= 1


async def _handle_request_inner(request: web.Request) -> web.StreamResponse:
    target_host = request.headers.get("X-Fos-Target")
    if not target_host:
        return web.Response(status=400, text="Missing X-Fos-Target header")

    # X-Fos-Target is normally a bare host (production: HTTPS implied).
    # Tests using moto run an http-only upstream on 127.0.0.1, so we
    # honor an explicit scheme prefix when one is present.
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
            # ``fos_native_endpoint`` only exists on the derived source dict
            # produced by ``backend.config.to_source_dict()`` — the raw
            # service-config JSON loaded here has ``fos_endpoint`` instead.
            # Without the fallback, every signed FOS request (sync's
            # ListObjectsV2, commit's PutObject, etc.) reads ``fos=None``
            # at the allowlist gate and gets rejected with a 400, stalling
            # ingestion silently. Fall through to ``fos_endpoint`` so the
            # gate's region-isolation guarantee still holds against the
            # value the rest of the codebase already treats as canonical.
            fos_native = (cfg.get("fos_native_endpoint") or cfg.get("fos_endpoint") or "").strip() or None

    # F6 defense-in-depth: when we have a service config the gate is
    # active. See ``_is_target_host_allowed`` for the rationale and the
    # allow-set. The check is skipped when no service_id is supplied
    # (the proxy already forwards unsigned in that branch — FOS will
    # 403, so there is no credential exposure to gate) or when the
    # service has no loadable config (existing path logs and forwards
    # unsigned for the same 403 fate).
    if service_id and cfg is not None and not _is_target_host_allowed(target_host_for_classify, cdn_host, fos_native):
        logger.warning(
            "[telemetry-proxy] rejected disallowed X-Fos-Target=%r for service_id=%r (cdn=%r fos=%r)",
            target_host_for_classify,
            service_id,
            cdn_host,
            fos_native,
        )
        return web.Response(status=400, text="X-Fos-Target not allowed for service")

    # SigV4 requires SHA256 of the body, which forces buffering when we
    # sign. botocore.auth.SigV4Auth doesn't support the streaming signed-
    # payload variant out of the box, so large PUTs (multi-GB compacted
    # commits) would buffer fully in memory if routed through the proxy.
    # Today's compaction flow uploads directly to FOS — only metadata.json
    # and small avro manifests transit the proxy (kilobytes each), so the
    # buffering is bounded. If a future flow routes bulk payloads through
    # here, switch to STREAMING-AWS4-HMAC-SHA256-PAYLOAD chunked signing
    # before increasing the request-body size limit.
    # ``data`` is either a fully-buffered ``bytes`` (signed paths) or a
    # streaming ``aiohttp.StreamReader`` (unsigned fallback) — aiohttp's
    # client accepts both, so ``Any`` keeps the union narrow at the
    # callsite without forcing a buffer-up that would defeat streaming.
    data: Any
    if service_id and request.can_read_body:
        body = await request.read()
        data = body
        out_headers = _sign_request(request.method, upstream_url, out_headers, body, service_id)
    elif service_id:
        out_headers = _sign_request(request.method, upstream_url, out_headers, b"", service_id)
        data = None
    else:
        # No X-Telemetry-Service-Id means we can't look up credentials and
        # the request goes upstream unsigned. For a private FOS bucket that's
        # a guaranteed 403. Log so the missing-header bug at the caller is
        # identifiable instead of debugging the upstream rejection.
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
    # Track whether response bytes have started flowing to the client. The
    # internal retry below MUST NOT fire after we've called prepare(), or we'd
    # be retrying a response the client has already partially observed.
    client_response_started = False
    # Idempotent methods are safe to replay against the upstream. The retry
    # exists to absorb keep-alive races (see _RETRYABLE_CONNECTION_ERRORS):
    # the pool hands out a connection the upstream already half-closed, the
    # write fails, we retry once on a fresh connection. Pre-fix telemetry
    # showed 97% of the resulting 502s were immediately followed by a
    # successful boto3 retry — moving that retry inside the proxy hides the
    # blip from boto3 and avoids the doubled wire-cost we were paying.
    _IDEMPOTENT_METHODS = ("GET", "HEAD", "OPTIONS")
    attempt = 0

    try:
        while True:
            try:
                # encoded=True preserves the URL verbatim on the wire. botocore's
                # S3SigV4Auth derives the canonical URI from urlsplit(url).path
                # without re-encoding, so a path containing %3D (e.g. iceberg
                # partition keys like timestamp_hour%3D2026-05-19-23) signs the
                # %3D form. aiohttp's default URL(str, encoded=False) would DECODE
                # %3D -> = on the wire, leaving the canonical URI we signed
                # diverged from what R2 verifies — producing
                # HTTP 403 'The calculated signature does not match'.
                wire_url = yarl.URL(upstream_url, encoded=True)
                assert _SESSION is not None, "telemetry-proxy session not initialised"
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
                    # RFC 7231 §4.3.2: HEAD responses MUST NOT include a body.
                    # Drain the upstream body for byte-counting + telemetry,
                    # but never forward to the client. aiohttp 3.14's stricter
                    # parser otherwise rejects HEAD-with-body as BadStatusLine.
                    is_head = request.method == "HEAD"
                    try:
                        async for chunk in upstream_resp.content.iter_chunked(65536):
                            bytes_received += len(chunk)
                            if not is_head:
                                await proxy_resp.write(chunk)
                        await proxy_resp.write_eof()
                    except ConnectionResetError as ce:
                        # Client (e.g. aiobotocore) closed its socket
                        # mid-stream. The upstream GET to FOS already
                        # completed and will bill us — record the row with
                        # the upstream status (typically 200) we captured
                        # above. Reporting this as a synthetic 502 would
                        # both over-count failures in our telemetry and
                        # under-report the upstream-billable success.
                        # boto3 will retry on its end if it needs the body.
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
                # Only safe to retry while no bytes have crossed back to the
                # client and the method is naturally idempotent. We retry at most
                # once — repeated failures indicate something worse than a
                # pool-races-server-FIN blip (e.g. upstream down) and we should
                # surface it instead of doubling the latency budget.
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
        # Write the telemetry row regardless of success/failure — errored
        # requests are still billable in FOS.
        if service_id:
            elapsed_ms = round((time.time() - t0) * 1000, 2)
            service = _service_for_target(target_host_for_classify, cdn_host)
            status_str = "OK" if (upstream_status and 200 <= upstream_status < 300) else f"HTTP {upstream_status}"
            # Row schema MUST match what metadata_db.log_usage_calls reads.
            # The X-Cache value MUST be the first `· `-separated chunk of
            # `details` — the shield-egress doubling at backend.core.metadata.usage_log.log_usage_calls
            # parses it from there.
            # Translate the raw HTTP verb to the S3 op name when we can
            # recognise the shape — log_usage_calls keys Class A vs Class B
            # off the S3 op name (LIST_OBJECTS_V2 = A), so a bare `GET`
            # would otherwise misclassify every boto3 list_objects_v2 call
            # as a Class B read. Only LIST is common enough to bother with;
            # other S3 ops keep their raw HTTP verb (PUT/POST/COPY are
            # already in the Class A list, HEAD/DELETE/GET-of-object are
            # correctly Class B).
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

            # Cost guardrail: dashboard reads MUST NOT hit FOS directly.
            # Warn loudly so the regression class (the iceberg_scan vs
            # read_parquet incident) is visible. Do not block — visibility
            # over enforcement until Phase 3 wires clients to the proxy.
            if service == "FOS" and request.method in ("GET", "HEAD") and (process_context or "").startswith("api:"):
                logger.warning(
                    "[telemetry-proxy] dashboard context hitting FOS: %s %s ctx=%s",
                    request.method,
                    request.path_qs,
                    process_context,
                )

            # CDN MISS → synth a Class B FOS GET_OBJECT row. Fastly's
            # behavior on a cache miss against an object-storage origin
            # is to fetch the full body regardless of the client's HTTP
            # method (so subsequent reads hit cache), so the underlying
            # FOS op is always GET_OBJECT, not HEAD_OBJECT. The MISS, HIT
            # chain (edge missed, shield hit) does NOT touch FOS.
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
    # Built once on startup so two concurrent requests don't both lazy-init
    # and leak the loser.
    #
    # enable_cleanup_closed reaps SSL connections that the upstream has
    # closed but whose FIN we haven't observed yet; without it, aiohttp can
    # hand a half-dead socket to the next request and the write fails with
    # "Cannot write to closing transport". keepalive_timeout below the
    # upstream's keep-alive bounds the window in which that race is even
    # possible. The internal-retry loop in _handle_request_inner is the
    # third line of defense for the residual race.
    return aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
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

    # Cap request bodies at 4GB. aiohttp's default 1MB cap is too small
    # for our use case (Iceberg commit multiparts), but the previous
    # ``client_max_size=0`` (unlimited) made the proxy a credible OOM
    # vector: ``await request.read()`` buffers the whole body, so two
    # concurrent multi-GB PUTs through the proxy could blow past the
    # 12GB container limit by themselves. 4GB covers any realistic
    # single multipart upload part (S3 individual part max is 5GB but
    # we never write parts that big) while bounding worst-case buffer
    # bloat. A 413 above 4GB is the right failure mode — callers can
    # split into smaller parts.
    app = web.Application(client_max_size=4 * 1024 * 1024 * 1024)
    app.router.add_get("/healthz", handle_healthz)
    app.router.add_route("*", "/{path_info:.*}", handle_request)

    # access_log=None silences aiohttp.access's per-request INFO spam.
    # Every FOS GET/HEAD/PUT routes through this proxy — at typical ingest
    # rates that's hundreds of lines/minute of noise burying real signal.
    # We still capture each request's metadata via the telemetry hooks
    # below, which write to usage_log with full context.
    _RUNNER = web.AppRunner(app, access_log=None)
    _LOOP.run_until_complete(_RUNNER.setup())

    site = web.TCPSite(_RUNNER, "127.0.0.1", 0)
    _LOOP.run_until_complete(site.start())

    # OS-assigned port becomes available only after .start().
    # asyncio's AbstractServer base class doesn't declare ``sockets`` but
    # every concrete implementation (Server/UnixServer) provides it; the
    # site is started so `_server` is the concrete subclass at runtime.
    _server = site._server
    assert _server is not None
    _PORT = _server.sockets[0].getsockname()[1]  # type: ignore[attr-defined]
    _READY.set()

    _LOOP.run_forever()


def start_proxy_server() -> None:
    global _SERVER_THREAD
    # Fast path: server is up and serving. ``_PORT`` is set inside
    # ``_run_server`` after the server has bound, so testing it (not just
    # ``_SERVER_THREAD.is_alive()``) is what tells us a concurrent caller
    # is safe to read ``proxy_endpoint()`` immediately.
    if _PORT is not None and _SERVER_THREAD is not None and _SERVER_THREAD.is_alive():
        return
    with _START_LOCK:
        # Re-check under the lock. The first caller spawns the thread;
        # every subsequent caller falls through to ``_READY.wait`` below
        # without re-spawning so we don't have N threads each starting
        # their own server (which would also leak module globals).
        if _SERVER_THREAD is None or not _SERVER_THREAD.is_alive():
            _READY.clear()
            _SERVER_THREAD = threading.Thread(target=_run_server, daemon=True, name="telemetry-proxy")
            _SERVER_THREAD.start()
    # Wait OUTSIDE the lock so concurrent callers all block in parallel
    # rather than serialising. Anything beyond ~2s is a bind failure;
    # fail loud rather than racing.
    if not _READY.wait(timeout=2.0):
        raise RuntimeError("telemetry proxy failed to start within 2s")


def stop_proxy_server() -> None:
    global _RUNNER, _LOOP, _SERVER_THREAD, _SESSION, _PORT
    if _LOOP is not None:
        if _SESSION is not None:
            try:
                asyncio.run_coroutine_threadsafe(_SESSION.close(), _LOOP).result(timeout=2.0)
            except Exception:
                pass
        if _RUNNER is not None:
            try:
                asyncio.run_coroutine_threadsafe(_RUNNER.cleanup(), _LOOP).result(timeout=2.0)
            except Exception:
                pass
        _LOOP.call_soon_threadsafe(_LOOP.stop)
    if _SERVER_THREAD is not None:
        _SERVER_THREAD.join(timeout=2.0)


def proxy_endpoint() -> str:
    if _PORT is None:
        raise RuntimeError("proxy server is not running")
    return f"http://127.0.0.1:{_PORT}"


def install_boto3_proxy_hook(client, source: dict) -> None:
    """Attach a before-send event handler that injects the proxy's required
    headers on every boto3 S3 request:
      - X-Fos-Target: CDN host for object downloads when cdn_url is set,
        else the native FOS endpoint. Writes/lists always native because
        the CDN is read-only and LIST isn't reliably CDN-served.
      - x-fastly-key: CDN auth header, injected only when targeting CDN
        (mirrors _configure_fos's DuckDB-httpfs pattern at duckdb.py:253-255).
      - X-Telemetry-Service-Id: for credential lookup at sign time
      - X-Telemetry-Caller: f"boto3.<operation>" (parsed from event_name)
      - X-Telemetry-Context: current process_context ContextVar (per-request)

    boto3's before-send event passes only `event_name` (e.g.
    "before-send.s3.HeadBucket"); operation name is the trailing segment.
    The request is an AWSPreparedRequest whose `.headers` is mutable.
    """
    native_target = source.get("fos_native_endpoint") or source["endpoint"]
    cdn_url = (source.get("cdn_url") or "").strip()
    cdn_target = _scheme_host(cdn_url) if cdn_url else None
    cdn_secret = source.get("cdn_secret") or ""
    service_id = source.get("service_id") or source.get("name", "default")
    # Only flip object downloads. LIST is HTTP GET too but the CDN
    # service isn't guaranteed to serve list responses correctly, and
    # mutations stay native because the CDN is read-only.
    _CDN_OPS = {"getobject", "headobject"}

    def _inject(request, event_name: str = "", **_kwargs):
        try:
            from backend.utils.telemetry import get_process_context_with_fallback

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
        hint = _BOTO3_CALLER_HINT.get()
        request.headers["X-Telemetry-Caller"] = hint if hint else f"boto3.{op}"
        # Always tag context: fall back to thread name so untagged work is
        # at least attributable to *some* thread instead of dropping into
        # the NULL bucket that blocks cost attribution.
        if not ctx:
            import threading as _threading

            ctx = f"untagged:{_threading.current_thread().name}"
        request.headers["X-Telemetry-Context"] = ctx

    client.meta.events.register("before-send.s3.*", _inject)


def _reset_for_tests() -> None:
    global _PORT, _SERVER_THREAD, _RUNNER, _LOOP, _SESSION
    # Drain any rows the prior test left in the coalescer queue so the next
    # test's log_usage_calls mock doesn't capture stale rows from an
    # already-torn-down patch context.
    try:
        _flush_log_writes_for_tests(timeout=1.0)
    except Exception:
        pass
    while True:
        try:
            _LOG_QUEUE.get_nowait()
        except queue.Empty:
            break
    _PORT = None
    _SERVER_THREAD = None
    _RUNNER = None
    _LOOP = None
    _SESSION = None
    _READY.clear()
