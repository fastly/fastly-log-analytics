from __future__ import annotations

import hmac
import json
import logging
import os
import re
import secrets
import sys
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from typing import Literal
from urllib.parse import urlparse, urlsplit

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


class SlidingWindowLimiter:
    def __init__(self, *, window_s: float, max_keys: int, clock: Callable[[], float] | None = None) -> None:
        self.window_s = window_s
        self.max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._events: OrderedDict[str, deque[tuple[float, int]]] = OrderedDict()
        self._locks: OrderedDict[str, float] = OrderedDict()

    @property
    def MAX_TRACKED_IPS(self) -> int:
        return self.max_keys

    @MAX_TRACKED_IPS.setter
    def MAX_TRACKED_IPS(self, value: int) -> None:
        self.max_keys = value

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.time()

    def _prune_key(self, key: str, cutoff: float) -> deque[tuple[float, int]]:
        events = self._events.get(key)
        if events is None:
            return deque()
        while events and events[0][0] < cutoff:
            events.popleft()
        return events

    def _prune_now(self, key: str) -> tuple[float, float, deque[tuple[float, int]]]:
        now = self._now()
        cutoff = now - self.window_s
        return now, cutoff, self._prune_key(key, cutoff)

    def _store(self, key: str, events: deque[tuple[float, int]], cutoff: float) -> None:
        self._events[key] = events
        self._events.move_to_end(key)
        while self._events:
            oldest, history = next(iter(self._events.items()))
            if history and history[-1][0] >= cutoff:
                break
            del self._events[oldest]
        while len(self._events) > self.max_keys:
            self._events.popitem(last=False)

    def try_acquire(
        self, key: str, weight: int = 0, *, max_events: int | None = None, max_weight: int | None = None
    ) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            if max_events is not None and len(events) >= max_events:
                self._store(key, events, cutoff)
                return False
            events.append((now, max(0, int(weight))))
            self._store(key, events, cutoff)
            return max_weight is None or sum(w for _, w in events) <= max_weight

    def strike(self, key: str, *, threshold: int, lockout_s: float, max_locks: int | None = None) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            events.append((now, 1))
            self._store(key, events, cutoff)
            if len(events) >= threshold and key not in self._locks:
                self._locks[key] = now + lockout_s
                while len(self._locks) > (max_locks if max_locks is not None else self.max_keys):
                    self._locks.popitem(last=False)
                return True
            return False

    def is_locked(self, key: str) -> tuple[bool, int]:
        with self._lock:
            until = self._locks.get(key)
            if until is None:
                return False, 0
            now = self._now()
            if now >= until:
                del self._locks[key]
                return False, 0
            return True, int(until - now)

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._lock:
            now = self._now()
            cutoff = now - self.window_s
            counts: dict[str, int] = {}
            for key in list(self._events):
                events = self._prune_key(key, cutoff)
                if events:
                    counts[key] = len(events)
                else:
                    del self._events[key]
            locks: dict[str, int] = {}
            for key, until in list(self._locks.items()):
                if until <= now:
                    del self._locks[key]
                else:
                    locks[key] = int(until - now)
            return counts, locks


ANALYST_SESSION_COOKIE = "analyst_session_id"
ANALYST_PENDING_SESSION_COOKIE = "analyst_pending_session_id"


def analyst_cookie_is_secure(request: Request, public_url: str | None) -> bool:
    if request.url.scheme.lower() == "https":
        return True
    if not public_url:
        return False
    try:
        parsed = urlsplit(public_url)
    except ValueError:
        return False
    return parsed.scheme.lower() == "https" and bool(parsed.netloc)


def set_analyst_session_cookies(
    response: Response,
    session_id: str,
    *,
    tos_pending: bool,
    secure: bool,
    samesite: Literal["strict", "lax"] = "strict",
) -> None:
    target_cookie = ANALYST_PENDING_SESSION_COOKIE if tos_pending else ANALYST_SESSION_COOKIE
    other_cookie = ANALYST_SESSION_COOKIE if tos_pending else ANALYST_PENDING_SESSION_COOKIE
    response.set_cookie(
        key=target_cookie,
        value=session_id,
        httponly=True,
        secure=secure,
        samesite=samesite,
        max_age=24 * 60 * 60,
        path="/",
    )
    response.delete_cookie(other_cookie, path="/")


def safe_audit_email(email: str | None) -> str:
    if not email:
        return "-"
    cleaned = "".join(c for c in email if c.isprintable())
    return cleaned[:254] or "-"


def analyst_allowed_services(request: Request) -> set[str] | None:
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return None
    return set(analyst_session.service_ids or [])


def require_service_in_scope(request: Request, service_id: str | None) -> set[str] | None:
    allowed = analyst_allowed_services(request)
    if allowed is not None and service_id not in allowed:
        raise HTTPException(status_code=403, detail={"error": "service_not_authorized", "service": service_id})
    return allowed


def require_owned_row(
    request: Request, service_id: str, row_id: str, fetch: Callable[[str, str], dict | None]
) -> dict | None:
    allowed = require_service_in_scope(request, service_id)
    if allowed is None:
        return None
    existing = fetch(row_id, service_id)
    if existing and existing.get("service_id") not in allowed:
        raise HTTPException(
            status_code=403, detail={"error": "service_not_authorized", "service": existing.get("service_id")}
        )
    return existing


def mask_ips_for(analyst_session: object | None) -> bool:
    if analyst_session is None:
        return False
    policy = getattr(analyst_session, "pii_policy", None)
    return bool(policy.get("mask_ips")) if isinstance(policy, dict) else False


sys.modules["backend.utils.rate_limit"] = sys.modules[__name__]
sys.modules["backend.utils.auth"] = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.rate_limit = sys.modules[__name__]  # type: ignore[attr-defined]
    _utils_pkg.auth = sys.modules[__name__]  # type: ignore[attr-defined]
from backend.core import share_db  # noqa: E402
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS  # noqa: E402
from backend.utils.tunnel import compute_fingerprint, get_tunnel_manager  # noqa: E402

ADMIN_GATEWAY_HEADER = "X-Admin-Gateway-Token"


def admin_gateway_required() -> bool:
    return os.getenv("ADMIN_GATEWAY_REQUIRED", "") == "1"


def validate_admin_gateway_config() -> None:
    if os.getenv("ADMIN_GATEWAY_REQUIRED", "") not in ("", "0", "1"):
        raise ValueError("ADMIN_GATEWAY_REQUIRED must be 0 or 1")
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    if (admin_gateway_required() or secret) and len(secret) < 32:
        raise ValueError("ADMIN_GATEWAY_SECRET must contain at least 32 characters for admin gateway access")


def authenticated_admin_gateway(request: Request) -> bool:
    if getattr(request.state, "admin_gateway_authenticated", False):
        return True
    if request.headers.get("x-proxied-by-caddy"):
        return False
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    values = request.headers.getlist(ADMIN_GATEWAY_HEADER)
    if len(secret) < 32 or len(values) != 1:
        return False
    return hmac.compare_digest(values[0].encode("utf-8"), secret.encode("utf-8"))


BODY_INSPECT_MAX_BYTES = 4 * 1024 * 1024
_UNAUTH_ANALYST_PATHS = {
    "/api/share/login",
    "/api/share/logout",
    "/api/share/heartbeat",
    "/api/share/acknowledge",
    "/api/share/tos",
    "/api/share/auth-config",
    "/api/health",
    "/api/bootstrap",
    "/js/rum.js",
    "/js/faro-sdk.js",
    "/rum-beacon",
    "/api/web-vitals",
    "/api/ux-events",
}
_ANALYST_BLOCKED_PREFIXES = (
    "/api/admin/",
    "/api/provision/",
    "/api/debug/",
    "/api/usage/",
    "/api/cron-runs",
    "/api/audit-logs",
    "/api/alerts",
)
_ANALYST_BLOCKED_SUBPATHS = (
    "/api/download",
    "/api/download-all",
    "/api/download-folder",
    "/api/cron-schedule",
    "/api/sync-status",
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
)
_ANALYST_BLOCKED_SUBPATH_REGEX: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/services/[^/]+/lake-info$"),
    re.compile(r"^/api/services/[^/]+/logging-settings(/.*)?$"),
    re.compile(r"^/api/services/[^/]+/log-fields$"),
    re.compile(r"^/api/services/[^/]+/custom-fields(/.*)?$"),
    re.compile(r"^/api/services/[^/]+/log-field-audit$"),
)
_ANALYST_BLOCKED_SCORING_SUFFIXES: tuple[str, ...] = (
    "/config",
    "/status",
    "/audit",
    "/threshold",
    "/exclude-regex",
    "/exclude-regex/validate",
    "/enforce-status-code",
    "/enforce-threshold",
    "/l2-enforce",
    "/matrix-versions",
    "/restore",
    "/retrain",
    "/rotate-key",
    "/enable",
    "/disable",
    "/dashboard",
    "/evaluation/per-reason",
)
_ANALYST_BLOCKED_RUM_SUFFIXES: tuple[str, ...] = ("/enable", "/disable", "/versions", "/upgrade", "/settings")
_ANALYST_ALLOWED_WRITE_PREFIXES = (
    "/api/share/",
    "/api/dashboard/",
    "/api/security/",
    "/api/origin/",
    "/api/performance/",
    "/api/insights",
    "/api/network-health",
    "/api/network-quality",
    "/api/network/",
    "/api/query",
    "/api/sessions",
    "/api/cmcd/",
    "/api/value/",
    "/api/web-vitals",
    "/api/charts/",
    "/api/ux-events",
    "/api/assets/",
    "/api/high-scale/",
)
_TELEMETRY_IDLE_EXEMPT_PREFIXES = ("/api/web-vitals", "/api/ux-events")
_ANALYST_SSE_ALLOWLIST: set[str] = {"/api/log-extents/stream", "/realtime-stream", "/control-room/error-stream"}
_LOCAL_HOST_ALLOWLIST = {
    "localhost",
    "127.0.0.1",
    "[::1]",
    "0.0.0.0",
    "testserver",
    "backend",
    "frontend",
    "caddy",
    "web",
}
_env_hosts = os.getenv("LOCAL_HOSTS") or os.getenv("LOCAL_HOST_ALLOWLIST") or os.getenv("ALLOWED_HOSTS")
if _env_hosts:
    for _h in _env_hosts.split(","):
        _clean_h = _h.strip().lower()
        if _clean_h:
            _LOCAL_HOST_ALLOWLIST.add(_clean_h)
import ipaddress

_LOCAL_ADMIN_CIDRS_ENV = "LOCAL_ADMIN_CIDRS"
_local_admin_cidrs_cache: tuple[str, tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]] = ("", ())


def _local_admin_networks() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    global _local_admin_cidrs_cache
    raw = os.environ.get(_LOCAL_ADMIN_CIDRS_ENV, "")
    if raw == _local_admin_cidrs_cache[0]:
        return _local_admin_cidrs_cache[1]
    nets: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            net = ipaddress.ip_network(part, strict=False)
        except ValueError:
            logger.error("[remote_access] ignoring invalid %s entry %r", _LOCAL_ADMIN_CIDRS_ENV, part)
            continue
        if not net.is_private or net.is_link_local or net.num_addresses > 2**16:
            logger.error(
                "[remote_access] REFUSING %s entry %r — admin trust segments must "
                "be private, non-link-local, and no wider than a /16. Scope it to "
                "the isolated infra network.",
                _LOCAL_ADMIN_CIDRS_ENV,
                part,
            )
            continue
        nets.append(net)
    _local_admin_cidrs_cache = (raw, tuple(nets))
    return _local_admin_cidrs_cache[1]


def _is_private_or_loopback(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return ip_str in ("testclient", "localhost")
    if ip.is_loopback:
        return True
    return any(ip in net for net in _local_admin_networks())


def is_request_remote(request: Request) -> bool:
    if authenticated_admin_gateway(request):
        return False
    if admin_gateway_required() or ADMIN_GATEWAY_HEADER in request.headers:
        return True
    if request.client is None:
        return True
    host = client_ip(request, default="127.0.0.1")
    if not _is_private_or_loopback(host):
        return True
    if request.headers.get("x-remote-analyst") == "1":
        mgr = get_tunnel_manager()
        if mgr.is_sharing_active():
            return True
    return False


def client_ip(request: Request, *, default: str = "0.0.0.0") -> str:
    return request.client.host if request.client else default


def _host_base(host_header: str) -> str:
    return host_header.split(":")[0].lower() if host_header else ""


def _in_local_allowlist(base: str) -> bool:
    return "*" in _LOCAL_HOST_ALLOWLIST or base in _LOCAL_HOST_ALLOWLIST


def _local_host_allowed(host_header: str) -> bool:
    base = _host_base(host_header)
    return bool(base) and (_is_private_or_loopback(base) or _in_local_allowlist(base))


def _allowed_remote_hosts() -> list[str]:
    state = get_tunnel_manager().state
    candidates: list[str] = []
    if state.public_endpoint:
        hostname = urlparse(state.public_endpoint).hostname
        if hostname:
            candidates.append(hostname.lower())
    try:
        from backend import config

        for cfg in config.list_configs():
            rf = cfg.get("remote_frontend")
            if isinstance(rf, dict):
                candidates.extend(
                    val.strip().lower()
                    for k in ("domain_name", "origin_host", "override_host")
                    if isinstance(val := rf.get(k), str) and val
                )
    except Exception:
        logger.debug("_allowed_remote_hosts: step failed (ignored)", exc_info=True)
    return candidates


def _remote_host_allowed(host_header: str) -> bool:
    base = _host_base(host_header)
    return bool(base) and (_in_local_allowlist(base) or base in _allowed_remote_hosts())


def _origin_allowed(origin: str) -> bool:
    host = (urlparse(origin).hostname or "").lower() if origin else ""
    return bool(host) and host in _allowed_remote_hosts()


def _is_blocked_path(path: str) -> bool:
    normalized = path.rstrip("/\r\n") or "/"
    if any(normalized == p.rstrip("/") or normalized.startswith(p) for p in _ANALYST_BLOCKED_PREFIXES):
        return True
    for sp in _ANALYST_BLOCKED_SUBPATHS:
        if normalized == sp or normalized.startswith(sp + "/") or normalized.startswith(sp + "?"):
            return True
    if "/scoring/" in normalized and normalized.endswith(_ANALYST_BLOCKED_SCORING_SUFFIXES):
        return True
    if "/rum/" in normalized and normalized.endswith(_ANALYST_BLOCKED_RUM_SUFFIXES):
        return True
    if any(pat.fullmatch(normalized) for pat in _ANALYST_BLOCKED_SUBPATH_REGEX):
        return True
    return False


_PATH_SERVICE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/services/([^/]+)(?:/|$)"),
    re.compile(r"^/api/alerts/([^/]+)(?:/|$)"),
    re.compile(r"^/api/views/([^/]+)(?:/|$)"),
)


def _path_service_ids(request: Request) -> list[str]:
    routes = getattr(getattr(getattr(request, "app", None), "router", None), "routes", None)
    if routes:
        from starlette.routing import Match

        for route in routes:
            match, child_scope = route.matches(request.scope)
            if match == Match.FULL:
                path_params = child_scope.get("path_params", {})
                out = [path_params[k] for k in ("service_id", "service") if k in path_params]
                if out:
                    return out
                break
    path = request.url.path
    return [m.group(1) for pat in _PATH_SERVICE_PATTERNS if (m := pat.match(path))]


def _is_sse_route(path: str) -> bool:
    if "/sse" in path:
        return True
    last_segment = path.rsplit("/", 1)[-1]
    return last_segment == "stream" or last_segment.endswith("-stream")


def apply_response_hardening(response: Response) -> Response:
    response.headers.setdefault("Cache-Control", "private, no-store")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' blob:; "
            "worker-src 'self' blob:; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: blob:; "
            "font-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; "
            "base-uri 'self'; "
            "form-action 'self'"
        ),
    )
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    return response


_QUERY_DEBUG_KEYS: tuple[str, ...] = (
    "_debug_queries",
    "debug_queries",
    "_debug_sqlite",
    "debug_sqlite",
    "_debug_postgres",
    "debug_postgres",
)
_API_DEBUG_KEYS: tuple[str, ...] = ("_debug_calls", "debug_calls")


def analyst_debug_visibility(request: Request | None = None) -> tuple[bool, bool]:
    if request is not None:
        cached = getattr(request.state, "_analyst_debug_vis", None)
        if cached is not None:
            return cached
    try:
        from backend.core.share_db.settings import get_setting

        q_vis = get_setting("query_debug_visibility", "disabled")
        a_vis = get_setting("api_call_debug_visibility", "disabled")
        res = (q_vis in ("analysts", "both"), a_vis in ("analysts", "both"))
    except Exception:
        res = (False, False)
    if request is not None:
        request.state._analyst_debug_vis = res
    return res


def _rebuild_response(response: Response, content: bytes, media_type: str, *, update_length: bool = False) -> Response:
    headers = dict(response.headers)
    if update_length:
        headers["content-length"] = str(len(content))
    return Response(content=content, status_code=response.status_code, headers=headers, media_type=media_type)


async def _strip_analyst_envelope(
    response: Response, analyst_session: object | None = None, request: Request | None = None
) -> Response:
    ct = response.headers.get("content-type", "")
    if "application/json" not in ct:
        return response
    raw_policy = getattr(analyst_session, "pii_policy", None) if analyst_session is not None else None
    pii_policy = raw_policy if isinstance(raw_policy, dict) and raw_policy.get("mask_ips") else None
    body = b""
    async for chunk in response.body_iterator:  # type: ignore[attr-defined]
        body += chunk
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return _rebuild_response(response, body, ct)
    changed = False
    if isinstance(data, dict):
        if request is not None and "_debug_queries" in data and "_debug_calls" in data:
            request.state._analyst_envelope_complete = True
        analyst_query_enabled, analyst_api_enabled = analyst_debug_visibility(request)
        from backend.models.common import _debug_responses_enabled

        if not _debug_responses_enabled():
            to_strip = (*_QUERY_DEBUG_KEYS, *_API_DEBUG_KEYS)
        else:
            to_strip = (
                *(_QUERY_DEBUG_KEYS if not analyst_query_enabled else ()),
                *(_API_DEBUG_KEYS if not analyst_api_enabled else ()),
            )
        for k in to_strip:
            if k in data:
                data.pop(k)
                changed = True
    if pii_policy is not None:
        from backend.core.share_db.validation import apply_pii_policy

        masked = apply_pii_policy(data, pii_policy)
        if masked is not data:
            data = masked
            changed = True
    if not changed:
        return _rebuild_response(response, body, ct)
    new_body = json.dumps(data, separators=(",", ":")).encode()
    return _rebuild_response(response, new_body, ct, update_length=True)


_BODY_INSPECT_UNSET = object()
_BODY_INSPECT_ATTR = "_analyst_body_json"


async def _inspect_request_body(request: Request) -> dict | None:
    cached = getattr(request.state, _BODY_INSPECT_ATTR, _BODY_INSPECT_UNSET)
    if cached is not _BODY_INSPECT_UNSET:
        return cached  # type: ignore[return-value]
    parsed = await _drain_request_body(request)
    setattr(request.state, _BODY_INSPECT_ATTR, parsed)
    return parsed


async def _drain_request_body(request: Request) -> dict | None:
    method = request.method.upper()
    if method != "POST":
        return None
    ct = request.headers.get("content-type", "")
    if "application/json" not in ct.lower():
        return None
    receive = request._receive  # type: ignore[attr-defined]
    chunks: list[bytes] = []
    bytes_read = 0
    try:
        more_body = True
        while more_body:
            msg = await receive()
            if msg.get("type") != "http.request":
                return None
            chunk = msg.get("body", b"")
            chunks.append(chunk)
            bytes_read += len(chunk)
            if bytes_read > BODY_INSPECT_MAX_BYTES:
                break
            more_body = bool(msg.get("more_body", False))
    except Exception:
        return None
    body_bytes = b"".join(chunks)
    sent = False

    async def _replay():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request._receive = _replay  # type: ignore[attr-defined]
    if hasattr(request, "_body"):
        try:
            del request._body  # type: ignore[attr-defined]
        except AttributeError:
            pass
    if not body_bytes:
        return None
    try:
        body = json.loads(body_bytes)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(body, dict):
        return None
    return body


async def _body_service_ids(request: Request) -> list[str]:
    body = await _inspect_request_body(request) or {}
    return [str(v) for k in ("service_id", "service") if isinstance(v := body.get(k), (str, int)) and str(v)]


_PII_FORBIDDEN_FILTER_COLS = IP_FAMILY_KEYS | SESSION_ID_KEYS


async def _body_filter_keys(request: Request) -> list[str]:
    body = await _inspect_request_body(request) or {}
    filters = body.get("filters")
    return [str(k) for k in filters] if isinstance(filters, dict) else []


async def _body_field_param(request: Request) -> str | None:
    body = await _inspect_request_body(request) or {}
    field = body.get("field")
    return field if isinstance(field, str) else None


class _StaticAssetLimiter(SlidingWindowLimiter):
    def __init__(
        self,
        *,
        req_limit: int = 600,
        byte_limit: int = 50 * 1024 * 1024,
        window_s: int = 60,
        max_tracked_ips: int = 10_000,
    ) -> None:
        super().__init__(window_s=window_s, max_keys=max_tracked_ips)
        self.REQ_LIMIT = req_limit
        self.BYTE_LIMIT = byte_limit

    def check(self, ip: str, content_length: int) -> bool:
        return self.try_acquire(ip, content_length, max_events=self.REQ_LIMIT, max_weight=self.BYTE_LIMIT)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._locks.clear()


_static_limiter = _StaticAssetLimiter()
_analyst_api_limiter = _StaticAssetLimiter()


def _new_request_id() -> str:
    return secrets.token_hex(8)


def _error_json(status_code: int, error: str, **extra: object) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": error, **extra})


def _log_access(logger_name: str, fmt: str, *args: object) -> None:
    try:
        logging.getLogger(logger_name).info(fmt, *args)
    except Exception:
        logger.debug("dispatch: logging.getLogger failed (ignored)", exc_info=True)


def _check_rate_limit(request: Request, path: str) -> JSONResponse | None:
    content_len = int(request.headers.get("content-length") or "0")
    if path.startswith(("/_next/", "/static/")):
        if not _static_limiter.check(client_ip(request), content_len):
            return _error_json(429, "rate_limited")
    elif path.startswith("/api/"):
        ip = client_ip(request)
        if not _is_private_or_loopback(ip) and not _analyst_api_limiter.check(ip, content_len):
            return _error_json(429, "rate_limited")
    return None


async def _enforce_service_and_pii_scope(request: Request, session) -> JSONResponse | None:
    from backend import config as svcconfig

    raw_candidates: list[str] = list(_path_service_ids(request))
    for src in (
        request.query_params.get("service"),
        request.query_params.get("service_id"),
        request.headers.get("x-fastly-service-id"),
        request.headers.get("x-service-id"),
    ):
        if src:
            raw_candidates.append(src)
    raw_candidates.extend(await _body_service_ids(request))
    cdn_map = svcconfig.get_cdn_service_id_map() if raw_candidates else {}
    resolved_candidates = [cand if svcconfig.load_config(cand) else cdn_map.get(cand, cand) for cand in raw_candidates]
    if not resolved_candidates:
        fallback = svcconfig.get_active_service_id()
        if fallback:
            resolved_candidates.append(fallback)
    allowed_services = set(session.service_ids or [])
    if not resolved_candidates:
        return _error_json(403, "service_not_authorized", service="")
    for eff in resolved_candidates:
        if not eff or eff not in allowed_services:
            return _error_json(403, "service_not_authorized", service=eff or "")
    if session.pii_policy.get("mask_ips"):
        from backend.repositories.utils.filters import normalize_filter_key

        dim = await _body_field_param(request)
        raw_keys = [*(await _body_filter_keys(request)), *((dim,) if dim is not None else ())]
        for raw_key in raw_keys:
            col = normalize_filter_key(raw_key)
            if col in _PII_FORBIDDEN_FILTER_COLS:
                return _error_json(403, "pii_policy_violation", field=col)
    return None


class RemoteAccessMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method.upper()
        host_header = request.headers.get("host", "")
        gateway_authenticated = authenticated_admin_gateway(request)
        if ADMIN_GATEWAY_HEADER in request.headers and not gateway_authenticated:
            logger.warning("[remote_access] rejected invalid admin gateway credential")
            return _error_json(401, "admin_gateway_invalid")
        request.state.admin_gateway_authenticated = gateway_authenticated
        if gateway_authenticated:
            request.scope["headers"] = [
                (key, value)
                for key, value in request.scope["headers"]
                if key.lower() != ADMIN_GATEWAY_HEADER.lower().encode()
            ]
            if hasattr(request, "_headers"):
                del request._headers
        is_remote = is_request_remote(request)
        request.state.is_remote = is_remote
        request.state.analyst_session = None
        request.state.request_id = _new_request_id()
        mgr = get_tunnel_manager()
        if (is_remote and not _remote_host_allowed(host_header)) or (
            not is_remote and host_header and not _local_host_allowed(host_header)
        ):
            return _error_json(400, "host_not_allowed", host=host_header)
        if gateway_authenticated:
            origin = request.headers.get("origin")
            cross_site = request.headers.get("sec-fetch-site") == "cross-site"
            unsafe_origin = (
                method not in ("GET", "HEAD", "OPTIONS")
                and origin is not None
                and origin.lower() != f"https://{host_header.lower()}"
            )
            if cross_site or unsafe_origin:
                logger.warning("[remote_access] rejected cross-origin admin gateway request")
                return _error_json(403, "admin_origin_not_allowed")
        if not is_remote:
            _t0 = time.perf_counter()
            response = await call_next(request)
            _dur_ms = (time.perf_counter() - _t0) * 1000.0
            _log_access(
                "backend.access.admin",
                "[admin] [%s] rid=%s %s %s -> %d (%.1fms)",
                client_ip(request, default="127.0.0.1"),
                getattr(request.state, "request_id", "-"),
                method,
                path,
                response.status_code,
                _dur_ms,
            )
            return apply_response_hardening(response)
        if (rate_err := _check_rate_limit(request, path)) is not None:
            return rate_err
        if method not in ("GET", "HEAD") and not path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES):
            origin = request.headers.get("origin", "")
            if not _origin_allowed(origin):
                return _error_json(403, "origin_not_allowed", origin=origin)
        if path in _UNAUTH_ANALYST_PATHS or path.startswith(("/api/share/claim/", "/api/share/oauth/", "/js/")):
            response = await call_next(request)
            if path == "/api/bootstrap":
                sid_boot = request.cookies.get(ANALYST_SESSION_COOKIE)
                boot_session = mgr.validate_session(sid_boot) if sid_boot else None
                response = await _strip_analyst_envelope(response, analyst_session=boot_session, request=request)
            return apply_response_hardening(response)
        session = mgr.validate_session(request.cookies.get(ANALYST_SESSION_COOKIE))
        if session is None:
            return _error_json(401, "unauthenticated")
        if getattr(session, "tos_pending", False):
            return _error_json(403, "tos_pending")
        from backend.core.query_attribution import Attribution as _Attr
        from backend.core.query_attribution import current_attribution as _cur_attr

        _cur_attr.set(
            _Attr.analyst(
                analyst_id=getattr(session, "session_id", None) or "unknown",
                analyst_name=getattr(session, "name", None) or None,
                request_path=path,
                request_id=getattr(request.state, "request_id", None),
            )
        )
        headers_lc = {k.lower(): v for k, v in request.headers.items()}
        if compute_fingerprint(headers_lc) != session.fingerprint_signature:
            mgr.boot_session(session.session_id, reason="fingerprint mismatch")
            share_db.log_share_audit_event(
                event_type="FINGERPRINT_MISMATCH",
                email=session.email,
                ip_address=client_ip(request),
                details=f"path={path}",
            )
            return _error_json(401, "fingerprint_mismatch")
        if _is_blocked_path(path):
            return _error_json(403, "admin_only")
        if _is_sse_route(path) and not any(
            path == allowed
            or path.endswith(allowed)
            or (allowed.startswith("/api/") and path.startswith(f"{allowed}/"))
            for allowed in _ANALYST_SSE_ALLOWLIST
        ):
            return _error_json(403, "sse_blocked")
        if method in ("PUT", "PATCH", "DELETE") or (
            method == "POST" and not path.startswith(_ANALYST_ALLOWED_WRITE_PREFIXES)
        ):
            return _error_json(403, "read_only")
        if not (path.startswith("/api/share/") or path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES)):
            if (scope_err := await _enforce_service_and_pii_scope(request, session)) is not None:
                return scope_err
        current_ip = client_ip(request)
        if current_ip != session.ip_address:
            invite = share_db.get_remote_invite(session.invite_id)
            if invite and share_db.ip_in_whitelist(current_ip, invite.get("ip_whitelist")):
                mgr.touch_session(session.session_id, new_ip=current_ip, bump_active=False)
            else:
                return _error_json(403, "ip_not_whitelisted")
        request.state.analyst_session = session
        _idle_exempt = path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES) or _is_sse_route(path)
        _active_hdr = request.headers.get("x-user-active")
        _touched_idle = not _idle_exempt and _active_hdr != "0"
        if _touched_idle:
            mgr.touch_session(session.session_id, last_activity=f"{method} {path} act={_active_hdr or '-'}")
        _t0 = time.perf_counter()
        response = await call_next(request)
        _dur_ms = (time.perf_counter() - _t0) * 1000.0
        _log_access(
            "backend.access.analyst",
            "[analyst] %s (%s) [%s] rid=%s %s %s -> %d (%.1fms) act=%s idle_touch=%d",
            session.email,
            session.name or "no-name",
            client_ip(request),
            getattr(request.state, "request_id", "-"),
            method,
            path,
            response.status_code,
            _dur_ms,
            _active_hdr or "-",
            int(_touched_idle),
        )
        response = await _strip_analyst_envelope(response, analyst_session=session, request=request)
        apply_response_hardening(response)
        return response


from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import parse_iso_utc
from backend.utils.error_redaction import redact_error_text

MAX_ANALYST_QUERY_SPAN = timedelta(days=366)


@dataclass
class TimeBounds:
    start: datetime | None = None
    end: datetime | None = None

    def clamp(
        self, req_start: datetime | None, req_end: datetime | None, *, max_span: timedelta | None = None
    ) -> tuple[datetime, datetime]:
        candidates_s = [c for c in (req_start, self.start) if c is not None]
        candidates_e = [c for c in (req_end, self.end) if c is not None]
        eff_start = max(candidates_s) if candidates_s else None
        eff_end = min(candidates_e) if candidates_e else None
        if eff_end is None:
            eff_end = datetime.now(UTC)
        if eff_start is None:
            eff_start = eff_end - timedelta(hours=1)
        if max_span is not None and (eff_end - eff_start) > max_span:
            eff_start = eff_end - max_span
        if eff_start >= eff_end:
            raise ValueError("clamped time range is empty")
        return eff_start, eff_end


def _time_bounds_from_params(
    query_start_time: str | None,
    query_end_time: str | None,
    query_window_hours: int | None,
    *,
    now: datetime | None = None,
) -> TimeBounds:
    end = parse_iso_utc(query_end_time) if query_end_time else None
    start = parse_iso_utc(query_start_time) if query_start_time else None
    if query_window_hours:
        anchor = now if now is not None else datetime.now(UTC)
        relative_start = anchor - timedelta(hours=int(query_window_hours))
        start = max(start, relative_start) if start else relative_start
        end = min(end, anchor) if end else anchor
    return TimeBounds(start=start, end=end)


def get_analyst_time_bounds(request: Request) -> TimeBounds:
    session = getattr(request.state, "analyst_session", None)
    if session is None:
        return TimeBounds()
    return _time_bounds_from_params(session.query_start_time, session.query_end_time, session.query_window_hours)


def analyst_clamp_cache_key(
    query_start_time: str | None, query_end_time: str | None, query_window_hours: int | None
) -> str:
    return f"{query_start_time or ''}|{query_end_time or ''}|{query_window_hours or ''}"


def resolve_analyst_insights_clamp(
    query_start_time: str | None,
    query_end_time: str | None,
    query_window_hours: int | None,
    *,
    baseline_hours: float,
    window_hours: float,
    now: datetime | None = None,
) -> tuple[str | None, str | None, str]:
    anchor = now if now is not None else datetime.now(UTC)
    tb = _time_bounds_from_params(query_start_time, query_end_time, query_window_hours, now=anchor)
    earliest = anchor - timedelta(hours=baseline_hours + window_hours)
    start, end = tb.clamp(earliest, anchor, max_span=MAX_ANALYST_QUERY_SPAN)
    cache_key = analyst_clamp_cache_key(query_start_time, query_end_time, query_window_hours)
    return start.isoformat(), end.isoformat(), cache_key


def clamp_or_400(
    tb: TimeBounds, req_start: str | None, req_end: str | None, *, analyst_session: object | None = None
) -> tuple[str | None, str | None]:
    if analyst_session is None and req_start is None and req_end is None:
        return None, None
    max_span = MAX_ANALYST_QUERY_SPAN if analyst_session is not None else None
    try:
        start, end = tb.clamp(
            parse_iso_utc(req_start) if req_start else None,
            parse_iso_utc(req_end) if req_end else None,
            max_span=max_span,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail={"error": redact_error_text(exc), "time_range_empty": True}
        ) from exc
    return start.isoformat(), end.isoformat()
