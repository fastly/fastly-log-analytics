"""Remote analyst access middleware, gateway authentication, and time-window clamping."""

from __future__ import annotations

import hmac
import ipaddress
import json
import logging
import os
import re
import secrets
import sys
import time
from collections.abc import MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from backend.core import share_db
from backend.core.share_db import (
    CMCD_SESSION_ALIAS_KEYS,
    EXTRA_IP_PII_KEYS,
    SAVED_VIEW_PII_FILTER_KEYS,
    _canon_pii_key,
    _extract_saved_view_filter_keys,
    is_saved_view_pii_key,
)
from backend.utils.auth import (
    ANALYST_PENDING_SESSION_COOKIE,
    ANALYST_SESSION_COOKIE,
    analyst_allowed_services,
    analyst_cookie_is_secure,
    mask_ips_for,
    require_owned_row,
    require_service_in_scope,
    safe_audit_email,
    set_analyst_session_cookies,
)
from backend.utils.date_utils import parse_iso_utc
from backend.utils.pii_masking import redact_error_text
from backend.utils.rate_limit import (
    SlidingWindowLimiter,
    SlidingWindowRateLimiter,
    _StaticAssetLimiter,
    normalize_rate_limit_ip,
)
from backend.utils.tunnel import compute_fingerprint, get_tunnel_manager

logger = logging.getLogger(__name__)

ADMIN_GATEWAY_HEADER = "X-Admin-Gateway-Token"
ALT_ADMIN_GATEWAY_HEADER = "X-FLA-Admin-Gateway"
_UNTRUSTED_PROXY_HEADERS = ("x-proxied-by-caddy", "x-fla-public-gateway", "x-fla-frontend-proxy")


def admin_gateway_required() -> bool:
    return os.getenv("ADMIN_GATEWAY_REQUIRED", "") == "1"


def validate_admin_gateway_config() -> None:
    if os.getenv("ADMIN_GATEWAY_REQUIRED", "") not in ("", "0", "1"):
        raise ValueError("ADMIN_GATEWAY_REQUIRED must be 0 or 1")
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    if (admin_gateway_required() or secret) and len(secret) < 32:
        raise ValueError("ADMIN_GATEWAY_SECRET must contain at least 32 characters for admin gateway access")


def _header_values(headers: object, name: str) -> list[str]:
    if headers is None:
        return []
    getlist = getattr(headers, "getlist", None)
    if callable(getlist):
        return [str(v) for v in getlist(name)]
    if isinstance(headers, dict):
        target = name.lower()
        return [str(v) for k, v in headers.items() if str(k).lower() == target]
    get_fn = getattr(headers, "get", None)
    if callable(get_fn):
        val = get_fn(name)
        return [str(val)] if val is not None else []
    return []


def _has_admin_gateway_header(request: Request) -> bool:
    headers = getattr(request, "headers", None)
    return bool(_header_values(headers, ADMIN_GATEWAY_HEADER) or _header_values(headers, ALT_ADMIN_GATEWAY_HEADER))


def authenticated_admin_gateway(request: Request) -> bool:
    state = getattr(request, "state", None)
    if getattr(state, "admin_gateway_authenticated", False):
        return True
    headers = getattr(request, "headers", None)
    if headers is None:
        return False
    for marker in _UNTRUSTED_PROXY_HEADERS:
        if _header_values(headers, marker):
            return False
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    if len(secret) < 32:
        return False
    values = _header_values(headers, ADMIN_GATEWAY_HEADER) + _header_values(headers, ALT_ADMIN_GATEWAY_HEADER)
    if len(values) != 1:
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
    "/metrics",
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
)
_ANALYST_BLOCKED_SUBPATH_REGEX: tuple[re.Pattern[str], ...] = (
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


def _is_loopback_ip(ip_str: str) -> bool:
    try:
        return ipaddress.ip_address(ip_str.strip()).is_loopback
    except ValueError:
        return False


def _install_raw_peer_capture() -> None:
    try:
        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
    except ImportError:
        return
    orig_call = ProxyHeadersMiddleware.__call__
    if getattr(orig_call, "_fla_raw_peer_wrapped", False):
        return

    async def _capturing_call(self, scope, receive, send):
        if isinstance(scope, dict) and scope.get("type") in ("http", "websocket"):
            raw_client = scope.get("client")
            if isinstance(raw_client, (tuple, list)) and raw_client and isinstance(raw_client[0], str):
                scope.setdefault("_raw_peer_ip", raw_client[0])
        return await orig_call(self, scope, receive, send)

    _capturing_call._fla_raw_peer_wrapped = True  # type: ignore[attr-defined]
    ProxyHeadersMiddleware.__call__ = _capturing_call  # type: ignore[method-assign]


_install_raw_peer_capture()


def _raw_peer_ip(request: Request, *, default: str = "") -> str:
    scope = getattr(request, "scope", None)
    if isinstance(scope, dict):
        raw = scope.get("_raw_peer_ip")
        if isinstance(raw, str) and raw:
            return raw
    return client_ip(request, default=default)


def _proxy_trust_allows_non_loopback() -> bool:
    raw_sources: list[str] = []
    for env_name in ("TRUSTED_PROXY_IPS", "UVICORN_FORWARDED_ALLOW_IPS", "FORWARDED_ALLOW_IPS"):
        val = os.environ.get(env_name, "").strip()
        if val:
            raw_sources.append(val)
    argv = sys.argv
    for idx, arg in enumerate(argv):
        if arg == "--forwarded-allow-ips" and idx + 1 < len(argv):
            cli_val = argv[idx + 1].strip()
            if cli_val:
                raw_sources.append(cli_val)
        elif arg.startswith("--forwarded-allow-ips="):
            cli_val = arg.partition("=")[2].strip()
            if cli_val:
                raw_sources.append(cli_val)
    for raw in raw_sources:
        for item in raw.split(","):
            entry = item.strip()
            if not entry:
                continue
            if entry == "*":
                return True
            try:
                if "/" in entry:
                    net = ipaddress.ip_network(entry, strict=False)
                    if not net.is_loopback:
                        return True
                elif not ipaddress.ip_address(entry).is_loopback:
                    return True
            except ValueError:
                return True
    return False


_METRICS_SCRAPE_CIDRS_ENV = "METRICS_SCRAPE_CIDRS"
_metrics_scrape_cidrs_cache: tuple[str, tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]] = ("", ())


def _metrics_scrape_networks() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    global _metrics_scrape_cidrs_cache
    raw = os.environ.get(_METRICS_SCRAPE_CIDRS_ENV, "")
    if raw == _metrics_scrape_cidrs_cache[0]:
        return _metrics_scrape_cidrs_cache[1]
    nets: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for part in raw.split(","):
        entry = part.strip()
        if not entry:
            continue
        try:
            net = ipaddress.ip_network(entry, strict=False)
        except ValueError:
            continue
        if net.is_private and not net.is_link_local:
            nets.append(net)
    _metrics_scrape_cidrs_cache = (raw, tuple(nets))
    return _metrics_scrape_cidrs_cache[1]


def is_allowed_metrics_scrape(request: Request) -> bool:
    state = getattr(request, "state", None)
    if getattr(state, "admin_gateway_authenticated", False) is True or authenticated_admin_gateway(request):
        return True
    if getattr(state, "analyst_session", None) is not None or getattr(state, "is_remote", False) is True:
        return False
    cookies = getattr(request, "cookies", None) or {}
    if cookies.get(ANALYST_SESSION_COOKIE) or cookies.get(ANALYST_PENDING_SESSION_COOKIE):
        return False
    headers = getattr(request, "headers", None)
    if _has_admin_gateway_header(request):
        return False
    for cookie_hdr in _header_values(headers, "cookie"):
        if ANALYST_SESSION_COOKIE in cookie_hdr or ANALYST_PENDING_SESSION_COOKIE in cookie_hdr:
            return False
    for hdr in (
        *_UNTRUSTED_PROXY_HEADERS,
        "x-remote-analyst",
        "fastly-client-ip",
        "true-client-ip",
        "forwarded",
        "x-real-ip",
        "x-forwarded-for",
    ):
        if _header_values(headers, hdr):
            return False
    host = _raw_peer_ip(request, default="")
    if not host:
        return False
    explicit_cidrs = bool(os.environ.get(_METRICS_SCRAPE_CIDRS_ENV, "").strip())
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    if ip.is_loopback:
        return True
    if explicit_cidrs:
        return any(ip in net for net in _metrics_scrape_networks())
    return ip.is_private and not ip.is_link_local


def is_request_remote(request: Request) -> bool:
    if authenticated_admin_gateway(request):
        return False
    state = getattr(request, "state", None)
    if not isinstance(request, Request):
        if getattr(state, "is_remote", False) is True or getattr(state, "analyst_session", None) is not None:
            return True
        if getattr(request, "client", None) is None and not bool(getattr(request, "headers", None)):
            return False
    if admin_gateway_required() or _has_admin_gateway_header(request):
        return True
    headers = getattr(request, "headers", None)
    for marker in (*_UNTRUSTED_PROXY_HEADERS, "fastly-client-ip", "true-client-ip", "forwarded", "x-real-ip"):
        vals = _header_values(headers, marker)
        if any(v.strip() for v in vals):
            return True
    ra_vals = _header_values(headers, "x-remote-analyst")
    if any(v.strip().lower() in ("1", "true") for v in ra_vals):
        return True
    xff_vals = [v.strip() for v in _header_values(headers, "x-forwarded-for") if v.strip()]
    if xff_vals:
        if len(xff_vals) > 1 or "," in xff_vals[0] or not _is_loopback_ip(xff_vals[0]):
            return True
        raw_peer = _raw_peer_ip(request, default="")
        if not _is_loopback_ip(raw_peer):
            return True
        host_vals = _header_values(headers, "host")
        req_host_base = _host_base(host_vals[0]) if host_vals else ""
        if _proxy_trust_allows_non_loopback() or (req_host_base and req_host_base in _allowed_remote_hosts()):
            return True
    if getattr(request, "client", None) is None:
        return True
    raw_peer_host = _raw_peer_ip(request, default="127.0.0.1")
    host = client_ip(request, default="127.0.0.1")
    if not _is_private_or_loopback(raw_peer_host) or not _is_private_or_loopback(host):
        return True
    return False


def client_ip(request: Request, *, default: str = "0.0.0.0") -> str:
    return request.client.host if request.client else default


def _host_base(host_header: str) -> str:
    if not host_header:
        return ""
    stripped = host_header.strip().lower()
    if stripped.startswith("["):
        end = stripped.find("]")
        return stripped[: end + 1] if end != -1 else stripped
    return stripped.split(":")[0]


def _in_local_allowlist(base: str) -> bool:
    return "*" in _LOCAL_HOST_ALLOWLIST or base in _LOCAL_HOST_ALLOWLIST


def _local_host_allowed(host_header: str) -> bool:
    base = _host_base(host_header)
    return bool(base) and (_is_private_or_loopback(base) or _in_local_allowlist(base))


def _allowed_remote_hosts() -> list[str]:
    mgr = get_tunnel_manager()
    pub_fn = getattr(mgr, "public_url", None)
    active_ep = pub_fn(sync_db=True) if callable(pub_fn) else None
    state_ep = getattr(getattr(mgr, "state", None), "public_endpoint", None)
    known_ep = getattr(mgr, "_known_public_endpoint", None)
    candidates: list[str] = []
    for ep in (active_ep, state_ep, known_ep):
        if isinstance(ep, str) and ep:
            parsed = urlparse(ep)
            hostname = parsed.hostname
            if hostname:
                formatted = f"[{hostname.lower()}]" if ":" in hostname else hostname.lower()
                if formatted not in candidates:
                    candidates.append(formatted)
    try:
        from backend import config

        for cfg in config.list_configs():
            rf = cfg.get("remote_frontend")
            if isinstance(rf, dict):
                for k in ("domain_name", "origin_host", "override_host"):
                    val = rf.get(k)
                    if isinstance(val, str) and val.strip():
                        hb = _host_base(val)
                        if hb and hb not in candidates:
                            candidates.append(hb)
    except Exception:
        logger.debug("_allowed_remote_hosts: step failed (ignored)", exc_info=True)
    return candidates


def _has_external_proxy_evidence(request: Request) -> bool:
    headers = getattr(request, "headers", None)
    for hdr in (
        "x-proxied-by-caddy",
        "x-fla-public-gateway",
        "x-fla-frontend-proxy",
        "fastly-client-ip",
        "true-client-ip",
        "forwarded",
        "x-real-ip",
        "x-forwarded-for",
    ):
        if _header_values(headers, hdr):
            return True
    peer = _raw_peer_ip(request, default="")
    if not peer:
        return False
    try:
        ip = ipaddress.ip_address(peer)
    except ValueError:
        return False
    if _header_values(headers, "x-remote-analyst"):
        return not ip.is_loopback
    return not (ip.is_loopback or ip.is_private)


def _remote_host_allowed(host_header: str, request: Request | None = None) -> bool:
    base = _host_base(host_header)
    if not base:
        return False
    if base in _allowed_remote_hosts():
        return True
    if request is None:
        return False
    return not _has_external_proxy_evidence(request) and _in_local_allowlist(base)


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
    re.compile(r"^/api/high-scale/services/([^/]+)(?:/|$)"),
    re.compile(r"^/api/query/schema/([^/]+)(?:/|$)"),
    re.compile(r"^/api/alerts/([^/]+)(?:/|$)"),
    re.compile(r"^/api/views/([^/]+)(?:/|$)"),
)


def _match_route_service_ids(routes: object, scope: MutableMapping[str, Any]) -> list[str]:
    from starlette.routing import Match

    for route in routes:  # type: ignore[attr-defined]
        eff_ctxs = getattr(route, "effective_route_contexts", None)
        if callable(eff_ctxs):
            for cand in eff_ctxs():
                match, child_scope = cand.matches(scope)
                if match == Match.FULL:
                    path_params = child_scope.get("path_params", {})
                    out = [path_params[k] for k in ("service_id", "service") if k in path_params]
                    if out:
                        return out
            continue
        sub_routes = getattr(route, "routes", None)
        if sub_routes:
            nested = _match_route_service_ids(sub_routes, scope)
            if nested:
                return nested
        match, child_scope = route.matches(scope)
        if match == Match.FULL:
            path_params = child_scope.get("path_params", {})
            out = [path_params[k] for k in ("service_id", "service") if k in path_params]
            if out:
                return out
            break
    return []


def _path_service_ids(request: Request) -> list[str]:
    routes = getattr(getattr(getattr(request, "app", None), "router", None), "routes", None)
    if routes:
        matched = _match_route_service_ids(routes, request.scope)
        if matched:
            return matched
    path = request.url.path
    return [m.group(1) for pat in _PATH_SERVICE_PATTERNS if (m := pat.match(path))]


def _is_sse_route(path: str) -> bool:
    if "/sse" in path:
        return True
    last_segment = path.rsplit("/", 1)[-1]
    return last_segment == "stream" or last_segment.endswith("-stream")


_HTML_CSP_DEFAULT = (
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
)
_API_CSP_DEFAULT = "default-src 'none'; frame-ancestors 'none'"


def apply_response_hardening(response: Response, *, path: str | None = None) -> Response:
    response.headers.setdefault("Cache-Control", "private, no-store")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
    is_api_path = bool(path and (path == "/api" or path.startswith("/api/")))
    response.headers.setdefault("Content-Security-Policy", _API_CSP_DEFAULT if is_api_path else _HTML_CSP_DEFAULT)
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
        from backend.core.share_db import apply_pii_policy

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
_RAW_BODY_ATTR = "_raw_body_bytes"
_BODY_TOO_LARGE_ATTR = "_body_too_large"


def _parse_content_length(request: Request) -> int | None:
    raw_vals = _header_values(getattr(request, "headers", None), "content-length")
    if not raw_vals:
        return 0
    vals = [v.strip() for v in raw_vals]
    if any(v == "" for v in vals):
        return None
    unique_vals = set(vals)
    if len(unique_vals) != 1:
        return None
    raw = next(iter(unique_vals))
    if not raw.isascii() or not raw.isdigit():
        return None
    try:
        parsed = int(raw)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


async def _drain_raw_body_bounded(request: Request, *, max_bytes: int = BODY_INSPECT_MAX_BYTES) -> bytes | None:
    cached_raw = getattr(request.state, _RAW_BODY_ATTR, _BODY_INSPECT_UNSET)
    if cached_raw is not _BODY_INSPECT_UNSET:
        return cached_raw  # type: ignore[return-value]
    receive = request._receive  # type: ignore[attr-defined]
    chunks: list[bytes] = []
    bytes_read = 0
    disconnected = False
    try:
        more_body = True
        while more_body:
            msg = await receive()
            msg_type = msg.get("type")
            if msg_type == "http.disconnect":
                disconnected = True
                break
            if msg_type != "http.request":
                setattr(request.state, _RAW_BODY_ATTR, None)
                return None
            chunk = msg.get("body", b"")
            chunks.append(chunk)
            bytes_read += len(chunk)
            if bytes_read > max_bytes:
                setattr(request.state, _BODY_TOO_LARGE_ATTR, True)
                setattr(request.state, _RAW_BODY_ATTR, None)
                return None
            more_body = bool(msg.get("more_body", False))
    except Exception:
        setattr(request.state, _RAW_BODY_ATTR, None)
        return None
    body_bytes = b"".join(chunks)
    sent = False

    async def _replay():
        nonlocal sent
        if sent:
            if disconnected:
                return {"type": "http.disconnect"}
            return await receive()
        sent = True
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request._receive = _replay  # type: ignore[attr-defined]
    if hasattr(request, "_body"):
        try:
            del request._body  # type: ignore[attr-defined]
        except AttributeError:
            pass
    setattr(request.state, _RAW_BODY_ATTR, body_bytes)
    return body_bytes


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
    body_bytes = await _drain_raw_body_bounded(request, max_bytes=BODY_INSPECT_MAX_BYTES)
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


_PII_FORBIDDEN_FILTER_COLS = SAVED_VIEW_PII_FILTER_KEYS | CMCD_SESSION_ALIAS_KEYS | frozenset({"origin_ip"})
_PII_FORBIDDEN_DIMENSION_COLS = _PII_FORBIDDEN_FILTER_COLS - frozenset({"oip"})


async def _body_filter_keys(request: Request) -> list[str]:
    body = await _inspect_request_body(request) or {}
    filters = body.get("filters")
    if isinstance(filters, dict):
        extracted, _ = _extract_saved_view_filter_keys(filters)
        return list({*(str(k) for k in filters), *extracted})
    if isinstance(filters, (list, str)):
        extracted, _ = _extract_saved_view_filter_keys(filters)
        return list(extracted)
    return []


async def _body_field_param(request: Request) -> str | None:
    body = await _inspect_request_body(request) or {}
    for key in ("field", "dimension"):
        val = body.get(key)
        if isinstance(val, str) and val:
            return val
    return None


async def _body_field_params(request: Request) -> tuple[list[str], list[str]]:
    body = await _inspect_request_body(request) or {}
    field_keys: list[str] = []
    dim_keys: list[str] = []
    singular_field = body.get("field")
    if isinstance(singular_field, str) and singular_field:
        field_keys.append(singular_field)
    plural_fields = body.get("fields")
    if isinstance(plural_fields, list):
        for item in plural_fields:
            if isinstance(item, str) and item:
                field_keys.append(item)
    singular_dim = body.get("dimension")
    if isinstance(singular_dim, str) and singular_dim:
        dim_keys.append(singular_dim)
    return field_keys, dim_keys


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


def _check_rate_limit(request: Request, path: str, content_len: int | None = None) -> JSONResponse | None:
    if content_len is None:
        content_len = _parse_content_length(request)
        if content_len is None:
            return _error_json(400, "invalid_content_length")
    ip_key = normalize_rate_limit_ip(client_ip(request))
    if path.startswith(("/_next/", "/static/")):
        if not _static_limiter.check(ip_key, content_len):
            return _error_json(429, "rate_limited")
    elif path.startswith("/api/"):
        if not _analyst_api_limiter.check(ip_key, content_len):
            return _error_json(429, "rate_limited")
    return None


def validate_analyst_request_binding(
    request: Request, session: object, *, mgr: object | None = None
) -> tuple[int, str] | None:
    if getattr(session, "tos_pending", False):
        return (403, "tos_pending")
    expected_fp = getattr(session, "fingerprint_signature", None)
    if expected_fp is not None:
        headers_lc = {k.lower(): v for k, v in request.headers.items()}
        if compute_fingerprint(headers_lc) != expected_fp:
            active_mgr = mgr or get_tunnel_manager()
            sid = getattr(session, "session_id", None)
            if sid:
                active_mgr.boot_session(sid, reason="fingerprint mismatch")  # type: ignore[attr-defined]
            share_db.log_share_audit_event(
                event_type="FINGERPRINT_MISMATCH",
                email=getattr(session, "email", None),
                ip_address=client_ip(request),
                details=f"path={request.url.path}",
            )
            return (401, "fingerprint_mismatch")
    session_ip = getattr(session, "ip_address", None)
    invite_id = getattr(session, "invite_id", None)
    if invite_id is not None:
        current_ip = client_ip(request)
        invite = share_db.get_remote_invite(invite_id)
        if invite is not None and not share_db.ip_in_whitelist(current_ip, invite.get("ip_whitelist")):
            return (403, "ip_not_whitelisted")
        if session_ip is not None and current_ip != session_ip:
            if invite is not None:
                active_mgr = mgr or get_tunnel_manager()
                sid = getattr(session, "session_id", None)
                if sid:
                    active_mgr.touch_session(sid, new_ip=current_ip, bump_active=False)  # type: ignore[attr-defined]
            else:
                return (403, "ip_not_whitelisted")
    return None


_EXTRA_FORBIDDEN_FILTER_COLS = EXTRA_IP_PII_KEYS


async def _enforce_service_and_pii_scope(request: Request, session) -> JSONResponse | None:
    from backend import config as svcconfig
    from backend.core.request_context import _valid_cookie_service_id

    raw_candidates: list[str] = list(_path_service_ids(request))
    for src in (
        request.query_params.get("service"),
        request.query_params.get("service_id"),
        request.headers.get("x-fastly-service-id"),
        request.headers.get("x-service-id"),
    ):
        if src:
            raw_candidates.append(src)
    body_sids = await _body_service_ids(request)
    if getattr(request.state, _BODY_TOO_LARGE_ATTR, False):
        return _error_json(413, "payload_too_large", max_bytes=BODY_INSPECT_MAX_BYTES)
    raw_candidates.extend(body_sids)
    if not raw_candidates:
        cookie_sid = _valid_cookie_service_id(request)
        if cookie_sid:
            raw_candidates.append(cookie_sid)
    caller_supplied = bool(raw_candidates)
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
            return _error_json(403, "service_not_authorized", service=(eff or "") if caller_supplied else "")
    if session.pii_policy.get("mask_ips"):
        from backend.repositories.utils.filters import normalize_filter_key

        field_keys, dim_keys = await _body_field_params(request)
        if getattr(request.state, _BODY_TOO_LARGE_ATTR, False):
            return _error_json(413, "payload_too_large", max_bytes=BODY_INSPECT_MAX_BYTES)
        query_filter_keys: list[str] = []
        raw_query_filters = request.query_params.get("filters")
        if raw_query_filters:
            qf_extracted, _ = _extract_saved_view_filter_keys(raw_query_filters)
            query_filter_keys.extend(qf_extracted)
        filter_and_field_keys = [*(await _body_filter_keys(request)), *query_filter_keys, *field_keys]
        for raw_key in filter_and_field_keys:
            col = normalize_filter_key(raw_key)
            canon = _canon_pii_key(raw_key)
            if (
                col in _PII_FORBIDDEN_FILTER_COLS
                or col in _EXTRA_FORBIDDEN_FILTER_COLS
                or canon in _PII_FORBIDDEN_FILTER_COLS
                or canon in _EXTRA_FORBIDDEN_FILTER_COLS
                or is_saved_view_pii_key(raw_key)
            ):
                return _error_json(403, "pii_policy_violation", field=col)
        for raw_dim in dim_keys:
            col = normalize_filter_key(raw_dim)
            canon = _canon_pii_key(raw_dim)
            if (
                col in _PII_FORBIDDEN_DIMENSION_COLS
                or col in _EXTRA_FORBIDDEN_FILTER_COLS
                or canon in _PII_FORBIDDEN_DIMENSION_COLS
                or canon in _EXTRA_FORBIDDEN_FILTER_COLS
                or (col != "oip" and canon != "oip" and is_saved_view_pii_key(raw_dim))
            ):
                return _error_json(403, "pii_policy_violation", field=col)
    return None


def _clear_rejected_analyst_cookies(
    request: Request,
    response: Response,
    *,
    mgr: object | None = None,
    clear_pending: bool = False,
) -> Response:
    active_mgr = mgr or get_tunnel_manager()
    pub_fn = getattr(active_mgr, "known_public_url", None) or getattr(active_mgr, "public_url", None)
    pub_url = pub_fn(sync_db=False) if callable(pub_fn) else None
    sec = analyst_cookie_is_secure(request, pub_url if isinstance(pub_url, str) else None)
    cookie_names = [ANALYST_SESSION_COOKIE]
    if clear_pending:
        cookie_names.append(ANALYST_PENDING_SESSION_COOKIE)
    for cname in cookie_names:
        if request.cookies.get(cname):
            response.set_cookie(
                key=cname,
                value="",
                max_age=0,
                path="/",
                httponly=True,
                secure=sec,
                samesite="strict",
            )
    return response


class RemoteAccessMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method.upper()
        host_header = request.headers.get("host", "")

        def _harden(resp: Response) -> Response:
            return apply_response_hardening(resp, path=path)

        gateway_authenticated = authenticated_admin_gateway(request)
        if _has_admin_gateway_header(request) and not gateway_authenticated:
            logger.warning("[remote_access] rejected invalid admin gateway credential")
            return _harden(_error_json(401, "admin_gateway_invalid"))
        request.state.admin_gateway_authenticated = gateway_authenticated
        if gateway_authenticated:
            stripped_names = {ADMIN_GATEWAY_HEADER.lower().encode(), ALT_ADMIN_GATEWAY_HEADER.lower().encode()}
            request.scope["headers"] = [
                (key, value) for key, value in request.scope["headers"] if key.lower() not in stripped_names
            ]
            if hasattr(request, "_headers"):
                del request._headers
        if (path.rstrip("/") or "/") == "/metrics":
            if not is_allowed_metrics_scrape(request):
                return _harden(_error_json(403, "admin_only"))
            request.state.is_remote = False
            request.state.analyst_session = None
            request.state.request_id = _new_request_id()
            response = await call_next(request)
            return _harden(response)
        is_remote = is_request_remote(request)
        request.state.is_remote = is_remote
        request.state.analyst_session = None
        request.state.request_id = _new_request_id()
        mgr = get_tunnel_manager()
        if (is_remote and not _remote_host_allowed(host_header, request)) or (
            not is_remote and host_header and not _local_host_allowed(host_header)
        ):
            return _harden(_error_json(400, "host_not_allowed", host=host_header))
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
                return _harden(_error_json(403, "admin_origin_not_allowed"))
        content_len = _parse_content_length(request)
        if content_len is None:
            return _harden(_error_json(400, "invalid_content_length"))
        enforce_body_cap = is_remote or path != "/api/admin/share/backup/import"
        if enforce_body_cap and content_len > BODY_INSPECT_MAX_BYTES:
            return _harden(_error_json(413, "payload_too_large", max_bytes=BODY_INSPECT_MAX_BYTES))
        if is_remote and (rate_err := _check_rate_limit(request, path, content_len)) is not None:
            return _harden(rate_err)
        if enforce_body_cap and method in ("POST", "PUT", "PATCH"):
            await _drain_raw_body_bounded(request, max_bytes=BODY_INSPECT_MAX_BYTES)
            if getattr(request.state, _BODY_TOO_LARGE_ATTR, False):
                return _harden(_error_json(413, "payload_too_large", max_bytes=BODY_INSPECT_MAX_BYTES))
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
            return _harden(response)
        if method not in ("GET", "HEAD") and not path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES):
            origin = request.headers.get("origin", "")
            if not _origin_allowed(origin):
                return _harden(_error_json(403, "origin_not_allowed", origin=origin))
        sharing_active = mgr.is_sharing_active()
        has_analyst_cookie = bool(
            request.cookies.get(ANALYST_SESSION_COOKIE) or request.cookies.get(ANALYST_PENDING_SESSION_COOKIE)
        )
        if not sharing_active and has_analyst_cookie and path not in ("/api/health", "/api/share/logout"):
            return _harden(
                _clear_rejected_analyst_cookies(
                    request,
                    _error_json(403, "sharing_disabled"),
                    mgr=mgr,
                    clear_pending=True,
                )
            )
        if path in _UNAUTH_ANALYST_PATHS or path.startswith(("/api/share/claim/", "/api/share/oauth/", "/js/")):
            boot_session = None
            sid_boot: str | None = None
            if path == "/api/bootstrap":
                sid_boot = request.cookies.get(ANALYST_SESSION_COOKIE)
                boot_session = mgr.validate_session(sid_boot) if sid_boot else None
                if boot_session is not None:
                    if (bind_err := validate_analyst_request_binding(request, boot_session, mgr=mgr)) is not None:
                        err_resp: Response = _error_json(bind_err[0], bind_err[1])
                        if bind_err[1] != "tos_pending":
                            err_resp = _clear_rejected_analyst_cookies(request, err_resp, mgr=mgr)
                        return _harden(err_resp)
                    request.state.analyst_session = boot_session
            response = await call_next(request)
            if path == "/api/bootstrap":
                response = await _strip_analyst_envelope(response, analyst_session=boot_session, request=request)
                if sid_boot and (boot_session is None or response.status_code in (401, 403)):
                    response = _clear_rejected_analyst_cookies(request, response, mgr=mgr)
            return _harden(response)
        session = mgr.validate_session(request.cookies.get(ANALYST_SESSION_COOKIE))
        if session is None:
            return _harden(_clear_rejected_analyst_cookies(request, _error_json(401, "unauthenticated"), mgr=mgr))
        if getattr(session, "tos_pending", False):
            return _harden(_error_json(403, "tos_pending"))
        from backend.core.request_telemetry import Attribution as _Attr
        from backend.core.request_telemetry import current_attribution as _cur_attr

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
            return _harden(_clear_rejected_analyst_cookies(request, _error_json(401, "fingerprint_mismatch"), mgr=mgr))
        if _is_blocked_path(path):
            return _harden(_error_json(403, "admin_only"))
        if _is_sse_route(path) and not any(
            path == allowed
            or path.endswith(allowed)
            or (allowed.startswith("/api/") and path.startswith(f"{allowed}/"))
            for allowed in _ANALYST_SSE_ALLOWLIST
        ):
            return _harden(_error_json(403, "sse_blocked"))
        if method in ("PUT", "PATCH", "DELETE") or (
            method == "POST" and not path.startswith(_ANALYST_ALLOWED_WRITE_PREFIXES)
        ):
            return _harden(_error_json(403, "read_only"))
        if not (path.startswith("/api/share/") or path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES)):
            if (scope_err := await _enforce_service_and_pii_scope(request, session)) is not None:
                return _harden(scope_err)
        current_ip = client_ip(request)
        invite = share_db.get_remote_invite(session.invite_id) if session.invite_id else None
        if invite is not None and not share_db.ip_in_whitelist(current_ip, invite.get("ip_whitelist")):
            return _harden(_clear_rejected_analyst_cookies(request, _error_json(403, "ip_not_whitelisted"), mgr=mgr))
        if current_ip != session.ip_address:
            if invite is not None:
                mgr.touch_session(session.session_id, new_ip=current_ip, bump_active=False)
            else:
                return _harden(
                    _clear_rejected_analyst_cookies(request, _error_json(403, "ip_not_whitelisted"), mgr=mgr)
                )
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
        return _harden(response)


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


__all__ = [
    "ADMIN_GATEWAY_HEADER",
    "ALT_ADMIN_GATEWAY_HEADER",
    "ANALYST_PENDING_SESSION_COOKIE",
    "ANALYST_SESSION_COOKIE",
    "BODY_INSPECT_MAX_BYTES",
    "MAX_ANALYST_QUERY_SPAN",
    "RemoteAccessMiddleware",
    "SlidingWindowLimiter",
    "SlidingWindowRateLimiter",
    "TimeBounds",
    "_ANALYST_ALLOWED_WRITE_PREFIXES",
    "_ANALYST_BLOCKED_PREFIXES",
    "_ANALYST_BLOCKED_RUM_SUFFIXES",
    "_ANALYST_BLOCKED_SCORING_SUFFIXES",
    "_ANALYST_BLOCKED_SUBPATHS",
    "_ANALYST_BLOCKED_SUBPATH_REGEX",
    "_ANALYST_SSE_ALLOWLIST",
    "_API_DEBUG_KEYS",
    "_BODY_INSPECT_ATTR",
    "_BODY_INSPECT_UNSET",
    "_LOCAL_ADMIN_CIDRS_ENV",
    "_LOCAL_HOST_ALLOWLIST",
    "_PATH_SERVICE_PATTERNS",
    "_PII_FORBIDDEN_FILTER_COLS",
    "_QUERY_DEBUG_KEYS",
    "_StaticAssetLimiter",
    "_TELEMETRY_IDLE_EXEMPT_PREFIXES",
    "_UNAUTH_ANALYST_PATHS",
    "_allowed_remote_hosts",
    "_analyst_api_limiter",
    "_body_field_param",
    "_body_filter_keys",
    "_body_service_ids",
    "_check_rate_limit",
    "_drain_request_body",
    "_enforce_service_and_pii_scope",
    "_error_json",
    "_host_base",
    "_in_local_allowlist",
    "_inspect_request_body",
    "_is_blocked_path",
    "_is_private_or_loopback",
    "_is_sse_route",
    "_local_admin_cidrs_cache",
    "_local_admin_networks",
    "_local_host_allowed",
    "_log_access",
    "_new_request_id",
    "_origin_allowed",
    "_path_service_ids",
    "_rebuild_response",
    "_remote_host_allowed",
    "_static_limiter",
    "_strip_analyst_envelope",
    "_time_bounds_from_params",
    "admin_gateway_required",
    "analyst_allowed_services",
    "analyst_clamp_cache_key",
    "analyst_cookie_is_secure",
    "analyst_debug_visibility",
    "apply_response_hardening",
    "authenticated_admin_gateway",
    "clamp_or_400",
    "client_ip",
    "get_analyst_time_bounds",
    "is_allowed_metrics_scrape",
    "is_request_remote",
    "mask_ips_for",
    "require_owned_row",
    "require_service_in_scope",
    "resolve_analyst_insights_clamp",
    "safe_audit_email",
    "set_analyst_session_cookies",
    "validate_admin_gateway_config",
    "validate_analyst_request_binding",
]
