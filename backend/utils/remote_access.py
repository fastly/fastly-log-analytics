"""Remote-analyst middleware + helpers.

Mounted in ``backend.main`` ahead of ``telemetry_middleware``. Lives in its
own module so the security tests can import the small helpers (is_remote
detection, IP extraction, hardening header set) without instantiating the
FastAPI app.

Per the plan:
- ``is_remote`` is socket-bound — a ``Host: tun-xyz.lhr.life`` header on a
  127.0.0.1 connection cannot promote a request into the remote branch.
- DNS rebinding gate validates ``Host`` against a strict whitelist per branch.
- Origin check on analyst-scope writes.
- Cookie + session lookup + fingerprint match.
- Service scope check.
- Time-bounds clamping is enforced at the route layer via ``get_analyst_time_bounds``.
- Hardening headers, Gzip with SSE skip, PII masking, static-asset rate limit,
  SSE allowlist.
"""

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


# ---------------------------------------------------------------------------
# Single in-process sliding-window limiter (folded from backend.utils.rate_limit)
# ---------------------------------------------------------------------------


class SlidingWindowLimiter:
    def __init__(
        self,
        *,
        window_s: float,
        max_keys: int,
        clock: Callable[[], float] | None = None,
    ) -> None:
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
        self,
        key: str,
        weight: int = 0,
        *,
        max_events: int | None = None,
        max_weight: int | None = None,
    ) -> bool:
        """Admit one event if the key is under ``max_events`` in the window."""
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            if max_events is not None and len(events) >= max_events:
                self._store(key, events, cutoff)
                return False
            events.append((now, max(0, int(weight))))
            self._store(key, events, cutoff)
            return max_weight is None or sum(w for _, w in events) <= max_weight

    def strike(self, key: str, *, threshold: int, lockout_s: float, max_locks: int | None = None) -> bool:
        """Record one failure; return True iff this call starts a NEW lockout."""
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
        """``(locked, remaining_seconds)``; an expired lockout is dropped."""
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
        """``(live_event_counts, remaining_lockout_seconds)`` with stale state pruned."""
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


# ---------------------------------------------------------------------------
# Shared analyst-scope helpers (folded from backend.utils.auth)
# ---------------------------------------------------------------------------

ANALYST_SESSION_COOKIE = "analyst_session_id"
ANALYST_PENDING_SESSION_COOKIE = "analyst_pending_session_id"


def analyst_cookie_is_secure(request: Request, public_url: str | None) -> bool:
    """Keep analyst bearer cookies secure when TLS terminates at a proxy."""
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
    """Issue the active or TOS-pending analyst session cookie and clear the counterpart."""
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
    """Bound + strip an unauth-supplied email before it reaches the audit log."""
    if not email:
        return "-"
    cleaned = "".join(c for c in email if c.isprintable())
    return cleaned[:254] or "-"


def analyst_allowed_services(request: Request) -> set[str] | None:
    """Return the set of service IDs the caller (analyst session) can see,
    or ``None`` for admin requests (no scope restriction).
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return None
    return set(analyst_session.service_ids or [])


def require_service_in_scope(request: Request, service_id: str | None) -> set[str] | None:
    """Raise 403 ``service_not_authorized`` when the caller's analyst scope
    excludes ``service_id``; return the allowed-service set (``None`` for
    admins).
    """
    allowed = analyst_allowed_services(request)
    if allowed is not None and service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": service_id},
        )
    return allowed


def require_owned_row(
    request: Request,
    service_id: str,
    row_id: str,
    fetch: Callable[[str, str], dict | None],
) -> dict | None:
    """Pre-mutation cross-tenant gate for a row addressed by ``(service_id, row_id)``."""
    allowed = require_service_in_scope(request, service_id)
    if allowed is None:
        return None
    existing = fetch(row_id, service_id)
    if existing and existing.get("service_id") not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": existing.get("service_id")},
        )
    return existing


def mask_ips_for(analyst_session: object | None) -> bool:
    """Return ``True`` iff the analyst's invite carries ``mask_ips``."""
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


# Response envelope fields that carry server-internal telemetry. Stripped
# Cap on how many bytes of a POST body the middleware will buffer to
# extract service_id/service for the scope check. Comfortably above the
# largest legitimate analytics payload (filter + query envelopes top out at
# a few KiB) but well below per-worker memory budget — so an authenticated
# attacker can't stream chunked bodies to OOM the worker.
BODY_INSPECT_MAX_BYTES = 4 * 1024 * 1024  # 4 MiB


# Paths that an analyst can always reach without a session (login, the static
# share-login bundle, heartbeat). The middleware short-circuits on these
# before doing the session lookup.
_UNAUTH_ANALYST_PATHS = {
    "/api/share/login",
    "/api/share/logout",
    "/api/share/heartbeat",
    "/api/share/acknowledge",
    # /tos is callable from the pending-cookie state (pre-TOS-acceptance) so
    # the middleware can't gate it on a full session — the handler validates
    # the pending or full cookie itself, mirroring /acknowledge.
    "/api/share/tos",
    # /auth-config drives which auth modes the unauth /share-login page renders
    # (passcode vs SSO providers); reached before any session exists.
    "/api/share/auth-config",
    "/api/health",
    # Bootstrap is callable without a session so the frontend can detect
    # is_remote_analyst=true and redirect anonymous remote visitors to
    # /share-login. The bootstrap endpoint itself manually validates the
    # session cookie and returns a stub response when absent.
    "/api/bootstrap",
    # Public RUM assets and beacon endpoints
    "/js/rum.js",
    "/js/faro-sdk.js",
    "/rum-beacon",
    "/api/web-vitals",
    "/api/ux-events",
}

# Path prefixes that are EXPLICITLY blocked for analysts even with a valid
# session. Admin surface, anything mutating provisioning, debug, and the
# operator-only usage/cost surface (H-1).
_ANALYST_BLOCKED_PREFIXES = (
    "/api/admin/",  # includes /api/admin/share/* — analyst can never reach admin tooling
    "/api/provision/",
    "/api/debug/",
    "/api/usage/",  # H-1: cost/billing/usage data is operator-only
    "/api/cron-runs",  # H-5: ingestion task history with absolute paths
    "/api/audit-logs",  # H-5: admin audit trail
    "/api/alerts",  # H-7: alerts surface is operator-only per directive
)

# Exact-path or path-with-query-string blocks for endpoints that live under an
# otherwise-permitted router but expose admin-only surface area. Matched via
# `path == p` OR `path.startswith(p + "?")` OR `path.startswith(p + "/")` so a
# bare segment like "/api/download" won't accidentally swallow a sibling such
# as "/api/download-foo". Each entry is the FULL path the route is mounted at.
_ANALYST_BLOCKED_SUBPATHS = (
    "/api/download",  # H-2: raw object download
    "/api/download-all",  # H-2: bulk raw object download
    "/api/download-folder",  # H-2: folder-level raw object download
    "/api/cron-schedule",  # H-3: exposes per-service cron cadence config
    "/api/sync-status",  # N-3: leaks ngwaf_workspace_id + active cron task state
    # L7: the OpenAPI surface leaks the full admin/provision/debug API map.
    # Blocked for analysts (401 unauth / 403 authed); admin reaches it over
    # loopback (not is_remote), as does the build-time codegen hook.
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
)

# Path-parameter-bearing endpoints to block for analysts. Each entry is a
# compiled regex matched with .fullmatch() against the URL path (no query
# string). Keep these surgical — every regex here must NOT accidentally match
# analyst-needed routes such as
# /api/services/{id}/scoring/{config,status,labels,sessions/...} which are
# handled by the scoring-suffix gate or are intentionally allowed.
_ANALYST_BLOCKED_SUBPATH_REGEX: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/services/[^/]+/lake-info$"),  # H-3: Iceberg/object-store layout
    re.compile(r"^/api/services/[^/]+/logging-settings(/.*)?$"),  # H-3: per-service logging cfg
    re.compile(
        r"^/api/services/[^/]+/log-fields$"
    ),  # H-3: per-service field map (catalog at /api/log-fields/catalog stays open)
    re.compile(r"^/api/services/[^/]+/custom-fields(/.*)?$"),  # H-6 + N-7: VCL schema list + export
    re.compile(r"^/api/services/[^/]+/log-field-audit$"),  # S-2: operator field-config disclosure
)

# Session-scoring sub-routes that are admin-only. The gate only fires for
# paths that contain "/scoring/" AND end with one of these suffixes, so
# analyst-needed reads like /scoring/labels, /scoring/sessions/<sid>/events,
# /scoring/top-flagged, /scoring/score-distribution, /scoring/compliance-
# breakdown, /scoring/health, /scoring/evaluation, /scoring/curves,
# /scoring/matrix-versions, /scoring/threshold-preview, /scoring/analytics
# stay reachable. (H-4)
#
# /threshold-preview is intentionally NOT gated: the operator's chosen
# threshold value is supplied by the CALLER as a query param, not returned,
# and the response payload (confusion-matrix counts at the given cutoff) is
# equivalent in sensitivity to /score-distribution + /compliance-breakdown
# which analysts already see. /threshold (without "-preview") IS gated
# because it returns the operator's persisted committed value.
_ANALYST_BLOCKED_SCORING_SUFFIXES: tuple[str, ...] = (
    "/config",
    "/status",
    "/audit",
    "/threshold",
    "/exclude-regex",
    "/exclude-regex/validate",
    "/enforce-status-code",
    "/enforce-threshold",  # N-5: operator's enforce decision; also a KV-ID-leak vector via outbound calls
    "/l2-enforce",  # operator's L2-enforcement opt-in; same enforce-decision + outbound-call sensitivity as /enforce-threshold
    "/matrix-versions",  # N-5: ML retrain history
    "/restore",  # matrix-versions/{version}/restore mutation
    "/retrain",  # ML retrain mutation
    "/rotate-key",  # secret key rotation mutation
    "/enable",  # SSE provisioning mutation
    "/disable",  # SSE teardown mutation
    "/dashboard",  # N-5: admin scoring dashboard (handler returned 400 to analyst, but block before reaching it)
    "/evaluation/per-reason",  # N-5: per-reason evaluation breakdown (same reasoning as /dashboard)
)

# RUM sub-routes that mutate provisioning or disclose the operator's pinned
# Faro Web SDK / enable-state configuration — same sensitivity class as the
# scoring-suffix gate above. Mirrors that gate's shape: any path containing
# "/rum/" AND ending with one of these suffixes is admin-only.
# /rum/status is intentionally NOT listed: the RUM page is analystVisible
# (frontend/components/AppLayout.tsx) and gates its entire body on this
# endpoint, so blocking it wholesale made the analyst RUM page permanently
# dead (F1 audit finding). The route itself (backend/routers/rum.py)
# projects the response down to {enabled, enabled_at} for an analyst
# caller — the analyst-safe sibling shape of /api/log-extents vs
# /api/sync-status — and keeps deployed_vcl_sha / current_vcl_sha /
# vcl_drift admin-only. /rum/beacon-health, /rum/analytics, and
# /rum/live-events are also NOT listed — they're read-only beacon
# telemetry with no operator-config disclosure, same sensitivity class as
# the scoring analytics reads that stay open per the comment on
# _ANALYST_BLOCKED_SCORING_SUFFIXES.
_ANALYST_BLOCKED_RUM_SUFFIXES: tuple[str, ...] = (
    "/enable",  # mutates deployed edge configuration
    "/disable",  # mutates deployed edge configuration
    "/versions",  # discloses the operator's pinned Faro Web SDK version
    "/upgrade",  # mutates deployed edge configuration
    "/settings",  # mutates RUM capture settings and re-deploys rum-tracker.js / Fastly VCL
)

# POST/PUT/PATCH/DELETE paths that analysts CAN reach despite the read-only
# gate, because the verb-based check is a blunt instrument: most of this
# app's read endpoints use POST so they can accept JSON filter bodies. List
# the routers whose POST endpoints are confirmed read-only queries.
# Anything not in this allowlist falls through to the read-only block —
# so alerts CRUD, views CRUD, services CRUD all stay denied (correct;
# analysts don't author dashboard config).
# Match by path.startswith() so we cover both exact paths (e.g.,
# /api/insights, /api/query) and sub-paths (e.g., /api/sessions/detail).
_ANALYST_ALLOWED_WRITE_PREFIXES = (
    "/api/share/",  # login/logout/acknowledge/heartbeat
    "/api/dashboard/",  # /aggregates, /raw/csv, /field-values
    "/api/security/",  # /aggregates, /top-bots
    "/api/origin/",  # /summary, /timeseries, /slow-urls, etc.
    "/api/performance/",  # /aggregates
    "/api/insights",  # POST /api/insights (no trailing slash — exact path)
    "/api/network-health",  # POST /api/network-health
    "/api/network-quality",  # POST /api/network-quality
    "/api/network/",  # POST /api/network/bundle
    "/api/query",  # POST /api/query
    "/api/sessions",  # POST /api/sessions and /api/sessions/detail
    "/api/cmcd/",  # POST /api/cmcd/aggregates — streaming read-only query
    "/api/value/",  # POST /api/value/summary — service summary read-only query
    "/api/web-vitals",  # POST /api/web-vitals — browser perf beacon (no PII)
    "/api/charts/",
    "/api/ux-events",  # POST /api/ux-events — DataTable column reorders + sibling UX signals
    "/api/assets/",  # POST /api/assets/aggregates — assets/shield read-only query
    "/api/high-scale/",  # POST high-scale request-fact queries are read-only
)

# Pure fire-and-forget telemetry beacons. These can fire from a backgrounded
# or idle tab (web-vitals flushes deltas on visibilitychange; ux-events on
# stray interactions), so they must NOT count as user activity for the analyst
# idle-timeout — otherwise a left-open tab that keeps beaconing would hold a
# session open indefinitely. They're still validated + service-scoped like any
# analyst request; we only skip the ``last_active_time`` bump (see the
# touch_session call in the middleware below). The 24h absolute cap and 2h
# idle-on-real-activity still apply.
_TELEMETRY_IDLE_EXEMPT_PREFIXES = ("/api/web-vitals", "/api/ux-events")

# SSE routes that ARE allowed for analysts. New SSE routes default to *off* for
# analysts; an explicit add here is the only way to expose one.
_ANALYST_SSE_ALLOWLIST: set[str] = {
    # Header-badge push channel. Projected payload (latest_log_at,
    # local_rows) is the analyst-safe sibling of /api/sync-status/stream
    # in the same way /api/log-extents is the analyst-safe sibling of
    # /api/sync-status — no ngwaf_workspace_id, no active_run, just the
    # two badge fields. Lets analysts see real-time "Latest Log: Xs ago"
    # updates matching the admin view.
    "/api/log-extents/stream",
    # Control Room's live metrics feed (backend/routers/control_room.py,
    # /api/services/{service_id}/realtime-stream — service_id makes this
    # one path-suffix, not an exact match; see the endswith check below).
    # S-1 decision: exposes aggregate metrics (rps, error rate, cache
    # ratio) — no PII, no infra details — so Control Room can be
    # analyst-visible. Before the _is_sse_route hyphenated-suffix fix,
    # this route silently bypassed the SSE gate entirely rather than
    # being deliberately allowed; this entry makes that allowance
    # explicit instead of accidental.
    "/realtime-stream",
    # Control Room's bounded recent 4xx/5xx request-event feed. Payloads are
    # analyst-safe because client IPs are masked in the router.
    "/control-room/error-stream",
}

# Local "is this a real LAN hostname" allowlist; admins can extend via env.
# ``testserver`` is starlette.testclient.TestClient's default Host header.
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


# Admins can extend the local host allowlist via comma-separated hostnames in env:
# e.g., LOCAL_HOSTS=backend,frontend,my-custom-service
_env_hosts = os.getenv("LOCAL_HOSTS") or os.getenv("LOCAL_HOST_ALLOWLIST") or os.getenv("ALLOWED_HOSTS")
if _env_hosts:
    for _h in _env_hosts.split(","):
        _clean_h = _h.strip().lower()
        if _clean_h:
            _LOCAL_HOST_ALLOWLIST.add(_clean_h)


import ipaddress

# LOCAL_ADMIN_CIDRS: explicit, default-OFF widening of the admin trust
# boundary for containerized topologies. On the single-VM prod deploy the
# admin boundary is loopback (SSH tunnel / Caddy on the same host), but in
# the multipod compose (and eventually k8s) NOTHING arrives over loopback —
# the operator's browser lands as the docker gateway IP and the frontend's
# SSR as its container IP, so both were misclassified as remote analysts.
# The operator declares the ISOLATED infra network (e.g. 172.28.0.0/16, the
# fixed compose subnet whose only ingress is a 127.0.0.1-bound Caddy) as
# admin-trusted. NEVER set this to a subnet that untrusted clients can
# source from; 0.0.0.0/0 (and ::/0) are rejected outright.
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
        # Refuse anything that isn't a NARROW, private infra segment. This
        # guard exists to make the two historical bypass classes
        # unconfigurable: a broad RFC1918 range (VPN analyst promoted to
        # admin — e.g. 10.0.0.0/8) and link-local (169.254.169.254 cloud
        # metadata SSRF-as-admin). 2**16 still admits a /16 compose/pod
        # subnet, which is as wide as an isolated infra network ever needs.
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
    """Check if the provided IP or hostname is loopback, an operator-declared
    local-admin network member, or a local-test stub.

    The original implementation treated ANY RFC1918 / link-local IP as
    "local admin" — which broke down for real users coming in from a
    private corporate network (10.x, 172.16/12, 192.168.x). A remote
    analyst behind a VPN would be misclassified as an admin and bypass
    the analyst-blocked endpoint prefixes (``/api/provision/``,
    ``/api/admin/`` etc.) entirely. Even worse, an SSRF probe of
    ``169.254.169.254`` (cloud metadata service — same IP on AWS, GCE,
    Azure) would land as "local" too.

    Production topology: Caddy connects to uvicorn over loopback
    (127.0.0.1, host network mode + ``--forwarded-allow-ips=127.0.0.1``)
    so the only legitimate "this is the admin / TestClient" peer is
    loopback. Keep ``is_loopback`` and the literal-stub set; drop the
    over-broad ``is_private`` rule. Containerized topologies opt in to a
    NARROW extra trust segment via ``LOCAL_ADMIN_CIDRS`` (see above) —
    default empty, so prod behavior is unchanged.

    Function name is retained for backwards compatibility with the rest
    of remote_access.py — callers see no signature change.
    """
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        # Hostnames or test client stub names (e.g. "testclient", "localhost")
        return ip_str in ("testclient", "localhost")
    if ip.is_loopback:
        return True
    return any(ip in net for net in _local_admin_networks())


def is_request_remote(request: Request) -> bool:
    """Decide whether this request is from a remote analyst.

    An authenticated mTLS gateway credential selects the admin branch.
    Required gateway mode disables the legacy socket-based admin branch.

    Production topology:
      Fastly edge → Caddy on this VM → uvicorn on 127.0.0.1.
      Caddy rewrites X-Forwarded-For to the authoritative Fastly-Client-IP
      header (stripping any client-supplied XFF). uvicorn runs with
      ``--proxy-headers --forwarded-allow-ips=127.0.0.1`` so it populates
      ``request.client.host`` from XFF ONLY when the TCP peer is loopback.

    Therefore by the time the middleware sees a request:
      * ``request.client.host == "127.0.0.1"`` — direct loopback connection
        (admin SSH-tunnel, container-internal healthcheck, TestClient stub).
      * otherwise — Caddy-proxied request and the value is the real client IP.

    Host and asserted identity headers never select the administrator branch.
    The gateway credential is verified separately, not accepted as a marker.

    The ``X-Remote-Analyst: 1`` fallback is honored ONLY when the TCP peer is
    loopback AND tunnel sharing is active. This exists for two legitimate
    paths: (a) tests using starlette TestClient which always presents
    127.0.0.1 as the peer, and (b) future deployments where the analyst
    surface is served via a same-host proxy (e.g., the Next.js dev rewrite at
    localhost:3000 → localhost:8000). Direct admin connections never set this
    header, so the gate stays closed for them.
    """
    # No socket peer information at all (``request.client is None``). We cannot
    # classify the peer, so fail CLOSED toward the more-restrictive remote
    # branch (analyst gating applies) rather than treating an unknown peer as a
    # trusted local admin. Pre-fix, ``client_ip(default="127.0.0.1")`` made the
    # no-client case look like loopback and skipped the analyst firewall. In
    # prod this is unreachable (Caddy is the sole ingress and always populates
    # the peer — see ADR-03 / prod-network-topology), so this only hardens the
    # abnormal ASGI case; TestClient always presents a "testclient" peer.
    if authenticated_admin_gateway(request):
        return False
    if admin_gateway_required() or ADMIN_GATEWAY_HEADER in request.headers:
        return True
    if request.client is None:
        return True

    host = client_ip(request, default="127.0.0.1")

    # Caddy-proxied request: uvicorn has rewritten the peer to the real
    # client IP via --proxy-headers, so any non-loopback/non-private peer is
    # genuinely remote. ``_is_private_or_loopback`` also accepts the stub
    # values starlette TestClient uses ("testclient", "localhost") so tests
    # don't accidentally hit the remote branch.
    if not _is_private_or_loopback(host):
        return True

    # Loopback peer. Promote to remote ONLY if the explicit marker is set AND
    # tunnel sharing is actually live. Tunnel-sharing gating means a stale
    # header on a non-sharing instance can't toggle the branch.
    if request.headers.get("x-remote-analyst") == "1":
        mgr = get_tunnel_manager()
        if mgr.is_sharing_active():
            return True

    return False


def client_ip(request: Request, *, default: str = "0.0.0.0") -> str:
    """Return ``request.client.host`` if present, else ``default``.

    Centralises the ``... if request.client else "<marker>"`` pattern
    written 11+ times across the request-handling tree with 4 different
    no-client markers (``"0.0.0.0"``, ``"127.0.0.1"``, ``"unknown"``,
    ``"admin"``). Callers continue to pass the marker they need; the
    helper only collapses the conditional shape.

    Security: we never re-parse the X-Forwarded-For header ourselves —
    that was the bypass that made leftmost-XFF spoofing exploitable.
    With uvicorn running ``--proxy-headers --forwarded-allow-ips=127.0.0.1``
    the framework already populates ``request.client.host`` from XFF
    when the TCP peer is loopback (i.e. Caddy on this host); for all
    other peers, ``request.client.host`` IS the socket peer.
    """
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
    """Return True if the analyst is forbidden from reaching this path.

    Three layers, in order of cost:
      1. Prefix match against ``_ANALYST_BLOCKED_PREFIXES`` (admin/provision/
         debug/usage entire trees).
      2. Exact / sub-path match against ``_ANALYST_BLOCKED_SUBPATHS`` —
         endpoints that share a router with permitted paths and must be
         identified individually. Uses ``path == p`` OR ``startswith(p + "/")``
         OR ``startswith(p + "?")`` so a bare "/api/download" entry won't
         shadow a sibling like "/api/download-foo".
      3. Session-scoring suffix gate: any path that contains "/scoring/" AND
         ends with one of ``_ANALYST_BLOCKED_SCORING_SUFFIXES`` is admin-only.
         The "/scoring/" containment check keeps analyst-needed reads like
         /scoring/labels and /scoring/sessions/<sid>/events accessible.
      4. RUM suffix gate: same shape as #3 — any path that contains "/rum/"
         AND ends with one of ``_ANALYST_BLOCKED_RUM_SUFFIXES`` is
         admin-only. Keeps /rum/status, /rum/beacon-health, /rum/analytics,
         and /rum/live-events accessible (the route layer projects
         /rum/status down to an analyst-safe body).
      5. Regex match against ``_ANALYST_BLOCKED_SUBPATH_REGEX`` for routes
         that embed a path parameter (e.g. /api/services/{id}/lake-info).

    Trailing slashes are normalized before matching so an attacker cannot
    bypass the gate by requesting ``/api/services/{id}/scoring/config/`` or
    ``/api/services/{id}/lake-info/``. Starlette's ``redirect_slashes=True``
    default would issue a 307 to the canonical form, but the middleware
    runs BEFORE routing so the redirect can't help us — we have to strip
    the slash ourselves. Multiple trailing slashes are collapsed (rare in
    practice, but cheap to defend against).
    """
    # Normalize: strip one or more trailing slashes for matching, but keep
    # the root "/" path itself intact (it doesn't appear in any blocklist
    # and an analyst can always reach the SPA shell).
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


# Path-parameter patterns that carry a service ID. The middleware extracts the
# service from the URL path so that an analyst scoped to service A cannot reach
# /api/services/serviceB/scoring/status by relying on the active-service
# fallback in get_active_service_id() to satisfy the per-request scope check
# while the route handler reads the unrelated service_id from the path. See
# audit finding 006 for the desync vector.
#
# Each pattern captures group(1) as the candidate service_id token. The token
# may be either a logging service ID or a CDN service ID — the dispatcher
# resolves both shapes against svcconfig.get_cdn_service_id_map() before
# enforcing the analyst's allowlist.
_PATH_SERVICE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/services/([^/]+)(?:/|$)"),
    re.compile(r"^/api/alerts/([^/]+)(?:/|$)"),
    re.compile(r"^/api/views/([^/]+)(?:/|$)"),
)


def _path_service_ids(request: Request) -> list[str]:
    """Return every service-ID token embedded in the request path parameters.

    Instead of relying on fragile regex path matching which is prone to desync and bypass
    vulnerabilities, we leverage Starlette's actual router definitions to match the
    request scope and extract any path parameters that identify the service.
    """
    # 1. Primary robust approach: match request scope against the application's actual router routes
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

    # 2. Resilient fallback: regex-based path matching for backwards-compatibility or cases
    # where routing details aren't populated/available on request.app
    path = request.url.path
    return [m.group(1) for pat in _PATH_SERVICE_PATTERNS if (m := pat.match(path))]


def _is_sse_route(path: str) -> bool:
    if "/sse" in path:
        return True
    # Match the final path segment being exactly "stream" (the original
    # `endswith("/stream")` case) OR hyphen-suffixed with "-stream" (e.g.
    # "realtime-stream") — a route whose last segment is hyphen-joined
    # before "stream" doesn't end with the literal substring "/stream"
    # (the character before "stream" is "-", not "/"), so it was
    # previously never classified as SSE at all and silently bypassed
    # the analyst SSE allowlist's default-closed gate.
    last_segment = path.rsplit("/", 1)[-1]
    return last_segment == "stream" or last_segment.endswith("-stream")


def apply_response_hardening(response: Response) -> Response:
    """Set defensive response headers on both analyst and admin branches.

    Analyst path: Caddy overrides most of these with its own ``security_headers``
    snippet (Caddyfile §38–60), including a full Content-Security-Policy.
    Backend defaults are still a useful belt-and-braces in case the Caddy
    config is bypassed (loopback testing, future deployment changes).

    Admin path: SSH-tunneled uvicorn on :3001 skips Caddy entirely, so these
    headers are the ONLY hardening the admin browser sees. The CSP is
    split-directive (matches the Caddy-fronted analyst CSP shape) instead
    of the prior monolithic ``default-src 'self' 'unsafe-inline' data: blob:``.
    Per-directive scoping lets us keep ``'unsafe-inline'`` confined to
    script-src / style-src (where Next.js needs it for hydration) without
    granting it on connect-src / img-src / etc.

    COOP `same-origin` blocks cross-origin window opener references —
    closes the cross-origin-leak side channel and is required for browser
    process isolation guarantees.
    """
    response.headers.setdefault("Cache-Control", "private, no-store")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
    )
    # Split-directive CSP mirroring the Caddyfile analyst CSP shape. Each
    # directive scopes a single resource class so 'unsafe-inline' (needed
    # by Next.js for inline runtime hooks) is confined to script-src /
    # style-src and doesn't leak to connect-src or img-src.
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
    """Return ``(query_debug_enabled, api_call_debug_enabled)`` for remote analysts."""
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
    response: Response,
    analyst_session: object | None = None,
    request: Request | None = None,
) -> Response:
    """Remove server-internal telemetry keys from analyst-bound JSON bodies
    and apply per-invite PII masking (mask_ips) when configured.

    Catches both ``BaseResponse``-built payloads and ad-hoc dict responses
    that escape ``DEBUG_RESPONSES`` gating in ``backend/models/common.py``.
    Non-JSON responses and bodies that fail to parse pass through unchanged.
    """
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
    """Drain + replay + parse a JSON POST body ONCE, returning the parsed dict.

    Used by both the service-scope gate (``_body_service_ids``) and the
    analyst IP-filter lock (``_body_filter_keys``). Idempotent: the first call
    buffers the body, installs a single-shot replay on ``request._receive`` so
    downstream handlers still see the bytes, and caches the parsed value on
    ``request.state``. Subsequent calls return the cached value WITHOUT
    re-draining — the replay is single-shot, so a second drain would starve the
    downstream handler of its body.

    We can't use ``await request.body()`` here because Starlette's
    ``BaseHTTPMiddleware`` constructs a fresh Request for the inner app whose
    ``_body`` cache is independent — the downstream handler would then see an
    empty body. The replay-receive pattern is the documented workaround.

    Returns the dict for a JSON-object body, or ``None`` for non-POST /
    non-JSON / empty / non-dict / unparseable bodies.
    """
    cached = getattr(request.state, _BODY_INSPECT_ATTR, _BODY_INSPECT_UNSET)
    if cached is not _BODY_INSPECT_UNSET:
        return cached  # type: ignore[return-value]

    parsed = await _drain_request_body(request)
    setattr(request.state, _BODY_INSPECT_ATTR, parsed)
    return parsed


async def _drain_request_body(request: Request) -> dict | None:
    """One-shot drain/replay/parse helper backing ``_inspect_request_body``.

    Do not call directly — go through ``_inspect_request_body`` so the result
    is cached and the body is never drained twice.
    """
    method = request.method.upper()
    if method != "POST":
        return None
    ct = request.headers.get("content-type", "")
    if "application/json" not in ct.lower():
        return None
    # Drain the receive stream once, capture the body bytes.
    #
    # Bound the buffered body to BODY_INSPECT_MAX_BYTES so an authenticated
    # attacker can't stream an arbitrarily large request (Transfer-Encoding:
    # chunked) and OOM the worker. 4 MiB is comfortably above the largest
    # legitimate analytics payload (filter + query envelopes top out at a
    # few KiB) but well below the per-worker memory budget.
    receive = request._receive  # type: ignore[attr-defined]
    chunks: list[bytes] = []
    bytes_read = 0
    try:
        more_body = True
        while more_body:
            msg = await receive()
            if msg.get("type") != "http.request":
                # Disconnect or something unexpected — bail without replay
                # (downstream will see the same disconnect).
                return None
            chunk = msg.get("body", b"")
            chunks.append(chunk)
            bytes_read += len(chunk)
            if bytes_read > BODY_INSPECT_MAX_BYTES:
                # Stop accumulating; the partial body still gets replayed
                # so the downstream handler sees what we saw. The handler's
                # own request-body parsing will reject the truncated JSON
                # if the legitimate body was larger than 4 MiB.
                break
            more_body = bool(msg.get("more_body", False))
    except Exception:
        return None
    body_bytes = b"".join(chunks)

    # Install a single-shot replay so the downstream handler can re-read
    # the body. Subsequent calls return http.disconnect so a misbehaving
    # client that tries to stream more bytes doesn't hang forever.
    sent = False

    async def _replay():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request._receive = _replay  # type: ignore[attr-defined]
    # Also clear any pre-cached body on the Request object so a downstream
    # call to ``await request.body()`` reads from our replay.
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
    """Extract ``service_id``/``service`` from a JSON POST body, if any.

    Used by the service-scope gate so a forged ``service_id`` field in the
    request body is treated as a candidate and rejected when it doesn't
    match the analyst's authorized services. Closes M-3 (silent fallback
    on ``POST /api/dashboard/aggregates`` when the body service_id mismatches).
    """
    body = await _inspect_request_body(request) or {}
    # Accept str AND int: downstream FastAPI/Pydantic coerces
    # ``{"service_id": 12345}`` into the str field ``"12345"`` and would
    # execute the request for service 12345 — so the scope check has to
    # see the same coerced string or a forged-id-as-int bypasses it.
    return [str(v) for k in ("service_id", "service") if isinstance(v := body.get(k), (str, int)) and str(v)]


# Columns a masking analyst must never be able to filter on — the same PII
# families ``apply_pii_policy`` masks in responses (imported as the single
# source of truth, so the filter-lock and the response masker can't drift).
# Masking those values in the response is pointless if the analyst can still
# pivot the whole dataset by an exact value they are guessing at: an ``ip``
# filter is a presence oracle for a specific real client, and a
# ``cookie_session`` filter is the same oracle for a specific session hash
# (Phase-4 Track C).
_PII_FORBIDDEN_FILTER_COLS = IP_FAMILY_KEYS | SESSION_ID_KEYS


async def _body_filter_keys(request: Request) -> list[str]:
    """Return the raw filter keys from a JSON POST body's ``filters`` map.

    Backs the analyst IP-filter lock: the caller normalizes each key (via
    ``normalize_filter_key``) and rejects the request when a masking analyst
    filters on an IP-family column. Returns [] when there is no JSON body or
    no ``filters`` object.
    """
    body = await _inspect_request_body(request) or {}
    filters = body.get("filters")
    return [str(k) for k in filters] if isinstance(filters, dict) else []


async def _body_field_param(request: Request) -> str | None:
    """Return the top-level ``field`` string from a JSON POST body, if any.

    Backs the analyst PII lock for the field-values ENUMERATION surface
    (POST /api/dashboard/field-values ``{"field": "..."}``). A mask_ips analyst
    who cannot filter on a PII column must likewise not be able to enumerate its
    distinct values — even with the values masked, the per-value COUNT plus
    ``search`` prefix matching form an enumeration oracle. Returns None when
    there is no JSON body or no string ``field``. Body is already drained +
    cached by the earlier service-scope check, so this is a dict read.
    """
    body = await _inspect_request_body(request) or {}
    field = body.get("field")
    return field if isinstance(field, str) else None


# ── Sliding-window static-asset rate limiter (per IP) ───────────────────────


class _StaticAssetLimiter(SlidingWindowLimiter):
    """Per-IP sliding-window limiter: ``req_limit`` requests/min OR ``byte_limit`` bytes/min."""

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
# L6: separate per-IP budget for analyst API calls.
_analyst_api_limiter = _StaticAssetLimiter()


# ── The middleware ──────────────────────────────────────────────────────────


def _new_request_id() -> str:
    """Short app-level correlation id, independent of the OTel exporter.

    SRE-01/02: the per-request id stamped on ``request.state.request_id`` and
    persisted as ``Attribution.request_id`` into ``slow_queries`` — the join
    key that lets an operator pivot slow-request → its queries → who-ran-it.
    Deliberately *not* the OTel trace_id, which is invalid (uniformly blank)
    whenever ``OTEL_EXPORTER=none`` — the production default per ADR-08 §2.3.
    Minted in this outermost middleware so the inner telemetry middleware and
    the access-log lines below all share one value via ``scope["state"]``.
    8 bytes → 16 hex chars: collision-free at our request volume, short enough
    to eyeball in a log line.
    """
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
    # Static-asset rate limit.
    if path.startswith(("/_next/", "/static/")):
        if not _static_limiter.check(client_ip(request), content_len):
            return _error_json(429, "rate_limited")
    # L6: per-IP request-rate limit on analyst API calls (the static
    # limiter above covered only /_next/ + /static/). Backstops M1/M2 —
    # repeated compute-heavy /api/query or /api/insights from one source.
    # Only genuinely remote source IPs are limited: SSR-forwarded fetches
    # arrive over loopback (127.0.0.1) and would otherwise all share one
    # bucket and throttle server-rendering under load.
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
    # M-3: a forged service_id in the JSON body was silently ignored
    # before, with the handler falling back to the session-authorized
    # service. Promote it to a candidate so the scope check below
    # rejects mismatched bodies with the same 403 we'd return for
    # query/path mismatches.
    raw_candidates.extend(await _body_service_ids(request))

    cdn_map = svcconfig.get_cdn_service_id_map() if raw_candidates else {}
    resolved_candidates = [cand if svcconfig.load_config(cand) else cdn_map.get(cand, cand) for cand in raw_candidates]
    if not resolved_candidates:
        # No explicit service in the request — fall back to the active
        # default (preserves pre-fix behavior for analyst-facing GET
        # /api/dashboard etc. where the active service comes from the
        # session config).
        fallback = svcconfig.get_active_service_id()
        if fallback:
            resolved_candidates.append(fallback)

    allowed_services = set(session.service_ids or [])
    if not resolved_candidates:
        # No candidate could be derived — fail closed.
        return _error_json(403, "service_not_authorized", service="")
    for eff in resolved_candidates:
        if not eff or eff not in allowed_services:
            return _error_json(403, "service_not_authorized", service=eff or "")

    # PII lock: a masking analyst must never filter by an IP-family
    # column. Display masking is response-side only, so an un-blocked
    # `ip` filter would let an analyst probe for a specific real IP (a
    # presence oracle) or dead-end on zero rows. Reject at the boundary
    # — the frontend also hides the affordances, but THIS is the actual
    # guarantee. Reuses the SAME key normalization as the SQL WHERE
    # builder so prefixed / dedup-suffixed variants (filter_ip, ip_2,
    # xfilter_client_ip, …) can't slip past. Body is already drained +
    # cached by the service-scope check above, so this is a dict read.
    if session.pii_policy.get("mask_ips"):
        from backend.repositories.utils.filters import normalize_filter_key

        # Same lock for the field-values ENUMERATION dimension
        # (POST /api/dashboard/field-values {"field": "ip"}): enumerating
        # a PII column's distinct values — even masked — leaks per-value
        # counts + a search-prefix oracle, so reject it exactly like a
        # PII filter key. normalize_filter_key resolves junk-suffixed
        # variants ("ip.", "cookie_session ") to the real column.
        dim = await _body_field_param(request)
        raw_keys = [*(await _body_filter_keys(request)), *((dim,) if dim is not None else ())]
        for raw_key in raw_keys:
            col = normalize_filter_key(raw_key)
            if col in _PII_FORBIDDEN_FILTER_COLS:
                return _error_json(403, "pii_policy_violation", field=col)
    return None


class RemoteAccessMiddleware(BaseHTTPMiddleware):
    """Top-level firewall for remote-analyst traffic.

    Stub-friendly: when the tunnel manager reports no active sharing, the
    middleware is a near no-op (it still does the DNS-rebinding check on
    every request, which is cheap, but skips the analyst-session work).
    """

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
        # SRE-01: mint the correlation id up front (outermost middleware) so
        # the inner telemetry middleware reuses it for attribution and the
        # access-log lines below join on the same value. The telemetry
        # middleware may overwrite it with the OTel trace_id when an exporter
        # is wired; in the prod default (none) this minted id is the join key.
        request.state.request_id = _new_request_id()

        mgr = get_tunnel_manager()

        # DNS-rebinding gate. Always enforced — even when sharing is off the
        # local admin surface is exposed on 127.0.0.1, and a rebinding-shaped
        # Host header is never legitimate.
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
            # Pure-local request — no analyst gating. Hardening headers still
            # apply: the admin tunnel (uvicorn on :3001 over SSH) bypasses
            # Caddy entirely, so this middleware is the only thing that can
            # set X-Frame-Options / Permissions-Policy / a minimal CSP on
            # admin responses. Analyst branch headers are largely overridden
            # by Caddy's security_headers snippet (Caddyfile §38).
            _t0 = time.perf_counter()
            response = await call_next(request)
            _dur_ms = (time.perf_counter() - _t0) * 1000.0
            # Companion to the [analyst] log line below — surface admin
            # request activity with an [admin] tag so it's easy to grep
            # "who hit what" across both auth modes. SRE-02: carries the
            # correlation id (rid=) so a slow line here joins to its
            # slow_queries rows, and the latency (dur_ms) so the slow
            # request is identifiable from `docker logs` in the first place.
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

        # ── From here down, we're on the analyst path. ──

        if (rate_err := _check_rate_limit(request, path)) is not None:
            return rate_err

        # Origin gate for non-GET/HEAD writes.  Telemetry beacons
        # (sendBeacon) may arrive without an Origin header through CDN;
        # they're fire-and-forget, no mutation, no data return — CSRF
        # protection adds no value.
        if method not in ("GET", "HEAD") and not path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES):
            origin = request.headers.get("origin", "")
            if not _origin_allowed(origin):
                return _error_json(403, "origin_not_allowed", origin=origin)

        # Unauthenticated paths (login, heartbeat, health). The OAuth handshake
        # routes (/api/share/oauth/authorize + /callback) carry NO analyst
        # session cookie during the handshake — they ARE the pre-auth login
        # step — so they must be exempted here or they'd 401 before the handler
        # runs (design §3.4). They set the session cookie themselves on success.
        if path in _UNAUTH_ANALYST_PATHS or path.startswith(("/api/share/claim/", "/api/share/oauth/", "/js/")):
            response = await call_next(request)
            # /api/bootstrap is in _UNAUTH_ANALYST_PATHS so it short-circuits
            # here, BEFORE the analyst-envelope strip applied on the
            # authenticated path below. Its response still carries operator-
            # only telemetry — ``_is_cached`` always, plus ``_debug_queries`` /
            # ``_debug_calls`` when DEBUG_RESPONSES is enabled — which must not
            # reach a remote analyst. Run the same strip here. Scoped to
            # bootstrap so the cookie-setting /api/share/* responses (whose
            # Set-Cookie headers must survive verbatim) are left untouched.
            if path == "/api/bootstrap":
                sid_boot = request.cookies.get(ANALYST_SESSION_COOKIE)
                boot_session = mgr.validate_session(sid_boot) if sid_boot else None
                response = await _strip_analyst_envelope(response, analyst_session=boot_session, request=request)
            return apply_response_hardening(response)

        # All other analyst paths require a valid session.
        session = mgr.validate_session(request.cookies.get(ANALYST_SESSION_COOKIE))
        if session is None:
            return _error_json(401, "unauthenticated")
        if getattr(session, "tos_pending", False):
            return _error_json(403, "tos_pending")

        # SRE-08: as soon as the session is resolved, attribute the rest of
        # this analyst request's __global_share__ SQLite (the fingerprint-
        # mismatch audit DELETE/INSERT below, the IP-roaming invite lookup +
        # touch_session, and the activity touch_session) to THIS analyst.
        # RemoteAccessMiddleware runs OUTSIDE the telemetry middleware that
        # sets current_attribution inside call_next, so without this these
        # high-frequency reads register as "System: thread:..." in the Live
        # Query Monitor. The validate_session() reads above physically precede
        # session resolution and remain a structural limit (see SRE-08).
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

        # Fingerprint match.
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

        # Block admin / provision / debug surfaces.
        if _is_blocked_path(path):
            return _error_json(403, "admin_only")

        # SSE allowlist gate. Entries match either an exact path
        # (non-parameterized routes, e.g. "/api/log-extents/stream") or a
        # path suffix (a leading "/"-prefixed segment, for routes
        # parameterized by service_id, e.g. "/realtime-stream" matching
        # "/api/services/{service_id}/realtime-stream").
        if _is_sse_route(path) and not any(
            path == allowed
            or path.endswith(allowed)
            or (allowed.startswith("/api/") and path.startswith(f"{allowed}/"))
            for allowed in _ANALYST_SSE_ALLOWLIST
        ):
            return _error_json(403, "sse_blocked")

        # Read-only gate: refuse mutating verbs except on routes confirmed to
        # be read-only-via-POST (most dashboard/security/etc. queries POST
        # JSON filter bodies). See _ANALYST_ALLOWED_WRITE_PREFIXES for the
        # allowlist rationale.
        if method in ("PUT", "PATCH", "DELETE") or (
            method == "POST" and not path.startswith(_ANALYST_ALLOWED_WRITE_PREFIXES)
        ):
            return _error_json(403, "read_only")

        # Service-scope gate (skipped for system/session paths and
        # fire-and-forget telemetry beacons that carry no service context).
        # Collect every candidate the route handler might key off:
        #   - path params (/api/services/{sid}/..., /api/alerts/{sid}, /api/views/{sid})
        #   - query params (service, service_id)
        #   - headers (x-fastly-service-id, x-service-id)
        # Each is resolved via the cdn_service_id map (same as deps.get_service_id)
        # and the analyst's invite allowlist must cover ALL of them. Requiring
        # every candidate to be authorized closes audit finding 006: a request
        # with the analyst's allowed service in the query string and a different
        # service in the path was previously accepted because only the query
        # value was checked, and the route handler then used the path value.
        if not (path.startswith("/api/share/") or path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES)):
            if (scope_err := await _enforce_service_and_pii_scope(request, session)) is not None:
                return scope_err

        # IP-roaming: update without booting if whitelist still passes. This is
        # NOT user activity — bump_active=False so it doesn't reset the idle
        # clock. Critical for rotating-egress proxies (per-request NAT, e.g.
        # 167.82.x.x pools) where current_ip differs on nearly every request:
        # bumping here would pin the session alive forever, bypassing the
        # X-User-Active gate below.
        current_ip = client_ip(request)
        if current_ip != session.ip_address:
            invite = share_db.get_remote_invite(session.invite_id)
            if invite and share_db.ip_in_whitelist(current_ip, invite.get("ip_whitelist")):
                mgr.touch_session(session.session_id, new_ip=current_ip, bump_active=False)
            else:
                return _error_json(403, "ip_not_whitelisted")

        # Stamp the session into request.state for downstream code.
        request.state.analyst_session = session
        # Background machine traffic doesn't count as user activity for the
        # idle timer — otherwise a tab left open never idles out:
        #   - telemetry beacons (web-vitals / ux-events) from a backgrounded tab.
        #   - SSE push streams (e.g. the header-badge /api/log-extents/stream):
        #     fetch-based, they keep reconnecting even while the tab is hidden,
        #     re-stamping last_active_time every ~30s and pinning the session
        #     alive until the 24h absolute cap (the "still logged in next day"
        #     bug). The heartbeat probe is already unauth-exempt above and never
        #     reaches here, by design — it detects expiry, it must not prevent it.
        #   - automated react-query refetches on a FOREGROUND tab (e.g. the
        #     ~12s POST /api/dashboard/bundle the badge stream invalidates as
        #     logs ingest). The backend can't tell these from a user click, so
        #     the client stamps X-User-Active: 0 when no genuine gesture
        #     (mouse/keyboard/scroll) has happened recently. Absent or "1" still
        #     bumps the timer (back-compat: an old bundle keeps today's
        #     behavior); only an explicit "0" suppresses.
        # Only genuine user-initiated requests bump last_active_time.
        _idle_exempt = path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES) or _is_sse_route(path)
        _active_hdr = request.headers.get("x-user-active")  # "1" | "0" | None (old bundle)
        _touched_idle = not _idle_exempt and _active_hdr != "0"
        if _touched_idle:
            # Record the activity signal in last_activity (set before call_next,
            # so it survives a client-cancelled long request that never reaches
            # the post-response access log) — makes "what kept this session
            # alive, and was it flagged active" answerable straight from the DB.
            mgr.touch_session(session.session_id, last_activity=f"{method} {path} act={_active_hdr or '-'}")

        # Hand off.
        _t0 = time.perf_counter()
        response = await call_next(request)
        _dur_ms = (time.perf_counter() - _t0) * 1000.0

        # Per-analyst access log so admin can see who hit what. Sits
        # alongside uvicorn's default access log (which only shows IP).
        # Surface email + name + IP + path → trivial to grep by user.
        # SRE-02: rid= correlation id + (dur_ms) latency so a slow analyst
        # request is both findable in `docker logs` and joinable to the
        # slow_queries rows it spawned.
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

        # N-1 + N-10: strip server-internal telemetry envelope from analyst
        # responses (success AND error bodies). The handler-side
        # ``DEBUG_RESPONSES`` gate in BaseResponse covers the Pydantic path
        # but misses ad-hoc dict responses in admin routers and the
        # short-circuit JSONResponse error bodies, so we do a final pass
        # here on the buffered body. SSE responses (text/event-stream) are
        # passed through unchanged inside the helper.
        response = await _strip_analyst_envelope(response, analyst_session=session, request=request)

        # SSE-safe: don't add hardening headers to SSE streams in a way that
        # interferes; the keep-alive headers go on the route itself.
        apply_response_hardening(response)
        return response


# ── Time-bounds dependency (Section #21) ────────────────────────────────────


from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import parse_iso_utc
from backend.utils.error_redaction import redact_error_text

# L5: hard ceiling on the width of an analyst's clamped query window. The
# per-invite ``query_window_hours`` already bounds scoped invites; this is the
# backstop for an open-window invite (all ``query_*`` None) that would
# otherwise let an analyst request an arbitrarily wide range and scan the
# whole retained dataset. Generous (1 year) so it never bites a legitimate
# absolute-window invite — it only kills pathological multi-year spans. Admin
# requests are not capped.
MAX_ANALYST_QUERY_SPAN = timedelta(days=366)


@dataclass
class TimeBounds:
    """The session's effective query window. ``None`` means unrestricted."""

    start: datetime | None = None
    end: datetime | None = None

    def clamp(
        self,
        req_start: datetime | None,
        req_end: datetime | None,
        *,
        max_span: timedelta | None = None,
    ) -> tuple[datetime, datetime]:
        """Clamp a requested range against the session's allowed window.

        Returns ``(start, end)``. Raises ``ValueError`` if the clamped range
        is empty (the route should translate this to a 422 — see the contract).

        ``max_span`` (L5): a hard ceiling on the width of the returned window.
        An open-window invite (all ``query_*`` None) otherwise lets an analyst
        request an arbitrarily wide range and scan the entire retained dataset.
        When set and the clamped span exceeds it, the start is pulled forward
        to ``end - max_span`` (keep the most-recent slice). Only the analyst
        path passes this (see ``clamp_or_400``); admin requests are uncapped.
        """
        # Lower bound: max of (request start, session start, -inf).
        candidates_s = [c for c in (req_start, self.start) if c is not None]
        candidates_e = [c for c in (req_end, self.end) if c is not None]
        eff_start = max(candidates_s) if candidates_s else None
        eff_end = min(candidates_e) if candidates_e else None
        # Anchor to wall-clock floor if the caller passed nothing.
        if eff_end is None:
            eff_end = datetime.now(UTC)
        if eff_start is None:
            eff_start = eff_end - timedelta(hours=1)
        # L5: cap the maximum span (keep the most-recent ``max_span`` slice).
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
    """Build a :class:`TimeBounds` from raw invite/session window params.

    Shared by :func:`get_analyst_time_bounds` (the request path) and
    :func:`resolve_analyst_insights_clamp` (the insights prewarmer, which has
    no ``Request``). ``now`` defaults to wall-clock for the relative-window
    anchor; callers that need a fixed anchor (the prewarmer, tests) pass it.
    """
    end = parse_iso_utc(query_end_time) if query_end_time else None
    start = parse_iso_utc(query_start_time) if query_start_time else None
    if query_window_hours:
        anchor = now if now is not None else datetime.now(UTC)
        relative_start = anchor - timedelta(hours=int(query_window_hours))
        start = max(start, relative_start) if start else relative_start
        # Ceiling the upper bound to the anchor ("now"). Without this a rolling
        # invite's end stays None, so TimeBounds.clamp would adopt a caller-
        # supplied req_end verbatim — including a future one (e.g. a 60s-
        # quantized wire anchor that rounded up past now) — and widen the window
        # forward. Take the more-restrictive of an explicit end cap and the
        # anchor; the start floor above is unchanged, so no rows leak.
        end = min(end, anchor) if end else anchor
    return TimeBounds(start=start, end=end)


def get_analyst_time_bounds(request: Request) -> TimeBounds:
    """FastAPI dependency: returns the active session's clamp window.

    For non-analyst (local-admin) requests, returns an open ``TimeBounds`` —
    so existing analytics routes can declare the dependency unconditionally
    without changing behavior for the admin.
    """
    session = getattr(request.state, "analyst_session", None)
    if session is None:
        return TimeBounds()
    return _time_bounds_from_params(
        session.query_start_time,
        session.query_end_time,
        session.query_window_hours,
    )


def analyst_clamp_cache_key(
    query_start_time: str | None,
    query_end_time: str | None,
    query_window_hours: int | None,
) -> str:
    """Stable cache-key fragment for an analyst clamp shape.

    Keyed on the invite's window PARAMETERS (not the ``now``-resolved, rolling
    clamp bounds), so the ``/api/insights`` cache entry is reused across an
    invite's requests instead of recomputing on every call. Mirrors the admin
    path's time-independent key + TTL-staleness contract (see the cache key in
    ``backend/repositories/insights/repository.py``). The insights prewarmer
    computes the identical fragment for each active invite so it warms exactly
    the key a live analyst request will look up.
    """
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
    """Request-free analyst clamp resolver for the insights prewarmer.

    Returns ``(clamp_start_iso, clamp_end_iso, clamp_cache_key)`` for one
    analyst clamp shape over the given insights ``window_hours``/
    ``baseline_hours`` selection. Mirrors
    ``backend/routers/insights.py:_analyst_lookback_clamp`` (which uses
    ``ctx.clamp`` on the request path) so the prewarmer warms the exact key +
    bounds a live analyst request will look up. Propagates ``ValueError`` from
    :meth:`TimeBounds.clamp` when the shape's window is empty — the caller
    skips that shape.
    """
    anchor = now if now is not None else datetime.now(UTC)
    tb = _time_bounds_from_params(query_start_time, query_end_time, query_window_hours, now=anchor)
    earliest = anchor - timedelta(hours=baseline_hours + window_hours)
    start, end = tb.clamp(earliest, anchor, max_span=MAX_ANALYST_QUERY_SPAN)
    cache_key = analyst_clamp_cache_key(query_start_time, query_end_time, query_window_hours)
    return start.isoformat(), end.isoformat(), cache_key


def clamp_or_400(
    tb: TimeBounds,
    req_start: str | None,
    req_end: str | None,
    *,
    analyst_session: object | None = None,
) -> tuple[str | None, str | None]:
    """Clamp a request's start/end against the analyst's TimeBounds.

    Returns the clamped pair as ISO-8601 strings (or ``(None, None)`` for
    admin requests with no bounds passed — see admin-passthrough below).

    Admin pass-through: when ``analyst_session is None`` AND both
    ``req_start`` and ``req_end`` are ``None``, returns ``(None, None)``
    so the repo's own default range still applies. Without this short-
    circuit, ``TimeBounds().clamp(None, None)`` would force a now-1h..now
    window even for admin no-bounds requests — a behavior change we don't
    want.

    Analyst pass-through: analyst with both ``None`` bounds still gets
    clamped (and falls into ``TimeBounds.clamp``'s default now-1h..now,
    capped by any per-invite window). Anyone supplying explicit bounds
    is always clamped against the open or session-derived TimeBounds.

    Empty-window edge case: when the clamp resolves to an empty range
    (e.g., request fully outside the analyst's allowed window), raises
    ``HTTPException(400, {"error": ..., "time_range_empty": True})``.
    The 400 status mirrors the existing ``/api/sessions`` 7-day clamp
    precedent at ``backend/repositories/sessions.py`` (its ``ValueError``
    propagates through ``@query_errors()`` which maps to 400).
    """
    if analyst_session is None and req_start is None and req_end is None:
        return None, None
    # L5: cap the span for analysts only (backstop for open-window invites);
    # admin requests stay uncapped.
    max_span = MAX_ANALYST_QUERY_SPAN if analyst_session is not None else None
    try:
        start, end = tb.clamp(
            parse_iso_utc(req_start) if req_start else None,
            parse_iso_utc(req_end) if req_end else None,
            max_span=max_span,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": redact_error_text(exc), "time_range_empty": True},
        ) from exc
    return start.isoformat(), end.isoformat()
