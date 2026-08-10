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

import json
import logging
import re
import secrets
import time

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from backend.core import share_db
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS
from backend.utils.tunnel import compute_fingerprint, get_tunnel_manager

logger = logging.getLogger(__name__)

# Response envelope fields that carry server-internal telemetry. Stripped
# unconditionally from analyst-bound JSON bodies after call_next so that
# routes which build responses as plain dicts (bypassing BaseResponse's
# DEBUG_RESPONSES gate) cannot leak operator-side data — concrete examples
# the QA pass surfaced: raw DuckDB SQL via _debug_queries, Fastly KV store
# paths via _debug_calls, server cache state via _is_cached.
#
# Bare-name forms (``debug_queries``, ``debug_calls``) without the leading
# underscore are also stripped. Plain-dict responses in /api/query and
# /api/dashboard/bundle emit those forms and would otherwise leak the full
# DuckDB Iceberg view-resolution SQL to analysts.
#
# ``_section_timings`` (and the bare ``section_timings``) carries internal
# phase names (``summary``, ``timeseries``, ``temp_table_create``, …) without
# any data / SQL / infra identifiers — it's pure observability that's a
# force-multiplier for the next perf audit on the analyst path. Kept in the
# response in both forms.
_ANALYST_STRIPPED_ENVELOPE_KEYS = (
    "_debug_queries",
    "_debug_calls",
    "_debug_sqlite",
    "_is_cached",
    "debug_queries",
    "debug_calls",
    "debug_sqlite",
)

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
    "/enforce-status-code",
    "/enforce-threshold",  # N-5: operator's enforce decision; also a KV-ID-leak vector via outbound calls
    "/l2-enforce",  # operator's L2-enforcement opt-in; same enforce-decision + outbound-call sensitivity as /enforce-threshold
    "/matrix-versions",  # N-5: ML retrain history
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
    "/api/query",  # POST /api/query
    "/api/sessions",  # POST /api/sessions and /api/sessions/detail
    "/api/cmcd/",  # POST /api/cmcd/aggregates — streaming read-only query
    "/api/value/",  # POST /api/value/summary — service summary read-only query
    "/api/web-vitals",  # POST /api/web-vitals — browser perf beacon (no PII)
    "/api/charts/",
    "/api/web-vitals",  # POST /api/web-vitals — client perf telemetry
    "/api/ux-events",  # POST /api/ux-events — DataTable column reorders + sibling UX signals
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

import os

# ── Shared-secret admin gate (opt-in, defense-in-depth) ─────────────────────
#
# When ``ADMIN_SHARED_SECRET`` is set in the backend environment, admin-branch
# requests must carry it in the ``X-Admin-Token`` header. Bootstrap and health
# are exempt so the SPA can fetch the token + the loopback healthcheck stays
# unauthenticated. With the env var unset (the default) the gate is a no-op,
# so deploying this change without provisioning the secret can't lock anyone
# out of the admin tunnel.
#
# The trust boundary for the admin branch is loopback (Caddy on the same VM,
# or an SSH-tunneled localhost connection). The shared secret is the second
# factor: if the loopback boundary is ever bypassed (caddyfile mistake,
# direct uvicorn port exposure, container-network misconfig), the gate still
# refuses admin endpoints without the token.
ADMIN_TOKEN_HEADER = "X-Admin-Token"
_ADMIN_TOKEN_EXEMPT_PATHS = {
    "/api/health",
    "/api/bootstrap",
    # Telemetry endpoints sent via ``navigator.sendBeacon``, which
    # physically cannot carry custom request headers (the Beacon spec
    # restricts the API to a request body + content type). The admin-
    # branch caller (WebVitalsReporter / reportUxEvent) would otherwise
    # 401-loop on every page load. Analyst traffic passes via
    # ``_ANALYST_ALLOWED_WRITE_PREFIXES``; this exemption relaxes the
    # second-factor gate on the admin loopback branch.
    "/api/web-vitals",
    "/api/ux-events",
}


def _admin_shared_secret() -> str:
    """Return the configured admin shared secret, or empty string.

    Re-reads env on every call so tests can flip the env var via
    ``monkeypatch.setenv`` / ``delenv`` without forcing a module reload."""
    return (os.getenv("ADMIN_SHARED_SECRET") or "").strip()


# Admins can extend the local host allowlist via comma-separated hostnames in env:
# e.g., LOCAL_HOSTS=backend,frontend,my-custom-service
_env_hosts = os.getenv("LOCAL_HOSTS") or os.getenv("LOCAL_HOST_ALLOWLIST") or os.getenv("ALLOWED_HOSTS")
if _env_hosts:
    for _h in _env_hosts.split(","):
        _clean_h = _h.strip().lower()
        if _clean_h:
            _LOCAL_HOST_ALLOWLIST.add(_clean_h)


import ipaddress


def _is_private_or_loopback(ip_str: str) -> bool:
    """Check if the provided IP or hostname is loopback or a local-test stub.

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
    over-broad ``is_private`` rule.

    Function name is retained for backwards compatibility with the rest
    of remote_access.py — callers see no signature change.
    """
    try:
        ip = ipaddress.ip_address(ip_str)
        return ip.is_loopback
    except ValueError:
        # Hostnames or test client stub names (e.g. "testclient", "localhost")
        return ip_str in ("testclient", "localhost")


def is_request_remote(request: Request) -> bool:
    """Decide whether this request is from a remote analyst.

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

    We never trust the ``Host`` header or any other client-supplied header for
    this classification — the Host header was the source of the critical
    auth bypass.

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


def _local_host_allowed(host_header: str) -> bool:
    if not host_header:
        return False
    base = host_header.split(":")[0].lower()
    if _is_private_or_loopback(base):
        return True
    return base in _LOCAL_HOST_ALLOWLIST


def _remote_host_allowed(host_header: str) -> bool:
    mgr = get_tunnel_manager()
    state = mgr.state
    if not host_header:
        return False
    base = host_header.split(":")[0].lower()
    candidates: list[str] = []
    if state.public_endpoint:
        from urllib.parse import urlparse

        pe = urlparse(state.public_endpoint)
        if pe.hostname:
            candidates.append(pe.hostname.lower())
    return any(base == c for c in candidates)


def _origin_allowed(origin: str) -> bool:
    if not origin:
        return False
    from urllib.parse import urlparse

    parsed = urlparse(origin)
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    mgr = get_tunnel_manager()
    state = mgr.state
    if state.public_endpoint:
        pe = urlparse(state.public_endpoint)
        if pe.hostname and pe.hostname.lower() == host:
            return True
    return False


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
    out: list[str] = []

    # 1. Primary robust approach: match request scope against the application's actual router routes
    app = getattr(request, "app", None)
    if app and hasattr(app, "router") and hasattr(app.router, "routes"):
        from starlette.routing import Match

        for route in app.router.routes:
            match, child_scope = route.matches(request.scope)
            if match == Match.FULL:
                path_params = child_scope.get("path_params", {})
                for k in ("service_id", "service"):
                    if k in path_params:
                        out.append(path_params[k])
                break

    # 2. Resilient fallback: regex-based path matching for backwards-compatibility or cases
    # where routing details aren't populated/available on request.app
    if not out:
        path = request.url.path
        for pat in _PATH_SERVICE_PATTERNS:
            m = pat.match(path)
            if m:
                out.append(m.group(1))

    return out


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


async def _strip_analyst_envelope(response: Response, analyst_session: object | None = None) -> Response:
    """Remove server-internal telemetry keys from analyst-bound JSON bodies
    and apply per-invite PII masking (mask_ips) when configured.

    Catches both ``BaseResponse``-built payloads and ad-hoc dict responses
    (e.g. ``return {**result, "_debug_calls": get_tracked_calls()}`` in
    admin routers) that escape ``DEBUG_RESPONSES`` gating in
    ``backend/models/common.py``. The strip is keyed on the four envelope
    fields listed in ``_ANALYST_STRIPPED_ENVELOPE_KEYS``; non-JSON
    responses and bodies that fail to parse pass through unchanged.

    When ``analyst_session`` carries ``pii_policy.mask_ips=True`` the body
    is walked recursively and every ``ip`` / ``ip_address`` / ``client_ip``
    / ``remote_addr`` field is masked via ``apply_pii_policy`` (last-octet
    ``xxx`` for IPv4, last-80-bit zero for IPv6 — see
    ``backend/core/share_db/validation.py``). Streaming CSV responses
    (``/api/dashboard/raw/csv``) bypass this helper — they're not JSON —
    and must mask in the handler.

    Operators (loopback / TestClient) never reach this helper — the
    middleware only invokes it on the ``is_remote`` branch — so the
    debug panel on the admin UI keeps working.
    """
    ct = response.headers.get("content-type", "")
    if "application/json" not in ct:
        return response

    # Resolve PII policy upfront so we know whether to re-walk the body
    # even when no envelope keys are present.
    pii_policy: dict | None = None
    if analyst_session is not None:
        raw_policy = getattr(analyst_session, "pii_policy", None)
        if isinstance(raw_policy, dict) and raw_policy.get("mask_ips"):
            pii_policy = raw_policy

    body = b""
    # `body_iterator` only exists on StreamingResponse; the caller wraps a
    # plain Response in a StreamingResponse before calling this helper.
    async for chunk in response.body_iterator:  # type: ignore[attr-defined]
        body += chunk
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=ct,
        )
    changed = False
    if isinstance(data, dict):
        for k in _ANALYST_STRIPPED_ENVELOPE_KEYS:
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
        return Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=ct,
        )
    new_body = json.dumps(data, separators=(",", ":")).encode()
    new_headers = dict(response.headers)
    new_headers["content-length"] = str(len(new_body))
    return Response(
        content=new_body,
        status_code=response.status_code,
        headers=new_headers,
        media_type=ct,
    )


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
    body = await _inspect_request_body(request)
    if not isinstance(body, dict):
        return []
    out: list[str] = []
    for k in ("service_id", "service"):
        v = body.get(k)
        # Accept str AND int: downstream FastAPI/Pydantic coerces
        # ``{"service_id": 12345}`` into the str field ``"12345"`` and would
        # execute the request for service 12345 — so the scope check has to
        # see the same coerced string or a forged-id-as-int bypasses it.
        if isinstance(v, (str, int)):
            v_str = str(v)
            if v_str:
                out.append(v_str)
    return out


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
    body = await _inspect_request_body(request)
    if not isinstance(body, dict):
        return []
    filters = body.get("filters")
    if not isinstance(filters, dict):
        return []
    return [str(k) for k in filters]


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
    body = await _inspect_request_body(request)
    if not isinstance(body, dict):
        return None
    field = body.get("field")
    return field if isinstance(field, str) else None


# ── Sliding-window static-asset rate limiter (per IP) ───────────────────────


class _StaticAssetLimiter:
    """Per-IP sliding-window limiter: ``req_limit`` requests/min OR
    ``byte_limit`` bytes/min. Two instances exist — the static-asset budget
    (``/_next/`` + ``/static/``) and the analyst-API budget (L6) — each with
    its own counters so they don't share a bucket. (Name kept for the
    importing tests; it's a generic limiter.)

    Security: bound the in-memory ``_reqs`` / ``_bytes`` dicts so a
    high-cardinality IP attack (one request per source) cannot OOM the
    server by inflating the dicts indefinitely. The original implementation
    never evicted; an attacker with a botnet (or one that spoofed XFF before
    Phase 0 closed it) could pump ~50 bytes of memory per unique IP per
    minute with no upper bound.
    """

    def __init__(
        self,
        *,
        req_limit: int = 600,
        byte_limit: int = 50 * 1024 * 1024,
        window_s: int = 60,
        # Total distinct IPs tracked concurrently. Sized to comfortably
        # accommodate a busy real workload (thousands of analyst sessions on
        # NAT'd corporate networks share a small set of egress IPs) while
        # blocking a runaway-cardinality DoS in single-digit-MB territory.
        max_tracked_ips: int = 10_000,
    ) -> None:
        import threading

        self.REQ_LIMIT = req_limit
        self.BYTE_LIMIT = byte_limit
        self.WINDOW_S = window_s
        self.MAX_TRACKED_IPS = max_tracked_ips
        self._lock = threading.Lock()
        self._reqs: dict[str, list[float]] = {}
        self._bytes: dict[str, list[tuple[float, int]]] = {}

    def _evict_locked(self, cutoff: float) -> None:
        """Sweep stale per-IP entries whose all timestamps fall before cutoff."""
        # Iterate over a snapshot so we can mutate during the loop.
        for ip in list(self._reqs.keys()):
            recent = [t for t in self._reqs[ip] if t >= cutoff]
            if not recent:
                self._reqs.pop(ip, None)
                self._bytes.pop(ip, None)
            else:
                # Take this opportunity to also trim the surviving list.
                self._reqs[ip] = recent
        if len(self._reqs) > self.MAX_TRACKED_IPS:
            # Cardinality bomb: drop everything rather than spending CPU
            # on quadratic LRU tracking. Limits get a one-minute reset for
            # all IPs but the next legitimate burst will re-grow the dict.
            self._reqs.clear()
            self._bytes.clear()

    def check(self, ip: str, content_length: int) -> bool:
        with self._lock:
            now = time.time()
            cutoff = now - self.WINDOW_S
            # Cheap pre-check: only sweep when we're past the cap. The sweep
            # is O(n) so we don't want to run it on every request.
            if len(self._reqs) > self.MAX_TRACKED_IPS:
                self._evict_locked(cutoff)
            rs = [t for t in self._reqs.get(ip, []) if t >= cutoff]
            if len(rs) >= self.REQ_LIMIT:
                self._reqs[ip] = rs
                return False
            rs.append(now)
            self._reqs[ip] = rs
            bs = [(t, n) for (t, n) in self._bytes.get(ip, []) if t >= cutoff]
            bs.append((now, max(0, int(content_length))))
            self._bytes[ip] = bs
            if sum(n for _, n in bs) > self.BYTE_LIMIT:
                return False
            return True


_static_limiter = _StaticAssetLimiter()
# L6: separate per-IP budget for analyst API calls. The static limiter only
# covered /_next/ + /static/, so a single source could hammer /api/query or
# /api/insights (both compute-heavy) unbounded. Own bucket so API and static
# traffic don't share a budget; same generous 600/min ceiling (NAT'd offices
# share an egress IP) — it caps single-source floods, not normal use.
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
        if is_remote:
            if not _remote_host_allowed(host_header):
                return JSONResponse(status_code=400, content={"error": "host_not_allowed", "host": host_header})
        elif host_header and not _local_host_allowed(host_header):
            return JSONResponse(status_code=400, content={"error": "host_not_allowed", "host": host_header})

        if not is_remote:
            # Pure-local request — no analyst gating. Hardening headers still
            # apply: the admin tunnel (uvicorn on :3001 over SSH) bypasses
            # Caddy entirely, so this middleware is the only thing that can
            # set X-Frame-Options / Permissions-Policy / a minimal CSP on
            # admin responses. Analyst branch headers are largely overridden
            # by Caddy's security_headers snippet (Caddyfile §38).
            #
            # Shared-secret defense-in-depth: when ADMIN_SHARED_SECRET is
            # configured, refuse admin endpoints whose X-Admin-Token doesn't
            # match. Exempt /api/bootstrap (the SPA fetches the token from
            # there) and /api/health (loopback container healthcheck).
            # All browser requests are same-origin via Caddy after the
            # api.ts shortcut removal, so no CORS preflight ever lands
            # here — every OPTIONS that does is a deliberate caller and
            # must still carry the token.
            secret = _admin_shared_secret()
            if secret and path not in _ADMIN_TOKEN_EXEMPT_PATHS:
                supplied = request.headers.get(ADMIN_TOKEN_HEADER, "")
                # Constant-time compare so the token length / prefix doesn't
                # leak through CPU-cache timing on a short-circuit mismatch.
                import hmac

                if not supplied or not hmac.compare_digest(supplied, secret):
                    return JSONResponse(
                        status_code=401,
                        content={
                            "detail": {
                                "error": "admin_token_invalid" if supplied else "admin_token_required",
                            }
                        },
                    )
            _t0 = time.perf_counter()
            response = await call_next(request)
            _dur_ms = (time.perf_counter() - _t0) * 1000.0
            # Companion to the [analyst] log line below — surface admin
            # request activity with an [admin] tag so it's easy to grep
            # "who hit what" across both auth modes. SRE-02: carries the
            # correlation id (rid=) so a slow line here joins to its
            # slow_queries rows, and the latency (dur_ms) so the slow
            # request is identifiable from `docker logs` in the first place.
            try:
                peer = client_ip(request, default="127.0.0.1")
                logging.getLogger("backend.access.admin").info(
                    "[admin] [%s] rid=%s %s %s -> %d (%.1fms)",
                    peer,
                    getattr(request.state, "request_id", "-"),
                    method,
                    path,
                    response.status_code,
                    _dur_ms,
                )
            except Exception:
                pass
            return apply_response_hardening(response)

        # ── From here down, we're on the analyst path. ──

        # Static-asset rate limit.
        if path.startswith("/_next/") or path.startswith("/static/"):
            ip = client_ip(request)
            if not _static_limiter.check(ip, int(request.headers.get("content-length") or "0")):
                return JSONResponse(status_code=429, content={"error": "rate_limited"})
        # L6: per-IP request-rate limit on analyst API calls (the static
        # limiter above covered only /_next/ + /static/). Backstops M1/M2 —
        # repeated compute-heavy /api/query or /api/insights from one source.
        # Only genuinely remote source IPs are limited: SSR-forwarded fetches
        # arrive over loopback (127.0.0.1) and would otherwise all share one
        # bucket and throttle server-rendering under load.
        elif path.startswith("/api/"):
            ip = client_ip(request)
            if not _is_private_or_loopback(ip) and not _analyst_api_limiter.check(
                ip, int(request.headers.get("content-length") or "0")
            ):
                return JSONResponse(status_code=429, content={"error": "rate_limited"})

        # Origin gate for non-GET/HEAD writes.  Telemetry beacons
        # (sendBeacon) may arrive without an Origin header through CDN;
        # they're fire-and-forget, no mutation, no data return — CSRF
        # protection adds no value.
        if method not in ("GET", "HEAD") and not any(path.startswith(p) for p in _TELEMETRY_IDLE_EXEMPT_PREFIXES):
            origin = request.headers.get("origin", "")
            if not _origin_allowed(origin):
                return JSONResponse(
                    status_code=403,
                    content={"error": "origin_not_allowed", "origin": origin},
                )

        # Unauthenticated paths (login, heartbeat, health). The OAuth handshake
        # routes (/api/share/oauth/authorize + /callback) carry NO analyst
        # session cookie during the handshake — they ARE the pre-auth login
        # step — so they must be exempted here or they'd 401 before the handler
        # runs (design §3.4). They set the session cookie themselves on success.
        if (
            path in _UNAUTH_ANALYST_PATHS
            or path.startswith("/api/share/claim/")
            or path.startswith("/api/share/oauth/")
        ):
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
                response = await _strip_analyst_envelope(response)
            return apply_response_hardening(response)

        # All other analyst paths require a valid session.
        sid = request.cookies.get("analyst_session_id")
        session = mgr.validate_session(sid)
        if session is None:
            return JSONResponse(status_code=401, content={"error": "unauthenticated"})
        if getattr(session, "tos_pending", False):
            return JSONResponse(status_code=403, content={"error": "tos_pending"})

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
            return JSONResponse(status_code=401, content={"error": "fingerprint_mismatch"})

        # Block admin / provision / debug surfaces.
        if _is_blocked_path(path):
            return JSONResponse(status_code=403, content={"error": "admin_only"})

        # SSE allowlist gate. Entries match either an exact path
        # (non-parameterized routes, e.g. "/api/log-extents/stream") or a
        # path suffix (a leading "/"-prefixed segment, for routes
        # parameterized by service_id, e.g. "/realtime-stream" matching
        # "/api/services/{service_id}/realtime-stream").
        if _is_sse_route(path) and not any(
            path == allowed or path.endswith(allowed) for allowed in _ANALYST_SSE_ALLOWLIST
        ):
            return JSONResponse(status_code=403, content={"error": "sse_blocked"})

        # Read-only gate: refuse mutating verbs except on routes confirmed to
        # be read-only-via-POST (most dashboard/security/etc. queries POST
        # JSON filter bodies). See _ANALYST_ALLOWED_WRITE_PREFIXES for the
        # allowlist rationale.
        if method in ("PUT", "PATCH", "DELETE"):
            return JSONResponse(status_code=403, content={"error": "read_only"})
        if method == "POST" and not any(path.startswith(p) for p in _ANALYST_ALLOWED_WRITE_PREFIXES):
            return JSONResponse(status_code=403, content={"error": "read_only"})

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
        _skip_scope = path.startswith("/api/share/") or any(path.startswith(p) for p in _TELEMETRY_IDLE_EXEMPT_PREFIXES)
        if not _skip_scope:
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
            resolved_candidates: list[str] = []
            for cand in raw_candidates:
                if svcconfig.load_config(cand):
                    resolved_candidates.append(cand)
                else:
                    resolved_candidates.append(cdn_map.get(cand, cand))

            if not resolved_candidates:
                # No explicit service in the request — fall back to the active
                # default (preserves pre-fix behavior for analyst-facing GET
                # /api/dashboard etc. where the active service comes from the
                # session config).
                fallback = svcconfig.get_active_service_id()
                if fallback:
                    resolved_candidates.append(fallback)

            allowed_services = set(session.service_ids or [])
            for eff in resolved_candidates:
                if not eff or eff not in allowed_services:
                    return JSONResponse(
                        status_code=403,
                        content={"error": "service_not_authorized", "service": eff or ""},
                    )
            if not resolved_candidates:
                # No candidate could be derived — fail closed.
                return JSONResponse(
                    status_code=403,
                    content={"error": "service_not_authorized", "service": ""},
                )

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

                for raw_key in await _body_filter_keys(request):
                    col = normalize_filter_key(raw_key)
                    if col in _PII_FORBIDDEN_FILTER_COLS:
                        return JSONResponse(
                            status_code=403,
                            content={"error": "pii_policy_violation", "field": col},
                        )
                # Same lock for the field-values ENUMERATION dimension
                # (POST /api/dashboard/field-values {"field": "ip"}): enumerating
                # a PII column's distinct values — even masked — leaks per-value
                # counts + a search-prefix oracle, so reject it exactly like a
                # PII filter key. normalize_filter_key resolves junk-suffixed
                # variants ("ip.", "cookie_session ") to the real column.
                dim = await _body_field_param(request)
                if dim is not None:
                    dim_col = normalize_filter_key(dim)
                    if dim_col in _PII_FORBIDDEN_FILTER_COLS:
                        return JSONResponse(
                            status_code=403,
                            content={"error": "pii_policy_violation", "field": dim_col},
                        )

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
                return JSONResponse(status_code=403, content={"error": "ip_not_whitelisted"})

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
        _idle_exempt = any(path.startswith(p) for p in _TELEMETRY_IDLE_EXEMPT_PREFIXES) or _is_sse_route(path)
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
        try:
            analyst_peer = client_ip(request)
            logging.getLogger("backend.access.analyst").info(
                "[analyst] %s (%s) [%s] rid=%s %s %s -> %d (%.1fms) act=%s idle_touch=%d",
                session.email,
                session.name or "no-name",
                analyst_peer,
                getattr(request.state, "request_id", "-"),
                method,
                path,
                response.status_code,
                _dur_ms,
                _active_hdr or "-",
                int(_touched_idle),
            )
        except Exception:
            pass

        # N-1 + N-10: strip server-internal telemetry envelope from analyst
        # responses (success AND error bodies). The handler-side
        # ``DEBUG_RESPONSES`` gate in BaseResponse covers the Pydantic path
        # but misses ad-hoc dict responses in admin routers and the
        # short-circuit JSONResponse error bodies, so we do a final pass
        # here on the buffered body. SSE responses (text/event-stream) are
        # passed through unchanged inside the helper.
        response = await _strip_analyst_envelope(response, analyst_session=session)

        # SSE-safe: don't add hardening headers to SSE streams in a way that
        # interferes; the keep-alive headers go on the route itself.
        apply_response_hardening(response)
        return response


# ── Time-bounds dependency (Section #21) ────────────────────────────────────


from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import parse_iso_utc

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
        candidates = [c for c in (req_start, self.start) if c is not None]
        eff_start = max(candidates) if candidates else None
        candidates = [c for c in (req_end, self.end) if c is not None]
        eff_end = min(candidates) if candidates else None
        # Anchor to wall-clock floor if the caller passed nothing.
        if eff_start is None and eff_end is None:
            now = datetime.now(UTC)
            eff_start = now - timedelta(hours=1)
            eff_end = now
        elif eff_start is None:
            assert eff_end is not None  # narrowed by `not (start is None and end is None)` above
            eff_start = eff_end - timedelta(hours=1)
        elif eff_end is None:
            eff_end = datetime.now(UTC)
        assert eff_start is not None and eff_end is not None  # narrowed by the branches above
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
            detail={"error": str(exc), "time_range_empty": True},
        ) from exc
    return start.isoformat(), end.isoformat()
