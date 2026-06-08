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

import logging
import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from backend.core import share_db
from backend.utils.tunnel import compute_fingerprint, get_tunnel_manager

logger = logging.getLogger(__name__)

# Paths that an analyst can always reach without a session (login, the static
# share-login bundle, heartbeat). The middleware short-circuits on these
# before doing the session lookup.
_UNAUTH_ANALYST_PATHS = {
    "/api/share/login",
    "/api/share/logout",
    "/api/share/heartbeat",
    "/api/health",
    # Bootstrap is callable without a session so the frontend can detect
    # is_remote_analyst=true and redirect anonymous remote visitors to
    # /share-login. The bootstrap endpoint itself manually validates the
    # session cookie and returns a stub response when absent.
    "/api/bootstrap",
}

# Path prefixes that are EXPLICITLY blocked for analysts even with a valid
# session. Admin surface, anything mutating provisioning, debug.
_ANALYST_BLOCKED_PREFIXES = (
    "/api/admin/",  # includes /api/admin/share/* — analyst can never reach admin tooling
    "/api/provision/",
    "/api/debug/",
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
    "/api/dashboard/",  # /aggregates, /raw, /raw/csv, /field-values
    "/api/security/",  # /aggregates, /top-bots
    "/api/origin/",  # /summary, /timeseries, /slow-urls, etc.
    "/api/performance/",  # /aggregates, /origin-ts
    "/api/insights",  # POST /api/insights (no trailing slash — exact path)
    "/api/network-health",  # POST /api/network-health
    "/api/network-quality",  # POST /api/network-quality
    "/api/query",  # POST /api/query
    "/api/sessions",  # POST /api/sessions and /api/sessions/detail
    "/api/charts/",
)

# SSE routes that ARE allowed for analysts. New SSE routes default to *off* for
# analysts; an explicit add here is the only way to expose one.
_ANALYST_SSE_ALLOWLIST: set[str] = set()

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
    ``169.254.169.254`` (GCE metadata) would land as "local" too.

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
    host = request.client.host if request.client else "127.0.0.1"

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


def get_client_ip(request: Request, *, is_remote: bool) -> str:
    """Return the trusted client IP.

    With uvicorn running ``--proxy-headers --forwarded-allow-ips=127.0.0.1``
    the framework already populates ``request.client.host`` from X-Forwarded-For
    when the TCP peer is loopback (i.e., Caddy on this host). For all other
    peers, ``request.client.host`` IS the socket peer. We never re-parse the
    XFF header ourselves — that's what made exploitable. The
    ``is_remote`` parameter is kept for backwards compatibility but no longer
    influences the result.
    """
    del is_remote  # signal: parameter intentionally ignored, kept for ABI stability
    return request.client.host if request.client else "0.0.0.0"


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
    if state.tunnel_url:
        candidates.append(state.tunnel_url.lower())
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
    if state.tunnel_url and state.tunnel_url.lower() == host:
        return True
    if state.public_endpoint:
        pe = urlparse(state.public_endpoint)
        if pe.hostname and pe.hostname.lower() == host:
            return True
    return False


def _is_blocked_path(path: str) -> bool:
    return any(path.startswith(p) for p in _ANALYST_BLOCKED_PREFIXES)


def _is_sse_route(path: str) -> bool:
    return "/sse" in path or path.endswith("/stream")


def apply_response_hardening(response: Response) -> Response:
    response.headers.setdefault("Cache-Control", "private, no-store")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    return response


# ── Sliding-window static-asset rate limiter (per IP) ───────────────────────


class _StaticAssetLimiter:
    """Per-IP token bucket: 600 requests/min OR 50 MB/min.

    Security: bound the in-memory ``_reqs`` / ``_bytes`` dicts so a
    high-cardinality IP attack (one request per source) cannot OOM the
    server by inflating the dicts indefinitely. The original implementation
    never evicted; an attacker with a botnet (or one that spoofed XFF before
    Phase 0 closed it) could pump ~50 bytes of memory per unique IP per
    minute with no upper bound.
    """

    REQ_LIMIT = 600
    BYTE_LIMIT = 50 * 1024 * 1024
    WINDOW_S = 60
    # Total distinct IPs we'll track concurrently. Sized to comfortably
    # accommodate a busy real workload (thousands of analyst sessions on
    # NAT'd corporate networks share a small set of egress IPs) while
    # blocking a runaway-cardinality DoS in single-digit-MB territory.
    MAX_TRACKED_IPS = 10_000

    def __init__(self) -> None:
        import threading

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
            rs.append(now)
            self._reqs[ip] = rs
            if len(rs) > self.REQ_LIMIT:
                return False
            bs = [(t, n) for (t, n) in self._bytes.get(ip, []) if t >= cutoff]
            bs.append((now, max(0, int(content_length))))
            self._bytes[ip] = bs
            if sum(n for _, n in bs) > self.BYTE_LIMIT:
                return False
            return True


_static_limiter = _StaticAssetLimiter()


# ── The middleware ──────────────────────────────────────────────────────────


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
            # Pure-local request — no analyst gating, no extra headers.
            response = await call_next(request)
            # Companion to the [analyst] log line below — surface admin
            # request activity with an [admin] tag so it's easy to grep
            # "who hit what" across both auth modes.
            try:
                peer = request.client.host if request.client else "127.0.0.1"
                logging.getLogger("backend.access.admin").info(
                    "[admin] [%s] %s %s -> %d",
                    peer,
                    method,
                    path,
                    response.status_code,
                )
            except Exception:
                pass
            return response

        # ── From here down, we're on the analyst path. ──

        # Static-asset rate limit.
        if path.startswith("/_next/") or path.startswith("/static/"):
            ip = get_client_ip(request, is_remote=True)
            if not _static_limiter.check(ip, int(request.headers.get("content-length") or "0")):
                return JSONResponse(status_code=429, content={"error": "rate_limited"})

        # Origin gate for non-GET/HEAD writes.
        if method not in ("GET", "HEAD"):
            origin = request.headers.get("origin", "")
            if not _origin_allowed(origin):
                return JSONResponse(
                    status_code=403,
                    content={"error": "origin_not_allowed", "origin": origin},
                )

        # Unauthenticated paths (login, heartbeat, health).
        if path in _UNAUTH_ANALYST_PATHS or path.startswith("/api/share/claim/"):
            response = await call_next(request)
            return apply_response_hardening(response)

        # All other analyst paths require a valid session.
        sid = request.cookies.get("analyst_session_id")
        session = mgr.validate_session(sid)
        if session is None:
            return JSONResponse(status_code=401, content={"error": "unauthenticated"})

        # Fingerprint match.
        headers_lc = {k.lower(): v for k, v in request.headers.items()}
        if compute_fingerprint(headers_lc) != session.fingerprint_signature:
            mgr.boot_session(session.session_id, reason="fingerprint mismatch")
            share_db.log_share_audit_event(
                event_type="FINGERPRINT_MISMATCH",
                email=session.email,
                ip_address=get_client_ip(request, is_remote=True),
                details=f"path={path}",
            )
            return JSONResponse(status_code=401, content={"error": "fingerprint_mismatch"})

        # Block admin / provision / debug surfaces.
        if _is_blocked_path(path):
            return JSONResponse(status_code=403, content={"error": "admin_only"})

        # SSE allowlist gate.
        if _is_sse_route(path) and path not in _ANALYST_SSE_ALLOWLIST:
            return JSONResponse(status_code=403, content={"error": "sse_blocked"})

        # Service-scope gate. If the route has a ?service= param, the linked
        # invite must be allowed to access it.
        service_param = (
            request.query_params.get("service")
            or request.headers.get("x-fastly-service-id")
            or request.headers.get("x-service-id")
        )
        if service_param and service_param not in (session.service_ids or []):
            return JSONResponse(
                status_code=403,
                content={"error": "service_not_authorized", "service": service_param},
            )

        # Read-only gate: refuse mutating verbs except on routes confirmed to
        # be read-only-via-POST (most dashboard/security/etc. queries POST
        # JSON filter bodies). See _ANALYST_ALLOWED_WRITE_PREFIXES for the
        # allowlist rationale.
        if method in ("POST", "PUT", "PATCH", "DELETE") and not any(
            path.startswith(p) for p in _ANALYST_ALLOWED_WRITE_PREFIXES
        ):
            return JSONResponse(status_code=403, content={"error": "read_only"})

        # IP-roaming: update without booting if whitelist still passes.
        current_ip = get_client_ip(request, is_remote=True)
        if current_ip != session.ip_address:
            invite = share_db.get_remote_invite(session.invite_id)
            if invite and share_db.ip_in_whitelist(current_ip, invite.get("ip_whitelist")):
                mgr.touch_session(session.session_id, new_ip=current_ip)
            else:
                return JSONResponse(status_code=403, content={"error": "ip_not_whitelisted"})

        # Stamp the session into request.state for downstream code.
        request.state.analyst_session = session
        mgr.touch_session(session.session_id, last_activity=f"{method} {path}")

        # Hand off.
        response = await call_next(request)

        # Per-analyst access log so admin can see who hit what. Sits
        # alongside uvicorn's default access log (which only shows IP).
        # Surface email + name + IP + path → trivial to grep by user.
        try:
            client_ip = get_client_ip(request, is_remote=True)
            logging.getLogger("backend.access.analyst").info(
                "[analyst] %s (%s) [%s] %s %s -> %d",
                session.email,
                session.name or "no-name",
                client_ip,
                method,
                path,
                response.status_code,
            )
        except Exception:
            pass

        # SSE-safe: don't add hardening headers to SSE streams in a way that
        # interferes; the keep-alive headers go on the route itself.
        apply_response_hardening(response)
        return response


# ── Time-bounds dependency (Section #21) ────────────────────────────────────


from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import parse_iso_utc


@dataclass
class TimeBounds:
    """The session's effective query window. ``None`` means unrestricted."""

    start: datetime | None = None
    end: datetime | None = None

    def clamp(self, req_start: datetime | None, req_end: datetime | None) -> tuple[datetime, datetime]:
        """Clamp a requested range against the session's allowed window.

        Returns ``(start, end)``. Raises ``ValueError`` if the clamped range
        is empty (the route should translate this to a 422 — see the contract).
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
            eff_start = eff_end - timedelta(hours=1)
        elif eff_end is None:
            eff_end = datetime.now(UTC)
        if eff_start >= eff_end:
            raise ValueError("clamped time range is empty")
        return eff_start, eff_end


def get_analyst_time_bounds(request: Request) -> TimeBounds:
    """FastAPI dependency: returns the active session's clamp window.

    For non-analyst (local-admin) requests, returns an open ``TimeBounds`` —
    so existing analytics routes can declare the dependency unconditionally
    without changing behavior for the admin.
    """
    session = getattr(request.state, "analyst_session", None)
    if session is None:
        return TimeBounds()
    end = parse_iso_utc(session.query_end_time) if session.query_end_time else None
    start = parse_iso_utc(session.query_start_time) if session.query_start_time else None
    if session.query_window_hours:
        relative_start = datetime.now(UTC) - timedelta(hours=int(session.query_window_hours))
        start = max(start, relative_start) if start else relative_start
    return TimeBounds(start=start, end=end)
