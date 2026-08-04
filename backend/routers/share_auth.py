"""Analyst login + TOS acknowledgment + claim-token reveal routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, Response

from backend.core import share_db
from backend.core.oauth import registry as oauth_registry
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.share_auth import (
    AuthConfigResponse,
    ShareAcknowledgeResponse,
    ShareClaimResponse,
    ShareHeartbeatResponse,
    ShareLoginPayload,
    ShareLoginResponse,
    ShareLogoutResponse,
    TosAckPayload,
    TosDocument,
)
from backend.utils.analyst_session import (
    ANALYST_PENDING_SESSION_COOKIE,
    ANALYST_SESSION_COOKIE,
    safe_audit_email,
)
from backend.utils.remote_access import client_ip
from backend.utils.tunnel import TunnelCapacityError, get_tunnel_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/share", tags=["share-auth"], responses=DEFAULT_ERROR_RESPONSES)
# Canonical definitions live in backend.utils.analyst_session (shared with the
# OAuth router without a router→router import). Kept as module-level aliases so
# existing references (and tests) keep resolving them here.
COOKIE_NAME = ANALYST_SESSION_COOKIE
PENDING_COOKIE_NAME = ANALYST_PENDING_SESSION_COOKIE
_safe_audit_email = safe_audit_email


@router.get("/auth-config", response_model=AuthConfigResponse)
def share_auth_config():
    """Unauthenticated: which auth modes the ``/share-login`` page should render.

    Reached pre-auth (no session cookie) — exempted in the middleware unauth
    allowlist. Exposes only enabled providers, and only ``id`` + ``display_name``
    (never client_id/secret/discovery_url). Drives graceful degradation (§5.2):
    on fetch failure the frontend fails OPEN to the passcode form.
    """
    providers = [p.public_dict() for p in oauth_registry.get_enabled_providers()]
    return AuthConfigResponse(
        passcode_enabled=oauth_registry.passcode_login_enabled(),
        providers=providers,
    )


@router.post("/login", response_model=ShareLoginResponse)
def share_login(payload: ShareLoginPayload, request: Request, response: Response):
    ip = client_ip(request)
    user_agent = request.headers.get("user-agent", "")
    headers = {k.lower(): v for k, v in request.headers.items()}

    # Passcode login can be turned off (SSO-exclusive deployments). Fail closed
    # at the endpoint, not just in the UI, so disabling it in /auth-config can't
    # be bypassed by posting directly. Default is ON — passcode flow unchanged.
    if not oauth_registry.passcode_login_enabled():
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=_safe_audit_email(payload.email),
            ip_address=ip,
            details="passcode login disabled",
        )
        raise HTTPException(status_code=403, detail={"error": "passcode_login_disabled"})

    mgr = get_tunnel_manager()

    locked, remaining = mgr.check_rate_limit(ip)
    if locked:
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=_safe_audit_email(payload.email),
            ip_address=ip,
            details=f"locked out (remaining {remaining}s)",
        )
        raise HTTPException(
            status_code=429,
            detail={"error": "rate_limited", "retry_after_s": remaining},
            headers={"Retry-After": str(remaining)},
        )

    invite = share_db.get_remote_invite_by_email_passcode(payload.email, payload.passcode)
    if invite is None:
        mgr.record_login_failure(ip, payload.email)
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=_safe_audit_email(payload.email),
            ip_address=ip,
            details="invalid credentials",
        )
        raise HTTPException(status_code=401, detail={"error": "invalid_credentials"})

    # IP whitelist check.
    if not share_db.ip_in_whitelist(ip, invite.get("ip_whitelist")):
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=invite["email"],
            ip_address=ip,
            details="IP not in whitelist",
        )
        raise HTTPException(status_code=403, detail={"error": "ip_not_whitelisted"})

    # Capacity cap (default 10 — set in share_settings via migration). Checked
    # atomically with the insert inside create_session (cap=) — a separate
    # check-then-act here would race concurrent logins past the cap.
    cap = share_db.get_max_concurrent_sessions()
    try:
        session = mgr.create_session(invite=invite, ip_address=ip, user_agent=user_agent, headers=headers, cap=cap)
    except TunnelCapacityError as e:
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=invite["email"],
            ip_address=ip,
            details=f"global session cap exceeded ({cap})",
        )
        raise HTTPException(
            status_code=503,
            detail={"error": "capacity_exceeded", "current": e.current, "cap": e.cap},
        ) from e
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS",
        email=invite["email"],
        ip_address=ip,
        details=f"session={session.session_id[:8]}…",
    )

    tos = share_db.get_latest_tos()
    tos_pending = bool(
        tos and (invite.get("tos_accepted_at") is None or (invite.get("tos_version") or "") != tos["version"])
    )

    # Two-cookie protocol: while tos_pending=True the session id lives in
    # PENDING_COOKIE_NAME. The middleware refuses analyst-protected endpoints
    # on a pending session and the SPA redirects to /share-login/acknowledge.
    # Only after TOS acceptance does the cookie get upgraded to COOKIE_NAME
    # via /api/share/acknowledge (which also rotates the session id).
    # Previously the full cookie was issued unconditionally — pending/full
    # was a fiction and a session-fixation gap across the TOS boundary.
    target_cookie = PENDING_COOKIE_NAME if tos_pending else COOKIE_NAME
    other_cookie = COOKIE_NAME if tos_pending else PENDING_COOKIE_NAME
    response.set_cookie(
        key=target_cookie,
        value=session.session_id,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=24 * 60 * 60,
        path="/",
    )
    response.delete_cookie(other_cookie, path="/")

    return ShareLoginResponse(
        ok=True,
        name=session.name,
        email=session.email,
        tos_pending=tos_pending,
        tos=TosDocument(version=tos["version"], text=tos["text"]) if (tos_pending and tos) else None,
        service_ids=session.service_ids,
        redirect="/share-login/acknowledge" if tos_pending else "/dashboard",
    )


@router.post("/logout", response_model=ShareLogoutResponse)
def share_logout(request: Request, response: Response):
    sid = request.cookies.get(COOKIE_NAME) or request.cookies.get(PENDING_COOKIE_NAME)
    mgr = get_tunnel_manager()
    if sid:
        mgr.boot_session(sid, reason="analyst logout")
    response.delete_cookie(COOKIE_NAME, path="/")
    response.delete_cookie(PENDING_COOKIE_NAME, path="/")
    return ShareLogoutResponse(ok=True)


@router.get("/tos", response_model=TosDocument)
def share_get_tos(request: Request):
    """Return the latest TOS document so the acknowledge page can render the
    real text and POST back the matching version.

    Session-gated (pending OR full cookie) — the same shape /acknowledge uses —
    so anonymous callers can't enumerate the TOS surface. The strict version
    check in /acknowledge (audit finding 021) means the frontend must know the
    exact current version; this endpoint is how it learns it.
    """
    sid = request.cookies.get(PENDING_COOKIE_NAME) or request.cookies.get(COOKIE_NAME)
    mgr = get_tunnel_manager()
    session = mgr.validate_session(sid)
    if session is None:
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    tos = share_db.get_latest_tos()
    if not tos:
        raise HTTPException(status_code=404, detail={"error": "no_tos"})
    return TosDocument(version=tos["version"], text=tos["text"])


@router.post("/acknowledge", response_model=ShareAcknowledgeResponse)
def share_acknowledge_tos(payload: TosAckPayload, request: Request, response: Response):
    sid = request.cookies.get(PENDING_COOKIE_NAME) or request.cookies.get(COOKIE_NAME)
    mgr = get_tunnel_manager()
    session = mgr.validate_session(sid)
    if session is None:
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})

    tos = share_db.get_latest_tos()
    if tos and payload.version != tos["version"]:
        raise HTTPException(status_code=400, detail={"error": "invalid_tos_version"})

    share_db.mark_tos_accepted(session.invite_id, payload.version)
    share_db.log_share_audit_event(
        event_type="TOS_ACCEPTED",
        email=session.email,
        ip_address=session.ip_address,
        details=f"version={payload.version}",
    )

    # Rotate the session id at the TOS-acceptance boundary so any cookie
    # value an attacker could have observed during the pending window can
    # no longer be replayed against the now-fully-scoped session. If the
    # session evaporated between validate and rotate (extreme race), force
    # the user back through login rather than re-emitting the old id.
    new_sid = mgr.rotate_session_id(session.session_id)
    if new_sid is None:
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    response.set_cookie(
        key=COOKIE_NAME,
        value=new_sid,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=24 * 60 * 60,
        path="/",
    )
    response.delete_cookie(PENDING_COOKIE_NAME, path="/")
    return ShareAcknowledgeResponse(ok=True)


@router.get("/heartbeat", response_model=ShareHeartbeatResponse)
def share_heartbeat(request: Request):
    """Session validity probe AND the dedicated user-activity channel.

    Returns 401 if the session is gone so the frontend redirects to login.

    Also the one reliable way the backend learns the user is still genuinely
    present: data requests only flow when the dashboard refetches (new logs /
    navigation), so on a quiet tab an active user would otherwise generate no
    activity signal and get idle-logged-out. The poller fires every ~30s and
    carries ``X-User-Active``; when it reports genuine recent interaction
    ("1") we reset the idle clock. When it reports idle ("0", or the header is
    absent), we DON'T — so a backgrounded/abandoned tab still expires at the
    2h idle cap. This is the inverse of the original bug: the heartbeat now
    keeps the session alive only while the user is actually there.
    """
    sid = request.cookies.get(COOKIE_NAME) or request.cookies.get(PENDING_COOKIE_NAME)
    mgr = get_tunnel_manager()
    session = mgr.validate_session(sid)
    if session is None:
        mgr.record_heartbeat_unauth()
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    if request.headers.get("x-user-active") == "1":
        mgr.touch_session(session.session_id, last_activity="heartbeat (active)")
    return ShareHeartbeatResponse(
        ok=True,
        last_active=session.last_active_time,
    )


@router.post("/claim/{token}", response_model=ShareClaimResponse)
def share_claim(token: str, request: Request):
    """One-time-view reveal of an invite's plaintext credentials.

    The plaintext passcode itself isn't stored — the hash is one-way — so
    this endpoint reveals everything *except* the passcode. The original
    plaintext is communicated by the admin via the share card; the claim
    URL exists to confirm scope and identity to the analyst without
    putting credentials in a chat tool that retains history.
    """
    ip = client_ip(request)
    row = share_db.claim_token(token, ip)
    if row is None:
        share_db.log_share_audit_event(
            event_type="CLAIM_FAIL",
            email=None,
            ip_address=ip,
            details=f"token {token[:6]}… invalid/expired/claimed",
        )
        raise HTTPException(status_code=404, detail={"error": "invalid_or_used"})
    invite = share_db.get_remote_invite(row["invite_id"])
    share_db.log_share_audit_event(
        event_type="INVITE_CLAIMED",
        email=invite.get("email") if invite else None,
        ip_address=ip,
        details=f"token {token[:6]}…",
    )
    return ShareClaimResponse(
        ok=True,
        name=invite.get("name") if invite else None,
        email=invite.get("email") if invite else None,
        expires_at=invite.get("expires_at") if invite else None,
        service_ids=(invite.get("service_ids") if invite else []) or [],
    )
