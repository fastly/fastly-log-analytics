"""Analyst login + TOS acknowledgment + claim-token reveal routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from backend.core import share_db
from backend.models.share_auth import (
    ShareAcknowledgeResponse,
    ShareClaimResponse,
    ShareHeartbeatResponse,
    ShareLoginResponse,
    ShareLogoutResponse,
    TosDocument,
)
from backend.utils.tunnel import get_tunnel_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/share", tags=["share-auth"])

COOKIE_NAME = "analyst_session_id"


def _client_ip(request: Request) -> str:
    """Extract the real client IP.

    The middleware that wraps remote requests sets ``request.state.is_remote``;
    when true we honor ``X-Forwarded-For`` (the Next.js proxy injects it).
    On local-listener traffic we ignore the header to prevent IP spoofing
    (Section #5).
    """
    is_remote = getattr(request.state, "is_remote", False)
    if is_remote:
        fwd = request.headers.get("x-forwarded-for")
        if fwd:
            return fwd.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "0.0.0.0"


class ShareLoginPayload(BaseModel):
    email: str
    passcode: str


@router.post("/login", response_model=ShareLoginResponse)
def share_login(payload: ShareLoginPayload, request: Request, response: Response):
    ip = _client_ip(request)
    user_agent = request.headers.get("user-agent", "")
    headers = {k.lower(): v for k, v in request.headers.items()}

    mgr = get_tunnel_manager()

    locked, remaining = mgr.check_rate_limit(ip)
    if locked:
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=payload.email,
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
            email=payload.email,
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

    # Capacity cap (default 10 — set in share_settings via migration).
    cap = share_db.get_max_concurrent_sessions()
    if mgr.active_session_count() >= cap:
        share_db.log_share_audit_event(
            event_type="LOGIN_FAIL",
            email=invite["email"],
            ip_address=ip,
            details=f"global session cap exceeded ({cap})",
        )
        raise HTTPException(
            status_code=503,
            detail={"error": "capacity_exceeded", "current": mgr.active_session_count(), "cap": cap},
        )

    # Success.
    mgr.clear_login_failures(ip)
    session = mgr.create_session(invite=invite, ip_address=ip, user_agent=user_agent, headers=headers)
    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS",
        email=invite["email"],
        ip_address=ip,
        details=f"session={session.session_id[:8]}…",
    )

    # Cookie contract — see Section #4. secure=True is non-negotiable.
    # In test mode (TestClient defaults to http://testserver), uvicorn won't
    # send secure cookies; we tag it anyway because tests can read Set-Cookie.
    response.set_cookie(
        key=COOKIE_NAME,
        value=session.session_id,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=share_db.iso_z_now() and 24 * 60 * 60,
        path="/",
    )

    tos = share_db.get_latest_tos()
    tos_pending = bool(
        tos and (invite.get("tos_accepted_at") is None or (invite.get("tos_version") or "") != tos["version"])
    )

    return ShareLoginResponse(
        ok=True,
        session_id=session.session_id,
        name=session.name,
        email=session.email,
        tos_pending=tos_pending,
        tos=TosDocument(version=tos["version"], text=tos["text"]) if (tos_pending and tos) else None,
        service_ids=session.service_ids,
        redirect="/share-login/acknowledge" if tos_pending else "/dashboard",
    )


@router.post("/logout", response_model=ShareLogoutResponse)
def share_logout(request: Request, response: Response):
    sid = request.cookies.get(COOKIE_NAME)
    mgr = get_tunnel_manager()
    if sid:
        mgr.boot_session(sid, reason="analyst logout")
    response.delete_cookie(COOKIE_NAME, path="/")
    return ShareLogoutResponse(ok=True)


class TosAckPayload(BaseModel):
    version: str


@router.post("/acknowledge", response_model=ShareAcknowledgeResponse)
def share_acknowledge_tos(payload: TosAckPayload, request: Request):
    sid = request.cookies.get(COOKIE_NAME)
    mgr = get_tunnel_manager()
    session = mgr.validate_session(sid)
    if session is None:
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    share_db.mark_tos_accepted(session.invite_id, payload.version)
    share_db.log_share_audit_event(
        event_type="TOS_ACCEPTED",
        email=session.email,
        ip_address=session.ip_address,
        details=f"version={payload.version}",
    )
    return ShareAcknowledgeResponse(ok=True)


@router.get("/heartbeat", response_model=ShareHeartbeatResponse)
def share_heartbeat(request: Request):
    """Cheap session validity probe used by the idle heartbeat poller.

    Returns 401 if the session is gone so the frontend redirects to login.
    """
    sid = request.cookies.get(COOKIE_NAME)
    mgr = get_tunnel_manager()
    session = mgr.validate_session(sid)
    if session is None:
        mgr.record_heartbeat_unauth()
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    return ShareHeartbeatResponse(
        ok=True,
        session_id=session.session_id,
        last_active=session.last_active_time,
    )


@router.get("/claim/{token}", response_model=ShareClaimResponse)
def share_claim(token: str, request: Request):
    """One-time-view reveal of an invite's plaintext credentials.

    The plaintext passcode itself isn't stored — the hash is one-way — so
    this endpoint reveals everything *except* the passcode. The original
    plaintext is communicated by the admin via the share card; the claim
    URL exists to confirm scope and identity to the analyst without
    putting credentials in a chat tool that retains history.
    """
    ip = _client_ip(request)
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
        service_ids=invite.get("service_ids") if invite else [],
    )
