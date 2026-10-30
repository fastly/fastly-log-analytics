"""Admin session/backup/GDPR/settings/TOS routes and analyst passcode/TOS/heartbeat/claim routes."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response

import backend.core.oauth as oauth_registry
from backend.core import share_db
from backend.models.admin import (
    AuthConfigResponse,
    BackupExportPayload,
    BackupImportResponse,
    GdprErasePayload,
    GdprEraseResponse,
    OAuthProvidersResponse,
    SettingsPayload,
    ShareAcknowledgeResponse,
    ShareClaimResponse,
    ShareHeartbeatResponse,
    ShareLoginPayload,
    ShareLoginResponse,
    ShareLogoutResponse,
    ShareSettingsResponse,
    ShareTosPayload,
    ShareTosResponse,
    TosAckPayload,
    TosDocument,
    WordphraseResponse,
)
from backend.models.analytics import SharePurgeResponse
from backend.models.common import DEFAULT_ERROR_RESPONSES, OkResponse
from backend.routers.share_admin.tunnel_routes import _audit
from backend.utils.auth import (
    ANALYST_PENDING_SESSION_COOKIE,
    ANALYST_SESSION_COOKIE,
    analyst_cookie_is_secure,
    safe_audit_email,
    set_analyst_session_cookies,
)
from backend.utils.remote_access import client_ip
from backend.utils.tunnel import TunnelCapacityError, get_tunnel_manager

logger = logging.getLogger(__name__)

COOKIE_NAME = ANALYST_SESSION_COOKIE
PENDING_COOKIE_NAME = ANALYST_PENDING_SESSION_COOKIE
_safe_audit_email = safe_audit_email

sessions_admin_router = APIRouter()
share_auth_router = APIRouter(prefix="/api/share", tags=["share-auth"], responses=DEFAULT_ERROR_RESPONSES)


# ── Sessions, Backup, GDPR, Settings, TOS ───────────────────────────────────
@sessions_admin_router.post("/sessions/{session_id}/boot", response_model=OkResponse)
def boot_session(session_id: str, request: Request):
    if not get_tunnel_manager().boot_session(session_id, reason="admin boot"):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return {"ok": True}


@sessions_admin_router.post("/sessions/{session_id}/revoke", response_model=OkResponse)
def revoke_session(session_id: str, request: Request):
    return boot_session(session_id, request)


@sessions_admin_router.post("/backup/export")
def backup_export(payload: BackupExportPayload, request: Request):
    if len(payload.passphrase) < 12:
        raise HTTPException(
            status_code=400, detail={"error": "weak_passphrase", "message": "passphrase must be ≥12 characters"}
        )
    blob = share_db.export_backup(payload.passphrase)
    _audit(request, "BACKUP_EXPORTED", None, f"bytes={len(blob)}")
    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename=remote-share-backup-{share_db.iso_z_now()}.enc"},
    )


@sessions_admin_router.post("/backup/import", response_model=BackupImportResponse, response_model_exclude_unset=True)
async def backup_import(
    request: Request, file: UploadFile = File(...), passphrase: str = Form(...), mode: str = Form("skip-collisions")
):
    if mode not in ("skip-collisions", "merge-services-on-collision", "abort"):
        raise HTTPException(status_code=400, detail={"error": "invalid_mode"})
    blob = await file.read()
    try:
        result = await asyncio.to_thread(share_db.import_backup, blob, passphrase, mode=mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "import_failed", "message": str(exc)}) from exc
    await asyncio.to_thread(
        share_db.log_share_audit_event,
        event_type="BACKUP_IMPORTED",
        email=None,
        ip_address=client_ip(request, default="127.0.0.1"),
        details=str(result),
    )
    return result


@sessions_admin_router.post("/gdpr/erase", response_model=GdprEraseResponse, response_model_exclude_unset=True)
def gdpr_erase(payload: GdprErasePayload, request: Request):
    try:
        return share_db.gdpr_erase(payload.email, payload.reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc


@sessions_admin_router.get("/settings", response_model=ShareSettingsResponse, response_model_exclude_unset=True)
def get_settings():
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


@sessions_admin_router.patch("/settings", response_model=ShareSettingsResponse, response_model_exclude_unset=True)
def update_settings(payload: SettingsPayload):
    if payload.max_concurrent_analyst_sessions is not None:
        if payload.max_concurrent_analyst_sessions < 1:
            raise HTTPException(status_code=400, detail={"error": "invalid_value"})
        share_db.set_setting(share_db.MAX_CONCURRENT_ANALYST_SESSIONS_KEY, str(payload.max_concurrent_analyst_sessions))
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


@sessions_admin_router.get("/tos", response_model=ShareTosResponse)
def get_tos():
    return share_db.get_latest_tos() or {"version": None, "text": None, "published_at": None}


@sessions_admin_router.post("/tos", response_model=ShareTosResponse)
@sessions_admin_router.put("/tos", response_model=ShareTosResponse)
def publish_tos(payload: ShareTosPayload, request: Request):
    version, text = (payload.version or "").strip(), (payload.text or "").strip()
    if not version or not text:
        raise HTTPException(
            status_code=400, detail={"error": "invalid_request", "message": "version and text are required."}
        )
    share_db.publish_tos_version(version, text)
    _audit(request, "TOS_PUBLISH", None, f"version={version}")
    tos = share_db.get_latest_tos() or {"version": version, "text": text, "published_at": share_db.iso_z_now()}
    return {"ok": True, **tos}


@sessions_admin_router.get("/wordphrase", response_model=WordphraseResponse, response_model_exclude_unset=True)
def wordphrase():
    return {"passcode": share_db.generate_wordphrase()}


@sessions_admin_router.get("/oauth-providers", response_model=OAuthProvidersResponse)
def oauth_providers():
    return OAuthProvidersResponse(providers=[p.admin_dict() for p in oauth_registry.get_all_providers()])


@sessions_admin_router.post("/purge", response_model=SharePurgeResponse, response_model_exclude_unset=True)
def trigger_share_purge(
    audit_retention_days: int | None = Query(
        default=None,
        ge=1,
        description="Audit retention window in days (defaults to share_audit_retention_days setting or 90)",
    ),
    max_idle_session_days: int = Query(
        default=30, ge=1, description="Max days an analyst session can remain idle before being pruned"
    ),
):
    if audit_retention_days is None:
        raw = share_db.get_setting(share_db.SHARE_AUDIT_RETENTION_DAYS_KEY, "90")
        try:
            audit_retention_days = max(1, int(raw or "90"))
        except (TypeError, ValueError):
            audit_retention_days = 90
    stats = share_db.purge_all_share_records(
        audit_retention_days=audit_retention_days, max_idle_session_days=max_idle_session_days
    )
    return {"ok": True, **stats}


# ── Analyst Auth Routes (/api/share/*) ──────────────────────────────────────
@share_auth_router.get("/auth-config", response_model=AuthConfigResponse)
def share_auth_config():
    return AuthConfigResponse(
        passcode_enabled=oauth_registry.passcode_login_enabled(),
        providers=[p.public_dict() for p in oauth_registry.get_enabled_providers()],
    )


@share_auth_router.post("/login", response_model=ShareLoginResponse)
def share_login(payload: ShareLoginPayload, request: Request, response: Response):
    ip = client_ip(request)
    user_agent = request.headers.get("user-agent", "")
    headers = {k.lower(): v for k, v in request.headers.items()}
    if not oauth_registry.passcode_login_enabled():
        _audit(None, "LOGIN_FAIL", _safe_audit_email(payload.email), "passcode login disabled", ip=ip)
        raise HTTPException(status_code=403, detail={"error": "passcode_login_disabled"})
    mgr = get_tunnel_manager()
    locked, remaining = mgr.check_rate_limit(ip)
    if locked:
        _audit(None, "LOGIN_FAIL", _safe_audit_email(payload.email), f"locked out (remaining {remaining}s)", ip=ip)
        raise HTTPException(
            status_code=429,
            detail={"error": "rate_limited", "retry_after_s": remaining},
            headers={"Retry-After": str(remaining)},
        )
    invite = share_db.get_remote_invite_by_email_passcode(payload.email, payload.passcode)
    if invite is None:
        mgr.record_login_failure(ip, payload.email)
        _audit(None, "LOGIN_FAIL", _safe_audit_email(payload.email), "invalid credentials", ip=ip)
        raise HTTPException(status_code=401, detail={"error": "invalid_credentials"})
    if not share_db.ip_in_whitelist(ip, invite.get("ip_whitelist")):
        _audit(None, "LOGIN_FAIL", invite["email"], "IP not in whitelist", ip=ip)
        raise HTTPException(status_code=403, detail={"error": "ip_not_whitelisted"})
    cap = share_db.get_max_concurrent_sessions()
    try:
        session = mgr.create_session(invite=invite, ip_address=ip, user_agent=user_agent, headers=headers, cap=cap)
    except TunnelCapacityError as e:
        _audit(None, "LOGIN_FAIL", invite["email"], f"global session cap exceeded ({cap})", ip=ip)
        raise HTTPException(
            status_code=503, detail={"error": "capacity_exceeded", "current": e.current, "cap": e.cap}
        ) from e
    _audit(None, "LOGIN_SUCCESS", invite["email"], f"session={session.session_id[:8]}…", ip=ip)
    tos = share_db.get_latest_tos()
    tos_pending = bool(
        tos and (invite.get("tos_accepted_at") is None or (invite.get("tos_version") or "") != tos["version"])
    )
    set_analyst_session_cookies(
        response,
        session.session_id,
        tos_pending=tos_pending,
        secure=analyst_cookie_is_secure(request, mgr.public_url()),
        samesite="strict",
    )
    return ShareLoginResponse(
        ok=True,
        name=session.name,
        email=session.email,
        tos_pending=tos_pending,
        tos=TosDocument(version=tos["version"], text=tos["text"]) if (tos_pending and tos) else None,
        service_ids=session.service_ids,
        redirect="/share-login/acknowledge" if tos_pending else "/dashboard",
    )


@share_auth_router.post("/logout", response_model=ShareLogoutResponse)
def share_logout(request: Request, response: Response):
    sid = request.cookies.get(COOKIE_NAME) or request.cookies.get(PENDING_COOKIE_NAME)
    if sid:
        get_tunnel_manager().boot_session(sid, reason="analyst logout")
    response.delete_cookie(COOKIE_NAME, path="/")
    response.delete_cookie(PENDING_COOKIE_NAME, path="/")
    return ShareLogoutResponse(ok=True)


def _require_analyst_session(request: Request, *, pending_first: bool = True):
    first, second = (PENDING_COOKIE_NAME, COOKIE_NAME) if pending_first else (COOKIE_NAME, PENDING_COOKIE_NAME)
    sid = request.cookies.get(first) or request.cookies.get(second)
    mgr = get_tunnel_manager()
    session = mgr.validate_session(sid)
    if session is None:
        if not pending_first:
            mgr.record_heartbeat_unauth()
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    return mgr, session


@share_auth_router.get("/tos", response_model=TosDocument)
def share_get_tos(request: Request):
    _, _ = _require_analyst_session(request, pending_first=True)
    tos = share_db.get_latest_tos()
    if not tos:
        raise HTTPException(status_code=404, detail={"error": "no_tos"})
    return TosDocument(version=tos["version"], text=tos["text"])


@share_auth_router.post("/acknowledge", response_model=ShareAcknowledgeResponse)
def share_acknowledge_tos(payload: TosAckPayload, request: Request, response: Response):
    mgr, session = _require_analyst_session(request, pending_first=True)
    tos = share_db.get_latest_tos()
    if tos and payload.version != tos["version"]:
        raise HTTPException(status_code=400, detail={"error": "invalid_tos_version"})
    share_db.mark_tos_accepted(session.invite_id, payload.version)
    _audit(None, "TOS_ACCEPTED", session.email, f"version={payload.version}", ip=session.ip_address)
    new_sid = mgr.rotate_session_id(session.session_id)
    if new_sid is None:
        raise HTTPException(status_code=401, detail={"error": "unauthenticated"})
    response.set_cookie(
        key=COOKIE_NAME,
        value=new_sid,
        httponly=True,
        secure=analyst_cookie_is_secure(request, mgr.public_url()),
        samesite="strict",
        max_age=24 * 60 * 60,
        path="/",
    )
    response.delete_cookie(PENDING_COOKIE_NAME, path="/")
    return ShareAcknowledgeResponse(ok=True)


@share_auth_router.get("/heartbeat", response_model=ShareHeartbeatResponse)
def share_heartbeat(request: Request):
    mgr, session = _require_analyst_session(request, pending_first=False)
    if request.headers.get("x-user-active") == "1":
        mgr.touch_session(session.session_id, last_activity="heartbeat (active)")
    return ShareHeartbeatResponse(ok=True, last_active=session.last_active_time)


@share_auth_router.post("/claim/{token}", response_model=ShareClaimResponse)
def share_claim(token: str, request: Request):
    ip = client_ip(request)
    row = share_db.claim_token(token, ip)
    if row is None:
        _audit(None, "CLAIM_FAIL", None, f"token {token[:6]}… invalid/expired/claimed", ip=ip)
        raise HTTPException(status_code=404, detail={"error": "invalid_or_used"})
    invite = share_db.get_remote_invite(row["invite_id"])
    if invite is None or invite.get("revoked"):
        _audit(
            None,
            "CLAIM_FAIL",
            invite.get("email") if invite else None,
            f"token {token[:6]}… invite revoked/deleted",
            ip=ip,
        )
        raise HTTPException(status_code=410, detail={"error": "invite_gone"})
    _audit(None, "INVITE_CLAIMED", invite.get("email"), f"token {token[:6]}…", ip=ip)
    return ShareClaimResponse(
        ok=True,
        name=invite.get("name"),
        email=invite.get("email"),
        expires_at=invite.get("expires_at"),
        service_ids=invite.get("service_ids") or [],
    )
