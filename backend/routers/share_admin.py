"""Admin and analyst-auth endpoints for remote-share management."""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import RedirectResponse, Response
from joserfc import jwt
from joserfc.jwk import RSAKey

import backend.core.oauth as oauth_registry
import backend.core.oauth as oidc
from backend import config as svcconfig
from backend.config import list_configs
from backend.core import share_db
from backend.core.oauth import OIDCError
from backend.core.request_context import require_admin
from backend.models.admin import (
    AuthConfigResponse,
    BackupExportPayload,
    BackupImportResponse,
    ClaimTokenResponse,
    GdprErasePayload,
    GdprEraseResponse,
    InviteMutationAck,
    InvitePayload,
    InviteRecord,
    OAuthProvidersResponse,
    PasscodePayload,
    PasscodeRegenerateResponse,
    PiiPolicyPayload,
    ServiceScopePayload,
    SettingsPayload,
    ShareAcknowledgeResponse,
    ShareAuditLogsResponse,
    ShareBannerResponse,
    ShareClaimResponse,
    ShareHeartbeatResponse,
    ShareInvitesResponse,
    ShareLiveResponse,
    ShareLoginPayload,
    ShareLoginResponse,
    ShareLogoutResponse,
    SharePanicResponse,
    ShareSettingsResponse,
    ShareStartPayload,
    ShareStartResponse,
    ShareStatusResponse,
    ShareTosPayload,
    ShareTosResponse,
    SharingDeployRequest,
    SharingDeployResponse,
    SharingPolicyPayload,
    SharingTeardownRequest,
    TosAckPayload,
    TosDocument,
    WordphraseResponse,
)
from backend.models.analytics import SharePurgeResponse
from backend.models.common import DEFAULT_ERROR_RESPONSES, OkMessageResponse, OkResponse
from backend.provision.fastly_api import delete_remote_frontend, deploy_remote_frontend
from backend.utils.auth import (
    ANALYST_PENDING_SESSION_COOKIE,
    ANALYST_SESSION_COOKIE,
    analyst_cookie_is_secure,
    safe_audit_email,
    set_analyst_session_cookies,
)
from backend.utils.remote_access import client_ip
from backend.utils.router_utils import make_error
from backend.utils.tunnel import TunnelCapacityError, build_share_live_payload, get_tunnel_manager

logger = logging.getLogger(__name__)
flow_state = oauth_registry
router = APIRouter()
admin_router = APIRouter(
    prefix="/api/admin/share",
    tags=["share-admin"],
    dependencies=[Depends(require_admin)],
    responses=DEFAULT_ERROR_RESPONSES,
)
share_auth_router = APIRouter(prefix="/api/share", tags=["share-auth"], responses=DEFAULT_ERROR_RESPONSES)
sharing_domain_router = APIRouter(
    prefix="/api/sharing", tags=["sharing"], dependencies=[Depends(require_admin)], responses=DEFAULT_ERROR_RESPONSES
)
COOKIE_NAME = ANALYST_SESSION_COOKIE
PENDING_COOKIE_NAME = ANALYST_PENDING_SESSION_COOKIE
_safe_audit_email = safe_audit_email


def _require_invite(invite_id: str) -> dict:
    invite = share_db.get_remote_invite(invite_id)
    if invite is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return invite


def _audit(
    request: Request | None,
    event_type: str,
    email: str | None = None,
    details: str | None = None,
    *,
    ip: str | None = None,
) -> None:
    resolved_ip = ip if ip is not None else (client_ip(request, default="127.0.0.1") if request else "127.0.0.1")
    share_db.log_share_audit_event(event_type=event_type, email=email, ip_address=resolved_ip, details=details or "")


# ── Status & Audit ──────────────────────────────────────────────────────────
@admin_router.get("/banner", response_model=ShareBannerResponse, response_model_exclude_unset=True)
def share_banner():
    mgr = get_tunnel_manager()
    return {"sharing_active": mgr.is_sharing_active(), "public_url": mgr.public_url()}


def _list_invites_with_last_login() -> list[dict]:
    invites = share_db.get_remote_invites()
    last_login_by_email = share_db.get_last_login_by_email()
    for inv in invites:
        inv["last_login_at"] = last_login_by_email.get((inv.get("email") or "").strip().lower())
    return invites


def build_share_status() -> dict:
    mgr = get_tunnel_manager()
    state = mgr.state
    services = []
    try:
        for cfg in svcconfig.list_configs():
            rf = cfg.get("remote_frontend") or {}
            services.append(
                {
                    "service_id": cfg.get("service_id"),
                    "name": cfg.get("name") or cfg.get("service_id"),
                    "remote_frontend_deployed": bool(rf),
                    "sharing_domain": rf.get("domain_name") if rf else None,
                    "remote_service_id": rf.get("service_id") if rf else None,
                }
            )
    except Exception:
        logger.exception("[share_admin] could not list services")
    return {
        "sharing_active": mgr.is_sharing_active(),
        "public_endpoint": state.public_endpoint,
        "public_url": mgr.public_url(),
        "forward_port": state.forward_port,
        "started_at": state.started_at,
        "max_concurrent_sessions": share_db.get_max_concurrent_sessions(),
        "active_session_count": mgr.active_session_count(),
        "services": services,
        "invites": _list_invites_with_last_login(),
        "sessions": [s.to_dict() for s in mgr.list_sessions()],
        "audit_logs": share_db.get_share_audit_logs(limit=50),
        "rate_limits": mgr.get_rate_limit_snapshot(),
        "telemetry": mgr.get_telemetry(),
    }


@admin_router.get("/status", response_model=ShareStatusResponse, response_model_exclude_unset=True)
def share_status():
    return build_share_status()


@admin_router.get("/live", response_model=ShareLiveResponse, response_model_exclude_unset=True)
def share_live():
    return build_share_live_payload()


@admin_router.get("/audit-logs", response_model=ShareAuditLogsResponse, response_model_exclude_unset=True)
def audit_logs(
    limit: int = Query(default=200, ge=1, le=2000),
    event_type: str | None = None,
    email: str | None = None,
    since: str | None = None,
    until: str | None = None,
):
    rows = share_db.get_share_audit_logs(
        limit=limit, event_type=event_type, email_substr=email, since=since, until=until
    )
    return {"audit_logs": rows}


# ── Tunnel lifecycle ───────────────────────────────────────────────────────
@admin_router.post("/start", response_model=ShareStartResponse, response_model_exclude_unset=True)
def share_start(payload: ShareStartPayload):
    mgr = get_tunnel_manager()
    try:
        return mgr.start_sharing(public_endpoint=payload.public_endpoint, forward_port=payload.forward_port)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc


@admin_router.post("/stop", response_model=OkResponse)
def share_stop():
    get_tunnel_manager().stop_sharing()
    return {"ok": True}


@admin_router.post("/panic", response_model=SharePanicResponse, response_model_exclude_unset=True)
def share_panic():
    return get_tunnel_manager().panic()


# ── Invites ────────────────────────────────────────────────────────────────
@admin_router.get("/invites", response_model=ShareInvitesResponse, response_model_exclude_unset=True)
def list_invites():
    return {"invites": _list_invites_with_last_login()}


@admin_router.post("/invites", response_model=InviteRecord, response_model_exclude_unset=True)
def create_invite(payload: InvitePayload, request: Request):
    if not payload.service_ids:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "Select at least one service for the invite."},
        )
    if payload.auth_method == "oauth":
        if not payload.oauth_provider:
            raise HTTPException(
                status_code=400,
                detail={"error": "invalid_request", "message": "Choose an identity provider for an SSO invite."},
            )
        if oauth_registry.get_provider(payload.oauth_provider) is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "invalid_request",
                    "message": f"Identity provider '{payload.oauth_provider}' is not configured.",
                },
            )
    expires_at = (
        share_db.iso_z(datetime.now(UTC) + timedelta(hours=int(payload.duration_hours)))
        if payload.duration_hours is not None and payload.duration_hours > 0
        else None
    )
    try:
        invite = share_db.create_remote_invite(
            name=payload.name,
            email=payload.email,
            passcode=payload.passcode,
            expires_at_utc=expires_at,
            ip_whitelist=payload.ip_whitelist,
            service_ids=payload.service_ids,
            pii_policy=payload.pii_policy.model_dump() if payload.pii_policy is not None else None,
            query_window_hours=payload.query_window_hours,
            query_start_time=payload.query_start_time,
            query_end_time=payload.query_end_time,
            allow_concurrent_sessions=payload.allow_concurrent_sessions,
            auth_method=payload.auth_method,
            oauth_provider=payload.oauth_provider,
        )
    except share_db.WeakPasscodeError as exc:
        raise HTTPException(status_code=400, detail={"error": "weak_passcode", "message": str(exc)}) from exc
    except (share_db.InvalidNameError, share_db.InvalidEmailError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_input", "message": str(exc)}) from exc
    except (share_db.InvalidPiiPolicyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    auth_detail = (
        f" auth={payload.auth_method} provider={payload.oauth_provider}" if payload.auth_method == "oauth" else ""
    )
    _audit(
        request,
        "INVITE_CREATE",
        invite["email"],
        f"invite_id={invite['id']} services={','.join(payload.service_ids)}{auth_detail}",
    )
    return invite


@admin_router.patch("/invites/{invite_id}/services", response_model=InviteRecord, response_model_exclude_unset=True)
def update_invite_services(invite_id: str, payload: ServiceScopePayload):
    _require_invite(invite_id)
    if not payload.service_ids:
        raise HTTPException(
            status_code=400, detail={"error": "invalid_request", "message": "An invite must keep at least one service."}
        )
    share_db.update_remote_invite_services(invite_id, payload.service_ids)
    return share_db.get_remote_invite(invite_id)


@admin_router.patch("/invites/{invite_id}/pii", response_model=InviteRecord, response_model_exclude_unset=True)
def update_invite_pii(invite_id: str, payload: PiiPolicyPayload, request: Request):
    invite = _require_invite(invite_id)
    try:
        share_db.update_remote_invite_pii(invite_id, {"mask_ips": payload.mask_ips})
    except (share_db.InvalidPiiPolicyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    _audit(request, "INVITE_UPDATE_PII", invite["email"], f"invite_id={invite_id} mask_ips={payload.mask_ips}")
    return share_db.get_remote_invite(invite_id)


@admin_router.patch("/invites/{invite_id}/sharing", response_model=InviteRecord, response_model_exclude_unset=True)
def update_invite_sharing(invite_id: str, payload: SharingPolicyPayload, request: Request):
    invite = _require_invite(invite_id)
    share_db.set_invite_concurrent_sessions(invite_id, payload.allow_concurrent_sessions)
    _audit(
        request,
        "INVITE_UPDATE_SHARING",
        invite["email"],
        f"invite_id={invite_id} allow_concurrent_sessions={payload.allow_concurrent_sessions}",
    )
    return share_db.get_remote_invite(invite_id)


@admin_router.patch("/invites/{invite_id}/passcode", response_model=OkResponse)
def update_invite_passcode(invite_id: str, payload: PasscodePayload, request: Request):
    _require_invite(invite_id)
    try:
        share_db.update_remote_invite_passcode(invite_id, payload.passcode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_passcode", str(e)))
    _audit(request, "INVITE_PASSCODE_UPDATE", None, f"invite_id={invite_id}")
    return {"ok": True}


@admin_router.post(
    "/invites/{invite_id}/regenerate-passcode",
    response_model=PasscodeRegenerateResponse,
    response_model_exclude_unset=True,
)
def regenerate_invite_passcode(invite_id: str, request: Request):
    invite = _require_invite(invite_id)
    new_passcode = share_db.generate_wordphrase()
    share_db.update_remote_invite_passcode(invite_id, new_passcode)
    _audit(request, "INVITE_PASSCODE_UPDATE", invite.get("email"), f"invite_id={invite_id} regenerated=true")
    return {"ok": True, "invite_id": invite_id, "passcode": new_passcode}


@admin_router.post("/invites/{invite_id}/revoke", response_model=InviteMutationAck, response_model_exclude_unset=True)
def revoke_invite(invite_id: str, request: Request):
    if not share_db.revoke_remote_invite(invite_id):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite revoked")
    _audit(request, "INVITE_REVOKE", None, f"invite_id={invite_id} booted_sessions={booted}")
    return {"ok": True, "booted_sessions": booted}


@admin_router.post("/invites/{invite_id}/reactivate", response_model=InviteRecord, response_model_exclude_unset=True)
def reactivate_invite(invite_id: str, request: Request):
    if not share_db.reactivate_remote_invite(invite_id):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    invite = share_db.get_remote_invite(invite_id) or {}
    _audit(request, "INVITE_REACTIVATE", invite.get("email"), f"invite_id={invite_id}")
    return {**invite, "ok": True}


@admin_router.delete("/invites/{invite_id}", response_model=InviteMutationAck, response_model_exclude_unset=True)
def delete_invite(invite_id: str, request: Request):
    _require_invite(invite_id)
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite deleted")
    share_db.delete_remote_invite(invite_id)
    _audit(request, "INVITE_DELETE", None, f"invite_id={invite_id} booted_sessions={booted}")
    return {"ok": True, "booted_sessions": booted}


@admin_router.post(
    "/invites/{invite_id}/claim-token", response_model=ClaimTokenResponse, response_model_exclude_unset=True
)
def issue_claim_token(invite_id: str):
    _require_invite(invite_id)
    return {"token": share_db.create_claim_token(invite_id, ttl_hours=24)}


# ── Sessions, Backup, GDPR, Settings, TOS ───────────────────────────────────
@admin_router.post("/sessions/{session_id}/boot", response_model=OkResponse)
def boot_session(session_id: str, request: Request):
    if not get_tunnel_manager().boot_session(session_id, reason="admin boot"):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return {"ok": True}


@admin_router.post("/sessions/{session_id}/revoke", response_model=OkResponse)
def revoke_session(session_id: str, request: Request):
    return boot_session(session_id, request)


@admin_router.post("/backup/export")
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


@admin_router.post("/backup/import", response_model=BackupImportResponse, response_model_exclude_unset=True)
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


@admin_router.post("/gdpr/erase", response_model=GdprEraseResponse, response_model_exclude_unset=True)
def gdpr_erase(payload: GdprErasePayload, request: Request):
    try:
        return share_db.gdpr_erase(payload.email, payload.reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc


@admin_router.get("/settings", response_model=ShareSettingsResponse, response_model_exclude_unset=True)
def get_settings():
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


@admin_router.patch("/settings", response_model=ShareSettingsResponse, response_model_exclude_unset=True)
def update_settings(payload: SettingsPayload):
    if payload.max_concurrent_analyst_sessions is not None:
        if payload.max_concurrent_analyst_sessions < 1:
            raise HTTPException(status_code=400, detail={"error": "invalid_value"})
        share_db.set_setting(share_db.MAX_CONCURRENT_ANALYST_SESSIONS_KEY, str(payload.max_concurrent_analyst_sessions))
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


@admin_router.get("/tos", response_model=ShareTosResponse)
def get_tos():
    return share_db.get_latest_tos() or {"version": None, "text": None, "published_at": None}


@admin_router.post("/tos", response_model=ShareTosResponse)
@admin_router.put("/tos", response_model=ShareTosResponse)
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


@admin_router.get("/wordphrase", response_model=WordphraseResponse, response_model_exclude_unset=True)
def wordphrase():
    return {"passcode": share_db.generate_wordphrase()}


@admin_router.get("/oauth-providers", response_model=OAuthProvidersResponse)
def oauth_providers():
    return OAuthProvidersResponse(providers=[p.admin_dict() for p in oauth_registry.get_all_providers()])


@admin_router.post("/purge", response_model=SharePurgeResponse, response_model_exclude_unset=True)
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


# ── Sharing Domain Deployment ────────────────────────────────────────────────
def _resolve_fastly_token(token_override: str | None) -> str:
    from backend.utils.router_utils import make_error, stored_fastly_token

    token = token_override
    if not token:
        for cfg in list_configs():
            key = stored_fastly_token(cfg)
            if key:
                token = key
                break
    if not token:
        env_token = os.environ.get("FASTLY_API_KEY")
        if env_token and env_token.strip():
            token = env_token.strip()
    if not token:
        raise HTTPException(status_code=400, detail=make_error("missing_api_token", "Fastly API Token is required"))
    return token


@sharing_domain_router.post("/deploy-frontend", response_model=SharingDeployResponse)
def deploy_frontend(payload: SharingDeployRequest):
    from backend.utils.router_utils import mutate_service_config, raise_internal

    token = _resolve_fastly_token(payload.token_override)
    try:
        res = deploy_remote_frontend(
            service_name=payload.service_name,
            domain_name=payload.domain_name,
            origin_host=payload.origin_host,
            origin_port=payload.origin_port,
            use_ssl=payload.use_ssl,
            token=token,
            override_host=payload.override_host,
        )
        if payload.service_id:
            remote_frontend_spec = {
                "service_id": res["service_id"],
                "version": res["version"],
                "domain_name": res["domain_name"],
                "origin_host": res["origin_host"],
            }
            try:
                mutate_service_config(
                    payload.service_id, lambda c: c.__setitem__("remote_frontend", remote_frontend_spec)
                )
            except Exception as e:
                logger.warning(f"Could not save remote_frontend to config of service {payload.service_id}: {e}")
        try:
            get_tunnel_manager().start_sharing(public_endpoint=f"https://{res['domain_name']}")
        except Exception as e:
            logger.warning(f"Could not automatically activate sharing for {res['domain_name']}: {e}")
        return res
    except Exception as exc:
        raise_internal(logger, exc)


@sharing_domain_router.post("/teardown-frontend", response_model=OkMessageResponse, response_model_exclude_unset=True)
def teardown_frontend(payload: SharingTeardownRequest):
    from backend.utils.router_utils import load_service_config, mutate_service_config, raise_internal

    token = _resolve_fastly_token(payload.token_override)
    try:
        cfg = load_service_config(payload.service_id)
        remote_frontend = cfg.get("remote_frontend")
        if not remote_frontend or not remote_frontend.get("service_id"):
            return {"ok": True, "message": "No remote frontend configured or already torn down"}
        remote_service_id = remote_frontend["service_id"]
        delete_remote_frontend(remote_service_id=remote_service_id, token=token)
        mutate_service_config(payload.service_id, lambda c: c.pop("remote_frontend", None))
        return {"ok": True, "message": f"Successfully deleted remote frontend service {remote_service_id}"}
    except HTTPException:
        raise
    except Exception as exc:
        raise_internal(logger, exc)


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


# ── Analyst OAuth / OIDC Login Routes (folded from share_oauth.py) ───────────

share_oauth_router = APIRouter(prefix="/api/share", tags=["share-oauth"], responses=DEFAULT_ERROR_RESPONSES)

# Granular OIDCError reasons that are token/identity verification failures
# (audit as OAUTH_VERIFY_FAIL); everything else is a callback/transport failure
# (OAUTH_CALLBACK_FAIL). Design §4 event table.
_VERIFY_REASONS = frozenset(
    {"bad_iss", "bad_aud", "expired", "nonce_mismatch", "alg_rejected", "bad_claim", "unverified_email", "wrong_domain"}
)

# Granular reason → coarse, user-facing oauth_error code. Anything not mapped
# collapses to the generic "auth_failed" (the frontend banner renders a generic
# "Sign-in failed" for any code outside its fixed allowlist — §5.2). The banner
# never reflects the raw param.
_USER_CODE = {
    "unverified_email": "unverified_email",
    "wrong_domain": "wrong_domain",
    "idp_unavailable": "idp_unavailable",
    "provider_unavailable": "idp_unavailable",
    "no_public_endpoint": "idp_unavailable",
}


def _user_code(reason: str) -> str:
    return _USER_CODE.get(reason, "auth_failed")


def _hash8(value: str) -> str:
    """Short, non-reversible tag for the CSRF ``state`` in the audit log — never
    log the raw state (§4)."""
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def _safe_return(target: str | None) -> str:
    """Relative-path allowlist for the post-login landing target (design §3.3).

    Must be a single-slash-rooted relative path. Rejects protocol-relative
    (``//host``), backslashes, and any scheme/absolute URL — defaults to the
    fixed ``/dashboard`` so a poisoned ``return`` can't become an open redirect.
    """
    if not target or not isinstance(target, str):
        return "/dashboard"
    if not target.startswith("/") or target.startswith("//"):
        return "/dashboard"
    if "\\" in target or "://" in target or "\n" in target or "\r" in target:
        return "/dashboard"
    return target


def _safe_invite_name(email: str) -> str:
    """Derive a validate_name-safe display name from an email local-part."""
    local = (email or "").split("@")[0]
    cleaned = "".join(c for c in local if c.isalnum() or c in " .,'-_").strip()
    return cleaned[:80] or "analyst"


def _auto_provision_invite(prov, email: str, ip: str) -> dict | None:
    """JIT-create an OAuth invite for a verified login with no pre-existing invite
    (design: trusted, org-restricted provider). Returns the enriched invite, or
    None if it can't be scoped/created. Only called when ``prov.auto_provision``."""
    service_ids = list(prov.auto_provision_service_ids) or svcconfig.list_service_ids()
    if not service_ids:
        logger.warning("[oauth] auto_provision enabled for %s but no services to scope to", prov.id)
        return None
    try:
        invite = share_db.create_remote_invite(
            name=_safe_invite_name(email),
            email=email,
            passcode=None,
            expires_at_utc=None,
            ip_whitelist=None,
            service_ids=service_ids,
            pii_policy={"mask_ips": prov.auto_provision_mask_ips},
            auth_method="oauth",
            oauth_provider=prov.id,
        )
    except Exception:
        logger.exception("[oauth] auto-provision failed for %s", _safe_audit_email(email))
        return None
    share_db.log_share_audit_event(
        event_type="INVITE_CREATE",
        email=invite["email"],
        ip_address=ip,
        details=f"auto-provisioned invite_id={invite['id']} provider={prov.id} services={','.join(service_ids)}",
    )
    return invite


def _redirect_uri() -> str | None:
    """The single fixed, pre-registered callback URL, derived from the tunnel
    ``public_endpoint`` (never templated from request input) — §2.6 / §3.3.

    ``OAUTH_REDIRECT_BASE`` overrides the base for environments where the callback
    origin isn't the tunnel endpoint (the E2E stack, where the browser lives on
    the frontend proxy origin). Unset in production → uses ``public_endpoint``.
    """
    base = os.getenv("OAUTH_REDIRECT_BASE", "").strip() or get_tunnel_manager().state.public_endpoint
    if not base:
        return None
    return base.rstrip("/") + "/api/share/oauth/callback"


def _login_redirect(user_code: str) -> RedirectResponse:
    resp = RedirectResponse(url=f"/share-login?oauth_error={user_code}", status_code=302)
    resp.delete_cookie(flow_state.COOKIE_NAME, path=flow_state.COOKIE_PATH)
    return resp


def _fail(
    ip: str,
    *,
    event: str,
    user_code: str,
    reason: str | None = None,
    details: str | None = None,
    email: str | None = None,
) -> RedirectResponse:
    """Write the audit row and return the failure redirect (clears flow-state)."""
    share_db.log_share_audit_event(
        event_type=event,
        email=email,
        ip_address=ip,
        details=details if details is not None else f"reason={reason}",
    )
    return _login_redirect(user_code)


def _fail_oidc(ip: str, reason: str, *, email: str | None = None) -> RedirectResponse:
    event = "OAUTH_VERIFY_FAIL" if reason in _VERIFY_REASONS else "OAUTH_CALLBACK_FAIL"
    return _fail(ip, event=event, user_code=_user_code(reason), reason=reason, email=email)


@share_oauth_router.get("/oauth/authorize")
def oauth_authorize(
    request: Request,
    provider: str = Query(...),
    return_target: str = Query("/dashboard", alias="return"),
) -> RedirectResponse:
    ip = client_ip(request)

    prov = oauth_registry.get_provider(provider)
    if prov is None or not prov.enabled:
        # Unknown / disabled / feature-off — no such SSO button should exist.
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="idp_unavailable", reason="provider_unavailable")

    redirect_uri = _redirect_uri()
    if redirect_uri is None:
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="idp_unavailable", reason="no_public_endpoint")

    try:
        metadata = oidc.get_metadata(prov)
        state = oidc.make_state_token()
        nonce = oidc.make_state_token()
        verifier = oidc.make_pkce_verifier()
        authorize_url = oidc.build_authorize_url(
            prov,
            metadata,
            redirect_uri=redirect_uri,
            state=state,
            nonce=nonce,
            code_challenge=oidc.pkce_challenge(verifier),
        )
        sealed = flow_state.seal_flow_state(
            {
                "provider": provider,
                "state": state,
                "nonce": nonce,
                "verifier": verifier,
                "redirect_uri": redirect_uri,
                "return": _safe_return(return_target),
            }
        )
    except OIDCError as e:
        return _fail_oidc(ip, e.reason)
    except flow_state.FlowStateError:
        # OAUTH_FLOW_STATE_SECRET unset while a provider is somehow configured —
        # fail closed rather than start a flow we can never complete.
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="idp_unavailable", reason="flow_state_unavailable")

    share_db.log_share_audit_event(
        event_type="OAUTH_AUTH_INIT",
        email=None,
        ip_address=ip,
        details=f"provider={provider} state={_hash8(state)}",
    )
    resp = RedirectResponse(url=authorize_url, status_code=302)
    resp.set_cookie(
        key=flow_state.COOKIE_NAME,
        value=sealed,
        max_age=flow_state.COOKIE_MAX_AGE_S,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="lax",  # REQUIRED: sent on the top-level redirect back from the IdP
        path=flow_state.COOKIE_PATH,
    )
    return resp


@share_oauth_router.get("/oauth/callback")
def oauth_callback(
    request: Request,
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
) -> RedirectResponse:
    ip = client_ip(request)

    # 1. Open + authenticate the flow-state cookie. Provider identity comes ONLY
    #    from this sealed cookie, never a query param (§2.1.2).
    try:
        fs = flow_state.open_flow_state(request.cookies.get(flow_state.COOKIE_NAME) or "")
    except flow_state.FlowStateError:
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="auth_failed", reason="missing_flow_state")

    if error:
        # The IdP redirected back with an error (user denied consent, etc.).
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="auth_failed", reason="idp_error")

    # 2. CSRF: query state must match the cookie state (constant-time).
    cookie_state = fs.get("state") or ""
    if not code or not state or not cookie_state or not hmac.compare_digest(state, cookie_state):
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="auth_failed", reason="csrf_state_mismatch")

    provider = fs.get("provider") or ""
    prov = oauth_registry.get_provider(provider)
    if prov is None or not prov.enabled:
        return _fail(ip, event="OAUTH_CALLBACK_FAIL", user_code="idp_unavailable", reason="provider_unavailable")

    # 3. Token exchange + id_token validation (signature / iss / aud / exp / iat /
    #    nbf / nonce / email_verified / hd). Every failure fails closed.
    try:
        metadata = oidc.get_metadata(prov)
        token = oidc.exchange_code(
            prov,
            metadata,
            code=code,
            code_verifier=fs.get("verifier") or "",
            redirect_uri=fs.get("redirect_uri") or "",
        )
        claims = oidc.verify_id_token(prov, metadata, token["id_token"], nonce=fs.get("nonce") or "")
    except OIDCError as e:
        # Use the verified email if we got one (we didn't past a verify failure).
        return _fail_oidc(ip, e.reason)

    email = claims.get("email") or ""
    sub = claims.get("sub") or ""
    safe_email = _safe_audit_email(email)

    # 4. Invite lookup (positive auth_method='oauth' gate) + subject pin. A
    #    missing invite AND a sub/provider mismatch produce a BYTE-IDENTICAL
    #    response (OAUTH_INVITE_NOT_FOUND → not_invited) — no user enumeration
    #    (§2.9). oauth_subject is pinned on first login and required thereafter.
    invite = share_db.get_remote_invite_oauth(email, provider)
    # Just-in-time provisioning: a trusted, org-restricted provider can auto-create
    # the invite on first login instead of requiring a pre-created one (§2.6.1 —
    # the invite-email allowlist is then replaced by the provider's org gate, which
    # runs above via email_verified + allowed_hd). Off unless configured.
    if invite is None and prov.auto_provision:
        invite = _auto_provision_invite(prov, email, ip)
    if invite is None or not sub or not share_db.bind_invite_oauth_subject(invite["id"], sub):
        return _fail(
            ip,
            event="OAUTH_INVITE_NOT_FOUND",
            user_code="not_invited",
            details=f"provider={provider}",
            email=safe_email,
        )

    # 5. Re-apply the SAME post-lookup gates the passcode path applies, so OAuth
    #    invites don't bypass the IP whitelist or the global concurrency cap
    #    (§2.5 / §6).
    if not share_db.ip_in_whitelist(ip, invite.get("ip_whitelist")):
        return _fail(
            ip,
            event="OAUTH_CALLBACK_FAIL",
            user_code="auth_failed",
            reason="ip_not_whitelisted",
            email=invite["email"],
        )

    mgr = get_tunnel_manager()
    # Capacity check happens atomically with the insert inside create_session
    # (cap=) — a separate check-then-act here would race concurrent OAuth
    # callbacks (or a callback racing a passcode login) past the cap.
    cap = share_db.get_max_concurrent_sessions()

    # 6. Success — create the session (inherits scope/mask/fingerprint) and set
    #    the analyst cookie via the SAME two-cookie TOS protocol as passcode
    #    login, so an OAuth analyst with an un-accepted TOS lands on
    #    /share-login/acknowledge (§5.5).
    user_agent = request.headers.get("user-agent", "")
    headers = {k.lower(): v for k, v in request.headers.items()}
    try:
        session = mgr.create_session(invite=invite, ip_address=ip, user_agent=user_agent, headers=headers, cap=cap)
    except TunnelCapacityError:
        return _fail(
            ip,
            event="OAUTH_CALLBACK_FAIL",
            user_code="auth_failed",
            reason=f"capacity_exceeded ({cap})",
            email=invite["email"],
        )

    tos = share_db.get_latest_tos()
    tos_pending = bool(
        tos and (invite.get("tos_accepted_at") is None or (invite.get("tos_version") or "") != tos["version"])
    )

    share_db.log_share_audit_event(
        event_type="LOGIN_SUCCESS_OAUTH",
        email=invite["email"],
        ip_address=ip,
        details=f"session={session.session_id[:8]}… provider={provider}",
    )

    dest = "/share-login/acknowledge" if tos_pending else _safe_return(fs.get("return"))
    resp = RedirectResponse(url=dest, status_code=302)
    # SameSite=Lax (NOT Strict, unlike the passcode path): the landing after
    # this 302 is a top-level navigation whose chain was INITIATED cross-site
    # by the IdP, so a Strict cookie would be withheld on that first request
    # and the dashboard/acknowledge page would see no session and bounce back
    # to /share-login. Lax is sent on top-level GET navigations, which is
    # exactly this case, and still blocks cross-site subresource/POST (CSRF).
    # On the TOS path, /api/share/acknowledge re-issues the full cookie from a
    # same-site context, so the session cookie only relies on Lax for the
    # single IdP-return hop.
    set_analyst_session_cookies(
        resp,
        session.session_id,
        tos_pending=tos_pending,
        secure=analyst_cookie_is_secure(request, mgr.public_url()),
        samesite="lax",
    )
    resp.delete_cookie(flow_state.COOKIE_NAME, path=flow_state.COOKIE_PATH)
    return resp


# ── In-process mock OIDC provider (E2E/dev ONLY, gated on OAUTH_MOCK_IDP=1) ──

mock_idp_router = APIRouter(prefix="/mock-idp", tags=["mock-idp"])

_KID = "mock-idp-key"
_key: RSAKey | None = None


def mock_idp_enabled() -> bool:
    return os.getenv("OAUTH_MOCK_IDP") == "1"


def _signing_key() -> RSAKey:
    global _key
    if _key is None:
        _key = RSAKey.generate_key(2048, {"kid": _KID, "use": "sig", "alg": "RS256"})
    return _key


def _issuer() -> str:
    return os.getenv("OAUTH_MOCK_IDP_ISSUER", "http://127.0.0.1:18004/mock-idp")


def _b64url(obj: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()


def _unb64url(s: str) -> dict:
    pad = "=" * (-len(s) % 4)
    return json.loads(base64.urlsafe_b64decode((s + pad).encode()))


@mock_idp_router.get("/.well-known/openid-configuration")
def discovery():
    b = _issuer()
    return {
        "issuer": b,
        "authorization_endpoint": f"{b}/authorize",
        "token_endpoint": f"{b}/token",
        "jwks_uri": f"{b}/jwks",
        "id_token_signing_alg_values_supported": ["RS256"],
    }


@mock_idp_router.get("/jwks")
def jwks():
    return {"keys": [_signing_key().as_dict(private=False)]}


@mock_idp_router.get("/authorize")
def authorize(request: Request, redirect_uri: str, state: str, nonce: str):
    """Auto-approve: bounce straight back to redirect_uri with an authorization
    code that carries the (test-controlled) identity + the request nonce."""
    email = request.cookies.get("mock_idp_email") or os.getenv("OAUTH_MOCK_IDP_EMAIL", "e2e-analyst@example.com")
    email_verified = (request.cookies.get("mock_idp_email_verified") or "true").lower() != "false"
    code = _b64url({"email": email, "nonce": nonce, "sub": f"mock-sub-{email}", "ev": email_verified})
    sep = "&" if "?" in redirect_uri else "?"
    return RedirectResponse(f"{redirect_uri}{sep}code={code}&state={state}", status_code=302)


@mock_idp_router.post("/token")
async def token(request: Request):
    form = await request.form()
    try:
        data = _unb64url(str(form.get("code") or ""))
    except Exception:
        from fastapi.responses import JSONResponse

        return JSONResponse({"error": "invalid_grant"}, status_code=400)
    now = int(time.time())
    claims = {
        "iss": _issuer(),
        "aud": os.getenv("OAUTH_GOOGLE_CLIENT_ID", "mock-client-id"),
        "sub": data["sub"],
        "email": data["email"],
        "email_verified": bool(data.get("ev", True)),
        "nonce": data["nonce"],
        "iat": now,
        "exp": now + 300,
    }
    id_token = jwt.encode({"alg": "RS256", "kid": _KID}, claims, _signing_key())
    return {"access_token": "mock-access-token", "token_type": "Bearer", "id_token": id_token}


router.include_router(admin_router)
router.include_router(share_auth_router)
router.include_router(share_oauth_router)
