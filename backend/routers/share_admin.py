"""Admin and analyst-auth endpoints for remote-share management."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response

import backend as _backend_pkg
from backend import config as svcconfig
from backend.config import list_configs
from backend.core import share_db
from backend.core.oauth import registry as oauth_registry
from backend.deps import require_admin
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin_actions import SharePurgeResponse
from backend.models.common import OkResponse
from backend.models.services import OkMessageResponse
from backend.models.share_admin import (
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

if hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.setdefault("backend.routers.share_auth", "backend.routers.share_admin")
    _backend_pkg._MODULE_ALIASES.setdefault("backend.routers.remote_share", "backend.routers.share_admin")
logger = logging.getLogger(__name__)
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


router.include_router(admin_router)
router.include_router(share_auth_router)
_self_mod = sys.modules[__name__]
sys.modules["backend.routers.share_auth"] = _self_mod
sys.modules["backend.routers.remote_share"] = _self_mod
_routers_pkg = sys.modules.get("backend.routers")
if _routers_pkg is not None:
    setattr(_routers_pkg, "share_admin", _self_mod)
    setattr(_routers_pkg, "share_auth", _self_mod)
    setattr(_routers_pkg, "remote_share", _self_mod)
