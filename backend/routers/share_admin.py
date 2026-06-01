"""Admin endpoints for remote-share management (separate from admin.py).

Lives in its own router so the share endpoints can be globbed onto a
sub-prefix without entangling the ingest/sync admin surface area. All
endpoints here MUST be blocked from analyst sessions by the middleware in
``main.py`` — the prefix ``/api/admin/share`` is on the analyst-blocked
list.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field

from backend import config as svcconfig
from backend.core import share_db
from backend.utils.tunnel import get_tunnel_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/share", tags=["share-admin"])


# ── Status ──────────────────────────────────────────────────────────────────


@router.get("/status")
def share_status():
    mgr = get_tunnel_manager()
    state = mgr.state
    invites = share_db.get_remote_invites()
    sessions = [s.to_dict() for s in mgr.list_sessions()]
    audit = share_db.get_share_audit_logs(limit=50)
    services = []
    try:
        for cfg in svcconfig.list_configs():
            services.append(
                {
                    "service_id": cfg.get("service_id"),
                    "name": cfg.get("name") or cfg.get("service_id"),
                }
            )
    except Exception:
        logger.exception("[share_admin] could not list services")
    return {
        "sharing_active": mgr.is_sharing_active(),
        "use_tunnel": state.use_tunnel,
        "tunnel_url": state.tunnel_url,
        "public_endpoint": state.public_endpoint,
        "public_url": mgr.public_url(),
        "forward_port": state.forward_port,
        "started_at": state.started_at,
        "max_concurrent_sessions": share_db.get_max_concurrent_sessions(),
        "active_session_count": mgr.active_session_count(),
        "services": services,
        "invites": invites,
        "sessions": sessions,
        "audit_logs": audit,
        "rate_limits": mgr.get_rate_limit_snapshot(),
        "telemetry": mgr.get_telemetry(),
    }


# ── Audit log (filterable) ─────────────────────────────────────────────────


@router.get("/audit-logs")
def audit_logs(
    limit: int = 200,
    event_type: str | None = None,
    email: str | None = None,
    since: str | None = None,
    until: str | None = None,
):
    if limit < 1 or limit > 2000:
        raise HTTPException(status_code=400, detail={"error": "invalid_limit"})
    rows = share_db.get_share_audit_logs(
        limit=limit,
        event_type=event_type,
        email_substr=email,
        since=since,
        until=until,
    )
    return {"audit_logs": rows}


# ── Tunnel lifecycle ───────────────────────────────────────────────────────


class ShareStartPayload(BaseModel):
    use_tunnel: bool = True
    public_endpoint: str | None = None
    forward_port: int = 3000


@router.post("/start")
def share_start(payload: ShareStartPayload):
    mgr = get_tunnel_manager()
    try:
        result = mgr.start_sharing(
            use_tunnel=payload.use_tunnel,
            public_endpoint=payload.public_endpoint,
            forward_port=payload.forward_port,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if "port" in msg.lower() and "not bound" in msg.lower():
            raise HTTPException(
                status_code=409,
                detail={"error": "port_unavailable", "hint": msg},
            ) from exc
        raise HTTPException(status_code=500, detail={"error": "tunnel_start_failed", "message": msg}) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    mgr.start_sleep_listener()
    return result


@router.post("/stop")
def share_stop():
    mgr = get_tunnel_manager()
    mgr.stop_sharing()
    return {"ok": True}


@router.post("/panic")
def share_panic():
    return get_tunnel_manager().panic()


# ── Invites ────────────────────────────────────────────────────────────────


class InvitePayload(BaseModel):
    name: str
    email: str
    passcode: str
    duration_hours: int | None = Field(default=24)
    ip_whitelist: str | None = None
    service_ids: list[str] = Field(default_factory=list)
    pii_policy: dict | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None


@router.post("/invites")
def create_invite(payload: InvitePayload, request: Request):
    from datetime import UTC, datetime, timedelta

    expires_at = None
    if payload.duration_hours is not None and payload.duration_hours > 0:
        expires_at = share_db.iso_z(datetime.now(UTC) + timedelta(hours=int(payload.duration_hours)))
    try:
        invite = share_db.create_remote_invite(
            name=payload.name,
            email=payload.email,
            passcode=payload.passcode,
            expires_at_utc=expires_at,
            ip_whitelist=payload.ip_whitelist,
            service_ids=payload.service_ids,
            pii_policy=payload.pii_policy,
            query_window_hours=payload.query_window_hours,
            query_start_time=payload.query_start_time,
            query_end_time=payload.query_end_time,
        )
    except share_db.WeakPasscodeError as exc:
        raise HTTPException(status_code=400, detail={"error": "weak_passcode", "message": str(exc)}) from exc
    except (share_db.InvalidNameError, share_db.InvalidEmailError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_input", "message": str(exc)}) from exc
    except (share_db.InvalidPiiPolicyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc

    share_db.log_share_audit_event(
        event_type="INVITE_CREATE",
        email=invite["email"],
        ip_address=request.client.host if request.client else "127.0.0.1",
        details=f"invite_id={invite['id']} services={','.join(payload.service_ids)}",
    )
    return invite


class ServiceScopePayload(BaseModel):
    service_ids: list[str]


@router.patch("/invites/{invite_id}/services")
def update_invite_services(invite_id: str, payload: ServiceScopePayload):
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    share_db.update_remote_invite_services(invite_id, payload.service_ids)
    return share_db.get_remote_invite(invite_id)


class PasscodePayload(BaseModel):
    passcode: str


@router.patch("/invites/{invite_id}/passcode")
def update_invite_passcode(invite_id: str, payload: PasscodePayload, request: Request):
    """Rotate the passcode on an existing invite.

    Use when an analyst forgets their passcode — admin can set a new one and
    re-send the share card, no need to delete + recreate the invite (which
    would lose its audit history and any TOS acceptance).
    """
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    try:
        share_db.update_remote_invite_passcode(invite_id, payload.passcode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"error": str(e)})
    share_db.log_share_audit_event(
        event_type="INVITE_PASSCODE_UPDATE",
        email=None,
        ip_address=request.client.host if request.client else "127.0.0.1",
        details=f"invite_id={invite_id}",
    )
    return {"ok": True}


@router.post("/invites/{invite_id}/revoke")
def revoke_invite(invite_id: str, request: Request):
    if not share_db.revoke_remote_invite(invite_id):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite revoked")
    share_db.log_share_audit_event(
        event_type="INVITE_REVOKE",
        email=None,
        ip_address=request.client.host if request.client else "127.0.0.1",
        details=f"invite_id={invite_id} booted_sessions={booted}",
    )
    return {"ok": True, "booted_sessions": booted}


@router.delete("/invites/{invite_id}")
def delete_invite(invite_id: str, request: Request):
    """Hard-delete an invite row. Boots any live sessions first (cascade would
    otherwise leave the TunnelManager holding a stale reference), then deletes.
    Audit logs survive the cascade and retain the deletion trail.
    """
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite deleted")
    share_db.delete_remote_invite(invite_id)
    share_db.log_share_audit_event(
        event_type="INVITE_DELETE",
        email=None,
        ip_address=request.client.host if request.client else "127.0.0.1",
        details=f"invite_id={invite_id} booted_sessions={booted}",
    )
    return {"ok": True, "booted_sessions": booted}


@router.post("/invites/{invite_id}/claim-token")
def issue_claim_token(invite_id: str):
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    token = share_db.create_claim_token(invite_id, ttl_hours=24)
    return {"token": token}


# ── Sessions ────────────────────────────────────────────────────────────────


@router.post("/sessions/{session_id}/boot")
def boot_session(session_id: str, request: Request):
    ok = get_tunnel_manager().boot_session(session_id, reason="admin boot")
    if not ok:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return {"ok": True}


# ── Backup / Restore ────────────────────────────────────────────────────────


class BackupExportPayload(BaseModel):
    passphrase: str


@router.post("/backup/export")
def backup_export(payload: BackupExportPayload, request: Request):
    if len(payload.passphrase) < 12:
        raise HTTPException(
            status_code=400,
            detail={"error": "weak_passphrase", "message": "passphrase must be ≥12 characters"},
        )
    blob = share_db.export_backup(payload.passphrase)
    share_db.log_share_audit_event(
        event_type="BACKUP_EXPORTED",
        email=None,
        ip_address=request.client.host if request.client else "127.0.0.1",
        details=f"bytes={len(blob)}",
    )
    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename=remote-share-backup-{share_db.iso_z_now()}.enc",
        },
    )


@router.post("/backup/import")
async def backup_import(
    request: Request,
    file: UploadFile = File(...),
    passphrase: str = Form(...),
    mode: str = Form("skip-collisions"),
):
    if mode not in ("skip-collisions", "merge-services-on-collision", "abort"):
        raise HTTPException(status_code=400, detail={"error": "invalid_mode"})
    blob = await file.read()
    try:
        result = share_db.import_backup(blob, passphrase, mode=mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "import_failed", "message": str(exc)}) from exc
    share_db.log_share_audit_event(
        event_type="BACKUP_IMPORTED",
        email=None,
        ip_address=request.client.host if request.client else "127.0.0.1",
        details=str(result),
    )
    return result


# ── GDPR right-to-be-forgotten ──────────────────────────────────────────────


class GdprErasePayload(BaseModel):
    email: str
    reason: str


@router.post("/gdpr/erase")
def gdpr_erase(payload: GdprErasePayload, request: Request):
    try:
        result = share_db.gdpr_erase(payload.email, payload.reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    # boot any live sessions for that email (now-gone invite_id)
    return result


# ── Settings ────────────────────────────────────────────────────────────────


class SettingsPayload(BaseModel):
    max_concurrent_analyst_sessions: int | None = None


@router.patch("/settings")
def update_settings(payload: SettingsPayload):
    if payload.max_concurrent_analyst_sessions is not None:
        if payload.max_concurrent_analyst_sessions < 1:
            raise HTTPException(status_code=400, detail={"error": "invalid_value"})
        share_db.set_setting("max_concurrent_analyst_sessions", str(payload.max_concurrent_analyst_sessions))
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


# ── Wordphrase generator (used by admin invite form) ───────────────────────


@router.get("/wordphrase")
def wordphrase():
    return {"passcode": share_db.generate_wordphrase()}
