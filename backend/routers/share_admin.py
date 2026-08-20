"""Admin endpoints for remote-share management (separate from admin.py).

Lives in its own router so the share endpoints can be globbed onto a
sub-prefix without entangling the ingest/sync admin surface area. All
endpoints here MUST be blocked from analyst sessions by the middleware in
``main.py`` — the prefix ``/api/admin/share`` is on the analyst-blocked
list.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response

from backend import config as svcconfig
from backend.core import share_db
from backend.core.oauth import registry as oauth_registry
from backend.models.common import OkResponse
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.share_admin import (
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
    PiiPolicyPayload,
    ServiceScopePayload,
    SettingsPayload,
    ShareAuditLogsResponse,
    ShareBannerResponse,
    ShareLiveResponse,
    SharePanicResponse,
    ShareSettingsResponse,
    ShareStartPayload,
    ShareStartResponse,
    ShareStatusResponse,
    SharingPolicyPayload,
    WordphraseResponse,
)
from backend.utils.remote_access import client_ip
from backend.utils.router_utils import make_error
from backend.utils.tunnel import build_share_live_payload, get_tunnel_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/share", tags=["share-admin"], responses=DEFAULT_ERROR_RESPONSES)
# ── Status ──────────────────────────────────────────────────────────────────


@router.get("/banner", response_model=ShareBannerResponse, response_model_exclude_unset=True)
def share_banner():
    """Tiny payload (~80B) for the global share-status banner.

    Used by frontend/hooks/useShareStatusBanner.tsx — polls every 15s on
    every page that mounts AppLayout. The full /api/admin/share/status
    response is ~11KB and includes services + invites + sessions + audit
    logs + telemetry that the banner never reads. Per-poll-per-page
    multiplied across the 12+ pages with AppLayout was a meaningful
    cumulative cost.
    """
    mgr = get_tunnel_manager()
    return {
        "sharing_active": mgr.is_sharing_active(),
        "public_url": mgr.public_url(),
    }


def build_share_status() -> dict:
    """Compose the /admin/share/status response shape.

    Extracted from the router so /api/bootstrap can call it directly
    and seed React Query's [admin, share, status] cache — saving the
    /admin/share page's cold-load mount round-trip (187 ms p95 admin
    tunnel). Pure-Python read against in-memory tunnel state +
    SQLite-backed invites/sessions/audit; no FOS / no DuckDB.
    """
    mgr = get_tunnel_manager()
    state = mgr.state
    invites = share_db.get_remote_invites()
    # Attach each invite's last login (derived from the successful-login audit
    # events) so the admin can see who is actually using the app at a glance.
    # Joined on lowercased email (the only key audit rows carry); one invite per
    # email in practice.
    last_login_by_email = share_db.get_last_login_by_email()
    for inv in invites:
        inv["last_login_at"] = last_login_by_email.get((inv.get("email") or "").strip().lower())
    sessions = [s.to_dict() for s in mgr.list_sessions()]
    audit = share_db.get_share_audit_logs(limit=50)
    services = []
    try:
        for cfg in svcconfig.list_configs():
            services.append(
                {
                    "service_id": cfg.get("service_id"),
                    "name": cfg.get("name") or cfg.get("service_id"),
                    "remote_frontend_deployed": bool(cfg.get("remote_frontend")),
                    "sharing_domain": cfg.get("remote_frontend", {}).get("domain_name")
                    if cfg.get("remote_frontend")
                    else None,
                    "remote_service_id": cfg.get("remote_frontend", {}).get("service_id")
                    if cfg.get("remote_frontend")
                    else None,
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
        "invites": invites,
        "sessions": sessions,
        "audit_logs": audit,
        "rate_limits": mgr.get_rate_limit_snapshot(),
        "telemetry": mgr.get_telemetry(),
    }


@router.get("/status", response_model=ShareStatusResponse, response_model_exclude_unset=True)
def share_status():
    return build_share_status()


@router.get("/live", response_model=ShareLiveResponse, response_model_exclude_unset=True)
def share_live():
    """Lean 10-s poll payload for the share dashboard. Returns only the
    fields that change in real time and are surfaced continuously by
    SharingControlPanel (tunnel state + counters + rate limits +
    telemetry). The full /status mount-time payload (services /
    invites / sessions / audit_logs, ~11 KB) is fetched once on
    mount and refreshed on mutations — no need to re-ship it every
    10 seconds.

    Kept as a polling endpoint (one-shot snapshot on mutations — refresh
    button, session revoke — plus the page's 5-min safety-net refetch).
    Live freshness now rides the multiplexed admin event stream's ``share``
    channel (see ``backend/routers/admin/events.py``) instead of a second
    dedicated SSE connection — collapsing the /admin/share page from two
    concurrent streams over the HTTP/1.1 admin tunnel down to one.
    """
    return build_share_live_payload()


# ── Audit log (filterable) ─────────────────────────────────────────────────


@router.get("/audit-logs", response_model=ShareAuditLogsResponse, response_model_exclude_unset=True)
def audit_logs(
    # Out-of-band limit was 400-on-fail; FastAPI's Query(ge/le) emits a
    # structured 422 with the failing field path, which is what every
    # other validated param in this app uses.
    limit: int = Query(default=200, ge=1, le=2000),
    event_type: str | None = None,
    email: str | None = None,
    since: str | None = None,
    until: str | None = None,
):
    rows = share_db.get_share_audit_logs(
        limit=limit,
        event_type=event_type,
        email_substr=email,
        since=since,
        until=until,
    )
    return {"audit_logs": rows}


# ── Tunnel lifecycle ───────────────────────────────────────────────────────


@router.post("/start", response_model=ShareStartResponse, response_model_exclude_unset=True)
def share_start(payload: ShareStartPayload):
    mgr = get_tunnel_manager()
    try:
        result = mgr.start_sharing(
            public_endpoint=payload.public_endpoint,
            forward_port=payload.forward_port,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    return result


@router.post("/stop", response_model=OkResponse)
def share_stop():
    mgr = get_tunnel_manager()
    mgr.stop_sharing()
    return {"ok": True}


@router.post("/panic", response_model=SharePanicResponse, response_model_exclude_unset=True)
def share_panic():
    return get_tunnel_manager().panic()


# ── Invites ────────────────────────────────────────────────────────────────


@router.post("/invites", response_model=InviteRecord, response_model_exclude_unset=True)
def create_invite(payload: InvitePayload, request: Request):
    from datetime import UTC, datetime, timedelta

    # An invite with no services strands the analyst on "No service found"
    # (bootstrap returns an empty services list). Reject it here so an empty
    # scope can't be created even if a client bypasses the dialog guard.
    if not payload.service_ids:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "Select at least one service for the invite."},
        )

    # OAuth invites must name a provider that is actually configured (a registry
    # entry WITH env credentials). A disabled provider is allowed here so an
    # admin can pre-create an invite for a temporarily-disabled IdP (§5.1).
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
    share_db.log_share_audit_event(
        event_type="INVITE_CREATE",
        email=invite["email"],
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite['id']} services={','.join(payload.service_ids)}{auth_detail}",
    )
    return invite


@router.patch(
    "/invites/{invite_id}/services",
    response_model=InviteRecord,
    response_model_exclude_unset=True,
)
def update_invite_services(invite_id: str, payload: ServiceScopePayload):
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    if not payload.service_ids:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_request", "message": "An invite must keep at least one service."},
        )
    share_db.update_remote_invite_services(invite_id, payload.service_ids)
    return share_db.get_remote_invite(invite_id)


@router.patch(
    "/invites/{invite_id}/pii",
    response_model=InviteRecord,
    response_model_exclude_unset=True,
)
def update_invite_pii(invite_id: str, payload: PiiPolicyPayload, request: Request):
    """Toggle IP masking on an existing invite (no way to do this at create
    time only). The live analyst session re-syncs its policy from the invite
    on its next validate, so masking takes effect without a re-login."""
    invite = share_db.get_remote_invite(invite_id)
    if invite is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    try:
        share_db.update_remote_invite_pii(invite_id, {"mask_ips": payload.mask_ips})
    except (share_db.InvalidPiiPolicyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    share_db.log_share_audit_event(
        event_type="INVITE_UPDATE_PII",
        email=invite["email"],
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite_id} mask_ips={payload.mask_ips}",
    )
    return share_db.get_remote_invite(invite_id)


@router.patch(
    "/invites/{invite_id}/sharing",
    response_model=InviteRecord,
    response_model_exclude_unset=True,
)
def update_invite_sharing(invite_id: str, payload: SharingPolicyPayload, request: Request):
    """Toggle whether the invite allows shared (concurrent) analyst logins.

    Turning it ON lets several analysts use the same link at once instead of
    each login booting the previous session (still bounded by the global
    max_concurrent_analyst_sessions cap). Turning it OFF only affects *future*
    logins — any sessions already live under the invite are left to age out via
    the idle/absolute timeout; revoke the invite to force them off immediately.
    """
    invite = share_db.get_remote_invite(invite_id)
    if invite is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    share_db.set_invite_concurrent_sessions(invite_id, payload.allow_concurrent_sessions)
    share_db.log_share_audit_event(
        event_type="INVITE_UPDATE_SHARING",
        email=invite["email"],
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite_id} allow_concurrent_sessions={payload.allow_concurrent_sessions}",
    )
    return share_db.get_remote_invite(invite_id)


@router.patch("/invites/{invite_id}/passcode", response_model=OkResponse)
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
        raise HTTPException(status_code=400, detail=make_error("invalid_passcode", str(e)))
    share_db.log_share_audit_event(
        event_type="INVITE_PASSCODE_UPDATE",
        email=None,
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite_id}",
    )
    return {"ok": True}


@router.post(
    "/invites/{invite_id}/revoke",
    response_model=InviteMutationAck,
    response_model_exclude_unset=True,
)
def revoke_invite(invite_id: str, request: Request):
    if not share_db.revoke_remote_invite(invite_id):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite revoked")
    share_db.log_share_audit_event(
        event_type="INVITE_REVOKE",
        email=None,
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite_id} booted_sessions={booted}",
    )
    return {"ok": True, "booted_sessions": booted}


@router.delete(
    "/invites/{invite_id}",
    response_model=InviteMutationAck,
    response_model_exclude_unset=True,
)
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
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite_id} booted_sessions={booted}",
    )
    return {"ok": True, "booted_sessions": booted}


@router.post(
    "/invites/{invite_id}/claim-token",
    response_model=ClaimTokenResponse,
    response_model_exclude_unset=True,
)
def issue_claim_token(invite_id: str):
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    token = share_db.create_claim_token(invite_id, ttl_hours=24)
    return {"token": token}


# ── Sessions ────────────────────────────────────────────────────────────────


@router.post("/sessions/{session_id}/boot", response_model=OkResponse)
def boot_session(session_id: str, request: Request):
    ok = get_tunnel_manager().boot_session(session_id, reason="admin boot")
    if not ok:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return {"ok": True}


# ── Backup / Restore ────────────────────────────────────────────────────────


# response_model intentionally omitted: returns the encrypted backup blob
# (binary Response with Content-Disposition), not a JSON body.
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
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"bytes={len(blob)}",
    )
    return Response(
        content=blob,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename=remote-share-backup-{share_db.iso_z_now()}.enc",
        },
    )


@router.post("/backup/import", response_model=BackupImportResponse, response_model_exclude_unset=True)
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
        ip_address=client_ip(request, default="127.0.0.1"),
        details=str(result),
    )
    return result


# ── GDPR right-to-be-forgotten ──────────────────────────────────────────────


@router.post("/gdpr/erase", response_model=GdprEraseResponse, response_model_exclude_unset=True)
def gdpr_erase(payload: GdprErasePayload, request: Request):
    try:
        result = share_db.gdpr_erase(payload.email, payload.reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    # boot any live sessions for that email (now-gone invite_id)
    return result


# ── Settings ────────────────────────────────────────────────────────────────


@router.patch("/settings", response_model=ShareSettingsResponse, response_model_exclude_unset=True)
def update_settings(payload: SettingsPayload):
    if payload.max_concurrent_analyst_sessions is not None:
        if payload.max_concurrent_analyst_sessions < 1:
            raise HTTPException(status_code=400, detail={"error": "invalid_value"})
        share_db.set_setting(share_db.MAX_CONCURRENT_ANALYST_SESSIONS_KEY, str(payload.max_concurrent_analyst_sessions))
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


# ── Wordphrase generator (used by admin invite form) ───────────────────────


@router.get("/wordphrase", response_model=WordphraseResponse, response_model_exclude_unset=True)
def wordphrase():
    return {"passcode": share_db.generate_wordphrase()}


# ── OAuth provider registry (admin invite form) ─────────────────────────────


@router.get("/oauth-providers", response_model=OAuthProvidersResponse)
def oauth_providers():
    """List the configured OIDC providers for the admin invite form.

    Includes disabled providers (``enabled=false``) so an admin can pre-create
    an invite for a temporarily-disabled IdP — distinct from the unauth analyst
    ``/api/share/auth-config`` which exposes enabled providers only. Empty when
    the feature switch (``OAUTH_FLOW_STATE_SECRET``) is off. Never returns
    client_id/client_secret.
    """
    return OAuthProvidersResponse(providers=[p.admin_dict() for p in oauth_registry.get_all_providers()])
