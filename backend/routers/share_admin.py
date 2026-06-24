"""Admin endpoints for remote-share management (separate from admin.py).

Lives in its own router so the share endpoints can be globbed onto a
sub-prefix without entangling the ingest/sync admin surface area. All
endpoints here MUST be blocked from analyst sessions by the middleware in
``main.py`` — the prefix ``/api/admin/share`` is on the analyst-blocked
list.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import Response
from sse_starlette.sse import EventSourceResponse

from backend import config as svcconfig
from backend.core import share_db
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.share_admin import (
    BackupExportPayload,
    GdprErasePayload,
    InvitePayload,
    PasscodePayload,
    ServiceScopePayload,
    SettingsPayload,
    ShareStartPayload,
)
from backend.utils.remote_access import client_ip
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, make_error
from backend.utils.tunnel import get_tunnel_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/share", tags=["share-admin"], responses=DEFAULT_ERROR_RESPONSES)
# ── Status ──────────────────────────────────────────────────────────────────


@router.get("/banner")
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


@router.get("/status")
def share_status():
    return build_share_status()


def _live_payload() -> dict:
    """Compose the /live payload shape — extracted so the polling
    endpoint and the SSE stream share one source of truth."""
    mgr = get_tunnel_manager()
    return {
        "sharing_active": mgr.is_sharing_active(),
        "public_url": mgr.public_url(),
        "active_session_count": mgr.active_session_count(),
        "rate_limits": mgr.get_rate_limit_snapshot(),
        "telemetry": mgr.get_telemetry(),
    }


@router.get("/live")
def share_live():
    """Lean 10-s poll payload for the share dashboard. Returns only the
    fields that change in real time and are surfaced continuously by
    SharingControlPanel (tunnel state + counters + rate limits +
    telemetry). The full /status mount-time payload (services /
    invites / sessions / audit_logs, ~11 KB) is fetched once on
    mount and refreshed on mutations — no need to re-ship it every
    10 seconds.

    Kept as a polling endpoint alongside the /stream channel so the
    page can fetch a one-shot snapshot on mutations (refresh button,
    session revoke) without waiting for the next stream tick.
    """
    return _live_payload()


_SHARE_STREAM_SAMPLE_SECONDS = 10.0


@router.get("/stream")
async def share_stream(request: Request) -> EventSourceResponse:
    """Push the lean /live payload only when it changes.

    Replaces the 10-s poll the /admin/share page used to drive. Per-
    subscriber sampler (same pattern as
    /api/admin/system-metrics/stream): payload is dominated by
    in-memory tunnel-manager getters, so per-connection sampling is
    fine and lets us skip the publisher-binding lifecycle.

    Admin-only via the ``/api/admin/share`` prefix gate.
    """

    async def stream() -> AsyncIterator[str]:
        last_payload: dict | None = None
        initial = _live_payload()
        yield json.dumps(initial)
        last_payload = initial

        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(_SHARE_STREAM_SAMPLE_SECONDS)
            try:
                payload = _live_payload()
            except Exception:
                logger.exception("share-stream sample failed; will retry next tick")
                continue
            if payload != last_payload:
                yield json.dumps(payload)
                last_payload = payload

    return EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)


# ── Audit log (filterable) ─────────────────────────────────────────────────


@router.get("/audit-logs")
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


@router.post("/start")
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


@router.post("/stop")
def share_stop():
    mgr = get_tunnel_manager()
    mgr.stop_sharing()
    return {"ok": True}


@router.post("/panic")
def share_panic():
    return get_tunnel_manager().panic()


# ── Invites ────────────────────────────────────────────────────────────────


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
        ip_address=client_ip(request, default="127.0.0.1"),
        details=f"invite_id={invite['id']} services={','.join(payload.service_ids)}",
    )
    return invite


@router.patch("/invites/{invite_id}/services")
def update_invite_services(invite_id: str, payload: ServiceScopePayload):
    if share_db.get_remote_invite(invite_id) is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    share_db.update_remote_invite_services(invite_id, payload.service_ids)
    return share_db.get_remote_invite(invite_id)


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
        raise HTTPException(status_code=400, detail=make_error("invalid_passcode", str(e)))
    share_db.log_share_audit_event(
        event_type="INVITE_PASSCODE_UPDATE",
        email=None,
        ip_address=client_ip(request, default="127.0.0.1"),
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
        ip_address=client_ip(request, default="127.0.0.1"),
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
        ip_address=client_ip(request, default="127.0.0.1"),
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
        ip_address=client_ip(request, default="127.0.0.1"),
        details=str(result),
    )
    return result


# ── GDPR right-to-be-forgotten ──────────────────────────────────────────────


@router.post("/gdpr/erase")
def gdpr_erase(payload: GdprErasePayload, request: Request):
    try:
        result = share_db.gdpr_erase(payload.email, payload.reason)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    # boot any live sessions for that email (now-gone invite_id)
    return result


# ── Settings ────────────────────────────────────────────────────────────────


@router.patch("/settings")
def update_settings(payload: SettingsPayload):
    if payload.max_concurrent_analyst_sessions is not None:
        if payload.max_concurrent_analyst_sessions < 1:
            raise HTTPException(status_code=400, detail={"error": "invalid_value"})
        share_db.set_setting(share_db.MAX_CONCURRENT_ANALYST_SESSIONS_KEY, str(payload.max_concurrent_analyst_sessions))
    return {"max_concurrent_analyst_sessions": share_db.get_max_concurrent_sessions()}


# ── Wordphrase generator (used by admin invite form) ───────────────────────


@router.get("/wordphrase")
def wordphrase():
    return {"passcode": share_db.generate_wordphrase()}
