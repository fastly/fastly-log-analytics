"""Admin invite CRUD and analyst OAuth/OIDC login & JIT invite provisioning routes."""

from __future__ import annotations

import hmac
import logging
import os
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse

import backend.core.oauth as oauth_registry
import backend.core.oauth as oidc
from backend import config as svcconfig
from backend.core import share_db
from backend.core.oauth import OIDCError
from backend.models.admin import (
    ClaimTokenResponse,
    InviteMutationAck,
    InvitePayload,
    InviteRecord,
    PasscodePayload,
    PasscodeRegenerateResponse,
    PiiPolicyPayload,
    ServiceScopePayload,
    ShareInvitesResponse,
    SharingPolicyPayload,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES, OkResponse
from backend.routers.share_admin.tunnel_routes import _audit, _list_invites_with_last_login
from backend.utils.auth import (
    analyst_cookie_is_secure,
    safe_audit_email,
    set_analyst_session_cookies,
)
from backend.utils.remote_access import client_ip
from backend.utils.router_utils import make_error
from backend.utils.tunnel import TunnelCapacityError, get_tunnel_manager

logger = logging.getLogger(__name__)
flow_state = oauth_registry
_safe_audit_email = safe_audit_email

invites_admin_router = APIRouter()


def _require_invite(invite_id: str) -> dict:
    invite = share_db.get_remote_invite(invite_id)
    if invite is None:
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    return invite


# ── Invites ────────────────────────────────────────────────────────────────
@invites_admin_router.get("/invites", response_model=ShareInvitesResponse, response_model_exclude_unset=True)
def list_invites():
    return {"invites": _list_invites_with_last_login()}


@invites_admin_router.post("/invites", response_model=InviteRecord, response_model_exclude_unset=True)
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


@invites_admin_router.patch(
    "/invites/{invite_id}/services", response_model=InviteRecord, response_model_exclude_unset=True
)
def update_invite_services(invite_id: str, payload: ServiceScopePayload):
    _require_invite(invite_id)
    if not payload.service_ids:
        raise HTTPException(
            status_code=400, detail={"error": "invalid_request", "message": "An invite must keep at least one service."}
        )
    share_db.update_remote_invite_services(invite_id, payload.service_ids)
    return share_db.get_remote_invite(invite_id)


@invites_admin_router.patch("/invites/{invite_id}/pii", response_model=InviteRecord, response_model_exclude_unset=True)
def update_invite_pii(invite_id: str, payload: PiiPolicyPayload, request: Request):
    invite = _require_invite(invite_id)
    try:
        share_db.update_remote_invite_pii(invite_id, {"mask_ips": payload.mask_ips})
    except (share_db.InvalidPiiPolicyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc
    _audit(request, "INVITE_UPDATE_PII", invite["email"], f"invite_id={invite_id} mask_ips={payload.mask_ips}")
    return share_db.get_remote_invite(invite_id)


@invites_admin_router.patch(
    "/invites/{invite_id}/sharing", response_model=InviteRecord, response_model_exclude_unset=True
)
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


@invites_admin_router.patch("/invites/{invite_id}/passcode", response_model=OkResponse)
def update_invite_passcode(invite_id: str, payload: PasscodePayload, request: Request):
    _require_invite(invite_id)
    try:
        share_db.update_remote_invite_passcode(invite_id, payload.passcode)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=make_error("invalid_passcode", str(e)))
    _audit(request, "INVITE_PASSCODE_UPDATE", None, f"invite_id={invite_id}")
    return {"ok": True}


@invites_admin_router.post(
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


@invites_admin_router.post(
    "/invites/{invite_id}/revoke", response_model=InviteMutationAck, response_model_exclude_unset=True
)
def revoke_invite(invite_id: str, request: Request):
    if not share_db.revoke_remote_invite(invite_id):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite revoked")
    _audit(request, "INVITE_REVOKE", None, f"invite_id={invite_id} booted_sessions={booted}")
    return {"ok": True, "booted_sessions": booted}


@invites_admin_router.post(
    "/invites/{invite_id}/reactivate", response_model=InviteRecord, response_model_exclude_unset=True
)
def reactivate_invite(invite_id: str, request: Request):
    if not share_db.reactivate_remote_invite(invite_id):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    invite = share_db.get_remote_invite(invite_id) or {}
    _audit(request, "INVITE_REACTIVATE", invite.get("email"), f"invite_id={invite_id}")
    return {**invite, "ok": True}


@invites_admin_router.delete(
    "/invites/{invite_id}", response_model=InviteMutationAck, response_model_exclude_unset=True
)
def delete_invite(invite_id: str, request: Request):
    _require_invite(invite_id)
    booted = get_tunnel_manager().boot_sessions_for_invite(invite_id, reason="invite deleted")
    share_db.delete_remote_invite(invite_id)
    _audit(request, "INVITE_DELETE", None, f"invite_id={invite_id} booted_sessions={booted}")
    return {"ok": True, "booted_sessions": booted}


@invites_admin_router.post(
    "/invites/{invite_id}/claim-token", response_model=ClaimTokenResponse, response_model_exclude_unset=True
)
def issue_claim_token(invite_id: str):
    _require_invite(invite_id)
    return {"token": share_db.create_claim_token(invite_id, ttl_hours=24)}


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
