"""Analyst OAuth / OIDC login routes — the pre-auth handshake pipeline.

Two GET endpoints reached by TOP-LEVEL BROWSER NAVIGATION (not the openapi-fetch
client), so every outcome is a 302 redirect, never a JSON body (design §2.8):

* ``GET /api/share/oauth/authorize?provider=<key>`` → 302 to the IdP, after
  sealing the ``oauth_flow_state`` cookie (state/nonce/PKCE/redirect_uri/return).
* ``GET /api/share/oauth/callback?code&state`` → on success, sets the
  ``analyst_session_id`` cookie (mirroring the passcode two-cookie TOS protocol)
  and 302s to the return target / ``/share-login/acknowledge``. On ANY failure,
  302s to ``/share-login?oauth_error=<code>`` and writes the matching audit row.

The handshake converges on the SAME ``TunnelManager`` session as passcode login,
so all downstream RBAC / masking / TOS / boot / revoke behavior is inherited.
ANALYST = adversary — every branch fails closed. No token / code / verifier /
secret is ever logged; session ids are truncated to ``[:8]``; attacker-supplied
emails go through ``_safe_audit_email`` into the audit ``email`` column.
"""

from __future__ import annotations

import base64
import hmac
import json
import logging
import os
import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import RedirectResponse
from joserfc import jwt
from joserfc.jwk import RSAKey

from backend.core import share_db
from backend.core.oauth import client as oidc
from backend.core.oauth import registry as oauth_registry
from backend.core.oauth.client import OIDCError
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.utils.auth import (
    analyst_cookie_is_secure,
    set_analyst_session_cookies,
)
from backend.utils.auth import (
    safe_audit_email as _safe_audit_email,
)
from backend.utils.remote_access import client_ip
from backend.utils.tunnel import TunnelCapacityError, get_tunnel_manager

flow_state = oauth_registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/share", tags=["share-oauth"], responses=DEFAULT_ERROR_RESPONSES)

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
    from backend import config as svcconfig

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
    import os

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


@router.get("/oauth/authorize")
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


@router.get("/oauth/callback")
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
