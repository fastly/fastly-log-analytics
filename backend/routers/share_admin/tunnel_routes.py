"""Tunnel lifecycle, status/audit, sharing domain deployment, and mock IdP routes."""

from __future__ import annotations

import base64
import json
import logging
import os
import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from joserfc import jwt
from joserfc.jwk import RSAKey

from backend import config as svcconfig
from backend.config import list_configs
from backend.core import share_db
from backend.core.request_context import require_admin
from backend.models.admin import (
    ShareAuditLogsResponse,
    ShareBannerResponse,
    ShareLiveResponse,
    SharePanicResponse,
    ShareStartPayload,
    ShareStartResponse,
    ShareStatusResponse,
    SharingDeployRequest,
    SharingDeployResponse,
    SharingTeardownRequest,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES, OkMessageResponse, OkResponse
from backend.provision.fastly_api import delete_remote_frontend, deploy_remote_frontend
from backend.utils.remote_access import client_ip
from backend.utils.tunnel import build_share_live_payload, get_tunnel_manager

logger = logging.getLogger(__name__)

tunnel_admin_router = APIRouter()
sharing_domain_router = APIRouter(
    prefix="/api/sharing", tags=["sharing"], dependencies=[Depends(require_admin)], responses=DEFAULT_ERROR_RESPONSES
)


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
@tunnel_admin_router.get("/banner", response_model=ShareBannerResponse, response_model_exclude_unset=True)
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


@tunnel_admin_router.get("/status", response_model=ShareStatusResponse, response_model_exclude_unset=True)
def share_status():
    return build_share_status()


@tunnel_admin_router.get("/live", response_model=ShareLiveResponse, response_model_exclude_unset=True)
def share_live():
    return build_share_live_payload()


@tunnel_admin_router.get("/audit-logs", response_model=ShareAuditLogsResponse, response_model_exclude_unset=True)
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
@tunnel_admin_router.post("/start", response_model=ShareStartResponse, response_model_exclude_unset=True)
def share_start(payload: ShareStartPayload):
    mgr = get_tunnel_manager()
    try:
        return mgr.start_sharing(public_endpoint=payload.public_endpoint, forward_port=payload.forward_port)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_request", "message": str(exc)}) from exc


@tunnel_admin_router.post("/stop", response_model=OkResponse)
def share_stop():
    get_tunnel_manager().stop_sharing()
    return {"ok": True}


@tunnel_admin_router.post("/panic", response_model=SharePanicResponse, response_model_exclude_unset=True)
def share_panic():
    return get_tunnel_manager().panic()


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
