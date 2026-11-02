"""Admin and analyst-auth endpoints for remote-share management."""

from __future__ import annotations

from fastapi import APIRouter, Depends

import backend.core.oauth as oauth_registry
import backend.core.oauth as oidc
from backend import config as svcconfig
from backend.config import list_configs
from backend.core import share_db
from backend.core.oauth import OIDCError
from backend.core.request_context import require_admin
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.provision.fastly_api import delete_remote_frontend, deploy_remote_frontend
from backend.routers.share_admin import invites as _invites
from backend.routers.share_admin import sessions_and_tos as _sessions_and_tos
from backend.routers.share_admin import tunnel_routes as _tunnel_routes
from backend.routers.share_admin.invites import (
    _USER_CODE,
    _VERIFY_REASONS,
    _auto_provision_invite,
    _fail,
    _fail_oidc,
    _hash8,
    _login_redirect,
    _redirect_uri,
    _require_invite,
    _safe_invite_name,
    _safe_return,
    _user_code,
    create_invite,
    delete_invite,
    flow_state,
    invites_admin_router,
    issue_claim_token,
    list_invites,
    oauth_authorize,
    oauth_callback,
    reactivate_invite,
    regenerate_invite_passcode,
    revoke_invite,
    share_oauth_router,
    update_invite_passcode,
    update_invite_pii,
    update_invite_services,
    update_invite_sharing,
)
from backend.routers.share_admin.sessions_and_tos import (
    COOKIE_NAME,
    PENDING_COOKIE_NAME,
    _require_analyst_session,
    _safe_audit_email,
    backup_export,
    backup_import,
    boot_session,
    gdpr_erase,
    get_settings,
    get_tos,
    oauth_providers,
    publish_tos,
    revoke_session,
    sessions_admin_router,
    share_acknowledge_tos,
    share_auth_config,
    share_auth_router,
    share_claim,
    share_get_tos,
    share_heartbeat,
    share_login,
    share_logout,
    trigger_share_purge,
    update_settings,
    wordphrase,
)
from backend.routers.share_admin.tunnel_routes import (
    _KID,
    _audit,
    _b64url,
    _issuer,
    _key,
    _list_invites_with_last_login,
    _resolve_fastly_token,
    _signing_key,
    _unb64url,
    audit_logs,
    authorize,
    build_share_status,
    deploy_frontend,
    discovery,
    jwks,
    logger,
    mock_idp_enabled,
    mock_idp_router,
    share_banner,
    share_live,
    share_panic,
    share_start,
    share_status,
    share_stop,
    sharing_domain_router,
    teardown_frontend,
    token,
    tunnel_admin_router,
)
from backend.utils.auth import (
    ANALYST_PENDING_SESSION_COOKIE,
    ANALYST_SESSION_COOKIE,
    analyst_cookie_is_secure,
    safe_audit_email,
    set_analyst_session_cookies,
)
from backend.utils.module_shim import install_shim_module
from backend.utils.remote_access import client_ip
from backend.utils.router_utils import make_error
from backend.utils.tunnel import TunnelCapacityError, build_share_live_payload, get_tunnel_manager

admin_router = APIRouter(
    prefix="/api/admin/share",
    tags=["share-admin"],
    dependencies=[Depends(require_admin)],
    responses=DEFAULT_ERROR_RESPONSES,
)
admin_router.include_router(tunnel_admin_router)
admin_router.include_router(invites_admin_router)
admin_router.include_router(sessions_admin_router)

router = APIRouter()
router.include_router(admin_router)
router.include_router(share_auth_router)
router.include_router(share_oauth_router)

install_shim_module(
    __name__,
    (_tunnel_routes, _invites, _sessions_and_tos),
    skip_attrs=frozenset(
        {
            "router",
            "admin_router",
            "share_auth_router",
            "share_oauth_router",
            "sharing_domain_router",
            "mock_idp_router",
        }
    ),
    bidirectional_seed=True,
)

__all__ = [
    "ANALYST_PENDING_SESSION_COOKIE",
    "ANALYST_SESSION_COOKIE",
    "COOKIE_NAME",
    "DEFAULT_ERROR_RESPONSES",
    "OIDCError",
    "PENDING_COOKIE_NAME",
    "TunnelCapacityError",
    "_KID",
    "_USER_CODE",
    "_VERIFY_REASONS",
    "_audit",
    "_auto_provision_invite",
    "_b64url",
    "_fail",
    "_fail_oidc",
    "_hash8",
    "_issuer",
    "_key",
    "_list_invites_with_last_login",
    "_login_redirect",
    "_redirect_uri",
    "_require_analyst_session",
    "_require_invite",
    "_resolve_fastly_token",
    "_safe_audit_email",
    "_safe_invite_name",
    "_safe_return",
    "_signing_key",
    "_unb64url",
    "_user_code",
    "admin_router",
    "analyst_cookie_is_secure",
    "audit_logs",
    "authorize",
    "backup_export",
    "backup_import",
    "boot_session",
    "build_share_live_payload",
    "build_share_status",
    "client_ip",
    "create_invite",
    "delete_invite",
    "delete_remote_frontend",
    "deploy_frontend",
    "deploy_remote_frontend",
    "discovery",
    "flow_state",
    "gdpr_erase",
    "get_settings",
    "get_tos",
    "get_tunnel_manager",
    "invites_admin_router",
    "issue_claim_token",
    "jwks",
    "list_configs",
    "list_invites",
    "logger",
    "make_error",
    "mock_idp_enabled",
    "mock_idp_router",
    "oauth_authorize",
    "oauth_callback",
    "oauth_providers",
    "oauth_registry",
    "oidc",
    "publish_tos",
    "reactivate_invite",
    "regenerate_invite_passcode",
    "require_admin",
    "revoke_invite",
    "revoke_session",
    "router",
    "safe_audit_email",
    "sessions_admin_router",
    "set_analyst_session_cookies",
    "share_acknowledge_tos",
    "share_auth_config",
    "share_auth_router",
    "share_banner",
    "share_claim",
    "share_db",
    "share_get_tos",
    "share_heartbeat",
    "share_live",
    "share_login",
    "share_logout",
    "share_oauth_router",
    "share_panic",
    "share_start",
    "share_status",
    "share_stop",
    "sharing_domain_router",
    "svcconfig",
    "teardown_frontend",
    "token",
    "trigger_share_purge",
    "tunnel_admin_router",
    "update_invite_passcode",
    "update_invite_pii",
    "update_invite_services",
    "update_invite_sharing",
    "update_settings",
    "wordphrase",
]
