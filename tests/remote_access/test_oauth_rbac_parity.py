"""RBAC / masking / logout / revoke parity for OAuth sessions (design §2.7 / §6).

An OAuth-authenticated session IS an analyst session — SSO ≠ more trust. Auth
method must NOT influence nav gating, the /api/admin/* blocklist, or masking; and
logout/revoke evict it identically to a passcode session.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware

_REMOTE = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}


def _app() -> FastAPI:
    from backend.routers import share_admin, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)
    return app


def _oauth_invite(email="analyst@corp.com", provider="google", pii_policy=None, accept_tos=True):
    inv = share_db.create_remote_invite(
        name="Analyst",
        email=email,
        passcode=None,
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy=pii_policy,
        auth_method="oauth",
        oauth_provider=provider,
    )
    if accept_tos:
        tos = share_db.get_latest_tos()
        if tos:
            share_db.mark_tos_accepted(inv["id"], tos["version"])
    return share_db.get_remote_invite(inv["id"])


def _session_for(invite):
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint="https://testserver")
    return mgr.create_session(invite=invite, ip_address="9.9.9.9", user_agent="ua", headers={})


@pytest.mark.security_regression
def test_oauth_session_blocked_from_admin_paths():
    session = _session_for(_oauth_invite())
    with TestClient(_app()) as c:
        c.cookies.set("analyst_session_id", session.session_id)
        r = c.get("/api/admin/share/status", headers=_REMOTE)
    assert r.status_code == 403  # /api/admin/* is analyst-blocked regardless of auth method


@pytest.mark.security_regression
def test_oauth_session_inherits_masking_and_scope():
    session = _session_for(_oauth_invite(pii_policy={"mask_ips": True}))
    # Same posture as a passcode session: masking + scope come from the invite;
    # auth method is carried for display only.
    assert session.pii_policy.get("mask_ips") is True
    assert session.service_ids == ["svcA"]
    assert session.auth_method == "oauth"
    assert session.oauth_provider == "google"


@pytest.mark.security_regression
def test_logout_evicts_oauth_session():
    mgr = tunnel.get_tunnel_manager()
    session = _session_for(_oauth_invite())
    assert mgr.validate_session(session.session_id) is not None
    with TestClient(_app()) as c:
        c.cookies.set("analyst_session_id", session.session_id)
        r = c.post("/api/share/logout", headers=_REMOTE)
    assert r.status_code == 200
    assert mgr.validate_session(session.session_id) is None


@pytest.mark.security_regression
def test_revoke_invalidates_live_oauth_session():
    mgr = tunnel.get_tunnel_manager()
    invite = _oauth_invite()
    session = _session_for(invite)
    # Admin revoke boots live sessions immediately; validate_session also
    # re-checks invite.revoked on the next request as a backstop.
    share_db.revoke_remote_invite(invite["id"])
    mgr.boot_sessions_for_invite(invite["id"], reason="invite revoked")
    assert mgr.validate_session(session.session_id) is None


@pytest.mark.security_regression
def test_passcode_login_rejects_oauth_invite_over_http():
    """The positive auth_method gate at the endpoint: an OAuth invite's email
    cannot authenticate at POST /api/share/login with ANY passcode."""
    _oauth_invite(email="oauthonly@corp.com")
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    with TestClient(_app()) as c:
        r = c.post(
            "/api/share/login",
            json={"email": "oauthonly@corp.com", "passcode": "anything-at-all-here"},
            headers=_REMOTE,
        )
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "invalid_credentials"
