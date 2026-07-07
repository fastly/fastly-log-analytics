"""Route-level tests for /api/share/oauth/{authorize,callback} (design §2.8).

Both routes are top-level browser navigations: success/failure are ALWAYS 302
redirects (never JSON). Failures redirect to /share-login?oauth_error=<code> and
write the matching audit row. Callback tests seal their own flow-state cookie so
state/nonce/verifier are fully controlled.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.core.oauth import client as oidc
from backend.core.oauth import flow_state, registry
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware
from tests.remote_access import oauth_helpers as H

REDIRECT_URI = "https://testserver/api/share/oauth/callback"


@pytest.fixture(autouse=True)
def _reset():
    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()
    yield
    oidc.set_test_transport(None)
    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()


def _app() -> FastAPI:
    from backend.routers import share_oauth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_oauth.router)
    return app


def _client() -> TestClient:
    return TestClient(_app(), follow_redirects=False)


def _callback(sealed: str | None, params: dict):
    """Drive /callback, passing the flow-state via an explicit Cookie header
    (per-request cookies= is deprecated in starlette's TestClient; the sealed
    value is padstripped base64url, safe unquoted in a Cookie header)."""
    headers = dict(H.REMOTE_HEADERS)
    if sealed is not None:
        headers["Cookie"] = f"oauth_flow_state={sealed}"
    with TestClient(_app(), follow_redirects=False) as c:
        return c.get("/api/share/oauth/callback", params=params, headers=headers)


def _activate():
    tunnel.get_tunnel_manager().start_sharing(public_endpoint=H.PUBLIC_ENDPOINT)


def _seal(**over) -> str:
    payload = {
        "provider": "google",
        "state": "state-XYZ",
        "nonce": "nonce-XYZ",
        "verifier": "verifier-XYZ",
        "redirect_uri": REDIRECT_URI,
        "return": "/dashboard",
    }
    payload.update(over)
    return flow_state.seal_flow_state(payload)


def _make_oauth_invite(email="analyst@corp.com", provider="google", **kw):
    return share_db.create_remote_invite(
        name="Ana",
        email=email,
        passcode=None,
        expires_at_utc=kw.get("expires_at_utc"),
        ip_whitelist=kw.get("ip_whitelist"),
        service_ids=kw.get("service_ids", ["svcA"]),
        auth_method="oauth",
        oauth_provider=provider,
    )


def _is_set(r, name: str) -> bool:
    for line in r.headers.get_list("set-cookie"):
        if line.startswith(name + "="):
            val = line[len(name) + 1 :].split(";", 1)[0]
            if val and "max-age=0" not in line.lower():
                return True
    return False


def _audit_events() -> list[str]:
    return [a["event_type"] for a in share_db.get_share_audit_logs(limit=50)]


# ── /authorize ───────────────────────────────────────────────────────────────


def test_authorize_redirects_to_idp_and_sets_flow_state(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    oidc.set_test_transport(H.make_transport())
    _activate()
    with _client() as c:
        r = c.get("/api/share/oauth/authorize", params={"provider": "google"}, headers=H.REMOTE_HEADERS)
    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith(H.AUTHORIZATION_ENDPOINT)
    for token in ("response_type=code", "code_challenge_method=S256", "prompt=select_account", "state=", "nonce="):
        assert token in loc
    assert "redirect_uri=https" in loc
    assert _is_set(r, "oauth_flow_state")
    assert "OAUTH_AUTH_INIT" in _audit_events()


def test_authorize_unknown_provider_redirects_error(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    oidc.set_test_transport(H.make_transport())
    _activate()
    with _client() as c:
        r = c.get("/api/share/oauth/authorize", params={"provider": "nope"}, headers=H.REMOTE_HEADERS)
    assert r.status_code == 302
    assert r.headers["location"] == "/share-login?oauth_error=idp_unavailable"


def test_authorize_disabled_provider_redirects_error(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path, enabled=False)
    oidc.set_test_transport(H.make_transport())
    _activate()
    with _client() as c:
        r = c.get("/api/share/oauth/authorize", params={"provider": "google"}, headers=H.REMOTE_HEADERS)
    assert r.headers["location"] == "/share-login?oauth_error=idp_unavailable"


# ── /callback: success ───────────────────────────────────────────────────────


def test_callback_success_tos_pending_routes_to_acknowledge(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    _make_oauth_invite()  # fresh invite → TOS not yet accepted → tos_pending
    id_token = H.mint_id_token(nonce="nonce-XYZ")
    oidc.set_test_transport(H.make_transport(token_body=H.token_response(id_token)))
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.status_code == 302
    assert r.headers["location"] == "/share-login/acknowledge"
    assert _is_set(r, "analyst_pending_session_id")
    assert not _is_set(r, "analyst_session_id")
    assert "LOGIN_SUCCESS_OAUTH" in _audit_events()


def test_callback_success_tos_accepted_routes_to_dashboard(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    inv = _make_oauth_invite()
    tos = share_db.get_latest_tos()
    share_db.mark_tos_accepted(inv["id"], tos["version"])
    id_token = H.mint_id_token(nonce="nonce-XYZ")
    oidc.set_test_transport(H.make_transport(token_body=H.token_response(id_token)))
    r = _callback(_seal(**{"return": "/dashboard"}), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/dashboard"
    assert _is_set(r, "analyst_session_id")
    # SameSite=Lax (not Strict): the landing is a cross-site-initiated top-level
    # nav from the IdP, so Strict would be dropped and bounce back to /share-login.
    sid_cookie = next(c for c in r.headers.get_list("set-cookie") if c.startswith("analyst_session_id="))
    assert "samesite=lax" in sid_cookie.lower()


def test_callback_pins_subject_and_allows_repeat_login(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    inv = _make_oauth_invite()
    id_token = H.mint_id_token(nonce="nonce-XYZ", sub="google-sub-1")
    oidc.set_test_transport(H.make_transport(token_body=H.token_response(id_token)))
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.status_code == 302 and not r.headers["location"].startswith("/share-login?")
    assert share_db.get_remote_invite(inv["id"])["oauth_subject"] == "google-sub-1"


# ── /callback: failure redirects (each writes an audit row) ──────────────────


@pytest.mark.security_regression
def test_callback_missing_flow_state(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    oidc.set_test_transport(H.make_transport())
    r = _callback(None, {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=auth_failed"
    assert "OAUTH_CALLBACK_FAIL" in _audit_events()


@pytest.mark.security_regression
def test_callback_csrf_state_mismatch(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    oidc.set_test_transport(H.make_transport())
    r = _callback(_seal(state="state-XYZ"), {"code": "abc", "state": "WRONG-STATE"})
    assert r.headers["location"] == "/share-login?oauth_error=auth_failed"
    details = [a["details"] for a in share_db.get_share_audit_logs(limit=5)]
    assert any("csrf_state_mismatch" in d for d in details)


@pytest.mark.security_regression
def test_callback_idp_error_param(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    oidc.set_test_transport(H.make_transport())
    r = _callback(_seal(), {"error": "access_denied", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=auth_failed"


@pytest.mark.security_regression
def test_callback_uninvited_and_sub_mismatch_are_byte_identical(tmp_path, monkeypatch):
    """No user enumeration: an uninvited email and a sub-mismatch produce an
    identical response (design §2.9)."""
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    oidc.set_test_transport(
        H.make_transport(token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", email="nobody@corp.com")))
    )
    uninvited = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})

    # sub-mismatch: invite exists + subject already pinned to a different sub.
    inv = _make_oauth_invite(email="pinned@corp.com")
    share_db.bind_invite_oauth_subject(inv["id"], "the-real-sub")
    oidc.set_test_transport(
        H.make_transport(
            token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", email="pinned@corp.com", sub="attacker-sub"))
        )
    )
    mismatch = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})

    assert uninvited.status_code == mismatch.status_code == 302
    assert uninvited.headers["location"] == mismatch.headers["location"] == "/share-login?oauth_error=not_invited"
    assert not _is_set(uninvited, "analyst_session_id")
    assert not _is_set(mismatch, "analyst_session_id")
    assert "OAUTH_INVITE_NOT_FOUND" in _audit_events()


def test_callback_auto_provisions_when_enabled(tmp_path, monkeypatch):
    """With auto_provision on, a verified login for a NON-invited email creates an
    invite JIT and logs in (no OAUTH_INVITE_NOT_FOUND)."""
    H.configure_registry(monkeypatch, tmp_path, extra={"auto_provision": True, "auto_provision_service_ids": ["svcA"]})
    _activate()
    assert share_db.get_remote_invite_oauth("jit@corp.com", "google") is None  # none pre-exists
    oidc.set_test_transport(
        H.make_transport(token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", email="jit@corp.com")))
    )
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.status_code == 302
    assert not r.headers["location"].startswith("/share-login?")  # NOT an error redirect
    # An invite now exists for the auto-provisioned analyst, scoped as configured.
    inv = share_db.get_remote_invite_oauth("jit@corp.com", "google")
    assert inv is not None
    assert inv["service_ids"] == ["svcA"]
    assert inv["oauth_subject"]  # pinned on this first login


@pytest.mark.security_regression
def test_callback_no_auto_provision_by_default_is_not_invited(tmp_path, monkeypatch):
    """Default (auto_provision off): a non-invited verified login is still rejected
    with the generic not_invited — the invite allowlist stays the gate."""
    H.configure_registry(monkeypatch, tmp_path)  # auto_provision defaults off
    _activate()
    oidc.set_test_transport(
        H.make_transport(token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", email="stranger@corp.com")))
    )
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=not_invited"
    assert share_db.get_remote_invite_oauth("stranger@corp.com", "google") is None


@pytest.mark.security_regression
def test_auto_provision_still_enforces_hosted_domain(tmp_path, monkeypatch):
    """auto_provision does NOT bypass the org gate: a wrong-hd token is rejected
    before any invite is created."""
    H.configure_registry(
        monkeypatch,
        tmp_path,
        allowed_hd="corp.com",
        extra={"auto_provision": True, "auto_provision_service_ids": ["svcA"]},
    )
    _activate()
    oidc.set_test_transport(
        H.make_transport(
            token_body=H.token_response(
                H.mint_id_token(nonce="nonce-XYZ", email="evil@evil.com", extra={"hd": "evil.com"})
            )
        )
    )
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=wrong_domain"
    assert share_db.get_remote_invite_oauth("evil@evil.com", "google") is None


@pytest.mark.security_regression
def test_callback_wrong_provider_invite_not_found(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    _make_oauth_invite(email="okta-user@corp.com", provider="okta")  # invite for a DIFFERENT provider
    oidc.set_test_transport(
        H.make_transport(token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", email="okta-user@corp.com")))
    )
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=not_invited"


@pytest.mark.security_regression
def test_callback_ip_whitelist_enforced(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    _make_oauth_invite(ip_whitelist="10.0.0.0/8")  # excludes the test client IP
    oidc.set_test_transport(H.make_transport(token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ"))))
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=auth_failed"
    assert not _is_set(r, "analyst_session_id")
    details = [a["details"] for a in share_db.get_share_audit_logs(limit=5)]
    assert any("ip_not_whitelisted" in d for d in details)


@pytest.mark.security_regression
def test_callback_unverified_email_rejected(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    _make_oauth_invite()
    oidc.set_test_transport(
        H.make_transport(token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", email_verified=False)))
    )
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=unverified_email"
    assert "OAUTH_VERIFY_FAIL" in _audit_events()


@pytest.mark.security_regression
def test_callback_expired_token_rejected(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    _activate()
    _make_oauth_invite()
    oidc.set_test_transport(
        H.make_transport(
            token_body=H.token_response(H.mint_id_token(nonce="nonce-XYZ", iat_delta=-1000, exp_delta=-500))
        )
    )
    r = _callback(_seal(), {"code": "abc", "state": "state-XYZ"})
    assert r.headers["location"] == "/share-login?oauth_error=auth_failed"
    assert "OAUTH_VERIFY_FAIL" in _audit_events()
