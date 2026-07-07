"""Provider registry + auth-config + admin oauth-providers + create-invite
OAuth validation (design §2.6 / §5.1 / §5.2).

The registry is default-OFF: without OAUTH_FLOW_STATE_SECRET (the feature
switch) nothing is advertised, and passcode login is unchanged.
"""

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.core.oauth import registry
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware

_REMOTE_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}


@pytest.fixture(autouse=True)
def _reset_registry_cache():
    registry.reset_cache_for_tests()
    yield
    registry.reset_cache_for_tests()


def _configure_provider(
    tmp_path,
    monkeypatch,
    *,
    key="google",
    display_name="Google Workspace",
    enabled=True,
    with_creds=True,
    extra=None,
):
    entry = {
        "display_name": display_name,
        "discovery_url": "https://idp.example/.well-known/openid-configuration",
        "scopes": "openid email",
        "enabled": enabled,
    }
    if extra:
        entry.update(extra)
    path = tmp_path / "oauth_providers.json"
    path.write_text(json.dumps({key: entry}))
    monkeypatch.setenv("OAUTH_PROVIDERS_CONFIG_PATH", str(path))
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "test-flow-state-secret-0123456789")
    if with_creds:
        suffix = "".join(c if c.isalnum() else "_" for c in key).upper()
        monkeypatch.setenv(f"OAUTH_{suffix}_CLIENT_ID", "test-client-id")
        monkeypatch.setenv(f"OAUTH_{suffix}_CLIENT_SECRET", "test-client-secret")
    registry.reset_cache_for_tests()
    return path


def _auth_app() -> FastAPI:
    from backend.routers import share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    return app


def _admin_app() -> FastAPI:
    from backend.routers import share_admin

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_admin.router)
    return app


# ── Registry ────────────────────────────────────────────────────────────────


def test_oauth_disabled_by_default(monkeypatch):
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    monkeypatch.delenv("OAUTH_PROVIDERS_CONFIG_PATH", raising=False)
    registry.reset_cache_for_tests()
    assert registry.feature_on() is False
    assert registry.oauth_enabled() is False
    assert registry.get_all_providers() == []


def test_provider_configured_from_json_plus_env(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch)
    provs = registry.get_all_providers()
    assert len(provs) == 1
    p = provs[0]
    assert p.id == "google"
    assert p.display_name == "Google Workspace"
    assert p.client_id == "test-client-id"
    assert p.client_secret == "test-client-secret"
    assert p.scopes == "openid email"
    assert registry.oauth_enabled() is True


def test_provider_without_env_creds_is_invisible(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch, with_creds=False)
    assert registry.get_all_providers() == []
    # Feature switch is on (secret set), but no fully-configured provider.
    assert registry.feature_on() is True
    assert registry.oauth_enabled() is False


def test_providers_hidden_when_feature_switch_off(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch)
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    registry.reset_cache_for_tests()
    assert registry.feature_on() is False
    assert registry.get_all_providers() == []  # even with a fully-populated file


def test_disabled_provider_excluded_from_enabled_but_visible_to_admin(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch, enabled=False)
    assert registry.get_enabled_providers() == []
    all_p = registry.get_all_providers()
    assert len(all_p) == 1 and all_p[0].enabled is False
    assert registry.get_provider("google") is not None  # create-invite can still target it


def test_public_dict_never_leaks_secrets(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch)
    pub = registry.get_provider("google").public_dict()
    assert set(pub.keys()) == {"id", "display_name"}
    blob = json.dumps(pub)
    assert "test-client-id" not in blob and "test-client-secret" not in blob


def test_extra_issuers_and_allowed_hd_parsed(tmp_path, monkeypatch):
    _configure_provider(
        tmp_path,
        monkeypatch,
        extra={"extra_issuers": ["accounts.google.com", ""], "allowed_hd": "corp.com"},
    )
    p = registry.get_provider("google")
    assert p.extra_issuers == ("accounts.google.com",)  # blank entry dropped
    assert p.allowed_hd == "corp.com"


def test_malformed_registry_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "oauth_providers.json"
    path.write_text("{ not valid json at all")
    monkeypatch.setenv("OAUTH_PROVIDERS_CONFIG_PATH", str(path))
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-value-here-abcdef")
    registry.reset_cache_for_tests()
    assert registry.get_all_providers() == []


def test_passcode_login_enabled_default_and_toggles(monkeypatch):
    monkeypatch.delenv("SHARE_PASSCODE_LOGIN_ENABLED", raising=False)
    assert registry.passcode_login_enabled() is True
    for off in ("0", "false", "no", "off", "OFF"):
        monkeypatch.setenv("SHARE_PASSCODE_LOGIN_ENABLED", off)
        assert registry.passcode_login_enabled() is False
    monkeypatch.setenv("SHARE_PASSCODE_LOGIN_ENABLED", "1")
    assert registry.passcode_login_enabled() is True


# ── /api/share/auth-config (unauth) ─────────────────────────────────────────


def test_auth_config_default_passcode_only(monkeypatch):
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    registry.reset_cache_for_tests()
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    with TestClient(_auth_app()) as c:
        r = c.get("/api/share/auth-config", headers=_REMOTE_HEADERS)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["passcode_enabled"] is True
    assert body["providers"] == []


def test_auth_config_lists_enabled_providers_id_and_name_only(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch)
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    with TestClient(_auth_app()) as c:
        r = c.get("/api/share/auth-config", headers=_REMOTE_HEADERS)
    body = r.json()
    assert body["providers"] == [{"id": "google", "display_name": "Google Workspace"}]
    # No secrets/discovery_url leak to the unauth surface.
    assert "test-client" not in r.text and "discovery_url" not in r.text


def test_auth_config_excludes_disabled_provider(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch, enabled=False)
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    with TestClient(_auth_app()) as c:
        r = c.get("/api/share/auth-config", headers=_REMOTE_HEADERS)
    assert r.json()["providers"] == []


def test_auth_config_reports_passcode_disabled(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch)
    monkeypatch.setenv("SHARE_PASSCODE_LOGIN_ENABLED", "0")
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    with TestClient(_auth_app()) as c:
        r = c.get("/api/share/auth-config", headers=_REMOTE_HEADERS)
    body = r.json()
    assert body["passcode_enabled"] is False
    assert body["providers"] == [{"id": "google", "display_name": "Google Workspace"}]


# ── passcode /login fail-closed when disabled ───────────────────────────────


@pytest.mark.security_regression
def test_login_rejected_when_passcode_disabled(monkeypatch):
    monkeypatch.setenv("SHARE_PASSCODE_LOGIN_ENABLED", "0")
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
    )
    with TestClient(_auth_app()) as c:
        r = c.post(
            "/api/share/login",
            json={"email": "drew@example.com", "passcode": "ocean-breeze-cabin-42"},
            headers=_REMOTE_HEADERS,
        )
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "passcode_login_disabled"


# ── /api/admin/share/oauth-providers + create-invite OAuth validation ────────


def test_admin_oauth_providers_lists_all_including_disabled(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch, enabled=False)
    with TestClient(_admin_app()) as c:
        r = c.get("/api/admin/share/oauth-providers")
    assert r.status_code == 200, r.text
    assert r.json()["providers"] == [{"id": "google", "display_name": "Google Workspace", "enabled": False}]


def test_admin_oauth_providers_empty_when_feature_off(monkeypatch):
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    registry.reset_cache_for_tests()
    with TestClient(_admin_app()) as c:
        r = c.get("/api/admin/share/oauth-providers")
    assert r.json()["providers"] == []


def test_create_oauth_invite_requires_provider(monkeypatch):
    with TestClient(_admin_app()) as c:
        r = c.post(
            "/api/admin/share/invites",
            json={"name": "Ana", "email": "ana@corp.com", "auth_method": "oauth", "service_ids": ["svcA"]},
        )
    assert r.status_code == 400
    assert "provider" in r.json()["detail"]["message"].lower()


def test_create_oauth_invite_unconfigured_provider_rejected(monkeypatch):
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    registry.reset_cache_for_tests()
    with TestClient(_admin_app()) as c:
        r = c.post(
            "/api/admin/share/invites",
            json={
                "name": "Ana",
                "email": "ana@corp.com",
                "auth_method": "oauth",
                "oauth_provider": "google",
                "service_ids": ["svcA"],
            },
        )
    assert r.status_code == 400
    assert "not configured" in r.json()["detail"]["message"]


def test_create_oauth_invite_success(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch)
    with TestClient(_admin_app()) as c:
        r = c.post(
            "/api/admin/share/invites",
            json={
                "name": "Ana",
                "email": "ana@corp.com",
                "auth_method": "oauth",
                "oauth_provider": "google",
                "service_ids": ["svcA"],
            },
        )
        assert r.status_code == 200, r.text
        invite = r.json()
        assert invite["auth_method"] == "oauth"
        assert invite["oauth_provider"] == "google"
        # Surfaced in the admin status payload too.
        status = c.get("/api/admin/share/status").json()
        row = next(i for i in status["invites"] if i["email"] == "ana@corp.com")
        assert row["auth_method"] == "oauth"
        assert row["oauth_provider"] == "google"


def test_create_passcode_invite_still_works(monkeypatch):
    with TestClient(_admin_app()) as c:
        r = c.post(
            "/api/admin/share/invites",
            json={
                "name": "Drew",
                "email": "drew@corp.com",
                "passcode": "ocean-breeze-cabin-42",
                "service_ids": ["svcA"],
            },
        )
    assert r.status_code == 200, r.text
    assert r.json()["auth_method"] == "passcode"
    assert r.json()["oauth_provider"] is None
