from __future__ import annotations

"""OIDC client: id_token validation, discovery/JWKS caching, single-flight
refetch, token exchange (design §2.2 / §2.3 / §3.1 / §3.2)."""

import base64
import json
import threading

import httpx
import pytest
from joserfc import jwt
from joserfc.jwk import KeySet, OctKey, RSAKey

from backend.core.oauth import client, registry
from backend.core.oauth.client import OIDCError
from tests.remote_access import oauth_helpers as H


def _provider(tmp_path, monkeypatch, **kw):

    H.configure_registry(monkeypatch, tmp_path, **kw)
    return registry.get_provider("google")


# ── Discovery + caching ──────────────────────────────────────────────────────


def test_get_metadata_caches(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    calls: list = []
    client.set_test_transport(H.make_transport(discovery_calls=calls))
    m1 = client.get_metadata(prov)
    m2 = client.get_metadata(prov)
    assert m1["issuer"] == H.ISSUER and m2["issuer"] == H.ISSUER
    assert len(calls) == 1  # second call served from cache


def test_get_metadata_idp_unavailable(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport(discovery_boom=True))
    with pytest.raises(OIDCError) as ei:
        client.get_metadata(prov)
    assert ei.value.reason == "idp_unavailable"


# ── id_token validation: happy + negatives ───────────────────────────────────


def _verify(prov, nonce="n1", **mint_kw):
    meta = client.get_metadata(prov)
    tok = H.mint_id_token(nonce=mint_kw.pop("token_nonce", nonce), **mint_kw)
    return client.verify_id_token(prov, meta, tok, nonce=nonce)


def test_verify_happy_path(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    claims = _verify(prov)
    assert claims["email"] == "analyst@corp.com"
    assert claims["sub"] == "google-sub-1"


@pytest.mark.security_regression
def test_verify_nonce_mismatch(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, nonce="expected", token_nonce="different")
    assert ei.value.reason == "nonce_mismatch"


@pytest.mark.security_regression
def test_verify_missing_nonce(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, nonce="n1", omit=("nonce",))
    assert ei.value.reason == "nonce_mismatch"


@pytest.mark.security_regression
def test_verify_bad_aud(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, aud="some-other-client")
    assert ei.value.reason == "bad_aud"


@pytest.mark.security_regression
def test_verify_bad_azp(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, extra={"azp": "not-the-client"})
    assert ei.value.reason == "bad_aud"


@pytest.mark.security_regression
def test_verify_bad_iss(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, iss="https://evil.example")
    assert ei.value.reason == "bad_iss"


@pytest.mark.security_regression
def test_verify_expired(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, iat_delta=-1000, exp_delta=-500)
    assert ei.value.reason == "expired"


@pytest.mark.security_regression
def test_verify_nbf_future(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, nbf_delta=10_000)
    assert ei.value.reason == "expired"


@pytest.mark.security_regression
def test_verify_iat_older_than_cookie_ttl(tmp_path, monkeypatch):
    """A token minted long before this login could have started is rejected even
    if exp is still in the future (design §2.3.5)."""
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, iat_delta=-5000, exp_delta=5000)
    assert ei.value.reason == "expired"


@pytest.mark.security_regression
def test_verify_unverified_email(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, email_verified=False)
    assert ei.value.reason == "unverified_email"


@pytest.mark.security_regression
def test_verify_missing_email(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, omit=("email",))
    assert ei.value.reason == "unverified_email"


@pytest.mark.security_regression
def test_verify_wrong_hosted_domain(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch, allowed_hd="corp.com")
    client.set_test_transport(H.make_transport())
    with pytest.raises(OIDCError) as ei:
        _verify(prov, extra={"hd": "evil.com"})
    assert ei.value.reason == "wrong_domain"


def test_verify_matching_hosted_domain_ok(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch, allowed_hd="corp.com")
    client.set_test_transport(H.make_transport())
    claims = _verify(prov, extra={"hd": "corp.com"})
    assert claims["hd"] == "corp.com"


def test_verify_google_scheme_less_issuer_accepted(tmp_path, monkeypatch):
    """Google accepts both issuer forms; extra_issuers pins the scheme-less
    variant (design §2.9)."""
    prov = _provider(tmp_path, monkeypatch, extra={"extra_issuers": ["idp.example"]})
    client.set_test_transport(H.make_transport())
    claims = _verify(prov, iss="idp.example")  # not the discovery issuer, but pinned
    assert claims["iss"] == "idp.example"


# ── Alg-confusion / signature (defense-in-depth: pins our config) ────────────


@pytest.mark.security_regression
def test_verify_alg_none_rejected(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    meta = client.get_metadata(prov)

    # Hand-craft an unsigned alg:none token.
    def seg(d):
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    unsigned = (
        seg({"alg": "none", "kid": "test-key-1"})
        + "."
        + seg({"iss": H.ISSUER, "aud": H.CLIENT_ID, "nonce": "n1"})
        + "."
    )
    with pytest.raises(OIDCError) as ei:
        client.verify_id_token(prov, meta, unsigned, nonce="n1")
    assert ei.value.reason == "alg_rejected"


@pytest.mark.security_regression
def test_verify_hs256_confusion_rejected(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    meta = client.get_metadata(prov)
    # Sign HS256 with the public-key bytes — the classic RS256→HS256 downgrade.
    pub_bytes = json.dumps(H.load_jwks()).encode()
    hkey = OctKey.import_key(base64.urlsafe_b64encode(pub_bytes).decode())
    forged = jwt.encode(
        {"alg": "HS256", "kid": "test-key-1"}, {"iss": H.ISSUER, "aud": H.CLIENT_ID, "nonce": "n1"}, hkey
    )
    with pytest.raises(OIDCError) as ei:
        client.verify_id_token(prov, meta, forged, nonce="n1")
    assert ei.value.reason == "alg_rejected"


@pytest.mark.security_regression
def test_verify_token_signed_by_non_jwks_key_rejected(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    meta = client.get_metadata(prov)
    # Same kid, DIFFERENT key → signature won't verify against the served JWKS.
    rogue = RSAKey.generate_key(2048, {"kid": "test-key-1", "use": "sig"})
    tok = H.mint_id_token(nonce="n1", key=rogue)
    with pytest.raises(OIDCError) as ei:
        client.verify_id_token(prov, meta, tok, nonce="n1")
    assert ei.value.reason == "alg_rejected"


@pytest.mark.security_regression
def test_verify_unknown_kid_after_refetch_rejected(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    meta = client.get_metadata(prov)
    rogue = RSAKey.generate_key(2048, {"kid": "not-in-jwks", "use": "sig"})
    tok = H.mint_id_token(nonce="n1", key=rogue, kid="not-in-jwks")
    with pytest.raises(OIDCError) as ei:
        client.verify_id_token(prov, meta, tok, nonce="n1")
    assert ei.value.reason == "alg_rejected"


# ── Key rotation single-flight (design §3.2 / test §7.9) ─────────────────────


@pytest.mark.security_regression
def test_unknown_kid_triggers_exactly_one_refetch_under_concurrency(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    # Rotated key that appears in the JWKS only AFTER rotation is flipped on.
    rotated_key = RSAKey.generate_key(2048, {"kid": "k2", "use": "sig"})
    base = H.load_jwks()
    rotated = {"keys": base["keys"] + [rotated_key.as_dict(private=False)]}
    flag = {"on": False}
    jwks_calls: list = []

    def jwks_provider():
        return rotated if flag["on"] else base

    client.set_test_transport(H.make_transport(jwks_calls=jwks_calls, jwks_provider=jwks_provider))
    meta = client.get_metadata(prov)
    # Warm the JWKS cache with the pre-rotation set (kid k2 absent).
    client.get_jwks(prov, meta)
    assert len(jwks_calls) == 1
    jwks_calls.clear()

    # Now rotate and fire N concurrent verifications of a k2-signed token.
    flag["on"] = True
    tok = H.mint_id_token(nonce="n1", key=rotated_key, kid="k2")
    results: list = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        try:
            results.append(client.verify_id_token(prov, meta, tok, nonce="n1"))
        except Exception as e:  # noqa: BLE001
            results.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(isinstance(r, dict) for r in results), results
    # Exactly one JWKS refetch despite 8 concurrent unknown-kid verifications.
    assert len(jwks_calls) == 1


# ── Token exchange ───────────────────────────────────────────────────────────


def test_exchange_code_success(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)
    id_token = H.mint_id_token(nonce="n1")
    client.set_test_transport(H.make_transport(token_body=H.token_response(id_token)))
    meta = client.get_metadata(prov)
    token = client.exchange_code(prov, meta, code="c", code_verifier="v", redirect_uri="https://testserver/cb")
    assert token["id_token"] == id_token


@pytest.mark.security_regression
def test_exchange_code_rejected_is_pkce_failed(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)

    def on_token(request):
        return httpx.Response(400, json={"error": "invalid_grant"})

    client.set_test_transport(H.make_transport(on_token=on_token))
    meta = client.get_metadata(prov)
    with pytest.raises(OIDCError) as ei:
        client.exchange_code(prov, meta, code="bad", code_verifier="v", redirect_uri="https://testserver/cb")
    assert ei.value.reason == "pkce_failed"


def test_exchange_code_network_error_is_idp_unavailable(tmp_path, monkeypatch):
    prov = _provider(tmp_path, monkeypatch)

    def on_token(request):
        raise httpx.ConnectTimeout("timeout")

    client.set_test_transport(H.make_transport(on_token=on_token))
    meta = client.get_metadata(prov)
    with pytest.raises(OIDCError) as ei:
        client.exchange_code(prov, meta, code="c", code_verifier="v", redirect_uri="https://testserver/cb")
    assert ei.value.reason == "idp_unavailable"


def test_verify_uses_injected_transport_no_network(tmp_path, monkeypatch):
    """Sanity: with the transport set, no real socket is opened (KeySet parsed
    from the served static JWKS)."""
    prov = _provider(tmp_path, monkeypatch)
    client.set_test_transport(H.make_transport())
    meta = client.get_metadata(prov)
    ks = client.get_jwks(prov, meta)
    assert isinstance(ks, KeySet)


# ── Merged from test_oauth_config.py ──────────────────────────────────────────────────────

"""Provider registry + auth-config + admin oauth-providers + create-invite
OAuth validation (design §2.6 / §5.1 / §5.2).

The registry is default-OFF: without OAUTH_FLOW_STATE_SECRET (the feature
switch) nothing is advertised, and passcode login is unchanged.
"""


import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.routers import share_admin, share_auth
from backend.utils import tunnel

_REMOTE_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}


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
    return H.make_remote_access_app(share_auth.router)


def _admin_app() -> FastAPI:
    return H.make_remote_access_app(share_admin.router)


# ── Registry ────────────────────────────────────────────────────────────────


def test_oauth_disabled_by_default(monkeypatch):
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    monkeypatch.delenv("OAUTH_PROVIDERS_CONFIG_PATH", raising=False)
    registry.reset_cache_for_tests()
    assert registry.feature_on() is False
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


def test_provider_without_env_creds_is_invisible(tmp_path, monkeypatch):
    _configure_provider(tmp_path, monkeypatch, with_creds=False)
    assert registry.get_all_providers() == []
    # Feature switch is on (secret set), but no fully-configured provider.
    assert registry.feature_on() is True


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


# ── Merged from test_oauth_routes.py ──────────────────────────────────────────────────────

"""Route-level tests for /api/share/oauth/{authorize,callback} (design §2.8).

Both routes are top-level browser navigations: success/failure are ALWAYS 302
redirects (never JSON). Failures redirect to /share-login?oauth_error=<code> and
write the matching audit row. Callback tests seal their own flow-state cookie so
state/nonce/verifier are fully controlled.
"""

import pytest

from backend.core.oauth import client as oidc
from backend.core.oauth import registry as flow_state
from backend.routers import share_oauth

REDIRECT_URI = "https://testserver/api/share/oauth/callback"


def _app() -> FastAPI:
    return H.make_remote_access_app(share_oauth.router)


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


# ── Merged from test_oauth_pkce_state.py ──

import re
from urllib.parse import parse_qs, urlparse

import pytest

from backend.core.oauth.registry import FlowStateError
from backend.utils.remote_access import RemoteAccessMiddleware

_PAYLOAD = {
    "provider": "google",
    "state": "state-token",
    "nonce": "nonce-token",
    "verifier": "pkce-verifier",
    "redirect_uri": "https://testserver/api/share/oauth/callback",
    "return": "/dashboard",
}

# RFC 7636 Appendix B
RFC_VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
RFC_CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
UNRESERVED = re.compile(r"^[A-Za-z0-9\-._~]+$")


def _authorize() -> tuple[dict, dict]:
    """One /authorize round trip -> (authorize-URL query, opened flow-state)."""
    from backend.routers import share_oauth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_oauth.router)
    with TestClient(app, follow_redirects=False) as c:
        r = c.get("/api/share/oauth/authorize", params={"provider": "google"}, headers=H.REMOTE_HEADERS)
    assert r.status_code == 302
    q = {k: v[0] for k, v in parse_qs(urlparse(r.headers["location"]).query).items()}
    sealed = next(
        line.split("=", 1)[1].split(";", 1)[0]
        for line in r.headers.get_list("set-cookie")
        if line.startswith(flow_state.COOKIE_NAME + "=")
    )
    return q, flow_state.open_flow_state(sealed)


@pytest.mark.security_regression
def test_s256_challenge_matches_rfc7636_vector():
    assert oidc.pkce_challenge(RFC_VERIFIER) == RFC_CHALLENGE


@pytest.mark.security_regression
def test_verifier_length_and_charset_within_rfc7636_limits():
    for _ in range(200):
        v = oidc.make_pkce_verifier()
        assert 43 <= len(v) <= 128
        assert UNRESERVED.match(v)


@pytest.mark.security_regression
def test_generated_values_are_unique():
    verifiers = {oidc.make_pkce_verifier() for _ in range(500)}
    tokens = {oidc.make_state_token() for _ in range(500)}
    assert len(verifiers) == 500
    assert len(tokens) == 500
    assert all(len(t) >= 32 and UNRESERVED.match(t) for t in tokens)


@pytest.mark.security_regression
def test_authorize_flow_binds_challenge_to_sealed_verifier_and_is_fresh(tmp_path, monkeypatch):
    H.configure_registry(monkeypatch, tmp_path)
    oidc.set_test_transport(H.make_transport())
    tunnel.get_tunnel_manager().start_sharing(public_endpoint=H.PUBLIC_ENDPOINT)
    q1, fs1 = _authorize()
    q2, fs2 = _authorize()
    for q, fs in ((q1, fs1), (q2, fs2)):
        assert q["code_challenge_method"] == "S256"
        assert q["code_challenge"] == oidc.pkce_challenge(fs["verifier"])
        assert q["state"] == fs["state"]
        assert q["nonce"] == fs["nonce"]
        assert 43 <= len(fs["verifier"]) <= 128 and UNRESERVED.match(fs["verifier"])
        assert fs["state"] != fs["nonce"]
    assert len({fs1["state"], fs2["state"], fs1["nonce"], fs2["nonce"]}) == 4
    assert fs1["verifier"] != fs2["verifier"]
    assert q1["code_challenge"] != q2["code_challenge"]


# ── AES-256-GCM oauth_flow_state cookie codec ────────────────────────────────


def test_seal_open_round_trip(monkeypatch):
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-abcdef-0123456789")
    sealed = flow_state.seal_flow_state(_PAYLOAD)
    assert flow_state.open_flow_state(sealed) == _PAYLOAD
    assert "pkce-verifier" not in sealed and "nonce-token" not in sealed


@pytest.mark.security_regression
def test_tampered_cookie_fails(monkeypatch):
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-abcdef-0123456789")
    sealed = flow_state.seal_flow_state(_PAYLOAD)
    flipped = sealed[:-3] + ("AAA" if not sealed.endswith("AAA") else "BBB")
    with pytest.raises(FlowStateError):
        flow_state.open_flow_state(flipped)


@pytest.mark.security_regression
def test_wrong_key_cannot_open(monkeypatch):
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-A-000000000000")
    sealed = flow_state.seal_flow_state(_PAYLOAD)
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-B-111111111111")
    with pytest.raises(FlowStateError):
        flow_state.open_flow_state(sealed)


@pytest.mark.security_regression
def test_missing_secret_fails_closed(monkeypatch):
    monkeypatch.delenv("OAUTH_FLOW_STATE_SECRET", raising=False)
    with pytest.raises(FlowStateError):
        flow_state.seal_flow_state(_PAYLOAD)
    with pytest.raises(FlowStateError):
        flow_state.open_flow_state("anything")


@pytest.mark.security_regression
def test_empty_and_garbage_cookie_fail(monkeypatch):
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-abcdef-0123456789")
    for bad in ("", "not-base64!!", "aGVsbG8"):  # empty / bad b64 / too-short
        with pytest.raises(FlowStateError):
            flow_state.open_flow_state(bad)


# ── Merged from test_oauth_rbac_parity.py ──

import pytest

_REMOTE = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}


def _rbac_app() -> FastAPI:
    return H.make_remote_access_app(share_auth.router, share_admin.router)


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
    with TestClient(_rbac_app()) as c:
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
    with TestClient(_rbac_app()) as c:
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
    with TestClient(_rbac_app()) as c:
        r = c.post(
            "/api/share/login",
            json={"email": "oauthonly@corp.com", "passcode": "anything-at-all-here"},
            headers=_REMOTE,
        )
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "invalid_credentials"
