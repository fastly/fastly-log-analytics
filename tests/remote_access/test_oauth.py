"""OIDC client: id_token validation, discovery/JWKS caching, single-flight
refetch, token exchange (design §2.2 / §2.3 / §3.1 / §3.2)."""

from __future__ import annotations

import asyncio
import base64
import json
import re
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
from backend.core.oauth.client import OIDCError
from backend.utils.tunnel.manager import AnalystSession
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from joserfc import jwt
from joserfc.jwk import KeySet, OctKey, RSAKey
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse

from backend.core import share_db
from backend.core.oauth import client, registry
from backend.core.share_db import validation
from backend.utils import remote_access as ra
from backend.utils import tunnel
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import (
    _PII_FORBIDDEN_FILTER_COLS,
    BODY_INSPECT_MAX_BYTES,
    MAX_ANALYST_QUERY_SPAN,
    RemoteAccessMiddleware,
    TimeBounds,
    _analyst_api_limiter,
    _body_service_ids,
    _is_blocked_path,
    _is_private_or_loopback,
    _is_sse_route,
    _local_host_allowed,
    _origin_allowed,
    _remote_host_allowed,
    _static_limiter,
    _StaticAssetLimiter,
    _strip_analyst_envelope,
    _time_bounds_from_params,
    analyst_clamp_cache_key,
    apply_response_hardening,
    clamp_or_400,
    client_ip,
    get_analyst_time_bounds,
    is_request_remote,
    resolve_analyst_insights_clamp,
)
from tests.remote_access import conftest as H


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

from backend.routers import share_admin, share_auth

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

from urllib.parse import parse_qs, urlparse

import pytest
from backend.core.oauth.registry import FlowStateError

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


# ── Merged from tests/utils/test_remote_access_branches.py ──


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Per-test share DB + reset tunnel singleton."""
    monkeypatch.setenv("REMOTE_SHARE_DB_DIR", str(tmp_path / "system"))
    tunnel.reset_for_tests()
    yield
    share_db.close_all_connections()
    tunnel.reset_for_tests()


# ── App builder + helpers (lifted from test_middleware to keep the suite self-contained) ──


def _build_app() -> FastAPI:
    from backend.routers import share_admin, share_auth

    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    @app.get("/api/dashboard")
    def _dash():
        return {"ok": True}

    @app.post("/api/dashboard/aggregates")
    def _dash_agg(payload: dict | None = None):
        # Read-allowed POST under /api/dashboard/ for the body-service-id branch.
        return {"ok": True}

    @app.get("/api/services/{service_id}/scoring/labels")
    def _scoring_labels(service_id: str):
        return {"ok": True, "service_id": service_id}

    @app.get("/_next/static/foo.js")
    def _static():
        return {"ok": True}

    return app


@pytest.fixture
def app():
    return _build_app()


@pytest.fixture
def ra_client(app):
    with TestClient(app) as c:
        yield c


def _seed_invite(service_ids=None, ip_whitelist=None) -> dict:
    invite = share_db.create_remote_invite(
        name="Drew",
        email="drew@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=ip_whitelist,
        service_ids=service_ids or ["svcA"],
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])
    return share_db.get_remote_invite(invite["id"])


def _start_share(endpoint: str = "https://testserver"):
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint=endpoint)
    return mgr


def _login_analyst(client, invite, *, host="testserver") -> str:
    r = client.post(
        "/api/share/login",
        json={"email": invite["email"], "passcode": "ocean-breeze-cabin-42"},
        headers={
            "X-Remote-Analyst": "1",
            "Host": host,
            "Origin": f"https://{host}",
        },
    )
    assert r.status_code == 200, r.text
    # L1: the session id is cookie-only now (not in the JSON body). Pull it from
    # the response Set-Cookie (pending before TOS accept, full after); the
    # TestClient jar refuses `secure` cookies over http://, so set it manually.
    sid = ""
    for raw in r.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')  # the deletion cookie carries value="" (quoted)
        if name in ("analyst_pending_session_id", "analyst_session_id") and val:
            sid = val
    client.cookies.set("analyst_session_id", sid)
    return sid


def _make_req(
    *,
    method: str = "POST",
    path: str = "/x",
    content_type: str | None = "application/json",
    body: bytes = b"",
    client: tuple[str, int] | None = ("127.0.0.1", 12345),
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if content_type is not None:
        headers.append((b"content-type", content_type.encode()))
    if extra_headers:
        headers.extend(extra_headers)
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": headers,
        "query_string": b"",
        "client": client,
    }
    sent = {"v": False}

    async def _recv():
        if sent["v"]:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent["v"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, _recv)


def _make_analyst_session(
    *,
    query_window_hours: int | None = None,
    query_start_time: str | None = "2026-06-01T00:00:00Z",
    query_end_time: str | None = "2026-06-30T00:00:00Z",
) -> AnalystSession:
    return AnalystSession(
        session_id="sid",
        invite_id="iid",
        name="n",
        email="e@x",
        ip_address="1.2.3.4",
        user_agent="ua",
        fingerprint_signature="fp",
        pii_policy={},
        query_window_hours=query_window_hours,
        query_start_time=query_start_time,
        query_end_time=query_end_time,
        login_time="2026-06-15T00:00:00Z",
        last_active_time="2026-06-15T00:00:00Z",
    )


# ── Helper: _is_private_or_loopback ────────────────────────────────────────


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("127.0.0.1", True),
        ("::1", True),
        # Drops the over-broad "private" rule — RFC1918 must NOT count as local.
        ("10.0.0.5", False),
        ("192.168.1.1", False),
        ("169.254.169.254", False),  # AWS metadata
        ("8.8.8.8", False),
        # ValueError fallback for non-IP strings — only the two stubs return True.
        ("testclient", True),
        ("localhost", True),
        ("evil.example.com", False),
        ("not-an-ip-or-stub", False),
        ("", False),
    ],
)
def test_is_private_or_loopback_classification(host, expected):
    assert _is_private_or_loopback(host) is expected


# ── Helper: LOCAL_ADMIN_CIDRS opt-in trust segment ─────────────────────────


@pytest.mark.security_regression
def test_local_admin_cidrs_default_off(monkeypatch):
    """With the env unset, RFC1918 stays remote — prod behavior unchanged."""
    monkeypatch.delenv("LOCAL_ADMIN_CIDRS", raising=False)
    assert _is_private_or_loopback("172.28.0.1") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_scoped_trust(monkeypatch):
    """A declared infra subnet classifies as admin; everything else stays
    remote (a VPN analyst on another private range must NOT be promoted)."""
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "172.28.0.0/16")
    assert _is_private_or_loopback("172.28.0.1") is True
    assert _is_private_or_loopback("172.28.44.9") is True
    assert _is_private_or_loopback("10.0.0.5") is False
    assert _is_private_or_loopback("192.168.1.1") is False
    assert _is_private_or_loopback("169.254.169.254") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_rejects_catch_all(monkeypatch):
    """0.0.0.0/0 (or anything catch-all-sized) is the Host-spoof admin bypass
    reborn — it must be refused, not honored."""
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "0.0.0.0/0")
    assert _is_private_or_loopback("8.8.8.8") is False
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "::/0")
    assert _is_private_or_loopback("2001:db8::1") is False


@pytest.mark.security_regression
def test_local_admin_cidrs_rejects_historical_bypass_classes(monkeypatch):
    """The guard must make the two removed bypasses unconfigurable: broad
    RFC1918 (VPN analyst → admin) and link-local (169.254.169.254 cloud
    metadata SSRF → admin). Public ranges are refused outright."""
    for cidr, probe in [
        ("10.0.0.0/8", "10.1.2.3"),
        ("169.254.0.0/16", "169.254.169.254"),
        ("169.254.169.254/32", "169.254.169.254"),
        ("8.8.8.0/24", "8.8.8.8"),
    ]:
        monkeypatch.setenv("LOCAL_ADMIN_CIDRS", cidr)
        assert _is_private_or_loopback(probe) is False


@pytest.mark.security_regression
def test_local_admin_cidrs_ignores_garbage(monkeypatch):
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "not-a-cidr, ,172.28.0.0/16")
    assert _is_private_or_loopback("172.28.0.1") is True
    assert _is_private_or_loopback("8.8.8.8") is False


# ── Helper: client_ip ────────────────────────────────────────────────────


def test_client_ip_returns_default_when_no_client():
    """Starlette gives request.client = None for ASGI scopes without a peer."""
    fake_req = SimpleNamespace(client=None)
    assert client_ip(fake_req, default="custom-default") == "custom-default"
    assert client_ip(fake_req) == "0.0.0.0"

    asgi_req = _make_req(method="GET", path="/", content_type=None, client=None)
    assert client_ip(asgi_req, default="0.0.0.0") == "0.0.0.0"
    assert client_ip(asgi_req, default="admin") == "admin"


# ── Helper: _local_host_allowed / _remote_host_allowed / _origin_allowed ──


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("", False),
        ("127.0.0.1", True),
        ("127.0.0.1:8000", True),
        ("testserver", True),
        ("backend:8000", True),
        ("attacker.example.com", False),
    ],
)
def test_local_host_allowed(host, expected):
    assert _local_host_allowed(host) is expected


def test_remote_host_allowed_matches_registered_endpoint():
    assert _remote_host_allowed("") is False
    assert _remote_host_allowed("anything.example.com") is False
    _start_share("https://tun-xyz.lhr.life")
    assert _remote_host_allowed("tun-xyz.lhr.life") is True
    assert _remote_host_allowed("tun-xyz.lhr.life:443") is True
    assert _remote_host_allowed("other.lhr.life") is False


def test_remote_host_and_origin_mocked_tunnel_state(monkeypatch):
    """Line 371-380: verify mocked tunnel manager endpoint checks for both helpers."""
    mgr = MagicMock()
    mgr.state.public_endpoint = None
    monkeypatch.setattr(ra, "get_tunnel_manager", lambda: mgr)
    assert ra._remote_host_allowed("anything.example.com") is False
    assert _origin_allowed("") is False
    assert _origin_allowed("https://") is False
    assert _origin_allowed("file://") is False

    mgr.state.public_endpoint = "https://my.endpoint.example/"
    assert ra._origin_allowed("https://other.example.com/path") is False
    assert ra._origin_allowed("https://my.endpoint.example/path") is True


def test_origin_allowed_matches_endpoint_hostname():
    _start_share("https://tun-abc.lhr.life")
    assert _origin_allowed("https://tun-abc.lhr.life") is True
    assert _origin_allowed("https://attacker.example.com") is False


# ── Helper: _strip_analyst_envelope ────────────────────────────────────────


def _make_streaming(body: bytes, *, content_type: str, status: int = 200) -> StreamingResponse:
    async def _gen():
        yield body

    resp = StreamingResponse(_gen(), status_code=status, media_type=content_type)
    resp.headers["content-type"] = content_type
    return resp


def _strip_json(payload: dict | list, *, mask_ips: bool | None = None) -> tuple[Response, dict | list]:
    resp = _make_streaming(json.dumps(payload).encode(), content_type="application/json")
    session = None if mask_ips is None else SimpleNamespace(pii_policy={"mask_ips": mask_ips})
    out = asyncio.run(_strip_analyst_envelope(resp, analyst_session=session))
    return out, json.loads(out.body)


def test_strip_envelope_non_json_and_invalid_json():
    resp = _make_streaming(b"hello world", content_type="text/plain")
    out = asyncio.run(_strip_analyst_envelope(resp))
    # Non-JSON returns the SAME StreamingResponse object (no body buffering).
    assert out is resp

    bad_resp = _make_streaming(b"<not json>", content_type="application/json")
    bad_out = asyncio.run(_strip_analyst_envelope(bad_resp))
    assert bad_out.status_code == 200
    assert bad_out.body == b"<not json>"

    # Top-level lists hit the `isinstance(data, dict)` False branch.
    _, list_parsed = _strip_json([1, 2, 3])
    assert list_parsed == [1, 2, 3]


@pytest.mark.parametrize(
    ("payload", "removed_keys", "expected_kept"),
    [
        (
            {"data": [1, 2, 3], "_section_timings": {"summary": 0.1}},
            [],
            {"data": [1, 2, 3], "_section_timings": {"summary": 0.1}},
        ),
        (
            {
                "data": [1],
                "_debug_queries": ["SELECT 1"],
                "_debug_calls": [{"url": "https://api.fastly.com/foo"}],
                "_debug_sqlite": [{"seq": 1, "sql": "SELECT session_id FROM scoring_labels"}],
                "_is_cached": True,
            },
            ["_debug_queries", "_debug_calls", "_debug_sqlite"],
            {"data": [1], "_is_cached": True},
        ),
        (
            {
                "columns": ["x"],
                "data": [{"x": 1}],
                "debug_queries": [{"sql": "-- DuckDB Storage View Resolution\nCREATE VIEW logs ..."}],
                "debug_sqlite": [{"seq": 1, "sql": "SELECT * FROM cron_runs"}],
            },
            ["debug_queries", "debug_sqlite"],
            {"columns": ["x"], "data": [{"x": 1}]},
        ),
        (
            {"data": [], "debug_calls": [{"url": "https://api.fastly.com/services/foo"}]},
            ["debug_calls"],
            {"data": []},
        ),
        (
            {
                "data": [1],
                "section_timings": [{"section": "summary", "time_ms": 1.2}],
                "_section_timings": [{"section": "validate", "time_ms": 0.5}],
            },
            [],
            {
                "data": [1],
                "section_timings": [{"section": "summary", "time_ms": 1.2}],
                "_section_timings": [{"section": "validate", "time_ms": 0.5}],
            },
        ),
    ],
)
def test_strip_envelope_debug_keys_and_timings(payload, removed_keys, expected_kept):
    out, parsed = _strip_json(payload)
    for k in removed_keys:
        assert k not in parsed
    for k, v in expected_kept.items():
        assert parsed[k] == v
    if removed_keys:
        assert out.headers["content-length"] == str(len(out.body))


# ── R-2: mask_ips PII masking via analyst session policy ───────────────────


def test_strip_envelope_masks_ips_when_session_policy_enabled():
    """When analyst_session.pii_policy.mask_ips is True, the helper walks
    the response body and masks any `ip` / `client_ip` / `ip_address` /
    `remote_addr` field via apply_pii_policy. Last-octet-xxx for IPv4."""
    payload = {
        "sessions": [
            {"ip": "1.2.3.4", "country": "US", "request_count": 7},
            {"ip": "5.6.7.8", "country": "GB", "request_count": 3},
        ],
        "total": 2,
    }
    out, parsed = _strip_json(payload, mask_ips=True)
    ips = [row["ip"] for row in parsed["sessions"]]
    assert ips == ["1.2.3.xxx", "5.6.7.xxx"], f"expected masked IPs, got {ips}"
    assert parsed["sessions"][0]["country"] == "US"
    assert parsed["sessions"][0]["request_count"] == 7
    assert out.headers["content-length"] == str(len(out.body))


@pytest.mark.parametrize("mask_ips", [False, None])
def test_strip_envelope_does_not_mask_when_disabled_or_admin(mask_ips):
    _, parsed = _strip_json({"sessions": [{"ip": "1.2.3.4"}]}, mask_ips=mask_ips)
    assert parsed["sessions"][0]["ip"] == "1.2.3.4"


def test_strip_envelope_combines_strip_and_mask_in_one_pass():
    """Both the envelope strip AND the PII mask apply in the same buffered
    body — we re-walk only once."""
    payload = {
        "data": [{"ip": "9.9.9.9"}],
        "_debug_queries": [{"sql": "SELECT 1"}],
        "section_timings": [{"section": "summary", "time_ms": 1.0}],
    }
    _, parsed = _strip_json(payload, mask_ips=True)
    assert "_debug_queries" not in parsed
    assert parsed["data"][0]["ip"] == "9.9.9.xxx"
    assert parsed["section_timings"][0]["section"] == "summary"


@pytest.mark.security_regression
def test_strip_envelope_masks_dashboard_top_n_ip_panel():
    """PII LEAK REGRESSION (Top IPs card): with mask_ips=True the IP top-N panel's
    generic ``value`` strings are masked, while non-IP panels (url, ua, country) stay verbatim."""
    payload = {
        "data": {
            "ip": {
                "top": [
                    {"value": "203.0.113.45", "count": 12},
                    {"value": "198.51.100.7", "count": 5},
                ],
                "total": 17,
            },
            "url": {"top": [{"value": "/login", "count": 9}], "total": 9},
            "ua": {"top": [{"value": "curl/8.4.0", "count": 4}], "total": 4},
            "country": {"top": [{"value": "US", "count": 17}], "total": 17},
        },
        "map_data": [{"country": "US", "count": 17}],
        "total_rows": 17,
    }
    out, parsed = _strip_json(payload, mask_ips=True)
    ip_values = [r["value"] for r in parsed["data"]["ip"]["top"]]
    assert ip_values == ["203.0.113.xxx", "198.51.100.xxx"], (
        f"Top IPs card leaked raw client IPs to a mask_ips analyst: {ip_values}"
    )
    assert parsed["data"]["url"]["top"][0]["value"] == "/login"
    assert parsed["data"]["ua"]["top"][0]["value"] == "curl/8.4.0"
    assert parsed["data"]["country"]["top"][0]["value"] == "US"
    assert parsed["map_data"][0]["country"] == "US"
    assert out.headers["content-length"] == str(len(out.body))


@pytest.mark.security_regression
def test_strip_envelope_admin_top_n_ip_panel_not_masked():
    """Admin path (no analyst session) must NOT mask the Top IPs panel."""
    payload = {"data": {"ip": {"top": [{"value": "203.0.113.45", "count": 1}], "total": 1}}}
    _, parsed = _strip_json(payload, mask_ips=None)
    assert parsed["data"]["ip"]["top"][0]["value"] == "203.0.113.45"


@pytest.mark.security_regression
def test_strip_envelope_leaves_oip_panel_visible_for_analyst():
    """ACCEPTED-INTENT pin (2026-06-24 PII audit): the origin IP (``oip``)
    top-N panel is INTENTIONALLY left visible to a mask_ips analyst."""
    payload = {
        "data": {
            "ip": {"top": [{"value": "203.0.113.45", "count": 9}], "total": 9},
            "oip": {"top": [{"value": "151.101.1.51", "count": 9}], "total": 9},
        },
    }
    _, parsed = _strip_json(payload, mask_ips=True)
    assert parsed["data"]["ip"]["top"][0]["value"] == "203.0.113.xxx"
    assert parsed["data"]["oip"]["top"][0]["value"] == "151.101.1.51"


# ── apply_response_hardening ───────────────────────────────────────────────


def test_apply_response_hardening_sets_defaults_and_preserves_existing():
    resp = Response(content="x")
    apply_response_hardening(resp)
    assert resp.headers["Cache-Control"] == "private, no-store"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"
    csp = resp.headers["Content-Security-Policy"]
    assert "script-src" in csp
    assert "connect-src 'self'" in csp
    assert "connect-src 'self' 'unsafe-inline'" not in csp
    assert "img-src 'self' 'unsafe-inline'" not in csp
    assert resp.headers["Cross-Origin-Opener-Policy"] == "same-origin"

    resp_custom = Response(content="x", headers={"Cache-Control": "public, max-age=300"})
    apply_response_hardening(resp_custom)
    assert resp_custom.headers["Cache-Control"] == "public, max-age=300"


# ── _StaticAssetLimiter ────────────────────────────────────────────────────


def test_static_limiter_thresholds_and_eviction(monkeypatch):
    lim = _StaticAssetLimiter()
    for _ in range(10):
        assert lim.check("1.2.3.4", 100) is True

    lim_req = _StaticAssetLimiter()
    for _ in range(lim_req.REQ_LIMIT):
        lim_req.check("1.2.3.4", 0)
    assert lim_req.check("1.2.3.4", 0) is False

    lim_byte = _StaticAssetLimiter()
    assert lim_byte.check("1.2.3.4", lim_byte.BYTE_LIMIT) is True
    assert lim_byte.check("1.2.3.4", 100) is False

    # Stale per-IP entries are dropped when the tracked-IP cap is exceeded.
    lim_evict = _StaticAssetLimiter()
    monkeypatch.setattr(lim_evict, "MAX_TRACKED_IPS", 3)
    real_now = time.time()
    monkeypatch.setattr(time, "time", lambda: real_now - 9999)
    for i in range(3):
        lim_evict.check(f"ip-{i}", 100)
    monkeypatch.setattr(time, "time", lambda: real_now)
    assert lim_evict.check("ip-new", 50) is True
    assert len(lim_evict._events) == 1

    # Fresh keys beyond the cap evict the least-recently-touched IP, never exceeding the cap.
    lim_lru = _StaticAssetLimiter()
    monkeypatch.setattr(lim_lru, "MAX_TRACKED_IPS", 2)
    for i in range(5):
        lim_lru.check(f"ip-{i}", 10)
    assert len(lim_lru._events) == 2
    lim_lru.check("ip-trigger", 0)
    assert len(lim_lru._events) == 2


# ── TimeBounds.clamp + get_analyst_time_bounds ────────────────────────────


@pytest.mark.parametrize(
    ("bounds", "req_start", "req_end", "expected_start", "expected_end"),
    [
        (
            TimeBounds(start=datetime(2026, 6, 1, tzinfo=UTC), end=datetime(2026, 6, 30, tzinfo=UTC)),
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 6, 1, tzinfo=UTC),
            datetime(2026, 6, 30, tzinfo=UTC),
        ),
        (
            TimeBounds(start=datetime(2026, 6, 1, tzinfo=UTC), end=datetime(2026, 6, 30, tzinfo=UTC)),
            datetime(2026, 6, 10, tzinfo=UTC),
            datetime(2026, 6, 20, tzinfo=UTC),
            datetime(2026, 6, 10, tzinfo=UTC),
            datetime(2026, 6, 20, tzinfo=UTC),
        ),
        (
            TimeBounds(),
            None,
            datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 15, 11, 0, 0, tzinfo=UTC),
            datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC),
        ),
    ],
)
def test_timebounds_clamp_explicit_cases(bounds, req_start, req_end, expected_start, expected_end):
    start, end = bounds.clamp(req_start, req_end)
    assert start == expected_start
    assert end == expected_end


def test_timebounds_clamp_defaults_and_empty_error():
    bounds = TimeBounds()
    start, end = bounds.clamp(None, None)
    assert timedelta(minutes=59) < (end - start) < timedelta(minutes=61)

    fixed_start = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
    s2, e2 = bounds.clamp(fixed_start, None)
    assert s2 == fixed_start and e2 > s2

    empty_bounds = TimeBounds(
        start=datetime(2026, 6, 10, tzinfo=UTC),
        end=datetime(2026, 6, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="empty"):
        empty_bounds.clamp(None, None)


def test_get_analyst_time_bounds_session_variants():
    req_open = SimpleNamespace(state=SimpleNamespace())
    out_open = get_analyst_time_bounds(req_open)
    assert out_open.start is None and out_open.end is None

    req_abs = SimpleNamespace(state=SimpleNamespace(analyst_session=_make_analyst_session()))
    out_abs = get_analyst_time_bounds(req_abs)
    assert out_abs.start == datetime(2026, 6, 1, tzinfo=UTC)
    assert out_abs.end == datetime(2026, 6, 30, tzinfo=UTC)

    # Relative window (e.g. last 24h) tightens ancient start and ceilings end to now.
    rel_session = _make_analyst_session(
        query_window_hours=24,
        query_start_time="2020-01-01T00:00:00Z",
        query_end_time=None,
    )
    out_rel = get_analyst_time_bounds(SimpleNamespace(state=SimpleNamespace(analyst_session=rel_session)))
    assert out_rel.end is not None and out_rel.start is not None
    assert out_rel.start > datetime(2026, 1, 1, tzinfo=UTC)
    assert (out_rel.end - out_rel.start) == timedelta(hours=24)


# ── clamp_or_400 — route-layer helper (audit R-1) ──────────────────────────


def test_clamp_or_400_admin_and_analyst_cases():
    # Admin (no analyst session) with both bounds None passes through as (None, None).
    assert clamp_or_400(TimeBounds(), None, None, analyst_session=None) == (None, None)

    # Admin with explicit bounds clamps against open TimeBounds().
    out_adm = clamp_or_400(
        TimeBounds(),
        "2026-06-01T00:00:00Z",
        "2026-06-02T00:00:00Z",
        analyst_session=None,
    )
    assert out_adm[0].startswith("2026-06-01T00:00:00")
    assert out_adm[1].startswith("2026-06-02T00:00:00")

    # Analyst with request outside window [T1, T3] is clamped to [T1, T3].
    tb = TimeBounds(
        start=datetime(2026, 6, 10, tzinfo=UTC),
        end=datetime(2026, 6, 20, tzinfo=UTC),
    )
    out_clamped = clamp_or_400(
        tb,
        "2026-06-01T00:00:00Z",
        "2026-06-30T00:00:00Z",
        analyst_session=object(),
    )
    assert out_clamped[0].startswith("2026-06-10")
    assert out_clamped[1].startswith("2026-06-20")

    # Analyst with no request bounds clamps to default 1h window.
    out_def = clamp_or_400(TimeBounds(), None, None, analyst_session=object())
    assert out_def[0] is not None and out_def[1] is not None
    start_dt = datetime.fromisoformat(out_def[0].replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(out_def[1].replace("Z", "+00:00"))
    assert (end_dt - start_dt) == timedelta(hours=1)

    # Request fully outside the analyst's allowed window raises 400.
    tb_empty = TimeBounds(
        start=datetime(2026, 6, 10, tzinfo=UTC),
        end=datetime(2026, 6, 5, tzinfo=UTC),
    )
    with pytest.raises(HTTPException) as exc_info:
        clamp_or_400(tb_empty, None, None, analyst_session=object())
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail.get("time_range_empty") is True
    assert "empty" in exc_info.value.detail.get("error", "").lower()


# ── Middleware integration: branches that need a logged-in analyst ─────────


def test_static_asset_rate_limit_429(ra_client, monkeypatch):
    """Hammer /_next/* past the per-IP cap → 429."""
    _start_share()
    invite = _seed_invite()
    _login_analyst(ra_client, invite)

    monkeypatch.setattr(ra._static_limiter, "REQ_LIMIT", 3)
    ra._static_limiter.clear()

    headers = {"X-Remote-Analyst": "1", "Host": "testserver"}
    for _ in range(3):
        ra_client.get("/_next/static/foo.js", headers=headers)
    r = ra_client.get("/_next/static/foo.js", headers=headers)
    assert r.status_code == 429
    assert r.json()["error"] == "rate_limited"


def test_tos_pending_session_returns_403(ra_client):
    """A session whose linked invite has not accepted the current TOS gets
    its ``tos_pending`` flag re-synced by ``validate_session`` on every
    request, then the middleware returns 403 tos_pending."""
    _start_share()
    invite = _seed_invite()
    sid = _login_analyst(ra_client, invite)
    con = share_db.get_global_share_con()
    con.execute(
        "UPDATE remote_invites SET tos_accepted_at=NULL, tos_version=NULL WHERE id=?",
        (invite["id"],),
    )
    con.commit()

    r = ra_client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    tos = share_db.get_latest_tos()
    if tos is None:
        pytest.skip("No TOS row seeded; tos_pending branch unreachable.")
    assert r.status_code == 403
    assert r.json()["error"] == "tos_pending"
    _ = sid


def test_fingerprint_mismatch_boots_session(ra_client):
    """Changing User-Agent mid-session → 401 fingerprint_mismatch + the
    session is removed from the manager."""
    _start_share()
    invite = _seed_invite()
    sid = _login_analyst(ra_client, invite)
    mgr = tunnel.get_tunnel_manager()
    assert sid in mgr._sessions

    r = ra_client.get(
        "/api/dashboard?service=svcA",
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "User-Agent": "Mozilla/5.0 EvilBrowser/99 Linux",
        },
    )
    assert r.status_code == 401
    assert r.json()["error"] == "fingerprint_mismatch"
    assert sid not in mgr._sessions
    logs = share_db.get_share_audit_logs(limit=20, event_type="FINGERPRINT_MISMATCH")
    assert any(row.get("email") == invite["email"] for row in logs)


def test_ip_roaming_blocks_when_not_whitelisted(ra_client):
    """Login captures the request IP. If a subsequent request comes from a
    different IP and the invite has an explicit whitelist that does NOT
    include the new IP, the middleware rejects with ip_not_whitelisted."""
    _start_share()
    invite = _seed_invite(ip_whitelist="10.20.30.40")
    with patch("backend.core.share_db.ip_in_whitelist", return_value=True):
        sid = _login_analyst(ra_client, invite)
    mgr = tunnel.get_tunnel_manager()
    mgr._sessions[sid].ip_address = "10.20.30.40"

    r = ra_client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "ip_not_whitelisted"


def test_ip_roaming_allows_when_whitelist_passes(ra_client):
    """If the new IP IS in the whitelist (or whitelist is None), the
    session ip_address is updated rather than booted."""
    _start_share()
    invite = _seed_invite()
    sid = _login_analyst(ra_client, invite)
    mgr = tunnel.get_tunnel_manager()
    mgr._sessions[sid].ip_address = "9.9.9.9"

    r = ra_client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r.status_code == 200
    assert mgr._sessions[sid].ip_address == "testclient"


@pytest.mark.parametrize(
    ("body_svc", "expected_status", "expected_error"),
    [
        ("svcZ", 403, "service_not_authorized"),
        ("svcA", 200, None),
    ],
)
def test_body_service_id_authorization(ra_client, body_svc, expected_status, expected_error):
    """M-3: a forged service_id in the JSON body must be checked against the analyst's allowlist."""
    _start_share()
    invite = _seed_invite(service_ids=["svcA"])
    _login_analyst(ra_client, invite)

    r = ra_client.post(
        "/api/dashboard/aggregates",
        json={"service_id": body_svc, "filter": {}},
        headers={
            "X-Remote-Analyst": "1",
            "Host": "testserver",
            "Origin": "https://testserver",
        },
    )
    assert r.status_code == expected_status
    if expected_error:
        assert r.json()["error"] == expected_error
        assert r.json()["service"] == body_svc


@pytest.mark.parametrize(
    ("method", "content_type", "body", "expected"),
    [
        ("POST", "text/plain", b"", set()),
        ("GET", "application/json", b"", set()),
        ("POST", "application/json", b"<not json>", set()),
        ("POST", "application/json", json.dumps({"service_id": "svcA", "service": "svcB"}).encode(), {"svcA", "svcB"}),
        ("POST", "application/json", b"[1, 2, 3]", set()),
        # Regression for F002 (audit run 7ba15352): int service_id coerced to str.
        ("POST", "application/json", json.dumps({"service_id": 12345, "service": 67890}).encode(), {"12345", "67890"}),
    ],
)
def test_body_service_ids_helper_cases(method, content_type, body, expected):
    req = _make_req(method=method, content_type=content_type, body=body)
    assert set(asyncio.run(_body_service_ids(req))) == expected


def test_body_service_ids_helper_caps_buffered_body_at_4mib():
    """Streaming an arbitrarily large chunked body must not unboundedly
    accumulate in memory (Regression for F003, audit run 7ba15352)."""
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/x",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
    }
    chunk_size = 2 * 1024 * 1024
    chunks_served = {"n": 0}
    big_chunk = b"x" * chunk_size

    async def _recv():
        chunks_served["n"] += 1
        if chunks_served["n"] > 100:
            return {"type": "http.disconnect"}
        return {"type": "http.request", "body": big_chunk, "more_body": True}

    req = Request(scope, _recv)
    asyncio.run(_body_service_ids(req))
    assert chunks_served["n"] <= 3, (
        f"helper read {chunks_served['n']} chunks of {chunk_size} bytes — "
        f"expected ≤ 3 once cumulative > {BODY_INSPECT_MAX_BYTES}"
    )


# ── is_request_remote: loopback peer + X-Remote-Analyst, sharing inactive ──


@pytest.mark.parametrize(
    ("client_peer", "extra_headers", "expected"),
    [
        # Loopback + X-Remote-Analyst: 1 but sharing inactive → still local.
        (("127.0.0.1", 12345), [(b"host", b"testserver"), (b"x-remote-analyst", b"1")], False),
        # Non-loopback peer is treated as remote regardless of sharing state.
        (("8.8.8.8", 12345), [], True),
        # No socket peer (request.client is None) fails closed as REMOTE.
        (None, [], True),
    ],
)
def test_is_request_remote_peer_classification(client_peer, extra_headers, expected):
    req = _make_req(method="GET", content_type=None, client=client_peer, extra_headers=extra_headers)
    assert is_request_remote(req) is expected


# ── _is_blocked_path: each layer ────────────────────────────────────────


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        # Prefix matches
        ("/api/admin/anything", True),
        ("/api/admin/share/foo", True),
        # Trailing-slash normalisation
        ("/api/download/", True),
        ("/api/download", True),
        ("/api/download///", True),
        # Subpath exact & prefix matches
        ("/api/sync-status", True),
        ("/api/sync-status/anything", True),
        # Sibling routes not swallowed
        ("/api/download-foo", False),
        ("/api/cron-schedule-x", False),
        # Scoring admin-only vs analyst-open suffixes
        ("/api/services/abc/scoring/config", True),
        ("/api/services/abc/scoring/status", True),
        ("/api/services/abc/scoring/labels", False),
        # Regex subpath matches
        ("/api/services/xyz/custom-fields", True),
        ("/api/services/xyz/custom-fields/items", True),
        # OpenAPI surface blocked for analysts (L7)
        ("/openapi.json", True),
        ("/docs", True),
        ("/redoc", True),
        ("/docs/oauth2-redirect", True),
        # Unrelated analyst-reachable paths
        ("/api/dashboard", False),
        ("/", False),
    ],
)
def test_is_blocked_path_layers(path, expected):
    assert _is_blocked_path(path) is expected


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "suffix",
    ["/enable", "/disable", "/versions", "/upgrade"],
)
def test_is_blocked_path_rum_suffix_admin_only(suffix):
    """RUM suffix gate: paths containing ``/rum/`` AND ending with an
    admin-only suffix are blocked. Covers the pre-existing /rum/enable,
    /rum/disable endpoints too — before this gate was added they had no
    ``/rum/`` entry anywhere in the analyst blocklists and were reachable
    by an authenticated analyst. /rum/status is deliberately NOT in this
    list (F1 audit fix) — see test_is_blocked_path_rum_reads_stay_open."""
    assert _is_blocked_path(f"/api/services/abc/rum{suffix}") is True


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "path",
    [
        "/api/services/abc/rum/status",
        "/api/services/abc/rum/beacon-health",
        "/api/services/abc/rum/analytics",
        "/api/services/abc/rum/live-events",
    ],
)
def test_is_blocked_path_rum_reads_stay_open(path):
    """Negative control: read-only RUM telemetry must NOT be shadowed by
    the suffix gate (mirrors the scoring-suffix negative control).
    /rum/status was blocked pre-fix (F1 audit finding), which made the
    analystVisible RUM page 403 → dead; the route itself now projects an
    analyst-safe body ({enabled, enabled_at}, no VCL fingerprint fields)."""
    assert _is_blocked_path(path) is False


# ── _is_sse_route ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/sse/foo", True),
        ("/foo/sse/bar", True),
        ("/api/cron-runs/stream", True),
        ("/api/dashboard", False),
    ],
)
def test_is_sse_route_standard_cases(path, expected):
    assert _is_sse_route(path) is expected


@pytest.mark.security_regression
def test_is_sse_route_detects_hyphenated_stream_routes():
    """The analyst SSE gate is documented as "default OFF; explicit allow
    only" (see the `_ANALYST_SSE_ALLOWLIST` docstring in remote_access.py).
    Fix `_is_sse_route` to recognize any "*-stream"-suffixed final path
    segment (not just an exact "/stream" boundary) so the allowlist gate
    is actually consulted for every SSE-shaped route."""
    assert _is_sse_route("/api/services/svc-A/realtime-stream") is True
    assert _is_sse_route("/api/services/svc-A/cost-governor-stream") is True


# ── L5: max date-range span cap ──────────────────────────────────────────────


def test_clamp_max_span_and_clamp_or_400_span_cap():
    end = datetime(2026, 6, 17, tzinfo=UTC)
    start = end - timedelta(days=400)

    s_cap, e_cap = TimeBounds().clamp(start, end, max_span=timedelta(days=366))
    assert e_cap == end and (e_cap - s_cap) == timedelta(days=366)

    s_uncap, e_uncap = TimeBounds().clamp(start, end)
    assert (e_uncap - s_uncap) == timedelta(days=400)

    # analyst → capped at MAX_ANALYST_QUERY_SPAN
    s_an, e_an = clamp_or_400(TimeBounds(), start.isoformat(), end.isoformat(), analyst_session=object())
    assert parse_iso_utc(e_an) - parse_iso_utc(s_an) == MAX_ANALYST_QUERY_SPAN
    # admin with explicit bounds → uncapped
    s_ad, e_ad = clamp_or_400(TimeBounds(), start.isoformat(), end.isoformat(), analyst_session=None)
    assert parse_iso_utc(e_ad) - parse_iso_utc(s_ad) == timedelta(days=400)


# ── L6: analyst API rate limiter (parametrized + separate bucket) ────────────


def test_limiter_req_limit_and_separate_bucket():
    lim = _StaticAssetLimiter(req_limit=2)
    assert lim.check("9.9.9.9", 0) is True
    assert lim.check("9.9.9.9", 0) is True
    assert lim.check("9.9.9.9", 0) is False  # 3rd exceeds req_limit=2
    assert lim.check("8.8.8.8", 0) is True  # a different IP is independent
    assert _static_limiter is not _analyst_api_limiter


# ── PII boundary: the IP-family set is one source of truth across the stack ───


@pytest.mark.security_regression
def test_ip_family_set_single_source_of_truth_across_stack():
    """The analyst filter-lock, the response masker, and the frontend
    drill-down hide list MUST all reference the same IP-family field set."""
    assert _PII_FORBIDDEN_FILTER_COLS == validation.IP_FAMILY_KEYS | validation.SESSION_ID_KEYS
    assert validation.IP_FAMILY_KEYS <= _PII_FORBIDDEN_FILTER_COLS
    assert validation.SESSION_ID_KEYS <= _PII_FORBIDDEN_FILTER_COLS

    repo_root = Path(__file__).resolve().parents[2]
    pii_ts = (repo_root / "frontend" / "lib" / "utils.ts").read_text(encoding="utf-8")
    m = re.search(r"IP_FAMILY_FIELDS\s*=\s*\[([^\]]*)\]", pii_ts)
    assert m is not None, "IP_FAMILY_FIELDS literal not found in frontend/lib/utils.ts"
    fe_fields = set(re.findall(r"""['"]([^'"]+)['"]""", m.group(1)))
    assert fe_fields == set(validation.IP_FAMILY_KEYS)


# ── insights clamp helpers: stable cache key + request-free resolver ──────────


def test_analyst_clamp_cache_key_and_resolve_insights_clamp():
    assert analyst_clamp_cache_key(None, None, 24) == "||24"
    assert (
        analyst_clamp_cache_key("2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", None)
        == "2026-01-01T00:00:00+00:00|2026-01-02T00:00:00+00:00|"
    )

    t0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=5)
    cs0, ce0, k0 = resolve_analyst_insights_clamp(None, None, 24, baseline_hours=168.0, window_hours=1.0, now=t0)
    cs1, ce1, k1 = resolve_analyst_insights_clamp(None, None, 24, baseline_hours=168.0, window_hours=1.0, now=t1)
    assert k0 == k1 == "||24"
    assert ce0 != ce1
    assert parse_iso_utc(ce0) == t0 and parse_iso_utc(ce1) == t1
    assert parse_iso_utc(cs0) == t0 - timedelta(hours=24)

    qs = (t0 + timedelta(hours=1)).isoformat()
    qe = (t0 - timedelta(hours=1)).isoformat()
    with pytest.raises(ValueError):
        resolve_analyst_insights_clamp(qs, qe, None, baseline_hours=168.0, window_hours=1.0, now=t0)


def test_time_bounds_from_params_matches_request_dep():
    """``get_analyst_time_bounds`` delegates to ``_time_bounds_from_params``."""
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    tb_helper = _time_bounds_from_params(None, None, 48, now=now)
    assert tb_helper.start == now - timedelta(hours=48)
    assert tb_helper.end == now

    session = SimpleNamespace(query_start_time=None, query_end_time=None, query_window_hours=48)
    req = SimpleNamespace(state=SimpleNamespace(analyst_session=session))
    tb_req = get_analyst_time_bounds(req)
    assert tb_req.start is not None and tb_req.end is not None


@pytest.mark.security_regression
def test_rolling_invite_ceilings_future_req_end_to_anchor():
    """A rolling-window invite must not let a caller-supplied *future* ``req_end``
    widen the effective window forward."""
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    tb = _time_bounds_from_params(None, None, 24, now=now)

    eff_start, eff_end = tb.clamp(now - timedelta(hours=24), now + timedelta(hours=1))
    assert eff_end == now
    assert eff_start == now - timedelta(hours=24)

    _, e2 = tb.clamp(now - timedelta(hours=24), now - timedelta(minutes=5))
    assert e2 == now - timedelta(minutes=5)

    tb_cap = _time_bounds_from_params(None, (now - timedelta(hours=2)).isoformat(), 24, now=now)
    _, e3 = tb_cap.clamp(now - timedelta(hours=24), now + timedelta(hours=1))
    assert e3 == now - timedelta(hours=2)

    tb_future = _time_bounds_from_params(None, (now + timedelta(days=365)).isoformat(), 24, now=now)
    _, e4 = tb_future.clamp(now - timedelta(hours=24), now + timedelta(hours=1))
    assert e4 == now
