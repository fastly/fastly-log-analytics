"""PKCE / state / nonce generation pins (RFC 7636) for the OAuth authorize flow."""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core.oauth import client as oidc
from backend.core.oauth import flow_state, registry
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware
from tests.remote_access import oauth_helpers as H

# RFC 7636 Appendix B
RFC_VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
RFC_CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
UNRESERVED = re.compile(r"^[A-Za-z0-9\-._~]+$")


@pytest.fixture(autouse=True)
def _reset():
    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()
    yield
    oidc.set_test_transport(None)
    registry.reset_cache_for_tests()
    oidc.reset_caches_for_tests()


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
