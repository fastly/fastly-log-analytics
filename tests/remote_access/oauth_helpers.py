"""Deterministic OIDC test harness — no network, committed keypair.

The mock IdP serves canned discovery + the static JWKS from
``tests/fixtures/oauth/`` and mints id_tokens signed with the committed test
private key, so joserfc runs its REAL verification path against the served key
set (design §7). Every outbound OIDC call is routed through an injected
``httpx.MockTransport`` via ``client.set_test_transport`` — nothing touches the
network.
"""

from __future__ import annotations

import json
import pathlib
import time

import httpx
from joserfc import jwt
from joserfc.jwk import RSAKey

_FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "oauth"

ISSUER = "https://idp.example"
DISCOVERY_URL = "https://idp.example/.well-known/openid-configuration"
AUTHORIZATION_ENDPOINT = "https://idp.example/authorize"
TOKEN_ENDPOINT = "https://idp.example/token"
JWKS_URI = "https://idp.example/jwks"
CLIENT_ID = "test-client-id"
CLIENT_SECRET = "test-client-secret"
PUBLIC_ENDPOINT = "https://testserver"

REMOTE_HEADERS = {"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"}


def load_private_key() -> RSAKey:
    return RSAKey.import_key(json.loads((_FIXTURES / "private_key.json").read_text()))


def load_jwks() -> dict:
    return json.loads((_FIXTURES / "jwks.json").read_text())


def discovery_doc(**overrides) -> dict:
    doc = {
        "issuer": ISSUER,
        "authorization_endpoint": AUTHORIZATION_ENDPOINT,
        "token_endpoint": TOKEN_ENDPOINT,
        "jwks_uri": JWKS_URI,
        "id_token_signing_alg_values_supported": ["RS256"],
    }
    doc.update(overrides)
    return doc


def mint_id_token(
    *,
    nonce: str,
    iss: str = ISSUER,
    aud: str = CLIENT_ID,
    sub: str = "google-sub-1",
    email: str = "analyst@corp.com",
    email_verified=True,
    iat_delta: int = 0,
    exp_delta: int = 300,
    nbf_delta: int | None = None,
    kid: str = "test-key-1",
    alg: str = "RS256",
    key: RSAKey | None = None,
    extra: dict | None = None,
    omit: tuple[str, ...] = (),
) -> str:
    """Mint a signed id_token relative to real ``time.time()`` (offsets ≫ test
    runtime, so validation is deterministic)."""
    now = int(time.time())
    payload: dict = {
        "iss": iss,
        "aud": aud,
        "sub": sub,
        "email": email,
        "email_verified": email_verified,
        "nonce": nonce,
        "iat": now + iat_delta,
        "exp": now + exp_delta,
    }
    if nbf_delta is not None:
        payload["nbf"] = now + nbf_delta
    if extra:
        payload.update(extra)
    for k in omit:
        payload.pop(k, None)
    header = {"alg": alg, "kid": kid}
    return jwt.encode(header, payload, key or load_private_key())


def token_response(id_token: str, **extra) -> dict:
    body = {"access_token": "mock-access-token", "token_type": "Bearer", "id_token": id_token}
    body.update(extra)
    return body


def make_transport(
    *,
    on_token=None,
    token_body: dict | None = None,
    token_status: int = 200,
    jwks: dict | None = None,
    discovery_overrides: dict | None = None,
    discovery_boom: bool = False,
    jwks_calls: list | None = None,
    discovery_calls: list | None = None,
    jwks_provider=None,
) -> httpx.MockTransport:
    """Build a MockTransport routing discovery / jwks / token.

    ``jwks_calls`` / ``discovery_calls`` (if given) record each fetch so tests
    can assert the single-flight refetch + cache-hit counts. ``jwks_provider``
    (callable → dict) overrides the served JWKS per-call (key-rotation tests);
    ``on_token`` overrides the token handler.
    """
    _jwks = jwks if jwks is not None else load_jwks()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/.well-known/openid-configuration"):
            if discovery_calls is not None:
                discovery_calls.append(1)
            if discovery_boom:
                raise httpx.ConnectTimeout("idp down")
            return httpx.Response(200, json=discovery_doc(**(discovery_overrides or {})))
        if path.endswith("/jwks"):
            if jwks_calls is not None:
                jwks_calls.append(1)
            body = jwks_provider() if jwks_provider is not None else _jwks
            return httpx.Response(200, json=body)
        if path.endswith("/token"):
            if on_token is not None:
                return on_token(request)
            return httpx.Response(token_status, json=token_body or {})
        return httpx.Response(404, json={"error": "not_found", "path": path})

    return httpx.MockTransport(handler)


def configure_registry(monkeypatch, tmp_path, *, enabled: bool = True, allowed_hd: str | None = None, extra=None):
    """Write a one-provider registry file + env creds and turn the feature on."""
    from backend.core.oauth import registry

    entry = {
        "display_name": "Google Workspace",
        "discovery_url": DISCOVERY_URL,
        "scopes": "openid email",
        "enabled": enabled,
    }
    if allowed_hd:
        entry["allowed_hd"] = allowed_hd
    if extra:
        entry.update(extra)
    path = tmp_path / "oauth_providers.json"
    path.write_text(json.dumps({"google": entry}))
    monkeypatch.setenv("OAUTH_PROVIDERS_CONFIG_PATH", str(path))
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "test-flow-state-secret-0123456789")
    monkeypatch.setenv("OAUTH_GOOGLE_CLIENT_ID", CLIENT_ID)
    monkeypatch.setenv("OAUTH_GOOGLE_CLIENT_SECRET", CLIENT_SECRET)
    registry.reset_cache_for_tests()
