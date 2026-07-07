"""OIDC client: id_token validation, discovery/JWKS caching, single-flight
refetch, token exchange (design §2.2 / §2.3 / §3.1 / §3.2)."""

from __future__ import annotations

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


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    registry.reset_cache_for_tests()
    client.reset_caches_for_tests()
    yield
    client.set_test_transport(None)
    registry.reset_cache_for_tests()
    client.reset_caches_for_tests()


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
