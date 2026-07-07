"""AES-256-GCM oauth_flow_state cookie codec (design §2.1)."""

from __future__ import annotations

import pytest

from backend.core.oauth import flow_state
from backend.core.oauth.flow_state import FlowStateError

_PAYLOAD = {
    "provider": "google",
    "state": "state-token",
    "nonce": "nonce-token",
    "verifier": "pkce-verifier",
    "redirect_uri": "https://testserver/api/share/oauth/callback",
    "return": "/dashboard",
}


def test_seal_open_round_trip(monkeypatch):
    monkeypatch.setenv("OAUTH_FLOW_STATE_SECRET", "secret-abcdef-0123456789")
    sealed = flow_state.seal_flow_state(_PAYLOAD)
    assert flow_state.open_flow_state(sealed) == _PAYLOAD
    # Opaque — the verifier/nonce are not readable in the cookie value.
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
