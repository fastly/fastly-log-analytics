"""Unit tests for the opaque AES-GCM session-reference token codec.

Pins the seal/open round-trip and — critically — that a tampered, wrong-service,
or wrong-key token is rejected (raises ``SessionTokenError``) rather than
silently decoding to attacker-chosen values. The detail endpoint relies on this:
an opaque token that an analyst can't forge or invert is what lets a PII-masking
analyst drill into a session without ever holding the real IP.
"""

from __future__ import annotations

import pytest

from backend.core import session_token as st
from backend.core.session_token import SessionTokenError, open_session_token, seal_session_token


@pytest.fixture
def fixed_key(monkeypatch):
    """Pin a deterministic key (SESSION_TOKEN_SECRET) and reset the module
    cache so the test controls key material. Resets again on teardown so the
    cached ephemeral key doesn't leak across tests."""
    monkeypatch.setenv("SESSION_TOKEN_SECRET", "unit-test-secret-A")
    st._key = None
    yield
    st._key = None


def _rekey(monkeypatch, secret: str) -> None:
    monkeypatch.setenv("SESSION_TOKEN_SECRET", secret)
    st._key = None


def test_round_trip(fixed_key):
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    ip, ja4, start, end = open_session_token(token, service_id="svcA")
    assert ip == "203.0.113.7"
    assert ja4 == "ja4abc"
    assert start == "2026-06-29T00:00:00+00:00"
    assert end == "2026-06-29T00:30:00+00:00"


def test_round_trip_null_ja4(fixed_key):
    """ja4 is often absent — it must round-trip as None, not the string 'None'
    or '' (the detail SQL treats None as 'don't filter on ja4')."""
    token = seal_session_token(
        "203.0.113.7", None, "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    ip, ja4, _start, _end = open_session_token(token, service_id="svcA")
    assert ip == "203.0.113.7"
    assert ja4 is None


def test_token_is_opaque_not_derivable_from_ip(fixed_key):
    """The token must not be a plain function of the IP — it must not contain
    the IP in cleartext (else masking is trivially reversible)."""
    ip = "198.51.100.23"
    token = seal_session_token(
        ip, "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    assert ip not in token
    # last octet (the masked part) must not be recoverable from the token text
    assert ".23" not in token


def test_tampered_token_rejected(fixed_key):
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    # Flip a character in the ciphertext body (last char is inside the GCM tag).
    tampered = token[:-1] + ("A" if token[-1] != "A" else "B")
    with pytest.raises(SessionTokenError):
        open_session_token(tampered, service_id="svcA")


def test_wrong_service_aad_rejected(fixed_key):
    """A token minted for svcA must not open under svcB (cross-service replay)."""
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    with pytest.raises(SessionTokenError):
        open_session_token(token, service_id="svcB")


def test_wrong_key_rejected(fixed_key, monkeypatch):
    """A token sealed under one key (e.g. before a restart) fails to open under
    a different key — the caller surfaces a 'reload the page' 400."""
    token = seal_session_token(
        "203.0.113.7", "ja4abc", "2026-06-29T00:00:00+00:00", "2026-06-29T00:30:00+00:00", service_id="svcA"
    )
    _rekey(monkeypatch, "unit-test-secret-B")
    with pytest.raises(SessionTokenError):
        open_session_token(token, service_id="svcA")


def test_malformed_token_rejected(fixed_key):
    with pytest.raises(SessionTokenError):
        open_session_token("not-a-valid-token!!!", service_id="svcA")
    with pytest.raises(SessionTokenError):
        open_session_token("", service_id="svcA")
