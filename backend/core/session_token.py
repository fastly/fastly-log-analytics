"""Opaque AES-GCM session-reference token for the Sessions detail lookup.

Seals a session's identity tuple ``(ip, ja4, session_start, session_end)`` into
an opaque, authenticated token attached to every ``/api/sessions`` row. The
detail endpoint unseals it server-side to run the existing exact-match lookup,
so a PII-masking analyst — who only ever sees the *masked* ``ip`` on the wire —
can still drill into a session WITHOUT the real IP ever leaving the server and
without the masked string (``1.2.3.xxx``) being round-tripped as a lookup key
that can never match a real stored IP.

Why encryption (AEAD) and not a signed/hashed handle:

* The tuple carries the real client IP, which must not reach a masking analyst —
  so the token must be *confidential*, not merely authenticated.
* A plain hash of the IP is brute-forceable across the 2^32 IPv4 space (ja4 and
  the time window are visible to the analyst, so they add no entropy against an
  adversary who already has them). AES-256-GCM under a server-held key is opaque
  and unforgeable.

Key resolution: a process-lifetime ephemeral 32-byte key by default — safe
because prod runs a single uvicorn worker (no ``--workers``), so the process
that seals a token is the one that opens it. Set ``SESSION_TOKEN_SECRET`` to
derive a stable key (SHA-256 → 32 bytes) when restart-stable tokens are wanted.
A token that fails to open (tamper, wrong service, or a key rotated away by a
restart under the ephemeral default) raises :class:`SessionTokenError`; the
caller surfaces a "reload the page" 400 rather than a silent empty result.

Wire format (after AES-GCM and Base64URL)::

    base64url( nonce(12 B) || AES-GCM(json([ip, ja4, start, end]), aad) || tag )

    aad = service_id (UTF-8) — binds a token to one customer service, so a token
          minted for service A cannot be replayed against service B.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import threading
from typing import Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_BYTES: Final[int] = 12
KEY_BYTES: Final[int] = 32  # AES-256
GCM_TAG_BYTES: Final[int] = 16
_ENV_SECRET: Final[str] = "SESSION_TOKEN_SECRET"


class SessionTokenError(Exception):
    """Raised when a session token is malformed, tampered with, minted for a
    different service, or sealed under a key this process no longer holds (e.g.
    after a restart with the ephemeral default). Callers treat all of these the
    same: the reference is stale — ask the client to reload the session list."""


_key: bytes | None = None
_key_lock = threading.Lock()


def _resolve_key() -> bytes:
    """Lazily resolve (and cache) the 32-byte AES key.

    Cached for the process lifetime so the ephemeral default is stable within a
    worker. Tests that need to flip keys reset the module-level ``_key`` to
    ``None`` after changing ``SESSION_TOKEN_SECRET``.

    The double-checked lock matters only for the ephemeral default: without it,
    two concurrent first-ever requests could each mint a *different* random key
    and one would overwrite the other, so a token sealed under the loser's key
    could never be opened (a spurious ``SessionTokenError`` until the client
    reloads). Under a set ``SESSION_TOKEN_SECRET`` both threads derive the same
    key, so the race is harmless — but the lock is free and keeps the test
    rekey path deterministic.
    """
    global _key
    if _key is None:
        with _key_lock:
            if _key is None:
                env = os.getenv(_ENV_SECRET, "").strip()
                _key = hashlib.sha256(env.encode("utf-8")).digest() if env else secrets.token_bytes(KEY_BYTES)
    return _key


def _aead() -> AESGCM:
    return AESGCM(_resolve_key())


def _b64url_encode(data: bytes) -> str:
    """Base64URL without padding — URL/JSON-safe, RFC 4648 §5."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def seal_session_token(
    ip: str,
    ja4: str | None,
    session_start: str,
    session_end: str,
    *,
    service_id: str,
) -> str:
    """Seal a session identity tuple into an opaque token bound to ``service_id``."""
    payload = json.dumps([ip, ja4, session_start, session_end], separators=(",", ":")).encode("utf-8")
    nonce = os.urandom(NONCE_BYTES)
    ciphertext = _aead().encrypt(nonce, payload, service_id.encode("utf-8"))
    return _b64url_encode(nonce + ciphertext)


def open_session_token(token: str, *, service_id: str) -> tuple[str, str | None, str, str]:
    """Inverse of :func:`seal_session_token`. Returns ``(ip, ja4, start, end)``.

    Raises :class:`SessionTokenError` on any failure (bad base64, truncated
    blob, AEAD tag mismatch from tampering, wrong service AAD, or a key this
    process no longer holds).
    """
    try:
        raw = _b64url_decode(token)
    except Exception as e:  # malformed base64
        raise SessionTokenError(f"base64url decode failed: {e}") from e
    if len(raw) < NONCE_BYTES + GCM_TAG_BYTES:
        raise SessionTokenError(f"token too short: {len(raw)} bytes")
    nonce, ciphertext = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
    try:
        plaintext = _aead().decrypt(nonce, ciphertext, service_id.encode("utf-8"))
    except InvalidTag as e:
        raise SessionTokenError("AEAD verification failed") from e
    try:
        ip, ja4, start, end = json.loads(plaintext)
    except (ValueError, TypeError) as e:
        raise SessionTokenError(f"payload decode failed: {e}") from e
    return ip, ja4, start, end
