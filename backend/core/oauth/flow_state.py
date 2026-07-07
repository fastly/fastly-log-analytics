"""AES-256-GCM sealed ``oauth_flow_state`` cookie (design §2.1).

Binds ``/callback`` to the ``/authorize`` that started it: the sealed payload
carries the ``provider`` registry key, the CSRF ``state``, the OIDC ``nonce``,
the PKCE ``code_verifier``, the exact ``redirect_uri`` used in the authorize
request, and the post-login ``return`` target. Sealed (not merely signed) so
the browser — and any attacker — can neither read the PKCE verifier/nonce nor
forge the cookie.

This parallels ``backend/core/session_token.py``'s AEAD codec but with TWO
deliberate differences:

1. **A dedicated, REQUIRED key** derived from ``OAUTH_FLOW_STATE_SECRET`` — with
   **no** process-lifetime ephemeral fallback. ``session_token``'s ephemeral
   default is safe there (a token is sealed and opened within a single request),
   but an ``oauth_flow_state`` cookie must survive the seconds-to-minutes the
   analyst spends at the IdP, INCLUDING across a uvicorn restart. An ephemeral
   key would make every in-flight login undecryptable after a restart — a real
   ~10-minute login-outage window. If the secret is unset the codec raises, and
   the feature is off anyway (see ``registry.feature_on``).
2. **A distinct AAD label** (``b"oauth_flow_state"``) so the two codecs' keys
   and ciphertexts can never be cross-purposed.

Wire format (after AES-GCM + Base64URL)::

    base64url( nonce(12 B) || AES-GCM(json(payload), aad=b"oauth_flow_state") || tag )
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from backend.core.oauth import registry

NONCE_BYTES: Final[int] = 12
GCM_TAG_BYTES: Final[int] = 16
_AAD: Final[bytes] = b"oauth_flow_state"

# Cookie contract (design §2.1.4): SameSite=Lax is REQUIRED so the browser sends
# the cookie on the top-level redirect back from the IdP (contrast the
# analyst_session_id cookie, which is SameSite=Strict).
COOKIE_NAME: Final[str] = "oauth_flow_state"
COOKIE_PATH: Final[str] = "/api/share/oauth"
COOKIE_MAX_AGE_S: Final[int] = 600  # 10 minutes


class FlowStateError(Exception):
    """Raised when the flow-state cookie is missing, malformed, tampered with,
    or sealed under a key this process no longer holds. Callers treat every
    variant the same: abort the handshake and redirect with an oauth_error."""


def _key() -> bytes:
    secret = registry.flow_state_secret()
    if not secret:
        # Fail closed — never fall back to an ephemeral key (see module docstring).
        raise FlowStateError("OAUTH_FLOW_STATE_SECRET is not set")
    return hashlib.sha256(secret.encode("utf-8")).digest()


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def seal_flow_state(payload: dict) -> str:
    """Seal the flow-state dict into the opaque cookie value."""
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    import os

    nonce = os.urandom(NONCE_BYTES)
    ct = AESGCM(_key()).encrypt(nonce, raw, _AAD)
    return _b64url_encode(nonce + ct)


def open_flow_state(token: str) -> dict:
    """Inverse of :func:`seal_flow_state`. Raises :class:`FlowStateError` on any
    failure (missing/empty, bad base64, truncated, AEAD tag mismatch, or a key
    this process no longer holds)."""
    if not token:
        raise FlowStateError("missing flow-state cookie")
    try:
        raw = _b64url_decode(token)
    except Exception as e:  # malformed base64
        raise FlowStateError(f"base64url decode failed: {e}") from e
    if len(raw) < NONCE_BYTES + GCM_TAG_BYTES:
        raise FlowStateError(f"token too short: {len(raw)} bytes")
    nonce, ct = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
    try:
        pt = AESGCM(_key()).decrypt(nonce, ct, _AAD)
    except InvalidTag as e:
        raise FlowStateError("AEAD verification failed") from e
    try:
        data = json.loads(pt)
    except (ValueError, TypeError) as e:
        raise FlowStateError(f"payload decode failed: {e}") from e
    if not isinstance(data, dict):
        raise FlowStateError("payload is not an object")
    return data
