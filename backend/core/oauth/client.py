"""OIDC relying-party handshake: discovery, JWKS, token exchange, id_token
validation — built on the Authlib ecosystem, never hand-rolled JOSE.

* **Token exchange** uses Authlib's httpx ``OAuth2Client`` (``client_secret_basic``
  + PKCE ``code_verifier``) — design §2.2.
* **id_token / signature / claims** use **joserfc** (Authlib's own, non-deprecated
  JOSE engine — ``authlib.jose`` is deprecated in 1.7 in favor of joserfc): the
  RS256-only algorithm allowlist, key selection from the JWKS by ``kid``, and
  ``iss``/``aud``/``exp``/``iat``/``nbf``/``nonce`` validation — design §2.3.

Every outbound call (discovery, JWKS, token) runs through a single injectable
httpx transport with a hard 3.0s connect/read/write ceiling, so a slow IdP can't
hang a worker and tests never touch the network (design §3.1). Discovery + JWKS
are cached in a thread-safe :class:`BoundedTTLCache` (24h) with a per-provider
single-flight lock; an unknown ``kid`` triggers exactly one JWKS refetch under
concurrency (design §3.2).

Failures raise :class:`OIDCError` carrying a granular ``reason`` (the audit
``details``); the router maps reasons to the coarse user-facing ``oauth_error``.
No token / code / verifier / secret is ever logged.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
from typing import Final
from urllib.parse import urlencode

import httpx
from authlib.integrations.base_client import OAuthError
from authlib.integrations.httpx_client import OAuth2Client
from joserfc import jwt
from joserfc.errors import (
    ExpiredTokenError,
    InvalidClaimError,
    JoseError,
    MissingClaimError,
)
from joserfc.jwk import KeySet

from backend.core.oauth.registry import OAuthProvider
from backend.utils.bounded_cache import BoundedTTLCache

logger = logging.getLogger(__name__)

_TIMEOUT_S: Final[float] = 3.0
_LEEWAY_S: Final[int] = 60
# Reject an id_token whose iat is older than the flow-state cookie TTL (10 min)
# plus one leeway window — a fresh login can't legitimately present a stale token.
_MAX_IAT_AGE_S: Final[int] = 600 + _LEEWAY_S
# RS256-only. NEVER widen to HS*/none — that reopens alg-confusion (design §2.3.1).
_ALLOWED_ALGS: Final[list[str]] = ["RS256"]

_metadata_cache: BoundedTTLCache = BoundedTTLCache(maxsize=10, ttl_seconds=86400)
_jwks_cache: BoundedTTLCache = BoundedTTLCache(maxsize=10, ttl_seconds=86400)
_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()

# Injectable transport so tests never hit the network (design §3.1/§7). None =
# real network (prod, and the e2e mock IdP served over loopback).
_test_transport: httpx.BaseTransport | None = None


class OIDCError(Exception):
    """A handshake failure. ``reason`` is the granular audit detail (e.g.
    ``bad_iss`` / ``expired`` / ``idp_unavailable``); the router maps it to the
    coarse user-facing ``oauth_error`` code and the audit event type."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def set_test_transport(transport: httpx.BaseTransport | None) -> None:
    """Route all outbound OIDC HTTP through ``transport`` (tests only)."""
    global _test_transport
    _test_transport = transport


def reset_caches_for_tests() -> None:
    _metadata_cache.clear()
    _jwks_cache.clear()


def _lock_for(key: str) -> threading.Lock:
    with _locks_guard:
        lock = _locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _locks[key] = lock
        return lock


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(_TIMEOUT_S), transport=_test_transport)


def _fetch_json(url: str) -> dict:
    try:
        with _http_client() as c:
            resp = c.get(url)
            resp.raise_for_status()
            return resp.json()
    except (httpx.HTTPError, ValueError) as e:
        # Timeout / 5xx / connection error / bad JSON — fail closed (§3.1).
        logger.warning("[oauth.client] outbound fetch failed for %s: %s", url, type(e).__name__)
        raise OIDCError("idp_unavailable") from e


# ── Discovery + JWKS (cached, single-flight) ─────────────────────────────────


def get_metadata(provider: OAuthProvider) -> dict:
    """Return the provider's cached ``.well-known/openid-configuration`` doc."""
    key = provider.id
    cached = _metadata_cache.get(key)
    if cached is not None:
        return cached
    with _lock_for(key):
        cached = _metadata_cache.get(key)
        if cached is not None:
            return cached
        meta = _fetch_json(provider.discovery_url)
        if not meta.get("issuer") or not meta.get("jwks_uri") or not meta.get("token_endpoint"):
            raise OIDCError("idp_unavailable")
        _metadata_cache[key] = meta
        return meta


def _load_keyset(provider: OAuthProvider, metadata: dict) -> KeySet:
    raw = _fetch_json(metadata["jwks_uri"])
    try:
        # joserfc accepts a parsed JWKS dict; its stub types the arg more narrowly.
        return KeySet.import_key_set(raw)  # type: ignore[arg-type]
    except Exception as e:
        raise OIDCError("idp_unavailable") from e


def get_jwks(provider: OAuthProvider, metadata: dict) -> KeySet:
    key = provider.id
    cached = _jwks_cache.get(key)
    if cached is not None:
        return cached
    with _lock_for(key):
        cached = _jwks_cache.get(key)
        if cached is not None:
            return cached
        keyset = _load_keyset(provider, metadata)
        _jwks_cache[key] = keyset
        return keyset


def _refresh_jwks(provider: OAuthProvider, metadata: dict, stale: KeySet) -> KeySet:
    """Refetch JWKS after an unknown ``kid`` — collapsing concurrent refetches
    into ONE (design §3.2 / test §7.9). Under the per-provider lock, if another
    thread already replaced the stale keyset we return its result without a
    second network call."""
    key = provider.id
    with _lock_for(key):
        current = _jwks_cache.get(key)
        if current is not None and current is not stale:
            return current
        keyset = _load_keyset(provider, metadata)
        _jwks_cache[key] = keyset
        return keyset


# ── Authorize URL + PKCE ─────────────────────────────────────────────────────


def _b64url_nopad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def make_pkce_verifier() -> str:
    import secrets

    return secrets.token_urlsafe(64)


def pkce_challenge(verifier: str) -> str:
    import hashlib

    return _b64url_nopad(hashlib.sha256(verifier.encode("ascii")).digest())


def build_authorize_url(
    provider: OAuthProvider,
    metadata: dict,
    *,
    redirect_uri: str,
    state: str,
    nonce: str,
    code_challenge: str,
) -> str:
    """Build the IdP authorization URL with the explicit §2.2 parameter set.

    ``prompt=select_account`` forces Google's account chooser — the primary
    defense against the "wrong Google account auto-authenticates" trap (§5.2).
    ``access_type=online`` — no refresh token (one-shot login → local session).
    """
    endpoint = metadata.get("authorization_endpoint")
    if not endpoint:
        raise OIDCError("idp_unavailable")
    params = {
        "response_type": "code",
        "client_id": provider.client_id,
        "redirect_uri": redirect_uri,
        "scope": provider.scopes,
        "state": state,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "access_type": "online",
        "prompt": "select_account",
    }
    sep = "&" if "?" in endpoint else "?"
    return f"{endpoint}{sep}{urlencode(params)}"


# ── Token exchange ───────────────────────────────────────────────────────────


def exchange_code(
    provider: OAuthProvider,
    metadata: dict,
    *,
    code: str,
    code_verifier: str,
    redirect_uri: str,
) -> dict:
    """Exchange the authorization ``code`` for tokens (Authlib, client_secret_basic
    + PKCE). Returns the token dict (must contain ``id_token``)."""
    token_endpoint = metadata.get("token_endpoint")
    if not token_endpoint:
        raise OIDCError("idp_unavailable")
    try:
        with OAuth2Client(
            client_id=provider.client_id,
            client_secret=provider.client_secret,
            token_endpoint_auth_method="client_secret_basic",
            timeout=httpx.Timeout(_TIMEOUT_S),
            transport=_test_transport,
        ) as oc:
            token = oc.fetch_token(
                token_endpoint,
                grant_type="authorization_code",
                code=code,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
            )
    except OAuthError as e:
        # IdP rejected the code / PKCE verifier (invalid_grant etc.).
        logger.warning("[oauth.client] token exchange rejected: %s", getattr(e, "error", "oauth_error"))
        raise OIDCError("pkce_failed") from e
    except httpx.HTTPError as e:
        logger.warning("[oauth.client] token exchange transport error: %s", type(e).__name__)
        raise OIDCError("idp_unavailable") from e
    if not token or not token.get("id_token"):
        raise OIDCError("idp_unavailable")
    return dict(token)


# ── id_token validation ──────────────────────────────────────────────────────


def _unverified_kid(id_token: str) -> str | None:
    """Read the JWT header ``kid`` WITHOUT trusting it — only to route to the
    right JWKS key (the signature is still verified against that key)."""
    try:
        header_seg = id_token.split(".", 1)[0]
        pad = "=" * (-len(header_seg) % 4)
        header = json.loads(base64.urlsafe_b64decode(header_seg + pad))
        kid = header.get("kid")
        return str(kid) if kid else None
    except Exception:
        return None


def _keyset_has_kid(keyset: KeySet, kid: str) -> bool:
    try:
        keyset.get_by_kid(kid)
        return True
    except Exception:
        return False


def _is_true(value) -> bool:
    """email_verified may arrive as a bool or the string 'true' (§2.4)."""
    return value is True or (isinstance(value, str) and value.strip().lower() == "true")


def verify_id_token(
    provider: OAuthProvider,
    metadata: dict,
    id_token: str,
    *,
    nonce: str,
) -> dict:
    """Verify signature + claims and return the id_token claims dict.

    Enforces (all fail closed): RS256-only allowlist; signature against the JWKS
    key for the token's ``kid`` (never the header alg); ``iss`` in the provider's
    accepted set (Google: both issuer forms via ``extra_issuers``); ``aud`` ==
    client_id (+ ``azp`` if present); ``exp``/``iat``/``nbf`` with 60s leeway;
    ``iat`` not older than the cookie TTL; ``nonce`` present AND == the flow-state
    nonce; ``email_verified`` == true and ``email`` present; optional ``hd`` ==
    ``allowed_hd``.
    """
    keyset = get_jwks(provider, metadata)
    kid = _unverified_kid(id_token)
    # Unknown kid (rotated signing key): refetch JWKS exactly once (§3.2).
    if kid and not _keyset_has_kid(keyset, kid):
        keyset = _refresh_jwks(provider, metadata, keyset)
        if not _keyset_has_kid(keyset, kid):
            raise OIDCError("alg_rejected")

    try:
        decoded = jwt.decode(id_token, keyset, algorithms=_ALLOWED_ALGS)
    except JoseError as e:
        # Bad signature, unsupported/none alg, HS→RS confusion, decode error.
        logger.warning("[oauth.client] id_token decode rejected: %s", type(e).__name__)
        raise OIDCError("alg_rejected") from e

    claims = dict(decoded.claims)
    now = int(time.time())
    accepted_iss = [metadata.get("issuer"), *provider.extra_issuers]
    accepted_iss = [i for i in accepted_iss if i]
    registry_opts = {
        "iss": {"essential": True, "values": accepted_iss},
        "aud": {"essential": True, "value": provider.client_id},
        "exp": {"essential": True},
        "iat": {"essential": True},
        "nonce": {"essential": True, "value": nonce},
    }
    try:
        # registry_opts values are joserfc ClaimsOption dicts; its stub can't
        # infer that through **-unpacking.
        jwt.JWTClaimsRegistry(now=now, leeway=_LEEWAY_S, **registry_opts).validate(claims)  # type: ignore[arg-type]
    except ExpiredTokenError as e:
        raise OIDCError("expired") from e
    except (InvalidClaimError, MissingClaimError) as e:
        cn = getattr(e, "claim", "") or ""
        reason = {
            "iss": "bad_iss",
            "aud": "bad_aud",
            "nonce": "nonce_mismatch",
            "exp": "expired",
            "iat": "expired",
            "nbf": "expired",
        }.get(cn, "bad_claim")
        raise OIDCError(reason) from e
    except JoseError as e:
        raise OIDCError("bad_claim") from e

    # azp, when present, must equal the client_id (OIDC Core 3.1.3.7).
    azp = claims.get("azp")
    if azp is not None and azp != provider.client_id:
        raise OIDCError("bad_aud")

    # Reject a token minted before this login could have started.
    try:
        iat = int(claims.get("iat", 0))
    except (TypeError, ValueError):
        iat = 0
    if now - iat > _MAX_IAT_AGE_S:
        raise OIDCError("expired")

    if not _is_true(claims.get("email_verified")) or not claims.get("email"):
        raise OIDCError("unverified_email")

    if provider.allowed_hd and claims.get("hd") != provider.allowed_hd:
        raise OIDCError("wrong_domain")

    return claims
