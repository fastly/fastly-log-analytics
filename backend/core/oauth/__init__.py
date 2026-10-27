"""Analyst OAuth / OIDC core: provider registry, flow-state cookie codec, and OIDC client."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import stat
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import cachetools
import httpx
from authlib.common.security import generate_token
from authlib.integrations.base_client.errors import OAuthError
from authlib.integrations.httpx_client import OAuth2Client
from authlib.oauth2.rfc7636 import create_s256_code_challenge
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from joserfc import jwt
from joserfc.errors import ExpiredTokenError, InvalidClaimError, JoseError, MissingClaimError
from joserfc.jwk import KeySet

from backend.config import SYSTEM_DATA_DIR

logger = logging.getLogger(__name__)
# ── Provider Registry ────────────────────────────────────────────────────────
_SECRET_ENV = "OAUTH_FLOW_STATE_SECRET"
_CONFIG_PATH_ENV = "OAUTH_PROVIDERS_CONFIG_PATH"
_PASSCODE_ENABLED_ENV = "SHARE_PASSCODE_LOGIN_ENABLED"
_DEFAULT_SCOPES = "openid email profile"


@dataclass(frozen=True)
class OAuthProvider:
    id: str
    display_name: str
    discovery_url: str
    client_id: str
    client_secret: str
    scopes: str = _DEFAULT_SCOPES
    enabled: bool = True
    allowed_hd: str | None = None
    extra_issuers: tuple[str, ...] = ()
    auto_provision: bool = False
    auto_provision_service_ids: tuple[str, ...] = ()
    auto_provision_mask_ips: bool = True

    def public_dict(self) -> dict[str, str]:
        """Minimal shape safe for the unauthenticated login page."""
        return {"id": self.id, "display_name": self.display_name}

    def admin_dict(self) -> dict[str, object]:
        """Shape for ``GET /api/admin/share/oauth-providers`` (still no secrets)."""
        return {"id": self.id, "display_name": self.display_name, "enabled": self.enabled}


_registry_lock = threading.Lock()
_cache: tuple[str, float, list[OAuthProvider]] | None = None


def _env_suffix(key: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in key).upper()


def _config_path() -> Path:
    override = os.environ.get(_CONFIG_PATH_ENV, "").strip()
    return Path(override) if override else SYSTEM_DATA_DIR / "oauth_providers.json"


def feature_on() -> bool:
    """Master switch: OAuth is off unless the flow-state secret is configured."""
    return bool(os.environ.get(_SECRET_ENV, "").strip())


def _load_from_disk(path: Path) -> list[OAuthProvider]:
    path_str = str(path)
    try:
        st = os.stat(path_str)
    except FileNotFoundError:
        return []
    except OSError:
        logger.exception("[oauth.registry] stat failed for %s — OAuth disabled", path_str)
        return []
    if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        logger.warning(
            "[oauth.registry] %s has group/other bits set (mode %s); expected 0600", path_str, oct(st.st_mode & 0o777)
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("top-level JSON value must be an object keyed by provider id")
    except Exception:
        logger.exception("[oauth.registry] failed to parse provider registry at %s — OAuth disabled", path_str)
        return []
    providers: list[OAuthProvider] = []
    for key, entry in raw.items():
        if not isinstance(key, str) or not key.strip() or not isinstance(entry, dict):
            logger.warning("[oauth.registry] skipping malformed entry %r in %s", key, path_str)
            continue
        pid = key.strip()
        suffix = _env_suffix(pid)
        client_id = os.environ.get(f"OAUTH_{suffix}_CLIENT_ID", "").strip()
        client_secret = os.environ.get(f"OAUTH_{suffix}_CLIENT_SECRET", "").strip()
        if not (client_id and client_secret):
            continue
        discovery_url = str(entry.get("discovery_url") or "").strip()
        display_name = str(entry.get("display_name") or pid).strip()
        if not discovery_url:
            logger.warning("[oauth.registry] provider %r missing discovery_url — skipped", pid)
            continue
        scopes = str(entry.get("scopes") or _DEFAULT_SCOPES).strip() or _DEFAULT_SCOPES
        allowed_hd = str(entry["allowed_hd"]).strip() if entry.get("allowed_hd") else None
        raw_extra = entry.get("extra_issuers") or ()
        extra_issuers = (
            tuple(str(x).strip() for x in raw_extra if str(x).strip()) if isinstance(raw_extra, (list, tuple)) else ()
        )
        raw_svcs = entry.get("auto_provision_service_ids") or ()
        auto_svcs = (
            tuple(str(x).strip() for x in raw_svcs if str(x).strip()) if isinstance(raw_svcs, (list, tuple)) else ()
        )
        providers.append(
            OAuthProvider(
                id=pid,
                display_name=display_name,
                discovery_url=discovery_url,
                client_id=client_id,
                client_secret=client_secret,
                scopes=scopes,
                enabled=bool(entry.get("enabled", True)),
                allowed_hd=allowed_hd or None,
                extra_issuers=extra_issuers,
                auto_provision=bool(entry.get("auto_provision", False)),
                auto_provision_service_ids=auto_svcs,
                auto_provision_mask_ips=bool(entry.get("auto_provision_mask_ips", True)),
            )
        )
    return providers


def _providers() -> list[OAuthProvider]:
    global _cache
    if not feature_on():
        return []
    path = _config_path()
    path_str = str(path)
    try:
        mtime = os.stat(path_str).st_mtime
    except OSError:
        mtime = -1.0
    with _registry_lock:
        if _cache is not None and _cache[0] == path_str and _cache[1] == mtime:
            return list(_cache[2])
        loaded = _load_from_disk(path) if mtime >= 0 else []
        _cache = (path_str, mtime, loaded)
        return list(loaded)


def get_all_providers() -> list[OAuthProvider]:
    """All configured providers (including ``enabled=False``) — admin surface."""
    return _providers()


def get_enabled_providers() -> list[OAuthProvider]:
    """Enabled, fully-configured providers — login page + ``/authorize``."""
    return [p for p in _providers() if p.enabled]


def get_provider(provider_id: str) -> OAuthProvider | None:
    """Look up a configured provider by id (regardless of ``enabled``)."""
    if not provider_id:
        return None
    for p in _providers():
        if p.id == provider_id:
            return p
    return None


def passcode_login_enabled() -> bool:
    """Whether the passcode login path is enabled (default ``True``)."""
    val = os.environ.get(_PASSCODE_ENABLED_ENV, "true").strip().lower()
    return val not in ("0", "false", "no", "off")


def reset_cache_for_tests() -> None:
    """Clear the mtime-memoized provider cache."""
    global _cache
    with _registry_lock:
        _cache = None


# ── Flow-State Cookie Codec (AES-256-GCM) ────────────────────────────────────
_AAD = b"oauth_flow_state_v1"
COOKIE_NAME = "oauth_flow_state"
COOKIE_PATH = "/api/share/oauth"
COOKIE_MAX_AGE_S = 300


class FlowStateError(ValueError):
    """Raised when the ``oauth_flow_state`` cookie is missing, malformed, or fails AEAD authentication."""


def _key() -> bytes:
    raw = os.environ.get(_SECRET_ENV, "").strip()
    if not raw:
        raise FlowStateError("OAUTH_FLOW_STATE_SECRET is not configured")
    return hashlib.sha256(f"{raw}|{_AAD.decode()}".encode()).digest()


def seal_flow_state(payload: dict[str, Any]) -> str:
    """Encrypt + authenticate ``payload`` into a URL-safe cookie value."""
    key = _key()
    nonce = os.urandom(12)
    pt = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ct = AESGCM(key).encrypt(nonce, pt, _AAD)
    return base64.urlsafe_b64encode(nonce + ct).rstrip(b"=").decode("ascii")


def open_flow_state(cookie_value: str) -> dict[str, Any]:
    """Authenticate + decrypt a cookie value sealed by :func:`seal_flow_state`."""
    if not cookie_value or not isinstance(cookie_value, str):
        raise FlowStateError("empty flow-state cookie")
    key = _key()
    pad = "=" * (-len(cookie_value) % 4)
    try:
        raw = base64.urlsafe_b64decode((cookie_value + pad).encode("ascii"))
    except Exception as exc:
        raise FlowStateError("malformed base64 in flow-state cookie") from exc
    if len(raw) < 12 + 16 + 2:
        raise FlowStateError("truncated flow-state cookie")
    nonce, ct = raw[:12], raw[12:]
    try:
        pt = AESGCM(key).decrypt(nonce, ct, _AAD)
        obj = json.loads(pt.decode("utf-8"))
    except (InvalidTag, ValueError, UnicodeDecodeError) as exc:
        raise FlowStateError("flow-state authentication failed") from exc
    if not isinstance(obj, dict):
        raise FlowStateError("flow-state payload is not an object")
    return obj


# ── OIDC Client (Discovery, JWKS, PKCE, Code Exchange, ID Token Verification) ──
ALLOWED_ALGS: tuple[str, ...] = ("RS256", "ES256")
DISCOVERY_TTL_S = 24 * 3600
JWKS_TTL_S = 3600
JWKS_MIN_REFETCH_INTERVAL_S = 60.0
HTTP_TIMEOUT_S = 5.0
CLOCK_SKEW_S = 60
MAX_IAT_AGE_S = 600


class OIDCError(Exception):
    """Raised on any discovery / token-exchange / id_token validation failure."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason


@dataclass
class _MetadataCacheEntry:
    data: dict[str, Any]
    fetched_at: float


@dataclass
class _JwksCacheEntry:
    key_set: KeySet
    raw: dict[str, Any]
    fetched_at: float
    last_refetch_at: float


_metadata_cache: cachetools.LRUCache[str, _MetadataCacheEntry] = cachetools.LRUCache(maxsize=128)
_jwks_cache: cachetools.LRUCache[str, _JwksCacheEntry] = cachetools.LRUCache(maxsize=128)
_jwks_locks: dict[str, threading.Lock] = {}
_cache_lock = threading.Lock()
_test_transport: httpx.BaseTransport | None = None


def set_test_transport(transport: httpx.BaseTransport | None) -> None:
    """Inject an ``httpx.MockTransport`` for deterministic, zero-network tests."""
    global _test_transport
    _test_transport = transport
    reset_caches_for_tests()


def reset_caches_for_tests() -> None:
    with _cache_lock:
        _metadata_cache.clear()
        _jwks_cache.clear()
        _jwks_locks.clear()


def _http_client() -> httpx.Client:
    return httpx.Client(timeout=HTTP_TIMEOUT_S, follow_redirects=False, transport=_test_transport)


def _fetch_json(url: str) -> dict[str, Any]:
    try:
        with _http_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("[oauth.client] GET %s failed: %s", url, type(exc).__name__)
        raise OIDCError("idp_unavailable", f"GET {url} failed") from exc
    if not isinstance(data, dict):
        raise OIDCError("idp_unavailable", f"{url} did not return a JSON object")
    return data


def get_metadata(provider: OAuthProvider) -> dict[str, Any]:
    """Return OIDC discovery metadata for ``provider.id`` (24h in-process TTL)."""
    now = time.monotonic()
    with _cache_lock:
        entry = _metadata_cache.get(provider.id)
        if entry is not None and (now - entry.fetched_at) < DISCOVERY_TTL_S:
            return entry.data
    data = _fetch_json(provider.discovery_url)
    for req in ("issuer", "authorization_endpoint", "token_endpoint", "jwks_uri"):
        if not isinstance(data.get(req), str) or not data[req].strip():
            raise OIDCError("idp_unavailable", f"discovery missing {req}")
    with _cache_lock:
        _metadata_cache[provider.id] = _MetadataCacheEntry(data=data, fetched_at=time.monotonic())
    return data


def _load_key_set(raw: dict[str, Any]) -> KeySet:
    try:
        return KeySet.import_key_set(cast(Any, raw))
    except Exception as exc:
        raise OIDCError("idp_unavailable", "invalid JWKS document") from exc


def get_jwks(provider: OAuthProvider, metadata: dict[str, Any], *, force_refetch: bool = False) -> KeySet:
    """Return the provider's ``KeySet`` (1h TTL; single-flight refetch on unknown ``kid``)."""
    jwks_uri = metadata["jwks_uri"]
    now = time.monotonic()
    with _cache_lock:
        entry = _jwks_cache.get(provider.id)
        if entry is not None and not force_refetch and (now - entry.fetched_at) < JWKS_TTL_S:
            return entry.key_set
        lock = _jwks_locks.setdefault(provider.id, threading.Lock())
    with lock:
        now = time.monotonic()
        with _cache_lock:
            entry = _jwks_cache.get(provider.id)
            if entry is not None:
                if not force_refetch and (now - entry.fetched_at) < JWKS_TTL_S:
                    return entry.key_set
                if force_refetch and (now - entry.last_refetch_at) < JWKS_MIN_REFETCH_INTERVAL_S:
                    return entry.key_set
        raw = _fetch_json(jwks_uri)
        ks = _load_key_set(raw)
        fetched = time.monotonic()
        with _cache_lock:
            _jwks_cache[provider.id] = _JwksCacheEntry(
                key_set=ks, raw=raw, fetched_at=fetched, last_refetch_at=fetched if force_refetch else 0.0
            )
        return ks


def make_pkce_verifier() -> str:
    """RFC 7636 §4.1 code_verifier (64 characters in [A-Za-z0-9-._~])."""
    return generate_token(64)


def pkce_challenge(verifier: str) -> str:
    """RFC 7636 §4.2 S256 code_challenge = BASE64URL-NOPAD(SHA256(ASCII(verifier)))."""
    return create_s256_code_challenge(verifier)


def make_state_token() -> str:
    """256-bit CSRF state / OIDC nonce token."""
    return generate_token(43)


def build_authorize_url(
    provider: OAuthProvider, metadata: dict[str, Any], *, redirect_uri: str, state: str, nonce: str, code_challenge: str
) -> str:
    """Construct the IdP authorization URL with PKCE S256 + state + nonce."""
    oauth = OAuth2Client(
        client_id=provider.client_id,
        client_secret=provider.client_secret,
        scope=provider.scopes,
        redirect_uri=redirect_uri,
        code_challenge_method="S256",
    )
    url, _ = oauth.create_authorization_url(
        metadata["authorization_endpoint"],
        state=state,
        nonce=nonce,
        code_challenge=code_challenge,
        code_challenge_method="S256",
        prompt="select_account",
    )
    return url


def exchange_code(
    provider: OAuthProvider, metadata: dict[str, Any], *, code: str, code_verifier: str, redirect_uri: str
) -> dict[str, Any]:
    """POST the authorization code + PKCE ``code_verifier`` to ``token_endpoint``."""
    try:
        with OAuth2Client(
            client_id=provider.client_id,
            client_secret=provider.client_secret,
            token_endpoint_auth_method="client_secret_post",
            timeout=HTTP_TIMEOUT_S,
            follow_redirects=False,
            transport=_test_transport,
        ) as oauth:
            token = oauth.fetch_token(
                metadata["token_endpoint"],
                grant_type="authorization_code",
                code=code,
                redirect_uri=redirect_uri,
                code_verifier=code_verifier,
            )
    except OAuthError as exc:
        raise OIDCError("pkce_failed", f"token endpoint rejected exchange ({exc.error})") from exc
    except httpx.HTTPStatusError as exc:
        raise OIDCError("pkce_failed", f"token endpoint HTTP {exc.response.status_code}") from exc
    except Exception as exc:
        logger.warning("[oauth.client] token exchange transport error for %s: %s", provider.id, type(exc).__name__)
        raise OIDCError("idp_unavailable", "token endpoint unreachable") from exc
    if not isinstance(token, dict) or not isinstance(token.get("id_token"), str) or not token["id_token"]:
        raise OIDCError("pkce_failed", "token response missing id_token")
    return dict(token)


def _peek_header(raw_jwt: str) -> dict[str, Any]:
    parts = raw_jwt.split(".")
    if len(parts) != 3 or not parts[0]:
        raise OIDCError("bad_claim", "malformed JWT structure")
    pad = "=" * (-len(parts[0]) % 4)
    try:
        hdr = json.loads(base64.urlsafe_b64decode((parts[0] + pad).encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise OIDCError("bad_claim", "malformed JWT header") from exc
    if not isinstance(hdr, dict):
        raise OIDCError("bad_claim", "JWT header is not an object")
    return hdr


def _kid_in_keyset(kid: str | None, key_set: KeySet) -> bool:
    if not kid:
        return True
    return any(getattr(k, "kid", None) == kid for k in key_set.keys)


def verify_id_token(
    provider: OAuthProvider, metadata: dict[str, Any], raw_id_token: str, *, nonce: str
) -> dict[str, Any]:
    """Verify ``raw_id_token`` against OIDC Core §3.1.3.7 and return its claims."""
    if not isinstance(raw_id_token, str) or not raw_id_token:
        raise OIDCError("bad_claim", "empty id_token")
    header = _peek_header(raw_id_token)
    if header.get("alg") not in ALLOWED_ALGS:
        raise OIDCError("alg_rejected", f"disallowed alg {header.get('alg')!r}")
    kid = header.get("kid") if isinstance(header.get("kid"), str) else None
    key_set = get_jwks(provider, metadata)
    if kid and not _kid_in_keyset(kid, key_set):
        key_set = get_jwks(provider, metadata, force_refetch=True)
    try:
        token = jwt.decode(raw_id_token, key_set, algorithms=list(ALLOWED_ALGS))
    except JoseError as exc:
        raise OIDCError("alg_rejected", f"signature verification failed: {type(exc).__name__}") from exc
    claims = token.claims
    if not isinstance(claims, dict):
        raise OIDCError("bad_claim", "claims payload is not an object")
    allowed_issuers = [metadata["issuer"], *provider.extra_issuers]
    now = int(time.time())
    registry_check = jwt.JWTClaimsRegistry(
        now=now,
        leeway=CLOCK_SKEW_S,
        iss={"essential": True, "values": allowed_issuers},
        aud={"essential": True, "value": provider.client_id},
        sub={"essential": True},
        exp={"essential": True},
        iat={"essential": True},
    )
    try:
        registry_check.validate(claims)
    except ExpiredTokenError as exc:
        raise OIDCError("expired", str(exc)) from exc
    except MissingClaimError as exc:
        claim = getattr(exc, "claim", "")
        reason = (
            "bad_iss"
            if claim == "iss"
            else "bad_aud"
            if claim == "aud"
            else "expired"
            if claim in ("exp", "iat")
            else "bad_claim"
        )
        raise OIDCError(reason, str(exc)) from exc
    except InvalidClaimError as exc:
        claim = getattr(exc, "claim", "")
        reason = (
            "bad_iss"
            if claim == "iss"
            else "bad_aud"
            if claim == "aud"
            else "expired"
            if claim in ("exp", "nbf", "iat")
            else "bad_claim"
        )
        raise OIDCError(reason, str(exc)) from exc
    except JoseError as exc:
        raise OIDCError("bad_claim", str(exc)) from exc
    iat = claims.get("iat")
    if not isinstance(iat, (int, float)) or isinstance(iat, bool):
        raise OIDCError("expired", "non-numeric iat")
    if (now - float(iat)) > (MAX_IAT_AGE_S + CLOCK_SKEW_S) or float(iat) > (now + CLOCK_SKEW_S):
        raise OIDCError("expired", "iat outside acceptable window")
    azp = claims.get("azp")
    if azp is not None and azp != provider.client_id:
        raise OIDCError("bad_aud", "azp does not match client_id")
    if isinstance(claims.get("aud"), list) and len(claims["aud"]) > 1 and azp != provider.client_id:
        raise OIDCError("bad_aud", "multi-audience token missing matching azp")
    tok_nonce = claims.get("nonce")
    if not isinstance(tok_nonce, str) or not nonce or not hmac.compare_digest(tok_nonce, nonce):
        raise OIDCError("nonce_mismatch", "nonce claim mismatch")
    email = claims.get("email")
    if not isinstance(email, str) or not email.strip() or claims.get("email_verified") is not True:
        raise OIDCError("unverified_email", "email missing or not verified")
    if provider.allowed_hd and claims.get("hd") != provider.allowed_hd:
        raise OIDCError("wrong_domain", "hd claim does not match allowed_hd")
    sub = claims.get("sub")
    if not isinstance(sub, str) or not sub.strip():
        raise OIDCError("bad_claim", "empty sub")
    return dict(claims)


_self_mod = sys.modules[__name__]
registry: Any = _self_mod
client: Any = _self_mod
flow_state: Any = _self_mod
for _sub in ("registry", "client", "flow_state"):
    setattr(_self_mod, _sub, _self_mod)
    sys.modules[f"{__name__}.{_sub}"] = _self_mod
