"""Deployment-global OIDC provider registry (hybrid config).

The OAuth handshake is pre-auth and serviceless — there is no active service at
``/authorize``/``/callback`` — so credentials cannot live in per-service
``configs/{service_id}.json``. Instead they live at deployment scope:

* **Non-secret per-provider fields** (``display_name``, ``discovery_url``,
  ``scopes``, ``allowed_hd``, ``extra_issuers``, ``enabled``) live in a
  gitignored ``0600`` JSON file (default ``data/system/oauth_providers.json``,
  overridable via ``OAUTH_PROVIDERS_CONFIG_PATH``). This mirrors the existing
  ``usage_logging.json`` global-config pattern.
* **Secrets** (``client_id`` / ``client_secret``) come from env vars
  ``OAUTH_<KEY>_CLIENT_ID`` / ``OAUTH_<KEY>_CLIENT_SECRET`` so they never touch
  disk in the tree, matching the VM ``.env`` + ``${VAR:-}`` passthrough convention.

Nothing here is tracked in git (see
``tests/security/test_no_infra_leak_in_tracked_tree.py``). Endpoints, ``jwks_uri``,
the canonical ``issuer`` and supported algs are NOT stored — Authlib discovers
them from ``discovery_url`` at runtime (see ``client.py``); ``extra_issuers`` is
the one issuer-related override (Google accepts both ``https://accounts.google.com``
and the scheme-less ``accounts.google.com`` — design §2.9).

The whole feature is default-OFF: it is inert unless BOTH a provider registry
and ``OAUTH_FLOW_STATE_SECRET`` are configured.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path

from backend.config import SYSTEM_DATA_DIR

logger = logging.getLogger(__name__)

_ENV_CONFIG_PATH = "OAUTH_PROVIDERS_CONFIG_PATH"
_ENV_FLOW_STATE_SECRET = "OAUTH_FLOW_STATE_SECRET"
_ENV_PASSCODE_ENABLED = "SHARE_PASSCODE_LOGIN_ENABLED"

_DEFAULT_SCOPES = "openid email"

# (path_str, mtime_ns, parsed) — keyed on path so a test that repoints
# OAUTH_PROVIDERS_CONFIG_PATH invalidates automatically.
_cache: tuple[str, int, dict] | None = None
_lock = threading.Lock()


@dataclass(frozen=True)
class OAuthProvider:
    """A fully-configured OIDC provider (both non-secret fields and env creds).

    Only providers with a display name, discovery URL, client_id AND
    client_secret become an ``OAuthProvider`` — a half-configured entry is
    invisible everywhere (never returned by the accessors below).
    """

    id: str
    display_name: str
    discovery_url: str
    client_id: str
    client_secret: str
    scopes: str = _DEFAULT_SCOPES
    allowed_hd: str | None = None
    extra_issuers: tuple[str, ...] = ()
    enabled: bool = True
    # Just-in-time provisioning: when True, a verified login with NO pre-created
    # invite auto-creates one instead of being rejected — for a fully-trusted,
    # org-restricted provider (pair with allowed_hd / an Internal consent screen).
    # The invite-email allowlist stops being the gate; the provider's own org
    # restriction becomes it. auto_provision_service_ids scopes the JIT invite
    # (empty → all currently-configured services); auto_provision_mask_ips sets
    # its PII policy.
    auto_provision: bool = False
    auto_provision_service_ids: tuple[str, ...] = ()
    auto_provision_mask_ips: bool = False

    def public_dict(self) -> dict:
        """Analyst-facing view — id + display_name ONLY.

        NEVER includes client_id/client_secret/discovery_url. This is what the
        unauth ``/api/share/auth-config`` endpoint serves.
        """
        return {"id": self.id, "display_name": self.display_name}

    def admin_dict(self) -> dict:
        """Admin-facing view — adds ``enabled`` so an admin can pre-create an
        invite for a temporarily-disabled provider. Still no secrets."""
        return {"id": self.id, "display_name": self.display_name, "enabled": self.enabled}


def _config_path() -> Path:
    override = os.getenv(_ENV_CONFIG_PATH, "").strip()
    if override:
        return Path(override)
    return SYSTEM_DATA_DIR / "oauth_providers.json"


def _load_raw() -> dict:
    """Return the parsed registry JSON, or ``{}`` when missing/malformed.

    Fails closed: a missing or unparseable registry disables OAuth rather than
    raising into a request path. mtime-cached (keyed on path) to keep the
    pre-auth ``/authorize`` and unauth ``/auth-config`` hot paths cheap.
    """
    global _cache
    path = _config_path()
    try:
        mtime_ns = path.stat().st_mtime_ns
    except (FileNotFoundError, NotADirectoryError):
        return {}
    path_str = str(path)
    with _lock:
        if _cache is not None and _cache[0] == path_str and _cache[1] == mtime_ns:
            return _cache[2]
    try:
        with open(path, "rb") as f:
            raw = json.loads(f.read())
    except Exception:
        logger.exception("[oauth.registry] failed to parse provider registry at %s — OAuth disabled", path_str)
        return {}
    if not isinstance(raw, dict):
        logger.error("[oauth.registry] provider registry at %s is not a JSON object — OAuth disabled", path_str)
        return {}
    with _lock:
        _cache = (path_str, mtime_ns, raw)
    return raw


def _env_suffix(provider_key: str) -> str:
    """``OAUTH_<SUFFIX>_CLIENT_ID`` — non-alnum chars collapse to underscore."""
    return "".join(c if c.isalnum() else "_" for c in provider_key).upper()


def _provider_from_entry(key: str, entry: object) -> OAuthProvider | None:
    if not isinstance(entry, dict):
        return None
    display_name = str(entry.get("display_name") or "").strip()
    discovery_url = str(entry.get("discovery_url") or "").strip()
    suffix = _env_suffix(key)
    client_id = os.getenv(f"OAUTH_{suffix}_CLIENT_ID", "").strip()
    client_secret = os.getenv(f"OAUTH_{suffix}_CLIENT_SECRET", "").strip()
    # A provider is only "configured" with all four present. A registry entry
    # naming a provider whose env creds are absent stays invisible — never a
    # half-configured button that dead-ends at /authorize.
    if not (display_name and discovery_url and client_id and client_secret):
        return None
    scopes = str(entry.get("scopes") or _DEFAULT_SCOPES).strip() or _DEFAULT_SCOPES
    allowed_hd = str(entry.get("allowed_hd") or "").strip() or None
    raw_extra = entry.get("extra_issuers")
    extra_issuers = tuple(str(x).strip() for x in raw_extra if str(x).strip()) if isinstance(raw_extra, list) else ()
    enabled = bool(entry.get("enabled", True))
    raw_aps = entry.get("auto_provision_service_ids")
    auto_provision_service_ids = (
        tuple(str(x).strip() for x in raw_aps if str(x).strip()) if isinstance(raw_aps, list) else ()
    )
    return OAuthProvider(
        id=key,
        display_name=display_name,
        discovery_url=discovery_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
        allowed_hd=allowed_hd,
        extra_issuers=extra_issuers,
        enabled=enabled,
        auto_provision=bool(entry.get("auto_provision", False)),
        auto_provision_service_ids=auto_provision_service_ids,
        auto_provision_mask_ips=bool(entry.get("auto_provision_mask_ips", False)),
    )


def flow_state_secret() -> str | None:
    """The dedicated ``OAUTH_FLOW_STATE_SECRET`` env value, or None if unset.

    Required for any OAuth flow (seals the ``oauth_flow_state`` cookie). Its
    presence is the master feature switch — without it the feature is inert.
    """
    return os.getenv(_ENV_FLOW_STATE_SECRET, "").strip() or None


def feature_on() -> bool:
    """True iff the OAuth feature switch (the flow-state secret) is set."""
    return flow_state_secret() is not None


def get_all_providers() -> list[OAuthProvider]:
    """All fully-configured providers (enabled or not), sorted by id.

    Empty unless the feature switch is on — even a fully-populated registry file
    is ignored when ``OAUTH_FLOW_STATE_SECRET`` is unset, so an operator can't
    accidentally advertise providers the handshake can't complete.
    """
    if not feature_on():
        return []
    raw = _load_raw()
    out: list[OAuthProvider] = []
    for key in sorted(raw.keys()):
        provider = _provider_from_entry(key, raw[key])
        if provider is not None:
            out.append(provider)
    return out


def get_enabled_providers() -> list[OAuthProvider]:
    """Configured providers with ``enabled=true`` — the analyst-visible set."""
    return [p for p in get_all_providers() if p.enabled]


def get_provider(key: str) -> OAuthProvider | None:
    """Return a specific configured provider by id (enabled or not), else None.

    Callers that must not act on a disabled provider (e.g. ``/authorize``) check
    ``.enabled`` themselves; create-invite validation intentionally allows a
    disabled provider so an admin can pre-create.
    """
    if not key:
        return None
    for provider in get_all_providers():
        if provider.id == key:
            return provider
    return None


def oauth_enabled() -> bool:
    """True iff the feature switch is on AND at least one provider is configured."""
    return feature_on() and bool(get_all_providers())


def passcode_login_enabled() -> bool:
    """Whether passcode login is offered. Default ON (passcode flow unchanged).

    Set ``SHARE_PASSCODE_LOGIN_ENABLED=0`` (or false/no/off) to make SSO the
    exclusive gateway. When off, the passcode ``/login`` endpoint fails closed
    (see share_auth) and ``/auth-config`` reports ``passcode_enabled=false``.
    """
    v = os.getenv(_ENV_PASSCODE_ENABLED, "").strip().lower()
    return v not in ("0", "false", "no", "off")


def reset_cache_for_tests() -> None:
    """Drop the mtime cache so a test that rewrites the registry file mid-run
    (same path, sub-microsecond mtime) is picked up."""
    global _cache
    with _lock:
        _cache = None
