"""Faro Web SDK version discovery and bundle downloading.

Fetches available versions from the npm registry and downloads IIFE bundles
from unpkg.com. Both functions wrap every transport/parse failure in
``ValueError`` so callers have a single exception type to handle.

The bundle is downloaded from a different origin (unpkg.com) than the one
that publishes its version list (registry.npmjs.org), and unpkg serves the
bytes with no integrity guarantee of its own. ``fetch_faro_bundle``
verifies the downloaded bytes against the npm registry's published
``dist.integrity`` (SRI hash) / ``dist.shasum`` for that exact version
before returning — a mismatch raises rather than warns, so a mutated or
poisoned unpkg artifact never reaches the operator's FOS bucket (F2 audit
finding).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any

import httpx

REGISTRY_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk"
BUNDLE_URL_TEMPLATE = "https://unpkg.com/@grafana/faro-web-sdk@{version}/dist/bundle/faro-web-sdk.iife.js"

_TIMEOUT = 10.0

# SRI integrity strings are "<algo>-<base64 digest>". Only algorithms with a
# hashlib constructor of the same name are attempted; npm publishes sha512
# almost universally, sha256/sha384 are supported as a courtesy.
_SRI_ALGOS = ("sha512", "sha384", "sha256")


def _version_sort_key(version: str) -> tuple[int, ...]:
    """Parse a semver string into a comparable tuple of its first 3 parts."""
    return tuple(int(part) for part in version.split(".")[:3])


def _is_stable_numeric(version: str) -> bool:
    """True for plain ``X.Y.Z`` releases — no pre-release or build metadata.

    The numeric check also guards the sort: a registry entry whose first
    three parts aren't all digits would make ``int()`` raise mid-sort.

    ``str.isdigit()`` is Unicode-aware (true for fullwidth "１" or Arabic-Indic
    "١", not just ASCII 0-9), so it's paired with ``isascii()`` here — a
    registry key using non-ASCII digit codepoints must not be treated as a
    valid version, since this return value can flow onward as a "version"
    string into VCL/object-path interpolation (see
    backend/core/fastly/rum_provisioning.py's _assert_faro_version_safe).
    """
    if "-" in version or "+" in version:
        return False
    parts = version.split(".")[:3]
    return len(parts) == 3 and all(part.isascii() and part.isdigit() for part in parts)


async def fetch_available_faro_versions() -> list[str]:
    """Fetch available Faro Web SDK versions from the npm registry.

    Filters out pre-releases (versions containing ``-`` or ``+``) and sorts
    descending by semver (newest first).

    Returns:
        List of version strings, newest first (e.g. ``["2.9.0", "2.8.2", ...]``).

    Raises:
        ValueError: On a non-200 response, a network error, malformed JSON,
            or a payload without a usable ``versions`` map.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(REGISTRY_URL, timeout=_TIMEOUT)
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPStatusError as exc:
        raise ValueError(f"Failed to fetch Faro versions: registry returned {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to fetch Faro versions: {exc}") from exc
    except ValueError as exc:  # json() raises ValueError/JSONDecodeError
        raise ValueError(f"Failed to fetch Faro versions: malformed registry JSON: {exc}") from exc

    versions_dict = data.get("versions") if isinstance(data, dict) else None
    if not isinstance(versions_dict, dict) or not versions_dict:
        raise ValueError("Failed to fetch Faro versions: registry response has no 'versions' map")

    stable_versions = [v for v in versions_dict if _is_stable_numeric(v)]
    stable_versions.sort(key=_version_sort_key, reverse=True)
    return stable_versions


async def _fetch_version_dist(client: httpx.AsyncClient, version: str) -> dict[str, Any]:
    """Return the npm registry's ``dist`` metadata for ``version``.

    A bundle can't be verified without this, so any registry failure here
    (network, non-200, malformed JSON, unknown version, missing dist block)
    raises ``ValueError`` — the caller must fail the download closed rather
    than silently skip verification on a registry hiccup.
    """
    try:
        response = await client.get(REGISTRY_URL, timeout=_TIMEOUT)
        response.raise_for_status()
        data = response.json()
    except httpx.HTTPStatusError as exc:
        raise ValueError(
            f"Failed to verify Faro bundle {version}: registry returned {exc.response.status_code}"
        ) from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to verify Faro bundle {version}: registry error: {exc}") from exc
    except ValueError as exc:  # json() raises ValueError/JSONDecodeError
        raise ValueError(f"Failed to verify Faro bundle {version}: malformed registry JSON: {exc}") from exc

    versions_dict = data.get("versions") if isinstance(data, dict) else None
    version_meta = versions_dict.get(version) if isinstance(versions_dict, dict) else None
    dist = version_meta.get("dist") if isinstance(version_meta, dict) else None
    if not isinstance(dist, dict):
        raise ValueError(f"Failed to verify Faro bundle {version}: registry has no dist metadata for this version")
    return dist


def _verify_bundle_integrity(bundle: bytes, dist: dict[str, Any]) -> bool:
    """Verify ``bundle`` bytes against the registry's published ``dist``.

    Prefers the SRI ``integrity`` field (``"<algo>-<base64 digest>"``,
    npm publishes sha512 almost universally); falls back to the legacy
    ``shasum`` (sha1 hex digest) field when integrity is absent. Returns
    False — never raises — when neither field is present/usable so the
    caller can raise a single, consistent "failed verification" error.
    """
    integrity = dist.get("integrity")
    if isinstance(integrity, str) and integrity:
        algo, sep, expected_b64 = integrity.partition("-")
        if sep and algo in _SRI_ALGOS and expected_b64:
            digest = hashlib.new(algo, bundle).digest()
            actual_b64 = base64.b64encode(digest).decode()
            return hmac.compare_digest(actual_b64, expected_b64)

    shasum = dist.get("shasum")
    if isinstance(shasum, str) and shasum:
        # npm's legacy ``dist.shasum`` field is always sha1 — not our
        # choice of algorithm, just matching what the registry publishes.
        actual_hex = hashlib.sha1(bundle, usedforsecurity=False).hexdigest()  # noqa: S324
        return hmac.compare_digest(actual_hex, shasum.lower())

    return False


async def fetch_faro_bundle(version: str) -> bytes:
    """Download the Faro Web SDK IIFE bundle from unpkg.com and verify it
    against the npm registry's published integrity hash before returning.

    The registry (registry.npmjs.org) and the bundle CDN (unpkg.com) are
    different origins, and unpkg serves bytes with no integrity guarantee
    of its own — those exact bytes get uploaded to the operator's FOS
    bucket and served to every visitor of the customer's website. This
    fetches the registry's ``dist.integrity`` / ``dist.shasum`` for
    ``version`` and verifies the downloaded bundle against it; a mismatch
    raises rather than warns.

    Args:
        version: Version string (e.g. ``"2.9.0"``).

    Returns:
        Raw, integrity-verified bytes of the IIFE bundle.

    Raises:
        ValueError: On 404 (unknown version), any other non-200 status, a
            network error, a registry lookup failure, or an integrity
            mismatch/absence between the downloaded bytes and the
            registry's published hash for this version.
    """
    bundle_url = BUNDLE_URL_TEMPLATE.format(version=version)

    try:
        async with httpx.AsyncClient() as client:
            dist = await _fetch_version_dist(client, version)
            response = await client.get(bundle_url, timeout=_TIMEOUT)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise ValueError(f"Faro version {version} not found (404)") from exc
        raise ValueError(f"Failed to download Faro bundle {version}: HTTP {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to download Faro bundle {version}: {exc}") from exc

    bundle = response.content
    if not _verify_bundle_integrity(bundle, dist):
        raise ValueError(
            f"Faro bundle {version} failed integrity verification against the npm registry's "
            "published hash for this version — refusing to return unverified bytes"
        )
    return bundle
