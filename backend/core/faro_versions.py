"""Faro Web SDK version discovery and bundle downloading.

Fetches available versions from the npm registry and downloads IIFE bundles
from unpkg.com. Both functions wrap every transport/parse failure in
``ValueError`` so callers have a single exception type to handle.
"""

from __future__ import annotations

import httpx

REGISTRY_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk"
BUNDLE_URL_TEMPLATE = "https://unpkg.com/@grafana/faro-web-sdk@{version}/dist/bundle/faro-web-sdk.iife.js"

_TIMEOUT = 10.0


def _version_sort_key(version: str) -> tuple[int, ...]:
    """Parse a semver string into a comparable tuple of its first 3 parts."""
    return tuple(int(part) for part in version.split(".")[:3])


def _is_stable_numeric(version: str) -> bool:
    """True for plain ``X.Y.Z`` releases — no pre-release or build metadata.

    The numeric check also guards the sort: a registry entry whose first
    three parts aren't all digits would make ``int()`` raise mid-sort.
    """
    if "-" in version or "+" in version:
        return False
    parts = version.split(".")[:3]
    return len(parts) == 3 and all(part.isdigit() for part in parts)


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


async def fetch_faro_bundle(version: str) -> bytes:
    """Download the Faro Web SDK IIFE bundle from unpkg.com.

    Args:
        version: Version string (e.g. ``"2.9.0"``).

    Returns:
        Raw bytes of the IIFE bundle.

    Raises:
        ValueError: On 404 (unknown version), any other non-200 status, or a
            network error.
    """
    bundle_url = BUNDLE_URL_TEMPLATE.format(version=version)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(bundle_url, timeout=_TIMEOUT)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise ValueError(f"Faro version {version} not found (404)") from exc
        raise ValueError(f"Failed to download Faro bundle {version}: HTTP {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to download Faro bundle {version}: {exc}") from exc

    return response.content
