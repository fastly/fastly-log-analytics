"""Faro Web SDK version discovery and bundle downloading.

Fetches available versions from npm registry and downloads IIFE bundles from unpkg.com.
"""

from __future__ import annotations

import httpx


async def fetch_available_faro_versions() -> list[str]:
    """Fetch available Faro Web SDK versions from npm registry.

    Fetches version list from https://registry.npmjs.org/@grafana/faro-web-sdk,
    filters out pre-releases (versions containing '-' or '+'), and sorts descending
    by semver (newest first).

    Returns:
        List of version strings (e.g., ['1.4.5', '1.4.4', '1.4.3', ...])
    """
    registry_url = "https://registry.npmjs.org/@grafana/faro-web-sdk"

    async with httpx.AsyncClient() as client:
        response = await client.get(registry_url, timeout=10.0)
        response.raise_for_status()

    data = response.json()
    versions_dict = data.get("versions", {})

    # Filter out pre-releases (versions containing '-' or '+')
    stable_versions = [v for v in versions_dict.keys() if "-" not in v and "+" not in v]

    # Sort descending by semver tuple
    def parse_version(v: str) -> tuple[int, ...]:
        """Parse semver string to tuple for comparison."""
        parts = v.split(".")
        return tuple(map(int, parts[:3]))  # Take first 3 parts for comparison

    stable_versions.sort(key=parse_version, reverse=True)

    return stable_versions


async def fetch_faro_bundle(version: str) -> bytes:
    """Download Faro Web SDK IIFE bundle from unpkg.com.

    Args:
        version: Version string (e.g., '1.4.5')

    Returns:
        Raw bytes of the IIFE bundle

    Raises:
        ValueError: On 404 or network errors
    """
    bundle_url = f"https://unpkg.com/@grafana/faro-web-sdk@{version}/dist/bundle/faro-web-sdk.iife.js"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(bundle_url, timeout=10.0)

            if response.status_code == 404:
                raise ValueError(f"Faro version {version} not found (404)")

            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(f"Faro version {version} not found (404)") from e
            raise ValueError(f"Failed to download Faro bundle: {e}") from e
        except httpx.RequestError as e:
            raise ValueError(f"Failed to download Faro bundle: {e}") from e

    return response.content
