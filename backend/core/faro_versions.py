"""Faro Web SDK version inventory, npm registry fetching, and bundle integrity helpers."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import io
import tarfile
import time
from typing import Any
from urllib.parse import urlsplit

import httpx

from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

REGISTRY_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk"
_BUNDLE_MEMBER_NAME = "package/dist/bundle/faro-web-sdk.iife.js"
DEFAULT_FARO_VERSION = "2.10.0"
_TIMEOUT = 10.0
_TARBALL_TIMEOUT = 20.0
_TARBALL_MAX_BYTES = 10 * 1024 * 1024
_BUNDLE_MAX_BYTES = 5 * 1024 * 1024
_SRI_ALGOS = ("sha512", "sha384", "sha256")
_TRUSTED_TARBALL_HOST = "registry.npmjs.org"
_ABBREVIATED_PACKUMENT_HEADERS = {"Accept": "application/vnd.npm.install-v1+json"}
_VERSIONS_CACHE_TTL = 3600.0
_VERSIONS_ERROR_BACKOFF = 30.0
_versions_cache_lock = asyncio.Lock()
_versions_cache: tuple[float, list[str]] | None = None


def _version_sort_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split(".")[:3])


def _is_stable_numeric(version: str) -> bool:
    if "-" in version or "+" in version:
        return False
    parts = version.split(".")[:3]
    return len(parts) == 3 and all(part.isascii() and part.isdigit() for part in parts)


async def _fetch_registry_packument(client: httpx.AsyncClient, err_prefix: str, *, req_err_mid: str = "") -> Any:
    from backend.utils.telemetry import tracked_call

    try:
        with tracked_call("GET", REGISTRY_URL, service="NPM Registry"):
            response = await client.get(REGISTRY_URL, headers=_ABBREVIATED_PACKUMENT_HEADERS, timeout=_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        raise ValueError(f"{err_prefix}: registry returned {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"{err_prefix}: {req_err_mid}{exc}") from exc
    except ValueError as exc:
        raise ValueError(f"{err_prefix}: malformed registry JSON: {exc}") from exc


async def fetch_available_faro_versions() -> list[str]:
    """Fetch available stable Faro Web SDK versions from the npm registry, newest first."""
    async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport()) as client:
        data = await _fetch_registry_packument(client, "Failed to fetch Faro versions")
    versions_dict = data.get("versions") if isinstance(data, dict) else None
    if not isinstance(versions_dict, dict) or not versions_dict:
        raise ValueError("Failed to fetch Faro versions: registry response has no 'versions' map")
    return sorted((v for v in versions_dict if _is_stable_numeric(v)), key=_version_sort_key, reverse=True)


def reset_versions_cache() -> None:
    global _versions_cache
    _versions_cache = None


class _VersionsCacheProxy:
    @staticmethod
    def clear() -> None:
        reset_versions_cache()


_CacheRegistry.register("faro_versions._versions_cache", _VersionsCacheProxy())


async def get_faro_versions_cached() -> list[str]:
    """Return the Faro Web SDK version list for page-load use, cached with stale-on-error."""
    global _versions_cache
    cached = _versions_cache
    if cached is not None and time.monotonic() < cached[0]:
        return cached[1]
    async with _versions_cache_lock:
        cached = _versions_cache
        if cached is not None and time.monotonic() < cached[0]:
            return cached[1]
        try:
            versions = await fetch_available_faro_versions()
        except ValueError:
            if cached is not None:
                _versions_cache = (time.monotonic() + _VERSIONS_ERROR_BACKOFF, cached[1])
                return cached[1]
            return []
        _versions_cache = (time.monotonic() + _VERSIONS_CACHE_TTL, versions)
        return versions


async def _fetch_version_dist(client: httpx.AsyncClient, version: str) -> dict[str, Any]:
    data = await _fetch_registry_packument(
        client, f"Failed to verify Faro bundle {version}", req_err_mid="registry error: "
    )
    versions_dict = data.get("versions") if isinstance(data, dict) else None
    version_meta = versions_dict.get(version) if isinstance(versions_dict, dict) else None
    dist = (
        version_meta.get("dist")
        if isinstance(version_meta, dict)
        else (data.get("dist") if isinstance(data, dict) else None)
    )
    if not isinstance(dist, dict):
        raise ValueError(f"Failed to verify Faro bundle {version}: registry has no dist metadata for this version")
    return dist


def _tarball_url_from_dist(dist: dict[str, Any], version: str) -> str:
    tarball = dist.get("tarball")
    if not isinstance(tarball, str) or not tarball:
        raise ValueError(f"Failed to verify Faro bundle {version}: registry dist has no tarball URL")
    parsed = urlsplit(tarball)
    if parsed.scheme != "https" or parsed.hostname != _TRUSTED_TARBALL_HOST:
        raise ValueError(
            f"Failed to verify Faro bundle {version}: refusing untrusted tarball URL host {parsed.hostname!r}"
        )
    return tarball


def _verify_artifact_integrity(artifact: bytes, dist: dict[str, Any]) -> bool:
    integrity = dist.get("integrity")
    if isinstance(integrity, str) and integrity:
        algo, sep, expected_b64 = integrity.partition("-")
        if sep and algo in _SRI_ALGOS and expected_b64:
            actual_b64 = base64.b64encode(hashlib.new(algo, artifact).digest()).decode()
            return hmac.compare_digest(actual_b64, expected_b64)
    shasum = dist.get("shasum")
    if isinstance(shasum, str) and shasum:
        actual_hex = hashlib.sha1(artifact, usedforsecurity=False).hexdigest()  # noqa: S324
        return hmac.compare_digest(actual_hex, shasum.lower())
    return False


def _extract_bundle_from_tarball(tarball: bytes, version: str) -> bytes:
    try:
        with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tar:
            member = next(
                (
                    c
                    for c in tar.getmembers()
                    if not c.name.startswith("/") and ".." not in c.name.split("/") and c.name == _BUNDLE_MEMBER_NAME
                ),
                None,
            )
            if member is None:
                raise ValueError(f"Faro tarball {version} is missing expected member {_BUNDLE_MEMBER_NAME!r}")
            if not member.isfile():
                raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} is not a regular file")
            if member.size > _BUNDLE_MAX_BYTES:
                raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} exceeds size ceiling")
            extracted = tar.extractfile(member)
            if extracted is None:
                raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} could not be read")
            data = extracted.read(_BUNDLE_MAX_BYTES + 1)
    except tarfile.TarError as exc:
        raise ValueError(f"Faro tarball {version} is not a valid gzipped tar archive: {exc}") from exc
    if len(data) > _BUNDLE_MAX_BYTES:
        raise ValueError(f"Faro tarball {version} member {_BUNDLE_MEMBER_NAME!r} exceeds size ceiling")
    return data


async def fetch_faro_bundle(version: str) -> bytes:
    """Download the Faro Web SDK npm tarball, verify integrity, and return the extracted IIFE bundle."""
    try:
        from backend.utils.telemetry import tracked_call

        async with httpx.AsyncClient(transport=httpx.AsyncHTTPTransport()) as client:
            dist = await _fetch_version_dist(client, version)
            tarball_url = _tarball_url_from_dist(dist, version)
            with tracked_call("GET", tarball_url, service="NPM Registry"):
                response = await client.get(tarball_url, timeout=_TARBALL_TIMEOUT)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise ValueError(f"Faro version {version} not found (404)") from exc
        raise ValueError(f"Failed to download Faro tarball {version}: HTTP {exc.response.status_code}") from exc
    except httpx.RequestError as exc:
        raise ValueError(f"Failed to download Faro tarball {version}: {exc}") from exc

    tarball = response.content
    if len(tarball) > _TARBALL_MAX_BYTES:
        raise ValueError(f"Faro tarball {version} exceeds size ceiling ({len(tarball)} bytes)")
    if not _verify_artifact_integrity(tarball, dist):
        raise ValueError(
            f"Faro tarball {version} failed integrity verification against the npm registry's "
            "published hash for this version — refusing to extract from an unverified archive"
        )
    return _extract_bundle_from_tarball(tarball, version)
