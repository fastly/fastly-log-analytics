"""Faro Web SDK version discovery and bundle downloading.

Fetches available versions from the npm registry and downloads the IIFE
bundle by pulling the published npm **tarball** from the registry itself
(no third-party CDN involved) and extracting the one file we need.

npm's registry publishes ``dist.integrity`` (SRI hash) / ``dist.shasum`` for
each version, but those hashes are computed over the package **tarball**
(the full ``.tgz``, e.g. ~570 files for ``@grafana/faro-web-sdk``) — never
over any single file inside it. An earlier version of this module fetched
the standalone ``dist/bundle/faro-web-sdk.iife.js`` from unpkg.com and
compared *those* bytes against ``dist.integrity``; that comparison can never
succeed for any real release, because the hashes describe different byte
sequences (confirmed empirically for 2.9.0: the tarball hash matches the
tarball, and does not match the unpkg-served single file). Every download
was therefore silently rejected as a "failed integrity verification",
regardless of whether unpkg served the genuine file.

The fix: download the tarball (the only artifact the registry's hash
actually describes), verify the *tarball* bytes against
``dist.integrity``/``dist.shasum``, and only after that succeeds extract
``package/dist/bundle/faro-web-sdk.iife.js`` from the verified archive. This
is real supply-chain verification — it checks exactly what npm published —
and it removes the unpkg dependency entirely: one request, one third-party
host, one signature the registry actually vouches for.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import io
import tarfile
from typing import Any
from urllib.parse import urlsplit

import httpx

REGISTRY_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk"

# npm tarballs use a fixed leading "package/" path component (mandated by
# the packing tool, not per-package config), so the member path inside the
# archive is stable across versions.
_BUNDLE_MEMBER_NAME = "package/dist/bundle/faro-web-sdk.iife.js"

# Single source of truth for "the version we self-host when nothing else is
# pinned." The generated RUM tracker JS unconditionally loads the first-party
# /js/faro-sdk.js (no third-party CDN fallback), so a version must always be
# pinned once RUM is enabled. ``enable_rum`` applies this default when a
# caller omits an explicit faro_version, and the RUM sync cron
# (backend/cron/jobs/rum_sync.py) adopts it to self-heal a service that was
# enabled before this default existed and never got a version pinned.
# Chosen by the operator to match npm's dist-tags.latest as of this task;
# bump deliberately when a new version is vetted, not automatically.
DEFAULT_FARO_VERSION = "2.10.0"

_TIMEOUT = 10.0
# The tarball (~180 KB for 2.9.0, includes source maps, README, etc.) is
# larger than the single extracted file (~98 KB) and originates from the
# same registry host as the metadata call, so it gets a slightly longer
# timeout than the plain JSON lookups.
_TARBALL_TIMEOUT = 20.0

# Real-world tarball is ~180 KB. This ceiling is generous headroom against a
# future release bloating the package, while still bounding memory/CPU
# spent hashing and un-gzipping a response before it's been verified as the
# artifact the registry actually published.
_TARBALL_MAX_BYTES = 10 * 1024 * 1024
# Real-world extracted bundle is ~98 KB.
_BUNDLE_MAX_BYTES = 5 * 1024 * 1024

# SRI integrity strings are "<algo>-<base64 digest>". Only algorithms with a
# hashlib constructor of the same name are attempted; npm publishes sha512
# almost universally, sha256/sha384 are supported as a courtesy.
_SRI_ALGOS = ("sha512", "sha384", "sha256")

# The registry is the only origin this module trusts for both the integrity
# metadata AND the tarball bytes it describes — a ``dist.tarball`` URL
# pointing anywhere else is refused before it is ever fetched.
_TRUSTED_TARBALL_HOST = "registry.npmjs.org"


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
        from backend.utils.telemetry import tracked_call

        async with httpx.AsyncClient() as client:
            with tracked_call("GET", REGISTRY_URL, service="NPM Registry"):
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
        from backend.utils.telemetry import tracked_call

        with tracked_call("GET", REGISTRY_URL, service="NPM Registry"):
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


def _tarball_url_from_dist(dist: dict[str, Any], version: str) -> str:
    """Return the registry-published tarball URL for ``version``, host-pinned.

    ``dist.tarball`` is the only URL this module trusts for the bytes that
    ``dist.integrity``/``dist.shasum`` actually describe. It is expected to
    live on the same origin as the metadata call itself; refusing anything
    else is cheap defense-in-depth against a malformed or tampered
    ``tarball`` field pointing the download at an untrusted host.
    """
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
    """Verify ``artifact`` bytes against the registry's published ``dist``.

    ``artifact`` must be the raw tarball bytes — ``dist.integrity`` /
    ``dist.shasum`` are computed by npm over the packed ``.tgz``, never over
    any single file extracted from it. Prefers the SRI ``integrity`` field
    (``"<algo>-<base64 digest>"``, npm publishes sha512 almost universally);
    falls back to the legacy ``shasum`` (sha1 hex digest) field when
    integrity is absent. Returns False — never raises — when neither field
    is present/usable so the caller can raise a single, consistent "failed
    verification" error.
    """
    integrity = dist.get("integrity")
    if isinstance(integrity, str) and integrity:
        algo, sep, expected_b64 = integrity.partition("-")
        if sep and algo in _SRI_ALGOS and expected_b64:
            digest = hashlib.new(algo, artifact).digest()
            actual_b64 = base64.b64encode(digest).decode()
            return hmac.compare_digest(actual_b64, expected_b64)

    shasum = dist.get("shasum")
    if isinstance(shasum, str) and shasum:
        # npm's legacy ``dist.shasum`` field is always sha1 — not our
        # choice of algorithm, just matching what the registry publishes.
        actual_hex = hashlib.sha1(artifact, usedforsecurity=False).hexdigest()  # noqa: S324
        return hmac.compare_digest(actual_hex, shasum.lower())

    return False


def _extract_bundle_from_tarball(tarball: bytes, version: str) -> bytes:
    """Extract the IIFE bundle from an *already-integrity-verified* tarball.

    Never call this before ``_verify_artifact_integrity`` has returned True
    for ``tarball`` — extracting from an unverified archive defeats the
    point of verifying it.

    Reads exactly one member, by its exact expected name — never
    ``extractall()``. There is deliberately no "extraction root" for a
    traversal to escape: a member whose name isn't byte-for-byte
    ``_BUNDLE_MEMBER_NAME`` (an absolute path, a ``..``-laden path, or any
    other name) simply never matches and is ignored, so a maliciously named
    entry elsewhere in the archive can't affect anything. The matched member
    is additionally required to be a plain file (rejects a symlink/hardlink
    masquerading under the expected name) and is read with a hard size cap
    rather than trusted at face value.
    """
    try:
        with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tar:
            member = None
            for candidate in tar.getmembers():
                name = candidate.name
                # Defense-in-depth: reject unsafe-looking paths outright,
                # even though the exact-match below already can't select
                # one (belt-and-suspenders against a future refactor that
                # loosens the match to e.g. a suffix comparison).
                if name.startswith("/") or any(part == ".." for part in name.split("/")):
                    continue
                if name == _BUNDLE_MEMBER_NAME:
                    member = candidate
                    break

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
    """Download the Faro Web SDK npm tarball, verify it against the
    registry's published integrity hash, and return the extracted IIFE
    bundle bytes.

    Verification happens against the tarball — the only artifact
    ``dist.integrity``/``dist.shasum`` actually describes — never against
    the extracted file. Extraction only ever runs after verification
    succeeds.

    Args:
        version: Version string (e.g. ``"2.9.0"``).

    Returns:
        Raw bytes of ``package/dist/bundle/faro-web-sdk.iife.js``, taken
        from an integrity-verified tarball.

    Raises:
        ValueError: On 404 (unknown version), any other non-200 status, a
            network error, a registry lookup failure, an untrusted tarball
            host, an integrity mismatch/absence between the downloaded
            tarball and the registry's published hash for this version, a
            tarball that exceeds the size ceiling, or a verified tarball
            that doesn't contain the expected bundle member.
    """
    try:
        from backend.utils.telemetry import tracked_call

        async with httpx.AsyncClient() as client:
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
