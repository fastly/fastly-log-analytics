"""Tests for Faro version discovery and download.

Tests the npm registry version fetching and unpkg.com bundle downloading.
"""

from __future__ import annotations

import pytest

from backend.core.faro_versions import fetch_available_faro_versions, fetch_faro_bundle


@pytest.mark.asyncio
async def test_fetch_available_faro_versions_returns_valid_semver():
    """Test that fetch_available_faro_versions returns a non-empty list of valid semver strings."""
    versions = await fetch_available_faro_versions()

    assert isinstance(versions, list)
    assert len(versions) > 0

    # Verify each version is a valid semver string (X.Y.Z)
    for version in versions:
        assert isinstance(version, str)
        parts = version.split(".")
        assert len(parts) >= 3, f"Version {version} is not valid semver"
        # Check that first 3 parts are integers
        assert parts[0].isdigit()
        assert parts[1].isdigit()
        assert parts[2].isdigit()

    # Verify no pre-releases (should be filtered out)
    for version in versions:
        assert "-" not in version, f"Pre-release version {version} should be filtered out"
        assert "+" not in version, f"Build metadata version {version} should be filtered out"


@pytest.mark.asyncio
async def test_fetch_available_faro_versions_sorted_descending():
    """Test that versions are sorted descending by semver (newest first)."""
    versions = await fetch_available_faro_versions()

    assert len(versions) > 1

    # Parse versions as tuples for comparison
    def parse_version(v: str) -> tuple[int, int, int]:
        parts = v.split(".")[:3]
        return tuple(map(int, parts))  # type: ignore

    version_tuples = [parse_version(v) for v in versions]

    # Verify sorted in descending order
    for i in range(len(version_tuples) - 1):
        assert version_tuples[i] >= version_tuples[i + 1], f"Version {versions[i]} should come before {versions[i + 1]}"


@pytest.mark.asyncio
async def test_fetch_faro_bundle_returns_bytes():
    """Test that fetch_faro_bundle returns actual Faro IIFE bundle as bytes."""
    # Use a known recent version
    bundle = await fetch_faro_bundle("2.9.0")

    assert isinstance(bundle, bytes)
    assert len(bundle) > 0

    # Check that it contains Faro SDK markers (case-insensitive)
    bundle_str = bundle.decode("utf-8", errors="ignore")
    assert "Faro" in bundle_str or "faro" in bundle_str, "Bundle should contain Faro SDK code"


@pytest.mark.asyncio
async def test_fetch_faro_bundle_raises_on_404():
    """Test that fetch_faro_bundle raises ValueError for non-existent versions."""
    with pytest.raises(ValueError) as exc_info:
        await fetch_faro_bundle("99.99.99")

    error_msg = str(exc_info.value).lower()
    assert "404" in error_msg or "not found" in error_msg, (
        f"Error message should mention 404 or not found, got: {exc_info.value}"
    )
