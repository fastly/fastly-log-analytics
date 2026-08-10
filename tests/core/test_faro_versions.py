"""Tests for ``backend.core.faro_versions``.

Version discovery (npm registry) and IIFE bundle download (unpkg.com) are
exercised against a captured registry payload and a synthetic bundle body
via ``httpx.MockTransport`` — same shape as
``tests/utils/test_refresh_fastly_cidrs.py``. No real network calls: the
npm registry and the unpkg CDN are third-party services whose availability
must never gate ``make test`` / ``make test-ci``.
"""

from __future__ import annotations

import json

import httpx
import pytest

from backend.core import faro_versions
from backend.core.faro_versions import _is_stable_numeric, fetch_available_faro_versions, fetch_faro_bundle

# Trimmed capture of the registry payload shape (2026-08-09). Deliberately
# unsorted, and salted with a pre-release + a build-metadata version so the
# filter and the descending semver sort are both exercised.
SAMPLE_REGISTRY_RESPONSE = {
    "name": "@grafana/faro-web-sdk",
    "versions": {
        "1.9.0": {"version": "1.9.0"},
        "2.10.0": {"version": "2.10.0"},
        "2.9.0": {"version": "2.9.0"},
        "1.4.5": {"version": "1.4.5"},
        "2.8.2": {"version": "2.8.2"},
        "2.9.0-alpha.1": {"version": "2.9.0-alpha.1"},
        "2.9.0+build.7": {"version": "2.9.0+build.7"},
    },
}

# Shape-accurate stand-in for the real IIFE bundle: the browser global the
# tracker looks for is what downstream tasks actually depend on.
SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"


# Bound at import time: the factory below replaces ``httpx.AsyncClient``, so it
# must call the real class through this alias or it recurses into itself.
_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _mock_transport(handler):
    """Patch ``httpx.AsyncClient`` so it is constructed with a mock transport.

    The two public functions own their client lifecycle (no injectable
    ``client`` parameter — later tasks depend on the signatures as-is), so
    the transport is swapped in at construction time instead.
    """

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_ASYNC_CLIENT(*args, **kwargs)

    return factory


@pytest.fixture
def mock_http(monkeypatch):
    """Return a callable that installs a MockTransport-backed AsyncClient."""

    def install(handler):
        monkeypatch.setattr(faro_versions.httpx, "AsyncClient", _mock_transport(handler))

    return install


# ── _is_stable_numeric ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "version",
    [
        "１.２.３",  # fullwidth digits (U+FF10-FF19) — str.isdigit() is True for these
        "١.٢.٣",  # Arabic-Indic digits (U+0660-0669) — also str.isdigit() True
        "2.9.٠",  # mixed ASCII + non-ASCII digit in one component
    ],
)
def test_is_stable_numeric_rejects_non_ascii_digits(version):
    """str.isdigit() is Unicode-aware (true for fullwidth/Arabic-Indic digits,
    not just ASCII 0-9). A registry key using these must not be treated as a
    valid stable version — this value can flow onward as a "version" string
    into VCL/object-path interpolation (rum_provisioning._assert_faro_version_safe)."""
    assert _is_stable_numeric(version) is False


def test_is_stable_numeric_accepts_plain_ascii_version():
    assert _is_stable_numeric("2.9.0") is True


async def test_fetch_available_faro_versions_excludes_non_ascii_digit_keys(mock_http):
    """End-to-end: a registry response containing a non-ASCII-digit "version"
    key must not appear in the returned list — confirms the fix, not just the
    helper it lives in."""
    payload = {
        "name": "@grafana/faro-web-sdk",
        "versions": {
            "2.9.0": {"version": "2.9.0"},
            "١.٢.٣": {"version": "١.٢.٣"},
            "１.２.３": {"version": "１.２.３"},
        },
    }
    mock_http(lambda _: httpx.Response(200, content=json.dumps(payload).encode()))

    versions = await fetch_available_faro_versions()

    assert versions == ["2.9.0"]


# ── fetch_available_faro_versions ───────────────────────────────────────────


async def test_fetch_available_faro_versions_filters_and_sorts_descending(mock_http):
    """Pre-releases/build metadata are dropped and the remainder comes back
    newest-first by semver — not lexicographically (2.10.0 must beat 2.9.0)."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == faro_versions.REGISTRY_URL
        return httpx.Response(200, content=json.dumps(SAMPLE_REGISTRY_RESPONSE).encode())

    mock_http(handler)
    versions = await fetch_available_faro_versions()

    assert versions == ["2.10.0", "2.9.0", "2.8.2", "1.9.0", "1.4.5"]
    assert all("-" not in v and "+" not in v for v in versions)


async def test_fetch_available_faro_versions_raises_on_non_200(mock_http):
    """A registry 500 must surface as ValueError, not a raw HTTPStatusError —
    callers handle one exception type across both public functions."""

    mock_http(lambda _: httpx.Response(500, content=b"upstream error"))

    with pytest.raises(ValueError, match="500"):
        await fetch_available_faro_versions()


async def test_fetch_available_faro_versions_raises_on_network_error(mock_http):
    """Connection failure is wrapped the same way as a bad status."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("registry unreachable", request=request)

    mock_http(handler)

    with pytest.raises(ValueError, match="Failed to fetch Faro versions"):
        await fetch_available_faro_versions()


async def test_fetch_available_faro_versions_raises_on_malformed_json(mock_http):
    """A 200 with a non-JSON body must not leak a JSONDecodeError."""

    mock_http(lambda _: httpx.Response(200, content=b"<html>proxy interstitial</html>"))

    with pytest.raises(ValueError, match="malformed registry JSON"):
        await fetch_available_faro_versions()


async def test_fetch_available_faro_versions_raises_when_versions_key_missing(mock_http):
    """Well-formed JSON without a usable ``versions`` map is still a failure —
    returning [] would let a later task silently publish nothing."""

    mock_http(lambda _: httpx.Response(200, content=json.dumps({"name": "x"}).encode()))

    with pytest.raises(ValueError, match="no 'versions' map"):
        await fetch_available_faro_versions()


# ── fetch_faro_bundle ───────────────────────────────────────────────────────


async def test_fetch_faro_bundle_returns_bytes(mock_http):
    """Happy path: correct unpkg URL requested, raw body returned verbatim."""
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, content=SAMPLE_BUNDLE)

    mock_http(handler)
    bundle = await fetch_faro_bundle("2.9.0")

    assert isinstance(bundle, bytes)
    assert bundle == SAMPLE_BUNDLE
    assert b"Faro" in bundle
    assert seen["url"] == ("https://unpkg.com/@grafana/faro-web-sdk@2.9.0/dist/bundle/faro-web-sdk.iife.js")


async def test_fetch_faro_bundle_raises_on_404(mock_http):
    """Unknown version — the message must name the 404 so the admin UI can
    distinguish 'no such version' from a transient CDN failure."""

    mock_http(lambda _: httpx.Response(404, content=b"Cannot find package"))

    with pytest.raises(ValueError, match="404") as exc_info:
        await fetch_faro_bundle("99.99.99")

    assert "99.99.99" in str(exc_info.value)


async def test_fetch_faro_bundle_raises_on_server_error(mock_http):
    """Non-404 failure status is wrapped too, and must NOT be reported as 404."""

    mock_http(lambda _: httpx.Response(503, content=b"service unavailable"))

    with pytest.raises(ValueError, match="503") as exc_info:
        await fetch_faro_bundle("2.9.0")

    assert "404" not in str(exc_info.value)


async def test_fetch_faro_bundle_raises_on_network_error(mock_http):
    """Transport-level failure (DNS/TCP) surfaces as ValueError, not ConnectError."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unpkg unreachable", request=request)

    mock_http(handler)

    with pytest.raises(ValueError, match="Failed to download Faro bundle"):
        await fetch_faro_bundle("2.9.0")
