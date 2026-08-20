"""Tests for ``backend.core.faro_versions``.

Version discovery (npm registry) and tarball download/verification are
exercised against a captured registry payload and a synthetic tarball body
via ``httpx.MockTransport`` — same shape as
``tests/utils/test_refresh_fastly_cidrs.py``. No real network calls: the
npm registry is a third-party service whose availability must never gate
``make test`` / ``make test-ci`` (the one exception is
``test_live_registry_tarball_verifies``, gated behind an env var — see its
docstring).

Fixture fidelity matters here specifically because the bug this module
fixes (F2-redux) was originally masked by a fixture that computed
``dist.integrity`` over the wrong bytes (the extracted file, not the
tarball) — the same misunderstanding baked into the code it was meant to
catch. ``_build_fixture_tarball`` below builds a REAL gzipped tar via the
stdlib ``tarfile`` module and every test computes ``dist.integrity`` /
``dist.shasum`` from the resulting tarball bytes, the way npm's publish
pipeline actually does — so a regression back to per-file hashing fails
these tests rather than passing them.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import tarfile

import httpx
import pytest

from backend.core import faro_versions
from backend.core.faro_versions import (
    _BUNDLE_MEMBER_NAME,
    _is_stable_numeric,
    fetch_available_faro_versions,
    fetch_faro_bundle,
)

TARBALL_URL = "https://registry.npmjs.org/@grafana/faro-web-sdk/-/faro-web-sdk-2.9.0.tgz"

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


def _add_tar_member(tar: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(content)
    tar.addfile(info, io.BytesIO(content))


def _build_fixture_tarball(
    *,
    bundle_name: str = _BUNDLE_MEMBER_NAME,
    bundle_content: bytes = SAMPLE_BUNDLE,
    include_bundle: bool = True,
    extra_files: dict[str, bytes] | None = None,
) -> bytes:
    """Build a REAL gzipped tar shaped like an npm package tarball.

    Includes some other package files (the real ``.tgz`` has ~570 of them)
    specifically so the fixture cannot be "simplified" back into a
    single-file archive where tarball bytes and bundle bytes coincide.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        _add_tar_member(tar, "package/package.json", b'{"name": "@grafana/faro-web-sdk", "version": "2.9.0"}')
        _add_tar_member(tar, "package/README.md", b"# Faro Web SDK\n\nMuch longer than the bundle itself.\n" * 20)
        _add_tar_member(tar, "package/dist/bundle/faro-web-sdk.iife.js.map", b'{"version":3,"sources":[]}')
        if include_bundle:
            _add_tar_member(tar, bundle_name, bundle_content)
        for name, content in (extra_files or {}).items():
            _add_tar_member(tar, name, content)
    return buf.getvalue()


# A fixture tarball with the real bundle at the expected path — the shared
# "happy path" archive most tests verify against.
SAMPLE_TARBALL = _build_fixture_tarball()


def _sri_integrity(data: bytes, algo: str = "sha512") -> str:
    """Build an SRI integrity string for ``data`` — same format npm
    publishes at ``versions[<v>].dist.integrity``. Callers must pass the
    TARBALL bytes, not any file extracted from it — that's what npm hashes."""
    digest = hashlib.new(algo, data).digest()
    return f"{algo}-{base64.b64encode(digest).decode()}"


def _registry_payload_with_dist(version_dist: dict[str, dict]) -> dict:
    """Build a minimal registry JSON payload with a ``dist`` block per version."""
    return {
        "name": "@grafana/faro-web-sdk",
        "versions": {v: {"version": v, "dist": dist} for v, dist in version_dist.items()},
    }


def _route_by_url(*, registry: httpx.Response | None = None, responses: dict[str, httpx.Response] | None = None):
    """Return an ``httpx.MockTransport`` handler that routes by exact URL.

    ``fetch_faro_bundle`` now issues two requests to the SAME origin
    (registry.npmjs.org) against the same mocked ``httpx.AsyncClient`` — one
    for the version metadata, one for the tarball ``dist.tarball`` points
    at — so routing has to key off the full URL, not the host.
    """
    responses = dict(responses or {})

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == faro_versions.REGISTRY_URL:
            assert registry is not None, "unexpected registry metadata request"
            return registry
        assert url in responses, f"unexpected request to {url}"
        return responses[url]

    return handler


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


async def test_fetch_faro_bundle_returns_extracted_bundle_bytes(mock_http):
    """Happy path: registry dist points at a tarball URL, the tarball is
    verified against the registry's published sha512 of the TARBALL, and
    the bytes returned are the extracted member — not the tarball itself."""
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=SAMPLE_TARBALL)},
        )
    )

    bundle = await fetch_faro_bundle("2.9.0")

    assert isinstance(bundle, bytes)
    assert bundle == SAMPLE_BUNDLE
    assert b"Faro" in bundle


def test_extracted_bundle_bytes_differ_from_tarball_bytes():
    """Pins the core fix: the tarball and the file extracted from it are
    NOT interchangeable. A fixture (or implementation) that collapses these
    two back into the same bytes would silently resurrect the original bug,
    where per-file hashes were compared against a whole-tarball digest."""
    assert SAMPLE_BUNDLE != SAMPLE_TARBALL
    assert SAMPLE_BUNDLE not in SAMPLE_TARBALL or len(SAMPLE_BUNDLE) < len(SAMPLE_TARBALL)
    assert len(SAMPLE_TARBALL) > len(SAMPLE_BUNDLE)


async def test_fetch_faro_bundle_verifies_against_shasum_fallback(mock_http):
    """When the registry entry has no ``integrity`` field, fall back to the
    legacy ``shasum`` (sha1 hex) field computed over the TARBALL — older
    registry snapshots only published shasum."""
    registry_payload = _registry_payload_with_dist(
        {
            "2.9.0": {
                "shasum": hashlib.sha1(SAMPLE_TARBALL, usedforsecurity=False).hexdigest(),
                "tarball": TARBALL_URL,
            }
        }
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=SAMPLE_TARBALL)},
        )
    )

    bundle = await fetch_faro_bundle("2.9.0")

    assert bundle == SAMPLE_BUNDLE


async def test_fetch_faro_bundle_raises_on_tarball_hash_mismatch(mock_http):
    """The core fix: a downloaded tarball whose bytes don't match the
    registry's published integrity hash must raise, not warn, and no bytes
    (extracted or otherwise) must ever reach the caller."""
    mutated_tarball = _build_fixture_tarball(bundle_content=b"!function(e){/* poisoned */}(window);")
    registry_payload = _registry_payload_with_dist(
        # integrity computed over SAMPLE_TARBALL, but the server serves a
        # DIFFERENT tarball — simulates a mutated/poisoned artifact.
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=mutated_tarball)},
        )
    )

    with pytest.raises(ValueError, match="failed integrity verification"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_member_missing_from_verified_archive(mock_http):
    """A tarball that verifies cleanly but doesn't contain the expected
    bundle member must still raise — integrity alone isn't the whole
    contract, the file has to actually be there."""
    tarball_without_bundle = _build_fixture_tarball(include_bundle=False)
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(tarball_without_bundle), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=tarball_without_bundle)},
        )
    )

    with pytest.raises(ValueError, match="missing expected member"):
        await fetch_faro_bundle("2.9.0")


@pytest.mark.parametrize(
    "malicious_name",
    [
        "/etc/passwd",
        "../../../etc/passwd",
        "package/../../../etc/passwd",
    ],
)
async def test_fetch_faro_bundle_rejects_malicious_member_name(mock_http, malicious_name):
    """An archive whose only bundle-shaped entry uses an absolute path or a
    ``..``-laden path (instead of the exact expected member name) must be
    treated as if the bundle were absent — never extracted, never trusted,
    even though the tarball itself verifies cleanly against its own hash."""
    hostile_tarball = _build_fixture_tarball(
        include_bundle=False,
        extra_files={malicious_name: b"attacker-controlled bytes"},
    )
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(hostile_tarball), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=hostile_tarball)},
        )
    )

    with pytest.raises(ValueError, match="missing expected member"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_registry_has_no_dist_for_version(mock_http):
    """No usable integrity target at all (version missing from the registry
    payload) must fail closed — refuse to return unverified bytes rather
    than silently skipping verification."""
    registry_payload = _registry_payload_with_dist(
        {"1.0.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=SAMPLE_TARBALL)},
        )
    )

    with pytest.raises(ValueError, match="no dist metadata"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_tarball_url_untrusted_host(mock_http):
    """A ``dist.tarball`` pointing at a host other than the registry itself
    must be refused before it's ever fetched — this module trusts exactly
    one origin for both the hash and the bytes it describes."""
    registry_payload = _registry_payload_with_dist(
        {
            "2.9.0": {
                "integrity": _sri_integrity(SAMPLE_TARBALL),
                "tarball": "https://evil.example/faro-web-sdk-2.9.0.tgz",
            }
        }
    )

    mock_http(lambda request: httpx.Response(200, content=json.dumps(registry_payload).encode()))

    with pytest.raises(ValueError, match="untrusted tarball URL host"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_on_404(mock_http):
    """Unknown version at the registry's tarball URL — the message must
    name the 404 so the admin UI can distinguish 'no such version' from a
    transient failure."""
    registry_payload = _registry_payload_with_dist(
        {"99.99.99": {"integrity": _sri_integrity(b"irrelevant"), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(404, content=b"Cannot find package")},
        )
    )

    with pytest.raises(ValueError, match="404") as exc_info:
        await fetch_faro_bundle("99.99.99")

    assert "99.99.99" in str(exc_info.value)


async def test_fetch_faro_bundle_raises_on_server_error(mock_http):
    """Non-404 failure status is wrapped too, and must NOT be reported as 404."""
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(503, content=b"service unavailable")},
        )
    )

    with pytest.raises(ValueError, match="503") as exc_info:
        await fetch_faro_bundle("2.9.0")

    assert "404" not in str(exc_info.value)


async def test_fetch_faro_bundle_raises_on_network_error(mock_http):
    """Transport-level failure (DNS/TCP) downloading the tarball surfaces as
    ValueError, not ConnectError. Registry lookup succeeds first."""
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(SAMPLE_TARBALL), "tarball": TARBALL_URL}}
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == faro_versions.REGISTRY_URL:
            return httpx.Response(200, content=json.dumps(registry_payload).encode())
        raise httpx.ConnectError("tarball host unreachable", request=request)

    mock_http(handler)

    with pytest.raises(ValueError, match="Failed to download Faro tarball"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_on_registry_network_error(mock_http):
    """A registry outage must fail the download closed — verification can't
    be skipped just because the registry is unreachable."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("registry unreachable", request=request)

    mock_http(handler)

    with pytest.raises(ValueError, match="Failed to verify Faro bundle"):
        await fetch_faro_bundle("2.9.0")


async def test_fetch_faro_bundle_raises_when_tarball_exceeds_size_ceiling(mock_http):
    """A response far larger than any real npm tarball must be refused
    before it's hashed or unpacked, not silently accepted."""
    oversized = b"0" * (faro_versions._TARBALL_MAX_BYTES + 1)
    registry_payload = _registry_payload_with_dist(
        {"2.9.0": {"integrity": _sri_integrity(oversized), "tarball": TARBALL_URL}}
    )

    mock_http(
        _route_by_url(
            registry=httpx.Response(200, content=json.dumps(registry_payload).encode()),
            responses={TARBALL_URL: httpx.Response(200, content=oversized)},
        )
    )

    with pytest.raises(ValueError, match="exceeds size ceiling"):
        await fetch_faro_bundle("2.9.0")


# ── live registry probe (opt-in, never runs in default/CI runs) ────────────


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("FARO_LIVE_REGISTRY_TEST") != "1",
    reason="opt-in live network test against the real npm registry; set FARO_LIVE_REGISTRY_TEST=1 to run",
)
async def test_live_registry_tarball_verifies():
    """Fetches the REAL registry metadata and REAL tarball for a pinned
    version and asserts verification succeeds end-to-end.

    This is the test that would have caught the original bug: the mocked
    tests above are only as honest as their fixtures, and the original
    fixture encoded the same "hash the extracted file" misunderstanding as
    the code it was meant to catch. This test has no fixture to be wrong
    about — it hits the actual registry.npmjs.org and verifies against
    whatever it actually publishes.

    Never runs in `make test` / `make test-ci` / any CI job: gated on
    ``FARO_LIVE_REGISTRY_TEST=1``, which nothing in this repo's CI sets.
    """
    bundle = await fetch_faro_bundle(faro_versions.DEFAULT_FARO_VERSION)
    assert isinstance(bundle, bytes)
    assert len(bundle) > 1000
    assert b"Faro" in bundle or b"faro" in bundle
