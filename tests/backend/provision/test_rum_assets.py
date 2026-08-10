"""Tests for the Faro Web SDK download+upload+cleanup functions in
``backend.provision.rum_assets``.

FOS access goes through ``httpx.Client`` + real (local-only) SigV4 signing;
``httpx.MockTransport`` intercepts the actual HTTP call so no bytes hit the
network — same shape as ``tests/core/test_faro_versions.py`` and
``tests/utils/test_refresh_fastly_cidrs.py``. ``fetch_faro_bundle`` (Task 1)
is monkeypatched directly rather than mocked at the transport layer, since
its own network behavior is already covered by ``test_faro_versions.py``.
"""

from __future__ import annotations

import hashlib

import httpx
import pytest

from backend.provision import rum_assets
from backend.provision.rum_assets import (
    cleanup_old_faro_versions,
    detect_faro_version_change,
    download_and_upload_faro,
)

FAKE_CFG = {
    "service_id": "svc_test",
    "fos_access_key_id": "AKIAFAKEEXAMPLE",
    "fos_secret_access_key": "fakeSecretKeyExample",
    "fos_bucket": "fake-test-bucket",
    "fos_region": "us-east-1",
}

# Shape-accurate stand-in for a real IIFE bundle.
SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"
OTHER_BUNDLE = b"!function(e){/* a different, re-released build */}(window);"

_REAL_CLIENT = httpx.Client


def _mock_client_factory(handler):
    """Patch ``httpx.Client`` so it is constructed with a mock transport.

    Mirrors ``_mock_transport`` in ``test_faro_versions.py`` but for the
    sync client the FOS upload/list/delete calls use.
    """

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_CLIENT(*args, **kwargs)

    return factory


@pytest.fixture
def mock_fos(monkeypatch):
    """Return a callable that installs a MockTransport-backed httpx.Client."""

    def install(handler):
        monkeypatch.setattr(rum_assets.httpx, "Client", _mock_client_factory(handler))

    return install


@pytest.fixture
def mock_fetch_bundle(monkeypatch):
    """Return a callable that stubs ``fetch_faro_bundle`` with fixed bytes."""

    def install(bundle: bytes = SAMPLE_BUNDLE, *, raise_if_called: bool = False):
        async def fake_fetch(version: str) -> bytes:
            if raise_if_called:
                raise AssertionError(f"fetch_faro_bundle must not be called for version {version!r} here")
            return bundle

        monkeypatch.setattr(rum_assets, "fetch_faro_bundle", fake_fetch)

    return install


def _config_pair(monkeypatch, cfg: dict):
    """Patch load_config to return a private copy of ``cfg`` and capture save_config calls."""
    saved: dict = {}

    def fake_load(service_id):
        return dict(cfg)

    def fake_save(service_id, new_cfg):
        saved["service_id"] = service_id
        saved["cfg"] = new_cfg

    monkeypatch.setattr(rum_assets.svcconfig, "load_config", fake_load)
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", fake_save)
    return saved


# ── download_and_upload_faro ────────────────────────────────────────────────


async def test_download_and_upload_faro_success(monkeypatch, mock_fos, mock_fetch_bundle):
    """Happy path: correct FOS key, byte count, and content hash returned;
    config is persisted with the new version + hash."""
    saved = _config_pair(monkeypatch, FAKE_CFG)
    mock_fetch_bundle(SAMPLE_BUNDLE)

    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url)
        seen["body"] = request.content
        return httpx.Response(200)

    mock_fos(handler)

    result = await download_and_upload_faro("svc_test", "2.9.0", "tok")

    expected_hash = hashlib.sha256(SAMPLE_BUNDLE).hexdigest()
    expected_etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    assert result["version"] == "2.9.0"
    assert result["path"] == "rum/faro-web-sdk-v2.9.0.iife.js"
    assert result["bytes_uploaded"] == len(SAMPLE_BUNDLE)
    assert result["content_hash"] == expected_hash
    assert result["fos_key"] == "s3://fake-test-bucket/rum/faro-web-sdk-v2.9.0.iife.js"

    assert seen["method"] == "PUT"
    assert seen["url"].endswith("/fake-test-bucket/rum/faro-web-sdk-v2.9.0.iife.js")
    assert seen["body"] == SAMPLE_BUNDLE

    assert saved["service_id"] == "svc_test"
    assert saved["cfg"]["rum"]["faro_version"] == "2.9.0"
    assert saved["cfg"]["rum"]["faro_content_hash"] == expected_hash
    # Distinct field for the cron's FOS ETag comparison — MD5 specifically,
    # since a single-part PUT's S3/FOS ETag is protocol-mandated MD5.
    assert saved["cfg"]["rum"]["faro_fos_etag_md5"] == expected_etag_md5


async def test_download_and_upload_faro_preserves_existing_rum_keys(monkeypatch, mock_fos, mock_fetch_bundle):
    """Existing cfg["rum"] keys (e.g. an admin-set sampling rate) must survive
    the upload — only faro_version/faro_content_hash get overwritten."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"sampling_rate": 0.5, "faro_version": "2.8.0", "faro_content_hash": "stale"}
    saved = _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda request: httpx.Response(200))

    await download_and_upload_faro("svc_test", "2.9.0", "tok")

    assert saved["cfg"]["rum"]["sampling_rate"] == 0.5
    assert saved["cfg"]["rum"]["faro_version"] == "2.9.0"


async def test_download_and_upload_faro_raises_on_upload_failure(monkeypatch, mock_fos, mock_fetch_bundle):
    """A FOS-side failure must surface as RuntimeError, and must not persist
    a version/hash for a bundle that was never actually stored."""
    saved = _config_pair(monkeypatch, FAKE_CFG)
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda request: httpx.Response(500, content=b"upstream error"))

    with pytest.raises(RuntimeError):
        await download_and_upload_faro("svc_test", "2.9.0", "tok")

    assert saved == {}


# ── detect_faro_version_change ──────────────────────────────────────────────


async def test_detect_faro_version_change_true_on_no_rum_config(monkeypatch, mock_fetch_bundle):
    """Never-provisioned service (no cfg["rum"] at all) always counts as changed."""
    _config_pair(monkeypatch, FAKE_CFG)
    mock_fetch_bundle(raise_if_called=True)

    assert await detect_faro_version_change("svc_test", "2.9.0") is True


async def test_detect_faro_version_change_true_on_version_mismatch(monkeypatch, mock_fetch_bundle):
    """Requested version differs from stored — no bundle download needed to decide."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.8.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()}
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(raise_if_called=True)

    assert await detect_faro_version_change("svc_test", "2.9.0") is True


async def test_detect_faro_version_change_true_on_missing_stored_hash(monkeypatch, mock_fetch_bundle):
    """Version matches but no hash was ever recorded — treat as changed."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.9.0", "faro_content_hash": None}
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(raise_if_called=True)

    assert await detect_faro_version_change("svc_test", "2.9.0") is True


async def test_detect_faro_version_change_true_on_hash_mismatch(monkeypatch, mock_fetch_bundle):
    """Same version string, but upstream re-released the bundle (e.g. a
    security patch) — the freshly-downloaded hash differs from what's stored."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.9.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()}
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(OTHER_BUNDLE)

    assert await detect_faro_version_change("svc_test", "2.9.0") is True


async def test_detect_faro_version_change_false_when_version_and_hash_match(monkeypatch, mock_fetch_bundle):
    """Fully up to date — no reconcile action needed."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.9.0", "faro_content_hash": hashlib.sha256(SAMPLE_BUNDLE).hexdigest()}
    _config_pair(monkeypatch, cfg)
    mock_fetch_bundle(SAMPLE_BUNDLE)

    assert await detect_faro_version_change("svc_test", "2.9.0") is False


# ── cleanup_old_faro_versions ───────────────────────────────────────────────

_LIST_RESPONSE_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>fake-test-bucket</Name>
  <Contents><Key>rum/faro-web-sdk-v2.8.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.9.0.iife.js</Key></Contents>
  <Contents><Key>rum/faro-web-sdk-v2.10.0.iife.js</Key></Contents>
</ListBucketResult>
"""


async def test_cleanup_old_faro_versions_deletes_old_keeps_current(monkeypatch, mock_fos):
    """Old bundle objects are deleted; the version named in
    cfg["rum"]["faro_version"] is left alone."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.10.0", "faro_content_hash": "irrelevant"}
    _config_pair(monkeypatch, cfg)

    deleted_keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            assert "list-type=2" in str(request.url)
            return httpx.Response(200, content=_LIST_RESPONSE_TEMPLATE.encode())
        if request.method == "DELETE":
            deleted_keys.append(request.url.path.split("/", 2)[-1])
            return httpx.Response(204)
        raise AssertionError(f"unexpected method {request.method}")

    mock_fos(handler)

    await cleanup_old_faro_versions("svc_test")

    assert sorted(deleted_keys) == [
        "rum/faro-web-sdk-v2.8.0.iife.js",
        "rum/faro-web-sdk-v2.9.0.iife.js",
    ]


async def test_cleanup_old_faro_versions_deletes_all_when_keep_current_false(monkeypatch, mock_fos):
    """keep_current=False sweeps every faro bundle, including the active one."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.10.0", "faro_content_hash": "irrelevant"}
    _config_pair(monkeypatch, cfg)

    deleted_keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, content=_LIST_RESPONSE_TEMPLATE.encode())
        if request.method == "DELETE":
            deleted_keys.append(request.url.path.split("/", 2)[-1])
            return httpx.Response(204)
        raise AssertionError(f"unexpected method {request.method}")

    mock_fos(handler)

    await cleanup_old_faro_versions("svc_test", keep_current=False)

    assert sorted(deleted_keys) == [
        "rum/faro-web-sdk-v2.10.0.iife.js",
        "rum/faro-web-sdk-v2.8.0.iife.js",
        "rum/faro-web-sdk-v2.9.0.iife.js",
    ]


async def test_cleanup_old_faro_versions_swallows_error(monkeypatch, mock_fos):
    """A FOS listing failure (network blip, transient 5xx) must never raise
    into the caller — cleanup runs after a successful upgrade and must not
    turn that success into a reported failure."""
    cfg = dict(FAKE_CFG)
    cfg["rum"] = {"faro_version": "2.10.0", "faro_content_hash": "irrelevant"}
    _config_pair(monkeypatch, cfg)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("FOS unreachable", request=request)

    mock_fos(handler)

    # Must not raise.
    result = await cleanup_old_faro_versions("svc_test")
    assert result is None
