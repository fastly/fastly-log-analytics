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

import datetime as _dt
import hashlib
import json
import logging
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from backend import config as svcconfig
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.provision import rum_assets
from backend.provision.rum_assets import (
    cleanup_old_faro_versions,
    detect_faro_version_change,
    download_and_upload_faro,
    faro_bundle_intact,
    faro_tracker_ready,
    generate_rum_tracker_js,
    upload_rum_tracker_js,
)
from backend.provision.rum_orchestrator_v2 import disable_rum, enable_rum, rum_vcl_fingerprint, upgrade_faro_version
from tests._helpers.config_fakes import config_lock_held as _lock_held
from tests._helpers.config_fakes import mutate_via_save
from tests._helpers.config_fakes import read_persisted_config as _persisted

# ── Merged from tests/provision/test_rum_assets.py ──
FAKE_CFG = {
    "service_id": "svc_test",
    "fos_access_key_id": "AKIAFAKEEXAMPLE",
    "fos_secret_access_key": "fakeSecretKeyExample",
    "fos_bucket": "fake-test-bucket",
    "fos_region": "us-east-1",
}

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
        monkeypatch.setattr(httpx, "Client", _mock_client_factory(handler))

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
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", mutate_via_save(fake_save))
    return saved


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


async def test_download_and_upload_faro_does_not_recreate_a_deleted_service_config(
    monkeypatch, mock_fos, mock_fetch_bundle
):
    """A service deleted while the bundle was in flight must stay deleted (no config resurrected)."""
    saved: list = []
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(FAKE_CFG))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: None)
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: saved.append(a))
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda request: httpx.Response(200))

    await download_and_upload_faro("svc_test", "2.9.0", "tok")

    assert saved == []


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


async def test_download_and_upload_faro_compare_and_set_skips_stale_pin_write(monkeypatch, mock_fos, mock_fetch_bundle):
    """F-5 audit finding, reproduced end to end: the cron re-syncs whatever
    version it observed as pinned — never a version change — but the
    download-from-unpkg + upload-to-FOS round trip between reading that
    pinned version and persisting config can take long enough for a
    concurrent, explicit ``upgrade_faro_version`` call to land in between.

    Sequence reproduced here:
      1. Config is pinned to 2.9.0 (what the cron read before starting).
      2. An operator upgrade completes concurrently, moving the pin to
         3.0.0 (simulated by mutating the shared config store directly,
         standing in for the real upgrade_faro_version call that would
         have run on another thread/process while this download was in
         flight).
      3. The cron's re-sync for the STALE "2.9.0" it originally observed
         now completes and tries to persist.

    Without the ``expected_current_version`` guard, step 3's write lands
    last and reverts the pin back to 2.9.0 — which then makes
    ``cleanup_old_faro_versions(keep_current=True)`` (run right after the
    real upgrade) compute its "keep" key from the reverted, stale version
    and delete the FOS object the live VCL actually routes to. The pin
    must survive untouched; the bundle bytes for 2.9.0 are still fine to
    have uploaded to their own, separate FOS key.
    """
    import copy

    store: dict[str, dict] = {
        "svc_test": {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_content_hash": "old-hash"}}
    }

    def fake_load(service_id):
        return copy.deepcopy(store.get(service_id))

    def fake_save(service_id, new_cfg):
        store[service_id] = copy.deepcopy(new_cfg)

    monkeypatch.setattr(rum_assets.svcconfig, "load_config", fake_load)
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", mutate_via_save(fake_save))
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda request: httpx.Response(200))

    # Step 2: the concurrent upgrade lands while this call's download+upload
    # is "in flight" (there's nothing to actually await here — the point is
    # that by the time THIS function reloads config to persist, below, the
    # store already reflects the newer pin).
    store["svc_test"]["rum"] = {"faro_version": "3.0.0", "faro_content_hash": "new-hash"}

    # Step 3: the cron's re-sync for the stale "2.9.0" it observed earlier.
    result = await download_and_upload_faro("svc_test", "2.9.0", "tok", expected_current_version="2.9.0")

    # The upload itself still happened (bytes for 2.9.0 landed at their own,
    # separate FOS key) ...
    assert result["version"] == "2.9.0"
    assert result["config_updated"] is False
    # ... but the pin was NOT reverted: the operator's newer pin wins.
    assert store["svc_test"]["rum"]["faro_version"] == "3.0.0"
    assert store["svc_test"]["rum"]["faro_content_hash"] == "new-hash"


async def test_download_and_upload_faro_compare_and_set_writes_when_pin_unchanged(
    monkeypatch, mock_fos, mock_fetch_bundle
):
    """Sanity check for the guard added above: when nothing raced —
    ``expected_current_version`` still matches what's stored — the write
    proceeds normally. Guards against a fix that's so conservative it never
    writes at all."""
    import copy

    store: dict[str, dict] = {
        "svc_test": {**FAKE_CFG, "rum": {"faro_version": "2.9.0", "faro_content_hash": "old-hash"}}
    }

    def fake_load(service_id):
        return copy.deepcopy(store.get(service_id))

    def fake_save(service_id, new_cfg):
        store[service_id] = copy.deepcopy(new_cfg)

    monkeypatch.setattr(rum_assets.svcconfig, "load_config", fake_load)
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", mutate_via_save(fake_save))
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda request: httpx.Response(200))

    result = await download_and_upload_faro("svc_test", "2.9.0", "tok", expected_current_version="2.9.0")

    assert result["config_updated"] is True
    expected_hash = hashlib.sha256(SAMPLE_BUNDLE).hexdigest()
    assert store["svc_test"]["rum"]["faro_content_hash"] == expected_hash


async def test_download_and_upload_faro_raises_on_upload_failure(monkeypatch, mock_fos, mock_fetch_bundle):
    """A FOS-side failure must surface as RuntimeError, and must not persist
    a version/hash for a bundle that was never actually stored."""
    saved = _config_pair(monkeypatch, FAKE_CFG)
    mock_fetch_bundle(SAMPLE_BUNDLE)
    mock_fos(lambda request: httpx.Response(500, content=b"upstream error"))

    with pytest.raises(RuntimeError):
        await download_and_upload_faro("svc_test", "2.9.0", "tok")

    assert saved == {}


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


def test_generate_rum_tracker_js_probes_correct_error_instrumentation_name():
    """The real export in every SDK version we've checked (1.19.0, 2.9.0) is
    ``ErrorsInstrumentation`` (plural). The generated tracker must probe that
    name — regression test for the misspelled singular probe that silently
    disabled JS error tracking."""
    js = generate_rum_tracker_js("svc_test")

    assert "Faro.ErrorsInstrumentation" in js


def test_generate_rum_tracker_js_does_not_reintroduce_the_misspelling():
    """``Faro.ErrorInstrumentation`` (singular) must not appear as the
    primary probe again — only as an explicit, named fallback."""
    js = generate_rum_tracker_js("svc_test")

    # The singular form is allowed to exist ONLY as the documented fallback
    # assigned into ErrorInstrumentationCtor; it must never be the sole/first
    # thing checked in an `if`.
    assert "if (Faro.ErrorInstrumentation)" not in js
    assert "new Faro.ErrorInstrumentation()" not in js


def test_generate_rum_tracker_js_warns_when_no_error_instrumentation_found():
    """Losing both spellings in a future SDK release must surface a
    console.warn, not fail silently like the original bug."""
    js = generate_rum_tracker_js("svc_test")

    assert "console.warn" in js
    assert "no error instrumentation export found" in js


def test_generate_rum_tracker_js_loads_the_sdk_from_the_first_party_path():
    """The whole point of self-hosting: the browser must load the Faro SDK
    from this service's own domain, not a third-party CDN. Regression test
    for the Critical gap where the tracker hardcoded jsDelivr and nothing
    ever consumed the self-hosted bundle uploaded to FOS."""
    js = generate_rum_tracker_js("svc_test")

    assert "script.src = '/js/faro-sdk.js';" in js
    assert "cdn.jsdelivr.net" not in js
    assert "unpkg.com" not in js


def test_generate_rum_tracker_js_contains_no_absolute_url_at_all():
    """Belt-and-braces: no absolute http(s):// URL anywhere in the generated
    tracker, so a future edit can't silently reintroduce a CDN dependency."""
    js = generate_rum_tracker_js("svc_test")

    assert not re.search(r"https?://", js)


def test_faro_tracker_ready_false_when_no_version_pinned():
    """The exact live failure: reconcile succeeded, but faro_version was
    never written to cfg["rum"]. Must not be reported ready — this is the
    case that must fail without the fix."""
    cfg = {**FAKE_CFG, "rum": {"enabled": True}}  # no faro_version key at all

    ready, reason = faro_tracker_ready(cfg)

    assert ready is False
    assert "no Faro version pinned" in reason


def test_faro_tracker_ready_false_when_pinned_but_bundle_missing_in_fos(mock_fos):
    """A version is pinned in config, but the FOS object behind it is gone
    (wiped bucket, interrupted upload, etc.) — HEAD 404s. Must not be
    reported ready."""
    cfg = {
        **FAKE_CFG,
        "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": "deadbeef"},
    }
    mock_fos(lambda request: httpx.Response(404))

    ready, reason = faro_tracker_ready(cfg)

    assert ready is False
    assert "not present/intact in FOS" in reason


def test_faro_tracker_ready_true_when_pinned_and_bundle_present(mock_fos):
    """A version is pinned AND its bundle's ETag matches what FOS actually
    holds — the only case that should be considered ready to publish."""
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    cfg = {
        **FAKE_CFG,
        "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": etag_md5},
    }
    mock_fos(lambda request: httpx.Response(200, headers={"ETag": f'"{etag_md5}"'}))

    ready, reason = faro_tracker_ready(cfg)

    assert ready is True
    assert reason == ""


def test_faro_bundle_intact_is_the_shared_implementation_behind_readiness(mock_fos):
    """faro_bundle_intact lives in rum_assets.py specifically so the cron's
    every-tick integrity check and this readiness gate share one
    implementation instead of drifting — sanity-check it's callable directly
    with the same semantics faro_tracker_ready relies on."""
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    cfg = {**FAKE_CFG, "rum": {"faro_fos_etag_md5": etag_md5}}
    mock_fos(lambda request: httpx.Response(200, headers={"ETag": f'"{etag_md5}"'}))

    assert faro_bundle_intact(cfg, "2.9.0") is True


def test_faro_bundle_intact_transient_head_error_logs_concisely_without_traceback(mock_fos, caplog):
    """A transient FOS HEAD failure (e.g. ConnectError under CPU contention)
    self-heals via the caller's restore path, so it must log a concise
    warning (exception type + message) WITHOUT a full traceback. A rich
    exc_info traceback for a handled, recovered transient is pure log noise
    that also trips the deploy log-error monitor on every contended run."""
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    cfg = {**FAKE_CFG, "rum": {"faro_fos_etag_md5": etag_md5}}

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection failed")

    mock_fos(boom)

    with caplog.at_level(logging.WARNING):
        result = faro_bundle_intact(cfg, "2.9.0")

    assert result is False
    rec = next(r for r in caplog.records if "integrity HEAD check failed" in r.getMessage())
    assert rec.exc_info is None, "handled, self-healing transient must not log a full traceback"
    assert "ConnectError" in rec.getMessage()


def test_upload_rum_tracker_js_skips_publish_when_no_faro_version_pinned(monkeypatch):
    """This is the stranded-tracker scenario end to end through the real
    publish function: a service with RUM config but no faro_version pinned
    (e.g. an activation that never wrote the pin) must NOT get a
    faro-referencing tracker published — the FOS PUT for rum-tracker.js
    must never happen."""
    cfg = {**FAKE_CFG, "rum": {"enabled": True}}
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(cfg))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: {})

    put_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            put_calls.append(str(request.url))
        raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

    monkeypatch.setattr(httpx, "Client", _mock_client_factory(handler))

    result = upload_rum_tracker_js("svc_test", "tok")

    assert result["skipped"] is True
    assert result["bytes_uploaded"] == 0
    assert put_calls == []


def test_upload_rum_tracker_js_skips_publish_when_pinned_but_bundle_missing(monkeypatch):
    """A version is pinned, but the bundle isn't actually in FOS (HEAD
    404s) — must still skip the tracker publish rather than trust the pin
    alone."""
    cfg = {
        **FAKE_CFG,
        "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": "deadbeef"},
    }
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(cfg))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: {})

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD" and "faro-web-sdk-v" in request.url.path:
            return httpx.Response(404)
        raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

    monkeypatch.setattr(httpx, "Client", _mock_client_factory(handler))

    result = upload_rum_tracker_js("svc_test", "tok")

    assert result["skipped"] is True


def test_upload_rum_tracker_js_publishes_when_pinned_and_bundle_present(monkeypatch):
    """The genuinely-ready case: version pinned, bundle's ETag matches FOS —
    the tracker publish must proceed and actually PUT the tracker JS."""
    etag_md5 = hashlib.md5(SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
    cfg = {
        **FAKE_CFG,
        "rum": {"faro_version": "2.9.0", "faro_fos_etag_md5": etag_md5},
    }
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(cfg))
    monkeypatch.setattr(rum_assets.svcconfig, "mutate_config", lambda *a, **k: {})

    put_bodies: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "HEAD" and "faro-web-sdk-v" in path:
            return httpx.Response(200, headers={"ETag": f'"{etag_md5}"'})
        if request.method == "HEAD" and path.endswith("rum-tracker.js"):
            return httpx.Response(404)  # never uploaded before
        if request.method == "PUT" and path.endswith("rum-tracker.js"):
            put_bodies["tracker"] = request.content
            return httpx.Response(200)
        raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

    monkeypatch.setattr(httpx, "Client", _mock_client_factory(handler))

    result = upload_rum_tracker_js("svc_test", "tok")

    assert "skipped" not in result
    assert result["bytes_uploaded"] > 0
    assert "/js/faro-sdk.js" in put_bodies["tracker"].decode()


# ── Merged from tests/provision/test_rum_integration.py ──
@pytest.fixture
def temp_config_dir(tmp_path, monkeypatch):
    """Create a temporary config directory and redirect backend.config to it."""

    from backend import config as svcconfig

    config_dir = Path(tmp_path) / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)

    return config_dir


@pytest.fixture
def sample_service_config():
    """Return a minimal service config with FOS credentials."""
    return {
        "service_id": "test_service_id",
        "service_name": "Test Service",
        "rum_enabled": False,
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "test_access_key",
        "fos_secret_access_key": "test_secret_key",
        "last_activated_version": 1,
    }


@pytest.fixture
def initialized_config(temp_config_dir, sample_service_config):
    """Write sample config to disk and return the service_id."""
    service_id = sample_service_config["service_id"]
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(json.dumps(sample_service_config, indent=2))

    return service_id, config_path


class TestRUMIntegration:
    """End-to-end integration tests for RUM enable/disable flow."""

    def test_enable_rum_full_flow(self, temp_config_dir, initialized_config):
        """Test full enable_rum flow: config write → bucket → upload → reconcile.

        Verifies:
        - result["activated"] is True
        - result["logging_service_active_version"] == 2 (post-activate)
        - "enabled_at" in result
        - Bucket was called once
        - Upload was called once
        - Reconciliation was called once
        - All three stages fired status callbacks
        """
        service_id, config_path = initialized_config
        token = "test_token"
        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
            with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                with patch(
                    "backend.core.faro_versions.fetch_available_faro_versions",
                    new_callable=AsyncMock,
                    return_value=[DEFAULT_FARO_VERSION],
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                        return_value={"version": DEFAULT_FARO_VERSION},
                    ) as mock_download_faro:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            # Setup mocks
                            mock_bucket.return_value = None  # ensure_fos_bucket returns None
                            mock_upload.return_value = {
                                "path": "rum/rum-tracker.js",
                                "bytes_uploaded": 1000,
                                "fos_key": "s3://test-bucket/rum/rum-tracker.js",
                            }
                            mock_reconcile.return_value = MagicMock(
                                activated_version=2,
                                activated_version_set=True,
                                changes_applied={},
                            )

                            # Call enable_rum
                            result = enable_rum(service_id, token, status_cb=mock_status_cb)

                        # Verify config was written with rum_enabled=True and timestamp
                        config_content = config_path.read_text()
                        config = json.loads(config_content)
                        assert config["rum_enabled"] is True
                        assert "rum_enabled_at" in config
                        assert config["service_id"] == service_id
                        # No CDN fallback exists, so enabling RUM without an
                        # explicit faro_version must still pin and upload a
                        # self-hosted bundle rather than leaving it unpinned.
                        assert config["rum"]["faro_version"] == DEFAULT_FARO_VERSION

                        # Verify bucket provisioning was bypassed since bucket was already configured
                        assert mock_bucket.call_count == 0

                        # Verify JS upload was called once
                        assert mock_upload.call_count == 1
                        upload_call = mock_upload.call_args
                        assert upload_call[0][0] == service_id
                        assert upload_call[0][1] == token

                        # Verify the Faro Web SDK bundle was uploaded with the default version
                        mock_download_faro.assert_called_once_with(
                            service_id, DEFAULT_FARO_VERSION, token, status_cb=mock_status_cb
                        )

                        # Verify reconciliation was called once
                        assert mock_reconcile.call_count == 1

                        # Verify result
                        assert result["activated"] is True
                        assert result["logging_service_active_version"] == 2
                        assert "enabled_at" in result

                        # Verify status callbacks fired for all stages
                        assert len(status_messages) >= 3  # At least: bucket, upload, reconcile
                        assert any("Using FOS bucket" in msg for msg in status_messages)
                        assert any("Uploading RUM tracker JS" in msg for msg in status_messages)
                        assert any("Reconciling VCL state" in msg for msg in status_messages)

    def test_disable_rum_full_flow(self, temp_config_dir):
        """Test full disable_rum flow: config write → delete JS → reconcile.

        Verifies:
        - result["deactivated"] is True
        - result["logging_service_active_version"] == 6
        - Delete was called once
        - Reconciliation was called once
        - Status callbacks fired for deletion and reconciliation stages
        """
        # Create config with rum_enabled=True
        config = {
            "service_id": "test_service_id",
            "service_name": "Test Service",
            "rum_enabled": True,
            "rum_enabled_at": _dt.datetime.now(_dt.UTC).isoformat(),
            "fos_bucket": "test-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }
        service_id = config["service_id"]
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(json.dumps(config, indent=2))

        token = "test_token"
        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                # Setup mocks
                mock_delete.return_value = None
                mock_reconcile.return_value = MagicMock(
                    activated_version=6,
                    changes_applied={},
                )

                # Call disable_rum
                result = disable_rum(service_id, token, status_cb=mock_status_cb)

                # Verify config was written with rum_enabled=False
                config_content = config_path.read_text()
                loaded_config = json.loads(config_content)
                assert loaded_config["rum_enabled"] is False

                # Verify JS deletion was called once
                assert mock_delete.call_count == 1
                delete_call = mock_delete.call_args
                assert delete_call[0][0] == service_id
                assert delete_call[0][1] == token

                # Verify reconciliation was called once
                assert mock_reconcile.call_count == 1

                # Verify result
                assert result["deactivated"] is True
                assert result["logging_service_active_version"] == 6

                # Verify status callbacks fired for all stages
                assert len(status_messages) >= 2  # At least: delete, reconcile
                assert any("Deleting RUM tracker JS" in msg for msg in status_messages)
                assert any("Reconciling VCL state" in msg for msg in status_messages)


_SAMPLE_BUNDLE = b"!function(e){var GrafanaFaroWebSdk={initializeFaro:function(){}};e.Faro=GrafanaFaroWebSdk}(window);"

_OTHER_BUNDLE = b"!function(e){/* a different, upgraded build */}(window);"

_REAL_HTTPX_CLIENT = httpx.Client


def _install_mock_transport(monkeypatch, handler) -> None:
    """Patch the shared ``httpx`` module's ``Client`` so every caller in the
    chain (rum_assets' FOS PUT/HEAD, rum_sync's FOS HEAD) is intercepted by
    ``handler`` — no bytes ever leave the process."""

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return _REAL_HTTPX_CLIENT(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", factory)


def _install_mock_fetch_bundle(monkeypatch, bundle: bytes) -> None:
    """Stub the one real network call in the chain that isn't FOS: the
    npm/unpkg download inside ``backend.provision.rum_assets``."""

    async def fake_fetch(version: str) -> bytes:
        return bundle

    monkeypatch.setattr(rum_assets, "fetch_faro_bundle", fake_fetch)


class TestFaroSelfHostingEndToEnd:
    """Task 10 integration coverage: provision with a chosen Faro version,
    upgrade to a different version, and the reconcile cron's self-heal —
    exercised through the real ``download_and_upload_faro``/
    ``upload_rum_tracker_js`` code paths with only FOS/npm/unpkg transport
    mocked, never the functions themselves.
    """

    def test_enable_rum_with_chosen_version_uploads_bundle_and_pins_config(
        self, monkeypatch, temp_config_dir, initialized_config
    ):
        """Provisioning with an explicit (non-default) Faro version must:
        - land the bundle in FOS at rum/faro-web-sdk-v{version}.iife.js
        - persist that exact version to cfg["rum"]["faro_version"]
        - upload a tracker JS that references the relative /js/faro-sdk.js
        """
        service_id, config_path = initialized_config
        chosen_version = "2.8.5"  # deliberately not DEFAULT_FARO_VERSION
        token = "test_token"

        _install_mock_fetch_bundle(monkeypatch, _SAMPLE_BUNDLE)

        uploads: dict[str, bytes] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "PUT" and path.endswith(f"faro-web-sdk-v{chosen_version}.iife.js"):
                uploads["bundle"] = request.content
                return httpx.Response(200)
            if request.method == "HEAD" and path.endswith(f"faro-web-sdk-v{chosen_version}.iife.js"):
                # upload_rum_tracker_js's faro_tracker_ready gate HEADs the
                # bundle it just uploaded to confirm the route it references
                # is genuinely backed by real content in FOS.
                etag = hashlib.md5(_SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
                return httpx.Response(200, headers={"ETag": f'"{etag}"'})
            if request.method == "HEAD" and path.endswith("rum-tracker.js"):
                # Never uploaded before — force upload_rum_tracker_js past
                # its up-to-date short-circuit.
                return httpx.Response(404)
            if request.method == "PUT" and path.endswith("rum-tracker.js"):
                uploads["tracker"] = request.content
                return httpx.Response(200)
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)

        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
            mock_reconcile.return_value = MagicMock(
                activated_version=2,
                draft_version=None,
                changes_applied={},
            )

            result = enable_rum(service_id, token, faro_version=chosen_version)

        # 1. Config persists the *chosen* version, not the default.
        config = json.loads(config_path.read_text())
        assert config["rum"]["faro_version"] == chosen_version
        assert config["rum_enabled"] is True

        # 2. The bundle actually landed at the version-specific FOS key.
        assert uploads["bundle"] == _SAMPLE_BUNDLE

        # 3. The tracker JS references the relative, first-party path...
        tracker_js = uploads["tracker"].decode()
        assert "/js/faro-sdk.js" in tracker_js
        # ...and contains no external hostname anywhere (the critical
        # invariant — this must be impossible to silently regress).
        assert not re.search(r"https?://", tracker_js)
        assert "cdn.jsdelivr.net" not in tracker_js

        # 4. VCL was reconciled exactly once, and the result reports success.
        assert mock_reconcile.call_count == 1
        assert result["activated"] is True

    def test_generated_tracker_js_has_no_external_hostname_end_to_end(self, monkeypatch, temp_config_dir):
        """Dedicated regression test for the user's core requirement ("no
        external dependencies, everything served from the main service").
        Runs the real enable_rum → upload_rum_tracker_js chain (not just
        generate_rum_tracker_js in isolation) and asserts on the exact bytes
        that would be written to FOS and served to browsers.
        """
        service_id = "no_cdn_service"
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(
            json.dumps(
                {
                    "service_id": service_id,
                    "service_name": "No CDN Service",
                    "rum_enabled": False,
                    "fos_bucket": "test-bucket",
                    "fos_region": "us-east-1",
                    "fos_access_key_id": "test_access_key",
                    "fos_secret_access_key": "test_secret_key",
                    "last_activated_version": 1,
                },
                indent=2,
            )
        )

        _install_mock_fetch_bundle(monkeypatch, _SAMPLE_BUNDLE)

        captured_tracker_js: dict[str, bytes] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "PUT" and "faro-web-sdk-v" in path:
                return httpx.Response(200)
            if request.method == "HEAD" and "faro-web-sdk-v" in path:
                # upload_rum_tracker_js's faro_tracker_ready gate HEADs the
                # bundle it just uploaded to confirm the route it references
                # is genuinely backed by real content in FOS.
                etag = hashlib.md5(_SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()
                return httpx.Response(200, headers={"ETag": f'"{etag}"'})
            if request.method == "HEAD" and path.endswith("rum-tracker.js"):
                return httpx.Response(404)
            if request.method == "PUT" and path.endswith("rum-tracker.js"):
                captured_tracker_js["body"] = request.content
                return httpx.Response(200)
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)

        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None, changes_applied={})
            enable_rum(service_id, "test_token")

        tracker_js = captured_tracker_js["body"].decode()
        assert tracker_js  # sanity: something was actually captured
        assert not re.search(r"https?://", tracker_js), "tracker JS must contain no absolute URL at all"
        assert "cdn.jsdelivr.net" not in tracker_js

    def test_upgrade_faro_version_uploads_new_bundle_and_reconciles(self, monkeypatch, temp_config_dir):
        """upgrade_faro_version to a different pinned version must upload the
        new bundle to its own FOS key, persist the new version to config,
        and invoke VCL reconcile — without disturbing the tracker JS (which
        never changes across a version bump)."""
        service_id = "upgrade_service"
        old_version = "2.8.5"
        new_version = "2.9.0"
        old_content_hash = hashlib.sha256(_SAMPLE_BUNDLE).hexdigest()
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(
            json.dumps(
                {
                    "service_id": service_id,
                    "service_name": "Upgrade Service",
                    "rum_enabled": True,
                    "rum_enabled_at": _dt.datetime.now(_dt.UTC).isoformat(),
                    "fos_bucket": "test-bucket",
                    "fos_region": "us-east-1",
                    "fos_access_key_id": "test_access_key",
                    "fos_secret_access_key": "test_secret_key",
                    "last_activated_version": 2,
                    "rum": {
                        "faro_version": old_version,
                        "faro_content_hash": old_content_hash,
                    },
                },
                indent=2,
            )
        )

        _install_mock_fetch_bundle(monkeypatch, _OTHER_BUNDLE)

        uploads: dict[str, bytes] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if request.method == "PUT" and path.endswith(f"faro-web-sdk-v{new_version}.iife.js"):
                uploads["new_bundle"] = request.content
                return httpx.Response(200)
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)

        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
            mock_reconcile.return_value = MagicMock(activated_version=3, draft_version=None, changes_applied={})
            # Cleanup of stale bundle versions is a best-effort, orthogonal
            # concern covered by tests/provision/test_rum_assets.py;
            # stub it here so this test stays focused on the upload+config+
            # reconcile chain this task cares about.
            with patch(
                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions", new_callable=AsyncMock
            ) as mock_cleanup:
                result = upgrade_faro_version(service_id, new_version, "test_token")

        # The new bundle landed at the new version's own FOS key.
        assert uploads["new_bundle"] == _OTHER_BUNDLE

        # Config now reflects the new version (and the old one is gone).
        config = json.loads(config_path.read_text())
        assert config["rum"]["faro_version"] == new_version
        assert config["rum"]["faro_content_hash"] == hashlib.sha256(_OTHER_BUNDLE).hexdigest()

        # VCL reconcile ran exactly once, and old-version cleanup was invoked.
        assert mock_reconcile.call_count == 1
        mock_cleanup.assert_called_once_with(service_id, keep_current=True)

        assert result["previous_version"] == old_version
        assert result["version"] == new_version
        assert result["activated"] is True

    def test_reconcile_cron_self_heals_unpinned_service_without_thrashing(self, monkeypatch, temp_config_dir):
        """A RUM-enabled service with no pinned Faro version at all (the
        pre-Task-2 state) must, on the first cron tick, adopt
        DEFAULT_FARO_VERSION, upload a bundle, persist the pin, AND
        reconcile the deployed VCL (F-4) — then on the very next tick do
        nothing further (no re-download, no re-upload, no re-reconcile)
        because it has converged.

        Also the "post-self-heal cron state" half of the missing invariant
        (VCL/FOS audit finding): once faro_version is pinned, the generated
        VCL for this service's config must actually contain a
        /js/faro-sdk.js route — combined with the FOS PUT assertion below
        (the object exists), this is the full invariant. Before the F-4 fix,
        the self-heal uploaded the bundle but never reconciled, so a
        service in this exact state would have a pinned version with NO
        route pointing at it."""
        service_id = "self_heal_service"
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(
            json.dumps(
                {
                    "service_id": service_id,
                    "service_name": "Self Heal Service",
                    "rum_enabled": True,
                    "log_period": 60,
                    "fos_bucket": "test-bucket",
                    "fos_region": "us-east-1",
                    "fos_access_key_id": "test_access_key",
                    "fos_secret_access_key": "test_secret_key",
                    "fastly_api_key": "test_token",
                    "last_activated_version": 2,
                    "rum": {},
                },
                indent=2,
            )
        )

        _install_mock_fetch_bundle(monkeypatch, _SAMPLE_BUNDLE)
        expected_etag = hashlib.md5(_SAMPLE_BUNDLE, usedforsecurity=False).hexdigest()

        put_count = 0
        head_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal put_count, head_count
            path = request.url.path
            if request.method == "PUT" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in path:
                put_count += 1
                return httpx.Response(200)
            if request.method == "HEAD" and f"faro-web-sdk-v{DEFAULT_FARO_VERSION}.iife.js" in path:
                head_count += 1
                return httpx.Response(200, headers={"ETag": f'"{expected_etag}"'})
            raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

        _install_mock_transport(monkeypatch, handler)
        # Freeze time so the throttled upstream-drift check runs exactly
        # once (last_check starts at 0) and stays inside the window on the
        # second tick, keeping the "no thrashing" assertion deterministic.
        frozen_time = 1_800_000_000.0
        monkeypatch.setattr(rum_sync_mod.time, "time", lambda: frozen_time)

        # The self-heal's one-time VCL reconcile (F-4) would otherwise clone/
        # validate/activate a real Fastly service version — mock it, and
        # record calls so "one-time, not per-tick" is actually verified.
        reconcile_calls: list[tuple] = []

        def fake_reconcile(service_id_arg, token_arg, *args, **kwargs):
            reconcile_calls.append((service_id_arg, token_arg))
            return MagicMock(activated_version=3, draft_version=None)

        monkeypatch.setattr("backend.provision.declarative.reconciler.reconcile_vcl_state", fake_reconcile)
        # The surrogate-key purge after adoption would otherwise hit the
        # real Fastly API (this config has a real-shaped fastly_api_key).
        purge_calls: list[tuple] = []
        monkeypatch.setattr(
            "backend.core.fastly.client.fastly",
            lambda method, path, *, token, expect_empty=False, **kw: purge_calls.append((method, path, token)),
        )

        # Tick 1: self-heal adoption.
        rum_sync_mod._reconcile_faro_bundle(service_id, None)

        config = json.loads(config_path.read_text())
        assert config["rum"]["faro_version"] == DEFAULT_FARO_VERSION
        assert put_count == 1
        assert head_count == 1
        assert reconcile_calls == [(service_id, "test_token")]
        assert purge_calls == [("POST", f"/service/{service_id}/purge/rum-faro-sdk", "test_token")]

        # Missing-invariant check: faro_version is now pinned, so the VCL
        # generated from this exact on-disk config must contain the
        # /js/faro-sdk.js route — combined with put_count == 1 above (the
        # FOS object exists), this is the full "pinned => route AND object"
        # invariant on the post-self-heal cron state.
        from backend.provision.declarative.generators import generate_consolidated_snippet
        from backend.provision.declarative.state import FeatureState

        state = FeatureState.from_config(config)
        recv_vcl = generate_consolidated_snippet(state, "vcl_recv")
        assert '"/js/faro-sdk.js"' in recv_vcl

        # Tick 2: already converged — no further FOS PUT, re-adoption, or
        # re-reconcile.
        rum_sync_mod._reconcile_faro_bundle(service_id, None)

        assert put_count == 1, "self-heal must not thrash on the next tick"
        assert reconcile_calls == [(service_id, "test_token")], "reconcile must not re-run once converged"
        config_after = json.loads(config_path.read_text())
        assert config_after["rum"]["faro_version"] == DEFAULT_FARO_VERSION


# ── Merged from tests/provision/test_rum_orchestrator_v2.py ──
def _seed(cfg: dict) -> None:
    svcconfig.save_config(cfg["service_id"], dict(cfg))


def _disk(service_id: str = "srv_test") -> dict:
    return json.loads(svcconfig.config_path(service_id).read_text())


_FOS_CFG = {
    "fos_bucket": "my-bucket",
    "fos_region": "us-east-1",
    "fos_access_key_id": "test_access_key",
    "fos_secret_access_key": "test_secret_key",
}


class TestEnableRumReusesConfiguredBucket:
    """enable_rum() reuses the FOS bucket already in config; it never provisions one.

    The imperative ensure_fos_bucket step was replaced by declarative
    reconciliation in c65c9da7: the bucket comes from request logging (or a
    prior RUM enable), and a missing bucket/credential is a hard error before
    any state is saved.
    """

    def test_enable_rum_uses_configured_bucket_without_provisioning(self):
        service_id = "srv_test"
        cfg = {"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG}
        status_messages: list[str] = []

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ) as mock_faro:
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                                mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)

                                result = enable_rum(service_id, "test_token", status_cb=status_messages.append)

        mock_bucket.assert_not_called()
        mock_faro.assert_called_once()
        mock_reconcile.assert_called_once()
        mock_upload.assert_called_once()
        assert result["logging_service_active_version"] == 2
        assert result["activated"] is True
        assert any("Using FOS bucket 'my-bucket'" in m for m in status_messages)

    def test_enable_rum_already_enabled_returns_early(self):
        """Verify early return if RUM already enabled."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "rum_enabled_at": "2026-08-05T10:00:00+00:00",
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        mock_load.return_value = cfg

                        # Call
                        result = enable_rum(service_id, "test_token")

                        # Verify bucket/upload/reconcile were NOT called
                        mock_bucket.assert_not_called()
                        mock_upload.assert_not_called()
                        mock_reconcile.assert_not_called()

                        # Verify result
                        assert result["activated"] is False
                        assert result["logging_service_active_version"] == 5


class TestEnableRumRollbackOnBucketFailure:
    """enable_rum() fails closed before saving anything when FOS settings are absent."""

    @pytest.mark.parametrize("missing", ["fos_bucket", "fos_region", "fos_access_key_id", "fos_secret_access_key"])
    def test_enable_rum_missing_single_fos_setting_raises_before_any_save(self, missing):
        service_id = "srv_test"
        cfg = {"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG}
        del cfg[missing]

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                    ) as mock_faro:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            with pytest.raises(RuntimeError, match="missing FOS configuration"):
                                enable_rum(service_id, "test_token")

        mock_save.assert_not_called()
        mock_faro.assert_not_called()
        mock_upload.assert_not_called()
        mock_reconcile.assert_not_called()
        assert cfg["rum_enabled"] is False

    def test_enable_rum_rollback_on_faro_bundle_upload_failure(self):
        """Verify config is rolled back if the Faro bundle upload fails.

        The bundle upload runs BEFORE reconcile and is still a blocking
        step (the VCL the reconciler is about to activate must not route to
        a bundle that was never uploaded), so a failure here must still
        roll back and never reach reconcile.
        """
        service_id = "srv_test"
        _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload,
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                side_effect=RuntimeError("Faro bundle upload failed"),
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            mock_reconcile.return_value = MagicMock(activated_version=2)

            with pytest.raises(RuntimeError, match="Faro bundle upload failed"):
                enable_rum(service_id, "test_token")

            # Verify reconcile was NOT called, and the tracker JS was never
            # uploaded (it only runs after a successful reconcile)
            mock_reconcile.assert_not_called()
            mock_upload.assert_not_called()

        saved = _disk(service_id)
        assert saved["rum_enabled"] is False
        assert "rum_vcl_sha" not in saved

    def test_enable_rum_tracker_upload_failure_after_reconcile_is_non_blocking(self):
        """Verify a tracker-JS upload failure AFTER a successful reconcile does
        NOT raise and does NOT roll back config.

        This is the ordering fix's core safety property: by the time the
        tracker upload runs, the Fastly side has already activated
        successfully and is live. Rolling back local config at that point
        would desync it from reality (Fastly thinks RUM is enabled; config
        would claim it isn't) for a problem that self-heals on the next
        reconcile/cron tick.
        """
        service_id = "srv_test"
        _seed({"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG})

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload,
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": "2.9.0"},
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            mock_upload.side_effect = RuntimeError("FOS unreachable")
            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)

            # Must NOT raise.
            result = enable_rum(service_id, "test_token")

            mock_reconcile.assert_called_once()
            mock_upload.assert_called_once()

        # No rollback: config still says enabled.
        assert _disk(service_id)["rum_enabled"] is True
        assert result["activated"] is True
        assert result["logging_service_active_version"] == 2

    def test_enable_rum_no_config_raises_error(self):
        """Verify RuntimeError is raised if service config not found."""
        service_id = "srv_nonexistent"

        with patch("backend.config.load_config") as mock_load:
            mock_load.return_value = None

            with pytest.raises(RuntimeError, match="No config found"):
                enable_rum(service_id, "test_token")


class TestEnableRumUploadArgs:
    """Test that enable_rum() calls upload_rum_tracker_js with correct arguments."""

    def test_enable_rum_upload_called_with_correct_args(self):
        """Verify upload_rum_tracker_js is called with service_id, token, and status_cb."""
        service_id = "srv_test"
        token = "fastly_test_token_12345"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_load.return_value = cfg
                                mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                                mock_reconcile.return_value = MagicMock(activated_version=2)

                                # Call
                                enable_rum(service_id, token, status_cb=mock_status_cb)

                                # Verify upload was called with correct arguments
                                mock_upload.assert_called_once_with(service_id, token, status_cb=mock_status_cb)

    def test_enable_rum_upload_called_without_status_cb(self):
        """Verify upload_rum_tracker_js is called with status_cb=None when no callback provided."""
        service_id = "srv_test"
        token = "fastly_test_token_12345"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_load.return_value = cfg
                                mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                                mock_reconcile.return_value = MagicMock(activated_version=2)

                                # Call without status_cb
                                enable_rum(service_id, token)

                                # Verify upload was called with status_cb=None
                                mock_upload.assert_called_once_with(service_id, token, status_cb=None)


class TestEnableRumStatusCallback:
    """status_cb is threaded through every enable step for UI updates."""

    def test_enable_rum_passes_status_cb_to_every_step(self):
        service_id = "srv_test"
        cfg = {"service_id": service_id, "rum_enabled": False, "last_activated_version": 1, **_FOS_CFG}

        def status_cb(msg):
            return None

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        new_callable=AsyncMock,
                    ) as mock_faro:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            mock_upload.return_value = {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}
                            mock_reconcile.return_value = MagicMock(activated_version=2, draft_version=None)

                            result = enable_rum(service_id, "test_token", status_cb=status_cb)

        assert mock_faro.call_args.kwargs["status_cb"] is status_cb
        assert mock_reconcile.call_args.kwargs["status_cb"] is status_cb
        assert mock_upload.call_args.kwargs["status_cb"] is status_cb
        assert result["activated"] is True


class TestDisableRumDeletesJs:
    """Test that disable_rum() calls JS deletion before reconciliation."""

    def test_disable_rum_deletes_js(self):
        """Verify JS deletion is called after config update but before reconciliation."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config") as mock_write:
                with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        # Setup
                        mock_load.return_value = cfg
                        mock_delete.return_value = None
                        mock_reconcile.return_value = MagicMock(activated_version=6)

                        # Call
                        result = disable_rum(service_id, "test_token", status_cb=None)

                        # Verify delete was called with correct args
                        mock_delete.assert_called_once_with(service_id, "test_token", status_cb=None)

                        # Verify reconcile was called after delete
                        assert mock_reconcile.called

                        # Verify result
                        assert result["logging_service_active_version"] == 6
                        assert result["deactivated"] is True

    def test_disable_rum_already_disabled_returns_early(self):
        """Verify early return if RUM already disabled (no JS deletion)."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                    mock_load.return_value = cfg

                    # Call
                    result = disable_rum(service_id, "test_token")

                    # Verify delete/reconcile were NOT called
                    mock_delete.assert_not_called()
                    mock_reconcile.assert_not_called()

                    # Verify result
                    assert result["deactivated"] is False
                    assert result["logging_service_active_version"] == 5

    def test_disable_rum_with_status_cb(self):
        """Verify status_cb is passed through for UI updates."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }

        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.config.load_config") as mock_load:
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        mock_load.return_value = cfg
                        mock_delete.return_value = None
                        mock_reconcile.return_value = MagicMock(activated_version=6)

                        # Call
                        disable_rum(service_id, "test_token", status_cb=mock_status_cb)

                        # Verify status_cb was called with delete status
                        mock_delete.assert_called_once()
                        call_args = mock_delete.call_args
                        assert call_args.kwargs["status_cb"] == mock_status_cb


class TestDisableRumContinuesOnDeleteFailure:
    """Test that disable_rum() continues reconciliation if JS deletion fails (non-blocking)."""

    def test_disable_rum_continues_on_delete_failure(self):
        """Verify that if JS deletion fails, VCL reconciliation still proceeds."""
        service_id = "srv_test"
        _seed({"service_id": service_id, "rum_enabled": True, "last_activated_version": 5, **_FOS_CFG})

        with (
            patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete,
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            # Setup: delete fails but reconcile still succeeds
            mock_delete.side_effect = RuntimeError("FOS bucket unreachable")
            mock_reconcile.return_value = MagicMock(activated_version=6)

            # Call and expect NO exception (non-blocking)
            result = disable_rum(service_id, "test_token")

            # Verify delete was called (and failed)
            mock_delete.assert_called_once()

            # Verify reconcile was STILL called (non-blocking)
            mock_reconcile.assert_called_once()

            # Verify result is valid (didn't rollback)
            assert result["logging_service_active_version"] == 6
            assert result["deactivated"] is True

        # Config was NOT rolled back (non-blocking): it stays disabled.
        assert _disk(service_id)["rum_enabled"] is False


class TestRumOrchestratorBypassActivation:
    """Test that enable_rum() and disable_rum() respect the activate=False parameter."""

    def test_enable_rum_bypasses_activation(self):
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                # Reconciler returned a draft version but no activated version
                                mock_reconcile.return_value = MagicMock(activated_version=None, draft_version=3)

                                result = enable_rum(service_id, "test_token", activate=False)

                                mock_reconcile.assert_called_once_with(
                                    service_id, "test_token", dry_run=False, status_cb=None, activate=False
                                )
                                assert result["logging_service_active_version"] == 3
                                assert result["activated"] is False

    def test_disable_rum_bypasses_activation(self):
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_assets.delete_rum_tracker_js"):
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                        # Reconciler returned a draft version but no activated version
                        mock_reconcile.return_value = MagicMock(activated_version=None, draft_version=6)

                        result = disable_rum(service_id, "test_token", activate=False)

                        mock_reconcile.assert_called_once_with(
                            service_id, "test_token", dry_run=False, status_cb=None, activate=False
                        )
                        assert result["logging_service_active_version"] == 6
                        assert result["deactivated"] is False


class TestEnableRumWithFaroVersion:
    """Test enable_rum()'s optional faro_version parameter."""

    def _cfg(self, service_id="srv_test", **overrides):
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }
        cfg.update(overrides)
        return cfg

    def test_enable_rum_faro_version_uploads_bundle_before_reconcile_and_tracker_after(self):
        """Pin the ordering, not just the outcome: the Faro bundle must upload
        BEFORE reconcile (the VCL about to be activated routes to it), and the
        tracker JS must upload AFTER reconcile (the tracker's dependency route
        only exists once that reconcile has activated) — never the reverse,
        which is the exact ordering hazard this fix closes."""
        service_id = "srv_test"
        cfg = self._cfg(service_id)
        call_order = []

        def mock_upload(*args, **kwargs):
            call_order.append("upload_js")
            return {"path": "rum/rum-tracker.js", "bytes_uploaded": 1000}

        async def mock_download_upload(*args, **kwargs):
            call_order.append("upload_faro")
            return {"version": "2.9.0", "bytes_uploaded": 5000}

        def mock_reconcile(*args, **kwargs):
            call_order.append("reconcile")
            return MagicMock(activated_version=2, draft_version=None)

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js", side_effect=mock_upload):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                        side_effect=mock_download_upload,
                    ):
                        with patch(
                            "backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile
                        ):
                            result = enable_rum(service_id, "test_token", faro_version="2.9.0")

        assert call_order == ["upload_faro", "reconcile", "upload_js"]
        assert result["activated"] is True

    def test_enable_rum_faro_version_persisted_before_reconcile(self):
        """Verify cfg["rum"]["faro_version"] is on disk by the time reconcile_vcl_state
        runs, since the generator reads it from the saved config."""
        service_id = "srv_test"
        _seed(self._cfg(service_id))

        def mock_reconcile(*args, **kwargs):
            # At the moment reconcile runs, the pinned version must already be persisted.
            assert _disk(service_id)["rum"]["faro_version"] == "2.9.0"
            return MagicMock(activated_version=2, draft_version=None)

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": "2.9.0"},
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile),
        ):
            enable_rum(service_id, "test_token", faro_version="2.9.0")

    def test_enable_rum_invalid_faro_version_rejected_before_any_work(self):
        """Verify a malformed faro_version is rejected before config is even loaded."""
        with patch("backend.config.load_config") as mock_load:
            with pytest.raises(ValueError, match="faro_version must be a plain"):
                enable_rum("srv_test", "test_token", faro_version="not-a-version")
            mock_load.assert_not_called()

    def test_enable_rum_faro_upload_failure_restores_previous_faro_version(self):
        """Verify config is rolled back to the pre-attempt faro_version (and
        rum_enabled=False) if the Faro bundle upload fails, without erasing a
        key another writer added while the upload was in flight."""
        service_id = "srv_test"
        _seed(self._cfg(service_id, rum={"faro_version": "1.0.0"}))

        async def _fail_after_concurrent_write(*_a, **_kw):
            svcconfig.mutate_config(service_id, lambda c: c.update(unrelated_marker="present-after-reload"))
            raise RuntimeError("unpkg unreachable")

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                side_effect=_fail_after_concurrent_write,
            ),
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile,
        ):
            with pytest.raises(RuntimeError, match="unpkg unreachable"):
                enable_rum(service_id, "test_token", faro_version="2.0.0")
            mock_reconcile.assert_not_called()

        last_saved = _disk(service_id)
        assert last_saved["rum_enabled"] is False
        assert last_saved["rum"]["faro_version"] == "1.0.0"
        assert last_saved["unrelated_marker"] == "present-after-reload"

    def test_enable_rum_faro_reconcile_failure_restores_previous_faro_version(self):
        """Verify config is rolled back to the pre-attempt faro_version if
        reconcile_vcl_state fails after the bundle was already uploaded,
        keeping a concurrent writer's key."""
        service_id = "srv_test"
        _seed(self._cfg(service_id, rum={"faro_version": "1.0.0"}))

        def _fail_after_concurrent_write(*_a, **_kw):
            svcconfig.mutate_config(service_id, lambda c: c.update(unrelated_marker="present-after-reload"))
            raise RuntimeError("VCL validation failed")

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": "2.0.0"},
            ),
            patch(
                "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                side_effect=_fail_after_concurrent_write,
            ),
        ):
            with pytest.raises(RuntimeError, match="VCL validation failed"):
                enable_rum(service_id, "test_token", faro_version="2.0.0")

        last_saved = _disk(service_id)
        assert last_saved["rum_enabled"] is False
        assert last_saved["rum"]["faro_version"] == "1.0.0"
        assert last_saved["unrelated_marker"] == "present-after-reload"

    def test_enable_rum_missing_fos_config_raises_before_any_save(self):
        """Verify the FOS bucket/credentials check runs BEFORE any config
        mutation is persisted (task-6 review Important finding) — cfg["rum"]
        (and rum_enabled) must be left exactly as it was on disk when FOS
        isn't configured yet, not just "an exception propagated"."""
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "rum": {"faro_version": "1.0.0"},
            "last_activated_version": 1,
            # Deliberately missing fos_bucket / fos_region / fos_access_key_id /
            # fos_secret_access_key.
        }
        original_rum_block = dict(cfg["rum"])

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config") as mock_save:
                with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                    with patch(
                        "backend.provision.rum_orchestrator_v2.download_and_upload_faro"
                    ) as mock_download_upload:
                        with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                            with pytest.raises(RuntimeError, match="missing FOS configuration"):
                                enable_rum(service_id, "test_token", faro_version="2.0.0")

        # Nothing was ever persisted, so cfg on "disk" (our mock) is provably
        # unchanged — not just that the exception propagated.
        mock_save.assert_not_called()
        mock_upload.assert_not_called()
        mock_download_upload.assert_not_called()
        mock_reconcile.assert_not_called()
        assert cfg["rum"] == original_rum_block
        assert cfg["rum_enabled"] is False

    def test_enable_rum_without_faro_version_defaults_to_default_faro_version(self):
        """Omitting faro_version must NOT leave RUM enabled-but-unpinned:
        the tracker JS unconditionally loads /js/faro-sdk.js with no CDN
        fallback, so enabling RUM must always pin and upload a bundle."""
        service_id = "srv_test"
        _seed(self._cfg(service_id))
        pinned_at_reconcile = []

        def mock_reconcile(*args, **kwargs):
            pinned_at_reconcile.append(_disk(service_id)["rum"]["faro_version"])
            return MagicMock(activated_version=2, draft_version=None)

        with (
            patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"),
            patch(
                "backend.core.faro_versions.fetch_available_faro_versions",
                new_callable=AsyncMock,
                return_value=[DEFAULT_FARO_VERSION],
            ),
            patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                return_value={"version": DEFAULT_FARO_VERSION},
            ) as mock_download_upload,
            patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile),
        ):
            enable_rum(service_id, "test_token")

        mock_download_upload.assert_called_once_with(service_id, DEFAULT_FARO_VERSION, "test_token", status_cb=None)
        assert pinned_at_reconcile == [DEFAULT_FARO_VERSION]


class TestUpgradeFaroVersion:
    """Test upgrade_faro_version() orchestration."""

    def _cfg(self, service_id="srv_test", faro_version="1.0.0", **overrides):
        cfg = {
            "service_id": service_id,
            "rum_enabled": True,
            "cdn_service_id": "cdn_test_service",
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "rum": {"faro_version": faro_version, "faro_content_hash": "abc123"},
            "last_activated_version": 5,
        }
        cfg.update(overrides)
        return cfg

    def test_upgrade_faro_version_happy_path(self):
        """Verify the full sequence: upload -> reconcile -> purge -> cleanup,
        and that the result reports both the old and new version."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")
        call_order = []

        async def mock_download_upload(*args, **kwargs):
            call_order.append("upload")
            return {"version": "2.0.0"}

        def mock_reconcile(*args, **kwargs):
            call_order.append("reconcile")
            return MagicMock(activated_version=7, draft_version=None)

        async def mock_cleanup(*args, **kwargs):
            call_order.append("cleanup")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    side_effect=mock_download_upload,
                ):
                    with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state", side_effect=mock_reconcile):
                        with patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key") as mock_purge:
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                side_effect=mock_cleanup,
                            ):
                                result = upgrade_faro_version(service_id, "2.0.0", "test_token")

        assert call_order == ["upload", "reconcile", "cleanup"]
        mock_purge.assert_called_once()
        assert result["previous_version"] == "1.0.0"
        assert result["version"] == "2.0.0"
        assert result["logging_service_active_version"] == 7
        assert result["activated"] is True

    def test_upgrade_faro_version_invalid_version_rejected_before_any_work(self):
        """Verify a malformed version is rejected before config is even loaded."""
        with patch("backend.config.load_config") as mock_load:
            with pytest.raises(ValueError, match="faro_version must be a plain"):
                upgrade_faro_version("srv_test", "not-a-version", "test_token")
            mock_load.assert_not_called()

    def test_upgrade_faro_version_refused_when_rum_not_enabled(self):
        """Verify upgrade is refused with a clear error if RUM isn't enabled."""
        service_id = "srv_test"
        cfg = self._cfg(service_id)
        cfg["rum_enabled"] = False

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro") as mock_download_upload:
                with pytest.raises(RuntimeError, match="RUM is not enabled"):
                    upgrade_faro_version(service_id, "2.0.0", "test_token")
                mock_download_upload.assert_not_called()

    def test_upgrade_faro_version_upload_failure_reraises_without_reconcile(self):
        """Verify an upload failure re-raises and never reaches reconcile."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch(
                "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                new_callable=AsyncMock,
                side_effect=RuntimeError("unpkg unreachable"),
            ):
                with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                    with pytest.raises(RuntimeError, match="unpkg unreachable"):
                        upgrade_faro_version(service_id, "2.0.0", "test_token")
                    mock_reconcile.assert_not_called()

    def test_upgrade_faro_version_reconcile_failure_restores_previous_version(self):
        """Verify config is rolled back to the previous faro_version (and its
        content hash) if reconcile fails after the new bundle was uploaded."""
        service_id = "srv_test"
        _seed(self._cfg(service_id, faro_version="1.0.0"))

        async def _persist_new_pin(*_a, **_kw):
            # download_and_upload_faro persists the new pin itself on success.
            def _apply(c):
                c["rum"].update(faro_version="2.0.0", faro_content_hash="new-hash")

            svcconfig.mutate_config(service_id, _apply)
            return {"version": "2.0.0"}

        with (
            patch("backend.provision.rum_orchestrator_v2.download_and_upload_faro", side_effect=_persist_new_pin),
            patch(
                "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                side_effect=RuntimeError("VCL validation failed"),
            ),
        ):
            with pytest.raises(RuntimeError, match="VCL validation failed"):
                upgrade_faro_version(service_id, "2.0.0", "test_token")

        saved_cfg = _disk(service_id)
        assert saved_cfg["rum"]["faro_version"] == "1.0.0"
        assert saved_cfg["rum"]["faro_content_hash"] == "abc123"

    def test_upgrade_faro_version_purge_failure_does_not_fail_upgrade(self):
        """Verify a surrogate-key purge failure is a warning, not an upgrade failure."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        return_value=MagicMock(activated_version=7, draft_version=None),
                    ):
                        with patch(
                            "backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key",
                            side_effect=RuntimeError("purge API unreachable"),
                        ):
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                new_callable=AsyncMock,
                            ):
                                result = upgrade_faro_version(service_id, "2.0.0", "test_token")

        assert result["version"] == "2.0.0"
        assert result["activated"] is True

    def test_upgrade_faro_version_never_uploads_tracker_js(self):
        """Verify upgrade_faro_version never calls upload_rum_tracker_js.

        The tracker body is a static wrapper that always loads the constant
        path /js/faro-sdk.js regardless of pinned version (see
        generate_rum_tracker_js's docstring) — a version bump has nothing
        new to publish there. This pins that claim as a regression test
        rather than leaving it as an unverified assumption.
        """
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        return_value=MagicMock(activated_version=7, draft_version=None),
                    ):
                        with patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key"):
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                new_callable=AsyncMock,
                            ):
                                with patch(
                                    "backend.provision.rum_orchestrator_v2.upload_rum_tracker_js"
                                ) as mock_upload_tracker:
                                    upgrade_faro_version(service_id, "2.0.0", "test_token")

        mock_upload_tracker.assert_not_called()

    def test_upgrade_faro_version_activate_false_skips_cleanup_and_sha_update(self):
        """F-6 audit finding: with activate=False, the draft is compiled and
        validated but never went live — the OLD bundle is still what the
        live VCL serves. cleanup_old_faro_versions(keep_current=True) reads
        the NEW (not-yet-live) pin from config and would delete the OLD,
        still-live bundle if run here; the stored rum_vcl_sha must likewise
        stay pointed at the deployed (old) VCL, not the compiled-but-inactive
        draft."""
        service_id = "srv_test"
        cfg = self._cfg(service_id, faro_version="1.0.0")

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config") as mock_save:
                with patch(
                    "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                    new_callable=AsyncMock,
                    return_value={"version": "2.0.0"},
                ):
                    with patch(
                        "backend.provision.rum_orchestrator_v2.reconcile_vcl_state",
                        return_value=MagicMock(activated_version=None, draft_version=8),
                    ):
                        with patch("backend.provision.rum_orchestrator_v2._purge_faro_surrogate_key") as mock_purge:
                            with patch(
                                "backend.provision.rum_orchestrator_v2.cleanup_old_faro_versions",
                                new_callable=AsyncMock,
                            ) as mock_cleanup:
                                result = upgrade_faro_version(service_id, "2.0.0", "test_token", activate=False)

        mock_cleanup.assert_not_called()
        # Purge is unconditional (best-effort, harmless either way) — only
        # cleanup and the rum_vcl_sha update are activation-gated.
        mock_purge.assert_called_once()
        assert result["activated"] is False
        assert result["logging_service_active_version"] == 8
        # save_config must not have been called with a payload carrying a
        # freshly-recomputed rum_vcl_sha (the activation-gated write this
        # fix adds) — every save_config call in this flow before that point
        # is download_and_upload_faro's own persistence, which this test
        # mocks out entirely, so ANY save_config call here would have to be
        # the (incorrectly, for activate=False) added sha update.
        mock_save.assert_not_called()


class TestEnableRumSkipsTrackerWhenNotActivated:
    """Test that enable_rum() does not publish the tracker when the VCL
    route it depends on never actually went live."""

    def test_enable_rum_activate_false_skips_tracker_upload(self):
        """Verify upload_rum_tracker_js is NOT called when activate=False.

        A draft that was compiled and validated but never activated does
        not serve /js/faro-sdk.js — publishing the tracker in that state
        would reproduce the exact ordering hazard this fix closes.
        """
        service_id = "srv_test"
        cfg = {
            "service_id": service_id,
            "rum_enabled": False,
            "fos_bucket": "my-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 1,
        }

        with patch("backend.config.load_config", return_value=cfg):
            with patch("backend.config.save_config"):
                with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket"):
                    with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                        with patch(
                            "backend.provision.rum_orchestrator_v2.download_and_upload_faro",
                            new_callable=AsyncMock,
                        ):
                            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                                mock_reconcile.return_value = MagicMock(activated_version=None, draft_version=3)

                                result = enable_rum(service_id, "test_token", activate=False)

        mock_upload.assert_not_called()
        assert result["activated"] is False


class TestRumVclFingerprint:
    """F-4 audit finding: rum_vcl_fingerprint must actually reflect a pinned
    faro_version, since that's the whole point of using it for /rum/status's
    vcl_drift — before this fix it called generate_rum_vcl() alone, whose
    Phase 1 output never varies with faro_version at all, so a service
    whose deployed VCL was generated with faro_version=None (never
    reconciled after a version was pinned) would report no drift."""

    def _cfg(self, **overrides):
        cfg = {
            "service_id": "srv_test",
            "fos_region": "us-east-1",
            "cdn_shield": "",
            "rum": {"faro_version": "2.9.0"},
        }
        cfg.update(overrides)
        return cfg

    def test_different_faro_versions_produce_different_fingerprints(self):
        """The whole point of the fix: two services differing ONLY in
        pinned faro_version must get different fingerprints."""
        cfg_a = self._cfg(rum={"faro_version": "2.9.0"})
        cfg_b = self._cfg(rum={"faro_version": "3.0.0"})

        assert rum_vcl_fingerprint("srv_test", cfg_a) != rum_vcl_fingerprint("srv_test", cfg_b)

    def test_unpinned_and_pinned_produce_different_fingerprints(self):
        """A service with faro_version=None (VCL missing the /js/faro-sdk.js
        route entirely) must not collide with one that has a version
        pinned — this is the exact drift case F-4 was about."""
        unpinned = self._cfg(rum={})
        pinned = self._cfg(rum={"faro_version": "2.9.0"})

        assert rum_vcl_fingerprint("srv_test", unpinned) != rum_vcl_fingerprint("srv_test", pinned)

    def test_same_faro_version_is_deterministic(self):
        """Same inputs, same output — this is a cache-comparison fingerprint,
        it must not vary run to run."""
        cfg = self._cfg()
        assert rum_vcl_fingerprint("srv_test", cfg) == rum_vcl_fingerprint("srv_test", cfg)

    def test_reads_faro_version_from_disk_when_cfg_omitted(self, monkeypatch):
        """Callers that don't pass cfg (e.g. /rum/status) must get a
        fingerprint reflecting the service's current on-disk config."""
        disk_cfg = self._cfg(rum={"faro_version": "1.2.3"})
        monkeypatch.setattr("backend.config.load_config", lambda service_id: disk_cfg)

        from_disk = rum_vcl_fingerprint("srv_test")
        explicit = rum_vcl_fingerprint("srv_test", disk_cfg)

        assert from_disk == explicit

    def test_cdn_shield_falls_back_to_shield_map_by_region(self):
        """No explicit cdn_shield — shield selection must derive from
        fos_region (mirroring _generate_rum_section_vcl's own fallback), not
        silently default to a fixed shield regardless of region."""
        cfg_iad = self._cfg(fos_region="us-east-1", cdn_shield="")
        cfg_lon = self._cfg(fos_region="uk-east-1", cdn_shield="")

        assert rum_vcl_fingerprint("srv_test", cfg_iad) != rum_vcl_fingerprint("srv_test", cfg_lon)


# ── Merged from tests/provision/test_rum_orchestrator_v2_config_merge.py ──
SID = "logsvc1"

OLD_VERSION = "1.0.0"

NEW_VERSION = "1.2.3"

_FOS = {
    "fos_bucket": "bkt",
    "fos_region": "us-east-1",
    "fos_access_key_id": "AK",
    "fos_secret_access_key": "SK",
}


def _concurrent_writer(**top_level):
    """Stand-in for another request/cron writing to the config while a Fastly call is in flight."""

    def _apply(c: dict) -> None:
        c.update(top_level)
        c.setdefault("rum", {})["concurrent_rum_key"] = "kept"

    assert not _lock_held(), "config lock held across a Fastly/FOS call"
    svcconfig.mutate_config(SID, _apply)


@pytest.fixture
def disabled_cfg():
    svcconfig.save_config(SID, {"service_id": SID, "keep_me": 1, "rum_enabled": False, **_FOS})
    return svcconfig.load_config(SID)


@pytest.fixture
def enabled_cfg():
    svcconfig.save_config(
        SID,
        {
            "service_id": SID,
            "keep_me": 1,
            "rum_enabled": True,
            "rum_vcl_sha": "stale",
            "rum": {"faro_version": OLD_VERSION, "faro_content_hash": "oldhash", "faro_fos_etag_md5": "oldmd5"},
            **_FOS,
        },
    )
    return svcconfig.load_config(SID)


@pytest.fixture
def stubs():
    mod = "backend.provision.rum_orchestrator_v2"
    with (
        patch(f"{mod}.download_and_upload_faro", new_callable=AsyncMock) as upload,
        patch(f"{mod}.reconcile_vcl_state") as reconcile,
        patch(f"{mod}.upload_rum_tracker_js") as tracker,
        patch(f"{mod}.cleanup_old_faro_versions", new_callable=AsyncMock),
        patch(f"{mod}._purge_faro_surrogate_key"),
        patch("backend.provision.rum_assets.delete_rum_tracker_js") as delete_js,
    ):
        reconcile.return_value = MagicMock(activated_version=7, draft_version=None)
        yield MagicMock(upload=upload, reconcile=reconcile, tracker=tracker, delete_js=delete_js)


def test_enable_persists_owned_keys_and_keeps_the_rest(disabled_cfg, stubs):
    salt = disabled_cfg["rum"]["cid_salt"]

    out = enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is True
    assert saved["rum_enabled_at"] == out["enabled_at"]
    assert saved["rum"]["faro_version"] == NEW_VERSION
    assert saved["rum"]["cid_salt"] == salt
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved)
    assert saved["keep_me"] == 1


def test_enable_does_not_hold_the_config_lock_across_fastly_calls(disabled_cfg, stubs):
    held: list[bool] = []

    async def _upload(*_a, **_kw):
        held.append(_lock_held())

    def _reconcile(*_a, **_kw):
        held.append(_lock_held())
        return MagicMock(activated_version=7, draft_version=None)

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _reconcile

    enable_rum(SID, "tok", faro_version=NEW_VERSION)

    assert held == [False, False]


def test_enable_rollback_after_upload_failure_keeps_concurrent_writes(disabled_cfg, stubs):
    async def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("upload failed")

    stubs.upload.side_effect = _boom

    with pytest.raises(RuntimeError, match="upload failed"):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False
    assert "rum_vcl_sha" not in saved
    assert "faro_version" not in saved["rum"]
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == disabled_cfg["rum"]["cid_salt"]


def test_enable_rollback_after_reconcile_failure_keeps_concurrent_writes(disabled_cfg, stubs):
    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="reconcile failed"):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    saved = _persisted()
    assert saved["rum_enabled"] is False
    assert "faro_version" not in saved["rum"]
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"


def test_enable_rollback_restores_the_previous_pin(stubs):
    svcconfig.save_config(
        SID,
        {"service_id": SID, "rum_enabled": False, "rum": {"faro_version": OLD_VERSION}, **_FOS},
    )
    stubs.reconcile.side_effect = RuntimeError("nope")

    with pytest.raises(RuntimeError):
        enable_rum(SID, "tok", faro_version=NEW_VERSION)

    assert _persisted()["rum"]["faro_version"] == OLD_VERSION


def test_upgrade_reconcile_failure_restores_the_pin_and_keeps_concurrent_writes(enabled_cfg, stubs):
    async def _upload(*_a, **_kw):
        svcconfig.mutate_config(
            SID,
            lambda c: c["rum"].update(
                faro_version=NEW_VERSION, faro_content_hash="newhash", faro_fos_etag_md5="newmd5"
            ),
        )

    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="reconcile failed"):
        upgrade_faro_version(SID, NEW_VERSION, "tok")

    saved = _persisted()
    assert saved["rum"]["faro_version"] == OLD_VERSION
    assert saved["rum"]["faro_content_hash"] == "oldhash"
    assert saved["rum"]["faro_fos_etag_md5"] == "oldmd5"
    assert saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == enabled_cfg["rum"]["cid_salt"]
    assert saved["concurrent_key"] == "kept"


def test_upgrade_success_refreshes_the_fingerprint_and_keeps_concurrent_writes(enabled_cfg, stubs):
    async def _upload(*_a, **_kw):
        svcconfig.mutate_config(SID, lambda c: c["rum"].update(faro_version=NEW_VERSION))

    def _reconcile(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        return MagicMock(activated_version=9, draft_version=None)

    stubs.upload.side_effect = _upload
    stubs.reconcile.side_effect = _reconcile

    upgrade_faro_version(SID, NEW_VERSION, "tok")

    saved = _persisted()
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved)
    assert saved["rum_vcl_sha"] != "stale"
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"


def test_disable_persists_owned_keys_and_keeps_the_rest(enabled_cfg, stubs):
    disable_rum(SID, "tok")

    saved = _persisted()
    assert saved["rum_enabled"] is False
    assert "rum_vcl_sha" not in saved
    assert saved["rum"] == enabled_cfg["rum"]
    assert saved["keep_me"] == 1


def test_disable_does_not_hold_the_config_lock_across_fastly_calls(enabled_cfg, stubs):
    held: list[bool] = []
    stubs.delete_js.side_effect = lambda *_a, **_kw: held.append(_lock_held())
    stubs.reconcile.side_effect = lambda *_a, **_kw: (held.append(_lock_held()), MagicMock(activated_version=2))[1]

    disable_rum(SID, "tok")

    assert held == [False, False]


def test_disable_rollback_restores_enabled_and_keeps_concurrent_writes(enabled_cfg, stubs):
    def _boom(*_a, **_kw):
        _concurrent_writer(concurrent_key="kept")
        raise RuntimeError("reconcile failed")

    stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="reconcile failed"):
        disable_rum(SID, "tok")

    saved = _persisted()
    assert saved["rum_enabled"] is True
    assert saved["rum_vcl_sha"] == rum_vcl_fingerprint(SID, saved)
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["concurrent_rum_key"] == "kept"
    assert saved["rum"]["cid_salt"] == enabled_cfg["rum"]["cid_salt"]
