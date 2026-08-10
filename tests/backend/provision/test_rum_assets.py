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
import re

import httpx
import pytest

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
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", fake_save)
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
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", fake_save)
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


# ── generate_rum_tracker_js ─────────────────────────────────────────────────


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


# ── faro_tracker_ready / upload_rum_tracker_js readiness gate ───────────────
#
# Regression coverage for the live-production failure: a reconcile can
# succeed with NO Faro route deployed at all (faro_version never pinned), yet
# generate_rum_tracker_js unconditionally references /js/faro-sdk.js. Before
# this gate, upload_rum_tracker_js would publish that tracker anyway — every
# visitor's browser then fetches a route with no VCL behind it and gets the
# origin's 2-byte "OK" fallthrough instead of the SDK, so Faro never
# initializes and zero beacons are collected.


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


def test_upload_rum_tracker_js_skips_publish_when_no_faro_version_pinned(monkeypatch):
    """This is the stranded-tracker scenario end to end through the real
    publish function: a service with RUM config but no faro_version pinned
    (e.g. an activation that never wrote the pin) must NOT get a
    faro-referencing tracker published — the FOS PUT for rum-tracker.js
    must never happen."""
    cfg = {**FAKE_CFG, "rum": {"enabled": True}}
    monkeypatch.setattr(rum_assets.svcconfig, "load_config", lambda service_id: dict(cfg))
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: None)

    put_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            put_calls.append(str(request.url))
        raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

    monkeypatch.setattr(rum_assets.httpx, "Client", _mock_client_factory(handler))

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
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: None)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD" and "faro-web-sdk-v" in request.url.path:
            return httpx.Response(404)
        raise AssertionError(f"unexpected FOS call: {request.method} {request.url}")

    monkeypatch.setattr(rum_assets.httpx, "Client", _mock_client_factory(handler))

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
    monkeypatch.setattr(rum_assets.svcconfig, "save_config", lambda *a, **k: None)

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

    monkeypatch.setattr(rum_assets.httpx, "Client", _mock_client_factory(handler))

    result = upload_rum_tracker_js("svc_test", "tok")

    assert "skipped" not in result
    assert result["bytes_uploaded"] > 0
    assert "/js/faro-sdk.js" in put_bodies["tracker"].decode()
