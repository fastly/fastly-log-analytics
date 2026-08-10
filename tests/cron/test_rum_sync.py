"""Tests for the Faro bundle reconcile step in ``backend.cron.jobs.rum_sync``.

Two seams to mock: FOS access (SigV4 HEAD over ``httpx.MockTransport``, same
shape as ``tests/backend/provision/test_rum_assets.py``) and the Task 2
functions (``download_and_upload_faro`` / ``detect_faro_version_change``,
monkeypatched directly at their source module, ``backend.provision.rum_assets``
— that is where the reconcile step's late ``from … import …`` resolves the
names on every call). No test makes a real network call.
"""

from __future__ import annotations

import copy
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.cron.jobs import rum_sync as rum_sync_mod

SERVICE_ID = "svc_test"

FAKE_CFG: dict[str, Any] = {
    "service_id": SERVICE_ID,
    "fos_access_key_id": "AKIAFAKEEXAMPLE",
    "fos_secret_access_key": "fakeSecretKeyExample",
    "fos_bucket": "fake-test-bucket",
    "fos_region": "us-east-1",
    "cdn_service_id": "SvcFakeCDN123",
    "fastly_api_key": "fake-fastly-token",
    "rum": {
        "enabled": True,
        "faro_version": "2.9.0",
        "faro_content_hash": "stored-hash-abc123",
        # _faro_bundle_intact compares against this field specifically (the
        # S3/FOS ETag is protocol-mandated MD5, independent of whatever
        # algorithm faro_content_hash uses) — see rum_sync.py's
        # _faro_bundle_intact docstring.
        "faro_fos_etag_md5": "stored-hash-abc123",
        "faro_last_upstream_check": 0,
        "faro_upstream_check_hours": 24,
    },
}

NOW = 1_800_000_000.0  # arbitrary frozen epoch

_REAL_CLIENT = httpx.Client


# ── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def config_store(monkeypatch):
    """In-memory config store backing ``svcconfig.load_config``/``save_config``.

    A real store (not a single static dict) so a test can assert what a
    later ``load_config`` call sees after an earlier ``save_config`` in the
    same reconcile pass — the throttle timestamp write depends on this.
    """
    store: dict[str, dict] = {}

    def fake_load(service_id):
        cfg = store.get(service_id)
        return copy.deepcopy(cfg) if cfg is not None else None

    def fake_save(service_id, new_cfg):
        store[service_id] = copy.deepcopy(new_cfg)

    monkeypatch.setattr("backend.config.load_config", fake_load)
    monkeypatch.setattr("backend.config.save_config", fake_save)
    return store


@pytest.fixture
def frozen_now(monkeypatch):
    """Pin ``time.time()`` as seen by the reconcile step (the module does a
    top-level ``import time``, so patching the shared module object affects
    it regardless of which module holds the reference)."""
    monkeypatch.setattr(rum_sync_mod.time, "time", lambda: NOW)
    return NOW


@pytest.fixture
def mock_fos(monkeypatch):
    """Install an ``httpx.MockTransport``-backed ``httpx.Client`` factory.

    ``_faro_bundle_intact`` does a local ``import httpx`` — that binds to
    the same shared module object, so patching ``httpx.Client`` here (the
    real module) is visible to it.
    """

    def install(handler):
        def factory(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return _REAL_CLIENT(*args, **kwargs)

        monkeypatch.setattr(httpx, "Client", factory)

    return install


@pytest.fixture
def mock_download(monkeypatch):
    """Stub ``download_and_upload_faro`` at its source module.

    Returns ``(calls, install)``: ``calls`` accumulates
    ``(service_id, version, token)`` tuples; ``install(raises=...)`` swaps
    in a version that raises instead of succeeding. Defaults to a
    success stub so tests that don't expect a download can just assert
    ``calls == []``.
    """
    calls: list[tuple] = []

    def install(*, raises: Exception | None = None):
        async def fake(service_id, version, token, *, status_cb=None):
            calls.append((service_id, version, token))
            if raises:
                raise raises
            return {
                "version": version,
                "path": f"rum/faro-web-sdk-v{version}.iife.js",
                "bytes_uploaded": 123,
                "content_hash": "new-hash-after-restore",
                "fos_key": f"s3://fake-test-bucket/rum/faro-web-sdk-v{version}.iife.js",
            }

        monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake)

    install()
    return calls, install


@pytest.fixture
def mock_detect(monkeypatch):
    """Stub ``detect_faro_version_change`` at its source module.

    Defaults to a version that raises ``AssertionError`` if called at all —
    tests that expect the throttled upstream check to actually run must
    call ``install(result=...)`` first.
    """
    calls: list[tuple] = []

    def install(*, result: bool = False, raises: Exception | None = None):
        async def fake(service_id, version):
            calls.append((service_id, version))
            if raises:
                raise raises
            return result

        monkeypatch.setattr("backend.provision.rum_assets.detect_faro_version_change", fake)

    async def _forbidden(service_id, version):
        raise AssertionError(f"detect_faro_version_change must not be called for {service_id!r}/{version!r} here")

    monkeypatch.setattr("backend.provision.rum_assets.detect_faro_version_change", _forbidden)
    return calls, install


@pytest.fixture
def mock_purge(monkeypatch):
    """Stub the Fastly CDN purge client; records surrogate-key purge calls."""
    calls: list[tuple] = []

    def fake(method, path, *, token, expect_empty=False, **kwargs):
        calls.append((method, path, token))

    monkeypatch.setattr("backend.core.fastly.client.fastly", fake)
    return calls


def _cfg(**rum_overrides) -> dict:
    cfg = copy.deepcopy(FAKE_CFG)
    cfg["rum"].update(rum_overrides)
    return cfg


def _head_response(status: int, etag: str | None = None) -> httpx.Response:
    headers = {"ETag": f'"{etag}"'} if etag is not None else {}
    return httpx.Response(status, headers=headers)


# ── 1. steady state: HEAD 200 + matching ETag ───────────────────────────


def test_steady_state_skips_download(config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge):
    """Common case: FOS already holds the pinned bundle intact, and the
    upstream-check window hasn't elapsed. No FOS PUT, no unpkg traffic."""
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_content_hash="stored-hash-abc123", faro_last_upstream_check=NOW - 10)

    seen_methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_methods.append(request.method)
        return _head_response(200, etag="stored-hash-abc123")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert seen_methods == ["HEAD"]
    assert download_calls == []
    assert mock_purge == []


# ── 2 & 3. integrity check triggers restore ─────────────────────────────


def test_head_404_triggers_restore_and_purge(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """Bundle missing from FOS (404) — restore it and purge the surrogate key."""
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)

    mock_fos(lambda request: httpx.Response(404))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]
    assert len(mock_purge) == 1
    method, path, token = mock_purge[0]
    assert method == "POST"
    assert path == "/service/SvcFakeCDN123/purge/rum-faro-sdk"
    assert token == "fake-fastly-token"


def test_etag_mismatch_triggers_restore(config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge):
    """Bundle present but corrupt (ETag differs from stored hash) — restore it."""
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)

    mock_fos(lambda request: _head_response(200, etag="some-other-etag"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]


def test_head_network_error_treated_as_needs_restore(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """A HEAD that fails outright (connection error) must be treated the
    same as "not intact" — never raise out of the integrity check."""
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("FOS unreachable", request=request)

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]


def test_missing_fos_credentials_treated_as_needs_restore(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """No FOS credentials configured — integrity check can't verify, so it
    reports "not intact" (the subsequent restore attempt will itself fail
    for the same reason, but that failure is swallowed independently)."""
    download_calls, install_download = mock_download
    install_download(raises=RuntimeError("missing FOS credentials"))
    cfg = _cfg(faro_last_upstream_check=NOW - 10)
    del cfg["fos_access_key_id"]
    config_store[SERVICE_ID] = cfg

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no FOS call should be attempted without credentials")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]


def test_purge_failure_is_swallowed(config_store, frozen_now, mock_fos, mock_download, mock_detect, monkeypatch):
    """A CDN purge failure after a successful restore must not raise or
    undo the restore — it's fire-and-forget."""
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)

    def failing_purge(method, path, *, token, expect_empty=False, **kwargs):
        raise RuntimeError("Fastly API unreachable")

    monkeypatch.setattr("backend.core.fastly.client.fastly", failing_purge)
    mock_fos(lambda request: httpx.Response(404))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]


def test_progress_reported_when_run_id_set(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge, monkeypatch
):
    """When a run_id is available (ingest is already tracking a cron run),
    reconcile progress messages are attached to it via add_progress."""
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)
    mock_fos(lambda request: httpx.Response(404))

    add_progress_calls: list[tuple] = []
    monkeypatch.setattr(
        "backend.cron_progress.add_progress",
        lambda run_id, event: add_progress_calls.append((run_id, event)),
    )

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, 99)

    assert add_progress_calls
    assert all(run_id == 99 for run_id, _ in add_progress_calls)
    assert any("restored" in event["message"] for _, event in add_progress_calls)


# ── 4 & 5. throttled upstream drift check ───────────────────────────────


def test_upstream_check_throttled_within_window(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """Second tick inside the 24h window must not call
    detect_faro_version_change at all (mock_detect's default fixture
    asserts exactly that)."""
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 3600)  # 1h ago, window=24h

    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert download_calls == []
    assert config_store[SERVICE_ID]["rum"]["faro_last_upstream_check"] == NOW - 3600


def test_upstream_check_runs_once_window_elapsed_no_drift(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """Window elapsed — detect_faro_version_change runs once, timestamp is
    updated to now, and (no drift) no re-download happens."""
    download_calls, _ = mock_download
    detect_calls, install_detect = mock_detect
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 25 * 3600)  # 25h ago, window=24h
    install_detect(result=False)

    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert detect_calls == [(SERVICE_ID, "2.9.0")]
    assert config_store[SERVICE_ID]["rum"]["faro_last_upstream_check"] == NOW
    assert download_calls == []


def test_upstream_drift_detected_triggers_resync(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """Upstream re-released the same version string — re-sync + purge, and
    the pinned version string itself must not change."""
    download_calls, _ = mock_download
    detect_calls, install_detect = mock_detect
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 25 * 3600)
    install_detect(result=True)

    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert detect_calls == [(SERVICE_ID, "2.9.0")]
    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]
    assert len(mock_purge) == 1
    assert config_store[SERVICE_ID]["rum"]["faro_version"] == "2.9.0"


def test_upstream_check_failure_still_writes_timestamp(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """A failing upstream check must still advance the throttle timestamp —
    otherwise a persistently-broken upstream re-triggers every tick."""
    download_calls, _ = mock_download
    detect_calls, install_detect = mock_detect
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 25 * 3600)
    install_detect(raises=ValueError("unpkg unreachable"))

    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert detect_calls == [(SERVICE_ID, "2.9.0")]
    assert config_store[SERVICE_ID]["rum"]["faro_last_upstream_check"] == NOW
    assert download_calls == []


# ── failure isolation: reconcile must never fail the cron ──────────────


def test_download_failure_during_restore_does_not_raise(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """A FOS/unpkg outage during restore must degrade to a logged warning,
    never an exception out of the reconcile step."""
    download_calls, install_download = mock_download
    install_download(raises=RuntimeError("FOS upload failed: 500"))
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)

    mock_fos(lambda request: httpx.Response(404))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]


def test_beacon_ingest_still_proceeds_when_faro_reconcile_raises(config_store, frozen_now, monkeypatch):
    """Wiring-level check: if the Faro reconcile call somehow raises all the
    way out (belt-and-braces — the unit tests above show it shouldn't), the
    beacon-ingest cron body must still handle it as a normal cron failure —
    logging and cleaning up — rather than leaving cron state inconsistent.
    """

    def raising_reconcile(service_id, run_id):
        raise RuntimeError("should never escape, simulated anyway")

    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", raising_reconcile)

    events = [
        ("started", 42),
        ("file_done", "beacon-1.log.gz", 10),
        ("done", 10),
    ]

    def fake_ingest(service_id):
        yield from events

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", fake_ingest)

    start_progress = MagicMock()
    add_progress = MagicMock()
    end_progress = MagicMock()
    cleanup = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.add_progress", add_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)  # must not raise

    cleanup.assert_called_once()
    error_messages = [
        call.args[1]["message"] for call in add_progress.call_args_list if call.args[1]["type"] == "error"
    ]
    assert any("RUM sync failed" in m for m in error_messages)


# ── skip conditions ──────────────────────────────────────────────────────


def test_skips_when_rum_not_enabled(config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge):
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(enabled=False)

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("FOS must not be contacted when RUM is disabled")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert download_calls == []


def test_top_level_rum_enabled_flag_alone_is_sufficient(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """The declarative enable_rum() orchestration path only ever sets the
    top-level ``rum_enabled`` flag (never ``rum.enabled``) — the reconcile
    must not treat such a service as disabled."""
    download_calls, _ = mock_download
    cfg = _cfg()
    del cfg["rum"]["enabled"]
    cfg["rum_enabled"] = True
    config_store[SERVICE_ID] = cfg

    seen_methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_methods.append(request.method)
        return _head_response(200, etag="stored-hash-abc123")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    # Proves the integrity check actually ran instead of returning early —
    # an incorrectly-skipped service would also leave download_calls empty.
    assert seen_methods == ["HEAD"]
    assert download_calls == []


# ── self-heal: RUM enabled, no version pinned at all ────────────────────


def test_self_heals_missing_pinned_version_to_default(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, monkeypatch
):
    """A service that's RUM-enabled but has never had a version pinned (the
    real-world case: a service enabled through the older imperative
    provisioning path, or before every enable path pinned a version by
    default) must self-heal to DEFAULT_FARO_VERSION and upload a bundle —
    the tracker JS unconditionally loads /js/faro-sdk.js with no CDN
    fallback, so leaving it unpinned means a permanent 404 for every
    visitor."""
    cfg = _cfg()
    del cfg["rum"]["faro_version"]
    del cfg["rum"]["faro_content_hash"]
    del cfg["rum"]["faro_fos_etag_md5"]
    config_store[SERVICE_ID] = cfg

    calls: list[tuple] = []

    async def fake_download(service_id, version, token, *, status_cb=None):
        calls.append((service_id, version, token))
        # Mirror the real download_and_upload_faro's persistence (it saves
        # faro_version/faro_content_hash/faro_fos_etag_md5 via its own
        # save_config call) so the integrity check right after self-heal
        # sees a matching bundle, same as it would in production.
        stored = config_store[service_id]
        rum_cfg = dict(stored.get("rum") or {})
        rum_cfg["faro_version"] = version
        rum_cfg["faro_content_hash"] = "adopted-hash"
        rum_cfg["faro_fos_etag_md5"] = "adopted-etag"
        stored["rum"] = rum_cfg
        return {"version": version}

    monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake_download)
    mock_fos(lambda request: _head_response(200, etag="adopted-etag"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert calls == [(SERVICE_ID, DEFAULT_FARO_VERSION, "fake-fastly-token")]
    assert config_store[SERVICE_ID]["rum"]["faro_version"] == DEFAULT_FARO_VERSION
    assert len(mock_purge) == 1


def test_self_heal_does_not_thrash_on_the_next_tick(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, monkeypatch
):
    """Once self-healed, the next tick must see a pinned version and an
    intact bundle and do nothing further — the adoption is one-time, not a
    per-tick re-download."""
    _, install_detect = mock_detect
    install_detect(result=False)

    cfg = _cfg()
    del cfg["rum"]["faro_version"]
    del cfg["rum"]["faro_content_hash"]
    del cfg["rum"]["faro_fos_etag_md5"]
    config_store[SERVICE_ID] = cfg

    calls: list[tuple] = []

    async def fake_download(service_id, version, token, *, status_cb=None):
        calls.append((service_id, version, token))
        stored = config_store[service_id]
        rum_cfg = dict(stored.get("rum") or {})
        rum_cfg["faro_version"] = version
        rum_cfg["faro_content_hash"] = "adopted-hash"
        rum_cfg["faro_fos_etag_md5"] = "adopted-etag"
        stored["rum"] = rum_cfg
        return {"version": version}

    monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake_download)
    mock_fos(lambda request: _head_response(200, etag="adopted-etag"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert len(calls) == 1

    # Second tick: version is now pinned and the bundle is intact — no
    # further adoption, no further download.
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert len(calls) == 1


def test_self_heal_adoption_failure_does_not_raise(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    """An unpkg/FOS outage during self-heal adoption must degrade to a
    logged warning, never an exception out of the reconcile step."""
    download_calls, install_download = mock_download
    install_download(raises=RuntimeError("unpkg unreachable"))
    cfg = _cfg()
    del cfg["rum"]["faro_version"]
    config_store[SERVICE_ID] = cfg

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no FOS HEAD should be attempted when adoption itself fails")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert download_calls == [(SERVICE_ID, DEFAULT_FARO_VERSION, "fake-fastly-token")]
    assert "faro_version" not in config_store[SERVICE_ID]["rum"]


def test_skips_when_no_rum_config_at_all(config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge):
    download_calls, _ = mock_download
    cfg = copy.deepcopy(FAKE_CFG)
    del cfg["rum"]
    config_store[SERVICE_ID] = cfg

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("FOS must not be contacted with no rum config block")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert download_calls == []


def test_skips_when_no_config(config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge):
    """No service config at all (never provisioned) — silent no-op."""
    download_calls, _ = mock_download

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("FOS must not be contacted with no service config")

    mock_fos(handler)

    rum_sync_mod._reconcile_faro_bundle("no-such-service", None)

    assert download_calls == []
