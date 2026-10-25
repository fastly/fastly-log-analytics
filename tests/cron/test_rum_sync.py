"""Consolidated tests for test_rum_sync."""

from __future__ import annotations

import copy
import sqlite3
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
def rum_lease(monkeypatch):
    """Stand-in for the per-(service, task) lease that ``cron_run`` acquires for rum_sync."""
    lease = MagicMock()
    lease.source = {"name": SERVICE_ID}
    lease.start = MagicMock(return_value=7)
    lease.log_cron_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: lease.source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lease.log_cron_run)
    return lease


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

    def fake_mutate(service_id, mutate, **kwargs):
        cfg = fake_load(service_id)
        if cfg is None:
            return None
        mutate(cfg)
        fake_save(service_id, cfg)
        return cfg

    monkeypatch.setattr("backend.config.load_config", fake_load)
    monkeypatch.setattr("backend.config.save_config", fake_save)
    monkeypatch.setattr("backend.config.mutate_config", fake_mutate)
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
        # Accepts **kwargs (not just status_cb) so it tolerates
        # expected_current_version — the F-5 compare-and-set guard the
        # reconcile step now passes on every call — without every existing
        # assertion needing to widen its recorded tuple.
        async def fake(service_id, version, token, *, status_cb=None, **kwargs):
            calls.append((service_id, version, token))
            if raises:
                raise raises
            return {
                "version": version,
                "path": f"rum/faro-web-sdk-v{version}.iife.js",
                "bytes_uploaded": 123,
                "content_hash": "new-hash-after-restore",
                "fos_key": f"s3://fake-test-bucket/rum/faro-web-sdk-v{version}.iife.js",
                "config_updated": True,
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


@pytest.fixture
def mock_reconcile(monkeypatch):
    """Stub ``reconcile_vcl_state`` — the self-heal's one-time VCL convergence
    call (F-4). Records ``(service_id, token)`` positional-arg tuples; no
    test may let this fall through to a real Fastly API call."""
    calls: list[tuple] = []

    def fake(service_id, token, *args, **kwargs):
        calls.append((service_id, token))
        return MagicMock(activated_version=2, draft_version=None)

    monkeypatch.setattr("backend.provision.declarative.reconciler.reconcile_vcl_state", fake)
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
    """Bundle missing from FOS (404) — restore it and purge the surrogate key.

    The purge MUST target the logging service (SERVICE_ID), not the CDN
    service fronting FOS (F-1 audit finding): Surrogate-Key: rum-faro-sdk is
    set only in the RUM fetch-cache snippet, which is deployed to the
    instrumented logging service — it never exists on the CDN service, so a
    purge against the CDN service can never match anything there.
    """
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)

    mock_fos(lambda request: httpx.Response(404))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]
    assert len(mock_purge) == 1
    method, path, token = mock_purge[0]
    assert method == "POST"
    assert path == f"/service/{SERVICE_ID}/purge/rum-faro-sdk"
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


def test_beacon_ingest_still_proceeds_when_faro_reconcile_raises(config_store, frozen_now, monkeypatch, rum_lease):
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

    def fake_ingest(service_id, *args, **kwargs):
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


def test_faro_only_reconcile_failure_marks_standard_rum_run_warning(monkeypatch, rum_lease):
    rum_lease.start.return_value = 42
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE cron_runs (id INTEGER, service_id TEXT, status TEXT)")
    con.execute("INSERT INTO cron_runs VALUES (42, ?, 'success')", (SERVICE_ID,))
    monkeypatch.setattr("backend.core.metadata.get_con", lambda service_id: con)
    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", lambda service_id, run_id: False)
    monkeypatch.setattr(
        rum_sync_mod, "ingest_rum_logs", lambda service_id, *args, **kwargs: iter([("started", 42), ("done", 0)])
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    assert con.execute("SELECT status FROM cron_runs WHERE id = 42").fetchone() == ("warning",)
    con.close()


def test_standard_rum_data_plane_error_is_not_downgraded_to_warning(monkeypatch, rum_lease):
    rum_lease.start.return_value = 43
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE cron_runs (id INTEGER, service_id TEXT, status TEXT)")
    con.execute("INSERT INTO cron_runs VALUES (43, ?, 'success')", (SERVICE_ID,))
    monkeypatch.setattr("backend.core.metadata.get_con", lambda service_id: con)
    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", lambda service_id, run_id: False)

    def failed_ingest(service_id, *args, **kwargs):
        yield ("started", 43)
        con.execute("UPDATE cron_runs SET status = 'error' WHERE id = 43")
        con.commit()
        yield ("error", "rum/beacon.log.gz", "corrupt gzip")
        yield ("done", 0)

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", failed_ingest)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    assert con.execute("SELECT status FROM cron_runs WHERE id = 43").fetchone() == ("error",)
    con.close()


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


def _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos) -> list[tuple]:
    cfg = _cfg()
    for k in ("faro_version", "faro_content_hash", "faro_fos_etag_md5"):
        cfg["rum"].pop(k, None)
    config_store[SERVICE_ID] = cfg
    calls: list[tuple] = []

    async def fake_download(service_id, version, token, *, status_cb=None, **kwargs):
        calls.append((service_id, version, token))
        stored = config_store[service_id]
        rum_cfg = dict(stored.get("rum") or {})
        rum_cfg.update(
            {"faro_version": version, "faro_content_hash": "adopted-hash", "faro_fos_etag_md5": "adopted-etag"}
        )
        stored["rum"] = rum_cfg
        return {"version": version}

    monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake_download)
    mock_fos(lambda request: _head_response(200, etag="adopted-etag"))
    return calls


def test_self_heals_missing_pinned_version_to_default(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, mock_reconcile, monkeypatch
):
    """A service that's RUM-enabled but has never had a version pinned must self-heal
    to DEFAULT_FARO_VERSION, upload a bundle, AND reconcile the deployed VCL (F-4)."""
    calls = _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert calls == [(SERVICE_ID, DEFAULT_FARO_VERSION, "fake-fastly-token")]
    assert config_store[SERVICE_ID]["rum"]["faro_version"] == DEFAULT_FARO_VERSION
    assert len(mock_purge) == 1
    assert mock_reconcile == [(SERVICE_ID, "fake-fastly-token")]


def test_self_heal_refreshes_stored_vcl_sha_after_reconcile(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, mock_reconcile, monkeypatch
):
    """#2 audit finding: after a successful self-heal reconcile, the persisted config
    carries a ``rum_vcl_sha`` equal to what ``rum_vcl_fingerprint`` computes."""
    from backend.provision.rum_orchestrator_v2 import rum_vcl_fingerprint

    _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    final_cfg = config_store[SERVICE_ID]
    assert final_cfg.get("rum_vcl_sha") == rum_vcl_fingerprint(SERVICE_ID, final_cfg)


def test_self_heal_does_not_thrash_on_the_next_tick(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, mock_reconcile, monkeypatch
):
    """Once self-healed, the next tick must see a pinned version and an
    intact bundle and do nothing further."""
    _, install_detect = mock_detect
    install_detect(result=False)
    calls = _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert len(calls) == 1
    assert len(mock_reconcile) == 1

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert len(calls) == 1
    assert len(mock_reconcile) == 1


def test_self_heal_reconcile_failure_does_not_raise(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, monkeypatch
):
    """A Fastly API outage during the self-heal's VCL reconcile (F-4) must
    degrade to a logged warning, never an exception out of the reconcile step."""
    _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos)

    def raising_reconcile(service_id, token, *args, **kwargs):
        raise RuntimeError("Fastly API unreachable")

    monkeypatch.setattr("backend.provision.declarative.reconciler.reconcile_vcl_state", raising_reconcile)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)  # must not raise

    assert config_store[SERVICE_ID]["rum"]["faro_version"] == DEFAULT_FARO_VERSION
    assert len(mock_purge) == 1


def test_restore_and_resync_pass_expected_current_version_for_compare_and_set(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, monkeypatch
):
    """Wiring check for F-5: both the every-tick integrity restore and the
    throttled upstream re-sync must pass the pinned version they observed
    as ``expected_current_version`` to ``download_and_upload_faro`` — the
    compare-and-set guard that stops a slow cron re-sync from reverting a
    concurrent operator upgrade (see that function's docstring and
    tests/provision/test_rum_assets.py's dedicated race test)."""
    seen_kwargs: list[dict] = []

    async def fake_download(service_id, version, token, *, status_cb=None, **kwargs):
        seen_kwargs.append(kwargs)
        return {"version": version}

    monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake_download)

    # Restore branch (integrity check fails).
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)
    mock_fos(lambda request: httpx.Response(404))
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert seen_kwargs == [{"expected_current_version": "2.9.0"}]

    # Upstream re-sync branch (drift detected).
    seen_kwargs.clear()
    _, install_detect = mock_detect
    install_detect(result=True)
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 25 * 3600)
    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert seen_kwargs == [{"expected_current_version": "2.9.0"}]


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


# ── Merged from test_rum_discovery_contract.py ──────────────────────────────────────────────

from contextlib import contextmanager
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import rum_ledger as rum_ledger_mod
from backend.cron.scheduler import Scheduler, _resolve_rum_sync_interval
from backend.deps import get_service_id, get_source
from backend.main import app

_RUM_DISC_SERVICE_ID = "svc_test_rum_contract"

FAKE_CFG_STANDARD = {
    "service_id": _RUM_DISC_SERVICE_ID,
    "name": _RUM_DISC_SERVICE_ID,
    "bucket": "test-rum-bucket",
    "endpoint": "https://test.fos.fastly.com",
    "access_key_id": "test_key",
    "secret_access_key": "test_secret",
    "deployment_mode": "standard",
    "access_level": "read_write",
    "rum": {"enabled": True, "sync_interval_seconds": 30, "commit_interval_mins": 1},
    "provisioning": {"access_level": "read_write"},
}
FAKE_SRC_STANDARD = FAKE_CFG_STANDARD

FAKE_CFG_HIGH_SCALE = {
    **FAKE_CFG_STANDARD,
    "deployment_mode": "high_scale",
    "rum": {"enabled": True, "sync_interval_seconds": 15},
}
FAKE_SRC_HIGH_SCALE = FAKE_CFG_HIGH_SCALE


@contextmanager
def _override_service_deps(src: dict, service_id: str | None = _RUM_DISC_SERVICE_ID):
    app.dependency_overrides[get_source] = lambda: src
    if service_id is not None:
        app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_source, None)
        if service_id is not None:
            app.dependency_overrides.pop(get_service_id, None)


def _sync_scheduler_for_cfg(
    monkeypatch,
    cfg: dict,
    src: dict,
    *,
    high_scale: bool = False,
    existing_job_id: str | None = None,
    patch_add_job: bool = True,
) -> tuple[Scheduler, MagicMock | None]:
    sched = Scheduler()
    sched._sched = MagicMock()
    mock_job = None
    if existing_job_id:
        mock_job = MagicMock()
        sched._sched.get_job.return_value = mock_job
        sched._job_ids = {existing_job_id: existing_job_id}
    else:
        sched._job_ids = {}
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda s: high_scale)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)
    with patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False):
        if patch_add_job:
            with patch.object(sched, "_add_job"):
                sched._sync_jobs()
        else:
            sched._sync_jobs()
    return sched, mock_job


def _setup_high_scale_discovery(
    monkeypatch,
    *,
    run_id: int = 701,
    broker_url: str = "redis://localhost:6379/0",
    reconcile=None,
    discover=None,
) -> dict:
    log_calls: list = []
    end_progress_mock = MagicMock()
    finalize_mock = MagicMock()
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", broker_url)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: run_id)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize_mock)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync._reconcile_faro_bundle",
        reconcile if reconcile is not None else (lambda sid, rid: True),
    )
    if discover is not None:
        monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", discover)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    return {"log_calls": log_calls, "end_progress": end_progress_mock, "finalize": finalize_mock}


# =============================================================================
# 1. Dual Architecture & Mode Routing Tests
# =============================================================================


def test_mode_routing_standard_registers_rum_sync_and_commit(monkeypatch):
    """Under Standard mode (DEPLOYMENT_MODE=standard):
    - APScheduler registers rum_sync_{service_id} and rum_commit_{service_id}
    - Does NOT register rum_discovery_{service_id} or ledger_rum_sweep_{service_id}
    """
    sched, _ = _sync_scheduler_for_cfg(monkeypatch, FAKE_CFG_STANDARD, FAKE_SRC_STANDARD, high_scale=False)
    added_ids = list(sched._job_ids.keys())
    assert f"rum_sync_{_RUM_DISC_SERVICE_ID}" in added_ids
    assert f"rum_commit_{_RUM_DISC_SERVICE_ID}" in added_ids
    assert f"rum_discovery_{_RUM_DISC_SERVICE_ID}" not in added_ids
    assert f"ledger_rum_sweep_{_RUM_DISC_SERVICE_ID}" not in added_ids


def test_mode_routing_high_scale_registers_discovery_and_sweep(monkeypatch):
    """Under High-Scale mode (DEPLOYMENT_MODE=high_scale):
    - Registers rum_discovery_{service_id} and ledger_rum_sweep_{service_id}
    - Does NOT register rum_sync_{service_id} or rum_commit_{service_id}
    """
    sched, _ = _sync_scheduler_for_cfg(monkeypatch, FAKE_CFG_HIGH_SCALE, FAKE_SRC_HIGH_SCALE, high_scale=True)
    added_ids = list(sched._job_ids.keys())
    assert f"rum_discovery_{_RUM_DISC_SERVICE_ID}" in added_ids
    assert f"ledger_rum_sweep_{_RUM_DISC_SERVICE_ID}" in added_ids
    assert f"rum_sync_{_RUM_DISC_SERVICE_ID}" not in added_ids
    assert f"rum_commit_{_RUM_DISC_SERVICE_ID}" not in added_ids


def test_mode_routing_no_co_registration_guarantee(monkeypatch):
    """The two pipelines must NEVER both be registered for the same service."""
    sched, _ = _sync_scheduler_for_cfg(monkeypatch, FAKE_CFG_HIGH_SCALE, FAKE_SRC_HIGH_SCALE, high_scale=True)
    has_discovery = f"rum_discovery_{_RUM_DISC_SERVICE_ID}" in sched._job_ids
    has_sync = f"rum_sync_{_RUM_DISC_SERVICE_ID}" in sched._job_ids
    assert not (has_discovery and has_sync), "Both discovery and sync must never co-register!"


def test_high_scale_job_early_exits_in_standard_mode(monkeypatch):
    """_run_rum_discovery_cron exits early if the service is not in high_scale mode."""
    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert len(start_calls) == 0, "High-Scale discovery must not start in standard mode"


# =============================================================================
# 2. Role Gate Tests
# =============================================================================


def test_role_gate_admin_executes_normally(monkeypatch):
    """Admin (read_write) service starts progress and completes normally."""
    state = _setup_high_scale_discovery(
        monkeypatch, run_id=701, discover=lambda sid, prefix_subpath=None, run_id=None: 1
    )
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert len(state["log_calls"]) == 1
    assert state["log_calls"][0][0][3] == "success"


def test_role_gate_analyst_path_a_discovery_skipped(monkeypatch):
    """Analyst Path A (read_only access_level) discovery tick early-exits without starting cron run."""
    ro_src = {**FAKE_SRC_HIGH_SCALE, "access_level": "read_only"}
    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert len(start_calls) == 0, "Read-only service must not start RUM discovery"


def test_role_gate_analyst_path_a_sync_skipped(monkeypatch):
    """Analyst Path A (read_only access_level) sync tick skips execution in standard mode."""
    ro_src = {**FAKE_SRC_STANDARD, "access_level": "read_only"}
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.core.rum_ingest.ingest_rum_logs", MagicMock())

    with _override_service_deps(ro_src) as client:
        resp = client.post(f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}")
        assert resp.status_code == 403
        assert "read-only" in resp.json()["detail"].lower()


# =============================================================================
# 3. Cadence & Registration Tests
# =============================================================================


def test_cadence_derived_from_sync_interval_seconds():
    """RUM interval respects explicit sync_interval_seconds (bounded to >= 5s)."""
    assert _resolve_rum_sync_interval({"sync_interval_seconds": 12}, {}, default_request_interval=60) == 12
    assert _resolve_rum_sync_interval({"sync_interval_seconds": 2}, {}, default_request_interval=60) == 5


def test_cadence_fallback_to_request_interval():
    """When sync_interval_seconds is omitted, derives interval from RUM period or falls back to request interval."""
    assert _resolve_rum_sync_interval({}, {}, default_request_interval=45) == 45
    assert _resolve_rum_sync_interval({"log_period": 30}, {}, default_request_interval=60) == 30
    assert _resolve_rum_sync_interval({}, {"rum_log_period": 90}, default_request_interval=60) == 45


def test_cadence_skips_when_rum_disabled(monkeypatch):
    """When RUM is disabled (rum.enabled=False), no RUM jobs are registered."""
    disabled_cfg = copy.deepcopy(FAKE_CFG_STANDARD)
    disabled_cfg["rum"]["enabled"] = False
    sched, _ = _sync_scheduler_for_cfg(monkeypatch, disabled_cfg, FAKE_SRC_STANDARD, high_scale=False)
    rum_jobs = [
        k for k in sched._job_ids if k.startswith(("rum_sync_", "rum_commit_", "rum_discovery_", "ledger_rum_sweep_"))
    ]
    assert len(rum_jobs) == 0, f"Expected 0 RUM jobs when disabled, got {rum_jobs}"


def test_cadence_dynamic_rescheduling_standard(monkeypatch):
    """Standard mode: when interval updates, rum_sync_{service_id} is dynamically rescheduled."""
    updated_cfg = copy.deepcopy(FAKE_CFG_STANDARD)
    updated_cfg["rum"]["sync_interval_seconds"] = 45
    sched, mock_job = _sync_scheduler_for_cfg(
        monkeypatch,
        updated_cfg,
        FAKE_SRC_STANDARD,
        high_scale=False,
        existing_job_id=f"rum_sync_{_RUM_DISC_SERVICE_ID}",
        patch_add_job=False,
    )
    sched._sched.get_job.assert_called_with(f"rum_sync_{_RUM_DISC_SERVICE_ID}")
    mock_job.reschedule.assert_called_once_with("interval", seconds=45)


def test_cadence_dynamic_rescheduling_high_scale(monkeypatch):
    """High-Scale mode: when interval updates, rum_discovery_{service_id} is dynamically rescheduled."""
    updated_cfg = copy.deepcopy(FAKE_CFG_HIGH_SCALE)
    updated_cfg["rum"]["sync_interval_seconds"] = 25
    sched, mock_job = _sync_scheduler_for_cfg(
        monkeypatch,
        updated_cfg,
        FAKE_SRC_HIGH_SCALE,
        high_scale=True,
        existing_job_id=f"rum_discovery_{_RUM_DISC_SERVICE_ID}",
        patch_add_job=False,
    )
    sched._sched.get_job.assert_called_with(f"rum_discovery_{_RUM_DISC_SERVICE_ID}")
    mock_job.reschedule.assert_called_once_with("interval", seconds=25)


@pytest.fixture
def rum_sync_lease(monkeypatch) -> MagicMock:
    """Stub the cron_run lease rum_sync acquires; ``start`` returns the leased run id."""
    lease = MagicMock()
    lease.start = MagicMock(return_value=710)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", MagicMock())
    return lease


# =============================================================================
# 4. Politeness Gate Removal Tests
# =============================================================================


def test_politeness_gate_not_deferred_by_active_requests(monkeypatch, rum_sync_lease):
    """Automated RUM ticks are NEVER deferred by active API requests."""
    ingest_mock = MagicMock(return_value=iter([("started", 1), ("done", 0)]))
    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", ingest_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: True)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert ingest_mock.called, "rum_sync must execute even when active requests exist"


# =============================================================================
# 5. Faro Bundle Integrity & Reconcile Tests
# =============================================================================


def test_faro_reconcile_warning_in_high_scale(monkeypatch):
    """When Faro reconcile fails in High-Scale mode, run status is 'warning' but discovery proceeds."""
    state = _setup_high_scale_discovery(
        monkeypatch,
        run_id=702,
        reconcile=MagicMock(side_effect=RuntimeError("Transient unpkg 503")),
        discover=lambda sid, prefix_subpath=None, run_id=None: 2,
    )
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)

    assert len(state["log_calls"]) == 1
    args, kwargs = state["log_calls"][0]
    assert args[3] == "warning"
    assert kwargs.get("files_downloaded") == 10  # 5 slices * 2 files
    assert "Faro bundle reconcile issue" in kwargs.get("summary", "")
    assert "Transient unpkg 503" in kwargs.get("error_message", "")


def test_faro_reconcile_warning_in_standard_mode(monkeypatch, rum_sync_lease):
    """When Faro reconcile returns False in Standard mode, warning is tracked."""
    update_calls = []
    fake_con = MagicMock()
    fake_con.execute.side_effect = lambda sql, params: update_calls.append((sql, params))

    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: False)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync.ingest_rum_logs",
        lambda sid, *a, **kw: iter([("started", 703), ("done", 10)]),
    )
    monkeypatch.setattr("backend.core.metadata.get_con", lambda sid: fake_con)
    monkeypatch.setattr("backend.cron.jobs.rum_commit._run_rum_commit", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)

    # Verify that status was updated to 'warning'
    assert any("UPDATE cron_runs SET status = 'warning'" in sql for sql, _ in update_calls)


# =============================================================================
# 6. Minute-Prefix Incremental Discovery Tests
# =============================================================================


def test_minute_prefix_incremental_discovery_slices(monkeypatch):
    """High-Scale discovery queries exactly 5 minute-slices via rum_minute_list_prefix."""
    discovered_slices = []

    def mock_discover(sid, prefix_subpath=None, run_id=None):
        discovered_slices.append(prefix_subpath)
        return 1

    _setup_high_scale_discovery(monkeypatch, run_id=704, discover=mock_discover)
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)

    assert len(discovered_slices) == 5
    for s in discovered_slices:
        assert s.startswith("raw/rum/")


# =============================================================================
# 7. Chunk Budgeting & Bounded Ingestion (Trap #41) Tests
# =============================================================================


def _setup_rum_ingest_logs_env(monkeypatch, *, run_id: int, new_files: list[str], file_size: int = 100):
    log_calls = []
    download_chunks = []
    monkeypatch.setattr("backend.core.rum_ingest.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: MagicMock())
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda sid, task: run_id)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", MagicMock())
    monkeypatch.setattr("backend.core.ingest._recover_in_flight", MagicMock())
    monkeypatch.setattr("backend.core.metadata.get_ingested_filenames", lambda *a, **k: set())

    file_sizes = {f: file_size for f in new_files}

    def fake_list_gen(**kw):
        yield {"type": "status", "message": "found files"}
        return {"new_files": new_files, "file_sizes": file_sizes, "stranded_already": []}

    def fake_download(s3, chunk, tmpdir):
        download_chunks.append(chunk)
        return {p: f"/fake/{p}" for p in chunk}, []

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)
    monkeypatch.setattr("backend.core.ingest._download_chunk_to_local", fake_download)
    monkeypatch.setattr(
        "backend.core.ingest._parse_rum_beacon_file",
        lambda p, sid: ([{"pathname": "/", "metric_name": "pageview", "metric_value": 1.0}], [], []),
    )
    monkeypatch.setattr("backend.core.iceberg.write_to_buffer", MagicMock())
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", MagicMock())
    return log_calls, download_chunks


def test_trap_41_chunk_zero_guaranteed_even_if_budget_expired(monkeypatch):
    """Trap #41: ingest_rum_logs respects max_seconds but always processes chunk 0."""
    from backend.core import rum_ingest as rum_ingest_mod

    fake_files = [f"s3://test-rum-bucket/raw/rum/beacon_{i:03d}.gz" for i in range(100)]
    _, download_chunks = _setup_rum_ingest_logs_env(monkeypatch, run_id=705, new_files=fake_files, file_size=100)

    t0 = 1000.0
    times = [t0, t0 + 5.0, t0 + 200.0, t0 + 205.0]
    monkeypatch.setattr(rum_ingest_mod.time, "time", lambda: times.pop(0) if times else 2000.0)

    events = list(rum_ingest_mod.ingest_rum_logs(_RUM_DISC_SERVICE_ID, max_seconds=10))

    assert len(download_chunks) == 1
    assert len(download_chunks[0]) == 50
    assert any(evt[0] == "status" and evt[1] == "budget" for evt in events)


# =============================================================================
# 8. Post-Sync Immediate Commit Tests
# =============================================================================


def _run_sync_with_beacon_total(monkeypatch, *, run_id: int, total: int) -> MagicMock:
    commit_mock = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.rum_commit._run_rum_commit", commit_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync.ingest_rum_logs",
        lambda sid, *a, **kw: iter([("started", run_id), ("done", total)]),
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)
    return commit_mock


def test_post_sync_immediate_commit_triggered_on_new_beacons(monkeypatch, rum_sync_lease):
    """When new beacons arrive (total > 0), _run_rum_commit is triggered immediately."""
    commit_mock = _run_sync_with_beacon_total(monkeypatch, run_id=706, total=42)
    commit_mock.assert_called_once_with(_RUM_DISC_SERVICE_ID)


def test_post_sync_immediate_commit_not_triggered_on_zero_beacons(monkeypatch, rum_sync_lease):
    """When zero new beacons arrive (total == 0), _run_rum_commit is NOT triggered."""
    commit_mock = _run_sync_with_beacon_total(monkeypatch, run_id=707, total=0)
    commit_mock.assert_not_called()


# =============================================================================
# 9. Accounting & Outcome Counters Tests
# =============================================================================


def test_accounting_outcome_counters_in_cron_runs(monkeypatch):
    """Outcome counters are computed, normalized, and logged in cron_runs."""
    from backend.core import rum_ingest as rum_ingest_mod

    fake_file = "s3://test-rum-bucket/raw/rum/beacon_001.gz"
    log_calls, _ = _setup_rum_ingest_logs_env(monkeypatch, run_id=708, new_files=[fake_file], file_size=50)

    list(rum_ingest_mod.ingest_rum_logs(_RUM_DISC_SERVICE_ID))

    assert len(log_calls) == 1
    _, kwargs = log_calls[0]
    counters = kwargs.get("outcome_counters")
    assert counters is not None
    assert counters["objects_processed"] == 1
    assert counters["objects_successful"] == 1
    assert counters["objects_failed"] == 0


# =============================================================================
# 10. Guaranteed Cleanup Tests
# =============================================================================


def test_guaranteed_cleanup_rum_discovery(monkeypatch):
    """end_progress and finalize_cron_run_if_running are guaranteed in finally on error."""
    state = _setup_high_scale_discovery(
        monkeypatch,
        run_id=709,
        discover=MagicMock(side_effect=RuntimeError("FOS connection aborted")),
    )
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)

    state["end_progress"].assert_called_once_with(709)
    state["finalize"].assert_called_once_with(FAKE_SRC_HIGH_SCALE, "rum_discovery", 709)


def test_guaranteed_cleanup_rum_sync(monkeypatch, rum_sync_lease):
    """end_progress and cleanup_progress_and_reap are guaranteed in finally on error."""
    end_progress_mock = MagicMock()
    reap_mock = MagicMock()

    def failing_ingest(sid, **kw):
        yield ("started", 710)
        raise RuntimeError("Disk full")

    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", failing_ingest)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", reap_mock)

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)

    end_progress_mock.assert_called_once_with(710)
    reap_mock.assert_called_once()


# =============================================================================
# 11. Manual Trigger Endpoint Tests
# =============================================================================


def test_manual_trigger_rum_sync_success():
    """POST /api/admin/rum/sync/{service_id} returns 200, run_id, and starts execution."""
    with _override_service_deps(FAKE_SRC_STANDARD) as client:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=801),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_sync._run_rum_sync"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_DISC_SERVICE_ID},
            )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == 801
        assert "started" in body["message"].lower()


def test_manual_trigger_rum_sync_reentrancy():
    """POST /api/admin/rum/sync/{service_id} returns 200 with already running message."""
    with _override_service_deps(FAKE_SRC_STANDARD) as client:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
            patch(
                "backend.cron_progress.list_active_runs",
                return_value=[{"service_id": _RUM_DISC_SERVICE_ID, "task": "rum_sync", "run_id": 802}],
            ),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_DISC_SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == 802
        assert "already running" in body["message"].lower()


def test_manual_trigger_rum_sync_busy_503():
    """POST /api/admin/rum/sync/{service_id} surfaces 503 cron_busy when lease cannot be acquired."""
    with _override_service_deps(FAKE_SRC_STANDARD) as client:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Lease lock busy")),
            patch("backend.cron_progress.list_active_runs", return_value=[]),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_DISC_SERVICE_ID},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["detail"]["error"] == "cron_busy"


def test_manual_trigger_rum_sync_read_only_rejected_403():
    """POST /api/admin/rum/sync/{service_id} on read-only service returns 403."""
    from tests.cron.conftest import assert_manual_trigger_read_only_rejected

    assert_manual_trigger_read_only_rejected(
        f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}", _RUM_DISC_SERVICE_ID, FAKE_SRC_STANDARD
    )


def test_manual_trigger_rum_sync_non_admin_rejected_403():
    """POST /api/admin/rum/sync/{service_id} by non-admin returns 403."""
    from tests.cron.conftest import assert_manual_trigger_non_admin_rejected

    assert_manual_trigger_non_admin_rejected(
        f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}", _RUM_DISC_SERVICE_ID, FAKE_SRC_STANDARD
    )


def test_manual_trigger_rum_discovery_success():
    """POST /api/admin/rum/discovery/{service_id} returns 200, run_id, and starts job."""
    with _override_service_deps(FAKE_SRC_HIGH_SCALE) as client:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=803),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_ledger._run_rum_discovery_cron"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/rum/discovery/{_RUM_DISC_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_DISC_SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == 803
        assert "started" in body["message"].lower()


def test_manual_trigger_rum_discovery_standard_mode_rejected_400():
    """POST /api/admin/rum/discovery/{service_id} in standard mode returns 400."""
    from tests.cron.conftest import assert_manual_trigger_standard_mode_rejected

    assert_manual_trigger_standard_mode_rejected(
        f"/api/admin/rum/discovery/{_RUM_DISC_SERVICE_ID}", _RUM_DISC_SERVICE_ID, FAKE_SRC_STANDARD
    )


def test_manual_trigger_rum_discovery_read_only_rejected_403():
    """POST /api/admin/rum/discovery/{service_id} on read-only service returns 403."""
    from tests.cron.conftest import assert_manual_trigger_read_only_rejected

    assert_manual_trigger_read_only_rejected(
        f"/api/admin/rum/discovery/{_RUM_DISC_SERVICE_ID}", _RUM_DISC_SERVICE_ID, FAKE_SRC_HIGH_SCALE
    )


def test_manual_trigger_rum_discovery_busy_503():
    """POST /api/admin/rum/discovery/{service_id} returns 503 on lease conflict."""
    with _override_service_deps(FAKE_SRC_HIGH_SCALE) as client:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Lease locked")),
            patch("backend.cron_progress.list_active_runs", return_value=[]),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/rum/discovery/{_RUM_DISC_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_DISC_SERVICE_ID},
            )

        assert resp.status_code == 503
        assert resp.json()["detail"]["error"] == "cron_busy"


# =============================================================================
# 12. Broker Configuration & Availability Error Tests (Restored from test_rum_discovery.py)
# =============================================================================


def test_rum_discovery_missing_broker_error(monkeypatch):
    """When CELERY_BROKER_URL is empty, rum_discovery logs an error run and exits without discovering."""
    discover_calls = []
    state = _setup_high_scale_discovery(
        monkeypatch,
        run_id=99,
        broker_url="",
        discover=lambda *a, **kw: discover_calls.append(a),
    )

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)

    assert len(discover_calls) == 0, "Should not attempt discovery without broker URL"
    assert len(state["log_calls"]) == 1
    args, kw = state["log_calls"][0]
    assert args[1] == "rum_discovery"
    assert args[3] == "error"
    assert "no broker URL" in (kw.get("summary") or "")
    assert "CELERY_BROKER_URL" in (kw.get("error_message") or "")
    assert kw.get("run_id") == 99


def test_rum_discovery_broker_unavailable_error(monkeypatch):
    """When broker enqueue fails during prefix discovery, rum_discovery records error status and error_message."""

    def _raise_broker_err(*_a, **_kw):
        raise ConnectionError("Valkey broker connection refused")

    state = _setup_high_scale_discovery(monkeypatch, run_id=100, discover=_raise_broker_err)
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)

    assert len(state["log_calls"]) == 1
    args, kw = state["log_calls"][0]
    assert args[1] == "rum_discovery"
    assert args[3] == "error"
    assert kw.get("summary") == "RUM discovery failed"
    assert "Valkey broker connection refused" in (kw.get("error_message") or "")
    assert kw.get("run_id") == 100


# =============================================================================
# 13. Lease & Wrapper Lifecycle Tests (Folded from test_rum_ledger_lifecycle / test_rum_sync_lifecycle)
# =============================================================================


def test_discovery_lease_held_is_clean_skip(monkeypatch):
    """When start_cron_run raises CronLeaseHeldError, _run_rum_discovery_cron skips cleanly."""
    log_mock = MagicMock()
    end_progress_mock = MagicMock()

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://x")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=CronLeaseHeldError("held")),
    )
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_mock)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)

    log_mock.assert_not_called()
    end_progress_mock.assert_not_called()


def test_rum_sync_reuses_externally_owned_run_id(monkeypatch, rum_sync_lease):
    """When run_id is passed into _run_rum_sync, it does not re-acquire a lease."""
    start_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    seen: dict = {}

    def fake_ingest(service_id, *args, **kwargs):
        seen.update(kwargs)
        yield ("done", 0)

    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", fake_ingest)

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID, run_id=99)

    start_mock.assert_not_called()
    assert seen["run_id"] == 99
    assert seen["max_seconds"] == 240


def test_rum_sync_skips_cleanly_when_lease_already_held(monkeypatch):
    """When start_cron_run raises CronLeaseHeldError, _run_rum_sync exits cleanly."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=CronLeaseHeldError("rum_sync is already running")),
    )
    log_mock = MagicMock()
    ingest_mock = MagicMock()
    start_progress_mock = MagicMock()
    end_progress_mock = MagicMock()

    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", ingest_mock)
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress_mock)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)

    ingest_mock.assert_not_called()
    log_mock.assert_not_called()
    start_progress_mock.assert_not_called()
    end_progress_mock.assert_not_called()


def test_rum_sync_unknown_service_is_a_clean_skip(monkeypatch):
    """When get_source_for_service returns None, _run_rum_sync exits without acquiring a lease."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    start_mock = MagicMock()
    ingest_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", ingest_mock)

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)

    ingest_mock.assert_not_called()
    start_mock.assert_not_called()


# ── Merged from test_rum_commit_contract.py ──────────────────────────────────────────────

import os
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.core.iceberg.rum_schema import CLIENT_VITALS_ARROW_SCHEMA

from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.iceberg.buffer import _commit_buffer_impl
from backend.core.local_compaction import compact_local_partitions
from backend.cron.jobs import rum_commit as rum_commit_mod

_RUM_COMMIT_SERVICE_ID = "svc_rum_contract_test"

_RUM_COMMIT_FAKE_CFG = {
    "service_id": _RUM_COMMIT_SERVICE_ID,
    "name": "RUM Contract Test Service",
    "provisioning": {"cron_sync": {"enabled": True}},
    "rum": {"enabled": True},
}

_RUM_COMMIT_FAKE_SRC = {
    "service_id": _RUM_COMMIT_SERVICE_ID,
    "name": "RUM Contract Test Service",
    "access_level": "read_write",
}


def _setup_rum_commit_env(monkeypatch, *, run_id: int = 1101, disk_ok: tuple[bool, str] = (True, "")) -> dict:
    log_calls: list = []
    ledger_calls: list = []
    status_calls: list = []
    sync_calls: list = []
    start_progress = MagicMock()
    end_progress = MagicMock()
    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_COMMIT_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_COMMIT_FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: run_id)
    monkeypatch.setattr("backend.cron.scheduler._check_disk_space", lambda dir, sid, task: disk_ok)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr(
        "backend.core.ingest._mark_ledger_published", lambda sid, rum=False: ledger_calls.append((sid, rum))
    )
    monkeypatch.setattr("backend.config.update_status", lambda sid, status: status_calls.append((sid, status)))
    monkeypatch.setattr("backend.core.iceberg.sync_data", lambda src, table_name: sync_calls.append(table_name))
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    return {
        "log_calls": log_calls,
        "ledger_calls": ledger_calls,
        "status_calls": status_calls,
        "sync_calls": sync_calls,
        "start_progress": start_progress,
        "end_progress": end_progress,
    }


def _make_vital_row(timestamp: datetime, **overrides) -> dict:
    base = {
        "timestamp": timestamp,
        "metric_name": "LCP",
        "metric_value": 2.5,
        "metric_rating": "good",
        "pathname": "/home",
        "browser": "Chrome",
        "os": "macOS",
        "device": "Desktop",
        "cid": "cid_user1",
        "req_id": "req_100",
        "city": "Denver",
        "region": "CO",
        "country": "US",
        "pop": "DEN",
        "tls": "1.3",
        "ttfb": 0.12,
    }
    base.update(overrides)
    return base


@pytest.fixture
def mock_orchestration_env(monkeypatch):

    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []
        yield mock_con

    import backend.core.iceberg._ducklake as _dl

    monkeypatch.setattr("backend.core.duckdb.get_connection", mock_get_connection)
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock())
    monkeypatch.setattr(_dl, "_ducklake_attach", MagicMock())


def test_contract_full_commit_success_lifecycle(monkeypatch, mock_orchestration_env):
    """Full commit success contract:
    - Status is 'success'
    - Rows and files committed across both tables recorded
    - Ledger published (rum=True)
    - SSE sync-status notification published
    - Progress cleanly starts and ends
    """
    sse_published = []
    state = _setup_rum_commit_env(monkeypatch, run_id=1101)

    def mock_commit(src, table_name):
        if table_name == "client_vitals":
            return {"files_committed": 3, "rows_committed": 45}
        if table_name == "client_errors":
            return {"files_committed": 2, "rows_committed": 15}
        return {}

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)

    latest = datetime(2026, 10, 11, 14, 30, tzinfo=UTC)
    monkeypatch.setattr("backend.core.rollups.rum.rum_count_and_latest", lambda con: (60, latest))

    mock_pub = MagicMock()
    mock_pub.publish.side_effect = lambda sid, snap: sse_published.append((sid, snap))
    monkeypatch.setattr("backend.sync_status_publisher.publisher", mock_pub)
    monkeypatch.setattr(
        "backend.sync_status_snapshot.compute_sync_status_cached",
        lambda sid: {"service_id": sid, "sync_stage": "idle"},
    )

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)

    state["start_progress"].assert_called_once_with(1101, service_id=_RUM_COMMIT_SERVICE_ID, task="rum_commit")
    state["end_progress"].assert_called_once_with(1101)

    # Ledger publication verified
    assert state["ledger_calls"] == [(_RUM_COMMIT_SERVICE_ID, True)]

    # Cron run log verified
    assert len(state["log_calls"]) == 1
    args, kwargs = state["log_calls"][0]
    assert args[3] == "success"
    assert kwargs.get("run_id") == 1101
    assert kwargs.get("rows_ingested") == 60
    assert "Committed 3 vitals files (45 rows) and 2 errors files (15 rows)" in kwargs.get("summary", "")

    # SSE notification verified
    assert len(sse_published) == 1
    assert sse_published[0][0] == _RUM_COMMIT_SERVICE_ID

    # Freshness update verified
    rum_updates = [s for _, s in state["status_calls"] if "rum" in s]
    assert rum_updates
    assert rum_updates[-1]["rum"]["total_rows"] == 60
    assert rum_updates[-1]["rum"]["latest_log_at"] == latest.isoformat()


def test_contract_partial_failure_warning_and_ledger_protection(monkeypatch, mock_orchestration_env):
    """Partial failure contract:
    - If client_errors fails, status is 'warning'
    - _mark_ledger_published is NOT called (preserves raw files)
    - Successful table (client_vitals) syncs metadata
    - Exception is safely handled without raising
    """
    state = _setup_rum_commit_env(monkeypatch, run_id=1102)

    def mock_commit(src, table_name):
        if table_name == "client_vitals":
            return {"files_committed": 2, "rows_committed": 30}
        raise OSError("Transient FOS S3 error on errors commit")

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)

    # Client vitals metadata sync occurred
    assert "client_vitals" in state["sync_calls"]
    # Ledger publication was BLOCKED
    assert state["ledger_calls"] == []

    assert len(state["log_calls"]) == 1
    args, kwargs = state["log_calls"][0]
    assert args[3] == "warning"
    assert kwargs.get("rows_ingested") == 30
    assert "Partial RUM commit" in kwargs.get("summary", "")
    assert "errors failed" in kwargs.get("summary", "")
    assert "Transient FOS S3 error" in kwargs.get("error_message", "")


def test_contract_both_tables_failure_error_status(monkeypatch, mock_orchestration_env):
    """Both tables failure contract:
    - Status is 'error'
    - Summary and error message describe both table failures
    - Ledger publication is blocked
    """
    state = _setup_rum_commit_env(monkeypatch, run_id=1103)

    def mock_commit(src, table_name):
        if table_name == "client_vitals":
            raise RuntimeError("Vitals commit network timeout")
        raise RuntimeError("Errors commit network timeout")

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)

    assert state["ledger_calls"] == []
    assert len(state["log_calls"]) == 1
    args, kwargs = state["log_calls"][0]
    assert args[3] == "error"
    assert "RUM commit failed for both tables" in kwargs.get("summary", "")
    assert "Vitals commit network timeout" in kwargs.get("error_message", "")
    assert "Errors commit network timeout" in kwargs.get("error_message", "")


def test_contract_low_disk_precheck_abort(monkeypatch, mock_orchestration_env):
    """Disk space safety contract:
    - When disk space is below safety threshold (<500MB), abort with status 'error'
    - Commit and sync are never attempted
    """
    commit_calls = []
    state = _setup_rum_commit_env(
        monkeypatch, run_id=1104, disk_ok=(False, "Free disk space is 250MB (< 500MB minimum)")
    )
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        lambda src, table_name: commit_calls.append(table_name),
    )

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)

    assert commit_calls == []
    assert len(state["log_calls"]) == 1
    args, kwargs = state["log_calls"][0]
    assert args[3] == "error"
    assert "RUM commit aborted" in kwargs.get("summary", "")
    assert "Free disk space is 250MB" in kwargs.get("error_message", "")


def test_contract_automated_tick_no_deferral(monkeypatch, mock_orchestration_env):
    """Active-requests politeness gate removal contract (commit 5a7541fd):
    - Automated ticks proceed immediately and are NOT deferred by in-flight queries.
    """
    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_COMMIT_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_COMMIT_FAKE_SRC)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        lambda src, task: start_calls.append(task) or 1105,
    )
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: True)

    with patch("backend.cron.scheduler._check_disk_space", return_value=(False, "disk check")):
        with patch("backend.core.duckdb.log_cron_run"):
            rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)
            # The run proceeded to start_cron_run without being deferred
            assert len(start_calls) == 1
            assert start_calls[0] == "rum_commit"


def test_contract_rum_buffer_natural_key_deduplication(tmp_path):
    """DuckLake buffer commit natural key deduplication contract:
    - client_vitals commits deduplicate rows by natural keys:
      ("timestamp", "metric_name", "pathname", "cid", "req_id")
    - Repeated or overlapping buffer files do NOT generate duplicate rows in DuckLake.
    """
    import duckdb

    cache_dir = tmp_path / "cache"
    buf_dir = cache_dir / "buffer" / "client_vitals"
    buf_dir.mkdir(parents=True)

    src = {
        "name": "rum_dedup_svc",
        "service_id": "rum_dedup_svc",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache_dir),
        "duckdb_path": str(tmp_path / "rum_dedup.duckdb"),
        "ducklake_dir": str(tmp_path / "lake"),
    }

    ts1 = datetime(2026, 10, 11, 10, 0, tzinfo=UTC)
    ts2 = datetime(2026, 10, 11, 10, 5, tzinfo=UTC)
    beacon_a = _make_vital_row(ts1)
    beacon_b = _make_vital_row(
        ts2, metric_name="CLS", metric_value=0.05, pathname="/about", cid="cid_user2", req_id="req_101", ttfb=0.08
    )
    beacon_c = _make_vital_row(
        ts2, metric_name="FID", metric_value=15.0, pathname="/contact", cid="cid_user3", req_id="req_102", ttfb=0.09
    )

    # Chunk 1: Beacon A (LCP) and Beacon B (CLS)
    pq.write_table(
        pa.Table.from_pylist([beacon_a, beacon_b], schema=CLIENT_VITALS_ARROW_SCHEMA),
        str(buf_dir / "buf1.parquet"),
    )

    res1 = _commit_buffer_impl(src, table_name="client_vitals")
    assert res1["files_committed"] == 1
    assert res1["rows_committed"] == 2

    # Chunk 2: Duplicate of Beacon A + New Beacon C (FID)
    pq.write_table(
        pa.Table.from_pylist([beacon_a, beacon_c], schema=CLIENT_VITALS_ARROW_SCHEMA),
        str(buf_dir / "buf2.parquet"),
    )

    res2 = _commit_buffer_impl(src, table_name="client_vitals")
    assert res2["files_committed"] == 1

    # Verify table in DuckLake contains exactly 3 distinct rows (Beacon A was not duplicated)
    con = duckdb.connect()
    try:
        _ducklake_attach(con, src, read_only=True)
        tbl_name = ducklake_table_name(src, table_name="client_vitals")
        count = con.execute(f'SELECT count(*) FROM lake."{tbl_name}"').fetchone()[0]
        assert count == 3, f"Expected 3 unique rows after deduplicating commit, got {count}"
    finally:
        con.close()


def test_contract_rum_local_compaction_natural_key_deduplication(tmp_path):
    """Local compaction natural key deduplication contract:
    - compact_local_partitions on client_vitals merges files and deduplicates
      by composite natural key.
    """
    import duckdb

    cache_dir = tmp_path / "cache"
    now = datetime.now(UTC)
    part_dir = cache_dir / "data_client_vitals" / now.strftime("timestamp_hour=%Y-%m-%d-%H")
    part_dir.mkdir(parents=True)

    src = {
        "name": "rum_compact_svc",
        "service_id": "rum_compact_svc",
        "_cache_dir_override": str(cache_dir),
    }

    ts = now.replace(minute=15, second=0, microsecond=0)
    common_kw = {
        "pathname": "/product/1",
        "browser": "Firefox",
        "os": "Linux",
        "cid": "cid_42",
        "city": "Austin",
        "region": "TX",
        "pop": "DFW",
        "ttfb": 0.25,
    }
    beacon_a = _make_vital_row(
        ts, metric_name="LCP", metric_value=3.1, metric_rating="needs-improvement", req_id="req_555", **common_kw
    )
    beacon_b = _make_vital_row(
        ts, metric_name="CLS", metric_value=0.02, metric_rating="good", req_id="req_556", **common_kw
    )

    # File 1 has Beacon A; File 2 has exact duplicate of Beacon A + new Beacon B
    pq.write_table(
        pa.Table.from_pylist([beacon_a], schema=CLIENT_VITALS_ARROW_SCHEMA),
        str(part_dir / "part1.parquet"),
    )
    pq.write_table(
        pa.Table.from_pylist([beacon_a, beacon_b], schema=CLIENT_VITALS_ARROW_SCHEMA),
        str(part_dir / "part2.parquet"),
    )

    res = compact_local_partitions(
        src,
        min_files_per_partition=1,
        table_name="client_vitals",
        compact_active_hour=True,
    )
    assert res["partitions_compacted"] == 1
    assert res["files_merged"] == 2

    remaining_files = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(remaining_files) == 1

    con = duckdb.connect()
    try:
        merged_path = str(part_dir / remaining_files[0])
        rows = con.execute(f"SELECT * FROM read_parquet('{merged_path}')").fetchall()
        assert len(rows) == 2, f"Expected 2 rows in compacted parquet, found {len(rows)}"
    finally:
        con.close()


def test_rum_commit_aggregates_self_heal_on_stale_lake(monkeypatch):
    """RUM aggregate recompute self-heals the cold-start lake-detach race (Trap #35)."""
    import duckdb

    recompute_calls = {"n": 0}

    def flaky_recompute(con, service_id, hours=None):
        recompute_calls["n"] += 1
        if recompute_calls["n"] == 1:
            raise duckdb.CatalogException(
                'Catalog Error: Table with name "lake.logs_svc__client_vitals" '
                'does not exist because schema "lake" does not exist.'
            )
        return None

    def _retry_double(con, source, fn, *args, table_name="logs", **kwargs):
        try:
            return fn(con, *args, **kwargs)
        except Exception:
            return fn(con, *args, **kwargs)

    _setup_rum_commit_env(monkeypatch, run_id=1004)
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer", lambda src, table_name: {"files_committed": 1, "rows_committed": 5}
    )
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", flaky_recompute)
    monkeypatch.setattr("backend.core.iceberg.execute_with_stale_view_retry", _retry_double)

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)

    assert recompute_calls["n"] == 2, "recompute must retry once after the stale-lake self-heal"


def test_rum_commit_lease_held_is_clean_skip_with_no_row(monkeypatch):
    """When start_cron_run raises CronLeaseHeldError, _run_rum_commit exits cleanly without side effects."""
    log_mock = MagicMock()
    commit_mock = MagicMock()
    end_progress_mock = MagicMock()

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_COMMIT_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_COMMIT_FAKE_SRC)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        MagicMock(side_effect=CronLeaseHeldError("held")),
    )
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_mock)
    monkeypatch.setattr("backend.core.iceberg.commit_buffer", commit_mock)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)

    log_mock.assert_not_called()
    commit_mock.assert_not_called()
    end_progress_mock.assert_not_called()


# ── Merged from test_ledger_rum_sweep_contract.py ──────────────────────────────────────────────

import time

from backend.deps import require_admin

_RUM_SWEEP_SERVICE_ID = "svc_test_rum_ledger_sweep"

_RUM_SWEEP_FAKE_CFG = {
    "service_id": _RUM_SWEEP_SERVICE_ID,
    "name": _RUM_SWEEP_SERVICE_ID,
    "deployment_mode": "high_scale",
    "access_level": "read_write",
    "rum": {"enabled": True},
    "provisioning": {
        "access_level": "read_write",
        "cron_ledger_rum_sweep": {"enabled": True, "interval_minutes": 15},
    },
}

_RUM_SWEEP_FAKE_SRC = {
    "service_id": _RUM_SWEEP_SERVICE_ID,
    "name": _RUM_SWEEP_SERVICE_ID,
    "access_level": "read_write",
    "deployment_mode": "high_scale",
    "bucket": "test-rum-sweep-bucket",
    "provisioning": {
        "access_level": "read_write",
        "cron_ledger_rum_sweep": {"enabled": True, "interval_minutes": 15},
    },
}


# -----------------------------------------------------------------------------
# 1. Manual Trigger Endpoint Tests

# -----------------------------------------------------------------------------


def test_manual_trigger_rum_sweep_endpoint_success():
    """POST /api/admin/rum/ledger/sweep/{service_id} returns 200, run_id, and starts job."""
    app.dependency_overrides[get_source] = lambda: _RUM_SWEEP_FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: _RUM_SWEEP_SERVICE_ID
    try:
        with (
            patch("backend.config.load_config", return_value=_RUM_SWEEP_FAKE_CFG),
            patch("backend.core.duckdb.start_cron_run", return_value="run-rum-sweep-101"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_ledger._run_rum_ledger_sweep"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_SWEEP_SERVICE_ID},
            )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-rum-sweep-101"
        assert "started" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sweep_endpoint_reentrancy():
    """POST /api/admin/rum/ledger/sweep/{service_id} returns 200 with already running message."""
    app.dependency_overrides[get_source] = lambda: _RUM_SWEEP_FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: _RUM_SWEEP_SERVICE_ID
    try:
        with (
            patch("backend.config.load_config", return_value=_RUM_SWEEP_FAKE_CFG),
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
            patch(
                "backend.cron_progress.list_active_runs",
                return_value=[
                    {"service_id": _RUM_SWEEP_SERVICE_ID, "task": "ledger_rum_sweep", "run_id": "run-rum-active"}
                ],
            ),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_SWEEP_SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-rum-active"
        assert "already running" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sweep_endpoint_standard_mode_rejected():
    """POST /api/admin/rum/ledger/sweep/{service_id} on standard mode returns 400."""
    from tests.cron.conftest import assert_manual_trigger_standard_mode_rejected

    resp = assert_manual_trigger_standard_mode_rejected(
        f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}", _RUM_SWEEP_SERVICE_ID, _RUM_SWEEP_FAKE_SRC
    )
    assert resp.json()["detail"]["error"] == "high_scale_required"


def test_manual_trigger_rum_sweep_endpoint_rum_disabled_rejected():
    """POST /api/admin/rum/ledger/sweep/{service_id} when RUM is disabled returns 400."""
    no_rum_cfg = {**_RUM_SWEEP_FAKE_CFG, "rum": {"enabled": False}}
    app.dependency_overrides[get_source] = lambda: _RUM_SWEEP_FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: _RUM_SWEEP_SERVICE_ID
    try:
        with patch("backend.config.load_config", return_value=no_rum_cfg):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}",
                headers={"x-fastly-service-id": _RUM_SWEEP_SERVICE_ID},
            )
        assert resp.status_code == 400
        assert resp.json()["detail"]["error"] == "rum_not_enabled"
        assert "rum is not enabled" in resp.json()["detail"]["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sweep_endpoint_read_only_rejected():
    """POST /api/admin/rum/ledger/sweep/{service_id} on read-only service returns 403."""
    from tests.cron.conftest import assert_manual_trigger_read_only_rejected

    resp = assert_manual_trigger_read_only_rejected(
        f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}", _RUM_SWEEP_SERVICE_ID, _RUM_SWEEP_FAKE_SRC
    )
    assert resp.json()["detail"]["error"] == "admin_only"


def test_manual_trigger_rum_sweep_endpoint_non_admin_rejected():
    """POST /api/admin/rum/ledger/sweep/{service_id} by non-admin returns 403."""
    from tests.cron.conftest import assert_manual_trigger_non_admin_rejected

    assert_manual_trigger_non_admin_rejected(
        f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}", _RUM_SWEEP_SERVICE_ID, _RUM_SWEEP_FAKE_SRC
    )


# -----------------------------------------------------------------------------
# 2. Quarantine Inspection Endpoint Tests
# -----------------------------------------------------------------------------


def test_ledger_quarantine_endpoint_with_source_type_rum(monkeypatch):
    """GET /api/admin/ledger/quarantine?service_id={service_id}&source_type=rum filters by source_type."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    mock_items = [
        {
            "id": 10,
            "source_type": "rum",
            "original_key": "raw/rum/2026/08/27/10/05/bad_beacon.json.gz",
            "line_ordinal": 3,
            "byte_offset": 50,
            "byte_length": 128,
            "error_category": "rum_schema_invalid",
            "error_text": "Missing session_id",
            "sha256": "abcdef1234567890rum",
            "quarantined_at": "2026-08-27T10:05:00+00:00",
        }
    ]

    list_calls = []

    def mock_list(sid, limit=100, offset=0, error_category=None, source_type=None):
        list_calls.append({"sid": sid, "error_category": error_category, "source_type": source_type})
        return mock_items

    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", side_effect=mock_list),
        patch("backend.routers.admin.quarantine.count_quarantine_evidence", return_value=1),
    ):
        client = TestClient(app)
        resp = client.get(f"/api/admin/ledger/quarantine?service_id={_RUM_SWEEP_SERVICE_ID}&source_type=rum")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1
    assert body["items"][0]["source_type"] == "rum"
    assert len(list_calls) == 1
    assert list_calls[0]["source_type"] == "rum"


def test_quarantine_endpoint_with_source_type_rum(monkeypatch):
    """GET /api/admin/quarantine?source_type=rum filters by source_type."""
    app.dependency_overrides[get_source] = lambda: _RUM_SWEEP_FAKE_SRC
    mock_items = [
        {
            "id": 11,
            "source_type": "rum",
            "original_key": "raw/rum/2026/08/27/10/05/bad_beacon2.json.gz",
            "line_ordinal": 1,
            "byte_offset": 0,
            "byte_length": 64,
            "error_category": "rum_json_malformed",
            "error_text": "Invalid JSON",
            "sha256": "rumhash123",
            "quarantined_at": "2026-08-27T10:06:00+00:00",
        }
    ]

    try:
        with (
            patch("backend.routers.admin.quarantine.list_quarantine_evidence", return_value=mock_items),
            patch("backend.routers.admin.quarantine.count_quarantine_evidence", return_value=1),
        ):
            client = TestClient(app)
            resp = client.get(
                "/api/admin/quarantine?source_type=rum",
                headers={"x-fastly-service-id": _RUM_SWEEP_SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert len(body["items"]) == 1
        assert body["items"][0]["source_type"] == "rum"
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_ledger_quarantine_endpoint_service_not_found(monkeypatch):
    """GET /api/admin/ledger/quarantine?service_id={unknown} returns 404."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)

    client = TestClient(app)
    resp = client.get("/api/admin/ledger/quarantine?service_id=nonexistent-svc")
    assert resp.status_code == 404


def test_ledger_quarantine_endpoint_read_only_rejected(monkeypatch):
    """GET /api/admin/ledger/quarantine on read-only service returns 403."""
    ro_src = {**_RUM_SWEEP_FAKE_SRC, "access_level": "read_only"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)

    client = TestClient(app)
    resp = client.get(f"/api/admin/ledger/quarantine?service_id={_RUM_SWEEP_SERVICE_ID}")
    assert resp.status_code == 403


def test_ledger_quarantine_endpoint_non_admin_rejected():
    """GET /api/admin/ledger/quarantine by non-admin returns 403."""
    from fastapi import HTTPException

    def reject_analyst():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_analyst
    try:
        client = TestClient(app)
        resp = client.get(f"/api/admin/ledger/quarantine?service_id={_RUM_SWEEP_SERVICE_ID}")
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.pop(require_admin, None)


# -----------------------------------------------------------------------------
# 3. Execution Lifecycle & Step-by-Step Logic (_run_rum_ledger_sweep)
# -----------------------------------------------------------------------------


def test_run_rum_ledger_sweep_success(monkeypatch):
    """Under high-scale mode with RUM enabled:
    - Runs sweep_rum_ledger_once
    - Records status 'success' with reclaim/redispatch/discovery counts
    - Starts and ends progress cleanly, emits progress status and done events, finalizes duration
    """
    log_calls = []
    finalize_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 888)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "backend.cron.jobs._common.finalize_cron_duration",
        lambda *args, **kwargs: finalize_calls.append((args, kwargs)),
    )

    sweep_result = {
        "reclaimed": 3,
        "redispatched": 3,
        "discovered": 7,
        "broker_ok": True,
        "dead_letter": 0,
    }
    sweep_calls = []

    def mock_sweep(sid, run_id=None):
        sweep_calls.append((sid, run_id))
        return sweep_result

    monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", mock_sweep)

    start_progress = MagicMock()
    end_progress = MagicMock()
    progress_mock = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", progress_mock)

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)

    start_progress.assert_called_once_with(888, service_id=_RUM_SWEEP_SERVICE_ID, task="ledger_rum_sweep")
    end_progress.assert_called_once_with(888)
    assert sweep_calls == [(_RUM_SWEEP_SERVICE_ID, 888)]
    assert len(finalize_calls) == 1
    assert progress_mock.call_count >= 2

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 888
    assert args[3] == "success"
    assert kwargs.get("files_downloaded") == 7
    assert "reclaimed=3 redispatched=3 discovered=7" in kwargs.get("summary", "")


def test_run_rum_ledger_sweep_run_id_reuse(monkeypatch):
    """When run_id is passed as an argument, it must NOT call start_cron_run."""
    start_cron_calls = []
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a: start_cron_calls.append(a))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: log_calls.append((a, k)))
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())
    monkeypatch.setattr(
        "backend.core.ingest.sweep_rum_ledger_once",
        lambda sid, run_id=None: {
            "reclaimed": 0,
            "redispatched": 0,
            "discovered": 0,
            "broker_ok": True,
            "dead_letter": 0,
        },
    )

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID, run_id=999)

    assert len(start_cron_calls) == 0
    assert len(log_calls) == 1
    assert log_calls[0][1].get("run_id") == 999


def test_run_rum_ledger_sweep_warning_on_dead_letter_or_broker_down(monkeypatch):
    """When dead-letter rows exist or broker probe fails, status is 'warning'."""
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 889)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())

    sweep_result = {
        "reclaimed": 1,
        "redispatched": 0,
        "discovered": 0,
        "broker_ok": False,
        "dead_letter": 2,
    }
    monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", lambda sid, run_id=None: sweep_result)

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert "Celery broker/queue depth probe failed" in kwargs.get("summary", "")
    assert "2 dead-letter/quarantined RUM row(s)" in kwargs.get("summary", "")


def test_run_rum_ledger_sweep_error_handling_and_guaranteed_cleanup(monkeypatch):
    """When an exception occurs during sweep:
    - Status is recorded as 'error'
    - end_progress and finalize_cron_duration are still executed in finally block
    """
    log_calls = []
    finalize_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 890)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: log_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "backend.cron.jobs._common.finalize_cron_duration",
        lambda *args, **kwargs: finalize_calls.append((args, kwargs)),
    )

    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())

    def fail_sweep(*a, **k):
        raise RuntimeError("Postgres connection dropped")

    monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", fail_sweep)

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)

    end_progress.assert_called_once_with(890)
    assert len(finalize_calls) == 1
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "Postgres connection dropped" in kwargs.get("error_message", "")


def test_run_rum_ledger_sweep_skips_in_standard_mode(monkeypatch):
    """In standard mode, _run_rum_ledger_sweep exits immediately without running."""
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    assert len(start_calls) == 0


def test_run_rum_ledger_sweep_skips_when_read_only(monkeypatch):
    """When service is read_only, _run_rum_ledger_sweep exits immediately."""
    start_calls = []
    ro_src = {**_RUM_SWEEP_FAKE_SRC, "access_level": "read_only"}

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    assert len(start_calls) == 0


def test_run_rum_ledger_sweep_skips_when_rum_disabled(monkeypatch):
    """When RUM is disabled, _run_rum_ledger_sweep exits immediately."""
    start_calls = []
    no_rum_cfg = {**_RUM_SWEEP_FAKE_CFG, "rum": {"enabled": False}}

    monkeypatch.setattr("backend.config.load_config", lambda sid: no_rum_cfg)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    assert len(start_calls) == 0


def test_run_rum_ledger_sweep_skips_when_dev_no_crons(monkeypatch):
    """When FLA_DEV_NO_CRONS=1, _run_rum_ledger_sweep exits immediately."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    assert len(start_calls) == 0


def test_run_rum_ledger_sweep_skips_when_config_missing(monkeypatch):
    """When config cannot be loaded, _run_rum_ledger_sweep exits cleanly."""
    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    assert len(start_calls) == 0


# -----------------------------------------------------------------------------
# 4. Scheduler Registration & Cadence Tests
# -----------------------------------------------------------------------------


def test_scheduler_registers_and_reschedules_rum_ledger_sweep():
    """Scheduler registers ledger_rum_sweep in high-scale mode with RUM and reschedules on interval changes."""
    sid = "svc-scheduler-rum-sweep"
    cfg = {
        "service_id": sid,
        "name": sid,
        "log_period": 60,
        "deployment_mode": "high_scale",
        "access_level": "read_write",
        "rum": {"enabled": True},
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
            "cron_ledger_rum_sweep": {"enabled": True, "interval_minutes": 15},
        },
    }
    src = {
        "name": sid,
        "service_id": sid,
        "bucket": "test-b",
        "access_level": "read_write",
        "deployment_mode": "high_scale",
    }

    s = Scheduler()
    mock_sched = MagicMock()
    s._sched = mock_sched

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_id = f"ledger_rum_sweep_{sid}"
    assert job_id in s._job_ids
    calls = [c for c in mock_sched.add_job.call_args_list if c.kwargs.get("id") == job_id]
    assert len(calls) == 1
    call = calls[0]
    assert call.args[0] == rum_ledger_mod._run_rum_ledger_sweep
    assert call.args[1] == "interval"
    assert call.kwargs["minutes"] == 15
    assert call.kwargs["coalesce"] is True
    assert call.kwargs["misfire_grace_time"] == 300
    assert call.kwargs["args"] == [sid]

    # Now change interval to 20 min
    cfg["provisioning"]["cron_ledger_rum_sweep"]["interval_minutes"] = 20
    mock_job = MagicMock()
    mock_sched.get_job = MagicMock(side_effect=lambda jid: mock_job if jid == job_id else None)
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
        mock_job.reschedule.assert_called_once_with("interval", minutes=20)


def test_scheduler_skips_rum_ledger_sweep_when_disabled():
    """Scheduler skips ledger_rum_sweep when cron_ledger_rum_sweep.enabled is False."""
    sid = "svc-rum-sweep-disabled"
    cfg = {
        "service_id": sid,
        "name": sid,
        "log_period": 60,
        "deployment_mode": "high_scale",
        "access_level": "read_write",
        "rum": {"enabled": True},
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
            "cron_ledger_rum_sweep": {"enabled": False, "interval_minutes": 15},
        },
    }
    src = {
        "name": sid,
        "service_id": sid,
        "bucket": "test-b",
        "access_level": "read_write",
        "deployment_mode": "high_scale",
    }

    s = Scheduler()
    s._sched = MagicMock()

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_id = f"ledger_rum_sweep_{sid}"
    assert job_id not in s._job_ids


def test_scheduler_skips_rum_ledger_sweep_when_rum_disabled():
    """Scheduler skips ledger_rum_sweep when RUM is disabled."""
    sid = "svc-rum-disabled"
    cfg = {
        "service_id": sid,
        "name": sid,
        "log_period": 60,
        "deployment_mode": "high_scale",
        "access_level": "read_write",
        "rum": {"enabled": False},
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
            "cron_ledger_rum_sweep": {"enabled": True, "interval_minutes": 15},
        },
    }
    src = {
        "name": sid,
        "service_id": sid,
        "bucket": "test-b",
        "access_level": "read_write",
        "deployment_mode": "high_scale",
    }

    s = Scheduler()
    s._sched = MagicMock()

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_id = f"ledger_rum_sweep_{sid}"
    assert job_id not in s._job_ids


# -----------------------------------------------------------------------------
# 5. Sweeper Engine Tests (sweep_rum_ledger_once)
# -----------------------------------------------------------------------------


def test_sweep_rum_ledger_reclaims_stale_claims_scoped_to_rum(monkeypatch):
    """sweep_rum_ledger_once reclaims stale claims scoping to raw/rum/% keys."""
    from backend.core.ingest import LEDGER_RECLAIM_AFTER_S, sweep_rum_ledger_once

    sid = "svc-test-rum-reclaim"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"

    now = time.time()
    stale_time = now - LEDGER_RECLAIM_AFTER_S - 60

    executed_statements = []

    class FakeCursor:
        def execute(self, query, params=None):
            executed_statements.append((query, params))

        def fetchall(self):
            for q, p in executed_statements:
                if "UPDATE ingest_ledger SET status='discovered'" in q:
                    return [(rum_key,)]
            return []

    class FakeCon:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def execute(self, query, params=None):
            executed_statements.append((query, params))
            mock_res = MagicMock()
            if "SELECT COUNT(*)" in query:
                mock_res.fetchone.return_value = (0,)
            else:
                mock_res.fetchall.return_value = []
            return mock_res

    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda s: FakeCon())
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: {"prefix": "", "bucket": "b"})
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda *a, **k: 0)
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": 0}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_rum_files.delay", mock_convert)

    summary = sweep_rum_ledger_once(sid)

    assert summary["reclaimed"] == 1
    assert summary["redispatched"] == 1
    mock_convert.assert_called_once_with(sid, [rum_key])

    # Check update statement scoped to raw/rum/%
    update_stmts = [stmt for stmt in executed_statements if "UPDATE ingest_ledger" in stmt[0]]
    assert len(update_stmts) == 1
    query, params = update_stmts[0]
    assert "object_key LIKE ?" in query
    assert "raw/rum/%" in params


def test_sweep_rum_ledger_queue_depth_guard(monkeypatch):
    """When queue depth exceeds pending batches, re-dispatch is skipped."""
    from backend.core.ingest import sweep_rum_ledger_once

    sid = "svc-test-rum-depth-guard"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"

    class FakeCursor:
        def execute(self, query, params=None):
            pass

        def fetchall(self):
            return [(rum_key,)]

    class FakeCon:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def execute(self, query, params=None):
            mock_res = MagicMock()
            if "SELECT COUNT(*)" in query:
                mock_res.fetchone.return_value = (0,)
            else:
                mock_res.fetchall.return_value = []
            return mock_res

    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda s: FakeCon())
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: {"prefix": "", "bucket": "b"})
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda *a, **k: 0)

    # Queue depth is 100 while pending is 1 batch -> should skip
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": 100}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_rum_files.delay", mock_convert)

    summary = sweep_rum_ledger_once(sid)

    assert summary["reclaimed"] == 1
    assert summary["redispatched"] == 0
    mock_convert.assert_not_called()
