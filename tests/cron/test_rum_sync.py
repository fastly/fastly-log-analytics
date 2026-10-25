"""Consolidated tests for RUM sync, discovery, commit, and ledger sweep crons."""

from __future__ import annotations

import copy
import os
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.core.iceberg.rum_schema import CLIENT_VITALS_ARROW_SCHEMA
from fastapi import HTTPException
from starlette.testclient import TestClient

from backend.core import rum_ingest as rum_ingest_mod
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.iceberg.buffer import _commit_buffer_impl
from backend.core.ingest import sweep_rum_ledger_once
from backend.core.local_compaction import compact_local_partitions
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import rum_commit as rum_commit_mod
from backend.cron.jobs import rum_ledger as rum_ledger_mod
from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.cron.scheduler import Scheduler, _resolve_rum_sync_interval
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app
from backend.provision.rum_orchestrator_v2 import rum_vcl_fingerprint
from tests.cron.conftest import (
    assert_manual_trigger_non_admin_rejected,
    assert_manual_trigger_read_only_rejected,
    assert_manual_trigger_standard_mode_rejected,
)

SERVICE_ID = "svc_test"
_RUM_DISC_SERVICE_ID = "svc_test_rum_contract"
_RUM_COMMIT_SERVICE_ID = "svc_rum_contract_test"
_RUM_SWEEP_SERVICE_ID = "svc_test_rum_ledger_sweep"
NOW = 1_800_000_000.0
_REAL_CLIENT = httpx.Client

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
        "faro_fos_etag_md5": "stored-hash-abc123",
        "faro_last_upstream_check": 0,
        "faro_upstream_check_hours": 24,
    },
}

FAKE_CFG_STANDARD: dict[str, Any] = {
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

FAKE_CFG_HIGH_SCALE: dict[str, Any] = {
    **FAKE_CFG_STANDARD,
    "deployment_mode": "high_scale",
    "rum": {"enabled": True, "sync_interval_seconds": 15},
}
FAKE_SRC_HIGH_SCALE = FAKE_CFG_HIGH_SCALE

_RUM_COMMIT_FAKE_CFG: dict[str, Any] = {
    "service_id": _RUM_COMMIT_SERVICE_ID,
    "name": "RUM Contract Test Service",
    "provisioning": {"cron_sync": {"enabled": True}},
    "rum": {"enabled": True},
}
_RUM_COMMIT_FAKE_SRC: dict[str, Any] = {
    "service_id": _RUM_COMMIT_SERVICE_ID,
    "name": "RUM Contract Test Service",
    "access_level": "read_write",
}

_RUM_SWEEP_FAKE_CFG: dict[str, Any] = {
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
_RUM_SWEEP_FAKE_SRC: dict[str, Any] = {
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


# ── Shared Fixtures & Helpers ────────────────────────────────────────────────


@pytest.fixture
def rum_lease(monkeypatch):
    lease = MagicMock()
    lease.source = {"name": SERVICE_ID}
    lease.start = MagicMock(return_value=7)
    lease.log_cron_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: lease.source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lease.log_cron_run)
    return lease


@pytest.fixture
def rum_sync_lease(monkeypatch) -> MagicMock:
    lease = MagicMock()
    lease.start = MagicMock(return_value=710)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", MagicMock())
    return lease


@pytest.fixture
def config_store(monkeypatch):
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
    monkeypatch.setattr(rum_sync_mod.time, "time", lambda: NOW)
    return NOW


@pytest.fixture
def mock_fos(monkeypatch):
    def install(handler):
        def factory(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return _REAL_CLIENT(*args, **kwargs)

        monkeypatch.setattr(httpx, "Client", factory)

    return install


@pytest.fixture
def mock_download(monkeypatch):
    calls: list[tuple] = []

    def install(*, raises: Exception | None = None):
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
    calls: list[tuple] = []

    def fake(method, path, *, token, expect_empty=False, **kwargs):
        calls.append((method, path, token))

    monkeypatch.setattr("backend.core.fastly.client.fastly", fake)
    return calls


@pytest.fixture
def mock_reconcile(monkeypatch):
    calls: list[tuple] = []

    def fake(service_id, token, *args, **kwargs):
        calls.append((service_id, token))
        return MagicMock(activated_version=2, draft_version=None)

    monkeypatch.setattr("backend.provision.declarative.reconciler.reconcile_vcl_state", fake)
    return calls


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


def _cfg(**rum_overrides) -> dict:
    cfg = copy.deepcopy(FAKE_CFG)
    cfg["rum"].update(rum_overrides)
    return cfg


def _head_response(status: int, etag: str | None = None) -> httpx.Response:
    headers = {"ETag": f'"{etag}"'} if etag is not None else {}
    return httpx.Response(status, headers=headers)


def _mock_progress(monkeypatch) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    start_p, add_p, end_p, cleanup_p = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_p)
    monkeypatch.setattr("backend.cron_progress.add_progress", add_p)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_p)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup_p)
    return start_p, add_p, end_p, cleanup_p


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
    finalize_mock = MagicMock()
    _, _, end_progress_mock, _ = _mock_progress(monkeypatch)
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
    return {"log_calls": log_calls, "end_progress": end_progress_mock, "finalize": finalize_mock}


def _setup_rum_commit_env(monkeypatch, *, run_id: int = 1101, disk_ok: tuple[bool, str] = (True, "")) -> dict:
    log_calls, ledger_calls, status_calls, sync_calls = [], [], [], []
    start_progress, _, end_progress, _ = _mock_progress(monkeypatch)
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
    return {
        "log_calls": log_calls,
        "ledger_calls": ledger_calls,
        "status_calls": status_calls,
        "sync_calls": sync_calls,
        "start_progress": start_progress,
        "end_progress": end_progress,
    }


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


# ── 1. Faro Bundle Integrity, Drift Check, Skip & Self-Heal ──────────────────


def test_steady_state_skips_download(config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge):
    download_calls, _ = mock_download
    config_store[SERVICE_ID] = _cfg(faro_content_hash="stored-hash-abc123", faro_last_upstream_check=NOW - 10)
    seen_methods: list[str] = []
    mock_fos(lambda req: seen_methods.append(req.method) or _head_response(200, etag="stored-hash-abc123"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert seen_methods == ["HEAD"]
    assert download_calls == []
    assert mock_purge == []


@pytest.mark.parametrize(
    "scenario",
    [
        "head_404",
        "etag_mismatch",
        "head_network_error",
        "missing_fos_credentials",
        "purge_failure_swallowed",
        "download_failure_swallowed",
    ],
)
def test_faro_integrity_restore_scenarios(
    scenario, config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge, monkeypatch
):
    download_calls, install_download = mock_download
    cfg = _cfg(faro_last_upstream_check=NOW - 10)

    if scenario == "missing_fos_credentials":
        install_download(raises=RuntimeError("missing FOS credentials"))
        del cfg["fos_access_key_id"]
        mock_fos(lambda req: (_ for _ in ()).throw(AssertionError("no FOS call without credentials")))
    elif scenario == "head_network_error":
        mock_fos(lambda req: (_ for _ in ()).throw(httpx.ConnectError("FOS unreachable", request=req)))
    elif scenario == "etag_mismatch":
        mock_fos(lambda req: _head_response(200, etag="some-other-etag"))
    else:
        mock_fos(lambda req: httpx.Response(404))

    if scenario == "purge_failure_swallowed":
        monkeypatch.setattr(
            "backend.core.fastly.client.fastly",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("Fastly API unreachable")),
        )
    elif scenario == "download_failure_swallowed":
        install_download(raises=RuntimeError("FOS upload failed: 500"))

    config_store[SERVICE_ID] = cfg
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert download_calls == [(SERVICE_ID, "2.9.0", "fake-fastly-token")]
    if scenario == "head_404":
        assert mock_purge == [("POST", f"/service/{SERVICE_ID}/purge/rum-faro-sdk", "fake-fastly-token")]


def test_progress_reported_when_run_id_set(
    config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge, monkeypatch
):
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)
    mock_fos(lambda request: httpx.Response(404))
    add_progress_calls: list[tuple] = []
    monkeypatch.setattr("backend.cron_progress.add_progress", lambda r, e: add_progress_calls.append((r, e)))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, 99)
    assert add_progress_calls and all(r == 99 for r, _ in add_progress_calls)
    assert any("restored" in e["message"] for _, e in add_progress_calls)


@pytest.mark.parametrize(
    ("elapsed_hours", "detect_result", "detect_raises", "expect_detect", "expect_download", "expect_ts"),
    [
        (1, False, None, False, False, NOW - 3600),
        (25, False, None, True, False, NOW),
        (25, True, None, True, True, NOW),
        (25, False, ValueError("unpkg unreachable"), True, False, NOW),
    ],
)
def test_upstream_drift_check_scenarios(
    elapsed_hours,
    detect_result,
    detect_raises,
    expect_detect,
    expect_download,
    expect_ts,
    config_store,
    frozen_now,
    mock_fos,
    mock_download,
    mock_detect,
    mock_purge,
):
    download_calls, _ = mock_download
    detect_calls, install_detect = mock_detect
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - elapsed_hours * 3600)
    if expect_detect:
        install_detect(result=detect_result, raises=detect_raises)
    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)

    assert detect_calls == ([(SERVICE_ID, "2.9.0")] if expect_detect else [])
    assert download_calls == ([(SERVICE_ID, "2.9.0", "fake-fastly-token")] if expect_download else [])
    assert config_store[SERVICE_ID]["rum"]["faro_last_upstream_check"] == expect_ts
    assert config_store[SERVICE_ID]["rum"]["faro_version"] == "2.9.0"
    if expect_download:
        assert len(mock_purge) == 1


@pytest.mark.parametrize("skip_mode", ["rum_disabled", "no_rum_block", "no_config", "top_level_rum_enabled"])
def test_faro_reconcile_skip_and_enable_conditions(
    skip_mode, config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge
):
    download_calls, _ = mock_download
    seen_methods: list[str] = []
    if skip_mode == "top_level_rum_enabled":
        cfg = _cfg()
        del cfg["rum"]["enabled"]
        cfg["rum_enabled"] = True
        config_store[SERVICE_ID] = cfg
        mock_fos(lambda req: seen_methods.append(req.method) or _head_response(200, etag="stored-hash-abc123"))
        rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
        assert seen_methods == ["HEAD"]
    else:
        if skip_mode == "rum_disabled":
            config_store[SERVICE_ID] = _cfg(enabled=False)
        elif skip_mode == "no_rum_block":
            cfg = copy.deepcopy(FAKE_CFG)
            del cfg["rum"]
            config_store[SERVICE_ID] = cfg
        mock_fos(lambda req: (_ for _ in ()).throw(AssertionError("FOS must not be contacted")))
        rum_sync_mod._reconcile_faro_bundle("no-such-service" if skip_mode == "no_config" else SERVICE_ID, None)
    assert download_calls == []


def test_self_heals_unpinned_version_vcl_sha_and_no_thrash(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, mock_reconcile, monkeypatch
):
    _, install_detect = mock_detect
    install_detect(result=False)
    calls = _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos)

    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert calls == [(SERVICE_ID, DEFAULT_FARO_VERSION, "fake-fastly-token")]
    final_cfg = config_store[SERVICE_ID]
    assert final_cfg["rum"]["faro_version"] == DEFAULT_FARO_VERSION
    assert final_cfg.get("rum_vcl_sha") == rum_vcl_fingerprint(SERVICE_ID, final_cfg)
    assert len(mock_purge) == 1
    assert mock_reconcile == [(SERVICE_ID, "fake-fastly-token")]

    # Second tick must not thrash
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert len(calls) == 1 and len(mock_reconcile) == 1


@pytest.mark.parametrize("fail_stage", ["vcl_reconcile", "adoption_download"])
def test_self_heal_failures_do_not_raise(
    fail_stage, config_store, frozen_now, mock_fos, mock_download, mock_detect, mock_purge, monkeypatch
):
    if fail_stage == "vcl_reconcile":
        _seed_unpinned_self_heal(config_store, monkeypatch, mock_fos)
        monkeypatch.setattr(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("Fastly API unreachable")),
        )
        rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
        assert config_store[SERVICE_ID]["rum"]["faro_version"] == DEFAULT_FARO_VERSION
        assert len(mock_purge) == 1
    else:
        download_calls, install_download = mock_download
        install_download(raises=RuntimeError("unpkg unreachable"))
        cfg = _cfg()
        del cfg["rum"]["faro_version"]
        config_store[SERVICE_ID] = cfg
        mock_fos(lambda req: (_ for _ in ()).throw(AssertionError("no FOS HEAD when adoption fails")))
        rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
        assert download_calls == [(SERVICE_ID, DEFAULT_FARO_VERSION, "fake-fastly-token")]
        assert "faro_version" not in config_store[SERVICE_ID]["rum"]


def test_restore_and_resync_pass_expected_current_version_for_compare_and_set(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, monkeypatch
):
    seen_kwargs: list[dict] = []

    async def fake_download(service_id, version, token, *, status_cb=None, **kwargs):
        seen_kwargs.append(kwargs)
        return {"version": version}

    monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake_download)

    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)
    mock_fos(lambda request: httpx.Response(404))
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert seen_kwargs == [{"expected_current_version": "2.9.0"}]

    seen_kwargs.clear()
    _, install_detect = mock_detect
    install_detect(result=True)
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 25 * 3600)
    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert seen_kwargs == [{"expected_current_version": "2.9.0"}]


# ── 2. RUM Sync Execution & Failure Isolation ────────────────────────────────


def test_beacon_ingest_still_proceeds_when_faro_reconcile_raises(config_store, frozen_now, monkeypatch, rum_lease):
    monkeypatch.setattr(
        rum_sync_mod, "_reconcile_faro_bundle", lambda sid, rid: (_ for _ in ()).throw(RuntimeError("simulated"))
    )
    monkeypatch.setattr(
        rum_sync_mod,
        "ingest_rum_logs",
        lambda sid, *a, **kw: iter([("started", 42), ("file_done", "beacon-1.log.gz", 10), ("done", 10)]),
    )
    _, add_progress, _, cleanup = _mock_progress(monkeypatch)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)
    cleanup.assert_called_once()
    assert any(
        "RUM sync failed" in c.args[1]["message"] for c in add_progress.call_args_list if c.args[1]["type"] == "error"
    )


@pytest.mark.parametrize(
    ("run_id", "ingest_fails", "expected_status"),
    [
        (42, False, "warning"),
        (43, True, "error"),
    ],
)
def test_standard_rum_status_on_faro_reconcile_failure(run_id, ingest_fails, expected_status, monkeypatch, rum_lease):
    rum_lease.start.return_value = run_id
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE cron_runs (id INTEGER, service_id TEXT, status TEXT)")
    con.execute("INSERT INTO cron_runs VALUES (?, ?, 'success')", (run_id, SERVICE_ID))
    monkeypatch.setattr("backend.core.metadata.get_con", lambda sid: con)
    monkeypatch.setattr(rum_sync_mod, "_reconcile_faro_bundle", lambda sid, rid: False)

    def fake_ingest(sid, *args, **kwargs):
        yield ("started", run_id)
        if ingest_fails:
            con.execute("UPDATE cron_runs SET status = 'error' WHERE id = ?", (run_id,))
            con.commit()
            yield ("error", "rum/beacon.log.gz", "corrupt gzip")
        yield ("done", 0)

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", fake_ingest)
    _mock_progress(monkeypatch)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (run_id,)).fetchone() == (expected_status,)
    con.close()


# ── 3. Scheduler Mode Routing & Cadence ──────────────────────────────────────


@pytest.mark.parametrize(
    ("high_scale", "rum_enabled", "expected_present", "expected_absent"),
    [
        (
            False,
            True,
            [f"rum_sync_{_RUM_DISC_SERVICE_ID}", f"rum_commit_{_RUM_DISC_SERVICE_ID}"],
            [f"rum_discovery_{_RUM_DISC_SERVICE_ID}", f"ledger_rum_sweep_{_RUM_DISC_SERVICE_ID}"],
        ),
        (
            True,
            True,
            [f"rum_discovery_{_RUM_DISC_SERVICE_ID}", f"ledger_rum_sweep_{_RUM_DISC_SERVICE_ID}"],
            [f"rum_sync_{_RUM_DISC_SERVICE_ID}", f"rum_commit_{_RUM_DISC_SERVICE_ID}"],
        ),
        (
            False,
            False,
            [],
            [
                f"rum_sync_{_RUM_DISC_SERVICE_ID}",
                f"rum_commit_{_RUM_DISC_SERVICE_ID}",
                f"rum_discovery_{_RUM_DISC_SERVICE_ID}",
                f"ledger_rum_sweep_{_RUM_DISC_SERVICE_ID}",
            ],
        ),
    ],
)
def test_mode_routing_and_disabled_registration(
    high_scale, rum_enabled, expected_present, expected_absent, monkeypatch
):
    base_cfg = FAKE_CFG_HIGH_SCALE if high_scale else FAKE_CFG_STANDARD
    base_src = FAKE_SRC_HIGH_SCALE if high_scale else FAKE_SRC_STANDARD
    cfg = copy.deepcopy(base_cfg)
    cfg["rum"]["enabled"] = rum_enabled
    sched, _ = _sync_scheduler_for_cfg(monkeypatch, cfg, base_src, high_scale=high_scale)
    added_ids = list(sched._job_ids.keys())
    for jid in expected_present:
        assert jid in added_ids
    for jid in expected_absent:
        assert jid not in added_ids


@pytest.mark.parametrize(
    ("rum_cfg", "svc_cfg", "default_req", "expected"),
    [
        ({"sync_interval_seconds": 12}, {}, 60, 12),
        ({"sync_interval_seconds": 2}, {}, 60, 5),
        ({}, {}, 45, 45),
        ({"log_period": 30}, {}, 60, 30),
        ({}, {"rum_log_period": 90}, 60, 45),
    ],
)
def test_resolve_rum_sync_interval(rum_cfg, svc_cfg, default_req, expected):
    assert _resolve_rum_sync_interval(rum_cfg, svc_cfg, default_request_interval=default_req) == expected


@pytest.mark.parametrize(
    ("high_scale", "job_prefix", "new_seconds"),
    [
        (False, "rum_sync", 45),
        (True, "rum_discovery", 25),
    ],
)
def test_cadence_dynamic_rescheduling(high_scale, job_prefix, new_seconds, monkeypatch):
    cfg = copy.deepcopy(FAKE_CFG_HIGH_SCALE if high_scale else FAKE_CFG_STANDARD)
    src = FAKE_SRC_HIGH_SCALE if high_scale else FAKE_SRC_STANDARD
    cfg["rum"]["sync_interval_seconds"] = new_seconds
    job_id = f"{job_prefix}_{_RUM_DISC_SERVICE_ID}"
    sched, mock_job = _sync_scheduler_for_cfg(
        monkeypatch, cfg, src, high_scale=high_scale, existing_job_id=job_id, patch_add_job=False
    )
    sched._sched.get_job.assert_called_with(job_id)
    mock_job.reschedule.assert_called_once_with("interval", seconds=new_seconds)


# ── 4. High-Scale Discovery, Politeness Gate, Chunk Budget & Commit Trigger ──


def test_high_scale_discovery_normal_slices_and_faro_warning(monkeypatch):
    discovered_slices: list[str] = []
    state = _setup_high_scale_discovery(
        monkeypatch,
        run_id=701,
        discover=lambda sid, prefix_subpath=None, run_id=None: discovered_slices.append(prefix_subpath) or 1,
    )
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert len(state["log_calls"]) == 1 and state["log_calls"][0][0][3] == "success"
    assert len(discovered_slices) == 5 and all(s.startswith("raw/rum/") for s in discovered_slices)

    # Faro reconcile failure degrades to warning while still discovering all 5 slices
    state_warn = _setup_high_scale_discovery(
        monkeypatch,
        run_id=702,
        reconcile=MagicMock(side_effect=RuntimeError("Transient unpkg 503")),
        discover=lambda sid, prefix_subpath=None, run_id=None: 2,
    )
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    args, kwargs = state_warn["log_calls"][0]
    assert args[3] == "warning" and kwargs.get("files_downloaded") == 10
    assert "Faro bundle reconcile issue" in kwargs.get("summary", "")
    assert "Transient unpkg 503" in kwargs.get("error_message", "")


@pytest.mark.parametrize(
    ("broker_url", "discover_err", "expected_summary_substr", "expected_err_substr"),
    [
        ("", None, "no broker URL", "CELERY_BROKER_URL"),
        (
            "redis://localhost:6379/0",
            ConnectionError("Valkey broker connection refused"),
            "RUM discovery failed",
            "Valkey broker connection refused",
        ),
    ],
)
def test_rum_discovery_broker_errors(
    broker_url, discover_err, expected_summary_substr, expected_err_substr, monkeypatch
):
    discover_calls = []

    def _discover(*a, **kw):
        if discover_err:
            raise discover_err
        discover_calls.append(a)
        return 1

    state = _setup_high_scale_discovery(monkeypatch, run_id=99, broker_url=broker_url, discover=_discover)
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    if not broker_url:
        assert discover_calls == []
    args, kw = state["log_calls"][0]
    assert args[1] == "rum_discovery" and args[3] == "error"
    assert expected_summary_substr in (kw.get("summary") or "")
    assert expected_err_substr in (kw.get("error_message") or "")


def test_politeness_gate_not_deferred_for_rum_sync_or_commit(monkeypatch, rum_sync_lease, mock_orchestration_env):
    ingest_mock = MagicMock(return_value=iter([("started", 1), ("done", 0)]))
    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", ingest_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: True)
    _mock_progress(monkeypatch)

    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert ingest_mock.called

    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_COMMIT_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_COMMIT_FAKE_SRC)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task) or 1105)
    with (
        patch("backend.cron.scheduler._check_disk_space", return_value=(False, "disk check")),
        patch("backend.core.duckdb.log_cron_run"),
    ):
        rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)
    assert start_calls == ["rum_commit"]


def _setup_rum_ingest_logs_env(monkeypatch, *, run_id: int, new_files: list[str], file_size: int = 100):
    log_calls, download_chunks = [], []
    monkeypatch.setattr("backend.core.rum_ingest.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: MagicMock())
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda sid, task: run_id)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", MagicMock())
    monkeypatch.setattr("backend.core.ingest._recover_in_flight", MagicMock())
    monkeypatch.setattr("backend.core.metadata.get_ingested_filenames", lambda *a, **k: set())

    def fake_list_gen(**kw):
        yield {"type": "status", "message": "found files"}
        return {"new_files": new_files, "file_sizes": {f: file_size for f in new_files}, "stranded_already": []}

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


def test_trap_41_chunk_zero_guaranteed_and_outcome_counters(monkeypatch):
    fake_files = [f"s3://test-rum-bucket/raw/rum/beacon_{i:03d}.gz" for i in range(100)]
    log_calls, download_chunks = _setup_rum_ingest_logs_env(
        monkeypatch, run_id=705, new_files=fake_files, file_size=100
    )

    times = [1000.0, 1005.0, 1200.0, 1205.0]
    monkeypatch.setattr(rum_ingest_mod.time, "time", lambda: times.pop(0) if times else 2000.0)
    events = list(rum_ingest_mod.ingest_rum_logs(_RUM_DISC_SERVICE_ID, max_seconds=10))

    assert len(download_chunks) == 1 and len(download_chunks[0]) == 50
    assert any(evt[0] == "status" and evt[1] == "budget" for evt in events)
    counters = log_calls[0][1].get("outcome_counters")
    assert (
        counters["objects_processed"] == 50 and counters["objects_successful"] == 50 and counters["objects_failed"] == 0
    )


@pytest.mark.parametrize(("total", "expect_commit"), [(42, True), (0, False)])
def test_post_sync_immediate_commit_trigger(total, expect_commit, monkeypatch, rum_sync_lease):
    commit_mock = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.rum_commit._run_rum_commit", commit_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync.ingest_rum_logs", lambda sid, *a, **kw: iter([("started", 706), ("done", total)])
    )
    _mock_progress(monkeypatch)
    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)
    assert commit_mock.call_count == (1 if expect_commit else 0)


def test_guaranteed_cleanup_on_discovery_and_sync_errors(monkeypatch, rum_sync_lease):
    state = _setup_high_scale_discovery(
        monkeypatch, run_id=709, discover=MagicMock(side_effect=RuntimeError("FOS connection aborted"))
    )
    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(_RUM_DISC_SERVICE_ID)
    state["end_progress"].assert_called_once_with(709)
    state["finalize"].assert_called_once_with(FAKE_SRC_HIGH_SCALE, "rum_discovery", 709)

    def failing_ingest(sid, **kw):
        yield ("started", 710)
        raise RuntimeError("Disk full")

    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", failing_ingest)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    _, _, end_progress_mock, reap_mock = _mock_progress(monkeypatch)
    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID)
    end_progress_mock.assert_called_once_with(709)
    reap_mock.assert_called_once()


# ── 5. Skip Conditions, Role Gates & Lease Conflicts Across RUM Crons ────────


@pytest.mark.parametrize(
    ("cron_fn", "sid", "cfg", "src", "high_scale", "env_no_crons"),
    [
        (
            rum_ledger_mod._run_rum_discovery_cron.__wrapped__,
            _RUM_DISC_SERVICE_ID,
            FAKE_CFG_STANDARD,
            FAKE_SRC_STANDARD,
            False,
            False,
        ),
        (
            rum_ledger_mod._run_rum_discovery_cron.__wrapped__,
            _RUM_DISC_SERVICE_ID,
            FAKE_CFG_HIGH_SCALE,
            {**FAKE_SRC_HIGH_SCALE, "access_level": "read_only"},
            True,
            False,
        ),
        (
            rum_ledger_mod._run_rum_ledger_sweep.__wrapped__,
            _RUM_SWEEP_SERVICE_ID,
            _RUM_SWEEP_FAKE_CFG,
            _RUM_SWEEP_FAKE_SRC,
            False,
            False,
        ),
        (
            rum_ledger_mod._run_rum_ledger_sweep.__wrapped__,
            _RUM_SWEEP_SERVICE_ID,
            _RUM_SWEEP_FAKE_CFG,
            {**_RUM_SWEEP_FAKE_SRC, "access_level": "read_only"},
            True,
            False,
        ),
        (
            rum_ledger_mod._run_rum_ledger_sweep.__wrapped__,
            _RUM_SWEEP_SERVICE_ID,
            {**_RUM_SWEEP_FAKE_CFG, "rum": {"enabled": False}},
            _RUM_SWEEP_FAKE_SRC,
            True,
            False,
        ),
        (
            rum_ledger_mod._run_rum_ledger_sweep.__wrapped__,
            _RUM_SWEEP_SERVICE_ID,
            _RUM_SWEEP_FAKE_CFG,
            _RUM_SWEEP_FAKE_SRC,
            True,
            True,
        ),
        (
            rum_ledger_mod._run_rum_ledger_sweep.__wrapped__,
            _RUM_SWEEP_SERVICE_ID,
            None,
            _RUM_SWEEP_FAKE_SRC,
            True,
            False,
        ),
        (rum_sync_mod._run_rum_sync.__wrapped__, _RUM_DISC_SERVICE_ID, FAKE_CFG_STANDARD, None, False, False),
    ],
)
def test_cron_early_exit_guards(cron_fn, sid, cfg, src, high_scale, env_no_crons, monkeypatch):
    if env_no_crons:
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda s: cfg)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: src)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda s: high_scale)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda s, task: start_calls.append(task))
    cron_fn(sid)
    assert start_calls == []


@pytest.mark.parametrize(
    ("cron_fn", "sid", "cfg", "src", "high_scale"),
    [
        (
            rum_ledger_mod._run_rum_discovery_cron.__wrapped__,
            _RUM_DISC_SERVICE_ID,
            FAKE_CFG_HIGH_SCALE,
            FAKE_SRC_HIGH_SCALE,
            True,
        ),
        (rum_sync_mod._run_rum_sync.__wrapped__, _RUM_DISC_SERVICE_ID, FAKE_CFG_STANDARD, FAKE_SRC_STANDARD, False),
        (
            rum_commit_mod._run_rum_commit.__wrapped__,
            _RUM_COMMIT_SERVICE_ID,
            _RUM_COMMIT_FAKE_CFG,
            _RUM_COMMIT_FAKE_SRC,
            False,
        ),
    ],
)
def test_lease_held_is_clean_skip(cron_fn, sid, cfg, src, high_scale, monkeypatch):
    log_mock, end_progress_mock = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.config.load_config", lambda s: cfg)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda s: high_scale)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://x")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("held")))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_mock)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)

    cron_fn(sid)
    log_mock.assert_not_called()
    end_progress_mock.assert_not_called()


def test_rum_sync_and_sweep_reuse_externally_owned_run_id(monkeypatch, rum_sync_lease):
    start_mock = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    _mock_progress(monkeypatch)
    seen: dict = {}

    def fake_ingest(service_id, *args, **kwargs):
        seen.update(kwargs)
        yield ("done", 0)

    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", fake_ingest)
    rum_sync_mod._run_rum_sync.__wrapped__(_RUM_DISC_SERVICE_ID, run_id=99)
    start_mock.assert_not_called()
    assert seen["run_id"] == 99 and seen["max_seconds"] == 240

    # Sweep run_id reuse
    log_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: log_calls.append((a, k)))
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
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
    start_mock.assert_not_called()
    assert log_calls[0][1].get("run_id") == 999


# ── 6. Manual Trigger Endpoints & Quarantine Endpoints ───────────────────────


@pytest.mark.parametrize(
    ("endpoint", "sid", "src", "cfg", "job_patch_target", "task_name", "run_id"),
    [
        (
            f"/api/admin/rum/sync/{_RUM_DISC_SERVICE_ID}",
            _RUM_DISC_SERVICE_ID,
            FAKE_SRC_STANDARD,
            FAKE_CFG_STANDARD,
            "backend.cron.jobs.rum_sync._run_rum_sync",
            "rum_sync",
            801,
        ),
        (
            f"/api/admin/rum/discovery/{_RUM_DISC_SERVICE_ID}",
            _RUM_DISC_SERVICE_ID,
            FAKE_SRC_HIGH_SCALE,
            FAKE_CFG_HIGH_SCALE,
            "backend.cron.jobs.rum_ledger._run_rum_discovery_cron",
            "rum_discovery",
            803,
        ),
        (
            f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}",
            _RUM_SWEEP_SERVICE_ID,
            _RUM_SWEEP_FAKE_SRC,
            _RUM_SWEEP_FAKE_CFG,
            "backend.cron.jobs.rum_ledger._run_rum_ledger_sweep",
            "ledger_rum_sweep",
            "run-rum-sweep-101",
        ),
    ],
)
def test_manual_trigger_endpoints_lifecycle(endpoint, sid, src, cfg, job_patch_target, task_name, run_id):
    # 1. Success (200)
    with (
        _override_service_deps(src, sid) as client,
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.start_cron_run", return_value=run_id),
        patch("backend.cron_progress.start_progress"),
        patch(job_patch_target),
        patch("backend.repositories.dashboard.invalidate_service"),
    ):
        resp = client.post(endpoint, headers={"x-fastly-service-id": sid})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True and body["run_id"] == run_id and "started" in body["message"].lower()

    # 2. Reentrancy (200 already running) for sync & sweep
    if task_name in ("rum_sync", "ledger_rum_sweep"):
        with (
            _override_service_deps(src, sid) as client,
            patch("backend.config.load_config", return_value=cfg),
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
            patch(
                "backend.cron_progress.list_active_runs",
                return_value=[{"service_id": sid, "task": task_name, "run_id": run_id}],
            ),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(endpoint, headers={"x-fastly-service-id": sid})
            assert resp.status_code == 200
            assert resp.json()["run_id"] == run_id and "already running" in resp.json()["message"].lower()

    # 3. Busy (503 cron_busy) for sync & discovery
    if task_name in ("rum_sync", "rum_discovery"):
        with (
            _override_service_deps(src, sid) as client,
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Lease lock busy")),
            patch("backend.cron_progress.list_active_runs", return_value=[]),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(endpoint, headers={"x-fastly-service-id": sid})
            assert resp.status_code == 503 and resp.json()["detail"]["error"] == "cron_busy"

    # 4. Read-only & Non-admin 403 rejections
    assert_manual_trigger_read_only_rejected(endpoint, sid, src)
    if task_name in ("rum_sync", "ledger_rum_sweep"):
        assert_manual_trigger_non_admin_rejected(endpoint, sid, src)

    # 5. Standard mode 400 rejection for high-scale-only endpoints
    if task_name in ("rum_discovery", "ledger_rum_sweep"):
        resp = assert_manual_trigger_standard_mode_rejected(endpoint, sid, src)
        detail = resp.json()["detail"]
        assert (
            (detail.get("error") == "high_scale_required") if isinstance(detail, dict) else ("high" in detail.lower())
        )


def test_manual_trigger_rum_sweep_rum_disabled_rejected():
    no_rum_cfg = {**_RUM_SWEEP_FAKE_CFG, "rum": {"enabled": False}}
    with (
        _override_service_deps(_RUM_SWEEP_FAKE_SRC, _RUM_SWEEP_SERVICE_ID) as client,
        patch("backend.config.load_config", return_value=no_rum_cfg),
    ):
        resp = client.post(
            f"/api/admin/rum/ledger/sweep/{_RUM_SWEEP_SERVICE_ID}",
            headers={"x-fastly-service-id": _RUM_SWEEP_SERVICE_ID},
        )
    assert resp.status_code == 400 and resp.json()["detail"]["error"] == "rum_not_enabled"


def test_quarantine_endpoints_with_source_type_rum_and_rbac(monkeypatch):
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
    list_calls: list[dict] = []

    def mock_list(sid, limit=100, offset=0, error_category=None, source_type=None):
        list_calls.append({"sid": sid, "error_category": error_category, "source_type": source_type})
        return mock_items

    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", side_effect=mock_list),
        patch("backend.routers.admin.quarantine.count_quarantine_evidence", return_value=1),
    ):
        client = TestClient(app)
        resp = client.get(f"/api/admin/ledger/quarantine?service_id={_RUM_SWEEP_SERVICE_ID}&source_type=rum")
        assert resp.status_code == 200 and resp.json()["items"][0]["source_type"] == "rum"
        assert list_calls[0]["source_type"] == "rum"

        with _override_service_deps(_RUM_SWEEP_FAKE_SRC, _RUM_SWEEP_SERVICE_ID) as c2:
            resp2 = c2.get(
                "/api/admin/quarantine?source_type=rum", headers={"x-fastly-service-id": _RUM_SWEEP_SERVICE_ID}
            )
            assert resp2.status_code == 200 and resp2.json()["items"][0]["source_type"] == "rum"

    # 404 unknown service
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    assert TestClient(app).get("/api/admin/ledger/quarantine?service_id=nonexistent-svc").status_code == 404

    # 403 read-only service
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service", lambda sid: {**_RUM_SWEEP_FAKE_SRC, "access_level": "read_only"}
    )
    assert TestClient(app).get(f"/api/admin/ledger/quarantine?service_id={_RUM_SWEEP_SERVICE_ID}").status_code == 403

    # 403 non-admin
    app.dependency_overrides[require_admin] = lambda: (_ for _ in ()).throw(
        HTTPException(status_code=403, detail={"error": "admin_only"})
    )
    try:
        assert (
            TestClient(app).get(f"/api/admin/ledger/quarantine?service_id={_RUM_SWEEP_SERVICE_ID}").status_code == 403
        )
    finally:
        app.dependency_overrides.pop(require_admin, None)


# ── 7. RUM Commit Contract & DuckLake Natural Key Deduplication ──────────────


@pytest.mark.parametrize("scenario", ["full_success", "partial_failure", "both_fail", "low_disk"])
def test_rum_commit_lifecycle_contracts(scenario, monkeypatch, mock_orchestration_env):
    disk_ok = (False, "Free disk space is 250MB (< 500MB minimum)") if scenario == "low_disk" else (True, "")
    state = _setup_rum_commit_env(monkeypatch, run_id=1101, disk_ok=disk_ok)
    sse_published = []

    def mock_commit(src, table_name):
        if scenario == "both_fail":
            raise RuntimeError(f"{table_name} network timeout")
        if table_name == "client_vitals":
            return {"files_committed": 3, "rows_committed": 45}
        if scenario == "partial_failure":
            raise OSError("Transient FOS S3 error on errors commit")
        return {"files_committed": 2, "rows_committed": 15}

    monkeypatch.setattr("backend.core.iceberg.commit_buffer", mock_commit)
    latest = datetime(2026, 10, 11, 14, 30, tzinfo=UTC)
    monkeypatch.setattr("backend.core.rollups.rum.rum_count_and_latest", lambda con: (60, latest))
    mock_pub = MagicMock()
    mock_pub.publish.side_effect = lambda sid, snap: sse_published.append((sid, snap))
    monkeypatch.setattr("backend.sync_status_publisher.publisher", mock_pub)
    monkeypatch.setattr(
        "backend.sync_status_snapshot.compute_sync_status_cached", lambda sid: {"service_id": sid, "sync_stage": "idle"}
    )

    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)
    args, kwargs = state["log_calls"][0]

    if scenario == "full_success":
        assert args[3] == "success" and kwargs.get("rows_ingested") == 60
        assert state["ledger_calls"] == [(_RUM_COMMIT_SERVICE_ID, True)]
        assert len(sse_published) == 1
        rum_updates = [s for _, s in state["status_calls"] if "rum" in s]
        assert rum_updates[-1]["rum"]["total_rows"] == 60
    elif scenario == "partial_failure":
        assert args[3] == "warning" and kwargs.get("rows_ingested") == 45
        assert "client_vitals" in state["sync_calls"] and state["ledger_calls"] == []
        assert "Partial RUM commit" in kwargs.get("summary", "")
    elif scenario == "both_fail":
        assert args[3] == "error" and state["ledger_calls"] == []
        assert "RUM commit failed for both tables" in kwargs.get("summary", "")
    elif scenario == "low_disk":
        assert args[3] == "error" and "RUM commit aborted" in kwargs.get("summary", "")


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


def test_rum_buffer_and_local_compaction_natural_key_deduplication(tmp_path):
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
    beacon_b = _make_vital_row(ts2, metric_name="CLS", metric_value=0.05, pathname="/about", cid="cid_2", req_id="r2")
    beacon_c = _make_vital_row(ts2, metric_name="FID", metric_value=15.0, pathname="/contact", cid="cid_3", req_id="r3")

    pq.write_table(
        pa.Table.from_pylist([beacon_a, beacon_b], schema=CLIENT_VITALS_ARROW_SCHEMA), str(buf_dir / "b1.parquet")
    )
    assert _commit_buffer_impl(src, table_name="client_vitals")["rows_committed"] == 2

    pq.write_table(
        pa.Table.from_pylist([beacon_a, beacon_c], schema=CLIENT_VITALS_ARROW_SCHEMA), str(buf_dir / "b2.parquet")
    )
    assert _commit_buffer_impl(src, table_name="client_vitals")["files_committed"] == 1

    con = duckdb.connect()
    try:
        _ducklake_attach(con, src, read_only=True)
        tbl_name = ducklake_table_name(src, table_name="client_vitals")
        assert con.execute(f'SELECT count(*) FROM lake."{tbl_name}"').fetchone()[0] == 3
    finally:
        con.close()

    # Local partition compaction deduplication
    now = datetime.now(UTC)
    part_dir = cache_dir / "data_client_vitals" / now.strftime("timestamp_hour=%Y-%m-%d-%H")
    part_dir.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([beacon_a], schema=CLIENT_VITALS_ARROW_SCHEMA), str(part_dir / "p1.parquet"))
    pq.write_table(
        pa.Table.from_pylist([beacon_a, beacon_b], schema=CLIENT_VITALS_ARROW_SCHEMA), str(part_dir / "p2.parquet")
    )
    res = compact_local_partitions(src, min_files_per_partition=1, table_name="client_vitals", compact_active_hour=True)
    assert res["partitions_compacted"] == 1 and res["files_merged"] == 2
    remaining = [f for f in os.listdir(part_dir) if f.endswith(".parquet")]
    assert len(remaining) == 1
    with duckdb.connect() as c2:
        assert len(c2.execute(f"SELECT * FROM read_parquet('{part_dir / remaining[0]}')").fetchall()) == 2


def test_rum_commit_aggregates_self_heal_on_stale_lake(monkeypatch):
    recompute_calls = {"n": 0}

    def flaky_recompute(con, service_id, hours=None):
        recompute_calls["n"] += 1
        if recompute_calls["n"] == 1:
            raise duckdb.CatalogException('Catalog Error: schema "lake" does not exist.')

    _setup_rum_commit_env(monkeypatch, run_id=1004)
    monkeypatch.setattr(
        "backend.core.iceberg.commit_buffer",
        lambda src, table_name="logs", **kw: {"files_committed": 1, "rows_committed": 5},
    )
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", flaky_recompute)
    monkeypatch.setattr(
        "backend.core.iceberg.execute_with_stale_view_retry",
        lambda con, source, fn, *a, **kw: _retry_once(con, fn),
    )
    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)
    assert recompute_calls["n"] == 2


def _retry_once(con, fn):
    try:
        return fn(con)
    except Exception:
        return fn(con)


# ── 8. RUM Ledger Sweep Lifecycle, Scheduler & Sweeper Engine ────────────────


@pytest.mark.parametrize("outcome", ["success", "warning", "error"])
def test_run_rum_ledger_sweep_outcomes(outcome, monkeypatch):
    log_calls, finalize_calls = [], []
    monkeypatch.setattr("backend.config.load_config", lambda sid: _RUM_SWEEP_FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _RUM_SWEEP_FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 888)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr(
        "backend.cron.jobs._common.finalize_cron_duration", lambda *a, **kw: finalize_calls.append((a, kw))
    )
    start_progress, _, end_progress, _ = _mock_progress(monkeypatch)
    monkeypatch.setattr("backend.cron.scheduler._log_and_add_progress", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())

    if outcome == "error":
        monkeypatch.setattr(
            "backend.core.ingest.sweep_rum_ledger_once",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("Postgres connection dropped")),
        )
    elif outcome == "warning":
        monkeypatch.setattr(
            "backend.core.ingest.sweep_rum_ledger_once",
            lambda sid, run_id=None: {
                "reclaimed": 1,
                "redispatched": 0,
                "discovered": 0,
                "broker_ok": False,
                "dead_letter": 2,
            },
        )
    else:
        monkeypatch.setattr(
            "backend.core.ingest.sweep_rum_ledger_once",
            lambda sid, run_id=None: {
                "reclaimed": 3,
                "redispatched": 3,
                "discovered": 7,
                "broker_ok": True,
                "dead_letter": 0,
            },
        )

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    start_progress.assert_called_once_with(888, service_id=_RUM_SWEEP_SERVICE_ID, task="ledger_rum_sweep")
    end_progress.assert_called_once_with(888)
    assert len(finalize_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == outcome


def test_scheduler_registers_reschedules_and_skips_rum_ledger_sweep():
    sid = "svc-scheduler-rum-sweep"

    def _make_cfg(rum_en=True, sweep_en=True, mins=15):
        return {
            "service_id": sid,
            "name": sid,
            "log_period": 60,
            "deployment_mode": "high_scale",
            "access_level": "read_write",
            "rum": {"enabled": rum_en},
            "provisioning": {
                "access_level": "read_write",
                "cron_sync": {"enabled": True},
                "cron_ledger_rum_sweep": {"enabled": sweep_en, "interval_minutes": mins},
            },
        }, {
            "name": sid,
            "service_id": sid,
            "bucket": "test-b",
            "access_level": "read_write",
            "deployment_mode": "high_scale",
        }

    def _sync(s, c, sr):
        with (
            patch("backend.config.list_configs", return_value=[c]),
            patch("backend.core.duckdb.get_source_for_service", return_value=sr),
            patch("backend.core.duckdb.is_configured", return_value=True),
            patch("backend.config.is_high_scale_mode", return_value=True),
            patch("backend.config.get_ngwaf_workspace_id", return_value=None),
            patch("backend.core.metadata.count_alerts", return_value=0),
        ):
            s._sync_jobs()

    cfg, src = _make_cfg(mins=15)
    s = Scheduler()
    s._sched = MagicMock()
    _sync(s, cfg, src)
    job_id = f"ledger_rum_sweep_{sid}"
    assert job_id in s._job_ids

    cfg["provisioning"]["cron_ledger_rum_sweep"]["interval_minutes"] = 20
    mock_job = MagicMock()
    s._sched.get_job = MagicMock(side_effect=lambda jid: mock_job if jid == job_id else None)
    _sync(s, cfg, src)
    mock_job.reschedule.assert_called_once_with("interval", minutes=20)

    for rum_en, sweep_en in [(True, False), (False, True)]:
        c2, sr2 = _make_cfg(rum_en=rum_en, sweep_en=sweep_en)
        s2 = Scheduler()
        s2._sched = MagicMock()
        _sync(s2, c2, sr2)
        assert job_id not in s2._job_ids


@pytest.mark.parametrize(("queue_depth", "expected_redispatched"), [(0, 1), (100, 0)])
def test_sweep_rum_ledger_reclaim_and_queue_depth_guard(queue_depth, expected_redispatched, monkeypatch):
    sid = "svc-test-rum-reclaim"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"
    executed_statements: list[tuple] = []

    class FakeCursor:
        def execute(self, query, params=None):
            executed_statements.append((query, params))

        def fetchall(self):
            return (
                [(rum_key,)]
                if any("UPDATE ingest_ledger SET status='discovered'" in q for q, _ in executed_statements)
                else []
            )

    class FakeCon:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def execute(self, query, params=None):
            executed_statements.append((query, params))
            res = MagicMock()
            res.fetchone.return_value = (0,)
            res.fetchall.return_value = []
            return res

    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda s: FakeCon())
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: {"prefix": "", "bucket": "b"})
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda *a, **k: 0)
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": queue_depth}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_rum_files.delay", mock_convert)

    summary = sweep_rum_ledger_once(sid)
    assert summary["reclaimed"] == 1 and summary["redispatched"] == expected_redispatched
    assert mock_convert.call_count == expected_redispatched
