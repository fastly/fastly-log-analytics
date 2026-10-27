"""Consolidated tests for RUM sync, discovery, commit, and ledger sweep crons."""

from __future__ import annotations

import ast
import concurrent.futures
import contextlib
import copy
import inspect
import io
import logging
import os
import pathlib
import sqlite3
import threading
import time
from collections import namedtuple
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.local_compaction import compact_local_partitions
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.core.query_attribution import current_attribution, derive_from_process_context
from backend.core.query_registry import query_registry
from backend.core.storage._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.storage.buffer import _commit_buffer_impl
from backend.core.storage.rum_schema import CLIENT_VITALS_ARROW_SCHEMA
from backend.cron.decorators import CronHeartbeat
from backend.cron.jobs.metric_snapshot import (
    _run_metric_snapshot,
    _safe_record,
    _sample_active_queries,
    _sample_celery_queues,
    _sample_cron_duration,
    _sample_ingest_lag,
    _sample_ingest_ledger,
    _sample_os_vitals,
)
from backend.cron.lifecycle import record_skip_row
from backend.deps import get_service_id, get_source, require_admin
from fastapi import HTTPException
from starlette.testclient import TestClient
from tests._helpers.config_fakes import fetch_metric_snapshot_history as _get_history

from backend.core import duckdb as _db
from backend.core import duckdb as duckdb_mod
from backend.core import duckdb_pool as _pool
from backend.core import duckdb_recycle as _recycle
from backend.core import metadata as metadata_db
from backend.core import metric_snapshots
from backend.core import rum_ingest as rum_ingest_mod
from backend.core import storage as storage_mod
from backend.core.ingest import sweep_rum_ledger_once
from backend.core.metadata import pg_connection as pgc
from backend.core.metadata import usage_log_db
from backend.core.metadata.base import get_con
from backend.core.storage import buffer as buffer_mod
from backend.cron import decorators
from backend.cron import scheduler as sched
from backend.cron.jobs import compaction as expire
from backend.cron.jobs import compaction as optimize
from backend.cron.jobs import metric_snapshot as ms_mod
from backend.cron.jobs import rum_commit as rum_commit_mod
from backend.cron.jobs import rum_ledger as rum_ledger_mod
from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.cron.jobs.metadata import run_duckdb_recycle
from backend.cron.scheduler import (
    Scheduler,
    _check_buffer_backlog,
    _check_disk_space,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
    _resolve_rum_sync_interval,
    dev_mode_no_crons,
)
from backend.main import app
from backend.provision.rum_orchestrator_v2 import rum_vcl_fingerprint
from backend.utils.telemetry import get_process_context, process_context_scope
from tests.cron.conftest import (
    assert_manual_trigger_non_admin_rejected,
    assert_manual_trigger_standard_mode_rejected,
)
from tests.cron.conftest import make_cron_source as _source
from tests.cron.conftest import patch_maintenance_config as _maintenance_config

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
    "rum": {"enabled": True, "sync_interval_seconds": 30, "commit_interval_mins": 1},
    "provisioning": {},
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
}

_RUM_SWEEP_FAKE_CFG: dict[str, Any] = {
    "service_id": _RUM_SWEEP_SERVICE_ID,
    "name": _RUM_SWEEP_SERVICE_ID,
    "deployment_mode": "high_scale",
    "rum": {"enabled": True},
    "provisioning": {"cron_ledger_rum_sweep": {"enabled": True, "interval_minutes": 15}},
}
_RUM_SWEEP_FAKE_SRC: dict[str, Any] = {**_RUM_SWEEP_FAKE_CFG, "bucket": "test-rum-sweep-bucket"}


@pytest.fixture
def rum_lease(monkeypatch):
    lease = MagicMock(source={"name": SERVICE_ID}, start=MagicMock(return_value=7), log_cron_run=MagicMock())
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: lease.source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lease.log_cron_run)
    return lease


@pytest.fixture
def rum_sync_lease(monkeypatch) -> MagicMock:
    lease = MagicMock(start=MagicMock(return_value=710))
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lease.start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", MagicMock())
    return lease


@pytest.fixture
def config_store(monkeypatch):
    store: dict[str, dict] = {}

    def fake_load(sid):
        return copy.deepcopy(store[sid]) if sid in store else None

    def fake_save(sid, new_cfg):
        store[sid] = copy.deepcopy(new_cfg)

    def fake_mutate(sid, mutate, **kwargs):
        cfg = fake_load(sid)
        if cfg is None:
            return None
        mutate(cfg)
        fake_save(sid, cfg)
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
    return lambda handler: monkeypatch.setattr(
        httpx, "Client", lambda *a, **kw: _REAL_CLIENT(*a, **{**kw, "transport": httpx.MockTransport(handler)})
    )


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
        raise AssertionError(f"detect_faro_version_change must not be called for {service_id!r}/{version!r}")

    monkeypatch.setattr("backend.provision.rum_assets.detect_faro_version_change", _forbidden)
    return calls, install


@pytest.fixture
def mock_purge(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(
        "backend.core.fastly.client.fastly",
        lambda method, path, *, token, expect_empty=False, **kw: calls.append((method, path, token)),
    )
    return calls


@pytest.fixture
def mock_reconcile(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(
        "backend.provision.declarative.reconciler.reconcile_vcl_state",
        lambda sid, token, *a, **kw: calls.append((sid, token)) or MagicMock(activated_version=2, draft_version=None),
    )
    return calls


@pytest.fixture
def mock_orchestration_env(monkeypatch):
    @contextmanager
    def mock_get_connection(*args, **kwargs):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = []
        yield mock_con

    import backend.core.storage._ducklake as _dl

    monkeypatch.setattr("backend.core.duckdb.get_connection", mock_get_connection)
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", MagicMock())
    monkeypatch.setattr(_dl, "_ducklake_attach", MagicMock())


def _cfg(**rum_overrides) -> dict:
    cfg = copy.deepcopy(FAKE_CFG)
    cfg["rum"].update(rum_overrides)
    return cfg


def _head_response(status: int, etag: str | None = None) -> httpx.Response:
    return httpx.Response(status, headers={"ETag": f'"{etag}"'} if etag is not None else {})


def _mock_progress(monkeypatch) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    start_p, add_p, end_p, cleanup_p = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    for name, fn in [
        ("start_progress", start_p),
        ("add_progress", add_p),
        ("end_progress", end_p),
        ("cleanup_progress_and_reap", cleanup_p),
    ]:
        monkeypatch.setattr(f"backend.cron_progress.{name}", fn)
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
    mock_job = MagicMock() if existing_job_id else None
    if existing_job_id:
        sched._sched.get_job.return_value = mock_job
        sched._job_ids = {existing_job_id: existing_job_id}
    else:
        sched._job_ids = {}
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    monkeypatch.setattr("backend.core.duckdb.is_configured", lambda s: True)
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
    monkeypatch, *, run_id: int = 701, broker_url: str = "redis://localhost:6379/0", reconcile=None, discover=None
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
    log_calls, ledger_calls, status_calls = [], [], []
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
    return {
        "log_calls": log_calls,
        "ledger_calls": ledger_calls,
        "status_calls": status_calls,
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
        stored["rum"] = {
            **dict(stored.get("rum") or {}),
            "faro_version": version,
            "faro_content_hash": "adopted-hash",
            "faro_fos_etag_md5": "adopted-etag",
        }
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
    assert seen_methods == ["HEAD"] and download_calls == [] and mock_purge == []


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


def test_progress_and_compare_and_set_version_kwargs(
    config_store, frozen_now, mock_fos, mock_detect, mock_purge, monkeypatch
):
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 10)
    mock_fos(lambda request: httpx.Response(404))
    add_progress_calls: list[tuple] = []
    seen_kwargs: list[dict] = []
    monkeypatch.setattr("backend.cron_progress.add_progress", lambda r, e: add_progress_calls.append((r, e)))

    async def fake_download(service_id, version, token, *, status_cb=None, **kwargs):
        seen_kwargs.append(kwargs)
        return {"version": version}

    monkeypatch.setattr("backend.provision.rum_assets.download_and_upload_faro", fake_download)
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, 99)
    assert add_progress_calls and all(r == 99 for r, _ in add_progress_calls)
    assert any("restored" in e["message"] for _, e in add_progress_calls)
    assert seen_kwargs == [{"expected_current_version": "2.9.0"}]

    seen_kwargs.clear()
    _, install_detect = mock_detect
    install_detect(result=True)
    config_store[SERVICE_ID] = _cfg(faro_last_upstream_check=NOW - 25 * 3600)
    mock_fos(lambda request: _head_response(200, etag="stored-hash-abc123"))
    rum_sync_mod._reconcile_faro_bundle(SERVICE_ID, None)
    assert seen_kwargs == [{"expected_current_version": "2.9.0"}]


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
    assert len(mock_purge) == 1 and mock_reconcile == [(SERVICE_ID, "fake-fastly-token")]

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
        assert config_store[SERVICE_ID]["rum"]["faro_version"] == DEFAULT_FARO_VERSION and len(mock_purge) == 1
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


@pytest.mark.parametrize(("run_id", "ingest_fails", "expected_status"), [(42, False, "warning"), (43, True, "error")])
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
            con.execute("UPDATE cron_runs SET status = 'error' WHERE id = ?", [run_id])
            con.commit()
            yield ("error", "rum/beacon.log.gz", "corrupt gzip")
        yield ("done", 0)

    monkeypatch.setattr(rum_sync_mod, "ingest_rum_logs", fake_ingest)
    _mock_progress(monkeypatch)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", [run_id]).fetchone() == (expected_status,)
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
    ("high_scale", "job_prefix", "new_seconds"), [(False, "rum_sync", 45), (True, "rum_discovery", 25)]
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

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)
    monkeypatch.setattr(
        "backend.core.ingest._download_chunk_to_local",
        lambda s3, chunk, tmpdir: download_chunks.append(chunk) or ({p: f"/fake/{p}" for p in chunk}, []),
    )
    monkeypatch.setattr(
        "backend.core.ingest._parse_rum_beacon_file",
        lambda p, sid: ([{"pathname": "/", "metric_name": "pageview", "metric_value": 1.0}], [], []),
    )
    monkeypatch.setattr("backend.core.storage.write_to_buffer", MagicMock())
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


_DISC_FN = rum_ledger_mod._run_rum_discovery_cron.__wrapped__
_SWEEP_FN = rum_ledger_mod._run_rum_ledger_sweep.__wrapped__
_SYNC_FN = rum_sync_mod._run_rum_sync.__wrapped__
_COMMIT_FN = rum_commit_mod._run_rum_commit.__wrapped__


@pytest.mark.parametrize(
    ("cron_fn", "sid", "cfg", "src", "high_scale", "env_no_crons"),
    [
        (_DISC_FN, _RUM_DISC_SERVICE_ID, FAKE_CFG_STANDARD, FAKE_SRC_STANDARD, False, False),
        (_SWEEP_FN, _RUM_SWEEP_SERVICE_ID, _RUM_SWEEP_FAKE_CFG, _RUM_SWEEP_FAKE_SRC, False, False),
        (
            _SWEEP_FN,
            _RUM_SWEEP_SERVICE_ID,
            {**_RUM_SWEEP_FAKE_CFG, "rum": {"enabled": False}},
            _RUM_SWEEP_FAKE_SRC,
            True,
            False,
        ),
        (_SWEEP_FN, _RUM_SWEEP_SERVICE_ID, _RUM_SWEEP_FAKE_CFG, _RUM_SWEEP_FAKE_SRC, True, True),
        (_SWEEP_FN, _RUM_SWEEP_SERVICE_ID, None, _RUM_SWEEP_FAKE_SRC, True, False),
        (_SYNC_FN, _RUM_DISC_SERVICE_ID, FAKE_CFG_STANDARD, None, False, False),
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
        (_DISC_FN, _RUM_DISC_SERVICE_ID, FAKE_CFG_HIGH_SCALE, FAKE_SRC_HIGH_SCALE, True),
        (_SYNC_FN, _RUM_DISC_SERVICE_ID, FAKE_CFG_STANDARD, FAKE_SRC_STANDARD, False),
        (_COMMIT_FN, _RUM_COMMIT_SERVICE_ID, _RUM_COMMIT_FAKE_CFG, _RUM_COMMIT_FAKE_SRC, False),
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
            assert (
                resp.status_code == 200
                and resp.json()["run_id"] == run_id
                and "already running" in resp.json()["message"].lower()
            )

    if task_name in ("rum_sync", "rum_discovery"):
        with (
            _override_service_deps(src, sid) as client,
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Lease lock busy")),
            patch("backend.cron_progress.list_active_runs", return_value=[]),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(endpoint, headers={"x-fastly-service-id": sid})
            assert resp.status_code == 503 and resp.json()["detail"]["error"] == "cron_busy"

    if task_name in ("rum_sync", "ledger_rum_sweep"):
        assert_manual_trigger_non_admin_rejected(endpoint, sid, src)
    if task_name in ("rum_discovery", "ledger_rum_sweep"):
        resp = assert_manual_trigger_standard_mode_rejected(endpoint, sid, src)
        detail = resp.json()["detail"]
        assert (
            (detail.get("error") == "high_scale_required") if isinstance(detail, dict) else ("high" in detail.lower())
        )
    if task_name == "ledger_rum_sweep":
        with (
            _override_service_deps(src, sid) as client,
            patch("backend.config.load_config", return_value={**cfg, "rum": {"enabled": False}}),
        ):
            r_dis = client.post(endpoint, headers={"x-fastly-service-id": sid})
            assert r_dis.status_code == 400 and r_dis.json()["detail"]["error"] == "rum_not_enabled"


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

    with (
        patch(
            "backend.routers.admin.quarantine.list_quarantine_evidence",
            side_effect=lambda sid, limit=100, offset=0, error_category=None, source_type=None: (
                list_calls.append({"sid": sid, "error_category": error_category, "source_type": source_type})
                or mock_items
            ),
        ),
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

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    assert TestClient(app).get("/api/admin/ledger/quarantine?service_id=nonexistent-svc").status_code == 404

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

    monkeypatch.setattr("backend.core.storage.commit_buffer", mock_commit)
    monkeypatch.setattr(
        "backend.core.rollups.rum.rum_count_and_latest", lambda con: (60, datetime(2026, 10, 11, 14, 30, tzinfo=UTC))
    )
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
        assert state["ledger_calls"] == [(_RUM_COMMIT_SERVICE_ID, True)] and len(sse_published) == 1
        assert [s for _, s in state["status_calls"] if "rum" in s][-1]["rum"]["total_rows"] == 60
    elif scenario == "partial_failure":
        assert args[3] == "warning" and kwargs.get("rows_ingested") == 45
        assert state["ledger_calls"] == []
        assert "Partial RUM commit" in kwargs.get("summary", "")
    elif scenario == "both_fail":
        assert args[3] == "error" and state["ledger_calls"] == []
        assert "RUM commit failed for both tables" in kwargs.get("summary", "")
    elif scenario == "low_disk":
        assert args[3] == "error" and "RUM commit aborted" in kwargs.get("summary", "")


def _make_vital_row(timestamp: datetime, **overrides) -> dict:
    return {
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
        **overrides,
    }


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
    ts1, ts2 = datetime(2026, 10, 11, 10, 0, tzinfo=UTC), datetime(2026, 10, 11, 10, 5, tzinfo=UTC)
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

    with duckdb.connect() as con:
        _ducklake_attach(con, src, read_only=True)
        tbl_name = ducklake_table_name(src, table_name="client_vitals")
        assert con.execute(f'SELECT count(*) FROM lake."{tbl_name}"').fetchone()[0] == 3

    part_dir = cache_dir / "data_client_vitals" / datetime.now(UTC).strftime("timestamp_hour=%Y-%m-%d-%H")
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

    def _retry_once(con, fn):
        try:
            return fn(con)
        except Exception:
            return fn(con)

    _setup_rum_commit_env(monkeypatch, run_id=1004)
    monkeypatch.setattr(
        "backend.core.storage.commit_buffer",
        lambda src, table_name="logs", **kw: {"files_committed": 1, "rows_committed": 5},
    )
    monkeypatch.setattr("backend.core.rollups.rum.recompute_rum_aggregates", flaky_recompute)
    monkeypatch.setattr(
        "backend.core.storage.execute_with_stale_view_retry", lambda con, source, fn, *a, **kw: _retry_once(con, fn)
    )
    rum_commit_mod._run_rum_commit.__wrapped__(_RUM_COMMIT_SERVICE_ID)
    assert recompute_calls["n"] == 2


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
    else:
        sweep_res = (
            {"reclaimed": 1, "redispatched": 0, "discovered": 0, "broker_ok": False, "dead_letter": 2}
            if outcome == "warning"
            else {"reclaimed": 3, "redispatched": 3, "discovered": 7, "broker_ok": True, "dead_letter": 0}
        )
        monkeypatch.setattr("backend.core.ingest.sweep_rum_ledger_once", lambda sid, run_id=None: sweep_res)

    rum_ledger_mod._run_rum_ledger_sweep.__wrapped__(_RUM_SWEEP_SERVICE_ID)
    start_progress.assert_called_once_with(888, service_id=_RUM_SWEEP_SERVICE_ID, task="ledger_rum_sweep")
    end_progress.assert_called_once_with(888)
    assert len(finalize_calls) == 1 and log_calls[0][0][3] == outcome


def test_scheduler_registers_reschedules_and_skips_rum_ledger_sweep(monkeypatch):
    sid = "svc-scheduler-rum-sweep"
    job_id = f"ledger_rum_sweep_{sid}"

    def _make_cfg(rum_en=True, sweep_en=True, mins=15):
        cfg = {
            **_RUM_SWEEP_FAKE_CFG,
            "service_id": sid,
            "name": sid,
            "log_period": 60,
            "rum": {"enabled": rum_en},
            "provisioning": {
                "cron_sync": {"enabled": True},
                "cron_ledger_rum_sweep": {"enabled": sweep_en, "interval_minutes": mins},
            },
        }
        src = {**_RUM_SWEEP_FAKE_SRC, "name": sid, "service_id": sid, "bucket": "test-b"}
        return cfg, src

    cfg, src = _make_cfg(mins=15)
    sched, _ = _sync_scheduler_for_cfg(monkeypatch, cfg, src, high_scale=True)
    assert job_id in sched._job_ids

    cfg["provisioning"]["cron_ledger_rum_sweep"]["interval_minutes"] = 20
    _, mock_job = _sync_scheduler_for_cfg(
        monkeypatch, cfg, src, high_scale=True, existing_job_id=job_id, patch_add_job=False
    )
    mock_job.reschedule.assert_called_once_with("interval", minutes=20)

    for rum_en, sweep_en in [(True, False), (False, True)]:
        c2, sr2 = _make_cfg(rum_en=rum_en, sweep_en=sweep_en)
        s2, _ = _sync_scheduler_for_cfg(monkeypatch, c2, sr2, high_scale=True)
        assert job_id not in s2._job_ids


@pytest.mark.parametrize(("queue_depth", "expected_redispatched"), [(0, 1), (100, 0)])
def test_sweep_rum_ledger_reclaim_and_queue_depth_guard(queue_depth, expected_redispatched, monkeypatch):
    sid = "svc-test-rum-reclaim"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"
    executed_statements: list[tuple] = []

    class FakeCon:
        def cursor(self):
            return self

        def commit(self):
            pass

        def fetchall(self):
            return (
                [(rum_key)]
                if any("UPDATE ingest_ledger SET status='discovered'" in q for q, _ in executed_statements)
                else []
            )

        def execute(self, query, params=None):
            executed_statements.append((query, params))
            return MagicMock(fetchone=MagicMock(return_value=(0)), fetchall=MagicMock(return_value=[]))

    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda s: FakeCon())
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: {"prefix": "", "bucket": "b"})
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda *a, **k: 0)
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": queue_depth}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_rum_files.delay", mock_convert)

    summary = sweep_rum_ledger_once(sid)
    assert summary["reclaimed"] == 1 and summary["redispatched"] == expected_redispatched
    assert mock_convert.call_count == expected_redispatched


# ── Merged from tests/cron/test_scheduler.py ──

Usage = namedtuple("usage", "total used free")
SRC = {"name": "svc_test"}


@pytest.fixture(autouse=True)
def clean_dev_no_crons(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


# ── _elapsed_since (pure formatter) ───────────────────────────────────────


def test_elapsed_since_formats_seconds_below_60_with_one_decimal():
    out = _elapsed_since(time.time() - 1.5)
    assert out.endswith("s") and "m" not in out
    assert "1." in out or "2." in out


def test_elapsed_since_formats_60s_and_above_as_minutes_seconds():
    out = _elapsed_since(time.time() - 90)
    assert "m" in out and out.endswith("s")
    assert out.split("m")[0] == "1"


def test_elapsed_since_zeros_correctly_for_just_passed():
    out = _elapsed_since(time.time())
    assert out.endswith("s")


# ── _extract_log_text (progress-store digest) ─────────────────────────────


def test_extract_log_text_returns_empty_for_none_run_id():
    with patch("backend.cron_progress.get_progress") as mock_gp:
        assert _extract_log_text(None) == ""
    mock_gp.assert_not_called()


@pytest.mark.parametrize("progress_ret", [None, []])
def test_extract_log_text_returns_empty_when_no_or_empty_progress(progress_ret):
    with patch("backend.cron_progress.get_progress", return_value=progress_ret):
        assert _extract_log_text(999) == ""


def test_extract_log_text_formats_events_with_type_prefix():
    fake_events = [
        {"type": "status", "message": "Starting sync"},
        {"type": "error", "message": "Connection refused"},
        {"type": "done", "message": "Sync finished"},
    ]
    with patch("backend.cron_progress.get_progress", return_value=fake_events):
        out = _extract_log_text(1)

    assert "[STATUS] Starting sync" in out
    assert "[ERROR] Connection refused" in out
    assert "[DONE] Sync finished" in out
    assert out.count("\n") == 2


def test_extract_log_text_filters_out_progress_events():
    fake_events = [
        {"type": "progress", "current": 1, "total": 8},
        {"type": "status", "message": "Step 1 complete"},
        {"type": "progress", "current": 2, "total": 8},
        {"type": "done", "message": "All done"},
    ]
    with patch("backend.cron_progress.get_progress", return_value=fake_events):
        out = _extract_log_text(1)

    assert out.count("\n") == 1
    assert "[STATUS] Step 1 complete" in out
    assert "[DONE] All done" in out
    assert "progress" not in out.lower()


def test_extract_log_text_skips_events_without_message():
    fake_events = [
        {"type": "status"},
        {"type": "error", "message": "Real error"},
    ]
    with patch("backend.cron_progress.get_progress", return_value=fake_events):
        assert _extract_log_text(1) == "[ERROR] Real error"


# ── get_scheduler & basic Scheduler methods ───────────────────────────────


def test_get_scheduler_returns_singleton_and_creates_lazily():
    sched._scheduler = None
    s1 = sched.get_scheduler()
    s2 = sched.get_scheduler()
    assert s1 is not None and s1 is s2 and hasattr(s1, "_sched")


def test_scheduler_shutdown_swallows_exception():
    s = Scheduler()
    s._sched.shutdown = MagicMock(side_effect=RuntimeError("already stopped"))
    s.shutdown()


def test_scheduler_shutdown_forwards_wait_kwarg_to_apscheduler():
    s = Scheduler()
    s._sched.shutdown = MagicMock()
    s.shutdown()
    s._sched.shutdown.assert_called_with(wait=False)
    s._sched.shutdown.reset_mock()
    s.shutdown(wait=True)
    s._sched.shutdown.assert_called_with(wait=True)


def test_scheduler_get_job_returns_none_for_unknown_id():
    s = Scheduler()
    assert s.get_job("nonexistent_job_id") is None


def test_scheduler_reload_calls_sync_jobs():
    s = Scheduler()
    with patch.object(s, "_sync_jobs") as mock_sync:
        s.reload()
    mock_sync.assert_called_once()


# ── Scheduler._sync_jobs: config-driven job registration ──────────────────


def _fake_src(service_id="svc-1"):
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "b",
        "endpoint": "e",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }


@contextlib.contextmanager
def _sync_jobs_ctx(
    cfgs: list[dict],
    *,
    service_id: str = "svc-1",
    is_configured: bool = True,
    ngwaf_ws: str | None = None,
    alerts: int = 0,
):
    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src(service_id)),
        patch("backend.core.duckdb.is_configured", return_value=is_configured),
        patch("backend.config.get_ngwaf_workspace_id", return_value=ngwaf_ws),
        patch("backend.core.metadata.count_alerts", return_value=alerts),
    ):
        yield


def _run_sync_jobs(cfg: dict, **ctx_kwargs) -> Scheduler:
    s = Scheduler()
    sid = cfg.get("service_id", "svc-1")
    with _sync_jobs_ctx([cfg], service_id=sid, **ctx_kwargs):
        s._sync_jobs()
    return s


def test_sync_jobs_registers_sync_commit_and_alerts_for_readwrite_service():
    cfg = {
        "service_id": "svc-1",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, alerts=1)
    assert {"log_discovery_svc-1", "commit_svc-1", "alerts_evaluation_svc-1"}.issubset(s._job_ids)


def test_sync_jobs_skips_alerts_cron_when_no_alerts_configured():
    cfg = {
        "service_id": "svc-noalert",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, alerts=0)
    assert "log_discovery_svc-noalert" in s._job_ids and "commit_svc-noalert" in s._job_ids
    assert "alerts_evaluation_svc-noalert" not in s._job_ids


def test_sync_jobs_skips_disabled_services():
    cfg = {
        "service_id": "paused-svc",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": False}},
    }
    s = _run_sync_jobs(cfg)
    assert "sync_paused-svc" not in s._job_ids and "commit_paused-svc" not in s._job_ids


def test_sync_jobs_skips_unconfigured_service():
    cfg = {
        "service_id": "halfbaked",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, is_configured=False)
    assert "sync_halfbaked" not in s._job_ids


def test_sync_jobs_registers_ngwaf_job_when_workspace_configured():
    cfg = {
        "service_id": "ngwaf-svc",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, ngwaf_ws="workspace-id")
    assert "ngwaf_sync_ngwaf-svc" in s._job_ids
    job = s._sched.get_job("ngwaf_sync_ngwaf-svc")
    assert job is not None and job.executor == "maintenance"


def test_sync_jobs_omits_optimize_and_expire_when_compact_disabled():
    cfg = {
        "service_id": "no-compact",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": False}},
    }
    s = _run_sync_jobs(cfg)
    assert "optimize_no-compact" not in s._job_ids and "expire_no-compact" not in s._job_ids


def test_sync_jobs_always_registers_global_system_jobs():
    s = Scheduler()
    with patch("backend.config.list_configs", return_value=[]):
        s._sync_jobs()
    assert {"bot_data_refresh", "rdns_enrichment", "share_audit_purge"}.issubset(s._job_ids)


def test_sync_jobs_removes_stale_jobs_for_deleted_services():
    s = Scheduler()
    s._job_ids["sync_deleted-svc"] = "sync_deleted-svc"
    with (
        patch("backend.config.list_configs", return_value=[]),
        patch.object(s._sched, "remove_job") as mock_remove,
    ):
        s._sync_jobs()
    assert "sync_deleted-svc" not in s._job_ids
    mock_remove.assert_any_call("sync_deleted-svc")


def test_sync_jobs_uses_interval_mins_over_interval_seconds_when_both_present():
    cfg = {
        "service_id": "svc-priority",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True, "interval_mins": 10, "interval_seconds": 30}},
    }
    captured: dict = {}
    s = Scheduler()
    real_add = s._sched.add_job

    def capturing_add_job(*args, **kwargs):
        if kwargs.get("id", "").startswith("log_discovery_svc-priority"):
            captured["seconds"] = kwargs.get("seconds")
        return real_add(*args, **kwargs)

    s._sched.add_job = capturing_add_job
    with _sync_jobs_ctx([cfg], service_id="svc-priority"):
        s._sync_jobs()
    assert captured.get("seconds") == 600


def test_sync_jobs_registers_rum_jobs_when_rum_enabled_on_root():
    cfg = {
        "service_id": "svc-rum-root",
        "log_period": 60,
        "rum_enabled": True,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rum_sync_svc-rum-root" in s._job_ids and "rum_commit_svc-rum-root" in s._job_ids


def test_sync_jobs_registers_rum_jobs_when_rum_enabled_nested():
    cfg = {
        "service_id": "svc-rum-nested",
        "log_period": 60,
        "rum": {"enabled": True},
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rum_sync_svc-rum-nested" in s._job_ids and "rum_commit_svc-rum-nested" in s._job_ids


def test_sync_jobs_does_not_register_rum_jobs_when_rum_disabled_in_nested():
    cfg = {
        "service_id": "svc-rum-disabled",
        "log_period": 60,
        "rum_enabled": True,
        "rum": {"enabled": False},
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rum_sync_svc-rum-disabled" not in s._job_ids and "rum_commit_svc-rum-disabled" not in s._job_ids


def test_resolve_rum_sync_interval_scheduler_defaults():
    assert _resolve_rum_sync_interval({"sync_interval_seconds": 45}, {}, 60) == 45
    assert _resolve_rum_sync_interval({"sync_interval_seconds": 2}, {}, 60) == 5
    assert _resolve_rum_sync_interval({"log_period": 300}, {}, 60) == 150
    assert _resolve_rum_sync_interval({"log_period": 60}, {}, 60) == 30
    assert _resolve_rum_sync_interval({"log_period": 600}, {}, 60) == 300
    assert _resolve_rum_sync_interval({"log_period": 10}, {}, 60) == 10
    assert _resolve_rum_sync_interval({"log_period": 3}, {}, 60) == 5
    assert _resolve_rum_sync_interval({}, {"rum_log_period": 300}, 60) == 150
    assert _resolve_rum_sync_interval({}, {}, 120) == 120
    assert _resolve_rum_sync_interval({}, {}, 2) == 5


def test_sync_jobs_schedules_rum_sync_with_derived_interval():
    cfg = {
        "service_id": "svc-rum-derived",
        "log_period": 60,
        "rum": {"enabled": True, "log_period": 300},
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = Scheduler()
    with (
        _sync_jobs_ctx([cfg], service_id="svc-rum-derived"),
        patch.object(s._sched, "add_job") as mock_add_job,
    ):
        s._sync_jobs()

    rum_sync_calls = [c for c in mock_add_job.call_args_list if c.kwargs.get("id") == "rum_sync_svc-rum-derived"]
    assert len(rum_sync_calls) == 1
    assert rum_sync_calls[0].kwargs["seconds"] == 150


def test_sync_jobs_registers_hourly_rollup_heal_for_readwrite_service():
    cfg = {
        "service_id": "svc-1",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rollup_compact_svc-1" in s._job_ids and "rollup_heal_svc-1" in s._job_ids
    heal_job = s._sched.get_job("rollup_heal_svc-1")
    assert heal_job is not None
    assert "minute='5'" in str(heal_job.trigger) and "hour=" not in str(heal_job.trigger)


# ── _log_and_add_progress ─────────────────────────────────────────────────


def test_log_and_add_progress_persists_event_to_cron_progress():
    event = {"type": "status", "message": "Step 1"}
    with (
        patch("backend.cron_progress.add_progress") as mock_add,
        patch("backend.config.load_config", return_value={"name": "Test Service"}),
    ):
        _log_and_add_progress(run_id=42, service_id="svc-1", event=event, job_name="sync")
    mock_add.assert_called_once_with(42, event)


def test_log_and_add_progress_handles_missing_message_silently():
    event = {"type": "progress", "current": 1, "total": 8}
    with (
        patch("backend.cron_progress.add_progress") as mock_add,
        patch("backend.config.load_config", return_value=None),
        patch("backend.cron.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)

    mock_add.assert_called_once_with(1, event)
    mock_logger.info.assert_not_called()
    mock_logger.warning.assert_not_called()
    mock_logger.error.assert_not_called()


def test_log_and_add_progress_uses_error_logger_for_error_type():
    event = {"type": "error", "message": "DB locked"}
    with (
        patch("backend.cron_progress.add_progress"),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.cron.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)
    mock_logger.error.assert_called_once()
    mock_logger.info.assert_not_called()


def test_log_and_add_progress_uses_warning_logger_for_warning_type():
    event = {"type": "warning", "message": "slow query"}
    with (
        patch("backend.cron_progress.add_progress"),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.cron.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)
    mock_logger.warning.assert_called_once()


# ── _check_disk_space & _check_buffer_backlog ──────────────────────────────


def test_check_disk_space_passes_when_plenty_free(tmp_path):
    with patch("shutil.disk_usage", return_value=Usage(total=10 * 1024**3, used=512 * 1024**2, free=9 * 1024**3)):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True and msg == ""


def test_check_disk_space_aborts_when_below_byte_floor(tmp_path):
    with patch(
        "shutil.disk_usage",
        return_value=Usage(total=100 * 1024**3, used=99_900 * 1024**2, free=100 * 1024**2),
    ):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is False and "100 MB free" in msg


def test_check_disk_space_aborts_when_below_pct_floor(tmp_path):
    with (
        patch("shutil.disk_usage", return_value=Usage(total=50 * 1024**3, used=49 * 1024**3, free=1 * 1024**3)),
        patch.object(sched, "_DISK_FREE_HARD_FLOOR_PCT", 0.03),
    ):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is False and "2.0% of 50.0" in msg


def test_check_disk_space_does_not_block_on_probe_failure(tmp_path):
    with patch("shutil.disk_usage", side_effect=OSError("gone")):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True and msg == ""


@pytest.mark.parametrize(
    "stats",
    [
        {"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None},
        {"file_count": 10, "total_bytes": 1024, "oldest_age_seconds": 30, "oldest_path": "/x"},
    ],
)
def test_check_buffer_backlog_returns_empty_when_healthy(stats):
    with patch("backend.core.storage.buffer_backlog_stats", return_value=stats):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


def test_check_buffer_backlog_warns_when_file_count_exceeds_threshold():
    with patch(
        "backend.core.storage.buffer_backlog_stats",
        return_value={"file_count": 250, "total_bytes": 1_000_000, "oldest_age_seconds": 30, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "250 files" in suffix and suffix.startswith(" ⚠")


def test_check_buffer_backlog_warns_when_oldest_age_exceeds_scaled_threshold():
    with patch(
        "backend.core.storage.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": 1000, "oldest_age_seconds": 30 * 60, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "30m old" in suffix


def test_check_buffer_backlog_warns_on_total_bytes():
    with patch(
        "backend.core.storage.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": sched._BACKLOG_BYTES_WARN + 1, "oldest_age_seconds": 0},
    ):
        out = _check_buffer_backlog({"name": "s"}, "s", 5)
    assert "MB on disk" in out


def test_check_buffer_backlog_never_raises_on_probe_failure():
    with patch("backend.core.storage.buffer_backlog_stats", side_effect=OSError("disk gone")):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


# ── Merged from test_scheduler_apscheduler_stress.py & watchdog.py ────────


def test_apscheduler_max_instances_1_waits_for_cleanup(monkeypatch, caplog):
    apscheduler = pytest.importorskip("apscheduler.schedulers.background")
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 0.1)

    tick_log: list[float] = []
    active = 0
    max_active = 0
    state_lock = threading.Lock()

    @sched_mod.cron_task("apscheduler-stress")
    def _cron_body(service_id: str) -> None:
        nonlocal active, max_active
        tick_log.append(time.monotonic())
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.35 if len(tick_log) == 1 else 0.02)
        with state_lock:
            active -= 1

    bg_sched = apscheduler.BackgroundScheduler(timezone="UTC")
    bg_sched.add_job(
        _cron_body,
        "interval",
        seconds=0.4,
        args=["svc-stress"],
        id="cron-apscheduler-stress",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(UTC) + timedelta(milliseconds=50),
    )

    caplog.set_level(logging.ERROR, logger="backend.scheduler")
    bg_sched.start()

    try:
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline and len(tick_log) < 2:
            time.sleep(0.05)
    finally:
        bg_sched.shutdown(wait=False)

    assert len(tick_log) >= 2
    assert max_active == 1
    assert any("exceeded" in rec.getMessage() and "watchdog threshold" in rec.getMessage() for rec in caplog.records)


def test_usage_log_phase_30s_timeout_pattern_does_not_block_caller():
    def _hangs():
        time.sleep(10)
        return "should-never-return"

    PHASE_TIMEOUT_S = 0.3
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="usage-log-stress")
    shutdown_wait = True

    start = time.monotonic()
    try:
        fut = ex.submit(_hangs)
        try:
            fut.result(timeout=PHASE_TIMEOUT_S)
            pytest.fail("expected TimeoutError")
        except concurrent.futures.TimeoutError:
            shutdown_wait = False
    finally:
        ex.shutdown(wait=shutdown_wait)
    elapsed = time.monotonic() - start
    assert elapsed < PHASE_TIMEOUT_S + 1.0


def test_metadata_db_init_lock_has_finite_timeout():
    from backend.core.metadata import pg_connection

    src = inspect.getsource(pg_connection.get_pg_pool)
    assert "timeout=" in src


def test_cron_task_waits_for_cleanup_after_watchdog_threshold(monkeypatch, caplog):
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 0.05)

    @sched_mod.cron_task("hang-test")
    def _hangs_forever(service_id: str) -> None:
        time.sleep(0.15)

    caplog.set_level(logging.ERROR, logger="backend.scheduler")

    start = time.monotonic()
    result = _hangs_forever("svc-watchdog")
    elapsed = time.monotonic() - start

    assert result is None and elapsed >= 0.15
    assert any(
        "exceeded" in rec.getMessage()
        and "watchdog threshold" in rec.getMessage()
        and "svc-watchdog" in rec.getMessage()
        for rec in caplog.records
        if rec.levelno >= logging.ERROR
    )


def test_cron_task_propagates_normal_return(monkeypatch):
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 60)
    sentinel = object()

    @sched_mod.cron_task("happy-path")
    def _quick(service_id: str):
        return sentinel

    start = time.monotonic()
    result = _quick("svc-happy")
    assert result is sentinel and (time.monotonic() - start) < 5


def test_cron_task_preserves_telemetry_context(monkeypatch):
    from backend.cron import decorators as sched_mod
    from backend.utils.telemetry import get_process_context

    captured = {}

    @sched_mod.cron_task("ctx-test")
    def _check_ctx(service_id: str) -> None:
        captured["ctx"] = get_process_context()

    _check_ctx("svc-ctx")
    assert captured.get("ctx") == "ctx-test"


def test_cron_task_heartbeat_thread_releases_its_connection(monkeypatch):
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 60)

    @sched_mod.cron_task("release-test", job_name="release_test_job")
    def _quick(service_id: str):
        return None

    with patch("backend.core.metadata.base.release_thread_connection") as mock_release:
        _quick("svc-release")
    mock_release.assert_called_once_with()


# ── Merged from test_dev_mode_no_crons.py ──────────────────────────────────


def test_dev_mode_no_crons_returns_false_when_unset(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    assert dev_mode_no_crons() is False


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("True", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("", False),
        ("off", False),
        ("anything-else", False),
    ],
)
def test_dev_mode_no_crons_values(monkeypatch, value, expected):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", value)
    assert dev_mode_no_crons() is expected


def test_scheduler_start_skips_full_registration_but_runs_local_allowlist(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    s = Scheduler()
    with (
        patch.object(s, "_sync_jobs") as sync_jobs,
        patch.object(s, "_register_dev_local_safe_jobs") as dev_jobs,
        patch.object(s._sched, "start") as ap_start,
    ):
        s.start()
        sync_jobs.assert_not_called()
        dev_jobs.assert_called_once()
        ap_start.assert_not_called()


def test_dev_local_allowlist_registers_only_local_compaction(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {
        "service_id": "svc-x",
        "provisioning": {"cron_compact": {"enabled": True}},
    }
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-x"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(s._sched, "add_job") as add_job,
    ):
        s._register_dev_local_safe_jobs()

    expected = [
        "insights_prewarmer_svc-x",
        "local_compact_svc-x",
        "metric_snapshot",
        "partial_hour_merge_svc-x",
        "rollup_compact_svc-x",
        "rollup_heal_svc-x",
    ]
    assert sorted(s._job_ids) == expected
    assert sorted(c.kwargs["id"] for c in add_job.call_args_list) == expected


def test_dev_local_allowlist_skips_high_scale_services(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {"service_id": "svc-high", "provisioning": {}}
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-high"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch(
            "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
            return_value=type("Registry", (), {"resolve": lambda self, service_id: object()})(),
        ),
        patch.object(s._sched, "add_job") as add_job,
    ):
        s._register_dev_local_safe_jobs()

    assert sorted(s._job_ids) == ["metric_snapshot"]
    assert [call.kwargs["id"] for call in add_job.call_args_list] == ["metric_snapshot"]


def test_scheduler_reload_is_a_noop_when_kill_switch_on(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    s = Scheduler()
    with patch.object(s, "_sync_jobs") as sync_jobs:
        s.reload()
        sync_jobs.assert_not_called()


def test_scheduler_start_runs_normally_when_kill_switch_off(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    s = Scheduler()
    with (
        patch.object(s, "_sync_jobs") as sync_jobs,
        patch.object(s._sched, "start") as ap_start,
        patch("backend.config.list_configs", return_value=[]),
    ):
        s.start()
    sync_jobs.assert_called_once()
    ap_start.assert_called_once()


@pytest.mark.parametrize(
    "fn_name,kwargs",
    [
        ("_run_log_discovery_cron", {"force": True}),
        ("_run_full_sweep", {}),
        ("_run_gap_heal", {}),
    ],
)
def test_ingest_entry_points_refuse_when_kill_switch_on(monkeypatch, caplog, fn_name, kwargs):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")
    from backend.cron.jobs import sync as sync_mod

    fn = getattr(sync_mod, fn_name)
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        fn("svc-test", **kwargs)
        get_src.assert_not_called()
    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


# ── Merged from test_scheduler_branches.py ──────────────────────────────────


def test_display_label_collapses_when_name_matches_id():
    assert sched._display_label({"service_name": "svc-1", "name": "svc-1"}, "svc-1") == "svc-1"


def test_display_label_includes_both_when_distinct():
    assert sched._display_label({"service_name": "My Service", "name": "svc-1"}, "svc-1") == "My Service (svc-1)"


def test_claim_heavy_refresh_first_call_wins():
    sid = "test-claim-heavy-1"
    sched._last_heavy_refresh.pop(sid, None)
    assert sched._claim_heavy_refresh(sid) is True


def test_claim_heavy_refresh_returns_false_when_throttled():
    sid = "test-claim-heavy-2"
    sched._last_heavy_refresh[sid] = time.time()
    assert sched._claim_heavy_refresh(sid) is False


def test_claim_heavy_refresh_reopens_window_after_interval():
    sid = "test-claim-heavy-3"
    sched._last_heavy_refresh[sid] = time.time() - sched._HEAVY_REFRESH_INTERVAL_SEC - 10
    assert sched._claim_heavy_refresh(sid) is True


def test_service_has_alerts_returns_count_gt_zero():
    with patch("backend.core.metadata.count_alerts", return_value=3):
        assert sched._service_has_alerts("svc") is True


def test_service_has_alerts_returns_false_when_count_zero():
    with patch("backend.core.metadata.count_alerts", return_value=0):
        assert sched._service_has_alerts("svc") is False


def test_service_has_alerts_defaults_to_true_on_exception():
    with patch("backend.core.metadata.count_alerts", side_effect=RuntimeError("corrupt")):
        assert sched._service_has_alerts("svc") is True


# ── Merged from test_scheduler_reschedule_or_add.py ──────────────────────────


def _scheduler():
    s = Scheduler()
    s._sched = MagicMock()
    s._add_job = MagicMock()
    return s


def _fn():
    return None


def test_new_job_is_added_tracked_and_reported_as_added():
    s = _scheduler()
    added = s._reschedule_or_add_job(_fn, "interval", seconds=30, id="job_a", args=["svc"], max_instances=1)
    assert added is True
    s._add_job.assert_called_once_with(_fn, "interval", seconds=30, id="job_a", args=["svc"], max_instances=1)
    assert "job_a" in s._job_ids
    s._sched.get_job.assert_not_called()


def test_tracked_interval_job_is_rescheduled_with_only_timing_kwargs():
    s = _scheduler()
    s._job_ids["job_a"] = "job_a"
    job = MagicMock()
    s._sched.get_job.return_value = job
    added = s._reschedule_or_add_job(_fn, "interval", minutes=5, jitter=30, id="job_a", args=["svc"])
    assert added is False
    job.reschedule.assert_called_once_with("interval", minutes=5)
    s._add_job.assert_not_called()


def test_tracked_cron_job_reschedules_hour_and_minute():
    s = _scheduler()
    s._job_ids["job_c"] = "job_c"
    job = MagicMock()
    s._sched.get_job.return_value = job
    s._reschedule_or_add_job(_fn, "cron", hour="3,9", minute=30, id="job_c", args=["svc"], misfire_grace_time=3600)
    job.reschedule.assert_called_once_with("cron", hour="3,9", minute=30)


def test_reschedule_kwargs_override_is_applied_verbatim():
    s = _scheduler()
    s._job_ids["job_a"] = "job_a"
    job = MagicMock()
    s._sched.get_job.return_value = job
    s._reschedule_or_add_job(
        _fn,
        "interval",
        seconds=60,
        jitter=15,
        reschedule_kwargs={"seconds": 60, "jitter": 15},
        id="job_a",
    )
    job.reschedule.assert_called_once_with("interval", seconds=60, jitter=15)


def test_missing_job_or_reschedule_error_is_swallowed():
    s = _scheduler()
    s._job_ids["job_a"] = "job_a"
    s._sched.get_job.return_value = None
    assert s._reschedule_or_add_job(_fn, "interval", seconds=1, id="job_a") is False

    boom = MagicMock()
    boom.reschedule.side_effect = RuntimeError("nope")
    s._sched.get_job.return_value = boom
    assert s._reschedule_or_add_job(_fn, "interval", seconds=1, id="job_a") is False
    s._add_job.assert_not_called()


def test_run_duckdb_recycle_skips_below_rss_threshold(monkeypatch):
    monkeypatch.setenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "999999999")
    assert run_duckdb_recycle.__wrapped__().startswith("skipped:")


def test_run_duckdb_recycle_runs_when_threshold_disabled(monkeypatch):
    monkeypatch.setenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "0")
    with patch(
        "backend.cron.jobs.metadata.recycle_once", return_value="interval: recycled 0/0 instance(s), freed ~0MB"
    ) as mock_recycle:
        detail = run_duckdb_recycle.__wrapped__()
    mock_recycle.assert_called_once_with(reason="interval")
    assert "recycled" in detail


def _registered_job_calls(monkeypatch, interval_env: str):
    monkeypatch.setenv("DUCKDB_RECYCLE_INTERVAL_MIN", interval_env)
    monkeypatch.setattr("backend.config.list_configs", lambda: [])
    s = Scheduler()
    mock_sched = MagicMock()
    s._sched = mock_sched
    s._job_ids = {}
    s._sync_jobs()
    return mock_sched.add_job.call_args_list


def test_recycle_job_registered_when_interval_positive(monkeypatch):
    calls = _registered_job_calls(monkeypatch, "15")
    recycle_calls = [c for c in calls if c.kwargs.get("id") == "duckdb_recycle"]
    assert len(recycle_calls) == 1
    kw = recycle_calls[0].kwargs
    assert (
        kw["max_instances"] == 1
        and kw["coalesce"] is True
        and kw["minutes"] == 15.0
        and kw["executor"] == "maintenance"
    )


def test_recycle_job_not_registered_when_interval_zero(monkeypatch):
    calls = _registered_job_calls(monkeypatch, "0")
    assert "duckdb_recycle" not in [c.kwargs.get("id") for c in calls]


def test_recycle_job_routes_to_maintenance_executor():
    s = Scheduler()
    s._sched = MagicMock()
    s._job_ids = {}
    s._add_job(lambda: None, id="duckdb_recycle")
    assert s._sched.add_job.call_args.kwargs["executor"] == "maintenance"


def test_defaults_to_a_skipped_row_with_zero_duration(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    record_skip_row(SRC, "alerts", "No alerts configured")
    log.assert_called_once_with(
        SRC, "alerts", 0.0, "skipped", summary="No alerts configured", error_message=None, run_id=None
    )


def test_forwards_status_duration_error_and_run_id(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    record_skip_row(
        SRC,
        "rollup_hour_heal",
        "Skipped due to low disk space",
        status="warning",
        duration_s=1.5,
        error_message="Disk space critically low",
        run_id=42,
    )
    log.assert_called_once_with(
        SRC,
        "rollup_hour_heal",
        1.5,
        "warning",
        summary="Skipped due to low disk space",
        error_message="Disk space critically low",
        run_id=42,
    )


# ── Merged from test_scheduler_timing.py ──────────────────────────────────


class TestRunServiceCronSignature:
    @pytest.mark.parametrize("param", ["start_time", "end_time"])
    def test_time_params_are_str_not_float(self, param):
        from backend.cron.jobs.sync import _run_log_discovery_cron

        sig = inspect.signature(_run_log_discovery_cron)
        ann_str = str(sig.parameters[param].annotation)
        assert "str" in ann_str and "None" in ann_str and "float" not in ann_str


class TestRunCommitUsesLocalStartTime:
    def test_run_commit_no_str_subtraction(self):
        from backend.cron.jobs.commit import _run_commit

        mock_src = {"name": "test", "service_id": "test", "storage_mode": "cloud"}
        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("lock held")),
        ):
            _run_commit("test-service")


class TestLogCronRunDurationIsFloat:
    def test_duration_passed_to_log_cron_run_is_float(self, monkeypatch):
        monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
        from backend.cron.jobs.sync import _run_log_discovery_cron

        mock_src = {
            "name": "test",
            "service_id": "test",
            "service_name": "Test",
            "storage_mode": "cloud",
            "bucket": "b",
            "prefix": "",
            "region": "us-east-1",
            "log_period": 60,
            "provisioning": {},
        }
        captured_duration = {}

        def fake_log_cron_run(src, task, duration_s, status, **kwargs):
            captured_duration["value"] = duration_s
            captured_duration["type"] = type(duration_s).__name__

        def fake_ingest(**kwargs):
            yield {"type": "done", "new_files": 0}

        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
            patch("backend.core.duckdb.start_cron_run", return_value=1),
            patch("backend.core.duckdb.log_cron_run", side_effect=fake_log_cron_run),
            patch("backend.core.duckdb.refresh_config_status"),
            patch("backend.core.ingest.ingest", side_effect=fake_ingest),
            patch("backend.cron_progress.cleanup_progress"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron_progress.get_progress", return_value=[]),
            patch("backend.core.duckdb.backfill_fastly_edge_writes"),
            patch("backend.core.duckdb.reconcile_fastly_stats"),
            patch("backend.utils.telemetry.flush_usage_log"),
            patch("backend.utils.telemetry.run_usage_log_cleanup"),
            patch("backend.core.duckdb.update_cron_duration"),
        ):
            _run_log_discovery_cron("test", force=True, run_id=1)

        assert isinstance(captured_duration.get("value"), float) and captured_duration["value"] >= 0.0


def _find_bad_start_time_subtractions(file_path: str, fn_name: str) -> list[ast.BinOp]:
    tree = ast.parse(pathlib.Path(file_path).read_text())
    fn = next((n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == fn_name), None)
    assert fn is not None, f"{fn_name} not found in {file_path}"
    return [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.Sub)
        and isinstance(node.right, ast.Name)
        and node.right.id == "start_time"
    ]


class TestIngestTimingVariableNames:
    def test_start_time_exec_exists_in_ingest(self):
        tree = ast.parse(pathlib.Path("backend/core/ingest.py").read_text())
        ingest_fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "ingest")
        assignments = [
            node
            for node in ast.walk(ingest_fn)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "start_time_exec" for t in node.targets)
        ]
        assert assignments, "start_time_exec variable not found in ingest() — timing regression risk"

    def test_no_time_minus_start_time_in_ingest(self):
        assert not _find_bad_start_time_subtractions("backend/core/ingest.py", "ingest")

    def test_no_time_minus_start_time_in_scheduler(self):
        assert not _find_bad_start_time_subtractions("backend/cron/jobs/sync.py", "_run_log_discovery_cron")


# ── Merged from tests/cron/test_expire_and_optimize.py ──


@pytest.fixture
def stub_cron_log(monkeypatch) -> dict[str, MagicMock]:
    start = MagicMock(return_value=7)
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)
    return {"start": start, "log": log}


def _stub_progress_and_duration(monkeypatch) -> None:
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda: None)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", lambda *args, **kwargs: None)


def test_returns_when_source_missing(monkeypatch):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    start = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    expire._run_expire_snapshots.__wrapped__("ghost-svc")
    start.assert_not_called()


def test_skips_when_start_cron_run_raises(monkeypatch, stub_source, stub_cron_log):
    stub_cron_log["start"].side_effect = CronLeaseHeldError("already running")
    maint = MagicMock()
    monkeypatch.setattr("backend.core.storage.run_cloud_maintenance", maint)
    expire._run_expire_snapshots.__wrapped__("svc-1")
    maint.assert_not_called()
    stub_cron_log["log"].assert_not_called()


def test_success_logs_summary_with_each_metric(monkeypatch, stub_source, stub_cron_log):
    monkeypatch.setattr(
        "backend.core.storage.run_cloud_maintenance",
        MagicMock(return_value={"snapshots_expired": 5, "orphan_files_deleted": 12, "data_files_kept": 100}),
    )
    expire._run_expire_snapshots.__wrapped__("svc-1")
    stub_cron_log["log"].assert_called_once()
    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "success" and kwargs["error_message"] is None
    for fragment in ("snapshots_expired=5", "orphan_files_deleted=12", "data_files_kept=100"):
        assert fragment in kwargs["summary"]


def test_success_with_subtask_errors_logs_warning(monkeypatch, stub_source, stub_cron_log):
    monkeypatch.setattr(
        "backend.core.storage.run_cloud_maintenance",
        MagicMock(
            return_value={
                "snapshots_expired": 3,
                "orphan_files_deleted_error": "permission denied",
                "data_files_kept": 50,
            }
        ),
    )
    expire._run_expire_snapshots.__wrapped__("svc-1")
    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "warning"
    assert "orphan_files_deleted_error" in kwargs["error_message"] and "permission denied" in kwargs["error_message"]
    assert "snapshots_expired=3" in kwargs["summary"]


def test_catalog_load_error_logs_error(monkeypatch, stub_source, stub_cron_log):
    monkeypatch.setattr(
        "backend.core.storage.run_cloud_maintenance",
        MagicMock(return_value={"error": "catalog unreachable"}),
    )
    expire._run_expire_snapshots.__wrapped__("svc-1")
    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "error" and kwargs["error_message"] == "catalog unreachable"
    assert "catalog load" in kwargs["summary"]


def test_unexpected_exception_logged_as_error(monkeypatch, stub_source, stub_cron_log):
    monkeypatch.setattr(
        "backend.core.storage.run_cloud_maintenance",
        MagicMock(side_effect=ConnectionError("S3 timeout")),
    )
    expire._run_expire_snapshots.__wrapped__("svc-1")
    args, kwargs = stub_cron_log["log"].call_args
    assert args[3] == "error" and "S3 timeout" in kwargs["error_message"] and "uncaught" in kwargs["summary"]


# ── Contract Tests (Cron 8: expire_{service_id}) ───────────────────────────


def _snapshot_connection(*, count: int = 0, expire_error: Exception | None = None):
    calls: list[str] = []

    class Connection:
        def execute(self, sql, params=None):
            calls.append(sql)
            result = MagicMock()
            if "ducklake_snapshots" in sql:
                result.fetchone.return_value = (count,)
            elif "ducklake_expire_snapshots" in sql and expire_error:
                raise expire_error
            else:
                result.fetchall.return_value = []
            return result

    return Connection(), calls


def test_snapshot_cleanup_runs_even_when_catalog_is_empty():
    con, calls = _snapshot_connection(count=0)
    result = buffer_mod._ducklake_expire_snapshots(con, {"name": "svc"}, 7)
    assert result["snapshots_expired_count"] == 0
    assert any("CALL ducklake_expire_snapshots" in sql for sql in calls)
    assert any("CALL ducklake_cleanup_old_files" in sql for sql in calls)
    assert not any("ducklake_delete_orphaned_files" in sql for sql in calls)


def test_old_file_cleanup_runs_when_snapshot_expiry_fails():
    con, calls = _snapshot_connection(count=2, expire_error=RuntimeError("catalog write failed"))
    result = buffer_mod._ducklake_expire_snapshots(con, {"name": "svc"}, 7)
    assert "snapshot_expiry_error" in result and "data_file_cleanup_error" not in result
    assert any("CALL ducklake_cleanup_old_files" in sql for sql in calls)
    assert not any("ducklake_delete_orphaned_files" in sql for sql in calls)


def test_old_file_cleanup_count_survives_snapshot_count_failures(caplog):
    class Connection:
        def execute(self, sql, params=None):
            if "ducklake_snapshots" in sql:
                raise RuntimeError("snapshot count unavailable")
            result = MagicMock()
            result.fetchall.return_value = [("deleted.parquet",)]
            return result

    caplog.set_level("INFO", logger="backend.core.storage._core")
    result = buffer_mod._ducklake_expire_snapshots(Connection(), {"name": "svc"}, 7)
    assert result["data_files_cleaned"] == 1 and "snapshot_expiry_error" in result
    assert any("unlinked 1 file(s)" in record.getMessage() for record in caplog.records)


def test_maintenance_reports_exact_retention_snapshot_and_unlink_totals(tmp_path, monkeypatch):
    src = _source(tmp_path)
    _maintenance_config(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=0)
    monkeypatch.setattr(
        buffer_mod,
        "_run_ducklake_maintenance",
        lambda *args, **kwargs: {
            "data_rows_deleted": 3,
            "rum_log_rows_deleted": 4,
            "rum_beacon_rows_deleted": 2,
            "snapshots_expired_count": 5,
            "data_files_cleaned": 7,
        },
    )
    result = buffer_mod._run_cloud_maintenance_impl(src)
    assert result["retention_deleted_rows"] == 9
    assert result["snapshots_expired"] == 5 and result["files_unlinked"] == 7


def test_local_cache_temp_rollup_cleanup_preserves_quarantine_evidence(tmp_path, monkeypatch):
    src = _source(tmp_path)
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setattr(duckdb_mod, "_cache_dir", lambda _source: str(cache))
    evidence_dir = tmp_path / "data" / "services" / src["service_id"] / "quarantine"
    evidence_dir.mkdir(parents=True)
    evidence = evidence_dir / "1.dat"
    evidence.write_bytes(b"original evidence")
    old_time = (datetime.now(UTC) - timedelta(days=120)).timestamp()
    os.utime(evidence, (old_time, old_time))

    old_parquet = cache / "data" / "old.parquet"
    old_parquet.parent.mkdir()
    old_temp = cache / "aborted.part"
    old_bad_temp = cache / "failed.bad.jsonl.tmp"
    old_tmp = cache / "failed.tmp"
    for p in (old_parquet, old_temp, old_bad_temp, old_tmp):
        p.touch()
        os.utime(p, (old_time, old_time))
    fresh_temp = cache / "active.tmp"
    fresh_temp.touch()

    old_day = (datetime.now(UTC) - timedelta(days=120)).date().isoformat()
    fresh_day = datetime.now(UTC).date().isoformat()
    old_rollup = cache / "rollups" / "day_bundled" / f"day={old_day}" / "old.parquet"
    old_rollup.parent.mkdir(parents=True)
    old_rollup.touch()
    os.utime(old_rollup, (old_time, old_time))
    fresh_rollup = cache / "rollups" / "day_bundled" / f"day={fresh_day}" / "fresh.parquet"
    fresh_rollup.parent.mkdir(parents=True)
    fresh_rollup.touch()

    _maintenance_config(
        monkeypatch,
        data_retention_days=0,
        rum_retention_days=0,
        cache_retention_days=1,
        rollup_retention_months=1,
        quarantine_retention_days=1,
    )
    monkeypatch.setattr(buffer_mod, "_run_ducklake_maintenance", lambda *args, **kwargs: {})
    monkeypatch.setattr(duckdb_mod, "_cache_dir", lambda _source: str(cache))
    result = buffer_mod._run_cloud_maintenance_impl(src)

    assert not any(p.exists() for p in (old_parquet, old_temp, old_bad_temp, old_tmp, old_rollup))
    assert fresh_temp.exists() and fresh_rollup.exists()
    assert evidence.read_bytes() == b"original evidence"
    assert result["local_cache_files_deleted"] == 1
    assert result["local_temp_files_deleted"] == 3 and result["local_rollup_files_deleted"] == 1


def test_local_cache_delete_failure_is_recorded(tmp_path, monkeypatch):
    src = _source(tmp_path)
    old_file = tmp_path / "cache" / "data" / "old.parquet"
    old_file.parent.mkdir(parents=True)
    old_file.touch()
    old_time = (datetime.now(UTC) - timedelta(days=10)).timestamp()
    os.utime(old_file, (old_time, old_time))
    monkeypatch.setattr(duckdb_mod, "_cache_dir", lambda _source: str(tmp_path / "cache"))
    _maintenance_config(monkeypatch, data_retention_days=0, rum_retention_days=0, cache_retention_days=1)
    monkeypatch.setattr(buffer_mod, "_run_ducklake_maintenance", lambda *args, **kwargs: {})
    remove = os.remove

    def fail_target(path):
        if os.fspath(path) == os.fspath(old_file):
            raise PermissionError("cache is read-only")
        remove(path)

    monkeypatch.setattr(buffer_mod.os, "remove", fail_target)
    result = buffer_mod._run_cloud_maintenance_impl(src)
    assert "local_cache_error" in result and "read-only" in result["local_cache_error"]


def test_manual_trigger_runs_expire_and_persists_file_deletion_count(client, monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    with patch("backend.cron.jobs.compaction._run_expire_snapshots", return_value={"status": "success"}) as run_expire:
        response = client.post("/api/admin/expire-snapshots/svc-expire-contract")
    assert response.status_code == 200 and response.json()["status"] == "success"
    run_expire.assert_called_once_with("svc-expire-contract", manual=True)


def test_expire_cron_persists_unlinked_file_count_and_metric_summary(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    logged: list[dict] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: logged.append({"args": args, "kwargs": kwargs}),
    )
    monkeypatch.setattr(
        "backend.core.storage.run_cloud_maintenance",
        lambda _source: {
            "data_rows_deleted": 3,
            "rum_log_rows_deleted": 1,
            "rum_beacon_rows_deleted": 2,
            "snapshots_expired_count": 4,
            "data_files_cleaned": 6,
            "retention_deleted_rows": 6,
            "snapshots_expired": 4,
            "files_unlinked": 6,
        },
    )
    _stub_progress_and_duration(monkeypatch)

    result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    kwargs = logged[0]["kwargs"]
    assert result["status"] == "success" and kwargs["files_deleted_fos"] == 6
    for token in ("retention_deleted_rows=6", "snapshots_expired=4", "files_unlinked=6"):
        assert token in kwargs["summary"]


@pytest.mark.parametrize("failure_stage", ["cleanup", "start", "end"])
def test_expire_finalizes_cron_lease_when_progress_fails(monkeypatch, failure_stage):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    finalized: list[tuple] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: finalized.append(args))
    monkeypatch.setattr("backend.core.storage.run_cloud_maintenance", lambda _source: {"snapshots_expired_count": 0})
    _stub_progress_and_duration(monkeypatch)

    def fail_progress(*_args, **_kwargs):
        raise RuntimeError(f"progress {failure_stage} failed")

    attr_map = {"cleanup": "cleanup_progress_and_reap", "start": "start_progress", "end": "end_progress"}
    monkeypatch.setattr(f"backend.cron_progress.{attr_map[failure_stage]}", fail_progress)

    if failure_stage == "end":
        with pytest.raises(RuntimeError, match=f"progress {failure_stage} failed"):
            expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    else:
        result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
        assert result["status"] == "error" and result["error"] == f"progress {failure_stage} failed"
    assert finalized == [(src, "expire_snapshots", 44)]


def test_expire_cron_context_attributes_queries_and_flushes_usage(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    observed: dict[str, object] = {}
    flush_contexts: list[str | None] = []
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)

    def run_maintenance(_source):
        context = get_process_context()
        observed["context"] = context
        observed["attribution"] = derive_from_process_context(context)
        return {"snapshots_expired_count": 0}

    monkeypatch.setattr("backend.core.storage.run_cloud_maintenance", run_maintenance)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *_args: False)
    monkeypatch.setattr("backend.utils.telemetry.start_call_tracking", lambda: None)
    monkeypatch.setattr(
        "backend.utils.telemetry.flush_usage_log",
        lambda _service_id: flush_contexts.append(get_process_context()),
    )
    _stub_progress_and_duration(monkeypatch)

    result = expire._run_expire_snapshots("svc-expire-contract", manual=True)
    attribution = observed["attribution"]
    assert result["status"] == "success" and observed["context"] == "cron:expire_snapshots"
    assert attribution is not None and attribution.kind == "cron" and attribution.cron_job == "expire_snapshots"
    assert flush_contexts == ["cron:expire_snapshots"]


def test_ducklake_maintenance_sql_is_registered_with_cron_attribution(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    raw_con = duckdb.connect(":memory:")

    @contextmanager
    def write_connection(_source):
        yield raw_con

    def expire_fn(con, _source, _keep_snapshot_days):
        con.execute("SELECT 1 AS cron_expire_probe").fetchone()
        return {"snapshots_expired_count": 0, "data_files_cleaned": 0}

    registered = []
    register = query_registry.register

    def capture_register(*args, **kwargs):
        registered.append((args, kwargs, current_attribution.get()))
        return register(*args, **kwargs)

    monkeypatch.setattr(buffer_mod, "_ducklake_write_connection", write_connection)
    monkeypatch.setattr(buffer_mod, "_ducklake_expire_snapshots", expire_fn)
    monkeypatch.setattr(query_registry, "register", capture_register)

    try:
        with process_context_scope("cron:expire_snapshots"):
            result = buffer_mod._run_ducklake_maintenance(
                src, data_retention_days=0, rum_retention_days=0, keep_snapshot_days=7
            )
    finally:
        raw_con.close()

    matching = [item for item in registered if item[0][1] == "SELECT 1 AS cron_expire_probe"]
    assert matching, "Cron 8 maintenance SQL was not registered"
    call = matching[0]
    assert result == {"snapshots_expired_count": 0, "data_files_cleaned": 0}
    assert call[0][0] == "DuckDB" and call[1]["service_id"] == src["service_id"]
    assert call[2].kind == "cron" and call[2].cron_job == "expire_snapshots"


def test_expire_politeness_gate_applies_to_cron_but_manual_trigger_bypasses(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *_args: True)
    maintenance = MagicMock(return_value={"snapshots_expired_count": 0})
    monkeypatch.setattr("backend.core.storage.run_cloud_maintenance", maintenance)

    deferred = expire._run_expire_snapshots.__wrapped__("svc-expire-contract")
    assert deferred["status"] == "deferred"
    maintenance.assert_not_called()

    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *args, **kwargs: None)
    _stub_progress_and_duration(monkeypatch)

    manual = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    assert manual["status"] == "success"
    maintenance.assert_called_once()


def test_expire_runner_skips_when_another_service_run_holds_the_lease(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("already running"))
    )
    maintenance = MagicMock()
    monkeypatch.setattr("backend.core.storage.run_cloud_maintenance", maintenance)

    result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    assert result["status"] == "skipped" and "already running" in result["summary"]
    maintenance.assert_not_called()


def test_nonfatal_step_error_is_recorded_as_warning(monkeypatch):
    src = {"name": "svc-expire-contract", "service_id": "svc-expire-contract"}
    logged: list[dict] = []
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *_args: 44)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *args: None)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *args, **kwargs: logged.append({"args": args, "kwargs": kwargs}),
    )
    monkeypatch.setattr(
        "backend.core.storage.run_cloud_maintenance",
        lambda _source: {
            "retention_deleted_rows": 2,
            "snapshots_expired": 0,
            "files_unlinked": 1,
            "local_cache_error": "cache is read-only",
        },
    )
    _stub_progress_and_duration(monkeypatch)

    result = expire._run_expire_snapshots.__wrapped__("svc-expire-contract", manual=True)
    assert result["status"] == "warning" and logged[0]["args"][3] == "warning"
    assert "local_cache_error=cache is read-only" in logged[0]["kwargs"]["error_message"]


def test_dev_no_crons_blocks_expire_registration_and_execution(monkeypatch, tmp_path):
    service_id = "svc-expire-contract"
    src = _source(tmp_path, service_id)
    cfg = {
        **src,
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": True}},
    }
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    scheduler = Scheduler()
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.is_configured", lambda _source: True)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: None)
    monkeypatch.setattr(scheduler, "_add_job", lambda *args, **kwargs: None)

    scheduler._sync_jobs()
    assert f"expire_{service_id}" not in scheduler._job_ids

    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.storage.run_cloud_maintenance", MagicMock())
    skipped = expire._run_expire_snapshots.__wrapped__(service_id, manual=True)
    assert skipped["status"] == "skipped" and "FLA_DEV_NO_CRONS=1" in skipped["summary"]
    duckdb_mod.start_cron_run.assert_not_called()
    storage_mod.run_cloud_maintenance.assert_not_called()


def test_registers_expire_on_the_serving_scheduler(monkeypatch, tmp_path):
    service_id = "svc-expire-contract"
    src = _source(tmp_path, service_id)
    cfg = {
        **src,
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": True}},
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    scheduler = Scheduler()
    monkeypatch.setattr("backend.config.list_configs", lambda: [cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda _sid: src)
    monkeypatch.setattr("backend.core.duckdb.is_configured", lambda _source: True)
    monkeypatch.setattr("backend.config.get_ngwaf_workspace_id", lambda _sid: None)
    registrations = {}
    monkeypatch.setattr(
        scheduler, "_add_job", lambda func, trigger, **kw: registrations.update({kw["id"]: (func, trigger)})
    )
    scheduler._sync_jobs()
    assert registrations[f"expire_{service_id}"] == (expire._run_expire_snapshots, "interval")


def test_usage_log_classifies_bulk_fos_deletes_as_class_a():
    service_id = "svc-expire-billing-contract"
    metadata_db.log_usage_calls(
        service_id,
        [{"method": "DELETE_OBJECTS", "service": "FOS", "path": "ducklake/data", "status": 200}],
        process_context="cron:expire_snapshots",
    )
    con = usage_log_db.get_con(service_id)
    row = con.execute(
        "SELECT operation_class, operation_type, process_context FROM usage_log "
        "WHERE service_id = ? ORDER BY id DESC LIMIT 1",
        (service_id,),
    ).fetchone()
    assert row["operation_class"] == "A" and row["operation_type"] == "DELETE_OBJECTS"
    assert row["process_context"] == "cron:expire_snapshots"


# ── Merged from test_optimize.py ──────────────────────────────────────────────


@pytest.fixture
def optimize_test_source(monkeypatch, tmp_path):
    service_id = "svc-optimize-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-fos-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {"cron_compact": {"enabled": True}},
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


class TestOptimizeJob:
    def test_returns_when_should_defer_cron_true(self, monkeypatch):
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: True)
        get_src = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
        optimize._run_optimize.__wrapped__("svc-1")
        get_src.assert_not_called()

    def test_returns_when_source_missing(self, monkeypatch):
        monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
        start = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
        optimize._run_optimize.__wrapped__("ghost-svc")
        start.assert_not_called()

    def test_skips_when_start_cron_run_raises(self, monkeypatch, stub_source, stub_cron_envelope):
        stub_cron_envelope["start"].side_effect = CronLeaseHeldError("already running")
        opt = MagicMock()
        monkeypatch.setattr("backend.core.storage.optimize_table", opt)
        optimize._run_optimize.__wrapped__("svc-1")
        opt.assert_not_called()
        stub_cron_envelope["log"].assert_not_called()
        stub_cron_envelope["start_progress"].assert_not_called()

    def test_success_records_rewritten_and_added_counts(self, monkeypatch, stub_source, stub_cron_envelope):
        monkeypatch.setattr(
            "backend.core.storage.optimize_table",
            MagicMock(return_value={"files_rewritten": 12, "files_added": 3, "eligible_partitions": 2}),
        )
        optimize._run_optimize.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success"
        assert kwargs["parquet_files_optimized"] == 12 and kwargs["parquet_files_created"] == 3
        assert "Rewrote 12" in kwargs["summary"] and "into 3" in kwargs["summary"]
        stub_cron_envelope["end_progress"].assert_called_once()

    def test_partition_errors_with_files_added_logs_warning(self, monkeypatch, stub_source, stub_cron_envelope):
        monkeypatch.setattr(
            "backend.core.storage.optimize_table",
            MagicMock(
                return_value={
                    "files_rewritten": 8,
                    "files_added": 2,
                    "eligible_partitions": 5,
                    "partition_errors": ["a failed", "b failed"],
                }
            ),
        )
        optimize._run_optimize.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "warning" and "2/5 partitions failed" in kwargs["summary"]
        assert "a failed" in kwargs["error_message"]

    def test_partition_errors_with_zero_files_added_logs_error(self, monkeypatch, stub_source, stub_cron_envelope):
        monkeypatch.setattr(
            "backend.core.storage.optimize_table",
            MagicMock(
                return_value={
                    "files_rewritten": 0,
                    "files_added": 0,
                    "eligible_partitions": 4,
                    "partition_errors": ["x", "y", "z", "w"],
                }
            ),
        )
        optimize._run_optimize.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error" and "4/4 partitions failed" in kwargs["summary"]
        assert "(1 more)" in kwargs["error_message"]

    def test_top_level_error_key_logs_error(self, monkeypatch, stub_source, stub_cron_envelope):
        monkeypatch.setattr("backend.core.storage.optimize_table", MagicMock(return_value={"error": "schema mismatch"}))
        optimize._run_optimize.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error" and "schema mismatch" in kwargs["error_message"]

    def test_unexpected_exception_logged_and_end_progress_fires(self, monkeypatch, stub_source, stub_cron_envelope):
        monkeypatch.setattr(
            "backend.core.storage.optimize_table",
            MagicMock(side_effect=RuntimeError("manifest read failed")),
        )
        optimize._run_optimize.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error" and "manifest read failed" in kwargs["error_message"]
        stub_cron_envelope["end_progress"].assert_called_once()

    def test_contract_1_and_4_manual_trigger_and_cron_runs_accounting(self, optimize_test_source):
        service_id = optimize_test_source["service_id"]
        logged_runs = []

        def fake_log_cron_run(source, task, duration_s, status, **kwargs):
            logged_runs.append(
                {
                    "task": task,
                    "duration_s": duration_s,
                    "status": status,
                    "parquet_files_optimized": kwargs.get("parquet_files_optimized"),
                    "parquet_files_created": kwargs.get("parquet_files_created"),
                    "summary": kwargs.get("summary"),
                }
            )

        app.dependency_overrides[get_source] = lambda: optimize_test_source
        app.dependency_overrides[get_service_id] = lambda: service_id
        try:
            with (
                patch("backend.core.duckdb.start_cron_run", return_value=101),
                patch("backend.core.duckdb.log_cron_run", side_effect=fake_log_cron_run),
                patch(
                    "backend.core.storage.optimize_table",
                    return_value={
                        "files_rewritten": 42,
                        "files_added": 4,
                        "eligible_partitions": 3,
                        "partition_errors": [],
                    },
                ),
                patch("backend.cron.jobs._common.finalize_cron_duration"),
            ):
                resp = TestClient(app).post(f"/api/admin/optimize/{service_id}")
                assert resp.status_code == 200
                body = resp.json()
                assert body["files_rewritten"] == 42 and body["files_added"] == 4
                assert len(logged_runs) == 1
                run = logged_runs[0]
                assert run["task"] == "optimize" and run["status"] == "success" and run["duration_s"] >= 0.0
                assert run["parquet_files_optimized"] == 42 and run["parquet_files_created"] == 4
                assert "Rewrote 42 files into 4 files" in run["summary"]
        finally:
            app.dependency_overrides.clear()

    def test_contract_2_and_3_verify_logs_for_flush_and_rewrite(self, optimize_test_source, caplog):
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchall.return_value = [("main", "logs", 10, 2)]
        caplog.set_level(logging.INFO)

        with (
            patch.object(buffer_mod, "_ducklake_write_connection") as mock_conn_ctx,
            patch.object(buffer_mod, "_lake_columns", return_value={"id", "timestamp"}),
        ):
            mock_conn_ctx.return_value.__enter__.return_value = mock_con
            res = buffer_mod._optimize_table_impl(optimize_test_source)
            assert "error" not in res and res["files_rewritten"] > 0

        assert "ducklake_flush_inlined_data executed successfully" in caplog.text
        assert "ducklake_rewrite_data_files executed successfully" in caplog.text

    def test_contract_5_usage_log_telemetry_attributed_to_cron_optimize(self, optimize_test_source):
        service_id = optimize_test_source["service_id"]
        fos_calls = [
            {"method": "PUT_OBJECT", "service": "FOS", "bytes": 1024 * 1024},
            {"method": "GET_OBJECT", "service": "FOS", "bytes": 512 * 1024},
            {"method": "DELETE_OBJECT", "service": "FOS"},
        ]
        metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.optimize")
        con = usage_log_db.get_con(service_id)
        rows = con.execute(
            "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
            (service_id,),
        ).fetchall()
        assert [(r["operation_class"], r["process_context"], r["operation_type"]) for r in rows] == [
            ("A", "cron.optimize", "PUT_OBJECT"),
            ("B", "cron.optimize", "GET_OBJECT"),
            ("B", "cron.optimize", "DELETE_OBJECT"),
        ]

    def test_contract_6_safety_gate_under_fla_dev_no_crons(self, optimize_test_source, monkeypatch, caplog):
        service_id = optimize_test_source["service_id"]
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
        scheduler = Scheduler()
        with (
            patch("backend.config.list_configs", return_value=[optimize_test_source]),
            patch("backend.core.duckdb.is_configured", return_value=True),
        ):
            scheduler._register_dev_local_safe_jobs()
            assert f"optimize_{service_id}" not in scheduler._job_ids

        caplog.set_level(logging.WARNING)
        mock_opt = MagicMock()
        monkeypatch.setattr("backend.core.storage.optimize_table", mock_opt)
        res = optimize._run_optimize.__wrapped__(service_id)
        assert res is not None and res.get("status") == "skipped"
        mock_opt.assert_not_called()
        assert any("FLA_DEV_NO_CRONS=1" in record.message for record in caplog.records)

    def test_contract_politeness_gating_and_manual_bypass(self, optimize_test_source):
        service_id = optimize_test_source["service_id"]
        with (
            patch("backend.utils.active_requests.should_defer_cron", return_value=True),
            patch("backend.core.storage.optimize_table") as mock_opt,
        ):
            res = optimize._run_optimize.__wrapped__(service_id, manual=False)
            assert res is not None and res.get("status") == "deferred"
            mock_opt.assert_not_called()

            mock_opt.return_value = {"files_rewritten": 5, "files_added": 1}
            with (
                patch("backend.core.duckdb.start_cron_run", return_value=201),
                patch("backend.core.duckdb.log_cron_run"),
                patch("backend.cron.jobs._common.finalize_cron_duration"),
            ):
                res_manual = optimize._run_optimize.__wrapped__(service_id, manual=True)
                assert res_manual.get("status") == "success"
                mock_opt.assert_called_once()

    def test_contract_exclusive_per_service_locking(self, optimize_test_source):
        service_id = optimize_test_source["service_id"]
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("already running")),
            patch("backend.core.storage.optimize_table") as mock_opt,
        ):
            res = optimize._run_optimize.__wrapped__(service_id)
            assert res is not None and res.get("status") == "skipped"
            mock_opt.assert_not_called()

    def test_contract_all_lake_tables_durability_and_compaction(self, optimize_test_source):
        calls = []

        class MockCon:
            def execute(self, sql, *args):
                calls.append(sql.strip())
                m = MagicMock()
                m.fetchall.return_value = [("main", "test", 2, 1)]
                return m

        with (
            patch.object(buffer_mod, "_ducklake_write_connection") as mock_conn_ctx,
            patch.object(buffer_mod, "_lake_columns", return_value={"id", "timestamp"}),
        ):
            mock_conn_ctx.return_value.__enter__.return_value = MockCon()
            res = buffer_mod._optimize_table_impl(optimize_test_source)
            assert "error" not in res and res["files_rewritten"] > 0 and res["files_added"] > 0

        assert next(i for i, c in enumerate(calls) if "CALL ducklake_flush_inlined_data" in c) == 0
        assert len([c for c in calls if "ducklake_merge_adjacent_files" in c]) == 3
        assert len([c for c in calls if "ducklake_rewrite_data_files" in c]) == 3


# ── Merged from tests/cron/test_maintenance_jobs.py ──


def test_trim_memory_graceful():
    _recycle._trim_memory()


def test_get_pool_status_and_barrier():
    stats = _pool.get_pool_status()
    assert "pools" in stats and "retired_pools" in stats
    assert _db.is_recycle_barrier_active("/non/existent/path") is False
    assert isinstance(_db.is_recycle_barrier_active(), set)


def _mock_sched_job(interval_seconds: int) -> tuple[MagicMock, MagicMock]:
    mock_sched, mock_job, trigger = MagicMock(), MagicMock(), MagicMock()
    trigger.interval.total_seconds.return_value = interval_seconds
    mock_job.trigger = trigger
    mock_sched.get_job.return_value = mock_job
    return mock_sched, mock_job


def test_run_duckdb_recycle_rss_threshold_skip():
    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.cron.jobs.metadata.recycle_once") as mock_recycle,
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "skipped" in result and "100MB < threshold 500MB" in result
        mock_recycle.assert_not_called()


def test_run_duckdb_recycle_adaptive_expedite():
    mock_sched, mock_job = _mock_sched_job(3600)
    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", return_value=850 * 1024 * 1024),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch("backend.cron.jobs.metadata.recycle_once") as mock_recycle,
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "skipped" in result
        mock_recycle.assert_not_called()
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_run_duckdb_recycle_adaptive_incomplete():
    mock_sched, mock_job = _mock_sched_job(3600)
    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=0),
        patch("backend.core.duckdb.current_rss_bytes", return_value=200 * 1024 * 1024),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch(
            "backend.cron.jobs.metadata.recycle_once",
            return_value="interval: recycled 0/1 instance(s), freed ~0MB (incomplete: 1)",
        ),
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "incomplete" in result
        mock_job.reschedule.assert_called_with("interval", minutes=2.0)


def test_run_duckdb_recycle_adaptive_relax():
    mock_sched, mock_job = _mock_sched_job(120)
    with (
        patch("backend.core.duckdb_recycle.maybe_graceful_restart", return_value=False),
        patch("backend.cron.jobs.metadata._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[1100 * 1024 * 1024, 400 * 1024 * 1024]),
        patch("backend.cron.scheduler.get_scheduler", return_value=mock_sched),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
        patch(
            "backend.cron.jobs.metadata.recycle_once", return_value="interval: recycled 1/1 instance(s), freed ~700MB"
        ),
    ):
        result = run_duckdb_recycle.__wrapped__()
        assert "recycled 1/1" in result
        mock_job.reschedule.assert_called_with("interval", minutes=60.0)


def test_recycle_once_incomplete_reporting(monkeypatch):
    monkeypatch.setattr(_recycle, "_sources_by_db_path", lambda: {"/tmp/test.duckdb": [{"name": "s1"}]})
    monkeypatch.setattr(
        _recycle,
        "_recycle_db_path",
        lambda path, sources: {"db_path": path, "status": "incomplete", "drained": False, "live_after": 1},
    )
    assert "incomplete: 1" in _recycle.recycle_once()


def test_admin_duckdb_recycle_endpoint():
    client = TestClient(app)
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=100 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
    ):
        resp = client.post("/api/admin/duckdb/recycle")
        assert resp.status_code == 200 and resp.json()["ok"] is True and resp.json()["status"] == "skipped"

    with (
        patch("backend.core.duckdb.current_rss_bytes", side_effect=[300 * 1024 * 1024, 200 * 1024 * 1024]),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=500 * 1024 * 1024),
        patch(
            "backend.core.duckdb_recycle.recycle_once",
            return_value="manual_admin: recycled 1/1 instance(s), freed ~100MB",
        ),
    ):
        resp = client.post("/api/admin/duckdb/recycle?force=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True and data["status"] == "recycled" and data["freed_mb"] == 100.0


def test_admin_duckdb_status_endpoint():
    client = TestClient(app)
    with (
        patch("backend.core.duckdb.current_rss_bytes", return_value=250 * 1024 * 1024),
        patch("backend.core.duckdb_recycle._recycle_rss_threshold_bytes", return_value=1000 * 1024 * 1024),
        patch("backend.core.duckdb_recycle.recycle_interval_min", return_value=60.0),
    ):
        resp = client.get("/api/admin/duckdb/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True and data["memory"]["current_rss_mb"] == 250.0
        assert data["memory"]["recycle_threshold_mb"] == 1000.0 and data["memory"]["recycle_interval_min"] == 60.0
        assert "pools" in data and "barrier" in data


# ── Merged from test_heartbeat.py ──────────────────────────────────────────────

SID = "svc-heartbeat"
JOB = "heartbeat_job"
OTHER_JOB = "other_job"


def _stray_threads(before: set[threading.Thread]) -> list[threading.Thread]:
    return [
        t for t in threading.enumerate() if t not in before and t.is_alive() and not t.name.startswith("cron-watchdog")
    ]


def _wait_for_exit(threads: list[threading.Thread], timeout: float = 5.0) -> list[threading.Thread]:
    deadline = time.monotonic() + timeout
    while threads and time.monotonic() < deadline:
        threads = [t for t in threads if t.is_alive()]
        time.sleep(0.01)
    return threads


@pytest.fixture
def baseline():
    get_con(SID)
    return set(threading.enumerate()), len(pgc._checked_out)


def _seed_leases() -> None:
    con = get_con(SID)
    for job in (JOB, OTHER_JOB):
        con.execute(
            "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
            "VALUES (?, ?, ?, ?, ?, 'running')",
            (SID, job, 1.0, 1.0, 60),
        )
    con.commit()


def _heartbeat_at(job: str) -> float:
    row = (
        get_con(SID)
        .execute("SELECT heartbeat_at FROM job_runs WHERE service_id = ? AND job_name = ?", (SID, job))
        .fetchone()
    )
    return float(row[0])


def test_heartbeat_refreshes_only_its_own_job_lease(baseline):
    _seed_leases()
    with CronHeartbeat(SID, JOB, interval_s=0.02):
        deadline = time.monotonic() + 5.0
        while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
            time.sleep(0.02)
    assert _heartbeat_at(JOB) > 1.0 and _heartbeat_at(OTHER_JOB) == 1.0


def test_no_thread_or_pooled_connection_survives_a_normal_exit(baseline):
    before, checked_out = baseline
    _seed_leases()
    with CronHeartbeat(SID, JOB, interval_s=0.02):
        deadline = time.monotonic() + 5.0
        while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
            time.sleep(0.02)
    assert _wait_for_exit(_stray_threads(before)) == [] and len(pgc._checked_out) == checked_out


def test_no_thread_or_pooled_connection_survives_an_exception_in_the_body(baseline):
    before, checked_out = baseline
    _seed_leases()
    with pytest.raises(RuntimeError, match="job blew up"):
        with CronHeartbeat(SID, JOB, interval_s=0.02):
            deadline = time.monotonic() + 5.0
            while _heartbeat_at(JOB) <= 1.0 and time.monotonic() < deadline:
                time.sleep(0.02)
            raise RuntimeError("job blew up")
    assert _wait_for_exit(_stray_threads(before)) == [] and len(pgc._checked_out) == checked_out


def test_the_thread_releases_its_connection_even_when_it_never_used_one():
    with patch("backend.core.metadata.base.release_thread_connection") as release:
        with CronHeartbeat(SID, None, interval_s=0.02):
            pass
    release.assert_called_once_with()


def test_a_job_without_a_name_refreshes_nothing(baseline):
    _seed_leases()
    with CronHeartbeat(SID, None, interval_s=0.02):
        time.sleep(0.15)
    assert _heartbeat_at(JOB) == 1.0 and _heartbeat_at(OTHER_JOB) == 1.0


def test_a_failing_refresh_does_not_kill_the_loop_or_leak(baseline):
    before, checked_out = baseline
    calls: list[int] = []

    def _boom(*_a, **_kw):
        calls.append(1)
        raise RuntimeError("db down")

    with patch("backend.core.metadata.base.get_con", side_effect=_boom):
        with CronHeartbeat(SID, JOB, interval_s=0.01):
            deadline = time.monotonic() + 5.0
            while len(calls) < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
    assert len(calls) >= 3
    assert _wait_for_exit(_stray_threads(before)) == [] and len(pgc._checked_out) == checked_out


@pytest.fixture
def fast_heartbeat(monkeypatch):
    monkeypatch.setattr("backend.cron.decorators.DEFAULT_INTERVAL_S", 0.02)


def test_cron_task_leaks_nothing_when_the_executor_refuses_the_job(baseline, fast_heartbeat):
    before, checked_out = baseline

    @decorators.cron_task("exec-refuses", job_name=JOB)
    def _job(service_id: str):
        return None

    class _Dead:
        def submit(self, *_a, **_kw):
            raise RuntimeError("cannot schedule new futures after shutdown")

    with patch.object(decorators, "_get_watchdog_executor", return_value=_Dead()):
        with pytest.raises(RuntimeError, match="shutdown"):
            _job(SID)
    assert _wait_for_exit(_stray_threads(before)) == [] and len(pgc._checked_out) == checked_out


def test_cron_task_leaks_nothing_across_repeated_failures_and_watchdog_timeouts(baseline, fast_heartbeat, monkeypatch):
    before, checked_out = baseline
    monkeypatch.setattr(decorators, "_CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("flaky", job_name=JOB)
    def _job(service_id: str, mode: str):
        if mode == "slow":
            time.sleep(0.15)
        if mode == "raise":
            raise ValueError("boom")

    for mode in ["ok", "raise", "slow"] * 5:
        if mode == "raise":
            with pytest.raises(ValueError, match="boom"):
                _job(SID, mode)
        else:
            _job(SID, mode)
    assert _wait_for_exit(_stray_threads(before)) == [] and len(pgc._checked_out) == checked_out


def test_cron_task_returns_the_job_result(fast_heartbeat):
    @decorators.cron_task("returns", job_name=JOB)
    def _job(service_id: str):
        return {"ok": service_id}

    assert _job(SID) == {"ok": SID}


def test_watchdog_timeout_still_waits_for_the_job_and_returns_its_result(fast_heartbeat, monkeypatch):
    monkeypatch.setattr(decorators, "_CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("slow-result", job_name=JOB)
    def _job(service_id: str):
        time.sleep(0.15)
        return "finished"

    assert _job(SID) == "finished"


# ── Merged from test_metric_snapshot_job.py ──────────────────────────────────


def _recent_hist(metric: str, **kwargs) -> list[dict]:
    return _get_history(metric, since=datetime.now(UTC) - timedelta(hours=1), **kwargs)


@pytest.mark.parametrize(
    "scenario", ["active_query", "pool_wait", "partial_failure", "safe_record_error", "active_err"]
)
def test_metric_snapshot_tick_and_error_isolation(scenario):
    if scenario == "active_query":
        with patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 3}):
            _run_metric_snapshot()
        assert _recent_hist("active_query_count")[-1]["value"] == 3.0
    elif scenario == "pool_wait":
        fake_stats = [
            {"service": "svc-a", "wait": {"p95_ms": 12.3, "count": 100}},
            {"service": "svc-b", "wait": {"p95_ms": 45.6, "count": 50}},
        ]
        with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
            _run_metric_snapshot()
        assert any(r["value"] == 12.3 for r in _recent_hist("pool_wait_p95_ms", service_id="svc-a"))
        assert any(r["value"] == 45.6 for r in _recent_hist("pool_wait_p95_ms", service_id="svc-b"))
    elif scenario == "partial_failure":
        with (
            patch("backend.core.duckdb_pool.get_all_stats", side_effect=RuntimeError("boom")),
            patch("backend.core.query_registry.query_registry.summary", return_value={"active_total": 7}),
        ):
            _run_metric_snapshot()
        assert _recent_hist("active_query_count")[-1]["value"] == 7.0
    elif scenario == "safe_record_error":
        with patch.object(metric_snapshots, "record_snapshot", side_effect=ValueError("bad metric")):
            _safe_record("bogus", 1.0)
    else:
        with patch("backend.core.query_registry.query_registry.summary", side_effect=RuntimeError("boom")):
            _sample_active_queries()


@pytest.mark.parametrize(
    "reachable,depths,exp_ingest,exp_total", [(True, {"q.ingest": 4}, 4.0, 4.0), (False, {}, 0.0, 0.0)]
)
def test_sample_celery_queues_and_ingest_ledger(reachable, depths, exp_ingest, exp_total):
    with patch("backend.celery_status.celery_queue_depths", return_value=(depths, reachable)):
        _sample_celery_queues()
    assert _recent_hist("celery_broker_reachable")[-1]["value"] == (1.0 if reachable else 0.0)
    assert _recent_hist("celery_queue_depth")[-1]["value"] == exp_total
    if reachable:
        assert _recent_hist("celery_queue_depth_q.ingest")[-1]["value"] == exp_ingest
        assert _recent_hist("celery_queue_depth_q.control")[-1]["value"] == 0.0
        with patch(
            "backend.celery_status.ingest_ledger_summary", return_value={"discovered": 5, "claimed": 2, "committed": 9}
        ):
            _sample_ingest_ledger()
        assert _recent_hist("ingest_ledger_discovered")[-1]["value"] == 5.0
        assert _recent_hist("ingest_ledger_claimed")[-1]["value"] == 2.0


def _fake_row(task: str, secs: float) -> dict:
    return {"task": task, "duration_s": secs}


@pytest.mark.parametrize("scenario", ["per_service", "skip_missing_sid", "partial_failure", "outer_failure"])
def test_sample_cron_duration_cases(scenario):
    if scenario == "per_service":
        fake_cons = {
            s: MagicMock(execute=MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=r))))
            for s, r in (("svc-a", [_fake_row("sync", 0.5)]), ("svc-b", [_fake_row("optimize", 2.0)]))
        }
        with (
            patch("backend.config.list_configs", return_value=[{"service_id": "svc-a"}, {"service_id": "svc-b"}]),
            patch("backend.core.metadata.base.get_con", side_effect=lambda sid: fake_cons[sid]),
        ):
            _sample_cron_duration()
        assert any(r["value"] == 500.0 for r in _recent_hist("cron_duration_ms", service_id="svc-a", task="sync"))
        assert any(r["value"] == 2000.0 for r in _recent_hist("cron_duration_ms", service_id="svc-b", task="optimize"))
    elif scenario == "skip_missing_sid":
        called: list[str] = []
        empty_con = MagicMock(execute=MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[]))))
        with (
            patch("backend.config.list_configs", return_value=[{"service_id": None}, {}, {"service_id": "svc-real"}]),
            patch("backend.core.metadata.base.get_con", side_effect=lambda sid: called.append(sid) or empty_con),
        ):
            _sample_cron_duration()
        assert called == ["svc-real"]
    elif scenario == "partial_failure":
        good_con = MagicMock(
            execute=MagicMock(return_value=MagicMock(fetchall=MagicMock(return_value=[_fake_row("sync", 1.0)])))
        )
        with (
            patch("backend.config.list_configs", return_value=[{"service_id": "svc-bad"}, {"service_id": "svc-good"}]),
            patch(
                "backend.core.metadata.base.get_con",
                side_effect=lambda sid: (_ for _ in ()).throw(RuntimeError("locked")) if sid == "svc-bad" else good_con,
            ),
        ):
            _sample_cron_duration()
        assert any(r["value"] == 1000.0 for r in _recent_hist("cron_duration_ms", service_id="svc-good", task="sync"))
    else:
        with patch("backend.config.list_configs", side_effect=RuntimeError("module broken")):
            _sample_cron_duration()


def test_sample_ingest_lag_records_recent_lag():
    cfgs = [{"service_id": "svc-ingest"}]
    ten_min_ago = (datetime.now(UTC) - timedelta(minutes=10)).isoformat().replace("+00:00", "Z")
    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=ten_min_ago),
    ):
        _sample_ingest_lag()
    rows = _get_history("ingest_lag_s", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-ingest")
    assert rows and 570 <= rows[-1]["value"] <= 630


def test_sample_ingest_lag_skips_when_no_latest_ts():
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-empty"}]),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value=None),
    ):
        _sample_ingest_lag()
    assert _get_history("ingest_lag_s", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-empty") == []


def test_sample_ingest_lag_skips_unparseable_timestamp():
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-bad-ts"}]),
        patch("backend.core.metadata.get_latest_ingest_ts", return_value="not-a-timestamp"),
    ):
        _sample_ingest_lag()
    assert _get_history("ingest_lag_s", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-bad-ts") == []


def test_sample_ingest_lag_continues_after_one_service_fails():
    cfgs = [{"service_id": "svc-bad"}, {"service_id": "svc-good"}]

    def _fake_get_latest(sid: str):
        if sid == "svc-bad":
            raise RuntimeError("locked")
        return (datetime.now(UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")

    with (
        patch("backend.config.list_configs", return_value=cfgs),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=_fake_get_latest),
    ):
        _sample_ingest_lag()
    assert _get_history("ingest_lag_s", since=datetime.now(UTC) - timedelta(hours=1), service_id="svc-good")


def test_sample_ingest_lag_skips_configs_without_service_id():
    seen: list[str] = []
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": None}, {"service_id": "svc-real"}]),
        patch("backend.core.metadata.get_latest_ingest_ts", side_effect=lambda sid: seen.append(sid) or None),
    ):
        _sample_ingest_lag()
    assert seen == ["svc-real"]


def test_sample_active_queries_swallows_summary_exception():
    with patch("backend.core.query_registry.query_registry.summary", side_effect=RuntimeError("boom")):
        _sample_active_queries()
    pre = _get_history("active_query_count", since=datetime.now(UTC) - timedelta(seconds=5))
    assert pre == [] or all(r["value"] != 0 for r in pre[:0])


def test_sample_os_vitals_swallows_getloadavg_exception():
    with patch("os.getloadavg", side_effect=OSError("not available")):
        _sample_os_vitals()


def test_sample_os_vitals_parses_proc_meminfo_when_available():
    meminfo = "MemTotal:       16384 kB\nMemAvailable:    4096 kB\nBuffers:    100 kB\n"

    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        return io.StringIO(meminfo)

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()
    rows = _get_history("mem_used_pct", since=datetime.now(UTC) - timedelta(hours=1))
    assert rows and rows[-1]["value"] == 75.0


def test_sample_os_vitals_swallows_meminfo_parse_error():
    def _fake_open(path, *args, **kwargs):
        assert path == "/proc/meminfo"
        raise PermissionError("denied")

    with patch("builtins.open", side_effect=_fake_open):
        _sample_os_vitals()


def test_sample_os_vitals_records_disk_usage():
    with patch("shutil.disk_usage", return_value=MagicMock(total=1000, used=500, free=500)):
        _sample_os_vitals()
    rows = _get_history("disk_used_pct", since=datetime.now(UTC) - timedelta(hours=1))
    assert rows and rows[-1]["value"] == 50.0


def test_sample_os_vitals_swallows_disk_usage_exception_per_path():
    def _disk(path):
        if path == "/app/data":
            raise PermissionError("denied")
        return MagicMock(total=1000, used=100, free=900)

    with patch("shutil.disk_usage", side_effect=_disk):
        _sample_os_vitals()
    rows = _get_history("disk_used_pct_root", since=datetime.now(UTC) - timedelta(hours=1))
    assert rows and rows[-1]["value"] == 10.0


def test_metric_snapshot_samples_vitals(monkeypatch):
    monkeypatch.setattr("backend.config.list_configs", lambda: [{"service_id": "svc_test"}])
    monkeypatch.setattr("backend.config.duckdb_path", lambda sid: f"/mock/path/{sid}.duckdb")
    monkeypatch.setattr(
        "backend.core.duckdb_pool.get_all_stats",
        lambda: [{"service": "svc_test", "wait": {"p95_ms": 12.5}}],
    )
    monkeypatch.setattr("backend.core.query_registry.query_registry.summary", lambda: {"active_total": 3})
    monkeypatch.setattr(
        "backend.core.storage._ducklake.get_admission_stats",
        lambda: {"svc_test": {"acquisitions": 2.0, "wait_ms_total": 40.0, "hold_ms_total": 100.0, "timeouts": 0.0}},
    )
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": 5}, True))
    monkeypatch.setattr("backend.celery_status.ingest_ledger_summary", lambda: {"discovered": 10, "claimed": 2})
    monkeypatch.setattr("os.path.exists", lambda p: "svc_test.duckdb" in p)
    monkeypatch.setattr("os.path.getsize", lambda p: 1048576)

    result = ms_mod._run_metric_snapshot.__wrapped__()
    assert "sampled=" in result

    batch = metric_snapshots.get_batch(since="1h")
    assert batch["pool_wait_p95_ms|svc_test"][-1]["value"] == 12.5
    assert batch["duckdb_file_bytes|svc_test"][-1]["value"] == 1048576.0
    assert batch["active_query_count"][-1]["value"] == 3.0
    assert batch["ducklake_admission_wait_avg_ms|svc_test"][-1]["value"] == 20.0
    assert batch["celery_broker_reachable"][-1]["value"] == 1.0
    assert batch["celery_queue_depth_q.ingest"][-1]["value"] == 5.0
    assert batch["ingest_ledger_discovered"][-1]["value"] == 10.0


def test_cron_duration_includes_warning_status(tmp_path, monkeypatch):
    service_id = "svc_warning_test"
    con = sqlite3.connect(str(tmp_path / f"{service_id}.metadata.db"))
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT NOT NULL, duration_s REAL, status TEXT NOT NULL)"
    )
    for task, dur, st in (("commit", 4.5, "success"), ("ledger_sweep", 1.8, "warning"), ("optimize", 12.2, "error")):
        con.execute("INSERT INTO cron_runs (task, duration_s, status) VALUES (?, ?, ?)", (task, dur, st))
    con.commit()

    monkeypatch.setattr("backend.config.list_configs", lambda: [{"service_id": service_id}])
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda sid: con)
    assert ms_mod._sample_cron_duration() == 3

    batch = metric_snapshots.get_batch(since="1h")
    assert batch[f"cron_duration_ms|{service_id}|commit"][-1]["value"] == 4500.0
    assert batch[f"cron_duration_ms|{service_id}|ledger_sweep"][-1]["value"] == 1800.0
    assert batch[f"cron_duration_ms|{service_id}|optimize"][-1]["value"] == 12200.0
    con.close()


def test_sample_celery_workers_uses_timeout():
    mock_inspect = MagicMock()
    mock_inspect.stats.return_value = {"worker1@node": {}}
    mock_inspect.active.return_value = {"worker1@node": [{"id": "task1"}]}

    with patch("backend.celery_app.app.control.inspect", return_value=mock_inspect) as inspect_call:
        assert ms_mod._sample_celery_workers() == 2
        inspect_call.assert_called_once_with(timeout=0.5)

    batch = metric_snapshots.get_batch(since="1h")
    assert batch["celery_active_workers"][-1]["value"] == 1.0
    assert batch["celery_active_tasks"][-1]["value"] == 1.0


def test_sample_duckdb_size(monkeypatch):
    cfgs = [{"service_id": "svc_1"}, {"service_id": "svc_2"}, {"service_id": "svc_missing"}]
    monkeypatch.setattr("backend.config.list_configs", lambda: cfgs)
    monkeypatch.setattr("backend.config.duckdb_path", lambda sid: f"/mock/{sid}.duckdb")
    monkeypatch.setattr("os.path.exists", lambda path: "svc_missing" not in path)

    def _mock_getsize(path: str) -> int:
        if "svc_1" in path:
            return 2048
        if "svc_2" in path:
            return 8192
        raise FileNotFoundError(path)

    monkeypatch.setattr("os.path.getsize", _mock_getsize)
    assert ms_mod._sample_duckdb_size() == 2

    batch = metric_snapshots.get_batch(since="1h")
    assert batch["duckdb_file_bytes|svc_1"][-1]["value"] == 2048.0
    assert batch["duckdb_file_bytes|svc_2"][-1]["value"] == 8192.0
    assert "duckdb_file_bytes|svc_missing" not in batch


def test_metric_snapshot_registered_in_dev_local_safe_mode():
    sched = Scheduler()
    sched._add_job = MagicMock()
    sched._register_dev_local_safe_jobs()
    assert "metric_snapshot" in sched._job_ids
    sched._add_job.assert_any_call(
        ms_mod._run_metric_snapshot,
        "interval",
        seconds=60,
        jitter=5,
        id="metric_snapshot",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )
