"""Consolidated tests for RUM discovery/ledger sweep, Faro bundle reconciliation, and scheduler lifecycle."""

from __future__ import annotations

import ast
import concurrent.futures
import inspect
import logging
import pathlib
import threading
import time
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.core.metadata import pg_pool as pgc
from backend.core.metadata.base import get_con
from backend.cron import scheduler as decorators
from backend.cron import scheduler as sched
from backend.cron.jobs.rum_ledger import (
    _run_rum_discovery_cron,
    _run_rum_ledger_sweep,
    discover_rum_prefix,
    sweep_rum_ledger_once,
)
from backend.cron.jobs.rum_sync import _faro_purge_surrogate_key, _reconcile_faro_bundle
from backend.cron.scheduler import (
    CronHeartbeat,
    Scheduler,
    _check_disk_space,
    _claim_heavy_refresh,
    _display_label,
    _elapsed_since,
    _extract_log_text,
    _log_and_add_progress,
    dev_mode_no_crons,
    get_scheduler,
    record_skip_row,
)
from backend.main import app

SRC = {"name": "svc-1", "service_id": "svc-1", "service_name": "Svc One", "bucket": "test-bucket", "prefix": "logs"}
Usage = namedtuple("Usage", ["total", "used", "free"])


def _rum_cfg(enabled: bool = True, **extra) -> dict:
    return {
        "service_id": "svc-1",
        "fastly_api_key": "tok-123",
        "rum": {"enabled": enabled, "faro_version": "2.2.2", **extra},
    }


@pytest.fixture
def stub_rum_env(monkeypatch, stub_source, stub_cron_envelope):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.config.load_config", lambda sid: _rum_cfg(True))
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, run_id: True)
    return stub_cron_envelope


# ── _faro_purge_surrogate_key ───────────────────────────────────────────────


@pytest.mark.parametrize("sid,token", [("", "tok"), ("svc-1", ""), ("", "")])
def test_faro_purge_noop_when_missing_args(sid, token):
    with patch("backend.core.fastly.client.fastly") as fastly_mock:
        _faro_purge_surrogate_key(sid, token)
    fastly_mock.assert_not_called()


def test_faro_purge_posts_surrogate_key_on_logging_service_id():
    with patch("backend.core.fastly.client.fastly") as fastly_mock:
        _faro_purge_surrogate_key("logging-svc-42", "tok-abc")
    fastly_mock.assert_called_once_with(
        "POST",
        "/service/logging-svc-42/purge/rum-faro-sdk",
        token="tok-abc",
        expect_empty=True,
    )


def test_faro_purge_swallows_exceptions(caplog):
    caplog.set_level(logging.WARNING, logger="backend.scheduler")
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("fastly 503")):
        _faro_purge_surrogate_key("svc-1", "tok-abc")
    assert any("Faro surrogate-key purge failed" in r.message for r in caplog.records)


# ── _reconcile_faro_bundle ──────────────────────────────────────────────────


def test_reconcile_faro_bundle_returns_true_when_rum_disabled(monkeypatch):
    monkeypatch.setattr("backend.config.load_config", lambda sid: _rum_cfg(False))
    assert _reconcile_faro_bundle("svc-1", None) is True


def test_reconcile_faro_bundle_adopts_default_version_when_unpinned(monkeypatch):
    cfg = {"service_id": "svc-1", "fastly_api_key": "tok-1", "rum": {"enabled": True}}
    mutated = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.mutate_config", lambda sid, fn: mutated.append(sid))
    with (
        patch("backend.provision.rum_assets.download_and_upload_faro", new=AsyncMock()) as dl_mock,
        patch("backend.provision.rum_assets.faro_bundle_intact", return_value=True),
        patch("backend.provision.rum_assets.detect_faro_version_change", new=AsyncMock(return_value=False)),
        patch("backend.cron.jobs.rum_sync._faro_purge_surrogate_key") as purge_mock,
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as vcl_mock,
        patch("backend.provision.rum_orchestrator.rum_vcl_fingerprint", return_value="sha-1"),
    ):
        ok = _reconcile_faro_bundle("svc-1", run_id=10)
    assert ok is True
    dl_mock.assert_awaited_once_with("svc-1", DEFAULT_FARO_VERSION, "tok-1", expected_current_version=None)
    purge_mock.assert_called_once_with("svc-1", "tok-1")
    vcl_mock.assert_called_once_with("svc-1", "tok-1", dry_run=False, activate=True)


def test_reconcile_faro_bundle_restores_when_bundle_corrupt(monkeypatch):
    cfg = _rum_cfg(True, faro_last_upstream_check=time.time())
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    with (
        patch("backend.provision.rum_assets.faro_bundle_intact", return_value=False),
        patch("backend.provision.rum_assets.download_and_upload_faro", new=AsyncMock()) as dl_mock,
        patch("backend.cron.jobs.rum_sync._faro_purge_surrogate_key") as purge_mock,
    ):
        ok = _reconcile_faro_bundle("svc-1", run_id=11)
    assert ok is True
    dl_mock.assert_awaited_once_with("svc-1", "2.2.2", "tok-123", expected_current_version="2.2.2")
    purge_mock.assert_called_once_with("svc-1", "tok-123")


def test_reconcile_faro_bundle_returns_false_when_restore_fails(monkeypatch):
    cfg = _rum_cfg(True, faro_last_upstream_check=time.time())
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    with (
        patch("backend.provision.rum_assets.faro_bundle_intact", return_value=False),
        patch(
            "backend.provision.rum_assets.download_and_upload_faro",
            new=AsyncMock(side_effect=RuntimeError("403 Unauthorized")),
        ),
    ):
        assert _reconcile_faro_bundle("svc-1", run_id=12) is False


def test_reconcile_faro_bundle_resyncs_on_upstream_drift(monkeypatch):
    cfg = _rum_cfg(True, faro_last_upstream_check=0)
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.mutate_config", lambda sid, fn: None)
    with (
        patch("backend.provision.rum_assets.faro_bundle_intact", return_value=True),
        patch("backend.provision.rum_assets.detect_faro_version_change", new=AsyncMock(return_value=True)),
        patch("backend.provision.rum_assets.download_and_upload_faro", new=AsyncMock()) as dl_mock,
        patch("backend.cron.jobs.rum_sync._faro_purge_surrogate_key") as purge_mock,
    ):
        assert _reconcile_faro_bundle("svc-1", run_id=13) is True
    dl_mock.assert_awaited_once_with("svc-1", "2.2.2", "tok-123", expected_current_version="2.2.2")
    purge_mock.assert_called_once_with("svc-1", "tok-123")


# ── _run_rum_discovery_cron & _run_rum_ledger_sweep ─────────────────────────


def test_rum_discovery_cron_refuses_when_dev_no_crons(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    load_cfg = MagicMock()
    monkeypatch.setattr("backend.config.load_config", load_cfg)
    _run_rum_discovery_cron.__wrapped__("svc-1")
    load_cfg.assert_not_called()


def test_rum_discovery_cron_skips_when_rum_disabled(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.config.load_config", lambda sid: _rum_cfg(False))
    get_src = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.get_source_for_service", get_src)
    _run_rum_discovery_cron.__wrapped__("svc-1")
    get_src.assert_not_called()


def test_rum_discovery_cron_succeeds_without_celery_broker_url(monkeypatch, stub_rum_env):
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.discover_rum_prefix", lambda sid, **kw: 1)
    _run_rum_discovery_cron.__wrapped__("svc-1")
    stub_rum_env["log"].assert_called_once()
    args, kwargs = stub_rum_env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 5
    assert "Discovered 5 new RUM file(s)" in kwargs["summary"]


def test_rum_discovery_cron_records_discovered_count(monkeypatch, stub_rum_env):
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.discover_rum_prefix", lambda sid, **kw: 2)
    _run_rum_discovery_cron.__wrapped__("svc-1")
    stub_rum_env["log"].assert_called_once()
    args, kwargs = stub_rum_env["log"].call_args
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 10
    assert "Discovered 10 new RUM file(s)" in kwargs["summary"]


def test_rum_discovery_cron_records_warning_when_faro_reconcile_fails(monkeypatch, stub_rum_env):
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, run_id: False)
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.discover_rum_prefix", lambda sid, **kw: 0)
    _run_rum_discovery_cron.__wrapped__("svc-1")
    args, kwargs = (
        stub_rum_env["log"].assert_args
        if hasattr(stub_rum_env["log"], "assert_args")
        else stub_rum_env["log"].call_args
    )
    assert args[3] == "warning"
    assert "Faro bundle reconcile issue" in kwargs["summary"]


def test_rum_ledger_sweep_records_success_and_warnings(monkeypatch, stub_rum_env):
    monkeypatch.setattr(
        "backend.cron.jobs.rum_ledger.sweep_rum_ledger_once",
        lambda sid, **kw: {"reclaimed": 1, "redispatched": 1, "discovered": 3, "dead_letter": 0, "broker_ok": True},
    )
    _run_rum_ledger_sweep.__wrapped__("svc-1")
    args, kwargs = stub_rum_env["log"].call_args
    assert args[3] == "success"
    assert "reclaimed=1 redispatched=1 discovered=3" in kwargs["summary"]
    assert kwargs["files_downloaded"] == 3

    stub_rum_env["log"].reset_mock()
    monkeypatch.setattr(
        "backend.cron.jobs.rum_ledger.sweep_rum_ledger_once",
        lambda sid, **kw: {"reclaimed": 0, "redispatched": 0, "discovered": 0, "dead_letter": 2, "broker_ok": False},
    )
    _run_rum_ledger_sweep.__wrapped__("svc-1")
    args, kwargs = stub_rum_env["log"].call_args
    assert args[3] == "warning"
    assert "2 dead-letter/quarantined RUM row(s)" in kwargs["summary"]
    assert "Celery broker/queue depth probe failed" in kwargs["summary"]


def test_discover_rum_prefix_registers_discovered_objects(monkeypatch, stub_source):
    fake_obj = MagicMock(status="discovered")
    fake_page = MagicMock(objects=[fake_obj])
    fake_lister = MagicMock()
    fake_lister.list_source_objects.return_value = fake_page
    fake_ctrl = MagicMock()
    fake_ctrl.initialize_owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    fake_ctrl.discover_sources_batch.return_value = {"k1": fake_obj}

    monkeypatch.setattr("backend.cron.jobs.rum_ledger._get_fos_client", lambda src: MagicMock())
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.S3SourceObjectLister", lambda *a, **kw: fake_lister)
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.PostgresControlPlane", lambda: fake_ctrl)

    count = discover_rum_prefix("svc-1", prefix_subpath="raw/rum/2026-04-12T14:00")
    assert count == 1
    fake_lister.list_source_objects.assert_called_once()


def test_sweep_rum_ledger_once_aggregates_control_plane_state(monkeypatch, stub_source):
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    monkeypatch.setattr("backend.cron.jobs.rum_ledger._discover_high_scale_prefix", lambda sid, **kw: 4)
    fake_ctrl = MagicMock()
    fake_ctrl.expired_claims.return_value = ["c1", "c2"]
    fake_ctrl.operational_snapshot.return_value = {
        "source_objects": [
            {"domain": "rum_vitals", "status": "source_missing", "count": 3},
            {"domain": "request", "status": "source_missing", "count": 9},
        ]
    }
    monkeypatch.setattr("backend.cron.jobs.rum_ledger.PostgresControlPlane", lambda: fake_ctrl)
    summary = sweep_rum_ledger_once("svc-1")
    assert summary == {
        "reclaimed": 2,
        "redispatched": 2,
        "discovered": 4,
        "dead_letter": 3,
        "broker_ok": True,
    }


def test_manual_rum_endpoints_and_quarantine_routes():
    src = {"name": "svc-rum", "service_id": "svc-rum", "bucket": "b"}
    client = TestClient(app)
    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=src),
        patch("backend.config.load_config", return_value=_rum_cfg(True)),
        patch("backend.core.fos_client.start_cron_run", return_value=99),
        patch("backend.cron.jobs.rum_ledger._run_rum_discovery_cron"),
    ):
        r = client.post("/api/admin/rum/discovery/svc-rum")
        assert r.status_code == 200
        assert r.json()["run_id"] == 99

    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=src),
        patch("backend.config.load_config", return_value=_rum_cfg(True)),
        patch("backend.core.fos_client.start_cron_run", return_value=100),
        patch("backend.cron.jobs.rum_ledger._run_rum_ledger_sweep"),
    ):
        r2 = client.post("/api/admin/rum/ledger/sweep/svc-rum")
        assert r2.status_code == 200
        assert r2.json()["run_id"] == 100


# ── Pure Helpers (_elapsed_since, _extract_log_text, get_scheduler) ─────────


def test_elapsed_since_formats_seconds_and_minutes():
    now = time.time()
    s_short = _elapsed_since(now - 12.3)
    assert s_short.endswith("s") and "m" not in s_short
    s_long = _elapsed_since(now - 125.0)
    assert s_long.startswith("2m") and s_long.endswith("s")


def test_extract_log_text_filters_events(monkeypatch):
    monkeypatch.setattr(
        "backend.cron.scheduler.lifecycle.get_progress",
        lambda run_id: [
            {"type": "status", "message": "step 1"},
            {"type": "progress", "message": "ignored"},
            {"type": "done", "message": "finished"},
            {"type": "error", "message": "boom"},
        ],
    )
    assert _extract_log_text(None) == ""
    text = _extract_log_text(1)
    assert "[STATUS] step 1" in text
    assert "[DONE] finished" in text
    assert "[ERROR] boom" in text
    assert "ignored" not in text


def test_get_scheduler_returns_singleton(monkeypatch):
    monkeypatch.setattr("backend.cron.scheduler.runner._scheduler", None)
    s1 = get_scheduler()
    s2 = get_scheduler()
    assert s1 is s2


# ── _log_and_add_progress ───────────────────────────────────────────────────


def test_log_and_add_progress_routes_status_warning_and_error():
    event = {"type": "warning", "message": "watch out"}
    with (
        patch("backend.cron.scheduler.lifecycle.add_progress") as add_prog,
        patch("backend.cron.jobs.sync.get_service_display_label", return_value="svc-1"),
        patch("backend.cron.scheduler.lifecycle.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)
    add_prog.assert_called_once_with(1, event)
    mock_logger.warning.assert_called_once()


# ── _check_disk_space ───────────────────────────────────────────────────────


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
        patch("backend.cron.scheduler.lifecycle._DISK_FREE_HARD_FLOOR_PCT", 0.03),
    ):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is False and "2.0% of 50.0" in msg


def test_check_disk_space_does_not_block_on_probe_failure(tmp_path):
    with patch("shutil.disk_usage", side_effect=OSError("gone")):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True and msg == ""


# ── Merged from test_scheduler_apscheduler_stress.py & watchdog.py ──────────


def test_apscheduler_max_instances_1_waits_for_cleanup(monkeypatch, caplog):
    apscheduler = pytest.importorskip("apscheduler.schedulers.background")
    import backend.cron.scheduler.lifecycle as sched_mod

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
    import backend.core.metadata.base as pg_connection

    src = inspect.getsource(pg_connection.get_pg_pool)
    assert "timeout=" in src


def test_cron_task_waits_for_cleanup_after_watchdog_threshold(monkeypatch, caplog):
    import backend.cron.scheduler.lifecycle as sched_mod

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
    import backend.cron.scheduler.lifecycle as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 60)
    sentinel = object()

    @sched_mod.cron_task("happy-path")
    def _quick(service_id: str):
        return sentinel

    start = time.monotonic()
    result = _quick("svc-happy")
    assert result is sentinel and (time.monotonic() - start) < 5


def test_cron_task_preserves_telemetry_context(monkeypatch):
    import backend.cron.scheduler.lifecycle as sched_mod
    from backend.utils.telemetry import get_process_context

    captured = {}

    @sched_mod.cron_task("ctx-test")
    def _check_ctx(service_id: str) -> None:
        captured["ctx"] = get_process_context()

    _check_ctx("svc-ctx")
    assert captured.get("ctx") == "ctx-test"


def test_cron_task_heartbeat_thread_releases_its_connection(monkeypatch):
    import backend.cron.scheduler.lifecycle as sched_mod

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


def test_dev_local_allowlist_registers_only_insights_prewarmer(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {
        "service_id": "svc-x",
        "provisioning": {},
    }
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.fos_client.get_source_for_service", return_value={"name": "svc-x"}),
        patch("backend.core.fos_client.is_configured", return_value=True),
        patch.object(s._sched, "add_job") as add_job,
    ):
        s._register_dev_local_safe_jobs()

    expected = ["insights_prewarmer_svc-x"]
    assert sorted(s._job_ids) == expected
    assert sorted(c.kwargs["id"] for c in add_job.call_args_list) == expected


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
    with patch("backend.cron.jobs.sync.get_source_for_service") as get_src:
        fn("svc-test", **kwargs)
        get_src.assert_not_called()
    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


# ── Merged from test_scheduler_branches.py ──────────────────────────────────


def test_display_label_collapses_when_name_matches_id():
    assert _display_label({"service_name": "svc-1", "name": "svc-1"}, "svc-1") == "svc-1"


def test_display_label_includes_both_when_distinct():
    assert _display_label({"service_name": "My Service", "name": "svc-1"}, "svc-1") == "My Service (svc-1)"


def test_claim_heavy_refresh_first_call_wins():
    sid = "test-claim-heavy-1"
    sched._last_heavy_refresh.pop(sid, None)
    assert _claim_heavy_refresh(sid) is True


def test_claim_heavy_refresh_returns_false_when_throttled():
    sid = "test-claim-heavy-2"
    sched._last_heavy_refresh[sid] = time.time()
    assert _claim_heavy_refresh(sid) is False


def test_claim_heavy_refresh_reopens_window_after_interval():
    sid = "test-claim-heavy-3"
    sched._last_heavy_refresh[sid] = time.time() - sched._HEAVY_REFRESH_INTERVAL_SEC - 10
    assert _claim_heavy_refresh(sid) is True


def test_claim_heavy_refresh_backs_off_in_proportion_to_the_last_run_cost():
    sid = "test-claim-heavy-cost"
    sched._last_heavy_refresh.pop(sid, None)
    sched._last_heavy_refresh_cost.pop(sid, None)
    assert _claim_heavy_refresh(sid) is True
    sched._record_heavy_refresh_cost(sid, 50.0)

    sched._last_heavy_refresh[sid] = time.time() - 300
    assert _claim_heavy_refresh(sid) is False
    sched._last_heavy_refresh[sid] = time.time() - 510
    assert _claim_heavy_refresh(sid) is True


def test_claim_heavy_refresh_cheap_runs_keep_the_base_interval():
    sid = "test-claim-heavy-cheap"
    sched._last_heavy_refresh_cost.pop(sid, None)
    sched._record_heavy_refresh_cost(sid, 1.0)
    sched._last_heavy_refresh[sid] = time.time() - sched._HEAVY_REFRESH_INTERVAL_SEC - 5
    assert _claim_heavy_refresh(sid) is True
    sched._last_heavy_refresh[sid] = time.time() - sched._HEAVY_REFRESH_INTERVAL_SEC / 2
    assert _claim_heavy_refresh(sid) is False


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


def test_defaults_to_a_skipped_row_with_zero_duration(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.fos_client.log_cron_run", log)
    record_skip_row(SRC, "alerts", "No alerts configured")
    log.assert_called_once_with(
        SRC, "alerts", 0.0, "skipped", summary="No alerts configured", error_message=None, run_id=None
    )


def test_forwards_status_duration_error_and_run_id(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.fos_client.log_cron_run", log)
    record_skip_row(
        SRC,
        "gap_heal",
        "Skipped due to low disk space",
        status="warning",
        duration_s=1.5,
        error_message="Disk space critically low",
        run_id=42,
    )
    log.assert_called_once_with(
        SRC,
        "gap_heal",
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


class TestLogCronRunDurationIsFloat:
    def test_duration_passed_to_log_cron_run_is_float(self, monkeypatch):
        monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
        monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
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

        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.cron.jobs.sync.get_source_for_service", return_value=mock_src),
            patch("backend.core.fos_client.get_source_for_service", return_value=mock_src),
            patch("backend.core.fos_client.start_cron_run", return_value=1),
            patch("backend.core.fos_client.log_cron_run", side_effect=fake_log_cron_run),
            patch("backend.cron.jobs.sync.refresh_config_status"),
            patch("backend.cron.jobs.sync.discover_prefix", return_value=0),
            patch("backend.cron.scheduler.lifecycle.cleanup_progress_and_reap"),
            patch("backend.cron.scheduler.lifecycle.start_progress"),
            patch("backend.cron.scheduler.lifecycle.end_progress"),
            patch("backend.cron.scheduler.lifecycle.get_progress", return_value=[]),
            patch("backend.cron.jobs.sync.backfill_fastly_edge_writes"),
            patch("backend.cron.jobs.sync.reconcile_fastly_stats"),
            patch("backend.utils.telemetry.flush_usage_log"),
            patch("backend.utils.telemetry.run_usage_log_cleanup"),
            patch("backend.core.fos_client.update_cron_duration"),
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
    def test_no_time_minus_start_time_in_scheduler(self):
        assert not _find_bad_start_time_subtractions("backend/cron/jobs/sync.py", "_run_log_discovery_cron")


# ── Merged from test_heartbeat.py ────────────────────────────────────────────

SID = "svc-heartbeat"
JOB = "heartbeat_job"
OTHER_JOB = "other_job"


def _stray_threads(before: set[threading.Thread]) -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t not in before and t.is_alive() and t.name == f"cron-heartbeat-{SID}"]


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
    monkeypatch.setattr("backend.cron.scheduler.lifecycle.DEFAULT_INTERVAL_S", 0.02)


def test_cron_task_leaks_nothing_when_the_executor_refuses_the_job(baseline, fast_heartbeat):
    before, checked_out = baseline

    @decorators.cron_task("exec-refuses", job_name=JOB)
    def _job(service_id: str):
        return None

    class _Dead:
        def submit(self, *_a, **_kw):
            raise RuntimeError("cannot schedule new futures after shutdown")

    with patch("backend.cron.scheduler.lifecycle._get_watchdog_executor", return_value=_Dead()):
        with pytest.raises(RuntimeError, match="shutdown"):
            _job(SID)
    assert _wait_for_exit(_stray_threads(before)) == [] and len(pgc._checked_out) == checked_out


def test_cron_task_leaks_nothing_across_repeated_failures_and_watchdog_timeouts(baseline, fast_heartbeat, monkeypatch):
    before, checked_out = baseline
    monkeypatch.setattr("backend.cron.scheduler.lifecycle._CRON_HARD_CAP_S", 0.05)

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
    monkeypatch.setattr("backend.cron.scheduler.lifecycle._CRON_HARD_CAP_S", 0.05)

    @decorators.cron_task("slow-result", job_name=JOB)
    def _job(service_id: str):
        time.sleep(0.15)
        return "finished"

    assert _job(SID) == "finished"
