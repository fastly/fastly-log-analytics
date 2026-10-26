"""Consolidated tests for test_scheduler."""

from __future__ import annotations

import ast
import concurrent.futures
import contextlib
import inspect
import logging
import pathlib
import threading
import time
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from backend.cron.lifecycle import record_skip_row

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron import scheduler as sched
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
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, alerts=1)
    assert {"log_discovery_svc-1", "commit_svc-1", "alerts_evaluation_svc-1"}.issubset(s._job_ids)


def test_sync_jobs_registers_metadata_sync_and_alerts_for_readonly_service():
    cfg = {
        "service_id": "svc-2",
        "log_period": 60,
        "access_level": "read_only",
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, alerts=1)
    assert "sync_metadata_svc-2" in s._job_ids
    assert "alerts_evaluation_svc-2" in s._job_ids
    assert "sync_svc-2" not in s._job_ids and "commit_svc-2" not in s._job_ids


def test_sync_jobs_skips_alerts_cron_when_no_alerts_configured():
    cfg = {
        "service_id": "svc-noalert",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, alerts=0)
    assert "log_discovery_svc-noalert" in s._job_ids and "commit_svc-noalert" in s._job_ids
    assert "alerts_evaluation_svc-noalert" not in s._job_ids


def test_sync_jobs_skips_disabled_services():
    cfg = {
        "service_id": "paused-svc",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": False}},
    }
    s = _run_sync_jobs(cfg)
    assert "sync_paused-svc" not in s._job_ids and "commit_paused-svc" not in s._job_ids


def test_sync_jobs_skips_unconfigured_service():
    cfg = {
        "service_id": "halfbaked",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg, is_configured=False)
    assert "sync_halfbaked" not in s._job_ids


def test_sync_jobs_registers_ngwaf_job_when_workspace_configured():
    cfg = {
        "service_id": "ngwaf-svc",
        "log_period": 60,
        "access_level": "read_write",
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
        "access_level": "read_write",
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
        "access_level": "read_write",
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
        "access_level": "read_write",
        "rum_enabled": True,
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rum_sync_svc-rum-root" in s._job_ids and "rum_commit_svc-rum-root" in s._job_ids


def test_sync_jobs_registers_rum_jobs_when_rum_enabled_nested():
    cfg = {
        "service_id": "svc-rum-nested",
        "log_period": 60,
        "access_level": "read_write",
        "rum": {"enabled": True},
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rum_sync_svc-rum-nested" in s._job_ids and "rum_commit_svc-rum-nested" in s._job_ids


def test_sync_jobs_does_not_register_rum_jobs_when_rum_disabled_in_nested():
    cfg = {
        "service_id": "svc-rum-disabled",
        "log_period": 60,
        "access_level": "read_write",
        "rum_enabled": True,
        "rum": {"enabled": False},
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    s = _run_sync_jobs(cfg)
    assert "rum_sync_svc-rum-disabled" not in s._job_ids and "rum_commit_svc-rum-disabled" not in s._job_ids


def test_resolve_rum_sync_interval():
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
        "access_level": "read_write",
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


def test_sync_jobs_skips_rollup_heal_for_read_only_service():
    cfg = {
        "service_id": "svc-2",
        "log_period": 60,
        "provisioning": {
            "cron_sync": {"enabled": True},
            "cron_compact": {"enabled": True},
            "access_level": "read_only",
        },
    }
    s = _run_sync_jobs(cfg)
    assert "rollup_compact_svc-2" not in s._job_ids and "rollup_heal_svc-2" not in s._job_ids


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
    with patch("backend.core.iceberg.buffer_backlog_stats", return_value=stats):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


def test_check_buffer_backlog_warns_when_file_count_exceeds_threshold():
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 250, "total_bytes": 1_000_000, "oldest_age_seconds": 30, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "250 files" in suffix and suffix.startswith(" ⚠")


def test_check_buffer_backlog_warns_when_oldest_age_exceeds_scaled_threshold():
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": 1000, "oldest_age_seconds": 30 * 60, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "30m old" in suffix


def test_check_buffer_backlog_warns_on_total_bytes():
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": sched._BACKLOG_BYTES_WARN + 1, "oldest_age_seconds": 0},
    ):
        out = _check_buffer_backlog({"name": "s"}, "s", 5)
    assert "MB on disk" in out


def test_check_buffer_backlog_never_raises_on_probe_failure():
    with patch("backend.core.iceberg.buffer_backlog_stats", side_effect=OSError("disk gone")):
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
        "provisioning": {"access_level": "read_write", "cron_compact": {"enabled": True}},
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


def test_dev_local_allowlist_skips_rollup_compact_for_read_only(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {"service_id": "svc-ro", "provisioning": {"access_level": "read_only"}}
    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ro"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(s._sched, "add_job") as add_job,
    ):
        s._register_dev_local_safe_jobs()

    expected = [
        "insights_prewarmer_svc-ro",
        "local_compact_svc-ro",
        "metric_snapshot",
        "partial_hour_merge_svc-ro",
    ]
    assert sorted(s._job_ids) == expected
    assert sorted(c.kwargs["id"] for c in add_job.call_args_list) == expected


def test_dev_local_allowlist_skips_high_scale_services(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    cfg = {"service_id": "svc-high", "provisioning": {"access_level": "read_write"}}
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

        mock_src = {"name": "test", "service_id": "test", "access_level": "read_write", "storage_mode": "cloud"}
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
            "access_level": "read_write",
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
