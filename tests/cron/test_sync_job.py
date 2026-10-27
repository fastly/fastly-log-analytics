"""Consolidated tests for test_sync_job."""

from __future__ import annotations

import contextlib
import gzip
import io
import json
import logging
import os
import stat
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.deps import get_service_id, get_source, require_admin
from fastapi import HTTPException
from starlette.testclient import TestClient
from tests._helpers.config_fakes import fake_mutate_config

from backend.core import metadata as metadata_db
from backend.core.ingest import sweep_ledger_once
from backend.core.metadata import usage_log_db
from backend.cron.jobs import commit
from backend.cron.jobs import rum_ledger as ledger_mod
from backend.cron.jobs import sync as sync_mod
from backend.cron.scheduler import Scheduler
from backend.main import app
from backend.routers.admin import SustainedLossAlert
from backend.utils.telemetry import get_process_context
from tests.cron.conftest import (
    assert_manual_trigger_non_admin_rejected,
    assert_manual_trigger_standard_mode_rejected,
)


@pytest.fixture(autouse=True)
def clean_dev_no_crons(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


def _fake_src(service_id: str = "svc-1") -> dict:
    return {"name": service_id, "service_id": service_id, "bucket": "fos-test-bkt"}


@pytest.fixture
def stub_progress(monkeypatch) -> dict[str, MagicMock]:
    start_progress, end_progress, cleanup, log_event = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", cleanup)
    monkeypatch.setattr(sync_mod, "_log_and_add_progress", log_event)
    monkeypatch.setattr(sync_mod, "_display_label", lambda src, sid: src.get("name", sid))
    monkeypatch.setattr(sync_mod, "_extract_log_text", lambda rid: "")
    monkeypatch.setattr(sync_mod, "_check_disk_space", lambda cache_dir, sid, name: (True, ""))
    monkeypatch.setattr(sync_mod, "_claim_heavy_refresh", lambda sid: False)
    return {"start_progress": start_progress, "end_progress": end_progress, "cleanup": cleanup, "log_event": log_event}


@pytest.fixture
def stub_usage_log_phase(monkeypatch) -> None:
    monkeypatch.setattr("backend.core.duckdb.backfill_fastly_edge_writes", MagicMock(return_value=0))
    monkeypatch.setattr("backend.core.duckdb.reconcile_fastly_stats", MagicMock(return_value=0))
    monkeypatch.setattr("backend.utils.telemetry.run_usage_log_cleanup", MagicMock())


@pytest.fixture
def stub_post_ingest(monkeypatch) -> None:
    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", MagicMock())
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr("backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    monkeypatch.setattr("backend.repositories.dashboard._dashboard_cache", {}, raising=False)
    monkeypatch.setattr("backend.repositories.dashboard.invalidate_service", MagicMock(), raising=False)


@pytest.fixture
def stub_load_config(monkeypatch):
    cfg = {
        "service_id": "svc-1",
        "name": "svc-1",
        "provisioning": {"cron_sync": {"enabled": True, "polling_mode": "regular"}},
    }
    load = MagicMock(return_value=cfg)
    save = MagicMock(side_effect=fake_mutate_config(lambda: cfg))
    monkeypatch.setattr("backend.config.load_config", load)
    monkeypatch.setattr("backend.config.mutate_config", save)
    return {"load": load, "save": save, "cfg": cfg}


def _make_ingest_events(events: list[dict]) -> Any:
    def _gen(*args, **kwargs):
        yield from events

    return _gen


def _setup_discovery_env(
    monkeypatch,
    events: list[dict] | None = None,
    *,
    run_id: int = 42,
) -> MagicMock:
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=run_id))
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    if events is not None:
        monkeypatch.setattr("backend.core.ingest.ingest", _make_ingest_events(events))
    return log_cron


# ── _run_log_discovery_cron ──────────────────────────────────────────────────


def test_scheduled_tick_is_not_deferred_by_in_flight_requests(monkeypatch, stub_load_config):
    from backend.utils import active_requests

    active_requests._reset_for_tests()
    active_requests.increment_active_requests()
    try:
        get_src = MagicMock(return_value=None)
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
        sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
        stub_load_config["load"].assert_called_once_with("svc-1")
        get_src.assert_called_once_with("svc-1")
    finally:
        active_requests._reset_for_tests()


@pytest.mark.parametrize("case", ["config_missing", "source_missing"])
def test_discovery_early_exit_gates(monkeypatch, stub_load_config, case):
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    start_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    if case == "config_missing":
        monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
        get_src = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
        sync_mod._run_log_discovery_cron.__wrapped__("ghost-svc")
        get_src.assert_not_called()
    elif case == "source_missing":
        monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=None))
        sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    start_cron.assert_not_called()


def test_high_scale_discovery_persists_its_originating_run_id(monkeypatch, stub_load_config):
    source = {**_fake_src(), "deployment_mode": "high_scale"}
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://broker")
    discovery = MagicMock(return_value=1)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discovery)
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=521)

    assert discovery.call_count == 5
    assert all(call.kwargs["run_id"] == 521 for call in discovery.call_args_list)
    assert log_cron.call_args.kwargs["run_id"] == 521


@pytest.mark.parametrize(
    ("broker_url", "discover_err", "run_id", "expected_calls", "expected_err"),
    [
        ("", None, 522, 0, "CELERY_BROKER_URL"),
        ("memory://", OSError("FOS unavailable"), 523, 1, "FOS unavailable"),
    ],
)
def test_high_scale_discovery_error_paths(
    monkeypatch, stub_load_config, broker_url, discover_err, run_id, expected_calls, expected_err
):
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: _fake_src())
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", broker_url)
    discover = MagicMock(side_effect=discover_err)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)
    log_run = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=run_id)

    assert discover.call_count == expected_calls
    if expected_calls:
        assert discover.call_args.kwargs["run_id"] == run_id
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["run_id"] == run_id
    assert expected_err in log_run.call_args.kwargs["error_message"]


def test_sync_no_new_files_surfaces_reclaimed_count(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    log_cron = _setup_discovery_env(
        monkeypatch,
        [
            {
                "type": "done",
                "new_files": 0,
                "rows_inserted": 0,
                "deleted_files": 3,
                "message": "No new files; reclaimed 3 raw file(s) left by an interrupted prior run.",
            }
        ],
    )
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)
    success_calls = [c for c in log_cron.call_args_list if c.args[3] == "success"]
    assert success_calls
    kwargs = success_calls[-1].kwargs
    assert kwargs.get("files_deleted_fos") == 3
    assert "reclaim" in kwargs.get("summary", "").lower()


def test_log_discovery_persists_data_plane_outcomes_as_error(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    counters = {
        "valid_records": 1,
        "malformed_records": 1,
        "corrupt_containers": 0,
        "quarantine_capture_failures": 0,
        "source_delete_failures": 0,
        "cap_evictions": 0,
        "objects_processed": 1,
        "objects_successful": 0,
        "objects_partial": 1,
        "objects_failed": 0,
    }
    log_cron = _setup_discovery_env(
        monkeypatch,
        [{"type": "done", "new_files": 1, "rows_inserted": 1, "corrupt_rows": 1, "outcome_counters": counters}],
    )
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)
    terminal = [call for call in log_cron.call_args_list if call.args[3] == "error"]
    assert terminal and terminal[-1].kwargs["outcome_counters"] == counters


def test_adaptive_ingest_stops_after_empty_followup_and_aggregates_outcomes(monkeypatch):
    pass_results = iter([2, 1, 0])
    calls, sleeps = [], []

    def ingest_fn(**kwargs):
        count = next(pass_results)
        calls.append(kwargs)
        yield {
            "type": "done",
            "new_files": count,
            "rows_inserted": count * 10,
            "outcome_counters": {"valid_records": count * 10, "objects_processed": count},
        }

    monkeypatch.setattr(sync_mod.time, "sleep", sleeps.append)
    events = list(sync_mod._ingest_with_adaptive_followups(ingest_fn, adaptive=True, service_id="svc"))
    done = events[-1]
    assert len(calls) == 3 and sleeps == [0.5, 0.5]
    assert (done["new_files"], done["rows_inserted"]) == (3, 30)
    assert done["outcome_counters"]["valid_records"] == 30 and done["outcome_counters"]["objects_processed"] == 3


def test_adaptive_ingest_publishes_rows_before_each_followup(monkeypatch):
    pass_results = iter([2, 1, 0])
    timeline: list[str] = []

    def ingest_fn(**_kwargs):
        count = next(pass_results)
        timeline.append(f"pass:{count}")
        yield {"type": "done", "new_files": count, "rows_inserted": count * 10}

    monkeypatch.setattr(sync_mod.time, "sleep", lambda _s: timeline.append("sleep"))
    list(
        sync_mod._ingest_with_adaptive_followups(
            ingest_fn, adaptive=True, after_pass=lambda: timeline.append("publish")
        )
    )
    assert timeline == ["pass:2", "publish", "sleep", "pass:1", "publish", "sleep", "pass:0"]


def test_adaptive_ingest_skips_publish_when_no_followup_runs():
    published: list[None] = []

    def ingest_fn(**_kwargs):
        yield {"type": "done", "new_files": 1, "rows_inserted": 5}

    list(sync_mod._ingest_with_adaptive_followups(ingest_fn, adaptive=False, after_pass=lambda: published.append(None)))
    assert published == []


def test_adaptive_ingest_obeys_twenty_second_followup_cap(monkeypatch):
    now, calls = [0.0], []

    def ingest_fn(**_kwargs):
        calls.append(None)
        now[0] = 18.0
        yield {"type": "done", "new_files": 1}

    monkeypatch.setattr(sync_mod.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(sync_mod.time, "sleep", lambda _seconds: now.__setitem__(0, now[0] + 3))
    list(sync_mod._ingest_with_adaptive_followups(ingest_fn, adaptive=True))
    assert len(calls) == 1


def test_log_discovery_stands_down_for_a_high_scale_shared_source(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    _setup_discovery_env(monkeypatch)
    stub_load_config["cfg"]["provisioning"]["cron_sync"]["delete_after"] = True
    stub_load_config["cfg"]["provisioning"]["cron_sync"]["high_scale_shared_source"] = True
    ingest_mock = MagicMock(
        side_effect=_make_ingest_events(
            [{"type": "done", "new_files": 0, "rows_inserted": 0, "deleted_files": 0, "message": "ok"}]
        )
    )
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)
    assert ingest_mock.call_args.kwargs["delete_after"] is False


def test_sync_status_published_after_request_extents_refresh(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    calls: list[str] = []
    status = {"latest_log_at": "2026-01-01T00:00:00Z"}
    _setup_discovery_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}])

    def _fake_refresh_config_status(*args, include_top_values=True, **kwargs):
        calls.append(f"refresh_config_status(top_values={include_top_values})")
        status["latest_log_at"] = "2026-01-02T00:00:00Z"

    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", _fake_refresh_config_status)
    monkeypatch.setattr(
        "backend.sync_status_snapshot.compute_sync_status_cached",
        lambda service_id: calls.append("compute_sync_status_cached") or dict(status),
    )
    fake_publisher = MagicMock()
    fake_publisher.publish.side_effect = lambda *a, **k: calls.append("publish")
    monkeypatch.setattr("backend.sync_status_publisher.publisher", fake_publisher)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", force=True)
    assert calls == [
        "refresh_config_status(top_values=False)",
        "compute_sync_status_cached",
        "publish",
        "refresh_config_status(top_values=True)",
    ]
    fake_publisher.publish.assert_called_once_with("svc-1", {"latest_log_at": "2026-01-02T00:00:00Z"})


@pytest.mark.parametrize("heavy", [False, True])
def test_usage_log_bookkeeping_runs_only_on_heavy_refresh_ticks(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, heavy
):
    monkeypatch.setattr(sync_mod, "_claim_heavy_refresh", lambda sid: heavy)
    _setup_discovery_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}])
    backfill = MagicMock(return_value=0)
    cleanup = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.backfill_fastly_edge_writes", backfill)
    monkeypatch.setattr("backend.core.duckdb.reconcile_fastly_stats", MagicMock(return_value=0))
    monkeypatch.setattr("backend.utils.telemetry.run_usage_log_cleanup", cleanup)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    assert backfill.called is heavy and cleanup.called is heavy


def test_manual_sync_all_clears_time_range(monkeypatch, stub_progress, stub_post_ingest, stub_usage_log_phase):
    cfg = {
        "service_id": "svc-1",
        "name": "svc-1",
        "provisioning": {"cron_sync": {"enabled": True}, "time_range": {"start": "2025-01-01", "end": "2025-01-31"}},
    }
    saved: list[dict] = []
    monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=cfg))
    monkeypatch.setattr("backend.config.mutate_config", fake_mutate_config(cfg, saves=saved))
    _setup_discovery_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}])

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1", run_id=42)
    assert saved and "time_range" not in saved[-1].get("provisioning", {})


def test_disk_space_failure_logs_error_and_returns(monkeypatch, stub_load_config, stub_progress):
    log_cron = _setup_discovery_env(monkeypatch)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda src: "/tmp/cache")
    monkeypatch.setattr(sync_mod, "_check_disk_space", lambda cache_dir, sid, name: (False, "Free space below floor"))
    ingest_mock = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    ingest_mock.assert_not_called()
    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error" and kwargs.get("error_message") == "Free space below floor"
    assert "Sync aborted" in kwargs.get("summary", "")


def test_ingest_error_event_logs_cron_with_processed_files(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    log_cron = _setup_discovery_env(
        monkeypatch,
        [
            {"type": "file_done", "current": 5, "total_inserted": 1234, "total_corrupt": 2},
            {"type": "error", "message": "S3 transient failure"},
        ],
    )
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    error_calls = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert error_calls
    kwargs = error_calls[0].kwargs
    assert kwargs.get("error_message") == "S3 transient failure"
    assert (kwargs.get("files_downloaded"), kwargs.get("rows_ingested"), kwargs.get("corrupt_rows")) == (5, 1234, 2)
    assert "5 files" in kwargs.get("summary", "")


def _wait_for_post_ingest_rollups(service_id: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        with sync_mod._rollup_lock:
            worker = sync_mod._rollup_workers.get(service_id)
        if worker is None:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        worker.join(min(remaining, 0.05))


def test_ingest_done_event_records_summary_and_recompute_rollups(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    log_cron = _setup_discovery_env(
        monkeypatch,
        [
            {
                "type": "done",
                "new_files": 7,
                "rows_inserted": 2000,
                "corrupt_rows": 0,
                "deleted_files": 7,
                "touched_hours": ["2026-06-15-09", "2026-06-15-10"],
            }
        ],
    )
    recompute = MagicMock()
    monkeypatch.setattr("backend.core.rollups.recompute_touched_hours", recompute)
    monkeypatch.setattr("backend.core.rollups.recompute_wellknown_bots_rollup", MagicMock(return_value=0))

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    assert _wait_for_post_ingest_rollups("svc-1", timeout=5)
    success_calls = [c for c in log_cron.call_args_list if c.args[3] == "success"]
    assert success_calls
    summary = success_calls[0].kwargs.get("summary", "")
    assert "7 files" in summary and "2000 rows" in summary
    recompute.assert_called_once()
    assert recompute.call_args.args[2] == {"2026-06-15-09", "2026-06-15-10"}


def test_rollup_recompute_does_not_block_discovery_tick(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    _setup_discovery_env(
        monkeypatch,
        [{"type": "done", "new_files": 1, "rows_inserted": 5, "deleted_files": 1, "touched_hours": ["2026-06-15-09"]}],
    )
    release, started = threading.Event(), threading.Event()

    def _slow_recompute(_sid, _src, _hours):
        started.set()
        release.wait(5)

    monkeypatch.setattr("backend.core.rollups.recompute_touched_hours", _slow_recompute)
    monkeypatch.setattr("backend.core.rollups.recompute_wellknown_bots_rollup", MagicMock(return_value=0))

    sync_mod._run_log_discovery_cron.__wrapped__("svc-block")
    assert started.wait(5)
    assert not _wait_for_post_ingest_rollups("svc-block", timeout=0.05)
    release.set()
    assert _wait_for_post_ingest_rollups("svc-block", timeout=5)


def test_post_ingest_rollups_coalesce_hours_queued_while_running(monkeypatch):
    calls: list[set[str]] = []
    threads: set[str] = set()
    release, first_started = threading.Event(), threading.Event()

    def _recompute(_sid, _src, hours):
        threads.add(threading.current_thread().name)
        calls.append(set(hours))
        if len(calls) == 1:
            first_started.set()
            release.wait(5)

    monkeypatch.setattr("backend.core.rollups.recompute_touched_hours", _recompute)
    monkeypatch.setattr("backend.core.rollups.recompute_wellknown_bots_rollup", MagicMock(return_value=0))

    src = _fake_src()
    sync_mod.schedule_post_ingest_rollups("svc-coalesce", src, {"2026-06-15-09"})
    assert first_started.wait(5)
    sync_mod.schedule_post_ingest_rollups("svc-coalesce", src, {"2026-06-15-10"})
    sync_mod.schedule_post_ingest_rollups("svc-coalesce", src, {"2026-06-15-11", "2026-06-15-10"})
    release.set()

    assert _wait_for_post_ingest_rollups("svc-coalesce", timeout=5)
    assert calls == [{"2026-06-15-09"}, {"2026-06-15-10", "2026-06-15-11"}]
    assert len(threads) == 1


def test_ingest_exception_records_crashed(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    log_cron = _setup_discovery_env(monkeypatch)

    def _boom(*args, **kwargs):
        yield {"type": "file_done", "current": 2, "total_inserted": 100, "total_corrupt": 0}
        raise RuntimeError("DuckDB connection refused")

    monkeypatch.setattr("backend.core.ingest.ingest", _boom)
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    error_calls = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert error_calls
    kwargs = error_calls[0].kwargs
    assert "Ingestion crashed" in kwargs.get("summary", "")
    assert "DuckDB connection refused" in kwargs.get("error_message", "")
    stub_progress["end_progress"].assert_called()


def test_ingest_without_terminal_done_event_finalizes_row(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    log_cron = _setup_discovery_env(
        monkeypatch, [{"type": "file_done", "current": 3, "total_inserted": 50, "total_corrupt": 0}]
    )
    finalize = MagicMock(return_value=True)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    assert not [c for c in log_cron.call_args_list if c.args[3] in ("success", "error")]
    finalize.assert_called_once()
    assert finalize.call_args.args[1:3] == ("log_discovery", 42)
    stub_progress["end_progress"].assert_called_once()


def test_successful_done_does_not_double_finalize(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    _setup_discovery_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}])
    finalize = MagicMock(return_value=False)
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    finalize.assert_called_once()
    assert finalize.call_args.args[2] == 42


# ── _run_full_sweep & time_range pinning ─────────────────────────────────────


def test_full_sweep_returns_silently_when_source_missing(monkeypatch, stub_load_config):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=None))
    start_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    sync_mod._run_full_sweep.__wrapped__("ghost-svc")
    start_cron.assert_not_called()


def test_scheduled_discovery_persists_service_creation_time_as_range_start(monkeypatch):
    source = _fake_src()
    cfg = {"created_at": "2026-09-29T12:00:00Z", "provisioning": {"cron_sync": {"enabled": True}}}
    save = MagicMock(side_effect=fake_mutate_config(cfg))
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.mutate_config", save)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "memory://")
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 108)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    discover = MagicMock(return_value=0)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-creation-range")
    assert cfg["provisioning"]["time_range"] == {"start": "2026-09-29T12:00:00Z"}
    assert source["time_range"] == cfg["provisioning"]["time_range"]
    save.assert_called_once()
    assert discover.call_count == 5


def test_manual_discovery_clears_pinned_range_before_listing(monkeypatch):
    source = _fake_src()
    cfg = {
        "provisioning": {
            "cron_sync": {"enabled": True, "polling_mode": "adaptive"},
            "time_range": {"start": "2026-09-01T00:00:00Z"},
        }
    }
    save = MagicMock(side_effect=fake_mutate_config(cfg))
    monkeypatch.setattr("backend.config.load_config", lambda sid: cfg)
    monkeypatch.setattr("backend.config.mutate_config", save)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "memory://")
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: source)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    discover = MagicMock(return_value=1)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-manual-range", force=True, run_id=109)
    assert "time_range" not in cfg["provisioning"] and source["time_range"] is None
    save.assert_called_once()
    assert discover.call_count == 5
    assert {call.kwargs["run_id"] for call in discover.call_args_list} == {109}


def test_full_sweep_skipped_when_start_cron_run_raises(monkeypatch, stub_load_config):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(side_effect=CronLeaseHeldError("held")))
    ingest_mock = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest_mock)
    start_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)

    sync_mod._run_full_sweep.__wrapped__("svc-1")
    ingest_mock.assert_not_called()
    start_progress.assert_not_called()


def test_full_sweep_success_logs_new_files_count(monkeypatch, stub_load_config, stub_progress, stub_post_ingest):
    log_cron = _setup_discovery_env(
        monkeypatch,
        [
            {"type": "file_done", "current": 4, "total_inserted": 500, "total_corrupt": 0},
            {"type": "done", "new_files": 4, "rows_inserted": 500, "corrupt_rows": 0},
        ],
        run_id=99,
    )
    sync_mod._run_full_sweep.__wrapped__("svc-1")
    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "success"
    assert "Backfilled 4" in kwargs.get("summary", "") and "500" in kwargs.get("summary", "")
    assert (kwargs.get("files_downloaded"), kwargs.get("rows_ingested")) == (4, 500)


def test_full_sweep_surfaces_reclaimed_strand_count(monkeypatch, stub_load_config, stub_progress):
    log_cron = _setup_discovery_env(
        monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0, "deleted_files": 5}], run_id=99
    )
    sync_mod._run_full_sweep.__wrapped__("svc-1")
    log_cron.assert_called_once()
    kwargs = log_cron.call_args.kwargs
    assert kwargs.get("files_deleted_fos") == 5 and "reclaim" in kwargs.get("summary", "").lower()


def test_full_sweep_error_event_logs_with_processed_counts(monkeypatch, stub_load_config, stub_progress):
    log_cron = _setup_discovery_env(
        monkeypatch,
        [
            {"type": "file_done", "current": 3, "total_inserted": 200, "total_corrupt": 1},
            {"type": "error", "message": "rate-limited"},
        ],
        run_id=99,
    )
    sync_mod._run_full_sweep.__wrapped__("svc-1")
    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error" and kwargs.get("error_message") == "rate-limited"
    assert (kwargs.get("files_downloaded"), kwargs.get("rows_ingested"), kwargs.get("corrupt_rows")) == (3, 200, 1)
    assert "Full-sweep failed" in kwargs.get("summary", "")


def test_full_sweep_finalizes_duration(monkeypatch, stub_load_config, stub_progress):
    _setup_discovery_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}], run_id=104)
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", finalize)

    sync_mod._run_full_sweep.__wrapped__("svc-1")
    finalize.assert_called_once()
    assert finalize.call_args.args[1] == 104


def test_high_scale_full_sweep_records_discovery_failure_on_originating_run(monkeypatch, stub_load_config):
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    log_run = _setup_discovery_env(monkeypatch, run_id=105)
    discover = MagicMock(side_effect=OSError("FOS listing unavailable"))
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", finalize)

    sync_mod._run_full_sweep.__wrapped__("svc-1")
    discover.assert_called_once_with("svc-1", run_id=105, originating_task="full_sync")
    assert log_run.call_args.args[3] == "error"
    assert log_run.call_args.kwargs["run_id"] == 105
    assert log_run.call_args.kwargs["error_message"] == "FOS listing unavailable"
    assert finalize.call_args.args[1] == 105


# ── Cron → ingest → metadata_db cascade (real ingest, moto S3) ───────────────


def test_run_log_discovery_cron_full_cascade_real_ingest_against_moto_s3(
    s3_mock, fos_source, monkeypatch, tmp_path, stub_progress, stub_usage_log_phase
):
    from backend.core import metadata as metadata_db
    from backend.core import storage as ice

    cache_path = str(tmp_path / "cache")
    os.makedirs(cache_path, exist_ok=True)
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _src: cache_path)
    monkeypatch.setattr("backend.core.duckdb._configure_fos", lambda *a, **kw: None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid})
    monkeypatch.setattr("backend.core.duckdb.DUCKDB_PATH", str(tmp_path / "test.duckdb"))

    class _CallerHintShim:
        def __init__(self, client):
            self._client = client

        def get_paginator(self, op, caller_hint=None):
            return self._client.get_paginator(op)

        def __getattr__(self, name):
            return getattr(self._client, name)

    monkeypatch.setattr("backend.core.ingest._get_fos_client", lambda _src: _CallerHintShim(s3_mock))
    if hasattr(ice, "_view_cache"):
        ice._view_cache.clear()
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: fos_source)

    base = datetime.now(UTC) - timedelta(hours=2)
    rows_per_file = 4
    seeded = [
        ("raw/request/year=2026/month=05/day=21/hour=10/minute=00/2026-05-21T10-00-00.svc.gz", base),
        (
            "raw/request/year=2026/month=05/day=21/hour=10/minute=05/2026-05-21T10-05-00.svc.gz",
            base + timedelta(minutes=5),
        ),
    ]
    for key, ts in seeded:
        rows = [
            {
                "timestamp": (ts + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S+0000"),
                "ip": f"10.0.0.{i}",
                "status": 200,
                "url": f"/c/{i}",
                "method": "GET",
            }
            for i in range(rows_per_file)
        ]
        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode="wb") as gz:
            gz.write(("\n".join(json.dumps(r) for r in rows) + "\n").encode())
        s3_mock.put_object(Bucket=fos_source["bucket"], Key=key, Body=body.getvalue())

    ice.init_storage_table(fos_source)
    svc_id, svc_name = fos_source["service_id"], fos_source["name"]
    sync_mod._run_log_discovery_cron.__wrapped__(svc_id, force=True)

    ingested_names = {row["file_name"] for row in metadata_db.list_ingested_files(svc_name)}
    for key, _ in seeded:
        assert any(name.endswith(key) for name in ingested_names)

    _total, runs = metadata_db.get_cron_runs(svc_name, task="log_discovery", per_page=10)
    assert runs and runs[0]["status"] == "success"
    assert runs[0].get("files_downloaded") == len(seeded)
    assert runs[0].get("rows_ingested") == rows_per_file * len(seeded)
    assert metadata_db.list_in_flight(svc_name) == []


# ── cron_run lifecycle (log_discovery) ───────────────────────────────────────


def _lifecycle_env(monkeypatch, events, *, log_text="LOGTEXT"):
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", MagicMock(return_value=_fake_src()))
    start, log_cron = MagicMock(return_value=42), MagicMock()
    order: list[Any] = []
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr(
        "backend.core.duckdb.finalize_cron_run_if_running",
        lambda *a, **k: order.append(("backstop", a, k)) or True,
    )
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda rid: order.append(("end_progress", rid)))
    monkeypatch.setattr("backend.cron.scheduler._extract_log_text", lambda rid: log_text)
    monkeypatch.setattr("backend.core.ingest.ingest", _make_ingest_events(events))
    return start, log_cron, order


def test_lifecycle_lease_held_skips_without_ingest_or_row(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    start, log_cron, order = _lifecycle_env(monkeypatch, [])
    start.side_effect = CronLeaseHeldError("held")
    ingest = MagicMock()
    monkeypatch.setattr("backend.core.ingest.ingest", ingest)

    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")
    ingest.assert_not_called()
    log_cron.assert_not_called()
    assert order == []


def test_lifecycle_backstop_gets_context_and_runs_before_end_progress(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    _, _, order = _lifecycle_env(monkeypatch, [{"type": "done", "new_files": 0, "rows_inserted": 0}])
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    assert [o[0] for o in order] == ["backstop", "end_progress"]
    _, args, kwargs = order[0]
    assert args[1:] == ("log_discovery", 42)
    assert kwargs["summary"] == "Sync exited without recording a terminal status"
    assert kwargs["error_message"] == "orphaned run auto-finalized (no terminal ingest event)"
    assert isinstance(kwargs["duration_s"], float)
    assert order[1] == ("end_progress", 42)


def test_lifecycle_error_event_row_shape(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    events = [
        {"type": "file_done", "current": 3, "total_inserted": 50, "total_corrupt": 1},
        {"type": "error", "message": "s3 timeout"},
    ]
    _, log_cron, _ = _lifecycle_env(monkeypatch, events)
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    errors = [c for c in log_cron.call_args_list if c.args[3] == "error"]
    assert len(errors) == 1
    kwargs = errors[0].kwargs
    assert kwargs["summary"] == "Ingestion failed after processing 3 files (50 rows)"
    assert kwargs["error_message"] == "s3 timeout"
    assert (kwargs["files_downloaded"], kwargs["rows_ingested"], kwargs["corrupt_rows"]) == (3, 50, 1)
    assert kwargs["log_output"] == "LOGTEXT" and kwargs["run_id"] == 42


def test_lifecycle_disk_failure_row_has_no_log_output(
    monkeypatch, stub_load_config, stub_progress, stub_post_ingest, stub_usage_log_phase
):
    _, log_cron, order = _lifecycle_env(monkeypatch, [])
    monkeypatch.setattr(sync_mod, "_check_disk_space", lambda cache_dir, sid, name: (False, "disk full"))
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Sync aborted: disk full" and kwargs["error_message"] == "disk full"
    assert kwargs["log_output"] is None and [o[0] for o in order] == ["backstop", "end_progress"]


def test_lifecycle_high_scale_missing_broker_row_has_no_log_output(monkeypatch, stub_load_config, stub_progress):
    _, log_cron, _ = _lifecycle_env(monkeypatch, [])
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "")
    sync_mod._run_log_discovery_cron.__wrapped__("svc-1")

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error"
    assert kwargs["summary"] == "Celery ingest misconfigured: no broker URL"
    assert kwargs["error_message"] == "DEPLOYMENT_MODE=high_scale requires CELERY_BROKER_URL"
    assert kwargs["log_output"] is None


# ── Merged from test_log_discovery_contract.py ───────────────────────────────


@pytest.fixture
def log_discovery_test_source(monkeypatch, tmp_path):
    service_id = "svc-log-discovery-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-log-discovery-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


def test_contract_1_manual_trigger_with_synthetic_gz_in_fos(log_discovery_test_source):
    service_id = log_discovery_test_source["service_id"]
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-contract-sync-123"

    app.dependency_overrides[get_source] = lambda: log_discovery_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.sync._run_log_discovery_cron"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = TestClient(app).post(f"/api/admin/sync/{service_id}", headers={"x-fastly-service-id": service_id})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True and body["run_id"] == "run-contract-sync-123"
        assert "started" in body["message"].lower() and started["task"] == "log_discovery"
    finally:
        app.dependency_overrides.clear()


def test_contract_2_cron_runs_records_success_and_nonzero_files_ingested(log_discovery_test_source):
    from backend.core import metadata as metadata_db

    service_id, source_name = log_discovery_test_source["service_id"], log_discovery_test_source["name"]
    run_id = metadata_db.start_cron_run(service_id, "log_discovery")
    assert run_id is not None

    metadata_db.log_cron_run(
        service_id,
        "log_discovery",
        duration_s=1.25,
        status="success",
        run_id=run_id,
        files_downloaded=5,
        rows_ingested=150,
        summary="Ingested 5 file(s) (150 rows) in 1.25s",
    )
    total, runs = metadata_db.get_cron_runs(source_name, task="log_discovery", per_page=5)
    assert total >= 1 and runs[0]["status"] == "success"
    assert (runs[0]["files_downloaded"], runs[0]["rows_ingested"]) == (5, 150)


def test_contract_3_usage_log_attributes_fos_class_a_list_and_class_b_get_to_cron_log_discovery(
    log_discovery_test_source, monkeypatch
):
    from backend.core import metadata as metadata_db
    from backend.core.metadata import usage_log_db
    from backend.utils.telemetry import get_process_context

    service_id = log_discovery_test_source["service_id"]
    observed: dict[str, object] = {}
    flush_contexts: list[str | None] = []

    def fake_ingest_followups(*args, **kwargs):
        observed["context"] = get_process_context()
        yield {"type": "done", "files": 0, "rows": 0, "corrupt": 0}

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: log_discovery_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 999)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron.jobs.sync._ingest_with_adaptive_followups", fake_ingest_followups)
    monkeypatch.setattr("backend.cron.jobs.sync._claim_heavy_refresh", lambda sid: False)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", lambda *a, **kw: None)
    monkeypatch.setattr(
        "backend.utils.telemetry.flush_usage_log", lambda sid: flush_contexts.append(get_process_context())
    )

    sync_mod._run_log_discovery_cron(service_id, force=True)
    assert observed["context"] == "cron.log_discovery" and flush_contexts == ["cron.log_discovery"]

    fos_calls = [
        {
            "method": "LISTOBJECTSV2",
            "service": "FOS",
            "details": "Class A · list",
            "url": "s3://test-bucket/raw/request/",
        },
        {
            "method": "GETOBJECT",
            "service": "FOS",
            "bytes": 65536,
            "details": "Class B · get",
            "url": "s3://test-bucket/raw/request/log.gz",
        },
    ]
    metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.log_discovery")
    rows = (
        usage_log_db.get_con(service_id)
        .execute(
            "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
            (service_id,),
        )
        .fetchall()
    )

    for op_type, expected_cls in [("LISTOBJECTSV2", "A"), ("GETOBJECT", "B")]:
        matched = [r for r in rows if r["operation_type"] == op_type]
        assert matched and matched[-1]["operation_class"] == expected_cls
        assert matched[-1]["process_context"] == "cron.log_discovery"


def test_contract_6_safety_gate_under_fla_dev_no_crons(log_discovery_test_source, monkeypatch, caplog):
    service_id = log_discovery_test_source["service_id"]
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[log_discovery_test_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=log_discovery_test_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
        assert f"log_discovery_{service_id}" not in [c.kwargs["id"] for c in add_job.call_args_list]

    caplog.set_level(logging.WARNING)
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        res = sync_mod._run_log_discovery_cron(service_id, force=True)
        get_src.assert_not_called()
        assert res is None or res.get("status") == "skipped"
    assert any("FLA_DEV_NO_CRONS=1" in record.message for record in caplog.records)


def test_contract_8_malformed_corrupt_caps_and_analyst_denial(log_discovery_test_source, tmp_path, monkeypatch):
    from backend.core.metadata.quarantine import list_quarantine_evidence

    from backend.core import quarantine

    service_id = log_discovery_test_source["service_id"]
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    payloads = [b'{"line": 1}\n', b'{"line": 2}\n', b'{"line": 3}\n']
    results = [
        quarantine.capture_evidence(
            service_id,
            "request",
            f"raw/request/part_{i}.log.gz",
            payload,
            line_ordinal=i,
            byte_offset=0,
            error_category="invalid_json",
            error_text="malformed json test",
        )
        for i, payload in enumerate(payloads, start=1)
    ]

    assert results[2]["cap_evictions"] == 1
    items = list_quarantine_evidence(service_id)
    remaining_ids = {item["id"] for item in items}
    assert len(items) == 2 and int(results[0]["id"]) not in remaining_ids and int(results[2]["id"]) in remaining_ids

    evidence_path = quarantine._evidence_path(service_id, int(results[2]["id"]))
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700

    _item, evidence_file = quarantine.read_evidence(service_id, int(results[2]["id"]))
    try:
        assert evidence_file.read() == payloads[2]
    finally:
        evidence_file.close()

    app.dependency_overrides[get_source] = lambda: log_discovery_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        client = TestClient(app)
        download_resp = client.get(f"/api/admin/quarantine/download/{results[2]['id']}")
        assert download_resp.status_code == 200 and download_resp.content == payloads[2]

        app.dependency_overrides[require_admin] = lambda: (_ for _ in ()).throw(
            HTTPException(status_code=403, detail={"error": "admin_only"})
        )
        denied_resp = client.get(f"/api/admin/quarantine/download/{results[2]['id']}")
        assert denied_resp.status_code == 403 and denied_resp.json()["detail"]["error"] == "admin_only"
    finally:
        app.dependency_overrides.clear()


# ── Merged from test_commit.py ───────────────────────────────────────────────


class TestCommitJob:
    @pytest.fixture
    def stub_load_config(self, monkeypatch):
        cfg = {
            "service_id": "svc-1",
            "name": "svc-1",
            "provisioning": {"cron_sync": {"enabled": True, "commit_interval_mins": 5}},
        }
        load = MagicMock(return_value=cfg)
        monkeypatch.setattr("backend.config.load_config", load)
        return {"load": load, "cfg": cfg}

    @pytest.mark.parametrize("case", ["config_missing", "source_missing", "disabled_sync"])
    def test_early_exit_gates(self, monkeypatch, stub_load_config, stub_source, case):
        start = MagicMock()
        monkeypatch.setattr("backend.core.duckdb.start_cron_run", start)
        if case == "config_missing":
            monkeypatch.setattr("backend.config.load_config", MagicMock(return_value=None))
            get_src = MagicMock()
            monkeypatch.setattr("backend.core.duckdb.get_source_for_service", get_src)
            commit._run_commit.__wrapped__("ghost-svc")
            get_src.assert_not_called()
        elif case == "source_missing":
            monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
            commit._run_commit.__wrapped__("svc-1")
        else:
            stub_load_config["cfg"]["provisioning"]["cron_sync"]["enabled"] = False
            commit._run_commit.__wrapped__("svc-1")
        start.assert_not_called()

    def test_disk_check_failure_logs_error_and_closes_progress(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.cron.jobs.commit._check_disk_space", lambda *a, **kw: (False, "disk 95% full"))
        cb = MagicMock()
        monkeypatch.setattr("backend.core.storage.commit_buffer", cb)

        commit._run_commit.__wrapped__("svc-1")
        cb.assert_not_called()
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error" and "disk 95% full" in kwargs["error_message"]
        assert "Commit aborted" in kwargs["summary"] and kwargs["log_output"] is None
        stub_cron_envelope["end_progress"].assert_called_once_with(33)

    def test_success_with_committed_files_logs_summary_and_refreshes_view(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.storage.commit_buffer",
            MagicMock(return_value={"files_committed": 2, "rows_committed": 1234, "snapshot_id": 8675309}),
        )
        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success" and kwargs["rows_ingested"] == 1234
        assert "1234 rows" in kwargs["summary"] and "8675309" in kwargs["summary"]

        from backend.cron.jobs import _common

        _common.refresh_view_and_warm_pool.assert_called_once()
        _common._refresh_status_after_commit.assert_called_once()
        assert _common._refresh_status_after_commit.call_args.args[0] == "svc-1"

    def test_success_no_files_committed_logs_no_new_data(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.storage.commit_buffer",
            MagicMock(return_value={"files_committed": 0, "rows_committed": 0, "snapshot_id": None}),
        )
        commit._run_commit.__wrapped__("svc-1")

        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success" and "No new data" in kwargs["summary"]
        from backend.cron.jobs import _common

        _common._refresh_status_after_commit.assert_not_called()

    def test_status_refresh_failure_does_not_fail_durable_commit(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.cron.jobs.commit._log_and_add_progress", stub_cron_envelope["log_event"])
        monkeypatch.setattr(
            "backend.core.storage.commit_buffer",
            MagicMock(return_value={"files_committed": 1, "rows_committed": 12, "snapshot_id": "ducklake"}),
        )
        monkeypatch.setattr(
            "backend.cron.jobs._common._refresh_status_after_commit",
            MagicMock(side_effect=RuntimeError("status unavailable")),
        )
        commit._run_commit.__wrapped__("svc-1")

        assert stub_cron_envelope["log"].call_args.args[3] == "success"
        assert any(
            call.kwargs.get("event", {}).get("type") == "warning"
            for call in stub_cron_envelope["log_event"].call_args_list
        )

    @pytest.mark.parametrize(
        ("result_dict", "expected_err", "expected_summary_sub", "expect_refresh"),
        [
            (
                {
                    "files_committed": 0,
                    "rows_committed": 0,
                    "snapshot_id": "ducklake",
                    "error": "write attach unavailable",
                },
                "write attach unavailable",
                "No buffer files committed",
                False,
            ),
            (
                {"files_committed": 5, "rows_committed": 100, "snapshot_id": 1, "quarantined_files": 2},
                "2 unreadable buffer file(s) quarantined",
                "quarantined 2",
                True,
            ),
            (
                {
                    "files_committed": 1,
                    "rows_committed": 12,
                    "snapshot_id": "ducklake",
                    "quarantined_files": 0,
                    "error": "later chunk attach failed",
                },
                "later chunk attach failed",
                "commit incomplete: later chunk attach failed",
                True,
            ),
        ],
    )
    def test_commit_buffer_incomplete_or_quarantined_outcomes(
        self,
        monkeypatch,
        stub_load_config,
        stub_source,
        stub_cron_envelope,
        result_dict,
        expected_err,
        expected_summary_sub,
        expect_refresh,
    ):
        monkeypatch.setattr("backend.cron.jobs.commit._log_and_add_progress", stub_cron_envelope["log_event"])
        monkeypatch.setattr("backend.core.storage.commit_buffer", MagicMock(return_value=result_dict))

        commit._run_commit.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error"
        assert expected_err in kwargs["error_message"]
        assert expected_summary_sub in kwargs["summary"]
        from backend.cron.jobs import _common

        assert _common.refresh_view_and_warm_pool.called is expect_refresh

    def test_high_scale_commit_uses_ledger_merge_and_scopes_lookup(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
        merge_calls = []
        monkeypatch.setattr("backend.core.ingest.merge_lake_files", lambda sid: merge_calls.append(("merge", sid)))
        monkeypatch.setattr(
            "backend.core.ingest.finalize_committed_raw",
            lambda sid: merge_calls.append(("finalize", sid)) or {"delete_after": False, "deleted": 0},
        )
        meta_con = MagicMock()
        meta_con.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=None)),
            MagicMock(fetchone=MagicMock(return_value=(4,))),
        ]
        monkeypatch.setattr("backend.core.metadata.base.get_con", lambda sid: meta_con)
        standard_commit = MagicMock()
        monkeypatch.setattr("backend.core.storage.commit_buffer", standard_commit)

        commit._run_commit.__wrapped__("svc-1")
        assert merge_calls == [("merge", "svc-1"), ("finalize", "svc-1")]
        standard_commit.assert_not_called()
        sql, params = meta_con.execute.call_args_list[0].args
        assert "service_id = ? AND task = 'commit'" in sql and params == ("svc-1",)
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "success" and "merged small lake files" in kwargs["summary"]
        assert kwargs["files_downloaded"] == 4
        from backend.cron.jobs import _common

        _common._refresh_status_after_commit.assert_not_called()

    def test_unexpected_exception_logged_as_error(self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope):
        monkeypatch.setattr(
            "backend.core.storage.commit_buffer", MagicMock(side_effect=RuntimeError("snapshot conflict"))
        )
        commit._run_commit.__wrapped__("svc-1")
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error" and "snapshot conflict" in kwargs["error_message"]
        stub_cron_envelope["end_progress"].assert_called_once()

    def test_lease_held_is_clean_skip_without_row_or_progress(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        stub_cron_envelope["start"].side_effect = CronLeaseHeldError("lease held")
        cb = MagicMock()
        monkeypatch.setattr("backend.core.storage.commit_buffer", cb)
        commit._run_commit.__wrapped__("svc-1")
        cb.assert_not_called()
        stub_cron_envelope["log"].assert_not_called()
        stub_cron_envelope["end_progress"].assert_not_called()

    def test_caller_supplied_run_id_is_reused_without_new_lease(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.storage.commit_buffer", MagicMock(return_value={"files_committed": 0, "rows_committed": 0})
        )
        commit._run_commit.__wrapped__("svc-1", run_id=77)
        stub_cron_envelope["start"].assert_not_called()
        assert stub_cron_envelope["log"].call_args.kwargs["run_id"] == 77
        stub_cron_envelope["end_progress"].assert_called_once_with(77)

    def test_exception_after_success_row_overwrites_with_error_row(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr(
            "backend.core.storage.commit_buffer",
            MagicMock(return_value={"files_committed": 1, "rows_committed": 5, "snapshot_id": 9}),
        )
        monkeypatch.setattr(
            "backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock(side_effect=RuntimeError("warm failed"))
        )
        commit._run_commit.__wrapped__("svc-1")
        assert [c.args[3] for c in stub_cron_envelope["log"].call_args_list] == ["success", "error"]
        last = stub_cron_envelope["log"].call_args
        assert last.kwargs["summary"] == "Buffer commit failed" and last.kwargs["error_message"] == "warm failed"

    def test_high_scale_failure_records_error_row_without_log_output(
        self, monkeypatch, stub_load_config, stub_source, stub_cron_envelope
    ):
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
        monkeypatch.setattr("backend.core.ingest.merge_lake_files", MagicMock(side_effect=RuntimeError("merge broke")))
        commit._run_commit.__wrapped__("svc-1")
        stub_cron_envelope["log"].assert_called_once()
        args, kwargs = stub_cron_envelope["log"].call_args
        assert args[3] == "error" and kwargs["summary"] == "DuckLake merge / raw finalization failed"
        assert kwargs["error_message"] == "merge broke" and kwargs["log_output"] is None and kwargs["run_id"] == 33


# ── Merged from tests/cron/test_full_sync_and_gap_heal.py ──


def _sync_scheduler(
    cfg: dict, src: dict | None = None, *, high_scale: bool = False, alerts: int = 0, sched: Scheduler | None = None
) -> Scheduler:
    s = sched or Scheduler()
    if sched is None:
        s._sched = MagicMock()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src or cfg),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_scale_mode", return_value=high_scale),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=alerts),
    ):
        s._sync_jobs()
    return s


def _assert_analyst_and_ro_rejected(client: TestClient, path: str, service_id: str, base_src: dict) -> None:
    def reject_analyst():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_analyst
    assert client.post(path, headers={"x-fastly-service-id": service_id}).status_code == 403


# ── Full sync contract ───────────────────────────────────────────────────────


@pytest.fixture
def full_sync_test_source(monkeypatch, tmp_path):
    service_id = "svc-full-sync-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "bucket": "test-full-sync-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {
            "cron_sync": {"enabled": True},
            "cron_full_sweep": {"enabled": True},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    return src


def test_contract_1_manual_trigger_endpoint(full_sync_test_source):
    service_id = full_sync_test_source["service_id"]
    started = {}
    app.dependency_overrides[get_source] = lambda: full_sync_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        client = TestClient(app)
        with (
            patch(
                "backend.core.duckdb.start_cron_run",
                side_effect=lambda src, task: started.setdefault("task", task) and "run-full-sweep-contract-456",
            ),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.sync._run_full_sweep"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/full-sweep/{service_id}?force=true", headers={"x-fastly-service-id": service_id}
            )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["ok"] is True and body["run_id"] == "run-full-sweep-contract-456"
        assert "started" in body["message"].lower() and started["task"] == "full_sync"
        _assert_analyst_and_ro_rejected(
            client, f"/api/admin/full-sweep/{service_id}", service_id, full_sync_test_source
        )
    finally:
        app.dependency_overrides.clear()


def test_contract_2_cron_runs_records_success_and_outcome_counters(full_sync_test_source):
    service_id, source_name = full_sync_test_source["service_id"], full_sync_test_source["name"]
    run_id = metadata_db.start_cron_run(service_id, "full_sync")
    assert run_id is not None
    outcome_counters = {
        "valid_records": 1000,
        "malformed_records": 0,
        "corrupt_containers": 0,
        "quarantine_capture_failures": 0,
        "source_delete_failures": 0,
        "objects_processed": 5,
        "objects_successful": 5,
        "objects_partial": 0,
        "objects_failed": 0,
    }
    metadata_db.log_cron_run(
        service_id,
        "full_sync",
        duration_s=2.5,
        status="success",
        run_id=run_id,
        files_downloaded=5,
        rows_ingested=1000,
        summary="Backfilled 5 late-arriving file(s) (1,000 rows) in 2.50s",
        outcome_counters=outcome_counters,
    )
    total, runs = metadata_db.get_cron_runs(source_name, task="full_sync", per_page=5)
    assert total >= 1
    latest = runs[0]
    assert (latest["status"], latest["files_downloaded"], latest["rows_ingested"]) == ("success", 5, 1000)
    assert "Backfilled 5" in latest["summary"]


def test_contract_3_usage_log_attribution_cron_full_sync(full_sync_test_source, monkeypatch):
    service_id = full_sync_test_source["service_id"]
    observed: dict[str, object] = {}
    flush_contexts: list[str | None] = []

    def fake_ingest(*args, **kwargs):
        observed["context"] = get_process_context()
        yield {"type": "done", "new_files": 0, "rows_inserted": 0, "corrupt_rows": 0}

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: full_sync_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 888)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: None)
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", lambda *a, **kw: None)
    monkeypatch.setattr("backend.core.ingest.ingest", fake_ingest)
    monkeypatch.setattr("backend.cron_progress.start_progress", lambda *a, **kw: None)
    monkeypatch.setattr("backend.cron_progress.end_progress", lambda *a, **kw: None)
    monkeypatch.setattr(
        "backend.utils.telemetry.flush_usage_log", lambda sid: flush_contexts.append(get_process_context())
    )

    sync_mod._run_full_sweep(service_id, force=True)
    assert observed["context"] == "cron.full_sync" and flush_contexts == ["cron.full_sync"]

    fos_calls = [
        {
            "method": "LISTOBJECTSV2",
            "service": "FOS",
            "details": "Class A · list",
            "url": "s3://test-full-sync-bucket/raw/request/",
        }
    ]
    metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.full_sync")
    rows = (
        usage_log_db.get_con(service_id)
        .execute(
            "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
            (service_id,),
        )
        .fetchall()
    )
    list_rows = [r for r in rows if r["operation_type"] == "LISTOBJECTSV2"]
    assert list_rows and (list_rows[-1]["operation_class"], list_rows[-1]["process_context"]) == (
        "A",
        "cron.full_sync",
    )


def test_contract_4_safety_gate_under_fla_dev_no_crons(full_sync_test_source, monkeypatch, caplog):
    service_id = full_sync_test_source["service_id"]
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[full_sync_test_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=full_sync_test_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
        assert f"full_sync_{service_id}" not in [c.kwargs["id"] for c in add_job.call_args_list]

    caplog.set_level(logging.WARNING)
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        assert sync_mod._run_full_sweep(service_id, force=True) is None
        get_src.assert_not_called()
    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


def _patch_full_sweep_env(monkeypatch, src, *, defer: bool = False, run_id: int = 777):
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: defer)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    start_cron = MagicMock(return_value=run_id)
    log_cron = MagicMock()
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", finalize)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    return start_cron, log_cron, finalize


def test_contract_5_to_8_deferral_budget_high_scale_and_corrupt_rows(full_sync_test_source, monkeypatch):
    service_id = full_sync_test_source["service_id"]
    start_cron, log_cron, finalize = _patch_full_sweep_env(monkeypatch, full_sync_test_source, defer=True, run_id=999)
    mock_ingest = MagicMock(side_effect=lambda *a, **k: iter([{"type": "done", "new_files": 0, "rows_inserted": 0}]))
    monkeypatch.setattr("backend.core.ingest.ingest", mock_ingest)

    # Contract 5: Active-request deferral & force bypass
    sync_mod._run_full_sweep(service_id, force=False)
    start_cron.assert_not_called()
    sync_mod._run_full_sweep(service_id, force=True)
    start_cron.assert_called_once()

    # Contract 6: Adaptive budget scaling
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    for file_count, total_bytes, exp_files, exp_sec in (
        (3000, 100_000_000, 5000, 300),
        (50, 1_000_000, 50000, 1200),
    ):
        monkeypatch.setattr(
            "backend.core.storage.buffer_backlog_stats",
            lambda src, fc=file_count, tb=total_bytes: {"file_count": fc, "total_bytes": tb},
        )
        sync_mod._run_full_sweep(service_id, force=True)
        assert (mock_ingest.call_args.kwargs["max_files"], mock_ingest.call_args.kwargs["max_seconds"]) == (
            exp_files,
            exp_sec,
        )

    # Contract 8: Corrupt rows & quarantine outcome counters
    counters = {
        "valid_records": 100,
        "malformed_records": 5,
        "corrupt_containers": 1,
        "quarantine_capture_failures": 0,
        "source_delete_failures": 0,
        "objects_processed": 2,
        "objects_successful": 1,
        "objects_partial": 0,
        "objects_failed": 1,
    }
    monkeypatch.setattr("backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock())
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        lambda *a, **k: iter(
            [
                {
                    "type": "done",
                    "new_files": 2,
                    "rows_inserted": 100,
                    "corrupt_rows": 6,
                    "outcome_counters": counters,
                    "corrupt_details": ["bad line at offset 42"],
                }
            ]
        ),
    )
    log_cron.reset_mock()
    sync_mod._run_full_sweep(service_id, force=True)
    args, kwargs = log_cron.call_args
    assert args[3] == "error" and kwargs.get("outcome_counters") == counters and kwargs.get("corrupt_rows") == 6
    assert kwargs.get("error_message") == "bad line at offset 42" and "quarantined" in kwargs.get("summary", "").lower()

    # Contract 7: High-Scale mode prefix discovery attribution
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr(
        "backend.celery_status.celery_queue_depths", lambda: ({"fastly:discovery": 6000, "fastly:convert": 100}, True)
    )
    discover = MagicMock(return_value=12)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)
    log_cron.reset_mock()
    finalize.reset_mock()
    sync_mod._run_full_sweep(service_id, force=True)
    discover.assert_called_once_with(service_id, run_id=999, originating_task="full_sync")
    assert log_cron.call_args.args[3] == "success" and log_cron.call_args.kwargs["files_downloaded"] == 12
    assert "12 unseen" in log_cron.call_args.kwargs["summary"] and finalize.call_args.args[1] == 999


def test_contract_9_scheduler_registration_reschedule_and_disabling(full_sync_test_source):
    service_id = full_sync_test_source["service_id"]
    sched = _sync_scheduler(full_sync_test_source)
    assert f"full_sync_{service_id}" in sched._job_ids
    add_call = next(c for c in sched._sched.add_job.call_args_list if c.kwargs.get("id") == f"full_sync_{service_id}")
    assert (
        add_call.args[1] == "cron"
        and add_call.kwargs.get("hour") == "3,9,15,21"
        and add_call.kwargs.get("minute") == 30
    )

    resched_src = {
        **full_sync_test_source,
        "provisioning": {
            **full_sync_test_source["provisioning"],
            "cron_full_sweep": {"enabled": True, "cron_hours": "1,13", "cron_minute": 15},
        },
    }
    full_sweep_job = MagicMock()
    sched._sched.get_job = MagicMock(
        side_effect=lambda jid: full_sweep_job if jid == f"full_sync_{service_id}" else MagicMock()
    )
    _sync_scheduler(resched_src, sched=sched)
    full_sweep_job.reschedule.assert_called_once_with("cron", hour="1,13", minute=15)

    disabled_src = {
        **full_sync_test_source,
        "provisioning": {**full_sync_test_source["provisioning"], "cron_full_sweep": {"enabled": False}},
    }
    assert f"full_sync_{service_id}" not in _sync_scheduler(disabled_src)._job_ids


# ── Gap heal contract ────────────────────────────────────────────────────────


@pytest.fixture
def gap_heal_test_source(monkeypatch, tmp_path):
    service_id = "svc-gap-heal-contract"
    cache_root = tmp_path / "cache" / service_id
    cache_root.mkdir(parents=True, exist_ok=True)
    src = {
        "name": service_id,
        "service_id": service_id,
        "service_name": service_id,
        "logging_service_id": "log-svc-gap-contract",
        "bucket": "test-gap-heal-bucket",
        "_cache_dir_override": str(cache_root),
        "provisioning": {
            "cron_sync": {"enabled": True},
            "cron_gap_heal": {"enabled": True, "interval_minutes": 30},
        },
    }
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src if sid == service_id else None)
    monkeypatch.setattr("backend.config.load_config", lambda sid: src if sid == service_id else None)
    sync_mod._GAP_HEAL_LAST_TRIGGER.pop(service_id, None)
    return src


@contextlib.contextmanager
def _gap_heal_ctx(*, run_id: int = 888, accounting_return=None, accounting_side_effect=None):
    start_calls, log_calls, full_sweep_calls = [], [], []
    acct_mock = (
        MagicMock(side_effect=accounting_side_effect)
        if accounting_side_effect is not None
        else MagicMock(return_value=accounting_return or {"sustained_loss": None, "buckets": [], "totals": None})
    )
    with (
        patch(
            "backend.core.duckdb.start_cron_run",
            side_effect=lambda *a, **k: start_calls.append(a) or run_id,
        ),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron.jobs._common.finalize_cron_duration"),
        patch(
            "backend.cron.jobs.sync._run_full_sweep",
            side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
        ),
        patch("backend.routers.admin.compute_log_accounting", acct_mock),
    ):
        yield start_calls, log_calls, full_sweep_calls


class TestGapHealContract:
    def test_contract_1_manual_trigger_endpoint(self, gap_heal_test_source):
        service_id = gap_heal_test_source["service_id"]
        started = {}
        app.dependency_overrides[get_source] = lambda: gap_heal_test_source
        app.dependency_overrides[get_service_id] = lambda: service_id
        try:
            client = TestClient(app)
            with (
                patch(
                    "backend.core.duckdb.start_cron_run",
                    side_effect=lambda src, task: started.setdefault("task", task) and "run-gap-heal-contract-101",
                ),
                patch("backend.cron_progress.start_progress"),
                patch("backend.cron.jobs.sync._run_gap_heal"),
                patch("backend.repositories.dashboard.invalidate_service"),
            ):
                resp = client.post(
                    f"/api/admin/gap-heal/{service_id}?force=true", headers={"x-fastly-service-id": service_id}
                )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert (
                body["ok"] is True and body["run_id"] == "run-gap-heal-contract-101" and started["task"] == "gap_heal"
            )

            with (
                patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
                patch(
                    "backend.cron_progress.list_active_runs",
                    return_value=[{"service_id": service_id, "task": "gap_heal", "run_id": "run-gap-heal-active-202"}],
                ),
                patch("backend.repositories.dashboard.invalidate_service"),
            ):
                body_busy = client.post(
                    f"/api/admin/gap-heal/{service_id}", headers={"x-fastly-service-id": service_id}
                ).json()
            assert (
                body_busy["ok"] is True
                and body_busy["run_id"] == "run-gap-heal-active-202"
                and "already running" in body_busy["message"].lower()
            )
            _assert_analyst_and_ro_rejected(
                client, f"/api/admin/gap-heal/{service_id}", service_id, gap_heal_test_source
            )
        finally:
            app.dependency_overrides.clear()

    def test_contract_2_run_gap_heal_accepts_run_id_and_force(self, gap_heal_test_source):
        service_id = gap_heal_test_source["service_id"]
        with _gap_heal_ctx() as (start_calls, log_calls, _):
            sync_mod._run_gap_heal(service_id, run_id=777, force=True)
        assert not start_calls and len(log_calls) == 1
        args, kwargs = log_calls[0]
        assert kwargs.get("run_id") == 777 and args[3] == "success" and "No sustained loss" in kwargs["summary"]

    @pytest.mark.parametrize(
        "gap_pct,lost_lines,expected_band,exp_files,exp_seconds",
        [
            (0.06, 1_000, "mild", 20_000, 900),
            (0.15, 12_000, "elevated", 20_000, 900),
            (0.55, 120_000, "severe", 50_000, 1500),
            (0.85, 600_000, "critical", 100_000, 1800),
        ],
    )
    def test_contract_3_sustained_loss_triggers_full_sweep_with_adaptive_budget(
        self, gap_heal_test_source, gap_pct, lost_lines, expected_band, exp_files, exp_seconds
    ):
        service_id = gap_heal_test_source["service_id"]
        sync_mod._GAP_HEAL_LAST_TRIGGER.clear()
        sustained = SustainedLossAlert(
            started_at="2026-10-08T10:00:00Z", n_buckets=3, max_gap_pct=gap_pct, total_lost_lines=lost_lines
        )
        with _gap_heal_ctx(accounting_return={"sustained_loss": sustained, "buckets": [], "totals": None}) as (
            _,
            log_calls,
            full_sweep_calls,
        ):
            sync_mod._run_gap_heal(service_id, force=True)

        assert len(full_sweep_calls) == 1
        sid, kw = full_sweep_calls[0]
        assert (sid, kw["max_files"], kw["max_seconds"], kw.get("force")) == (service_id, exp_files, exp_seconds, True)
        args, kwargs = log_calls[0]
        assert args[3] == "warning" and f"severity={expected_band}" in kwargs["summary"]

    def test_contract_4_and_5_throttle_cooldowns_and_error_handling(self, gap_heal_test_source):
        service_id = gap_heal_test_source["service_id"]
        sustained_elevated = SustainedLossAlert(
            started_at="2026-10-08T09:00:00Z", n_buckets=2, max_gap_pct=0.15, total_lost_lines=15_000
        )
        sync_mod._GAP_HEAL_LAST_TRIGGER[service_id] = time.time() - 3600

        with _gap_heal_ctx(accounting_return={"sustained_loss": sustained_elevated, "buckets": [], "totals": None}) as (
            _,
            log_calls,
            full_sweep_calls,
        ):
            sync_mod._run_gap_heal(service_id, force=False)
            assert not full_sweep_calls and "throttled" in log_calls[0][1]["summary"]
            sync_mod._run_gap_heal(service_id, force=True)
            assert len(full_sweep_calls) == 1 and "triggering full_sweep" in log_calls[1][1]["summary"]

        for exc, exp_status, exp_substr in (
            (
                HTTPException(status_code=502, detail={"error": "fastly_stats_failed"}),
                "success",
                "Fastly stats unavailable",
            ),
            (RuntimeError("Database query timed out"), "error", "crashed"),
        ):
            with _gap_heal_ctx(accounting_side_effect=exc) as (_, log_calls, _):
                sync_mod._run_gap_heal(service_id)
            assert log_calls[0][0][3] == exp_status and exp_substr.lower() in log_calls[0][1]["summary"].lower()

    def test_contract_6_to_9_deferral_kill_switch_telemetry_and_scheduler(self, gap_heal_test_source, monkeypatch):
        service_id = gap_heal_test_source["service_id"]

        # Contract 6: Politeness deferral
        with (
            patch("backend.utils.active_requests.should_defer_cron", return_value=True),
            _gap_heal_ctx() as (
                start_calls,
                _,
                _,
            ),
        ):
            sync_mod._run_gap_heal(service_id, force=False)
            assert not start_calls
            sync_mod._run_gap_heal(service_id, force=True)
            assert len(start_calls) == 1

        # Contract 7: Kill switch
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
        with _gap_heal_ctx() as (start_calls, _, _):
            sync_mod._run_gap_heal(service_id, force=True)
            assert not start_calls

        # Contract 8: Telemetry process_context
        monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
        captured_context = []
        with (
            patch(
                "backend.core.duckdb.get_source_for_service",
                return_value={"name": "svc-ctx"},
            ),
            _gap_heal_ctx(accounting_side_effect=lambda sid, *a, **k: captured_context.append(get_process_context())),
        ):
            sync_mod._run_gap_heal("svc-ctx")
        assert captured_context == ["gap_heal"]

        # Contract 9: Scheduler registration
        cfg = {
            "service_id": "svc-sched-gap",
            "log_period": 60,
            "logging_service_id": "log-svc-sched",
            "provisioning": {
                "cron_sync": {"enabled": True},
                "cron_gap_heal": {"enabled": True, "interval_minutes": 45},
            },
        }
        src = {"name": "svc-sched-gap"}
        assert "gap_heal_svc-sched-gap" in _sync_scheduler(cfg, src, alerts=1)._job_ids
        cfg_disabled = {**cfg, "provisioning": {"cron_sync": {"enabled": True}, "cron_gap_heal": {"enabled": False}}}
        assert "gap_heal_svc-sched-gap" not in _sync_scheduler(cfg_disabled, src, alerts=1)._job_ids


# ── Ledger sweep contract ────────────────────────────────────────────────────

SERVICE_ID = "svc_test_ledger_sweep"
FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "deployment_mode": "high_scale",
    "provisioning": {"cron_ledger_sweep": {"enabled": True, "interval_minutes": 15}},
}
FAKE_SRC = {**FAKE_CFG, "bucket": "test-sweep-bucket"}


def test_ledger_sweep_manual_trigger_and_quarantine_endpoints(monkeypatch):
    app.dependency_overrides[get_source] = lambda: FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    client = TestClient(app)
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value="run-sweep-101"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_ledger._run_ledger_sweep"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            resp = client.post(
                f"/api/admin/ledger/sweep/{SERVICE_ID}", headers={"x-fastly-service-id": SERVICE_ID}
            ).json()
        assert resp["ok"] is True and resp["run_id"] == "run-sweep-101" and "started" in resp["message"].lower()

        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
            patch(
                "backend.cron_progress.list_active_runs",
                return_value=[{"service_id": SERVICE_ID, "task": "ledger_sweep", "run_id": "run-sweep-active"}],
            ),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            busy = client.post(
                f"/api/admin/ledger/sweep/{SERVICE_ID}", headers={"x-fastly-service-id": SERVICE_ID}
            ).json()
        assert (
            busy["ok"] is True and busy["run_id"] == "run-sweep-active" and "already running" in busy["message"].lower()
        )
    finally:
        app.dependency_overrides.clear()

    assert_manual_trigger_standard_mode_rejected(f"/api/admin/ledger/sweep/{SERVICE_ID}", SERVICE_ID, FAKE_SRC)
    assert_manual_trigger_non_admin_rejected(f"/api/admin/ledger/sweep/{SERVICE_ID}", SERVICE_ID, FAKE_SRC)

    # Quarantine endpoint tests
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    mock_items = [
        {
            "id": 1,
            "source_type": "request",
            "original_key": "raw/2026/08/27/10/05/bad.json.gz",
            "line_ordinal": 42,
            "byte_offset": 100,
            "byte_length": 256,
            "error_category": "json_parse_error",
            "error_text": "Unexpected token",
            "sha256": "abcdef1234567890",
            "quarantined_at": "2026-08-27T10:05:00+00:00",
        }
    ]
    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", return_value=mock_items),
        patch("backend.routers.admin.quarantine.count_quarantine_evidence", return_value=1),
    ):
        q_body = client.get(f"/api/admin/ledger/quarantine?service_id={SERVICE_ID}").json()
    assert q_body["total"] == 1 and q_body["items"][0]["line_ordinal"] == 42

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)
    assert client.get("/api/admin/ledger/quarantine?service_id=nonexistent-svc").status_code == 404

    app.dependency_overrides[require_admin] = lambda: (_ for _ in ()).throw(
        HTTPException(status_code=403, detail={"error": "admin_only"})
    )
    try:
        assert client.get(f"/api/admin/ledger/quarantine?service_id={SERVICE_ID}").status_code == 403
    finally:
        app.dependency_overrides.pop(require_admin, None)


def _setup_ledger_sweep_env(monkeypatch, *, run_id=888, sweep_fn=None):
    start_cron_calls, log_calls, finalize_calls = [], [], []
    start_progress, end_progress = MagicMock(), MagicMock()
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a: start_cron_calls.append(a) or run_id)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: log_calls.append((a, k)))
    monkeypatch.setattr(
        "backend.cron.jobs._common.finalize_cron_duration", lambda *a, **k: finalize_calls.append((a, k))
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())
    if sweep_fn is not None:
        monkeypatch.setattr("backend.core.ingest.sweep_ledger_once", sweep_fn)
    return start_cron_calls, log_calls, finalize_calls, start_progress, end_progress


def test_run_ledger_sweep_lifecycle_outcomes(monkeypatch):
    # 1. Success
    sweep_calls = []
    _, log_calls, finalize_calls, start_progress, end_progress = _setup_ledger_sweep_env(
        monkeypatch,
        run_id=888,
        sweep_fn=lambda sid, run_id=None: (
            sweep_calls.append((sid, run_id))
            or {"reclaimed": 5, "redispatched": 5, "discovered": 10, "broker_ok": True, "dead_letter": 0}
        ),
    )
    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    start_progress.assert_called_once_with(888, service_id=SERVICE_ID, task="ledger_sweep")
    end_progress.assert_called_once_with(888)
    assert sweep_calls == [(SERVICE_ID, 888)] and len(finalize_calls) == 1
    args, kwargs = log_calls[0]
    assert (args[3], kwargs.get("run_id"), kwargs.get("files_downloaded")) == ("success", 888, 10)
    assert "reclaimed=5 redispatched=5 discovered=10" in kwargs.get("summary", "")

    # 2. run_id reuse
    start_cron_calls, log_calls, _, _, _ = _setup_ledger_sweep_env(
        monkeypatch,
        sweep_fn=lambda sid, run_id=None: {
            "reclaimed": 0,
            "redispatched": 0,
            "discovered": 0,
            "broker_ok": True,
            "dead_letter": 0,
        },
    )
    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID, run_id=999)
    assert not start_cron_calls and log_calls[0][1].get("run_id") == 999

    # 3. Warning on dead-letter or broker down
    _, log_calls, _, _, _ = _setup_ledger_sweep_env(
        monkeypatch,
        run_id=889,
        sweep_fn=lambda sid, run_id=None: {
            "reclaimed": 2,
            "redispatched": 0,
            "discovered": 0,
            "broker_ok": False,
            "dead_letter": 3,
        },
    )
    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    args, kwargs = log_calls[0]
    assert args[3] == "warning" and "Celery broker/queue depth probe failed" in kwargs.get("summary", "")
    assert "3 dead-letter/quarantined row(s)" in kwargs.get("summary", "")

    # 4. Error handling and guaranteed cleanup
    _, log_calls, finalize_calls, _, end_progress = _setup_ledger_sweep_env(
        monkeypatch, run_id=890, sweep_fn=MagicMock(side_effect=RuntimeError("Postgres connection lost"))
    )
    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    end_progress.assert_called_once_with(890)
    assert len(finalize_calls) == 1 and log_calls[0][0][3] == "error"
    assert "Postgres connection lost" in log_calls[0][1].get("error_message", "")


@pytest.mark.parametrize("mode", ["standard", "dev_no_crons", "missing_cfg"])
def test_run_ledger_sweep_skips_when_guarded(monkeypatch, mode):
    start_calls, _, _, _, _ = _setup_ledger_sweep_env(monkeypatch)
    if mode == "standard":
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    elif mode == "dev_no_crons":
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    elif mode == "missing_cfg":
        monkeypatch.setattr("backend.config.load_config", lambda sid: None)
    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert not start_calls


def test_scheduler_registers_reschedules_and_skips_ledger_sweep():
    sid = "svc-scheduler-sweep"
    cfg = {
        "service_id": sid,
        "name": sid,
        "log_period": 60,
        "deployment_mode": "high_scale",
        "provisioning": {
            "cron_sync": {"enabled": True},
            "cron_ledger_sweep": {"enabled": True, "interval_minutes": 15},
        },
    }
    src = {
        "name": sid,
        "service_id": sid,
        "bucket": "test-b",
        "deployment_mode": "high_scale",
    }
    job_id = f"ledger_sweep_{sid}"

    s = _sync_scheduler(cfg, src, high_scale=True)
    assert job_id in s._job_ids
    call = next(c for c in s._sched.add_job.call_args_list if c.kwargs.get("id") == job_id)
    assert call.args[:2] == (ledger_mod._run_ledger_sweep, "interval") and call.kwargs["minutes"] == 15
    assert call.kwargs["coalesce"] is True and call.kwargs["misfire_grace_time"] == 300 and call.kwargs["args"] == [sid]

    cfg["provisioning"]["cron_ledger_sweep"]["interval_minutes"] = 30
    mock_job = MagicMock()
    s._sched.get_job = MagicMock(side_effect=lambda jid: mock_job if jid == job_id else None)
    _sync_scheduler(cfg, src, high_scale=True, sched=s)
    mock_job.reschedule.assert_called_once_with("interval", minutes=30)

    cfg["provisioning"]["cron_ledger_sweep"]["enabled"] = False
    assert job_id not in _sync_scheduler(cfg, src, high_scale=True)._job_ids


@pytest.mark.parametrize(
    "queue_depth,expected_redispatched",
    [(0, 1), (100, 0)],
    ids=["reclaims_and_dispatches_excluding_rum", "queue_depth_guard_skips_dispatch"],
)
def test_sweep_ledger_reclaim_and_queue_depth_guard(monkeypatch, queue_depth, expected_redispatched):
    sid = "svc-test-reclaim"
    log_key = "raw/2026/08/27/10/05/logs.json.gz"
    executed_statements: list[tuple[str, object]] = []

    class FakeCursor:
        def execute(self, query, params=None):
            executed_statements.append((query, params))

        def fetchall(self):
            return [(log_key,)]

    class FakeCon:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def execute(self, query, params=None):
            executed_statements.append((query, params))
            mock_res = MagicMock()
            mock_res.fetchone.return_value = (0,)
            mock_res.fetchall.return_value = []
            return mock_res

    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda s: FakeCon())
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s: {"prefix": "", "bucket": "b"})
    monkeypatch.setattr("backend.core.ingest.discover_prefix", lambda *a, **k: 0)
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": queue_depth}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_files.delay", mock_convert)

    summary = sweep_ledger_once(sid)
    assert (summary["reclaimed"], summary["redispatched"]) == (1, expected_redispatched)
    if expected_redispatched:
        mock_convert.assert_called_once_with(sid, [log_key])
        update_stmts = [stmt for stmt in executed_statements if "UPDATE ingest_ledger" in stmt[0]]
        assert len(update_stmts) == 1
        assert "object_key NOT LIKE ?" in update_stmts[0][0] and "raw/rum/%" in update_stmts[0][1]
    else:
        mock_convert.assert_not_called()
