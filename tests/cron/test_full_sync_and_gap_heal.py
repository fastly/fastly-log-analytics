"""Consolidated tests for test_full_sync_and_gap_heal."""

from __future__ import annotations

import contextlib
import logging
import time
from unittest.mock import MagicMock, patch

import pytest
from backend.deps import get_service_id, get_source, require_admin
from fastapi import HTTPException
from starlette.testclient import TestClient

from backend.core import metadata as metadata_db
from backend.core.ingest import sweep_ledger_once
from backend.core.metadata import usage_log_db
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.cron.jobs import rum_ledger as ledger_mod
from backend.cron.jobs import sync as sync_mod
from backend.cron.scheduler import Scheduler
from backend.main import app
from backend.routers.admin import SustainedLossAlert
from backend.utils.telemetry import get_process_context
from tests.cron.conftest import (
    assert_manual_trigger_non_admin_rejected,
    assert_manual_trigger_read_only_rejected,
    assert_manual_trigger_standard_mode_rejected,
)


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

    app.dependency_overrides[get_source] = lambda: {**base_src, "access_level": "read_only"}
    app.dependency_overrides.pop(require_admin, None)
    ro_resp = client.post(path, headers={"x-fastly-service-id": service_id})
    assert ro_resp.status_code == 403 and "read-only" in ro_resp.json()["detail"].lower()


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
        "access_level": "read_write",
        "provisioning": {
            "access_level": "read_write",
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
            "backend.core.iceberg.buffer_backlog_stats",
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
        "access_level": "read_write",
        "provisioning": {
            "access_level": "read_write",
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
                return_value={"name": "svc-ctx", "access_level": "read_write"},
            ),
            _gap_heal_ctx(accounting_side_effect=lambda sid, *a, **k: captured_context.append(get_process_context())),
        ):
            sync_mod._run_gap_heal("svc-ctx")
        assert captured_context == ["gap_heal"]

        # Contract 9: Scheduler registration
        cfg = {
            "service_id": "svc-sched-gap",
            "log_period": 60,
            "access_level": "read_write",
            "logging_service_id": "log-svc-sched",
            "provisioning": {
                "cron_sync": {"enabled": True},
                "cron_gap_heal": {"enabled": True, "interval_minutes": 45},
            },
        }
        src = {"name": "svc-sched-gap", "access_level": "read_write"}
        assert "gap_heal_svc-sched-gap" in _sync_scheduler(cfg, src, alerts=1)._job_ids
        cfg_disabled = {**cfg, "provisioning": {"cron_sync": {"enabled": True}, "cron_gap_heal": {"enabled": False}}}
        assert "gap_heal_svc-sched-gap" not in _sync_scheduler(cfg_disabled, src, alerts=1)._job_ids


# ── Ledger sweep contract ────────────────────────────────────────────────────

SERVICE_ID = "svc_test_ledger_sweep"
FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "deployment_mode": "high_scale",
    "access_level": "read_write",
    "provisioning": {"access_level": "read_write", "cron_ledger_sweep": {"enabled": True, "interval_minutes": 15}},
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
    assert_manual_trigger_read_only_rejected(f"/api/admin/ledger/sweep/{SERVICE_ID}", SERVICE_ID, FAKE_SRC)
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

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service", lambda sid: {**FAKE_SRC, "access_level": "read_only"}
    )
    assert client.get(f"/api/admin/ledger/quarantine?service_id={SERVICE_ID}").status_code == 403

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


@pytest.mark.parametrize("mode", ["standard", "read_only", "dev_no_crons", "missing_cfg"])
def test_run_ledger_sweep_skips_when_guarded(monkeypatch, mode):
    start_calls, _, _, _, _ = _setup_ledger_sweep_env(monkeypatch)
    if mode == "standard":
        monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    elif mode == "read_only":
        monkeypatch.setattr(
            "backend.core.duckdb.get_source_for_service", lambda sid: {**FAKE_SRC, "access_level": "read_only"}
        )
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
        "access_level": "read_write",
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
            "cron_ledger_sweep": {"enabled": True, "interval_minutes": 15},
        },
    }
    src = {
        "name": sid,
        "service_id": sid,
        "bucket": "test-b",
        "access_level": "read_write",
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
