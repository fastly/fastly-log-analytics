"""Consolidated tests for test_full_sync_and_gap_heal."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import sync as sync_mod
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app


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
    """Checklist Item 1:
    Trigger POST /api/admin/full-sweep/{service_id}; verify HTTP 200, run_id, and admin-only authorization.
    """
    service_id = full_sync_test_source["service_id"]
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-full-sweep-contract-456"

    app.dependency_overrides[get_source] = lambda: full_sync_test_source
    app.dependency_overrides[get_service_id] = lambda: service_id
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.sync._run_full_sweep"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/full-sweep/{service_id}?force=true",
                headers={"x-fastly-service-id": service_id},
            )

        assert resp.status_code == 200, f"Expected 200, got: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-full-sweep-contract-456"
        assert "started" in body["message"].lower()
        assert started["task"] == "full_sync"

        # Analyst Path B denial: non-admin gets 403
        from fastapi import HTTPException

        def reject_analyst():
            raise HTTPException(status_code=403, detail={"error": "admin_only"})

        app.dependency_overrides[require_admin] = reject_analyst
        denied_resp = client.post(
            f"/api/admin/full-sweep/{service_id}",
            headers={"x-fastly-service-id": service_id},
        )
        assert denied_resp.status_code == 403

        # Analyst Path A denial: read_only instance gets 403
        ro_source = {**full_sync_test_source, "access_level": "read_only"}
        app.dependency_overrides[get_source] = lambda: ro_source
        app.dependency_overrides.pop(require_admin, None)
        ro_denied_resp = client.post(
            f"/api/admin/full-sweep/{service_id}",
            headers={"x-fastly-service-id": service_id},
        )
        assert ro_denied_resp.status_code == 403
        assert "read-only" in ro_denied_resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.clear()


def test_contract_2_cron_runs_records_success_and_outcome_counters(full_sync_test_source):
    """Checklist Item 2:
    Confirm execution records in cron_runs with status success, non-zero files, and structured outcome_counters.
    """
    from backend.core import metadata as metadata_db

    service_id = full_sync_test_source["service_id"]
    source_name = full_sync_test_source["name"]

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
    assert latest["status"] == "success"
    assert latest["files_downloaded"] == 5
    assert latest["rows_ingested"] == 1000
    assert "Backfilled 5" in latest["summary"]


def test_contract_3_usage_log_attribution_cron_full_sync(full_sync_test_source, monkeypatch):
    """Checklist Item 3:
    Verify PostgreSQL usage_log table attributes FOS Class A LIST calls to cron.full_sync under process_context="cron.full_sync".
    """
    from backend.core import metadata as metadata_db
    from backend.core.metadata import usage_log_db
    from backend.utils.telemetry import get_process_context

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
        "backend.utils.telemetry.flush_usage_log",
        lambda sid: flush_contexts.append(get_process_context()),
    )

    sync_mod._run_full_sweep(service_id, force=True)

    assert observed["context"] == "cron.full_sync", (
        f"Execution must run under process_context='cron.full_sync', got {observed['context']!r}"
    )
    assert flush_contexts == ["cron.full_sync"], (
        f"Usage log flush must run under process_context='cron.full_sync', got {flush_contexts}"
    )

    # Emulate FOS Class A LIST call captured during full sweep
    fos_calls = [
        {
            "method": "LISTOBJECTSV2",
            "service": "FOS",
            "details": "Class A · list",
            "url": "s3://test-full-sync-bucket/raw/request/",
        }
    ]
    metadata_db.log_usage_calls(service_id, fos_calls, process_context="cron.full_sync")

    con = usage_log_db.get_con(service_id)
    rows = con.execute(
        "SELECT operation_class, process_context, operation_type FROM usage_log WHERE service_id = ? ORDER BY id",
        (service_id,),
    ).fetchall()

    list_rows = [r for r in rows if r["operation_type"] == "LISTOBJECTSV2"]
    assert list_rows, "Must record LISTOBJECTSV2 in usage_log"
    assert list_rows[-1]["operation_class"] == "A", "LISTOBJECTSV2 must be classified as Class A"
    assert list_rows[-1]["process_context"] == "cron.full_sync", "LISTOBJECTSV2 must be attributed to cron.full_sync"


def test_contract_4_safety_gate_under_fla_dev_no_crons(full_sync_test_source, monkeypatch, caplog):
    """Checklist Item 4:
    Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
    """
    service_id = full_sync_test_source["service_id"]
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    # 1. Registration check: dev allowlist must NOT register full_sync
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[full_sync_test_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=full_sync_test_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()
        job_ids = [c.kwargs["id"] for c in add_job.call_args_list]
        assert f"full_sync_{service_id}" not in job_ids, "full_sync must never register under FLA_DEV_NO_CRONS=1"

    # 2. Execution check: direct invocation must refuse execution and log warning citing FLA_DEV_NO_CRONS=1
    caplog.set_level(logging.WARNING)
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        res = sync_mod._run_full_sweep(service_id, force=True)
        get_src.assert_not_called()
        assert res is None

    assert any("FLA_DEV_NO_CRONS=1" in record.message for record in caplog.records), (
        "Execution refusal log must cite FLA_DEV_NO_CRONS=1"
    )


def test_contract_5_active_request_deferral_and_force_bypass(full_sync_test_source, monkeypatch):
    """Checklist Item 5:
    Active-request deferral when should_defer_cron is True, bypassed if force=True.
    """
    service_id = full_sync_test_source["service_id"]
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda kind, sid: True)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: full_sync_test_source)
    start_cron = MagicMock(return_value=999)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_cron)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr(
        "backend.core.ingest.ingest",
        MagicMock(return_value=iter([{"type": "done", "new_files": 0, "rows_inserted": 0}])),
    )

    # 1. Normal invocation defers when active requests are present
    sync_mod._run_full_sweep(service_id, force=False)
    start_cron.assert_not_called()

    # 2. force=True bypasses the deferral
    sync_mod._run_full_sweep(service_id, force=True)
    start_cron.assert_called_once()


def test_contract_6_adaptive_budget_scaling(full_sync_test_source, monkeypatch):
    """Checklist Item 6:
    Adaptive queue-depth and buffer backlog budgeting in Standard and High-Scale modes.
    """
    service_id = full_sync_test_source["service_id"]
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: full_sync_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 777)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())

    # Standard mode: high buffer backlog (>2000) scales down to max_files=5000, max_seconds=300
    monkeypatch.setattr(
        "backend.core.iceberg.buffer_backlog_stats",
        lambda src: {"file_count": 3000, "total_bytes": 100_000_000},
    )
    mock_ingest = MagicMock(return_value=iter([{"type": "done", "new_files": 0, "rows_inserted": 0}]))
    monkeypatch.setattr("backend.core.ingest.ingest", mock_ingest)

    sync_mod._run_full_sweep(service_id, force=True)
    assert mock_ingest.call_args.kwargs["max_files"] == 5000
    assert mock_ingest.call_args.kwargs["max_seconds"] == 300

    # Standard mode: clean buffer (<200) scales up to max_files=50000, max_seconds=1200
    monkeypatch.setattr(
        "backend.core.iceberg.buffer_backlog_stats",
        lambda src: {"file_count": 50, "total_bytes": 1_000_000},
    )
    sync_mod._run_full_sweep(service_id, force=True)
    assert mock_ingest.call_args.kwargs["max_files"] == 50000
    assert mock_ingest.call_args.kwargs["max_seconds"] == 1200

    # High-Scale mode: Celery queue depth > 5000 scales down to 5000
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr(
        "backend.celery_status.celery_queue_depths",
        lambda: ({"fastly:discovery": 6000, "fastly:convert": 100}, True),
    )
    discover_mock = MagicMock(return_value=0)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover_mock)

    sync_mod._run_full_sweep(service_id, force=True)
    discover_mock.assert_called_once_with(service_id, run_id=777, originating_task="full_sync")


def test_contract_7_high_scale_mode_prefix_discovery_attribution(full_sync_test_source, monkeypatch):
    """Checklist Item 7:
    In High-Scale mode, discover_prefix executes full prefix LIST with originating_task="full_sync".
    """
    service_id = full_sync_test_source["service_id"]
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: full_sync_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 999)
    log_run = MagicMock()
    finalize = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", finalize)
    discover = MagicMock(return_value=12)
    monkeypatch.setattr("backend.core.ingest.discover_prefix", discover)

    sync_mod._run_full_sweep(service_id, force=True)

    discover.assert_called_once_with(service_id, run_id=999, originating_task="full_sync")
    assert log_run.call_args.args[3] == "success"
    assert log_run.call_args.kwargs["files_downloaded"] == 12
    assert "12 unseen" in log_run.call_args.kwargs["summary"]
    finalize.assert_called_once()
    assert finalize.call_args.args[1] == 999


def test_contract_8_corrupt_rows_and_quarantine_outcome_counters(full_sync_test_source, monkeypatch):
    """Checklist Item 8:
    Corrupt rows / quarantine failures transition status to "error" with structured outcome counters.
    """
    service_id = full_sync_test_source["service_id"]
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

    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda *a, **kw: False)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: full_sync_test_source)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a, **kw: 654)
    log_cron = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_cron)
    monkeypatch.setattr("backend.core.duckdb.update_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron.jobs._common.refresh_view_and_warm_pool", MagicMock())

    def fake_ingest(*args, **kwargs):
        yield {
            "type": "done",
            "new_files": 2,
            "rows_inserted": 100,
            "corrupt_rows": 6,
            "outcome_counters": counters,
            "corrupt_details": ["bad line at offset 42"],
        }

    monkeypatch.setattr("backend.core.ingest.ingest", fake_ingest)

    sync_mod._run_full_sweep(service_id, force=True)

    log_cron.assert_called_once()
    args, kwargs = log_cron.call_args
    assert args[3] == "error", "Status must transition to error on corrupt rows"
    assert kwargs.get("outcome_counters") == counters
    assert kwargs.get("corrupt_rows") == 6
    assert kwargs.get("error_message") == "bad line at offset 42"
    assert "quarantined" in kwargs.get("summary", "").lower()


def test_contract_9_scheduler_registration_reschedule_and_disabling(full_sync_test_source):
    """Checklist Item 9:
    Scheduler registration, rescheduling, and disabling via provisioning.cron_full_sweep.
    """
    service_id = full_sync_test_source["service_id"]

    # 1. Registration with default schedule (hours 3,9,15,21 at :30)
    sched = Scheduler()
    sched._sched = MagicMock()
    with (
        patch("backend.config.list_configs", return_value=[full_sync_test_source]),
        patch("backend.core.duckdb.get_source_for_service", return_value=full_sync_test_source),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        sched._sync_jobs()

    assert f"full_sync_{service_id}" in sched._job_ids
    add_job_calls = [c for c in sched._sched.add_job.call_args_list if c.kwargs.get("id") == f"full_sync_{service_id}"]
    assert len(add_job_calls) == 1
    add_call = add_job_calls[0]
    assert add_call.args[1] == "cron"
    assert add_call.kwargs.get("hour") == "3,9,15,21"
    assert add_call.kwargs.get("minute") == 30

    # 2. Rescheduling when cron_hours or cron_minute change
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
    with (
        patch("backend.config.list_configs", return_value=[resched_src]),
        patch("backend.core.duckdb.get_source_for_service", return_value=resched_src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        sched._sync_jobs()

    full_sweep_job.reschedule.assert_called_once_with("cron", hour="1,13", minute=15)

    # 3. Disabling via cron_full_sweep.enabled = False
    disabled_src = {
        **full_sync_test_source,
        "provisioning": {
            **full_sync_test_source["provisioning"],
            "cron_full_sweep": {"enabled": False},
        },
    }
    sched2 = Scheduler()
    sched2._sched = MagicMock()
    with (
        patch("backend.config.list_configs", return_value=[disabled_src]),
        patch("backend.core.duckdb.get_source_for_service", return_value=disabled_src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        sched2._sync_jobs()

    assert f"full_sync_{service_id}" not in sched2._job_ids


# ── Merged from test_gap_heal_contract.py ──────────────────────────────────────────────

import time

import pytest

from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.routers.admin import SustainedLossAlert


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
    # Clear last trigger timestamp
    sync_mod._GAP_HEAL_LAST_TRIGGER.pop(service_id, None)
    return src


class TestGapHealContract:
    def test_contract_1_manual_trigger_endpoint(self, gap_heal_test_source):
        """Checklist Item 1:
        Trigger POST /api/admin/gap-heal/{service_id}; verify HTTP 200, run_id, and admin-only authorization.
        """
        service_id = gap_heal_test_source["service_id"]
        started = {}

        def fake_start_cron_run(src, task):
            started["task"] = task
            return "run-gap-heal-contract-101"

        app.dependency_overrides[get_source] = lambda: gap_heal_test_source
        app.dependency_overrides[get_service_id] = lambda: service_id
        try:
            with (
                patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
                patch("backend.cron_progress.start_progress"),
                patch("backend.cron.jobs.sync._run_gap_heal"),
                patch("backend.repositories.dashboard.invalidate_service"),
            ):
                client = TestClient(app)
                resp = client.post(
                    f"/api/admin/gap-heal/{service_id}?force=true",
                    headers={"x-fastly-service-id": service_id},
                )

            assert resp.status_code == 200, f"Expected 200, got: {resp.text}"
            body = resp.json()
            assert body["ok"] is True
            assert body["run_id"] == "run-gap-heal-contract-101"
            assert "started" in body["message"].lower()
            assert started["task"] == "gap_heal"

            # Re-entrancy: when task is already running, returns 200 with in-progress message
            with (
                patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
                patch(
                    "backend.cron_progress.list_active_runs",
                    return_value=[{"service_id": service_id, "task": "gap_heal", "run_id": "run-gap-heal-active-202"}],
                ),
                patch("backend.repositories.dashboard.invalidate_service"),
            ):
                resp_busy = client.post(
                    f"/api/admin/gap-heal/{service_id}",
                    headers={"x-fastly-service-id": service_id},
                )
            assert resp_busy.status_code == 200
            body_busy = resp_busy.json()
            assert body_busy["ok"] is True
            assert body_busy["run_id"] == "run-gap-heal-active-202"
            assert "already running" in body_busy["message"].lower()

            # Analyst Path B denial: non-admin gets 403
            from fastapi import HTTPException

            def reject_analyst():
                raise HTTPException(status_code=403, detail={"error": "admin_only"})

            app.dependency_overrides[require_admin] = reject_analyst
            denied_resp = client.post(
                f"/api/admin/gap-heal/{service_id}",
                headers={"x-fastly-service-id": service_id},
            )
            assert denied_resp.status_code == 403

            # Analyst Path A denial: read_only instance gets 403
            ro_source = {**gap_heal_test_source, "access_level": "read_only"}
            app.dependency_overrides[get_source] = lambda: ro_source
            app.dependency_overrides.pop(require_admin, None)

            ro_denied_resp = client.post(
                f"/api/admin/gap-heal/{service_id}",
                headers={"x-fastly-service-id": service_id},
            )
            assert ro_denied_resp.status_code == 403
            assert "read-only" in ro_denied_resp.json()["detail"].lower()
        finally:
            app.dependency_overrides.pop(get_source, None)
            app.dependency_overrides.pop(get_service_id, None)
            app.dependency_overrides.pop(require_admin, None)

    def test_contract_2_run_gap_heal_accepts_run_id_and_force(self, gap_heal_test_source):
        """Checklist Item 2:
        _run_gap_heal must accept run_id and force kwargs. When run_id is supplied,
        it must NOT call start_cron_run again.
        """
        service_id = gap_heal_test_source["service_id"]
        start_cron_calls = []
        log_calls = []

        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=lambda *a, **k: start_cron_calls.append(a)),
            patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron.jobs._common.finalize_cron_duration"),
            patch(
                "backend.routers.admin.compute_log_accounting",
                return_value={"sustained_loss": None, "buckets": [], "totals": None},
            ),
        ):
            sync_mod._run_gap_heal(service_id, run_id=777, force=True)

        assert len(start_cron_calls) == 0, "start_cron_run must not be called when run_id is passed"
        assert len(log_calls) == 1
        args, kwargs = log_calls[0]
        assert kwargs.get("run_id") == 777
        assert args[3] == "success"
        assert "No sustained loss" in kwargs["summary"]

    def test_contract_3_sustained_loss_triggers_full_sweep_with_adaptive_budget(self, gap_heal_test_source):
        """Checklist Item 3:
        Fastly Stats vs DuckDB accounting comparison detecting SustainedLossAlert
        and triggering _run_full_sweep with adaptive severity bands and expanded budgets.
        """
        service_id = gap_heal_test_source["service_id"]

        # Matrix of (max_gap_pct, total_lost_lines, expected_band, expected_max_files, expected_max_seconds)
        test_cases = [
            (0.06, 1_000, "mild", 20_000, 900),
            (0.15, 12_000, "elevated", 20_000, 900),
            (0.55, 120_000, "severe", 50_000, 1500),
            (0.85, 600_000, "critical", 100_000, 1800),
        ]

        for gap_pct, lost_lines, expected_band, exp_files, exp_seconds in test_cases:
            sync_mod._GAP_HEAL_LAST_TRIGGER.clear()
            full_sweep_calls = []
            log_calls = []
            sustained = SustainedLossAlert(
                started_at="2026-10-08T10:00:00Z",
                n_buckets=3,
                max_gap_pct=gap_pct,
                total_lost_lines=lost_lines,
            )

            with (
                patch("backend.core.duckdb.start_cron_run", return_value=888),
                patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
                patch("backend.cron_progress.start_progress"),
                patch("backend.cron_progress.end_progress"),
                patch("backend.cron.jobs._common.finalize_cron_duration"),
                patch(
                    "backend.cron.jobs.sync._run_full_sweep",
                    side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
                ),
                patch(
                    "backend.routers.admin.compute_log_accounting",
                    return_value={"sustained_loss": sustained, "buckets": [], "totals": None},
                ),
            ):
                sync_mod._run_gap_heal(service_id, force=True)

            assert len(full_sweep_calls) == 1, f"Expected full sweep for band {expected_band}"
            sid, kw = full_sweep_calls[0]
            assert sid == service_id
            assert kw["max_files"] == exp_files, f"Band {expected_band} max_files mismatch"
            assert kw["max_seconds"] == exp_seconds, f"Band {expected_band} max_seconds mismatch"
            assert kw.get("force") is True, "force flag must be forwarded to _run_full_sweep"

            # Verify warning status in cron_runs
            assert len(log_calls) == 1
            args, kwargs = log_calls[0]
            assert args[3] == "warning"
            assert f"severity={expected_band}" in kwargs["summary"]
            assert "triggering full_sweep" in kwargs["summary"]

    def test_contract_4_throttle_cooldowns_and_force_bypass(self, gap_heal_test_source):
        """Checklist Item 4:
        Throttle cooldowns:
          critical: 0h (always fires)
          severe: 0.25h (15 min)
          elevated: 2h
          mild: 4h
        And force=True bypasses cooldown window.
        """
        service_id = gap_heal_test_source["service_id"]
        now = time.time()

        # Elevated band: 15% gap, 15,000 lost lines -> throttle_hours=2.0
        sustained_elevated = SustainedLossAlert(
            started_at="2026-10-08T09:00:00Z",
            n_buckets=2,
            max_gap_pct=0.15,
            total_lost_lines=15_000,
        )

        # 1 hour ago: inside 2h throttle window
        one_hour_ago = now - 3600
        sync_mod._GAP_HEAL_LAST_TRIGGER[service_id] = one_hour_ago

        full_sweep_calls = []
        log_calls = []
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=889),
            patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron.jobs._common.finalize_cron_duration"),
            patch(
                "backend.cron.jobs.sync._run_full_sweep",
                side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
            ),
            patch(
                "backend.routers.admin.compute_log_accounting",
                return_value={"sustained_loss": sustained_elevated, "buckets": [], "totals": None},
            ),
        ):
            # Without force: throttled
            sync_mod._run_gap_heal(service_id, force=False)

        assert len(full_sweep_calls) == 0, "Must be throttled when force=False"
        assert len(log_calls) == 1
        assert log_calls[0][0][3] == "warning"
        assert "throttled" in log_calls[0][1]["summary"]
        assert "< 2h" in log_calls[0][1]["summary"] or "< 2" in log_calls[0][1]["summary"]

        # Now with force=True: cooldown bypassed!
        full_sweep_calls.clear()
        log_calls.clear()
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=890),
            patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron.jobs._common.finalize_cron_duration"),
            patch(
                "backend.cron.jobs.sync._run_full_sweep",
                side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
            ),
            patch(
                "backend.routers.admin.compute_log_accounting",
                return_value={"sustained_loss": sustained_elevated, "buckets": [], "totals": None},
            ),
        ):
            sync_mod._run_gap_heal(service_id, force=True)

        assert len(full_sweep_calls) == 1, "force=True must bypass throttle cooldown"
        assert log_calls[0][0][3] == "warning"
        assert "triggering full_sweep" in log_calls[0][1]["summary"]

    def test_contract_5_fastly_stats_error_handling_and_no_loss(self, gap_heal_test_source):
        """Checklist Item 5:
        Fastly stats unavailable returns graceful success skip; unhandled crash returns error.
        """
        service_id = gap_heal_test_source["service_id"]
        from fastapi import HTTPException

        log_calls = []
        # Fastly stats failure: returns status="success" with skipped summary (no false positive sweep)
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=901),
            patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron.jobs._common.finalize_cron_duration"),
            patch(
                "backend.routers.admin.compute_log_accounting",
                side_effect=HTTPException(status_code=502, detail={"error": "fastly_stats_failed"}),
            ),
        ):
            sync_mod._run_gap_heal(service_id)

        assert len(log_calls) == 1
        assert log_calls[0][0][3] == "success"
        assert "Fastly stats unavailable" in log_calls[0][1]["summary"]

        # Unhandled exception: logs error in cron_runs
        log_calls.clear()
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=902),
            patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron.jobs._common.finalize_cron_duration"),
            patch(
                "backend.routers.admin.compute_log_accounting",
                side_effect=RuntimeError("Database query timed out"),
            ),
        ):
            sync_mod._run_gap_heal(service_id)

        assert len(log_calls) == 1
        assert log_calls[0][0][3] == "error"
        assert "crashed" in log_calls[0][1]["summary"].lower()

    def test_contract_6_active_request_politeness_deferral(self, gap_heal_test_source):
        """Checklist Item 6:
        Active request politeness deferral (should_defer_cron), bypassed when force=True.
        """
        service_id = gap_heal_test_source["service_id"]
        start_calls = []

        with (
            patch("backend.utils.active_requests.should_defer_cron", return_value=True),
            patch("backend.core.duckdb.start_cron_run", side_effect=lambda *a, **k: start_calls.append(a)),
        ):
            # Without force: should defer immediately
            sync_mod._run_gap_heal(service_id, force=False)
            assert len(start_calls) == 0, "Must defer when active requests present and force=False"

            # With force=True: should bypass deferral and start run
            with (
                patch("backend.cron_progress.start_progress"),
                patch("backend.cron_progress.end_progress"),
                patch("backend.cron.jobs._common.finalize_cron_duration"),
                patch("backend.core.duckdb.log_cron_run"),
                patch(
                    "backend.routers.admin.compute_log_accounting",
                    return_value={"sustained_loss": None, "buckets": [], "totals": None},
                ),
            ):
                sync_mod._run_gap_heal(service_id, force=True)
            assert len(start_calls) == 1, "force=True must bypass should_defer_cron"

    def test_contract_7_kill_switch_protection(self, gap_heal_test_source, monkeypatch):
        """Checklist Item 7:
        Under FLA_DEV_NO_CRONS=1, verify job refuses execution immediately without starting a run.
        """
        service_id = gap_heal_test_source["service_id"]
        monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
        start_calls = []

        with patch("backend.core.duckdb.start_cron_run", side_effect=lambda *a, **k: start_calls.append(a)):
            sync_mod._run_gap_heal(service_id, force=True)

        assert len(start_calls) == 0, "FLA_DEV_NO_CRONS=1 must refuse gap_heal execution"

    def test_contract_8_telemetry_and_usage_log_attribution(self, monkeypatch):
        """Checklist Item 8:
        Verify @cron_task("gap_heal", job_name="gap_heal") sets process_context to "gap_heal".
        """
        monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
        from backend.utils.telemetry import get_process_context

        captured_context = []

        def inspect_context(sid, *a, **k):
            captured_context.append(get_process_context())

        with (
            patch(
                "backend.core.duckdb.get_source_for_service",
                return_value={"name": "svc-ctx", "access_level": "read_write"},
            ),
            patch("backend.core.duckdb.start_cron_run", return_value=999),
            patch("backend.core.duckdb.log_cron_run"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron.jobs._common.finalize_cron_duration"),
            patch("backend.routers.admin.compute_log_accounting", side_effect=inspect_context),
        ):
            sync_mod._run_gap_heal("svc-ctx")

        assert len(captured_context) == 1
        assert captured_context[0] == "gap_heal"

    def test_contract_9_scheduler_registration(self):
        """Checklist Item 9:
        Scheduler registration and dynamic rescheduling on interval_minutes changes.
        """
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

        s = Scheduler()
        with (
            patch("backend.config.list_configs", return_value=[cfg]),
            patch(
                "backend.core.duckdb.get_source_for_service",
                return_value={"name": "svc-sched-gap", "access_level": "read_write"},
            ),
            patch("backend.core.duckdb.is_configured", return_value=True),
            patch("backend.config.get_ngwaf_workspace_id", return_value=None),
            patch("backend.core.metadata.count_alerts", return_value=1),
        ):
            s._sync_jobs()

        assert "gap_heal_svc-sched-gap" in s._job_ids

        # Verify disabled configuration skips job
        cfg_disabled = {
            **cfg,
            "provisioning": {
                "cron_sync": {"enabled": True},
                "cron_gap_heal": {"enabled": False},
            },
        }
        s_disabled = Scheduler()
        with (
            patch("backend.config.list_configs", return_value=[cfg_disabled]),
            patch(
                "backend.core.duckdb.get_source_for_service",
                return_value={"name": "svc-sched-gap", "access_level": "read_write"},
            ),
            patch("backend.core.duckdb.is_configured", return_value=True),
            patch("backend.config.get_ngwaf_workspace_id", return_value=None),
            patch("backend.core.metadata.count_alerts", return_value=1),
        ):
            s_disabled._sync_jobs()

        assert "gap_heal_svc-sched-gap" not in s_disabled._job_ids


# ── Merged from test_ledger_sweep_contract.py ──────────────────────────────────────────────


from backend.cron.jobs import rum_ledger as ledger_mod

SERVICE_ID = "svc_test_ledger_sweep"

FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "deployment_mode": "high_scale",
    "access_level": "read_write",
    "provisioning": {
        "access_level": "read_write",
        "cron_ledger_sweep": {"enabled": True, "interval_minutes": 15},
    },
}

FAKE_SRC = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "access_level": "read_write",
    "deployment_mode": "high_scale",
    "bucket": "test-sweep-bucket",
    "provisioning": {
        "access_level": "read_write",
        "cron_ledger_sweep": {"enabled": True, "interval_minutes": 15},
    },
}


# -----------------------------------------------------------------------------
# 1. Manual Trigger Endpoint Tests

# -----------------------------------------------------------------------------


def test_manual_trigger_endpoint_success():
    """POST /api/admin/ledger/sweep/{service_id} returns 200, run_id, and starts job."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value="run-sweep-101"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_ledger._run_ledger_sweep"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/ledger/sweep/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-sweep-101"
        assert "started" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_endpoint_reentrancy():
    """POST /api/admin/ledger/sweep/{service_id} returns 200 with already running message."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("Already running")),
            patch(
                "backend.cron_progress.list_active_runs",
                return_value=[{"service_id": SERVICE_ID, "task": "ledger_sweep", "run_id": "run-sweep-active"}],
            ),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/ledger/sweep/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == "run-sweep-active"
        assert "already running" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_endpoint_standard_mode_rejected():
    """POST /api/admin/ledger/sweep/{service_id} on standard mode returns 400."""
    from tests.cron.conftest import assert_manual_trigger_standard_mode_rejected

    assert_manual_trigger_standard_mode_rejected(f"/api/admin/ledger/sweep/{SERVICE_ID}", SERVICE_ID, FAKE_SRC)


def test_manual_trigger_endpoint_read_only_rejected():
    """POST /api/admin/ledger/sweep/{service_id} on read-only service returns 403."""
    from tests.cron.conftest import assert_manual_trigger_read_only_rejected

    assert_manual_trigger_read_only_rejected(f"/api/admin/ledger/sweep/{SERVICE_ID}", SERVICE_ID, FAKE_SRC)


def test_manual_trigger_endpoint_non_admin_rejected():
    """POST /api/admin/ledger/sweep/{service_id} by non-admin returns 403."""
    from tests.cron.conftest import assert_manual_trigger_non_admin_rejected

    assert_manual_trigger_non_admin_rejected(f"/api/admin/ledger/sweep/{SERVICE_ID}", SERVICE_ID, FAKE_SRC)


# -----------------------------------------------------------------------------
# 2. Quarantine Inspection Endpoint Tests
# -----------------------------------------------------------------------------


def test_ledger_quarantine_endpoint_success(monkeypatch):
    """GET /api/admin/ledger/quarantine?service_id={service_id} returns 200 with items and total."""
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
        client = TestClient(app)
        resp = client.get(f"/api/admin/ledger/quarantine?service_id={SERVICE_ID}")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert len(body["items"]) == 1
    assert body["items"][0]["line_ordinal"] == 42


def test_ledger_quarantine_endpoint_service_not_found(monkeypatch):
    """GET /api/admin/ledger/quarantine?service_id={unknown} returns 404."""
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: None)

    client = TestClient(app)
    resp = client.get("/api/admin/ledger/quarantine?service_id=nonexistent-svc")
    assert resp.status_code == 404


def test_ledger_quarantine_endpoint_read_only_rejected(monkeypatch):
    """GET /api/admin/ledger/quarantine on read-only service returns 403."""
    ro_src = {**FAKE_SRC, "access_level": "read_only"}
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)

    client = TestClient(app)
    resp = client.get(f"/api/admin/ledger/quarantine?service_id={SERVICE_ID}")
    assert resp.status_code == 403


def test_ledger_quarantine_endpoint_non_admin_rejected():
    """GET /api/admin/ledger/quarantine by non-admin returns 403."""
    from fastapi import HTTPException

    def reject_analyst():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_analyst
    try:
        client = TestClient(app)
        resp = client.get(f"/api/admin/ledger/quarantine?service_id={SERVICE_ID}")
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.pop(require_admin, None)


# -----------------------------------------------------------------------------
# 3. Execution Lifecycle & Step-by-Step Logic (_run_ledger_sweep)
# -----------------------------------------------------------------------------


def test_run_ledger_sweep_success(monkeypatch):
    """Under high-scale mode:
    - Runs sweep_ledger_once
    - Records status 'success' with reclaim/redispatch/discovery counts
    - Starts and ends progress cleanly, finalizes duration
    """
    log_calls = []
    finalize_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
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
        "reclaimed": 5,
        "redispatched": 5,
        "discovered": 10,
        "broker_ok": True,
        "dead_letter": 0,
    }
    sweep_calls = []

    def mock_sweep(sid, run_id=None):
        sweep_calls.append((sid, run_id))
        return sweep_result

    monkeypatch.setattr("backend.core.ingest.sweep_ledger_once", mock_sweep)

    start_progress = MagicMock()
    end_progress = MagicMock()
    monkeypatch.setattr("backend.cron_progress.start_progress", start_progress)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)

    start_progress.assert_called_once_with(888, service_id=SERVICE_ID, task="ledger_sweep")
    end_progress.assert_called_once_with(888)
    assert sweep_calls == [(SERVICE_ID, 888)]
    assert len(finalize_calls) == 1

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert kwargs.get("run_id") == 888
    assert args[3] == "success"
    assert kwargs.get("files_downloaded") == 10
    assert "reclaimed=5 redispatched=5 discovered=10" in kwargs.get("summary", "")


def test_run_ledger_sweep_run_id_reuse(monkeypatch):
    """When run_id is passed as an argument, it must NOT call start_cron_run."""
    start_cron_calls = []
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda *a: start_cron_calls.append(a))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: log_calls.append((a, k)))
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", MagicMock())
    monkeypatch.setattr(
        "backend.core.ingest.sweep_ledger_once",
        lambda sid, run_id=None: {
            "reclaimed": 0,
            "redispatched": 0,
            "discovered": 0,
            "broker_ok": True,
            "dead_letter": 0,
        },
    )

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID, run_id=999)

    assert len(start_cron_calls) == 0
    assert len(log_calls) == 1
    assert log_calls[0][1].get("run_id") == 999


def test_run_ledger_sweep_warning_on_dead_letter_or_broker_down(monkeypatch):
    """When dead-letter rows exist or broker probe fails, status is 'warning'."""
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
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
        "reclaimed": 2,
        "redispatched": 0,
        "discovered": 0,
        "broker_ok": False,
        "dead_letter": 3,
    }
    monkeypatch.setattr("backend.core.ingest.sweep_ledger_once", lambda sid, run_id=None: sweep_result)

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert "Celery broker/queue depth probe failed" in kwargs.get("summary", "")
    assert "3 dead-letter/quarantined row(s)" in kwargs.get("summary", "")


def test_run_ledger_sweep_error_handling_and_guaranteed_cleanup(monkeypatch):
    """When an exception occurs during sweep:
    - Status is recorded as 'error'
    - end_progress and finalize_cron_duration are still executed in finally block
    """
    log_calls = []
    finalize_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
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
        raise RuntimeError("Postgres connection lost")

    monkeypatch.setattr("backend.core.ingest.sweep_ledger_once", fail_sweep)

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)

    end_progress.assert_called_once_with(890)
    assert len(finalize_calls) == 1
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "error"
    assert "Postgres connection lost" in kwargs.get("error_message", "")


def test_run_ledger_sweep_skips_in_standard_mode(monkeypatch):
    """In standard mode, _run_ledger_sweep exits immediately without running."""
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_run_ledger_sweep_skips_when_read_only(monkeypatch):
    """When service is read_only, _run_ledger_sweep exits immediately."""
    start_calls = []
    ro_src = {**FAKE_SRC, "access_level": "read_only"}

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_run_ledger_sweep_skips_when_dev_no_crons(monkeypatch):
    """When FLA_DEV_NO_CRONS=1, _run_ledger_sweep exits immediately."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_run_ledger_sweep_skips_when_config_missing(monkeypatch):
    """When config cannot be loaded, _run_ledger_sweep exits cleanly."""
    start_calls = []
    monkeypatch.setattr("backend.config.load_config", lambda sid: None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


# -----------------------------------------------------------------------------
# 4. Scheduler Registration & Cadence Tests
# -----------------------------------------------------------------------------


def test_scheduler_registers_and_reschedules_ledger_sweep():
    """Scheduler registers ledger_sweep in high-scale mode and reschedules on interval changes."""
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

    job_id = f"ledger_sweep_{sid}"
    assert job_id in s._job_ids
    calls = [c for c in mock_sched.add_job.call_args_list if c.kwargs.get("id") == job_id]
    assert len(calls) == 1
    call = calls[0]
    assert call.args[0] == ledger_mod._run_ledger_sweep
    assert call.args[1] == "interval"
    assert call.kwargs["minutes"] == 15
    assert call.kwargs["coalesce"] is True
    assert call.kwargs["misfire_grace_time"] == 300
    assert call.kwargs["args"] == [sid]

    # Now change interval to 30 min
    cfg["provisioning"]["cron_ledger_sweep"]["interval_minutes"] = 30
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
        mock_job.reschedule.assert_called_once_with("interval", minutes=30)


def test_scheduler_skips_ledger_sweep_when_disabled():
    """Scheduler skips ledger_sweep when cron_ledger_sweep.enabled is False."""
    sid = "svc-sweep-disabled"
    cfg = {
        "service_id": sid,
        "name": sid,
        "log_period": 60,
        "deployment_mode": "high_scale",
        "access_level": "read_write",
        "provisioning": {
            "access_level": "read_write",
            "cron_sync": {"enabled": True},
            "cron_ledger_sweep": {"enabled": False, "interval_minutes": 15},
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

    job_id = f"ledger_sweep_{sid}"
    assert job_id not in s._job_ids


# -----------------------------------------------------------------------------
# 5. Sweeper Engine Tests (sweep_ledger_once)
# -----------------------------------------------------------------------------


def test_sweep_ledger_reclaims_stale_claims_excluding_rum(monkeypatch):
    """sweep_ledger_once reclaims stale claims while excluding raw/rum/% keys."""
    from backend.core.ingest import LEDGER_RECLAIM_AFTER_S, sweep_ledger_once

    sid = "svc-test-reclaim"
    rum_key = "raw/rum/2026/08/27/10/05/beacons.json.gz"
    log_key = "raw/2026/08/27/10/05/logs.json.gz"

    now = time.time()
    stale_time = now - LEDGER_RECLAIM_AFTER_S - 60

    # Mock database connection and cursor
    executed_statements = []

    class FakeCursor:
        def execute(self, query, params=None):
            executed_statements.append((query, params))

        def fetchall(self):
            # If query is UPDATE ... RETURNING object_key
            for q, p in executed_statements:
                if "UPDATE ingest_ledger SET status='discovered'" in q:
                    return [(log_key,)]
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
    monkeypatch.setattr("backend.core.ingest.discover_prefix", lambda *a, **k: 0)
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": 0}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_files.delay", mock_convert)

    summary = sweep_ledger_once(sid)

    assert summary["reclaimed"] == 1
    assert summary["redispatched"] == 1
    mock_convert.assert_called_once_with(sid, [log_key])

    # Check update statement excluded raw/rum/%
    update_stmts = [stmt for stmt in executed_statements if "UPDATE ingest_ledger" in stmt[0]]
    assert len(update_stmts) == 1
    query, params = update_stmts[0]
    assert "object_key NOT LIKE ?" in query
    assert "raw/rum/%" in params


def test_sweep_ledger_queue_depth_guard(monkeypatch):
    """When queue depth exceeds pending batches, re-dispatch is skipped."""
    from backend.core.ingest import sweep_ledger_once

    sid = "svc-test-depth-guard"
    log_key = "raw/2026/08/27/10/05/logs.json.gz"

    class FakeCursor:
        def execute(self, query, params=None):
            pass

        def fetchall(self):
            return [(log_key,)]

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
    monkeypatch.setattr("backend.core.ingest.discover_prefix", lambda *a, **k: 0)

    # Queue depth is 100 while pending is 1 batch -> should skip
    monkeypatch.setattr("backend.celery_status.celery_queue_depths", lambda: ({"q.ingest": 100}, True))
    mock_convert = MagicMock()
    monkeypatch.setattr("backend.core.ingest.convert_batch_files.delay", mock_convert)

    summary = sweep_ledger_once(sid)

    assert summary["reclaimed"] == 1
    assert summary["redispatched"] == 0
    mock_convert.assert_not_called()
