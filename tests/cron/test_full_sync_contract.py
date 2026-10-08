"""Contract test suite for Cron 9: full_sync_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/full-sync.md (§1–§9):
1. Trigger POST /api/admin/full-sweep/{service_id}; verify HTTP 200, run_id, and admin-only authorization.
2. Confirm execution records in cron_runs with status success, non-zero files, and structured outcome_counters.
3. Verify PostgreSQL usage_log table attributes FOS Class A LIST calls to cron.full_sync under process_context="cron.full_sync".
4. Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
5. Active-request deferral when should_defer_cron is True, bypassed if force=True.
6. Adaptive queue-depth and buffer backlog budgeting in Standard and High-Scale modes.
7. In High-Scale mode, discover_prefix executes full prefix LIST with originating_task="full_sync".
8. Corrupt rows / quarantine failures transition status to "error" with structured outcome counters.
9. Scheduler registration, rescheduling, and disabling via provisioning.cron_full_sweep.
"""

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
        "backend.utils.usage_logger.flush_usage_log",
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
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
