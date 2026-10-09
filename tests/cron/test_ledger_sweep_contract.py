"""Contract test suite for Cron 15: ledger_sweep_{service_id}.

Verifies all requirements and contract checklist items from docs/cron/jobs/ledger-sweep.md:
1. Manual Trigger Endpoint:
   - POST /api/admin/ledger/sweep/{service_id} returns 200 with run_id and starts execution.
   - Re-entrancy: returns 200 with in-progress message if already running.
   - Mode gate: returns 400 if service is in standard mode.
   - Role gate: returns 403 if service is read-only (Analyst Path A).
   - Role gate: returns 403 if caller lacks admin permission (Analyst Path B).
2. Quarantine Inspection Endpoint:
   - GET /api/admin/ledger/quarantine?service_id={service_id} returns 200 with items and total.
   - Returns 404 if service not found.
   - Returns 403 if service is read-only.
   - Returns 403 if caller lacks admin permission.
3. Execution Lifecycle & Step-by-Step Logic (_run_ledger_sweep):
   - High-throughput mode: executes sweep_ledger_once, records status 'success', emits progress, finalizes duration.
   - run_id reuse: accepts existing run_id without duplicate start_cron_run.
   - Guaranteed cleanup: end_progress and finalize_cron_duration execute in finally block even on error.
   - Health accounting: records status 'warning' when broker probe fails or dead-letter/quarantined rows exist.
   - Mode gate: skips immediately when deployment_mode is standard.
   - Role gate: skips immediately when service is read_only.
   - Dev kill-switch: skips immediately when FLA_DEV_NO_CRONS=1.
4. Scheduler Registration & Cadence:
   - Registers in High-Scale mode with default 15-minute interval (misfire_grace_time=300).
   - Reschedules dynamically on interval_minutes update.
   - Skips registration when cron_ledger_sweep.enabled=False.
5. Sweeper Engine Logic (sweep_ledger_once):
   - Stale worker claims reclaimed, excluding raw/rum/% keys.
   - Queue-depth guard: skips re-dispatch when q.ingest depth >= pending batches.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import ledger as ledger_mod
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app

SERVICE_ID = "svc_test_ledger_sweep"

FAKE_CFG = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "deployment_mode": "high_throughput",
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
    "deployment_mode": "high_throughput",
    "bucket": "test-sweep-bucket",
    "provisioning": {
        "access_level": "read_write",
        "cron_ledger_sweep": {"enabled": True, "interval_minutes": 15},
    },
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


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
            patch("backend.cron.jobs.ledger._run_ledger_sweep"),
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
            patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("Already running")),
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
    standard_src = {**FAKE_SRC, "deployment_mode": "standard"}
    app.dependency_overrides[get_source] = lambda: standard_src
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/ledger/sweep/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 400
        assert "high-throughput" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_endpoint_read_only_rejected():
    """POST /api/admin/ledger/sweep/{service_id} on read-only service returns 403."""
    ro_src = {**FAKE_SRC, "access_level": "read_only"}
    app.dependency_overrides[get_source] = lambda: ro_src
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/ledger/sweep/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 403
        assert "read-only" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_endpoint_non_admin_rejected():
    """POST /api/admin/ledger/sweep/{service_id} by non-admin returns 403."""
    from fastapi import HTTPException

    def reject_analyst():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[get_source] = lambda: FAKE_SRC
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    app.dependency_overrides[require_admin] = reject_analyst
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/ledger/sweep/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)
        app.dependency_overrides.pop(require_admin, None)


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
    mock_summary = {"total_items": 1, "category_counts": {"json_parse_error": 1}}

    with (
        patch("backend.routers.admin.quarantine.list_quarantine_evidence", return_value=mock_items),
        patch("backend.routers.admin.quarantine.get_quarantine_evidence_summary", return_value=mock_summary),
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
    """Under high-throughput mode:
    - Runs sweep_ledger_once
    - Records status 'success' with reclaim/redispatch/discovery counts
    - Starts and ends progress cleanly, finalizes duration
    """
    log_calls = []
    finalize_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_run_ledger_sweep_skips_when_read_only(monkeypatch):
    """When service is read_only, _run_ledger_sweep exits immediately."""
    start_calls = []
    ro_src = {**FAKE_SRC, "access_level": "read_only"}

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    ledger_mod._run_ledger_sweep.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0


def test_run_ledger_sweep_skips_when_dev_no_crons(monkeypatch):
    """When FLA_DEV_NO_CRONS=1, _run_ledger_sweep exits immediately."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC)
    monkeypatch.setattr("backend.config.is_high_throughput_mode", lambda src: True)
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
    """Scheduler registers ledger_sweep in high-throughput mode and reschedules on interval changes."""
    sid = "svc-scheduler-sweep"
    cfg = {
        "service_id": sid,
        "name": sid,
        "log_period": 60,
        "deployment_mode": "high_throughput",
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
        "deployment_mode": "high_throughput",
    }

    s = Scheduler()
    mock_sched = MagicMock()
    s._sched = mock_sched

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_throughput_mode", return_value=True),
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
        patch("backend.config.is_high_throughput_mode", return_value=True),
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
        "deployment_mode": "high_throughput",
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
        "deployment_mode": "high_throughput",
    }

    s = Scheduler()
    s._sched = MagicMock()

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.is_high_throughput_mode", return_value=True),
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
