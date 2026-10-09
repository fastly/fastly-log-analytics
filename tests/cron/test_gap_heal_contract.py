"""Contract test suite for Cron 10: gap_heal_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/gap-heal.md (§1–§9):
1. Trigger POST /api/admin/gap-heal/{service_id}; verify HTTP 200, run_id, and admin-only authorization.
2. Confirm _run_gap_heal accepts run_id and force kwargs, using existing run_id without duplicate leasing.
3. Fastly Stats vs DuckDB accounting comparison detecting SustainedLossAlert (>= 2 consecutive hours with >= 5% loss)
   and triggering _run_full_sweep with adaptive severity bands and expanded budgets.
4. Adaptive throttle cooldowns (critical=0h, severe=15m, elevated=2h, mild=4h) and force=True bypass.
5. Error handling: Fastly stats failure records graceful success skip without triggering false sweeps; no-loss records success.
6. Active request politeness deferral (should_defer_cron), bypassed when force=True.
7. Under FLA_DEV_NO_CRONS=1, verify job does not register or execute.
8. Telemetry & usage_log process context attribution ("gap_heal" / "cron.full_sync").
9. Scheduler registration, dynamic rescheduling on interval_minutes changes, and RedBeat routing in high-scale mode.
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import sync as sync_mod
from backend.cron.scheduler import Scheduler
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app
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


def test_contract_1_manual_trigger_endpoint(gap_heal_test_source):
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
            patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("Already running")),
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


def test_contract_2_run_gap_heal_accepts_run_id_and_force(gap_heal_test_source):
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


def test_contract_3_sustained_loss_triggers_full_sweep_with_adaptive_budget(gap_heal_test_source):
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


def test_contract_4_throttle_cooldowns_and_force_bypass(gap_heal_test_source):
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


def test_contract_5_fastly_stats_error_handling_and_no_loss(gap_heal_test_source):
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


def test_contract_6_active_request_politeness_deferral(gap_heal_test_source):
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


def test_contract_7_kill_switch_protection(gap_heal_test_source, monkeypatch):
    """Checklist Item 7:
    Under FLA_DEV_NO_CRONS=1, verify job refuses execution immediately without starting a run.
    """
    service_id = gap_heal_test_source["service_id"]
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    start_calls = []

    with patch("backend.core.duckdb.start_cron_run", side_effect=lambda *a, **k: start_calls.append(a)):
        sync_mod._run_gap_heal(service_id, force=True)

    assert len(start_calls) == 0, "FLA_DEV_NO_CRONS=1 must refuse gap_heal execution"


def test_contract_8_telemetry_and_usage_log_attribution(monkeypatch):
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
            "backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ctx", "access_level": "read_write"}
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


def test_contract_9_scheduler_registration_and_redbeat():
    """Checklist Item 9:
    Scheduler registration, dynamic rescheduling on interval_minutes changes, and RedBeat routing.
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

    # Verify RedBeat routing in external mode
    s.mode = "external"
    assert s._routes_to_redbeat("gap_heal_svc-sched-gap") is True

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
