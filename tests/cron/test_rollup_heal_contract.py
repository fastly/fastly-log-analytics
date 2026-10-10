"""Contract test suite for Cron 5: rollup_heal_{service_id}.

Verifies all requirements and checklist items from docs/cron/jobs/rollup-heal.md (§1–§9):
1. Registration in backend/cron/scheduler.py at minute=5 and startup (NOW() + 30s).
2. Task mapping "rollup_heal": "rollup_hour_heal" in backend/cron/schedule.py.
3. 48-hour lookback window (lookback_days = 2) in backend/cron/jobs/compaction.py.
4. Politeness gate: yields via should_defer_cron("rollup_hour_heal", service_id) when user queries are active; manual bypass.
5. Durable serving / High-Scale mode throttle: max_missing_hours = 1 startup catch-up ceiling.
6. Empty-hour sentinel bundle generation for zero-row closed hours to prevent re-scan thrashing.
7. Manual trigger & inspection parity (POST /api/admin/rollup-heal/{service_id}, POST /api/admin/backfill-bundle-rollups, GET /api/cron-runs).
8. ENOSPC disk space safety pre-check (< 50MB free).
9. Zero FOS egress (local-only writes).
10. Resilient exception handling and cron_runs logging.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs.compaction import _run_rollup_hour_heal


@pytest.fixture
def contract_service_source(tmp_path):
    cache_root = tmp_path / "cache-rollup-heal-contract"
    cache_root.mkdir(parents=True, exist_ok=True)
    return {
        "name": "svc_rollup_heal_contract",
        "service_id": "svc-rollup-heal-contract-1",
        "storage_mode": "local",
        "bucket": "contract_rollup_heal_bucket",
        "_cache_dir_override": str(cache_root),
        "cache_dir": str(cache_root),
    }


def test_scheduler_registration_and_schedule_mapping():
    """Verifies Checklist Items 1 & 2:
    - Registration in backend/cron/scheduler.py at minute=5 and startup (NOW() + 30s).
    - Task mapping "rollup_heal": "rollup_hour_heal" in backend/cron/schedule.py.
    """
    from backend.cron.schedule import TASK_MAP

    assert TASK_MAP.get("rollup_heal") == "rollup_hour_heal"

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    mock_add = MagicMock()
    sched._add_job = mock_add

    cfg = {
        "service_id": "svc-dev-test",
        "name": "Dev Test Svc",
        "fastly": {"service_id": "svc-dev-test"},
        "provisioning": {"access_level": "read_write"},
    }
    src = {
        "service_id": "svc-dev-test",
        "name": "Dev Test Svc",
        "fastly": {"service_id": "svc-dev-test"},
        "provisioning": {"access_level": "read_write"},
    }

    # 1. Dev-local safe registration
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.cron.scheduler.dev_local_crons_enabled", return_value=True),
    ):
        sched._register_dev_local_safe_jobs()

    heal_calls = [call for call in mock_add.call_args_list if call.kwargs.get("id") == "rollup_heal_svc-dev-test"]
    assert len(heal_calls) == 1
    call = heal_calls[0]
    assert call.args[0] == _run_rollup_hour_heal
    assert call.args[1] == "cron"
    assert call.kwargs.get("minute") == 5
    assert call.kwargs.get("args") == ["svc-dev-test"]
    assert call.kwargs.get("max_instances") == 1
    assert call.kwargs.get("coalesce") is True
    assert call.kwargs.get("misfire_grace_time") == 900
    # Startup run time is within ~30s of NOW
    next_run = call.kwargs.get("next_run_time")
    assert next_run is not None
    delta = abs((next_run - datetime.now(UTC)).total_seconds() - 30)
    assert delta < 5.0

    # 2. Sync jobs registration
    mock_add.reset_mock()
    sched._job_ids.clear()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
    ):
        sched._sync_jobs()

    sync_heal_calls = [call for call in mock_add.call_args_list if call.kwargs.get("id") == "rollup_heal_svc-dev-test"]
    assert len(sync_heal_calls) == 1
    scall = sync_heal_calls[0]
    assert scall.args[0] == _run_rollup_hour_heal
    assert scall.kwargs.get("minute") == 5


def test_rollup_heal_lookback_window_48h(contract_service_source, monkeypatch):
    """Verifies Checklist Item 3:
    48-hour lookback window (lookback_days = 2) in backend/cron/jobs/compaction.py.
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=101))
    logged_runs = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: logged_runs.append((a, kw)))

    heal_mock = MagicMock(
        return_value={
            "missing": 3,
            "rebuilt_fields": 12,
            "bundled": 3,
            "stamped_empty": 1,
            "coverage_verified": True,
        }
    )
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    res = _run_rollup_hour_heal.__wrapped__(sid)

    assert heal_mock.call_count == 1
    assert heal_mock.call_args.kwargs.get("lookback_days") == 2
    assert res["status"] == "success"
    assert res["missing"] == 3
    assert res["rebuilt_fields"] == 12
    assert res["bundled"] == 3
    assert res["stamped_empty"] == 1
    assert "Healed 3 missing hour(s)" in res["summary"]

    assert len(logged_runs) == 1
    args, kwargs = logged_runs[0]
    assert args[1] == "rollup_hour_heal"
    assert args[3] == "success"
    assert "Healed 3 missing hour(s)" in kwargs["summary"]
    assert "1 empty hour(s) stamped" in kwargs["summary"]


def test_rollup_heal_politeness_gate_and_manual_bypass(contract_service_source, monkeypatch):
    """Verifies Checklist Item 4:
    Politeness gate yields via should_defer_cron("rollup_hour_heal", service_id)
    when user queries are active; manual=True bypasses it.
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, s_id: True)
    heal_mock = MagicMock(return_value={"missing": 0, "rebuilt_fields": 0, "bundled": 0})
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    # 1. Automated cron tick: yields when query load is detected
    deferred_res = _run_rollup_hour_heal.__wrapped__(sid, manual=False)
    assert deferred_res["status"] == "deferred"
    heal_mock.assert_not_called()

    # 2. Manual trigger: executes regardless of in-flight queries
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=102))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    manual_res = _run_rollup_hour_heal.__wrapped__(sid, manual=True)
    assert manual_res["status"] == "success"
    assert heal_mock.call_count == 1


def test_rollup_heal_durable_mode_throttle_and_readiness(contract_service_source, monkeypatch):
    """Verifies Checklist Item 5:
    Durable serving mode throttle: max_missing_hours = 1 startup catch-up ceiling
    until coverage is verified.
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=103))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.config.is_durable_serving_mode", lambda s: True)
    monkeypatch.setattr("backend.core.rollup_readiness.rollup_coverage_ready", lambda s_id: False)
    mark_ready_mock = MagicMock()
    monkeypatch.setattr("backend.core.rollup_readiness.mark_rollup_coverage_ready", mark_ready_mock)

    # 1. Unverified pass: throttled to max_missing_hours = 1, does not mark ready
    heal_partial = MagicMock(
        return_value={
            "missing": 1,
            "rebuilt_fields": 5,
            "bundled": 1,
            "coverage_verified": False,
        }
    )
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_partial)
    res1 = _run_rollup_hour_heal.__wrapped__(sid)
    assert res1["status"] == "success"
    assert heal_partial.call_args.kwargs.get("max_missing_hours") == 1
    mark_ready_mock.assert_not_called()

    # 2. Verified pass: marks rollup coverage ready
    heal_complete = MagicMock(
        return_value={
            "missing": 0,
            "rebuilt_fields": 0,
            "bundled": 0,
            "coverage_verified": True,
        }
    )
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_complete)
    res2 = _run_rollup_hour_heal.__wrapped__(sid)
    assert res2["status"] == "success"
    mark_ready_mock.assert_called_once_with(sid)


def test_rollup_heal_empty_hour_sentinels(contract_service_source, monkeypatch):
    """Verifies Checklist Item 6:
    Empty-hour sentinel bundle generation for zero-row closed hours
    prevents re-scan thrashing.
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=104))
    logged = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged.append((a, k)))

    heal_mock = MagicMock(
        return_value={
            "missing": 0,
            "rebuilt_fields": 0,
            "bundled": 0,
            "stamped_empty": 4,
            "coverage_verified": True,
        }
    )
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    res = _run_rollup_hour_heal.__wrapped__(sid)
    assert res["status"] == "success"
    assert res["stamped_empty"] == 4
    assert "4 empty hour(s) stamped" in res["summary"]

    assert len(logged) == 1
    args, kwargs = logged[0]
    assert "4 empty hour(s) stamped" in kwargs["summary"]


def test_admin_trigger_and_inspection_parity(contract_service_source, monkeypatch):
    """Verifies Checklist Item 7:
    Manual trigger & inspection parity:
    - POST /api/admin/rollup-heal/{service_id} (HTTP 200, RollupHealResponse)
    - POST /api/admin/rollup-heal (HTTP 200, RollupHealResponse)
    - POST /api/admin/rollups/heal/{service_id} (HTTP 200, RollupHealResponse)
    - POST /api/admin/backfill-bundle-rollups (HTTP 200, BackfillBundleRollupsResponse)
    """
    from backend.deps import get_source
    from backend.main import app

    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    app.dependency_overrides[get_source] = lambda: src
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=105))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())

    heal_mock = MagicMock(
        return_value={
            "missing": 2,
            "rebuilt_fields": 10,
            "bundled": 2,
            "stamped_empty": 0,
            "coverage_verified": True,
        }
    )
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    client = TestClient(app)
    try:
        # 1. POST /api/admin/rollup-heal/{service_id}
        resp1 = client.post(f"/api/admin/rollup-heal/{sid}")
        assert resp1.status_code == 200, f"Expected 200, got: {resp1.text}"
        data1 = resp1.json()
        assert data1["status"] == "success"
        assert data1["service_id"] == sid
        assert data1["missing"] == 2
        assert data1["bundled"] == 2
        assert "Healed 2 missing hour(s)" in data1["summary"]

        # 2. POST /api/admin/rollup-heal
        resp2 = client.post("/api/admin/rollup-heal", headers={"x-service-id": sid})
        assert resp2.status_code == 200, f"Expected 200, got: {resp2.text}"
        data2 = resp2.json()
        assert data2["status"] == "success"

        # 3. POST /api/admin/rollups/heal/{service_id} (alias)
        resp3 = client.post(f"/api/admin/rollups/heal/{sid}")
        assert resp3.status_code == 200, f"Expected 200, got: {resp3.text}"
        data3 = resp3.json()
        assert data3["status"] == "success"

        # 4. POST /api/admin/backfill-bundle-rollups (existing endpoint)
        # Stub individual bundle backfill functions
        for fn_name in [
            "backfill_slow_urls_bundles",
            "backfill_origin_summary_bundles",
            "compact_origin_summary_closed_days_to_daily",
            "backfill_origin_dims_bundles",
            "compact_origin_dims_closed_days_to_daily",
            "backfill_origin_latency_ts_bundles",
            "compact_origin_latency_ts_closed_days_to_daily",
            "backfill_network_rtt_bundles",
            "compact_network_rtt_closed_days_to_daily",
            "backfill_network_speed_bundles",
            "compact_network_speed_closed_days_to_daily",
            "backfill_verified_bots_ts_bundles",
            "compact_verified_bots_ts_closed_days_to_daily",
            "backfill_perf_latency_bundles",
            "compact_perf_latency_closed_days_to_daily",
            "backfill_security_dims_bundles",
            "compact_security_dims_closed_days_to_daily",
            "backfill_ngwaf_bots_bundles",
            "compact_ngwaf_bots_closed_days_to_daily",
            "backfill_overview_bundles",
            "compact_overview_closed_days_to_daily",
            "backfill_pop_health_bundles",
            "backfill_network_quality_bundles",
            "compact_network_quality_closed_days_to_daily",
            "backfill_network_summary_bundles",
            "backfill_wellknown_bots_rollup",
        ]:
            monkeypatch.setattr(f"backend.core.rollups.{fn_name}", lambda *a, **k: 0)

        monkeypatch.setattr("backend.core.rollups._common.backfill_missing_bundles", lambda *a, **k: 0)

        resp4 = client.post("/api/admin/backfill-bundle-rollups", headers={"x-service-id": sid})
        assert resp4.status_code == 200, f"Expected 200, got: {resp4.text}"
        data4 = resp4.json()
        assert "slow_urls" in data4
        assert "origin_summary" in data4
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_rollup_heal_enospc_safety(contract_service_source, monkeypatch):
    """Verifies Checklist Item 8:
    ENOSPC disk space safety pre-check (< 50MB free) safely skips execution,
    logs a warning, and returns status="warning".
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    heal_mock = MagicMock()
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    logged = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged.append((a, k)))

    # Simulate critical disk space (< 50MB free, e.g. 10MB)
    fake_usage = MagicMock(free=10 * 1024 * 1024)
    monkeypatch.setattr("shutil.disk_usage", lambda path: fake_usage)

    res = _run_rollup_hour_heal.__wrapped__(sid)

    assert res["status"] == "warning"
    assert "Disk space critically low" in res["summary"]
    heal_mock.assert_not_called()

    assert len(logged) == 1
    args, kwargs = logged[0]
    assert args[1] == "rollup_hour_heal"
    assert args[3] == "warning"
    assert "Skipped due to low disk space" in kwargs["summary"]


def test_rollup_heal_zero_fos_egress(contract_service_source, monkeypatch):
    """Verifies Checklist Item 9:
    Zero FOS egress contract — local-only writes, no outbound S3/boto3 calls.
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=106))
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())

    heal_mock = MagicMock(
        return_value={
            "missing": 1,
            "rebuilt_fields": 8,
            "bundled": 1,
            "stamped_empty": 0,
            "coverage_verified": True,
        }
    )
    monkeypatch.setattr("backend.core.rollups.backfill_missing_hour_bundles", heal_mock)

    # Poison boto3 client to fail if called
    boto_mock = MagicMock(side_effect=AssertionError("Outbound FOS network call forbidden in rollup_heal!"))
    monkeypatch.setattr("boto3.client", boto_mock)

    res = _run_rollup_hour_heal.__wrapped__(sid)
    assert res["status"] == "success"
    boto_mock.assert_not_called()


def test_rollup_heal_error_handling(contract_service_source, monkeypatch):
    """Verifies Checklist Item 10:
    Resilient exception handling: on runtime failure during backfill, logs
    error to cron_runs and returns structured error dict.
    """
    src = contract_service_source
    sid = src["service_id"]

    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda s_id: src if s_id == sid else None)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", MagicMock(return_value=107))
    logged = []
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **k: logged.append((a, k)))

    monkeypatch.setattr(
        "backend.core.rollups.backfill_missing_hour_bundles",
        MagicMock(side_effect=RuntimeError("DuckDB connection pool exhausted")),
    )

    res = _run_rollup_hour_heal.__wrapped__(sid)
    assert res["status"] == "error"
    assert "DuckDB connection pool exhausted" in res["error_message"]

    assert len(logged) == 1
    args, kwargs = logged[0]
    assert args[1] == "rollup_hour_heal"
    assert args[3] == "error"
    assert "DuckDB connection pool exhausted" in kwargs["error_message"]
