"""Contract test suite for Cron 14: sync_metadata_{service_id}.

Authoritative specification: docs/cron/jobs/sync-metadata.md
Architecture & gotchas: AGENTS.md, docs/adr/17-analyst-path-a-ducklake.md, docs/adr/22-postgres-only-metadata.md

Verifies all contract requirements and operational boundaries:
1. Role Gate: Registered strictly for Analyst Path A standalone instances (access_level: "read_only"),
   never for Admin (read_write) or Analyst Path B (remote share).
2. Cadence & Trigger: Interval timer evaluated every interval_seconds (derived from log_period or
   cron_sync.interval_seconds / cron_metadata_sync.interval_seconds) plus application startup trigger
   (initial_sync_{service_id}).
3. Dynamic Registration & Gating: Gated on cron_metadata_sync.enabled (default true), dynamically
   reschedules on interval_seconds update, and unregisters when disabled or service removed.
4. Politeness Deferral: Scheduled runs (run_id is None) defer via should_defer_cron("metadata_sync",
   service_id) when interactive dashboard queries are active; manual trigger (POST /api/admin/rebuild-local-view)
   bypasses politeness deferral.
5. Graceful Uncommitted Table Handling: When DuckLake/Iceberg table has not yet been committed by the admin
   (ducklake_table_exists == False or attach fails with not found), logs "success" with skip summary, avoiding
   false alert errors on fresh services.
6. Time Range Bounds: Pinned time_range boundary persisted to provisioning.time_range on scoped syncs,
   cleared on manual "Sync All".
7. Admin State Sync Resilience: Captures import_admin_state(service_id) failures without failing the data
   sync, logging status "warning" in cron_runs.
8. Zero Class A Cloud Mutations: Analyst standalone instance executes only Class B GET operations
   attributed to cron.sync_metadata in usage_log, never executing FOS Class A PUT or DELETE operations.
9. Guaranteed Cleanup: end_progress(run_id) and finalize_cron_duration executed in guaranteed finally block.
10. Manual Trigger Endpoints: POST /api/admin/rebuild-local-view returns 202 Accepted, clears caches,
    spawns background worker, and returns 503 on cron_busy.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import metadata
from backend.cron.scheduler import Scheduler
from backend.deps import get_source
from backend.main import app

# ── Fixtures & Helpers ────────────────────────────────────────────────────────


def _fake_src(service_id: str = "svc-meta-contract", access_level: str = "read_only") -> dict:
    return {
        "name": service_id,
        "service_id": service_id,
        "service_name": service_id,
        "logging_service_id": f"log-{service_id}",
        "bucket": "test-fos-bucket",
        "access_level": access_level,
        "endpoint": "https://s3.example.com",
        "access_key_id": "test-key",
        "secret_access_key": "test-secret",
        "region": "us-east-1",
    }


def _fake_cfg(
    service_id: str = "svc-meta-contract",
    access_level: str = "read_only",
    log_period: int = 60,
    sync_enabled: bool = True,
    sync_interval_seconds: int | None = None,
    meta_enabled: bool = True,
    meta_interval_seconds: int | None = None,
    time_range: dict | None = None,
) -> dict:
    prov: dict = {
        "access_level": access_level,
        "cron_sync": {"enabled": sync_enabled},
    }
    if sync_interval_seconds is not None:
        prov["cron_sync"]["interval_seconds"] = sync_interval_seconds

    cron_meta: dict = {"enabled": meta_enabled}
    if meta_interval_seconds is not None:
        cron_meta["interval_seconds"] = meta_interval_seconds
    prov["cron_metadata_sync"] = cron_meta

    if time_range is not None:
        prov["time_range"] = time_range

    return {
        "service_id": service_id,
        "name": service_id,
        "log_period": log_period,
        "access_level": access_level,
        "provisioning": prov,
    }


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = _fake_src("svc-meta-contract", access_level="read_only")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def base_context(monkeypatch, stub_source):
    """Mocks common lifecycle helpers for direct _run_metadata_sync execution."""
    cfg = _fake_cfg("svc-meta-contract", access_level="read_only")
    load_cfg = MagicMock(return_value=cfg)
    save_cfg = MagicMock()
    monkeypatch.setattr("backend.config.load_config", load_cfg)
    monkeypatch.setattr("backend.config.save_config", save_cfg)

    fake_con = MagicMock()
    get_conn = MagicMock(return_value=fake_con)
    start_run = MagicMock(return_value=88)
    log_run = MagicMock()
    refresh_status = MagicMock()
    finalize_dur = MagicMock()

    start_prog = MagicMock()
    end_prog = MagicMock()
    log_prog = MagicMock()
    reap_prog = MagicMock()

    monkeypatch.setattr("backend.core.duckdb.get_connection", get_conn)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_run)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)
    monkeypatch.setattr("backend.core.duckdb.refresh_config_status", refresh_status)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    monkeypatch.setattr("backend.cron_progress.start_progress", start_prog)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", reap_prog)
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", log_prog)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, sid: False)

    return {
        "cfg": cfg,
        "load_cfg": load_cfg,
        "save_cfg": save_cfg,
        "src": stub_source,
        "fake_con": fake_con,
        "get_conn": get_conn,
        "start_run": start_run,
        "log_run": log_run,
        "refresh_status": refresh_status,
        "finalize_dur": finalize_dur,
        "start_prog": start_prog,
        "end_prog": end_prog,
        "log_prog": log_prog,
        "reap_prog": reap_prog,
    }


# ── Requirement 1: Role Gate & Tenant Isolation ─────────────────────────────


def test_contract_role_gate_analyst_path_a_only():
    """Requirement 1: Registered strictly for Analyst Path A standalone instances (access_level: "read_only"),
    never for Admin (read_write) in APScheduler.
    """
    s = Scheduler()

    # Case A: Analyst Path A (read_only) -> Registered
    cfg_analyst = _fake_cfg("svc-analyst-1", access_level="read_only")
    with (
        patch("backend.config.list_configs", return_value=[cfg_analyst]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-analyst-1", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-analyst-1" in s._job_ids
    # Verify write-side jobs are omitted for Analyst Path A
    assert "log_discovery_svc-analyst-1" not in s._job_ids
    assert "commit_svc-analyst-1" not in s._job_ids
    assert "optimize_svc-analyst-1" not in s._job_ids
    assert "expire_svc-analyst-1" not in s._job_ids

    # Case B: Admin (read_write) -> NOT registered in APScheduler
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_admin = _fake_cfg("svc-admin-1", access_level="read_write")
    with (
        patch("backend.config.list_configs", return_value=[cfg_admin]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-admin-1", "read_write")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-admin-1" not in s._job_ids


def test_contract_role_gate_analyst_path_b_blocked():
    """Requirement 1: Analyst Path B (remote share session) connects to host admin server and
    is blocked from accessing /api/admin/rebuild-local-view with HTTP 403 (admin_only).
    """
    from backend.utils.remote_access import _ANALYST_BLOCKED_PREFIXES, _is_blocked_path

    # Verify /api/admin/ is part of blocked prefixes
    assert any("/api/admin/" == p for p in _ANALYST_BLOCKED_PREFIXES)

    # Verify _is_blocked_path correctly flags /api/admin/rebuild-local-view
    assert _is_blocked_path("/api/admin/rebuild-local-view") is True
    assert _is_blocked_path("/api/admin/rebuild-local-view?service_id=svc-1") is True


def test_contract_role_gate_admin_post_commit_dispatch():
    """Requirement 1: Admins do not need a scheduled cron because they trigger view refresh
    on-demand via dispatch_post_commit_metadata_sync, which coalesces concurrent requests.
    """
    calls = []
    dispatched_threads = []

    class DeferredThread:
        def __init__(self, *, target, args, **kwargs):
            self.target = target
            self.args = args
            dispatched_threads.append(self)

        def start(self):
            return None

    def fake_sync(service_id: str) -> None:
        calls.append(service_id)
        if len(calls) == 1:
            metadata.dispatch_post_commit_metadata_sync(service_id)

    with (
        patch.object(metadata.threading, "Thread", DeferredThread),
        patch.object(metadata, "_run_post_commit_metadata_sync", fake_sync),
    ):
        metadata.dispatch_post_commit_metadata_sync("svc-admin-coalesce")
        metadata.dispatch_post_commit_metadata_sync("svc-admin-coalesce")

        assert len(dispatched_threads) == 1
        assert calls == []
        dispatched_threads[0].target(*dispatched_threads[0].args)

        assert calls == ["svc-admin-coalesce", "svc-admin-coalesce"]
        assert metadata._post_commit_metadata_sync_rerun == {}


# ── Requirement 2: Cadence, Trigger & Application Startup Execution ─────────


def test_contract_cadence_derivation_and_execution_flags():
    """Requirement 2: Cadence is derived from log_period (log_period // 2 when >= 60, or log_period when < 60),
    or overridden by cron_metadata_sync.interval_seconds.
    Trigger execution flags: max_instances=1, coalesce=True, misfire_grace_time=60.
    """
    s = Scheduler()

    # Case A: Derived from log_period >= 60 -> log_period // 2
    cfg_derived = _fake_cfg("svc-cadence-60", access_level="read_only", log_period=60)
    with (
        patch("backend.config.list_configs", return_value=[cfg_derived]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-60", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_derived = s._sched.get_job("sync_metadata_svc-cadence-60")
    assert job_derived is not None
    assert int(job_derived.trigger.interval.total_seconds()) == 30
    assert job_derived.coalesce is True
    assert job_derived.misfire_grace_time == 60

    # Case B: Derived from log_period < 60 -> log_period
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_short = _fake_cfg("svc-cadence-15", access_level="read_only", log_period=15)
    with (
        patch("backend.config.list_configs", return_value=[cfg_short]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-15", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_short = s._sched.get_job("sync_metadata_svc-cadence-15")
    assert job_short is not None
    assert int(job_short.trigger.interval.total_seconds()) == 15

    # Case C: Overridden by cron_metadata_sync.interval_seconds
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_custom = _fake_cfg("svc-cadence-custom", access_level="read_only", log_period=60, meta_interval_seconds=45)
    with (
        patch("backend.config.list_configs", return_value=[cfg_custom]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-custom", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job_custom = s._sched.get_job("sync_metadata_svc-cadence-custom")
    assert job_custom is not None
    assert int(job_custom.trigger.interval.total_seconds()) == 45


def test_contract_startup_trigger_initial_sync():
    """Requirement 2: Scheduler.start() registers one-shot initial_sync_{service_id} immediately at boot
    for Analyst Path A services with cron_sync.enabled = True, populating the dashboard without blocking boot.
    Admin services and disabled services do NOT register initial_sync_{service_id}.
    """
    s = Scheduler()
    added_jobs = []

    def fake_add_job(fn, *args, **kwargs):
        added_jobs.append({"fn": fn, "args": args, "kwargs": kwargs})

    s._add_job = fake_add_job
    s._sched = MagicMock()
    s._sync_jobs = MagicMock()

    cfg_analyst = _fake_cfg("svc-init-analyst", access_level="read_only", sync_enabled=True)
    cfg_admin = _fake_cfg("svc-init-admin", access_level="read_write", sync_enabled=True)
    cfg_disabled = _fake_cfg("svc-init-disabled", access_level="read_only", sync_enabled=False)

    with (
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
        patch("backend.core.duckdb_pool._pool_warm_at_boot_enabled", return_value=False),
        patch("backend.config.list_configs", return_value=[cfg_analyst, cfg_admin, cfg_disabled]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src()),
        patch("backend.core.duckdb.is_configured", return_value=True),
    ):
        s.start()

    job_ids = [j["kwargs"].get("id") for j in added_jobs]
    assert "initial_sync_svc-init-analyst" in job_ids
    assert "initial_sync_svc-init-admin" not in job_ids
    assert "initial_sync_svc-init-disabled" not in job_ids


# ── Requirement 3: Dynamic Registration Gating & Rescheduling ───────────────


def test_contract_dynamic_registration_gating_and_rescheduling():
    """Requirement 3: Gated on cron_metadata_sync.enabled (default true). Dynamically reschedules
    when interval_seconds is updated, and unregisters when disabled.
    """
    s = Scheduler()

    # Case A: Disabled via cron_metadata_sync.enabled = False
    cfg_disabled = _fake_cfg("svc-meta-dis", access_level="read_only", meta_enabled=False)
    with (
        patch("backend.config.list_configs", return_value=[cfg_disabled]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-meta-dis", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-meta-dis" not in s._job_ids

    # Case B: Reschedule existing job when interval changes
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids["sync_metadata_svc-meta-resched"] = "sync_metadata_svc-meta-resched"

    cfg_resched = _fake_cfg("svc-meta-resched", access_level="read_only", log_period=60, meta_interval_seconds=45)
    with (
        patch("backend.config.list_configs", return_value=[cfg_resched]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-meta-resched", "read_only")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    mock_job.reschedule.assert_called_once_with("interval", seconds=45)


# ── Requirement 4: Politeness Deferral Contract ─────────────────────────────


def test_contract_politeness_deferral_on_scheduled_runs(base_context):
    """Requirement 4: Scheduled runs (run_id is None) defer via should_defer_cron("metadata_sync", service_id)
    when active dashboard queries are in flight, avoiding view lock contention.
    """
    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        metadata._run_metadata_sync("svc-meta-contract")

        mock_defer.assert_called_once_with("metadata_sync", "svc-meta-contract")
        base_context["start_run"].assert_not_called()
        base_context["log_run"].assert_not_called()


def test_contract_politeness_deferral_bypassed_on_manual_trigger(base_context, monkeypatch):
    """Requirement 4: Manual trigger (run_id is not None) completely bypasses politeness deferral
    and executes to completion.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    with patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer:
        # Manual run passing explicit run_id
        metadata._run_metadata_sync("svc-meta-contract", run_id=777)

        # Politeness deferral was bypassed (never called)
        mock_defer.assert_not_called()
        base_context["log_run"].assert_called_once()
        args, kwargs = base_context["log_run"].call_args
        assert args[3] == "success"
        assert kwargs["run_id"] == 777


# ── Requirement 5: Graceful Uncommitted Table Handling ──────────────────────


def test_contract_uncommitted_table_logs_success_and_skips(base_context, monkeypatch):
    """Requirement 5: When the DuckLake/Iceberg table has not yet been committed by the admin
    (ducklake_table_exists == False), logs status "success" with skip summary, avoiding false alert
    errors on fresh services.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=False))
    sync_data = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_data)

    metadata._run_metadata_sync("svc-meta-contract")

    sync_data.assert_not_called()
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success"
    assert "not found" in kwargs["summary"].lower()
    assert base_context["end_prog"].called


def test_contract_table_not_found_exception_treated_as_graceful_skip(base_context, monkeypatch):
    """Requirement 5: If table init raises an exception matching 'not found', 'does not exist',
    or 'nosuchtable', it is treated gracefully as an uncommitted table with status 'success'.
    """
    monkeypatch.setattr(
        "backend.core.iceberg.init_iceberg_table",
        MagicMock(side_effect=ValueError("Table logs not found in catalog")),
    )
    sync_data = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_data)

    metadata._run_metadata_sync("svc-meta-contract")

    sync_data.assert_not_called()
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "success"
    assert "not found" in kwargs["summary"].lower()


def test_contract_catalog_attach_failure_raises_and_logs_error(base_context, monkeypatch):
    """Requirement 5: If init_iceberg_table returns None (catalog attach failure without table-not-found error),
    it raises RuntimeError and records status 'error'.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=None))

    metadata._run_metadata_sync("svc-meta-contract")

    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "error"
    assert "could not attach the DuckLake catalog" in kwargs["error_message"]
    assert "Metadata sync failed" in kwargs["summary"]


# ── Requirement 6: Time Range Bounds Management ─────────────────────────────


def test_contract_explicit_time_range_persisted_to_config(base_context, monkeypatch):
    """Requirement 6: Passing explicit start_time and/or end_time persists the bounds into
    cfg['provisioning']['time_range'] via save_config and updates src['time_range'].
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    sync_mock = MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync(
        "svc-meta-contract",
        run_id=12,
        start_time="2026-06-01T00:00:00Z",
        end_time="2026-06-15T00:00:00Z",
    )

    save_cfg = base_context["save_cfg"]
    assert save_cfg.called
    saved_cfg = save_cfg.call_args[0][1]
    tr = saved_cfg["provisioning"]["time_range"]
    assert tr["start"] == "2026-06-01T00:00:00Z"
    assert tr["end"] == "2026-06-15T00:00:00Z"
    assert base_context["src"]["time_range"] == tr
    assert sync_mock.call_args[1]["start_time"] == "2026-06-01T00:00:00Z"
    assert sync_mock.call_args[1]["end_time"] == "2026-06-15T00:00:00Z"


def test_contract_manual_sync_all_clears_pinned_time_range(base_context, monkeypatch):
    """Requirement 6: Manual trigger (run_id is not None) without start_time represents 'Sync All',
    clearing any previously pinned time_range limit from config and setting src['time_range'] = None.
    """
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-01-01T00:00:00Z"}
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    sync_mock = MagicMock(return_value={"files_downloaded": 1, "rows_downloaded": 10})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract", run_id=999)

    save_cfg = base_context["save_cfg"]
    assert save_cfg.called
    saved_cfg = save_cfg.call_args[0][1]
    assert "time_range" not in saved_cfg["provisioning"]
    assert base_context["src"]["time_range"] is None


def test_contract_scheduled_run_respects_existing_config_time_range(base_context, monkeypatch):
    """Requirement 6: Scheduled run (run_id is None) reads and passes configured provisioning.time_range.start
    to sync_data.
    """
    base_context["cfg"]["provisioning"]["time_range"] = {"start": "2026-05-01T00:00:00Z"}
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    sync_mock = MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0})
    monkeypatch.setattr("backend.core.iceberg.sync_data", sync_mock)
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract")

    assert sync_mock.call_args[1]["start_time"] == "2026-05-01T00:00:00Z"


# ── Requirement 7: Admin State Sync Resilience ──────────────────────────────


def test_contract_admin_state_import_failure_yields_warning_status(base_context, monkeypatch):
    """Requirement 7: import_admin_state failure does not crash metadata sync; it logs a warning event,
    allows view rebuild to complete, and records run_status = 'warning' in cron_runs.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 2, "rows_downloaded": 500}),
    )
    update_view = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", update_view)
    monkeypatch.setattr(
        "backend.state_sync.import_admin_state",
        MagicMock(side_effect=RuntimeError("connection refused to admin state bucket")),
    )

    metadata._run_metadata_sync("svc-meta-contract")

    update_view.assert_called_once()
    base_context["refresh_status"].assert_called_once_with("svc-meta-contract")
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "warning"
    assert "admin state import warning" in kwargs["summary"]
    assert "connection refused" in kwargs["summary"]
    assert kwargs["files_downloaded"] == 2
    assert kwargs["rows_ingested"] == 500


# ── Requirement 8: Zero Class A Cloud Mutations & Attribution Contract ──────


def test_contract_zero_class_a_cloud_mutations(base_context, monkeypatch):
    """Requirement 8: Analyst standalone instance executes only Class B GET operations
    (sync_data downloading parquet files) and never executes FOS Class A PUT or DELETE operations.
    """
    mock_s3 = MagicMock()
    monkeypatch.setattr("backend.core.duckdb._get_fos_client", lambda src: mock_s3)
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    # Directly verify sync_data file download uses download_file (Class B GET)
    # and zero Class A mutating operations (put_object, delete_object, etc.)
    with (
        patch("backend.core.iceberg.sync._core_mod._get_service_lock"),
        patch("backend.core.iceberg.sync._core_mod._get_catalog"),
        patch("backend.core.iceberg.sync._core_mod._table_identifier"),
        patch("backend.core.iceberg.sync._core_mod._refresh_local_catalog_metadata"),
        patch("backend.core.iceberg.sync._core_mod._load_table_cached") as mock_load,
    ):
        fake_table = MagicMock()
        fake_table.metadata_location = "s3://test-fos-bucket/iceberg/meta/v1.json"
        fake_file = MagicMock()
        fake_file.file.file_path = "s3://test-fos-bucket/data/logs/part-1.parquet"
        fake_file.file.record_count = 100
        fake_table.scan.return_value.plan_files.return_value = [fake_file]
        mock_load.return_value = fake_table

        from backend.core import iceberg as db_iceberg

        with (
            patch("os.path.exists", return_value=False),
            patch("os.rename"),
            patch("os.path.getsize", return_value=1024),
        ):
            data_res = db_iceberg.sync_data(base_context["src"])

    assert data_res["files_downloaded"] == 1
    # Assert s3.download_file was invoked (Class B)
    mock_s3.download_file.assert_called_once()

    # Assert zero Class A mutations
    mock_s3.put_object.assert_not_called()
    mock_s3.delete_object.assert_not_called()
    mock_s3.delete_objects.assert_not_called()
    mock_s3.create_multipart_upload.assert_not_called()


def test_contract_telemetry_attribution_and_dashboard_cache_invalidation(base_context, monkeypatch):
    """Requirement 8: View refresh executes update_iceberg_view, logs cron_runs entry with files/rows,
    and invalidates dashboard cache via invalidate_service.
    """
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 3, "rows_downloaded": 2_500}),
    )
    update_view = MagicMock()
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", update_view)
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())
    invalidate_mock = MagicMock()
    monkeypatch.setattr("backend.repositories.dashboard.invalidate_service", invalidate_mock)

    metadata._run_metadata_sync("svc-meta-contract")

    update_view.assert_called_once()
    invalidate_mock.assert_called_once_with(base_context["src"]["name"])
    base_context["log_run"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[1] == "metadata_sync"
    assert args[3] == "success"
    assert kwargs["files_downloaded"] == 3
    assert kwargs["rows_ingested"] == 2_500
    assert "downloaded 3 new Iceberg data file(s)" in kwargs["summary"]


# ── Requirement 9: Guaranteed Cleanup Contract ──────────────────────────────


def test_contract_guaranteed_cleanup_on_success_and_error(base_context, monkeypatch):
    """Requirement 9: end_progress(run_id) and finalize_cron_duration(src, run_id, start_time_exec)
    are guaranteed to execute in a finally block across both successful and error executions.
    """
    # Case A: Success execution
    monkeypatch.setattr("backend.core.iceberg.init_iceberg_table", MagicMock(return_value=True))
    monkeypatch.setattr("backend.core.iceberg.ducklake_table_exists", MagicMock(return_value=True))
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(return_value={"files_downloaded": 0, "rows_downloaded": 0}),
    )
    monkeypatch.setattr("backend.core.iceberg.update_iceberg_view", MagicMock())
    monkeypatch.setattr("backend.state_sync.import_admin_state", MagicMock())

    metadata._run_metadata_sync("svc-meta-contract", run_id=101)

    base_context["end_prog"].assert_called_with(101)
    base_context["finalize_dur"].assert_called_once()

    # Case B: Error execution
    base_context["end_prog"].reset_mock()
    base_context["finalize_dur"].reset_mock()
    monkeypatch.setattr(
        "backend.core.iceberg.sync_data",
        MagicMock(side_effect=RuntimeError("disk write error")),
    )

    metadata._run_metadata_sync("svc-meta-contract", run_id=102)

    base_context["end_prog"].assert_called_with(102)
    base_context["finalize_dur"].assert_called_once()
    args, kwargs = base_context["log_run"].call_args
    assert args[3] == "error"
    assert "disk write error" in kwargs["error_message"]


# ── Requirement 10: Manual Trigger Endpoints & Admin Controls ────────────────


def test_contract_rebuild_local_view_endpoint_lifecycle():
    """Requirement 10: POST /api/admin/rebuild-local-view returns 202 Accepted, clears source caches,
    removes snapshot_files_cache.json, starts cron run, starts progress, and launches background worker.
    """
    test_src = _fake_src("svc-rebuild-test")

    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
        patch("os.path.exists", return_value=True),
        patch("os.remove") as mock_remove,
        patch("backend.core.duckdb.start_cron_run", return_value="run-789") as mock_start_cron,
        patch("backend.cron_progress.start_progress") as mock_prog,
        patch("threading.Thread") as mock_thread,
    ):
        app.dependency_overrides[get_source] = lambda: test_src
        client = TestClient(app)
        try:
            resp = client.post("/api/admin/rebuild-local-view")
            assert resp.status_code == 202
            data = resp.json()
            assert data["ok"] is True
            assert data["run_id"] == "run-789"
            assert data["message"] == "Local view rebuild started."

            mock_clear.assert_called_once_with("svc-rebuild-test")
            mock_remove.assert_called_once_with("/tmp/test_cache/snapshot_files_cache.json")
            mock_start_cron.assert_called_once_with(test_src, "metadata_sync")
            mock_prog.assert_called_once_with("run-789", service_id="svc-rebuild-test", task="metadata_sync")
            mock_thread.assert_called_once()
        finally:
            app.dependency_overrides.pop(get_source, None)


def test_contract_rebuild_local_view_endpoint_busy_handling():
    """Requirement 10: POST /api/admin/rebuild-local-view returns HTTP 503 when start_cron_run raises
    RuntimeError (cron busy).
    """
    test_src = _fake_src("svc-busy-test")

    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp/test_cache"),
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("cron job busy")),
    ):
        app.dependency_overrides[get_source] = lambda: test_src
        client = TestClient(app)
        try:
            resp = client.post("/api/admin/rebuild-local-view")
            assert resp.status_code == 503
            err_data = resp.json()
            assert "cron_busy" in str(err_data)
        finally:
            app.dependency_overrides.pop(get_source, None)
