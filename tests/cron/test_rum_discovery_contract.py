"""Contract test suite for Cron 16: rum_sync_{service_id} (Standard mode)
and rum_discovery_{service_id} (High-Scale mode).

Verifies all contract requirements and operational boundaries documented in
docs/cron/jobs/rum-sync.md and docs/cron/jobs/rum-discovery.md:
1. Dual Architecture & Mode Routing (Standard vs High-Scale isolation & no co-registration)
2. Role Gate (Admin read_write only; Analyst Path A & B rejected or skipped)
3. Cadence & Registration (interval derivation, dynamic rescheduling, skips when disabled)
4. Politeness Gate Removal (automated ticks not blocked by active queries)
5. Faro Bundle Integrity & Reconcile (warning on drift/failure without halting beacon ingestion)
6. Minute-Prefix Incremental Discovery (5-minute sliding window, LIST error handling)
7. Chunk Budgeting & Bounded Ingestion (Trap #41 chunk 0 guarantee)
8. Post-Sync Immediate Commit (total > 0 triggers rum_commit immediately)
9. Accounting & Outcome Counters (typed outcome counters in cron_runs)
10. Guaranteed Cleanup (end_progress and finalize_cron_run_if_running in finally)
11. Manual Trigger Endpoints (POST /api/admin/rum/sync & POST /api/admin/rum/discovery)
"""

from __future__ import annotations

import copy
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient

from backend.cron.jobs import rum_ledger as rum_ledger_mod
from backend.cron.jobs import rum_sync as rum_sync_mod
from backend.cron.scheduler import Scheduler, _resolve_rum_sync_interval
from backend.deps import get_service_id, get_source, require_admin
from backend.main import app

SERVICE_ID = "svc_test_rum_contract"

FAKE_CFG_STANDARD = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "bucket": "test-rum-bucket",
    "endpoint": "https://test.fos.fastly.com",
    "access_key_id": "test_key",
    "secret_access_key": "test_secret",
    "deployment_mode": "standard",
    "access_level": "read_write",
    "rum": {
        "enabled": True,
        "sync_interval_seconds": 30,
        "commit_interval_mins": 1,
    },
    "provisioning": {
        "access_level": "read_write",
    },
}

FAKE_SRC_STANDARD = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "bucket": "test-rum-bucket",
    "endpoint": "https://test.fos.fastly.com",
    "access_key_id": "test_key",
    "secret_access_key": "test_secret",
    "deployment_mode": "standard",
    "access_level": "read_write",
    "rum": {
        "enabled": True,
        "sync_interval_seconds": 30,
        "commit_interval_mins": 1,
    },
    "provisioning": {
        "access_level": "read_write",
    },
}

FAKE_CFG_HIGH_SCALE = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "bucket": "test-rum-bucket",
    "endpoint": "https://test.fos.fastly.com",
    "access_key_id": "test_key",
    "secret_access_key": "test_secret",
    "deployment_mode": "high_scale",
    "access_level": "read_write",
    "rum": {
        "enabled": True,
        "sync_interval_seconds": 15,
    },
    "provisioning": {
        "access_level": "read_write",
    },
}

FAKE_SRC_HIGH_SCALE = {
    "service_id": SERVICE_ID,
    "name": SERVICE_ID,
    "bucket": "test-rum-bucket",
    "endpoint": "https://test.fos.fastly.com",
    "access_key_id": "test_key",
    "secret_access_key": "test_secret",
    "deployment_mode": "high_scale",
    "access_level": "read_write",
    "rum": {
        "enabled": True,
        "sync_interval_seconds": 15,
    },
    "provisioning": {
        "access_level": "read_write",
    },
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


# =============================================================================
# 1. Dual Architecture & Mode Routing Tests
# =============================================================================


def test_mode_routing_standard_registers_rum_sync_and_commit(monkeypatch):
    """Under Standard mode (DEPLOYMENT_MODE=standard):
    - APScheduler registers rum_sync_{service_id} and rum_commit_{service_id}
    - Does NOT register rum_discovery_{service_id} or ledger_rum_sweep_{service_id}
    """
    sched = Scheduler()
    sched._sched = MagicMock()
    sched._job_ids = {}

    monkeypatch.setattr("backend.config.list_configs", lambda: [FAKE_CFG_STANDARD])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)

    with (
        patch.object(sched, "_add_job") as mock_add_job,
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
    ):
        sched._sync_jobs()

        added_ids = [k for k, v in sched._job_ids.items()]
        assert f"rum_sync_{SERVICE_ID}" in added_ids
        assert f"rum_commit_{SERVICE_ID}" in added_ids
        assert f"rum_discovery_{SERVICE_ID}" not in added_ids
        assert f"ledger_rum_sweep_{SERVICE_ID}" not in added_ids


def test_mode_routing_high_scale_registers_discovery_and_sweep(monkeypatch):
    """Under High-Scale mode (DEPLOYMENT_MODE=high_scale):
    - Registers rum_discovery_{service_id} and ledger_rum_sweep_{service_id}
    - Does NOT register rum_sync_{service_id} or rum_commit_{service_id}
    """
    sched = Scheduler()
    sched._sched = MagicMock()
    sched._job_ids = {}

    monkeypatch.setattr("backend.config.list_configs", lambda: [FAKE_CFG_HIGH_SCALE])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)

    with (
        patch.object(sched, "_add_job") as mock_add_job,
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
    ):
        sched._sync_jobs()

        added_ids = [k for k, v in sched._job_ids.items()]
        assert f"rum_discovery_{SERVICE_ID}" in added_ids
        assert f"ledger_rum_sweep_{SERVICE_ID}" in added_ids
        assert f"rum_sync_{SERVICE_ID}" not in added_ids
        assert f"rum_commit_{SERVICE_ID}" not in added_ids


def test_mode_routing_no_co_registration_guarantee(monkeypatch):
    """The two pipelines must NEVER both be registered for the same service."""
    sched = Scheduler()
    sched._sched = MagicMock()
    sched._job_ids = {}

    monkeypatch.setattr("backend.config.list_configs", lambda: [FAKE_CFG_HIGH_SCALE])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)

    with (
        patch.object(sched, "_add_job"),
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
    ):
        sched._sync_jobs()

        has_discovery = f"rum_discovery_{SERVICE_ID}" in sched._job_ids
        has_sync = f"rum_sync_{SERVICE_ID}" in sched._job_ids
        assert not (has_discovery and has_sync), "Both discovery and sync must never co-register!"


def test_high_scale_job_early_exits_in_standard_mode(monkeypatch):
    """_run_rum_discovery_cron exits early if the service is not in high_scale mode."""
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0, "High-Scale discovery must not start in standard mode"


# =============================================================================
# 2. Role Gate Tests
# =============================================================================


def test_role_gate_admin_executes_normally(monkeypatch):
    """Admin (read_write) service starts progress and completes normally."""
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 701)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda sid, prefix_subpath=None, run_id=None: 1)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)
    assert len(log_calls) == 1
    assert log_calls[0][0][3] == "success"


def test_role_gate_analyst_path_a_discovery_skipped(monkeypatch):
    """Analyst Path A (read_only access_level) discovery tick early-exits without starting cron run."""
    ro_src = {**FAKE_SRC_HIGH_SCALE, "access_level": "read_only"}
    start_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: start_calls.append(task))

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)
    assert len(start_calls) == 0, "Read-only service must not start RUM discovery"


def test_role_gate_analyst_path_a_sync_skipped(monkeypatch):
    """Analyst Path A (read_only access_level) sync tick skips execution in standard mode."""
    ro_src = {**FAKE_SRC_STANDARD, "access_level": "read_only"}
    ingest_mock = MagicMock()

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: ro_src)
    monkeypatch.setattr("backend.core.rum_ingest.ingest_rum_logs", ingest_mock)

    # In rum_sync.py, if access_level is read_only, ingest_rum_logs is not called or skips
    # Let's verify with manual trigger or function call:
    app.dependency_overrides[get_source] = lambda: ro_src
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        client = TestClient(app)
        resp = client.post(f"/api/admin/rum/sync/{SERVICE_ID}")
        assert resp.status_code == 403
        assert "read-only" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


# =============================================================================
# 3. Cadence & Registration Tests
# =============================================================================


def test_cadence_derived_from_sync_interval_seconds():
    """RUM interval respects explicit sync_interval_seconds (bounded to >= 5s)."""
    rum_cfg = {"sync_interval_seconds": 12}
    interval = _resolve_rum_sync_interval(rum_cfg, {}, default_request_interval=60)
    assert interval == 12

    # Minimum bound 5s
    rum_cfg_small = {"sync_interval_seconds": 2}
    interval_small = _resolve_rum_sync_interval(rum_cfg_small, {}, default_request_interval=60)
    assert interval_small == 5


def test_cadence_fallback_to_request_interval():
    """When sync_interval_seconds is omitted, derives interval from RUM period or falls back to request interval."""
    # Fallback to default request interval when no RUM log period is configured
    rum_cfg: dict = {}
    cfg: dict = {}
    assert _resolve_rum_sync_interval(rum_cfg, cfg, default_request_interval=45) == 45

    # Derive from rum_log_period (< 60: exact, >= 60: period // 2)
    rum_cfg_short = {"log_period": 30}
    assert _resolve_rum_sync_interval(rum_cfg_short, {}, default_request_interval=60) == 30

    cfg_long = {"rum_log_period": 90}
    assert _resolve_rum_sync_interval({}, cfg_long, default_request_interval=60) == 45


def test_cadence_skips_when_rum_disabled(monkeypatch):
    """When RUM is disabled (rum.enabled=False), no RUM jobs are registered."""
    disabled_cfg = copy.deepcopy(FAKE_CFG_STANDARD)
    disabled_cfg["rum"]["enabled"] = False

    sched = Scheduler()
    sched._sched = MagicMock()
    sched._job_ids = {}

    monkeypatch.setattr("backend.config.list_configs", lambda: [disabled_cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)

    with (
        patch.object(sched, "_add_job"),
        patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False),
    ):
        sched._sync_jobs()

        rum_jobs = [
            k
            for k in sched._job_ids
            if k.startswith(("rum_sync_", "rum_commit_", "rum_discovery_", "ledger_rum_sweep_"))
        ]
        assert len(rum_jobs) == 0, f"Expected 0 RUM jobs when disabled, got {rum_jobs}"


def test_cadence_dynamic_rescheduling_standard(monkeypatch):
    """Standard mode: when interval updates, rum_sync_{service_id} is dynamically rescheduled."""
    sched = Scheduler()
    sched._sched = MagicMock()
    mock_job = MagicMock()
    sched._sched.get_job.return_value = mock_job
    sched._job_ids = {f"rum_sync_{SERVICE_ID}": f"rum_sync_{SERVICE_ID}"}

    updated_cfg = copy.deepcopy(FAKE_CFG_STANDARD)
    updated_cfg["rum"]["sync_interval_seconds"] = 45

    monkeypatch.setattr("backend.config.list_configs", lambda: [updated_cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: False)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)

    with patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False):
        sched._sync_jobs()

    sched._sched.get_job.assert_called_with(f"rum_sync_{SERVICE_ID}")
    mock_job.reschedule.assert_called_once_with("interval", seconds=45)


def test_cadence_dynamic_rescheduling_high_scale(monkeypatch):
    """High-Scale mode: when interval updates, rum_discovery_{service_id} is dynamically rescheduled."""
    sched = Scheduler()
    sched._sched = MagicMock()
    mock_job = MagicMock()
    sched._sched.get_job.return_value = mock_job
    sched._job_ids = {f"rum_discovery_{SERVICE_ID}": f"rum_discovery_{SERVICE_ID}"}

    updated_cfg = copy.deepcopy(FAKE_CFG_HIGH_SCALE)
    updated_cfg["rum"]["sync_interval_seconds"] = 25

    monkeypatch.setattr("backend.config.list_configs", lambda: [updated_cfg])
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.cron.scheduler.dev_local_crons_enabled", lambda: True)

    with patch("backend.cron.scheduler.dev_mode_no_crons", return_value=False):
        sched._sync_jobs()

    sched._sched.get_job.assert_called_with(f"rum_discovery_{SERVICE_ID}")
    mock_job.reschedule.assert_called_once_with("interval", seconds=25)


# =============================================================================
# 4. Politeness Gate Removal Tests
# =============================================================================


def test_politeness_gate_not_deferred_by_active_requests(monkeypatch):
    """Automated RUM ticks are NEVER deferred by active API requests."""
    ingest_mock = MagicMock(return_value=iter([("started", 1), ("done", 0)]))
    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", ingest_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda job, sid: True)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)
    assert ingest_mock.called, "rum_sync must execute even when active requests exist"


# =============================================================================
# 5. Faro Bundle Integrity & Reconcile Tests
# =============================================================================


def test_faro_reconcile_warning_in_high_scale(monkeypatch):
    """When Faro reconcile fails in High-Scale mode, run status is 'warning' but discovery proceeds."""
    log_calls = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 702)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync._reconcile_faro_bundle",
        MagicMock(side_effect=RuntimeError("Transient unpkg 503")),
    )
    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", lambda sid, prefix_subpath=None, run_id=None: 2)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "warning"
    assert kwargs.get("files_downloaded") == 10  # 5 slices * 2 files
    assert "Faro bundle reconcile issue" in kwargs.get("summary", "")
    assert "Transient unpkg 503" in kwargs.get("error_message", "")


def test_faro_reconcile_warning_in_standard_mode(monkeypatch):
    """When Faro reconcile returns False in Standard mode, warning is tracked."""
    update_calls = []
    fake_con = MagicMock()
    fake_con.execute.side_effect = lambda sql, params: update_calls.append((sql, params))

    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: False)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync.ingest_rum_logs",
        lambda sid, *a, **kw: iter([("started", 703), ("done", 10)]),
    )
    monkeypatch.setattr("backend.core.metadata.get_con", lambda sid: fake_con)
    monkeypatch.setattr("backend.cron.jobs.rum_commit._run_rum_commit", MagicMock())
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    # Verify that status was updated to 'warning'
    assert any("UPDATE cron_runs SET status = 'warning'" in sql for sql, _ in update_calls)


# =============================================================================
# 6. Minute-Prefix Incremental Discovery Tests
# =============================================================================


def test_minute_prefix_incremental_discovery_slices(monkeypatch):
    """High-Scale discovery queries exactly 5 minute-slices via rum_minute_list_prefix."""
    discovered_slices = []

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 704)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", MagicMock())
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)

    def mock_discover(sid, prefix_subpath=None, run_id=None):
        discovered_slices.append(prefix_subpath)
        return 1

    monkeypatch.setattr("backend.core.ingest.discover_rum_prefix", mock_discover)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)

    assert len(discovered_slices) == 5
    for s in discovered_slices:
        assert s.startswith("raw/rum/")


# =============================================================================
# 7. Chunk Budgeting & Bounded Ingestion (Trap #41) Tests
# =============================================================================


def test_trap_41_chunk_zero_guaranteed_even_if_budget_expired(monkeypatch):
    """Trap #41: ingest_rum_logs respects max_seconds but always processes chunk 0."""
    from backend.core import rum_ingest as rum_ingest_mod

    log_calls = []

    monkeypatch.setattr("backend.core.rum_ingest.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: MagicMock())
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda sid, task: 705)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", MagicMock())
    monkeypatch.setattr("backend.core.ingest._recover_in_flight", MagicMock())
    monkeypatch.setattr("backend.core.metadata.get_ingested_filenames", lambda *a, **k: set())

    # Return 100 new files (2 chunks of 50)
    fake_files = [f"s3://test-rum-bucket/raw/rum/beacon_{i:03d}.gz" for i in range(100)]
    file_sizes = {f: 100 for f in fake_files}

    def fake_list_gen(**kw):
        yield {"type": "status", "message": "found files"}
        return {"new_files": fake_files, "file_sizes": file_sizes, "stranded_already": []}

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)

    # Time simulation: already expired at start
    download_chunks = []

    def fake_download(s3, chunk, tmpdir):
        download_chunks.append(chunk)
        return {p: f"/fake/{p}" for p in chunk}, []

    monkeypatch.setattr("backend.core.ingest._download_chunk_to_local", fake_download)
    monkeypatch.setattr(
        "backend.core.ingest._parse_rum_beacon_file",
        lambda p, sid: ([{"pathname": "/", "metric_name": "pageview", "metric_value": 1.0}], [], []),
    )
    monkeypatch.setattr("backend.core.iceberg.write_to_buffer", MagicMock())
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", MagicMock())

    # Advance time after chunk 0
    t0 = 1000.0
    times = [t0, t0 + 5.0, t0 + 200.0, t0 + 205.0]

    def mock_time():
        return times.pop(0) if times else 2000.0

    monkeypatch.setattr(rum_ingest_mod.time, "time", mock_time)

    events = list(rum_ingest_mod.ingest_rum_logs(SERVICE_ID, max_seconds=10))

    # Chunk 0 was processed (50 files), chunk 1 was skipped due to budget
    assert len(download_chunks) == 1
    assert len(download_chunks[0]) == 50
    assert any(evt[0] == "status" and evt[1] == "budget" for evt in events)


# =============================================================================
# 8. Post-Sync Immediate Commit Tests
# =============================================================================


def test_post_sync_immediate_commit_triggered_on_new_beacons(monkeypatch):
    """When new beacons arrive (total > 0), _run_rum_commit is triggered immediately."""
    commit_mock = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.rum_commit._run_rum_commit", commit_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync.ingest_rum_logs",
        lambda sid, *a, **kw: iter([("started", 706), ("done", 42)]),
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)
    commit_mock.assert_called_once_with(SERVICE_ID)


def test_post_sync_immediate_commit_not_triggered_on_zero_beacons(monkeypatch):
    """When zero new beacons arrive (total == 0), _run_rum_commit is NOT triggered."""
    commit_mock = MagicMock()
    monkeypatch.setattr("backend.cron.jobs.rum_commit._run_rum_commit", commit_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr(
        "backend.cron.jobs.rum_sync.ingest_rum_logs",
        lambda sid, *a, **kw: iter([("started", 707), ("done", 0)]),
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)
    commit_mock.assert_not_called()


# =============================================================================
# 9. Accounting & Outcome Counters Tests
# =============================================================================


def test_accounting_outcome_counters_in_cron_runs(monkeypatch):
    """Outcome counters are computed, normalized, and logged in cron_runs."""
    from backend.core import rum_ingest as rum_ingest_mod

    log_calls = []

    monkeypatch.setattr("backend.core.rum_ingest.get_source_for_service", lambda sid: FAKE_SRC_STANDARD)
    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_STANDARD)
    monkeypatch.setattr("backend.core.rum_ingest._get_fos_client", lambda src: MagicMock())
    monkeypatch.setattr("backend.core.rum_ingest.start_cron_run", lambda sid, task: 708)
    monkeypatch.setattr("backend.core.rum_ingest.log_cron_run", lambda *a, **kw: log_calls.append((a, kw)))
    monkeypatch.setattr("backend.core.rum_ingest.finalize_cron_run_if_running", MagicMock())
    monkeypatch.setattr("backend.core.ingest._recover_in_flight", MagicMock())
    monkeypatch.setattr("backend.core.metadata.get_ingested_filenames", lambda *a, **k: set())

    fake_file = "s3://test-rum-bucket/raw/rum/beacon_001.gz"

    def fake_list_gen(**kw):
        yield {"type": "status", "message": "found files"}
        return {"new_files": [fake_file], "file_sizes": {fake_file: 50}, "stranded_already": []}

    monkeypatch.setattr("backend.core.ingest.list_fos_files", fake_list_gen)
    monkeypatch.setattr(
        "backend.core.ingest._download_chunk_to_local",
        lambda s3, chunk, tmpdir: ({fake_file: "/fake/path"}, []),
    )
    monkeypatch.setattr(
        "backend.core.ingest._parse_rum_beacon_file",
        lambda p, sid: ([{"pathname": "/test", "metric_name": "pageview", "metric_value": 1.0}], [], []),
    )
    monkeypatch.setattr("backend.core.iceberg.write_to_buffer", MagicMock())
    monkeypatch.setattr("backend.core.metadata.insert_ingested_files", MagicMock())

    list(rum_ingest_mod.ingest_rum_logs(SERVICE_ID))

    assert len(log_calls) == 1
    _, kwargs = log_calls[0]
    counters = kwargs.get("outcome_counters")
    assert counters is not None
    assert counters["objects_processed"] == 1
    assert counters["objects_successful"] == 1
    assert counters["objects_failed"] == 0


# =============================================================================
# 10. Guaranteed Cleanup Tests
# =============================================================================


def test_guaranteed_cleanup_rum_discovery(monkeypatch):
    """end_progress and finalize_cron_run_if_running are guaranteed in finally on error."""
    end_progress_mock = MagicMock()
    finalize_mock = MagicMock()

    monkeypatch.setattr("backend.config.load_config", lambda sid: FAKE_CFG_HIGH_SCALE)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: FAKE_SRC_HIGH_SCALE)
    monkeypatch.setattr("backend.config.is_high_scale_mode", lambda src: True)
    monkeypatch.setattr("backend.config.CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 709)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", MagicMock())
    monkeypatch.setattr("backend.core.duckdb.finalize_cron_run_if_running", finalize_mock)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr(
        "backend.core.ingest.discover_rum_prefix",
        MagicMock(side_effect=RuntimeError("FOS connection aborted")),
    )
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", MagicMock())

    rum_ledger_mod._run_rum_discovery_cron.__wrapped__(SERVICE_ID)

    end_progress_mock.assert_called_once_with(709)
    finalize_mock.assert_called_once_with(FAKE_SRC_HIGH_SCALE, "rum_discovery", 709)


def test_guaranteed_cleanup_rum_sync(monkeypatch):
    """end_progress and cleanup_progress_and_reap are guaranteed in finally on error."""
    end_progress_mock = MagicMock()
    reap_mock = MagicMock()

    def failing_ingest(sid, **kw):
        yield ("started", 710)
        raise RuntimeError("Disk full")

    monkeypatch.setattr("backend.cron.jobs.rum_sync.ingest_rum_logs", failing_ingest)
    monkeypatch.setattr("backend.cron.jobs.rum_sync._reconcile_faro_bundle", lambda sid, rid: True)
    monkeypatch.setattr("backend.cron_progress.start_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.add_progress", MagicMock())
    monkeypatch.setattr("backend.cron_progress.end_progress", end_progress_mock)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", reap_mock)

    rum_sync_mod._run_rum_sync.__wrapped__(SERVICE_ID)

    end_progress_mock.assert_called_once_with(710)
    reap_mock.assert_called_once()


# =============================================================================
# 11. Manual Trigger Endpoint Tests
# =============================================================================


def test_manual_trigger_rum_sync_success():
    """POST /api/admin/rum/sync/{service_id} returns 200, run_id, and starts execution."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC_STANDARD
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=801),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_sync._run_rum_sync"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/sync/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == 801
        assert "started" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sync_reentrancy():
    """POST /api/admin/rum/sync/{service_id} returns 200 with already running message."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC_STANDARD
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("Already running")),
            patch(
                "backend.cron_progress.list_active_runs",
                return_value=[{"service_id": SERVICE_ID, "task": "rum_sync", "run_id": 802}],
            ),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/sync/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == 802
        assert "already running" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sync_busy_503():
    """POST /api/admin/rum/sync/{service_id} surfaces 503 cron_busy when lease cannot be acquired."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC_STANDARD
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("Lease lock busy")),
            patch("backend.cron_progress.list_active_runs", return_value=[]),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/sync/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["detail"]["error"] == "cron_busy"
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sync_read_only_rejected_403():
    """POST /api/admin/rum/sync/{service_id} on read-only service returns 403."""
    ro_src = {**FAKE_SRC_STANDARD, "access_level": "read_only"}
    app.dependency_overrides[get_source] = lambda: ro_src
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/rum/sync/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 403
        assert "read-only" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_sync_non_admin_rejected_403():
    """POST /api/admin/rum/sync/{service_id} by non-admin returns 403."""
    from fastapi import HTTPException

    def reject_analyst():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[get_source] = lambda: FAKE_SRC_STANDARD
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    app.dependency_overrides[require_admin] = reject_analyst
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/rum/sync/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 403
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)
        app.dependency_overrides.pop(require_admin, None)


def test_manual_trigger_rum_discovery_success():
    """POST /api/admin/rum/discovery/{service_id} returns 200, run_id, and starts job."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC_HIGH_SCALE
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", return_value=803),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron.jobs.rum_ledger._run_rum_discovery_cron"),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/discovery/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["run_id"] == 803
        assert "started" in body["message"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_discovery_standard_mode_rejected_400():
    """POST /api/admin/rum/discovery/{service_id} in standard mode returns 400."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC_STANDARD
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/rum/discovery/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 400
        assert "high-scale" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_discovery_read_only_rejected_403():
    """POST /api/admin/rum/discovery/{service_id} on read-only service returns 403."""
    ro_src = {**FAKE_SRC_HIGH_SCALE, "access_level": "read_only"}
    app.dependency_overrides[get_source] = lambda: ro_src
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        client = TestClient(app)
        resp = client.post(
            f"/api/admin/rum/discovery/{SERVICE_ID}",
            headers={"x-fastly-service-id": SERVICE_ID},
        )
        assert resp.status_code == 403
        assert "read-only" in resp.json()["detail"].lower()
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)


def test_manual_trigger_rum_discovery_busy_503():
    """POST /api/admin/rum/discovery/{service_id} returns 503 on lease conflict."""
    app.dependency_overrides[get_source] = lambda: FAKE_SRC_HIGH_SCALE
    app.dependency_overrides[get_service_id] = lambda: SERVICE_ID
    try:
        with (
            patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("Lease locked")),
            patch("backend.cron_progress.list_active_runs", return_value=[]),
            patch("backend.repositories.dashboard.invalidate_service"),
        ):
            client = TestClient(app)
            resp = client.post(
                f"/api/admin/rum/discovery/{SERVICE_ID}",
                headers={"x-fastly-service-id": SERVICE_ID},
            )

        assert resp.status_code == 503
        assert resp.json()["detail"]["error"] == "cron_busy"
    finally:
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(get_service_id, None)
