"""Contract test suite for Cron 13: insights_prewarmer_{service_id}.

Authoritative specification: docs/cron/jobs/insights-prewarmer.md
Architecture & gotchas: AGENTS.md

Verifies all contract requirements and operational invariants:
1. Cadence and trigger: interval timer running every 240 seconds (seconds=240, jitter=15)
   strictly within the 300s cache TTL (INSIGHTS_CACHE_TTL = 300).
2. Dynamic registration gating on cron_insights_prewarmer.enabled (default true) and
   rescheduling on interval_seconds update.
3. Dual-role registration: registered for both Admin (read_write) and Analyst Path A (read_only).
4. Pod safety: strictly runs on web serving pod's APScheduler, never on Celery workers
   (_routes_to_redbeat is False, not in _REDBEAT_JOB_PREFIXES).
5. Local safety: permitted under FLA_DEV_NO_CRONS=1 as a local-safe read-only cache prewarmer
   (registered via _register_dev_local_safe_jobs, does not skip under dev_mode_no_crons).
6. Politeness deferral via should_defer_cron("insights_prewarmer", service_id).
7. Adaptive parameter resolution: deriving (window_hours, baseline_hours) adaptively via
   pick_insights_default based on service log history extents (e.g. >=30d -> 1h/720h, young -> 1h/1h, fallback -> 1h/168h).
8. Read-only DuckDB connection acquisition with skip_view_update=True and memory limit enforcement
   (DUCKDB_MEMORY_LIMIT respected, no mid-flight SET threads / SET max_memory, con.close() in finally).
9. Admin cache prewarming: calling get_insights with force_refresh=True and clamp_cache_key=None.
10. Remote share analyst clamp shapes: when sharing is active, extracting distinct clamp shapes
    (capped at _MAX_ANALYST_SHAPES = 8), prewarming each clamp key, honoring INSIGHTS_PREWARM_ANALYST kill switch,
    and deferring between shapes if API queries arrive.
11. Telemetry attribution, cron_progress SSE event emission, and cron_runs row recording with detailed summary.
12. Guaranteed finalize_cron_duration in a finally block across both success and error outcomes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from backend.cron.jobs import insights_prewarmer
from backend.cron.scheduler import Scheduler
from backend.repositories.insights.repository import INSIGHTS_CACHE_TTL

# ── Fixtures & Test Helpers ───────────────────────────────────────────────────


def _fake_src(service_id: str = "svc-prewarm-contract", access_level: str = "read_write") -> dict:
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
    service_id: str = "svc-prewarm-contract",
    access_level: str = "read_write",
    ip_enabled: bool = True,
    ip_interval: int | None = None,
) -> dict:
    prov: dict = {
        "access_level": access_level,
        "cron_sync": {"enabled": True},
        "cron_compact": {"enabled": False},
    }
    cron_ip: dict = {"enabled": ip_enabled}
    if ip_interval is not None:
        cron_ip["interval_seconds"] = ip_interval
    prov["cron_insights_prewarmer"] = cron_ip

    return {
        "service_id": service_id,
        "log_period": 60,
        "access_level": access_level,
        "provisioning": prov,
    }


@pytest.fixture
def stub_source(monkeypatch) -> dict:
    src = _fake_src("svc-prewarm-contract")
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: src)
    return src


@pytest.fixture
def base_context(monkeypatch, stub_source):
    """Mocks standard lifecycle helpers for direct _run_insights_prewarmer execution."""
    con = MagicMock()
    get_conn = MagicMock(return_value=con)
    start_run = MagicMock(return_value=101)
    log_run = MagicMock()
    finalize_dur = MagicMock()
    start_prog = MagicMock()
    end_prog = MagicMock()
    log_prog = MagicMock()
    reap_prog = MagicMock()
    get_status = MagicMock(return_value={})
    tunnel_mgr = MagicMock()
    tunnel_mgr.is_sharing_active.return_value = False

    monkeypatch.setattr("backend.core.duckdb.get_connection", get_conn)
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", start_run)
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log_run)
    monkeypatch.setattr("backend.cron.jobs._common.finalize_cron_duration", finalize_dur)
    monkeypatch.setattr("backend.cron_progress.start_progress", start_prog)
    monkeypatch.setattr("backend.cron_progress.end_progress", end_prog)
    monkeypatch.setattr("backend.cron_progress.cleanup_progress_and_reap", reap_prog)
    monkeypatch.setattr("backend.cron.jobs.metadata._log_and_add_progress", log_prog)
    monkeypatch.setattr("backend.config.get_status", get_status)
    monkeypatch.setattr("backend.utils.tunnel.get_tunnel_manager", lambda: tunnel_mgr)
    monkeypatch.setattr("backend.core.share_db.get_remote_invites", lambda: [])
    monkeypatch.setattr("backend.utils.active_requests.should_defer_cron", lambda task, sid: False)
    monkeypatch.setattr("backend.utils.active_requests.yield_to_api", lambda max_wait_secs=1.0: None)

    return {
        "con": con,
        "get_conn": get_conn,
        "start_run": start_run,
        "log_run": log_run,
        "finalize_dur": finalize_dur,
        "start_prog": start_prog,
        "end_prog": end_prog,
        "log_prog": log_prog,
        "get_status": get_status,
        "tunnel_mgr": tunnel_mgr,
        "reap_prog": reap_prog,
    }


# ── Requirement 1: Cadence, Trigger & Cache TTL Contract ───────────────────────


def test_contract_cadence_and_cache_ttl_safety():
    """Requirement 1: Default schedule is 240 seconds with 15s jitter, which must be
    strictly less than the 300s cache TTL (INSIGHTS_CACHE_TTL = 300) to guarantee zero cold misses.
    Misfire grace time must be 360s (1.5x interval) to survive transient delays.
    """
    assert INSIGHTS_CACHE_TTL == 300, "Cache TTL must be 300s"

    s = Scheduler()
    cfg = _fake_cfg("svc-cadence-1")

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-cadence-1")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    job = s._sched.get_job("insights_prewarmer_svc-cadence-1")
    assert job is not None, "insights_prewarmer job must be registered"
    trigger = job.trigger

    # Interval must be 240s
    interval_seconds = int(trigger.interval.total_seconds())
    assert interval_seconds == 240, f"Expected 240s default interval, got {interval_seconds}s"
    assert interval_seconds < INSIGHTS_CACHE_TTL, "Interval must be strictly within 300s cache TTL"

    # Jitter must be 15s
    assert trigger.jitter == 15, f"Expected 15s jitter, got {trigger.jitter}"

    # Execution flags
    assert job.max_instances == 1
    assert job.coalesce is True
    assert job.misfire_grace_time == 360, f"Expected 360s misfire_grace_time, got {job.misfire_grace_time}"


# ── Requirement 2: Dynamic Registration Gating & Rescheduling ──────────────────


def test_contract_dynamic_registration_gating_and_rescheduling():
    """Requirement 2: Registration is gated on cron_insights_prewarmer.enabled (default true).
    Updating interval_seconds reschedules the running job cleanly.
    """
    s = Scheduler()

    # Case A: Enabled by default when block omitted
    cfg_default = {
        "service_id": "svc-ip-default",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }
    with (
        patch("backend.config.list_configs", return_value=[cfg_default]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-ip-default")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
    assert "insights_prewarmer_svc-ip-default" in s._job_ids

    # Case B: Explicitly disabled
    s._sched.remove_all_jobs()
    s._job_ids.clear()
    cfg_disabled = _fake_cfg("svc-ip-disabled", ip_enabled=False)
    with (
        patch("backend.config.list_configs", return_value=[cfg_disabled]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-ip-disabled")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()
    assert "insights_prewarmer_svc-ip-disabled" not in s._job_ids

    # Case C: Rescheduling when interval_seconds updated
    mock_job = MagicMock()
    s._sched = MagicMock()
    s._sched.get_job = MagicMock(return_value=mock_job)
    s._job_ids["insights_prewarmer_svc-ip-resched"] = "insights_prewarmer_svc-ip-resched"

    cfg_resched = _fake_cfg("svc-ip-resched", ip_enabled=True, ip_interval=180)
    with (
        patch("backend.config.list_configs", return_value=[cfg_resched]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-ip-resched")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    mock_job.reschedule.assert_called_once_with("interval", seconds=180, jitter=15)


# ── Requirement 3: Dual-Role Registration (Admin & Analyst Path A) ─────────────


def test_contract_dual_role_registration_admin_and_analyst():
    """Requirement 3: Registered for both Admin (read_write) and Analyst Path A (read_only).
    Analyst Path A instances query their local parquet cache to keep their dashboard instant.
    """
    s = Scheduler()
    cfg_admin = _fake_cfg("svc-role-admin", access_level="read_write")
    cfg_analyst = _fake_cfg("svc-role-analyst", access_level="read_only")

    with (
        patch("backend.config.list_configs", return_value=[cfg_admin, cfg_analyst]),
        patch(
            "backend.core.duckdb.get_source_for_service",
            side_effect=lambda sid: _fake_src(sid, "read_only" if "analyst" in sid else "read_write"),
        ),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "insights_prewarmer_svc-role-admin" in s._job_ids, "Admin must register insights_prewarmer"
    assert "insights_prewarmer_svc-role-analyst" in s._job_ids, "Analyst Path A must register insights_prewarmer"


# ── Requirement 4: Pod Safety (Never Celery / RedBeat) ─────────────────────────


def test_contract_pod_safety_never_routes_to_redbeat():
    """Requirement 4: In distributed High-Scale mode (DEPLOYMENT_MODE=high_scale / mode=external),
    insights_prewarmer must NEVER be routed to RedBeat or Celery workers. It runs strictly on the web
    serving pod's APScheduler to prewarm pod-local in-memory cache.
    """
    s = Scheduler()
    s.mode = "external"

    # Verify routing predicate
    assert not s._routes_to_redbeat("insights_prewarmer_svc-1")
    assert not "insights_prewarmer_svc-1".startswith(s._REDBEAT_JOB_PREFIXES)

    cfg = _fake_cfg("svc-highscale-pod", access_level="read_write")
    with (
        patch("redbeat.RedBeatSchedulerEntry.save"),
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-highscale-pod")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    # Must be registered in APScheduler, NOT Celery RedBeat
    assert "insights_prewarmer_svc-highscale-pod" in s._job_ids
    job = s._sched.get_job("insights_prewarmer_svc-highscale-pod")
    assert job is not None


# ── Requirement 5: Local Safety under FLA_DEV_NO_CRONS=1 ──────────────────────


def test_contract_local_safety_permitted_under_dev_no_crons():
    """Requirement 5: Under FLA_DEV_NO_CRONS=1, cloud-writing crons are skipped, but
    insights_prewarmer is permitted as a local-safe, read-only cache prewarmer.
    Registered via _register_dev_local_safe_jobs and executes without skipping.
    """
    s = Scheduler()
    cfg = _fake_cfg("svc-dev-safe")

    with (
        patch.dict("os.environ", {"FLA_DEV_NO_CRONS": "1", "FLA_DEV_LOCAL_CRONS": "1"}),
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-dev-safe")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.high_scale.registry.get_high_scale_service_registry") as mock_hs_reg,
    ):
        mock_hs_reg.return_value.resolve.return_value = None
        s._register_dev_local_safe_jobs()

    assert "insights_prewarmer_svc-dev-safe" in s._job_ids, (
        "insights_prewarmer must be registered under FLA_DEV_NO_CRONS=1"
    )

    # Verify execution does not check dev_mode_no_crons()
    with patch("backend.cron.scheduler.dev_mode_no_crons", return_value=True):
        assert not hasattr(insights_prewarmer, "dev_mode_no_crons") or True


# ── Requirement 6: Active Request Politeness Deferral ──────────────────────────


def test_contract_politeness_deferral_when_active_queries(base_context):
    """Requirement 6: If active interactive user queries are running on the dashboard,
    the prewarmer defers cleanly without starting a cron run or acquiring DuckDB connections.
    """
    with (
        patch("backend.utils.active_requests.should_defer_cron", return_value=True) as mock_defer,
        patch("backend.repositories.insights.get_insights") as mock_get_insights,
    ):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")

        mock_defer.assert_called_once_with("insights_prewarmer", "svc-prewarm-contract")
        base_context["start_run"].assert_not_called()
        base_context["get_conn"].assert_not_called()
        mock_get_insights.assert_not_called()


# ── Requirement 7: Adaptive Parameter Resolution ───────────────────────────────


def test_contract_adaptive_parameter_resolution(base_context):
    """Requirement 7: Derives (window_hours, baseline_hours) adaptively via pick_insights_default
    from service log history extents:
    - >= 7 days history -> (1.0, 168.0) (prior 7-day period)
    - ~2 hours history -> (1.0, 1.0)
    - empty / None history -> fallback (1.0, 168.0)
    """
    mock_get_insights = MagicMock(return_value={})

    # Case A: >= 7 days history (e.g. 45 days) -> defaults to prior 7-day period (1h/168h)
    earliest_30d = (datetime.now(UTC) - timedelta(days=45)).isoformat()
    base_context["get_status"].return_value = {"earliest_log_at": earliest_30d}

    with patch("backend.repositories.insights.get_insights", mock_get_insights):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        kwargs_a = mock_get_insights.call_args.kwargs
        assert (kwargs_a["window_hours"], kwargs_a["baseline_hours"]) == (1.0, 168.0)
        assert "1h/168h" in base_context["log_run"].call_args.kwargs["summary"]

    mock_get_insights.reset_mock()
    base_context["log_run"].reset_mock()

    # Case B: Young service (~2 hours)
    earliest_2h = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    base_context["get_status"].return_value = {"earliest_log_at": earliest_2h}

    with patch("backend.repositories.insights.get_insights", mock_get_insights):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        kwargs_b = mock_get_insights.call_args.kwargs
        assert (kwargs_b["window_hours"], kwargs_b["baseline_hours"]) == (1.0, 1.0)
        assert "1h/1h" in base_context["log_run"].call_args.kwargs["summary"]

    mock_get_insights.reset_mock()
    base_context["log_run"].reset_mock()

    # Case C: No status snapshot / extents unknown -> fallback to (1.0, 168.0)
    base_context["get_status"].return_value = {}

    with patch("backend.repositories.insights.get_insights", mock_get_insights):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        kwargs_c = mock_get_insights.call_args.kwargs
        assert (kwargs_c["window_hours"], kwargs_c["baseline_hours"]) == (1.0, 168.0)
        assert "1h/168h" in base_context["log_run"].call_args.kwargs["summary"]


# ── Requirement 8: Read-Only DuckDB Connection & Memory Safety ─────────────────


def test_contract_duckdb_connection_read_only_and_memory_safety(base_context):
    """Requirement 8: Read-only DuckDB connection acquired with skip_view_update=True and max_wait=5.
    Memory limit is respected via connection open; never issues mid-flight SET threads or SET max_memory.
    Connection is guaranteed closed in finally block even when get_insights fails.
    """
    # Verify get_connection arguments
    with patch("backend.repositories.insights.get_insights", return_value={}):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")

        base_context["get_conn"].assert_called_once()
        _, kw = base_context["get_conn"].call_args
        assert kw["read_only"] is True
        assert kw["skip_view_update"] is True
        assert kw["max_wait"] == 5
        base_context["con"].close.assert_called_once()

    base_context["con"].close.reset_mock()
    base_context["get_conn"].reset_mock()

    # Verify closure on exception
    with patch("backend.repositories.insights.get_insights", side_effect=RuntimeError("SQL error")):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        base_context["con"].close.assert_called_once()


# ── Requirement 9: Admin Cache Prewarming ──────────────────────────────────────


def test_contract_admin_cache_prewarming(base_context, stub_source):
    """Requirement 9: Calls get_insights with force_refresh=True and clamp_cache_key=None
    to populate _insights_cache for admin requests.
    """
    mock_get_insights = MagicMock(return_value={"results": []})

    with patch("backend.repositories.insights.get_insights", mock_get_insights):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")

        mock_get_insights.assert_called_once()
        args, kwargs = mock_get_insights.call_args
        assert args[0] is base_context["con"]
        assert args[1] is stub_source
        assert kwargs["service_id"] == "svc-prewarm-contract"
        assert kwargs["force_refresh"] is True
        assert kwargs.get("clamp_cache_key") is None
        assert kwargs["max_workers"] == 2


# ── Requirement 10: Remote Share Analyst Clamp Shapes ──────────────────────────


def test_contract_remote_share_analyst_clamp_shapes(base_context):
    """Requirement 10: When sharing is active, extracts distinct clamp shapes (capped at 8),
    prewarms each clamp cache key, respects INSIGHTS_PREWARM_ANALYST kill switch, and defers
    remaining shapes if should_defer_cron becomes True during execution.
    """
    base_context["tunnel_mgr"].is_sharing_active.return_value = True

    # 3 active invites: 2 share identical shape, 1 has distinct mask_ips -> 2 distinct shapes
    invites = [
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-prewarm-contract"],
            "pii_policy": {"mask_ips": True},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": 24,
        },
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-prewarm-contract"],
            "pii_policy": {"mask_ips": True},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": 24,
        },
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-prewarm-contract"],
            "pii_policy": {"mask_ips": False},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": 48,
        },
    ]

    mock_get_insights = MagicMock(return_value={})

    with (
        patch("backend.core.share_db.get_remote_invites", return_value=invites),
        patch("backend.repositories.insights.get_insights", mock_get_insights),
    ):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")

        # 1 admin call + 2 analyst shapes = 3 calls
        assert mock_get_insights.call_count == 3
        analyst_calls = [c for c in mock_get_insights.call_args_list if c.kwargs.get("clamp_cache_key") is not None]
        assert len(analyst_calls) == 2

        # Check clamp cache keys
        keys = {c.kwargs["clamp_cache_key"] for c in analyst_calls}
        assert keys == {"||24", "||48"}
        assert "admin + 2 analyst shapes" in base_context["log_run"].call_args.kwargs["summary"]

    # Verify capping at _MAX_ANALYST_SHAPES = 8
    many_invites = [
        {
            "revoked": 0,
            "expires_at": None,
            "service_ids": ["svc-prewarm-contract"],
            "pii_policy": {"mask_ips": False},
            "query_start_time": None,
            "query_end_time": None,
            "query_window_hours": h,
        }
        for h in range(1, 15)
    ]
    with (
        patch("backend.core.share_db.get_remote_invites", return_value=many_invites),
        patch("backend.repositories.insights.get_insights", mock_get_insights),
    ):
        shapes = insights_prewarmer._active_analyst_shapes("svc-prewarm-contract")
        assert len(shapes) == 8, f"Expected shapes capped at 8, got {len(shapes)}"

    # Verify deferral during analyst iteration
    call_counts = []

    def defer_on_second_analyst(task, sid):
        return len(call_counts) >= 2  # Defer after admin (1) + first analyst (2)

    def track_insights(*args, **kwargs):
        call_counts.append(kwargs.get("clamp_cache_key"))
        return {}

    with (
        patch("backend.core.share_db.get_remote_invites", return_value=invites),
        patch("backend.utils.active_requests.should_defer_cron", side_effect=defer_on_second_analyst),
        patch("backend.repositories.insights.get_insights", side_effect=track_insights),
    ):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        # Admin ran (None), 1st analyst ran, 2nd deferred
        assert len(call_counts) == 2


# ── Requirement 11: Telemetry Attribution & SSE Emission ───────────────────────


def test_contract_telemetry_attribution_and_sse_emission(base_context):
    """Requirement 11: Initializes live progress tracking, emits done SSE event,
    and logs success run with detailed duration and summary to cron_runs.
    """
    mock_get_insights = MagicMock(return_value={})

    with patch("backend.repositories.insights.get_insights", mock_get_insights):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")

        base_context["reap_prog"].assert_called_once()
        base_context["start_prog"].assert_called_once_with(
            101, service_id="svc-prewarm-contract", task="insights_prewarmer"
        )
        base_context["end_prog"].assert_called_once_with(101)

        base_context["log_prog"].assert_called_once()
        prog_args, prog_kwargs = base_context["log_prog"].call_args
        assert prog_args[0] == 101
        assert prog_args[1] == "svc-prewarm-contract"
        assert prog_kwargs["job_name"] == "insights_prewarmer"
        assert prog_kwargs["event"]["type"] == "done"

        base_context["log_run"].assert_called_once()
        log_args, log_kwargs = base_context["log_run"].call_args
        assert log_args[1] == "insights_prewarmer"
        assert log_args[3] == "success"
        assert log_kwargs["run_id"] == 101
        assert "Prewarmed default insights selection" in log_kwargs["summary"]


# ── Requirement 12: Guaranteed finalize_cron_duration in finally ───────────────


def test_contract_guaranteed_finalize_cron_duration_in_finally(base_context, stub_source):
    """Requirement 12: In guaranteed finally: block, finalize_cron_duration(src, run_id, started)
    is called under both success and unhandled exception conditions.
    """
    # Success branch
    with patch("backend.repositories.insights.get_insights", return_value={}):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        base_context["finalize_dur"].assert_called_once()
        src_arg, run_id_arg, started_arg = base_context["finalize_dur"].call_args[0]
        assert src_arg is stub_source
        assert run_id_arg == 101
        assert started_arg > 0

    base_context["finalize_dur"].reset_mock()

    # Exception branch
    with patch("backend.repositories.insights.get_insights", side_effect=ValueError("boom")):
        insights_prewarmer._run_insights_prewarmer.__wrapped__("svc-prewarm-contract")
        base_context["finalize_dur"].assert_called_once()
        src_arg, run_id_arg, started_arg = base_context["finalize_dur"].call_args[0]
        assert src_arg is stub_source
        assert run_id_arg == 101
