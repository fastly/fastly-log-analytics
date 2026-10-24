"""Consolidated tests for test_scheduler."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def clean_dev_no_crons(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)


# ── _elapsed_since (pure formatter) ───────────────────────────────────────


def test_elapsed_since_formats_seconds_below_60_with_one_decimal():
    """<60 s → ``"1.5s"`` (one-decimal float). Pinned because the
    progress log lines key on this format — refactoring to integer
    would silently drop sub-second resolution that admins use to
    debug fast cron loops."""
    from backend.cron.scheduler import _elapsed_since

    out = _elapsed_since(time.time() - 1.5)
    assert out.endswith("s")
    # Ranges from 1.4s to 1.6s depending on test-execution jitter
    assert "1." in out or "2." in out


def test_elapsed_since_formats_60s_and_above_as_minutes_seconds():
    """>=60 s → ``"1m30s"``. Pinned because the dashboard's "last
    run took X" pill renders this exact format."""
    from backend.cron.scheduler import _elapsed_since

    out = _elapsed_since(time.time() - 90)
    assert "m" in out
    # Should be "1m30s" give-or-take a few seconds
    minute_part = out.split("m")[0]
    assert minute_part == "1"


def test_elapsed_since_zeros_correctly_for_just_passed():
    """Approximately zero seconds → ``"0.0s"`` or similar (NOT empty,
    NOT crash on negative jitter)."""
    from backend.cron.scheduler import _elapsed_since

    out = _elapsed_since(time.time())
    assert out.endswith("s")


# ── _extract_log_text (progress-store digest) ─────────────────────────────


def test_extract_log_text_returns_empty_when_no_progress():
    """Unknown run_id → empty string. Pinned because the admin
    "download cron log" button must produce SOMETHING (empty file)
    even if the progress store was reaped."""
    from backend.cron.scheduler import _extract_log_text

    with patch("backend.cron_progress.get_progress", return_value=None):
        assert _extract_log_text(999) == ""


def test_extract_log_text_formats_events_with_type_prefix():
    """Each event renders as ``[TYPE_UPPERCASE] message``. Pinned
    because admins grep these logs for [ERROR] / [DONE] prefixes."""
    from backend.cron.scheduler import _extract_log_text

    fake_events = [
        {"type": "status", "message": "Starting sync"},
        {"type": "error", "message": "Connection refused"},
        {"type": "done", "message": "Sync finished"},
    ]
    with patch("backend.cron_progress.get_progress", return_value=fake_events):
        out = _extract_log_text(1)

    assert "[STATUS] Starting sync" in out
    assert "[ERROR] Connection refused" in out
    assert "[DONE] Sync finished" in out
    # Lines joined with newlines
    assert out.count("\n") == 2


def test_extract_log_text_filters_out_progress_events():
    """`type=progress` events are skipped — they're just the
    numerical 0/8 → 8/8 counter, not user-readable lines. Pinned
    because including them would clutter the downloaded log with
    9-12 noise lines per run."""
    from backend.cron.scheduler import _extract_log_text

    fake_events = [
        {"type": "progress", "current": 1, "total": 8},  # filter out
        {"type": "status", "message": "Step 1 complete"},
        {"type": "progress", "current": 2, "total": 8},  # filter out
        {"type": "done", "message": "All done"},
    ]
    with patch("backend.cron_progress.get_progress", return_value=fake_events):
        out = _extract_log_text(1)

    # Only the 2 message-bearing events
    assert out.count("\n") == 1
    assert "[STATUS] Step 1 complete" in out
    assert "[DONE] All done" in out
    assert "progress" not in out.lower()


def test_extract_log_text_skips_events_without_message():
    """An event with no ``message`` key is skipped (not stringified
    as None). Pinned because the progress store sometimes captures
    raw API responses with no `message` field."""
    from backend.cron.scheduler import _extract_log_text

    fake_events = [
        {"type": "status"},  # no message
        {"type": "error", "message": "Real error"},
    ]
    with patch("backend.cron_progress.get_progress", return_value=fake_events):
        out = _extract_log_text(1)

    # Only the error line
    assert out == "[ERROR] Real error"


# ── get_scheduler (singleton factory) ─────────────────────────────────────


def test_get_scheduler_returns_singleton():
    """Subsequent calls return the same instance. Pinned because the
    admin endpoints + cron handlers all call get_scheduler() to
    inspect jobs — losing the singleton would mean each importer
    has its own APScheduler with no jobs."""
    import backend.cron.scheduler as sched_mod

    # Reset module-global so test is independent of previous calls
    sched_mod._scheduler = None
    s1 = sched_mod.get_scheduler()
    s2 = sched_mod.get_scheduler()
    assert s1 is s2


def test_get_scheduler_creates_lazily_on_first_call():
    """First call returns a Scheduler (not None). Pinned because the
    main app calls get_scheduler() at startup — if it returned None,
    the first job registration would AttributeError."""
    import backend.cron.scheduler as sched_mod

    sched_mod._scheduler = None
    s = sched_mod.get_scheduler()
    assert s is not None
    assert hasattr(s, "_sched")  # APScheduler under the hood


# ── Scheduler.shutdown / Scheduler.get_job ───────────────────────────────


def test_scheduler_shutdown_swallows_exception():
    """If APScheduler.shutdown() raises (already-stopped, lock
    contention), the wrapper swallows the error and logs. Pinned
    because shutdown is called from a finally block during pytest
    teardown — a raise here would mask test failures."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    # Force shutdown to fail
    s._sched.shutdown = MagicMock(side_effect=RuntimeError("already stopped"))
    # Should NOT raise
    s.shutdown()


def test_scheduler_shutdown_forwards_wait_kwarg_to_apscheduler():
    """``_bounded_scheduler_shutdown`` in backend.main passes
    ``wait=True`` so APScheduler joins any in-flight cron job before
    the executor's worker threads die. Pinned because dropping the
    ``wait`` kwarg from this wrapper (it was missing pre-fix) raised
    "Scheduler.shutdown() got an unexpected keyword argument 'wait'"
    in prod — the kwarg was silently rejected, APScheduler shut down
    with wait=False, and the bounded-wait pattern in backend.main
    became a no-op. A mid-flight 4-minute sync tick would then get
    cut at Docker's SIGTERM grace period instead of draining."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    s._sched.shutdown = MagicMock()
    # Default kwarg: wait=False (matches the historic wrapper default).
    s.shutdown()
    s._sched.shutdown.assert_called_with(wait=False)
    # Pass-through: wait=True forwards to APScheduler so it joins running
    # jobs before returning.
    s._sched.shutdown.reset_mock()
    s.shutdown(wait=True)
    s._sched.shutdown.assert_called_with(wait=True)


def test_scheduler_get_job_returns_none_for_unknown_id():
    """Unknown job_id → None (not raise). Pinned because the admin
    /system-jobs endpoint calls get_job() on every job_id and
    distinguishes None (no schedule yet) from a real job."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    assert s.get_job("nonexistent_job_id") is None


def test_scheduler_reload_calls_sync_jobs():
    """`reload()` is a thin alias for `_sync_jobs()`. Pinned because
    every wizard "save" calls reload via the route helper, and a
    refactor that broke the wiring would silently ignore wizard
    changes until process restart."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    with patch.object(s, "_sync_jobs") as mock_sync:
        s.reload()
    mock_sync.assert_called_once()


# ── Scheduler._sync_jobs: config-driven job registration ──────────────────


def _fake_src(service_id="svc-1"):
    """Source dict that passes `is_configured`."""
    return {
        "name": service_id,
        "service_id": service_id,
        "bucket": "b",
        "endpoint": "e",
        "access_key_id": "k",
        "secret_access_key": "s",
        "region": "us-east-1",
    }


def test_sync_jobs_registers_sync_commit_and_alerts_for_readwrite_service():
    """A read-write service with at least one alert gets sync + commit
    + alerts jobs. Pinned because these are the three baseline cron
    jobs every admin service needs once alerts are configured."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-1",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src()),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=1),
    ):
        s._sync_jobs()

    assert "log_discovery_svc-1" in s._job_ids
    assert "commit_svc-1" in s._job_ids
    assert "alerts_evaluation_svc-1" in s._job_ids


def test_sync_jobs_registers_metadata_sync_and_alerts_for_readonly_service():
    """Read-only (analyst) services with at least one alert get
    metadata_sync + alerts — NOT the sync/commit jobs. Pinned because
    analyst replicas can't perform write operations against Fastly."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-2",
        "log_period": 60,
        "access_level": "read_only",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-2")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=1),
    ):
        s._sync_jobs()

    assert "sync_metadata_svc-2" in s._job_ids
    assert "alerts_evaluation_svc-2" in s._job_ids
    # Write-side jobs absent
    assert "sync_svc-2" not in s._job_ids
    assert "commit_svc-2" not in s._job_ids


def test_sync_jobs_skips_alerts_cron_when_no_alerts_configured():
    """When a service has zero alerts, the alerts cron should NOT be
    registered — otherwise it just fires every `log_period` writing
    "skipped" cron_runs entries. Once an alert is created, the alerts
    router calls scheduler.reload() to register the job."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-noalert",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-noalert")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "log_discovery_svc-noalert" in s._job_ids
    assert "commit_svc-noalert" in s._job_ids
    # Alerts cron must not be registered until at least one alert exists.
    assert "alerts_evaluation_svc-noalert" not in s._job_ids


def test_sync_jobs_skips_disabled_services():
    """`cron_sync.enabled=False` → no jobs registered for that
    service. Pinned because the wizard's "pause cron" toggle keys
    on this flag."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "paused-svc",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": False}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("paused-svc")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    assert "sync_paused-svc" not in s._job_ids
    assert "commit_paused-svc" not in s._job_ids


def test_sync_jobs_skips_unconfigured_service():
    """`is_configured` returning False → warn + skip (no jobs).
    Pinned because mid-teardown can leave a config without
    credentials, and crashing here would freeze the scheduler."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "halfbaked",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("halfbaked")),
        patch("backend.core.duckdb.is_configured", return_value=False),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    assert "sync_halfbaked" not in s._job_ids


def test_sync_jobs_registers_ngwaf_job_when_workspace_configured():
    """Service with NGWAF workspace_id → also gets an ngwaf_sync job.
    Pinned because losing this would silently disable bot sync for
    NGWAF-enabled services."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "ngwaf-svc",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("ngwaf-svc")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value="workspace-id"),
    ):
        s._sync_jobs()

    assert "ngwaf_sync_ngwaf-svc" in s._job_ids
    job = s._sched.get_job("ngwaf_sync_ngwaf-svc")
    assert job is not None
    assert job.executor == "maintenance"


def test_sync_jobs_omits_optimize_and_expire_when_compact_disabled():
    """`cron_compact.enabled=False` → no optimize or expire jobs.
    Pinned because customers retaining ALL snapshots (legal hold)
    must be able to disable the expire job."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "no-compact",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": False}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("no-compact")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    assert "optimize_no-compact" not in s._job_ids
    assert "expire_no-compact" not in s._job_ids


def test_sync_jobs_always_registers_global_system_jobs():
    """Bot-data-refresh + rdns-enrichment + share-audit-purge are global
    jobs (not per-service). They must register on every _sync_jobs call.
    Pinned because losing these would freeze the global bot list, rDNS
    cache, or remote-share audit retention."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[]),  # No services
    ):
        s._sync_jobs()

    assert "bot_data_refresh" in s._job_ids
    assert "rdns_enrichment" in s._job_ids
    assert "share_audit_purge" in s._job_ids


def test_sync_jobs_removes_stale_jobs_for_deleted_services():
    """A job that exists in `_job_ids` but is no longer in any service
    config gets removed (service was deleted). Pinned because losing
    the cleanup would leave zombie jobs trying to ingest deleted
    services and 500ing every interval."""
    from backend.cron.scheduler import Scheduler

    s = Scheduler()
    # Seed a stale job_id from a "previous" config sync
    s._job_ids["sync_deleted-svc"] = "sync_deleted-svc"

    with (
        patch("backend.config.list_configs", return_value=[]),  # All services deleted
        patch.object(s._sched, "remove_job") as mock_remove,
    ):
        s._sync_jobs()

    # The stale job was removed
    assert "sync_deleted-svc" not in s._job_ids
    mock_remove.assert_any_call("sync_deleted-svc")


def test_sync_jobs_uses_interval_mins_over_interval_seconds_when_both_present():
    """`cron_sync.interval_mins` takes priority over
    `cron_sync.interval_seconds`. Pinned because the UI writes
    interval_mins while admin provisioning scripts write
    interval_seconds — letting interval_seconds win would silently
    ignore UI changes."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-priority",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {
            "cron_sync": {"enabled": True, "interval_mins": 10, "interval_seconds": 30},
        },
    }

    captured: dict = {}

    s = Scheduler()
    real_add = s._sched.add_job

    def capturing_add_job(*args, **kwargs):
        if kwargs.get("id", "").startswith("log_discovery_svc-priority"):
            captured["seconds"] = kwargs.get("seconds")
        return real_add(*args, **kwargs)

    s._sched.add_job = capturing_add_job

    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-priority")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    # 10 minutes = 600 seconds (interval_mins wins, NOT 30 from interval_seconds)
    assert captured.get("seconds") == 600


def test_sync_jobs_registers_rum_jobs_when_rum_enabled_on_root():
    """RUM sync and commit jobs are registered when `rum_enabled` is True at the root level."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-rum-root",
        "log_period": 60,
        "access_level": "read_write",
        "rum_enabled": True,
        "provisioning": {
            "cron_sync": {"enabled": True},
        },
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-rum-root")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    assert "rum_sync_svc-rum-root" in s._job_ids
    assert "rum_commit_svc-rum-root" in s._job_ids


def test_sync_jobs_registers_rum_jobs_when_rum_enabled_nested():
    """RUM sync and commit jobs are registered when `rum.enabled` is True in nested configuration."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-rum-nested",
        "log_period": 60,
        "access_level": "read_write",
        "rum": {"enabled": True},
        "provisioning": {
            "cron_sync": {"enabled": True},
        },
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-rum-nested")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    assert "rum_sync_svc-rum-nested" in s._job_ids
    assert "rum_commit_svc-rum-nested" in s._job_ids


def test_sync_jobs_does_not_register_rum_jobs_when_rum_disabled_in_nested():
    """RUM sync and commit jobs are NOT registered when `rum.enabled` is False, even if `rum_enabled` is True on root."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-rum-disabled",
        "log_period": 60,
        "access_level": "read_write",
        "rum_enabled": True,
        "rum": {"enabled": False},
        "provisioning": {
            "cron_sync": {"enabled": True},
        },
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-rum-disabled")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
    ):
        s._sync_jobs()

    assert "rum_sync_svc-rum-disabled" not in s._job_ids
    assert "rum_commit_svc-rum-disabled" not in s._job_ids


def test_resolve_rum_sync_interval():
    """Verify RUM sync interval resolution rules."""
    from backend.cron.scheduler import _resolve_rum_sync_interval

    # Explicit sync_interval_seconds in rum_cfg takes precedence
    assert _resolve_rum_sync_interval({"sync_interval_seconds": 45}, {}, 60) == 45
    assert _resolve_rum_sync_interval({"sync_interval_seconds": 2}, {}, 60) == 5  # min 5s clamp

    # Derived from rum_log_period (nested in rum_cfg or flat in cfg)
    assert _resolve_rum_sync_interval({"log_period": 300}, {}, 60) == 150  # 300 // 2
    assert _resolve_rum_sync_interval({"log_period": 60}, {}, 60) == 30  # 60 // 2
    assert _resolve_rum_sync_interval({"log_period": 600}, {}, 60) == 300  # 600 // 2
    assert _resolve_rum_sync_interval({"log_period": 10}, {}, 60) == 10  # < 60: max(5, 10)
    assert _resolve_rum_sync_interval({"log_period": 3}, {}, 60) == 5  # < 60: min 5s clamp

    # From flat cfg when rum_cfg has no log_period
    assert _resolve_rum_sync_interval({}, {"rum_log_period": 300}, 60) == 150

    # Fallback to default request interval when rum_log_period not configured
    assert _resolve_rum_sync_interval({}, {}, 120) == 120
    assert _resolve_rum_sync_interval({}, {}, 2) == 5  # min 5s clamp


def test_sync_jobs_schedules_rum_sync_with_derived_interval():
    """Verify rum_sync job is added to the scheduler with the interval derived from rum_log_period."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-rum-derived",
        "log_period": 60,
        "access_level": "read_write",
        "rum": {"enabled": True, "log_period": 300},
        "provisioning": {
            "cron_sync": {"enabled": True},
        },
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-rum-derived")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch.object(s._sched, "add_job") as mock_add_job,
    ):
        s._sync_jobs()

    # Find the call to add rum_sync job
    rum_sync_calls = [
        call for call in mock_add_job.call_args_list if call.kwargs.get("id") == "rum_sync_svc-rum-derived"
    ]
    assert len(rum_sync_calls) == 1
    assert rum_sync_calls[0].kwargs["seconds"] == 150  # 300 // 2


# ── _log_and_add_progress ─────────────────────────────────────────────────


def test_log_and_add_progress_persists_event_to_cron_progress():
    """Always adds the event to the cron-progress store. Pinned
    because the SSE consumer in the cron-runs-stream endpoint reads
    from that exact store."""
    from backend.cron.scheduler import _log_and_add_progress

    event = {"type": "status", "message": "Step 1"}
    with (
        patch("backend.cron_progress.add_progress") as mock_add,
        patch("backend.config.load_config", return_value={"name": "Test Service"}),
    ):
        _log_and_add_progress(run_id=42, service_id="svc-1", event=event, job_name="sync")

    mock_add.assert_called_once_with(42, event)


def test_log_and_add_progress_handles_missing_message_silently():
    """Events without a `message` key get persisted but NOT logged.
    Pinned because the progress store sometimes captures raw API
    responses; logging None as a message would render "Test (svc):
    None" in the cron log."""
    from backend.cron.scheduler import _log_and_add_progress

    event = {"type": "progress", "current": 1, "total": 8}
    with (
        patch("backend.cron_progress.add_progress") as mock_add,
        patch("backend.config.load_config", return_value=None),
        patch("backend.cron.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)

    # Persisted to progress store
    mock_add.assert_called_once_with(1, event)
    # NOT logged (no message)
    mock_logger.info.assert_not_called()
    mock_logger.warning.assert_not_called()
    mock_logger.error.assert_not_called()


def test_log_and_add_progress_uses_error_logger_for_error_type():
    """`type=error` → `logger.error()` (not `.info()`). Pinned
    because error-rate dashboards/alerts key on the log level —
    misrouting to .info would hide cron failures."""
    from backend.cron.scheduler import _log_and_add_progress

    event = {"type": "error", "message": "DB locked"}
    with (
        patch("backend.cron_progress.add_progress"),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.cron.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)

    mock_logger.error.assert_called_once()
    mock_logger.info.assert_not_called()


def test_log_and_add_progress_uses_warning_logger_for_warning_type():
    """`type=warning` → `logger.warning()`."""
    from backend.cron.scheduler import _log_and_add_progress

    event = {"type": "warning", "message": "slow query"}
    with (
        patch("backend.cron_progress.add_progress"),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.cron.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)

    mock_logger.warning.assert_called_once()


def test_check_disk_space_passes_when_plenty_free(tmp_path):
    """Happy path: 10 GB total / 9.5 GB free → no abort."""
    from collections import namedtuple

    from backend.cron.scheduler import _check_disk_space

    Usage = namedtuple("usage", "total used free")
    with patch("shutil.disk_usage", return_value=Usage(total=10 * 1024**3, used=512 * 1024**2, free=9 * 1024**3)):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True
    assert msg == ""


def test_check_disk_space_aborts_when_below_byte_floor(tmp_path):
    """100 MB free is below the 500 MB hard floor → abort with a
    human-readable reason."""
    from collections import namedtuple

    from backend.cron.scheduler import _check_disk_space

    Usage = namedtuple("usage", "total used free")
    with patch(
        "shutil.disk_usage",
        return_value=Usage(total=100 * 1024**3, used=99_900 * 1024**2, free=100 * 1024**2),
    ):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is False
    assert "100 MB free" in msg


def test_check_disk_space_aborts_when_below_pct_floor(tmp_path):
    """Disk is large enough in absolute bytes (1 GB free), but that's
    only 2% of 50 GB total — pct floor (3%) should still trip."""
    from collections import namedtuple

    import backend.cron.scheduler as scheduler
    from backend.cron.scheduler import _check_disk_space

    Usage = namedtuple("usage", "total used free")
    with (
        patch(
            "shutil.disk_usage",
            return_value=Usage(total=50 * 1024**3, used=49 * 1024**3, free=1 * 1024**3),
        ),
        patch.object(scheduler, "_DISK_FREE_HARD_FLOOR_PCT", 0.03),
    ):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is False
    assert "2.0% of 50.0" in msg


def test_check_disk_space_does_not_block_on_probe_failure(tmp_path):
    """If shutil.disk_usage raises (e.g. dir disappears mid-cron), do
    NOT block — let the underlying job try and fail with the real
    error. Conservative because we don't want a transient FS issue to
    kill all cron runs."""
    from backend.cron.scheduler import _check_disk_space

    with patch("shutil.disk_usage", side_effect=OSError("gone")):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True
    assert msg == ""


def test_check_buffer_backlog_returns_empty_when_drained():
    """Healthy drain: nothing in buffer after commit → no suffix, no warning."""
    from backend.cron.scheduler import _check_buffer_backlog

    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None},
    ):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


def test_check_buffer_backlog_warns_when_file_count_exceeds_threshold():
    """More than 200 files left after a commit → suffix mentions count."""
    from backend.cron.scheduler import _check_buffer_backlog

    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 250, "total_bytes": 1_000_000, "oldest_age_seconds": 30, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "250 files" in suffix
    assert suffix.startswith(" ⚠")


def test_check_buffer_backlog_warns_when_oldest_age_exceeds_scaled_threshold():
    """Oldest file > 3 × commit_interval_mins → suffix flags age. At
    interval=5, threshold is 15min; pass 30 min and expect a warning."""
    from backend.cron.scheduler import _check_buffer_backlog

    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": 1000, "oldest_age_seconds": 30 * 60, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "30m old" in suffix


def test_check_buffer_backlog_never_raises_on_probe_failure():
    """The backlog probe must not bubble exceptions back to _run_commit.
    A corrupt iceberg helper must yield "" not a stack trace."""
    from backend.cron.scheduler import _check_buffer_backlog

    with patch("backend.core.iceberg.buffer_backlog_stats", side_effect=OSError("disk gone")):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


# Silence ruff unused-imports
_ = MagicMock
_ = pytest


def test_sync_jobs_registers_hourly_rollup_heal_for_readwrite_service():
    """Read-write services get the hourly rollup_heal job alongside the
    daily rollup_compact. Pinned because without it, closed hours of
    bursty services stay absent from top-N until the 02:00 deep pass
    (the 2026-07-06 huge total_rows-vs-field_total divergence)."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-1",
        "log_period": 60,
        "provisioning": {"cron_sync": {"enabled": True}, "cron_compact": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src()),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "rollup_compact_svc-1" in s._job_ids
    assert "rollup_heal_svc-1" in s._job_ids
    heal_job = s._sched.get_job("rollup_heal_svc-1")
    assert heal_job is not None
    # Hourly at :05 — late enough for Fastly log delivery lag to land the
    # just-closed hour's rows, hourly so staleness caps at ~1 hour.
    assert "minute='5'" in str(heal_job.trigger)
    assert "hour=" not in str(heal_job.trigger)


def test_sync_jobs_skips_rollup_heal_for_read_only_service():
    """Analyst replicas own no rollup data — same gate as rollup_compact."""
    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-2",
        "log_period": 60,
        "provisioning": {
            "cron_sync": {"enabled": True},
            "cron_compact": {"enabled": True},
            "access_level": "read_only",
        },
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-2")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=0),
    ):
        s._sync_jobs()

    assert "rollup_compact_svc-2" not in s._job_ids
    assert "rollup_heal_svc-2" not in s._job_ids


# ── Merged from test_scheduler_apscheduler_stress.py ──

import concurrent.futures
import logging
import threading
from datetime import UTC, datetime, timedelta

import pytest


def test_apscheduler_max_instances_1_waits_for_cleanup(monkeypatch, caplog):
    """The scheduler never overlaps a resource-owning cron invocation."""
    apscheduler = pytest.importorskip("apscheduler.schedulers.background")
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 0.1)

    tick_log: list[float] = []
    active = 0
    max_active = 0
    state_lock = threading.Lock()

    @sched_mod.cron_task("apscheduler-stress")
    def _cron_body(service_id: str) -> None:
        nonlocal active, max_active
        tick_log.append(time.monotonic())
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.35 if len(tick_log) == 1 else 0.02)
        with state_lock:
            active -= 1

    sched = apscheduler.BackgroundScheduler(timezone="UTC")
    sched.add_job(
        _cron_body,
        "interval",
        seconds=0.4,
        args=["svc-stress"],
        id="cron-apscheduler-stress",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(UTC) + timedelta(milliseconds=50),
    )

    caplog.set_level(logging.ERROR, logger="backend.scheduler")
    sched.start()

    try:
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline and len(tick_log) < 2:
            time.sleep(0.05)
    finally:
        sched.shutdown(wait=False)

    assert len(tick_log) >= 2, (
        f"APScheduler stalled at {len(tick_log)} tick(s): {[r.getMessage() for r in caplog.records]}"
    )
    assert max_active == 1

    assert any("exceeded" in rec.getMessage() and "watchdog threshold" in rec.getMessage() for rec in caplog.records), (
        f"expected watchdog ERROR log; got {[r.getMessage() for r in caplog.records]}"
    )


def test_usage_log_phase_30s_timeout_pattern_does_not_block_caller():
    """Pins the per-phase pattern in _run_log_discovery_cron (backend/cron/jobs/sync.py).

    The phase is wrapped in its own single-worker ThreadPoolExecutor with
    a 30s timeout. On timeout we MUST call ``shutdown(wait=False)`` — if
    that's ever changed to ``wait=True`` the caller blocks on the leaked
    thread and we're right back in the wedge.

    This test reproduces the exact shape (executor + submit +
    result(timeout=...) + shutdown(wait=False)) and verifies the caller
    returns within timeout + small jitter.
    """

    def _hangs():
        time.sleep(10)
        return "should-never-return"

    PHASE_TIMEOUT_S = 0.3

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="usage-log-stress")
    shutdown_wait = True

    start = time.monotonic()
    try:
        fut = ex.submit(_hangs)
        try:
            fut.result(timeout=PHASE_TIMEOUT_S)
            pytest.fail("expected TimeoutError — hangs() sleeps 10s but timeout is 0.3s")
        except concurrent.futures.TimeoutError:
            shutdown_wait = False
    finally:
        ex.shutdown(wait=shutdown_wait)
    elapsed = time.monotonic() - start

    # The full block must return well inside the timeout + small overhead.
    # If shutdown_wait=True ever creeps back in, this elapsed jumps to ~10s.
    assert elapsed < PHASE_TIMEOUT_S + 1.0, (
        f"phase pattern took {elapsed:.2f}s — caller is blocking on the leaked "
        f"thread, defeating the per-phase timeout. Check that shutdown_wait "
        f"is set to False on TimeoutError."
    )


def test_metadata_db_init_lock_has_finite_timeout():
    """Verify that Postgres connection pool acquires with a finite timeout."""
    import inspect

    from backend.core.metadata import pg_connection

    src = inspect.getsource(pg_connection.get_pg_pool)
    assert "timeout=" in src, "get_pg_pool must configure a finite connection timeout"


# ── Merged from test_scheduler_watchdog.py ──


def test_cron_task_waits_for_cleanup_after_watchdog_threshold(monkeypatch, caplog):
    """An overlong body is reported but its resource-owning thread is joined."""
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 0.05)

    @sched_mod.cron_task("hang-test")
    def _hangs_forever(service_id: str) -> None:
        time.sleep(0.15)

    caplog.set_level(logging.ERROR, logger="backend.scheduler")

    start = time.monotonic()
    result = _hangs_forever("svc-watchdog")
    elapsed = time.monotonic() - start

    assert result is None
    assert elapsed >= 0.15, "the resource-owning worker must finish before release"
    assert any(
        "exceeded" in rec.getMessage()
        and "watchdog threshold" in rec.getMessage()
        and "svc-watchdog" in rec.getMessage()
        for rec in caplog.records
        if rec.levelno >= logging.ERROR
    ), f"expected ERROR log naming the service; got {[r.getMessage() for r in caplog.records]}"


def test_cron_task_propagates_normal_return(monkeypatch):
    """Fast inner function returns a sentinel; wrapper returns the same value.

    Pins that the watchdog is transparent on the happy path — refactoring
    it must not silently swallow the return value or wrap it in a Future.
    """
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 60)

    sentinel = object()

    @sched_mod.cron_task("happy-path")
    def _quick(service_id: str):
        return sentinel

    start = time.monotonic()
    result = _quick("svc-happy")
    elapsed = time.monotonic() - start

    assert result is sentinel
    assert elapsed < 5, f"happy path took {elapsed:.2f}s — should be near-instant"


def test_cron_task_preserves_telemetry_context(monkeypatch):
    """The wrapper's process_context_scope must apply inside the worker thread.

    Pins that wrapping in ThreadPoolExecutor didn't break telemetry attribution.
    The 2026-05-21 incident left this contract intact and we don't want a
    silent regression where every cron's usage rows land with NULL context.
    """
    from backend.cron import decorators as sched_mod
    from backend.utils.telemetry import get_process_context

    captured = {}

    @sched_mod.cron_task("ctx-test")
    def _check_ctx(service_id: str) -> None:
        captured["ctx"] = get_process_context()

    _check_ctx("svc-ctx")

    assert captured.get("ctx") == "ctx-test", (
        f"expected process_context='ctx-test' inside cron body; got {captured.get('ctx')!r}. "
        "If None, the process_context_scope is being applied in the wrong thread."
    )


def test_cron_task_heartbeat_thread_releases_its_connection(monkeypatch):
    """Live incident (2026-09-03): the per-invocation heartbeat thread
    (a fresh ``threading.Thread``, never reused across ticks — unlike the
    bounded watchdog executor) called ``get_con()`` but never gave the
    connection back. Under Postgres that permanently pins one slot of the
    bounded METADATA_PG_POOL_MAX pool per cron tick across the WHOLE
    scheduler, and every metadata-touching request/cron eventually starts
    raising PoolTimeout once the pool is exhausted. The heartbeat thread
    must release its connection on exit, whether or not it ever actually
    used one (job_name=None skips the UPDATE, but the finally must still
    run and be a safe no-op)."""
    from backend.cron import decorators as sched_mod

    monkeypatch.setattr(sched_mod, "_CRON_HARD_CAP_S", 60)

    @sched_mod.cron_task("release-test", job_name="release_test_job")
    def _quick(service_id: str):
        return None

    with patch("backend.core.metadata.base.release_thread_connection") as mock_release:
        _quick("svc-release")

    mock_release.assert_called_once_with()


# ── Merged from test_dev_mode_no_crons.py ──────────────────────────────────────────────


import pytest

# ── The env-var helper ──────────────────────────────────────────────────────


def test_dev_mode_no_crons_returns_false_when_unset(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    from backend.cron.scheduler import dev_mode_no_crons

    assert dev_mode_no_crons() is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "True"])
def test_dev_mode_no_crons_truthy_values(monkeypatch, value):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", value)
    from backend.cron.scheduler import dev_mode_no_crons

    assert dev_mode_no_crons() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "", "off"])
def test_dev_mode_no_crons_falsy_values(monkeypatch, value):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", value)
    from backend.cron.scheduler import dev_mode_no_crons

    assert dev_mode_no_crons() is False


# ── Scheduler.start() ──────────────────────────────────────────────────────


def test_scheduler_start_skips_full_registration_but_runs_local_allowlist(monkeypatch):
    """Most important contract test: start() must NOT call _sync_jobs (the
    full registration that arms FOS-writing / ingest / outbound jobs) when
    the kill switch is set — it only runs the local-only allowlist via
    _register_dev_local_safe_jobs. If _sync_jobs regressed back in, every
    cron job (sync/commit/optimize/expire/...) would tick on dev."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    with (
        patch.object(sched, "_sync_jobs") as sync_jobs,
        patch.object(sched, "_register_dev_local_safe_jobs") as dev_jobs,
        patch.object(sched._sched, "start") as ap_start,
    ):
        sched.start()
        sync_jobs.assert_not_called()  # full registration must NEVER run on dev
        dev_jobs.assert_called_once()  # only the local-safe allowlist runs
        # allowlist was mocked → no job IDs → APScheduler stays stopped
        ap_start.assert_not_called()


def test_dev_local_allowlist_registers_only_local_compaction(monkeypatch):
    """_register_dev_local_safe_jobs arms local_compact + rollup_compact +
    rollup_heal and NOTHING else — no sync/commit/optimize/expire/ngwaf/etc.
    All three only rewrite the local parquet cache (never FOS), which is the
    allowlist's admission bar."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    cfg = {
        "service_id": "svc-x",
        "provisioning": {"access_level": "read_write", "cron_compact": {"enabled": True}},
    }
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-x"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()

    assert sorted(sched._job_ids) == [
        "insights_prewarmer_svc-x",
        "local_compact_svc-x",
        "metric_snapshot",
        "partial_hour_merge_svc-x",
        "rollup_compact_svc-x",
        "rollup_heal_svc-x",
    ]
    added = sorted(c.kwargs["id"] for c in add_job.call_args_list)
    assert added == [
        "insights_prewarmer_svc-x",
        "local_compact_svc-x",
        "metric_snapshot",
        "partial_hour_merge_svc-x",
        "rollup_compact_svc-x",
        "rollup_heal_svc-x",
    ]


def test_dev_local_allowlist_skips_rollup_compact_for_read_only(monkeypatch):
    """read_only services own no rollup data → only local_compact arms."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    cfg = {"service_id": "svc-ro", "provisioning": {"access_level": "read_only"}}
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-ro"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()

    assert sorted(sched._job_ids) == [
        "insights_prewarmer_svc-ro",
        "local_compact_svc-ro",
        "metric_snapshot",
        "partial_hour_merge_svc-ro",
    ]
    added = sorted(c.kwargs["id"] for c in add_job.call_args_list)
    assert added == [
        "insights_prewarmer_svc-ro",
        "local_compact_svc-ro",
        "metric_snapshot",
        "partial_hour_merge_svc-ro",
    ]


def test_dev_local_allowlist_skips_high_scale_services(monkeypatch):
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    cfg = {"service_id": "svc-high", "provisioning": {"access_level": "read_write"}}
    sched = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc-high"}),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch(
            "backend.high_scale.registry.get_high_scale_service_registry",
            return_value=type("Registry", (), {"resolve": lambda self, service_id: object()})(),
        ),
        patch.object(sched._sched, "add_job") as add_job,
    ):
        sched._register_dev_local_safe_jobs()

    assert sorted(sched._job_ids) == ["metric_snapshot"]
    assert [call.kwargs["id"] for call in add_job.call_args_list] == ["metric_snapshot"]


def test_scheduler_reload_is_a_noop_when_kill_switch_on(monkeypatch):
    """reload() is called by service-config saves. If it re-registers jobs
    after start() bailed, an admin clicking 'Save' in the cron settings
    modal would sneak crons back in. This pins that it can't happen."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    with patch.object(sched, "_sync_jobs") as sync_jobs:
        sched.reload()
        sync_jobs.assert_not_called()


def test_scheduler_start_runs_normally_when_kill_switch_off(monkeypatch):
    """The kill switch defaults to off — prod behavior is unchanged."""
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)

    from backend.cron.scheduler import Scheduler

    sched = Scheduler()
    # Stub the heavy parts so the test stays fast; just verify the
    # gating logic doesn't short-circuit them.
    with patch.object(sched, "_sync_jobs") as sync_jobs, patch.object(sched._sched, "start") as ap_start:
        with patch("backend.config.list_configs", return_value=[]):
            sched.start()
        sync_jobs.assert_called_once()
        ap_start.assert_called_once()


# ── Ingest-class job entry points ───────────────────────────────────────────


def test_run_log_discovery_cron_refuses_when_kill_switch_on(monkeypatch, caplog):
    """_run_log_discovery_cron is the path that manual /admin/ingest-logs hits
    with force=True. force=True intentionally bypasses
    provisioning.cron_sync.enabled. The env var beats both."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")

    from backend.cron.jobs.sync import _run_log_discovery_cron

    # If the gate works, _run_log_discovery_cron returns immediately and
    # ingest() is never imported. Patch get_source_for_service to a
    # MagicMock; if the gate fails, the function would call it and the
    # MagicMock would record the call.
    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        _run_log_discovery_cron("svc-test", force=True)
        get_src.assert_not_called()

    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


def test_run_full_sweep_refuses_when_kill_switch_on(monkeypatch, caplog):
    """_run_full_sweep is what gap_heal invokes directly (NOT through a
    config check). This is the leading-suspect bypass-candidate the
    dev-mode investigation identified for the 23:21:11 surprise sync."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")

    from backend.cron.jobs.sync import _run_full_sweep

    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        _run_full_sweep("svc-test")
        get_src.assert_not_called()

    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


def test_run_gap_heal_refuses_when_kill_switch_on(monkeypatch, caplog):
    """_run_gap_heal sees local dev as having a 100% ingest gap against
    prod's FOS bucket (because dev never ingested) — without the gate it
    would happily trigger a full sweep over the prod data."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    caplog.set_level("WARNING")

    from backend.cron.jobs.sync import _run_gap_heal

    with patch("backend.core.duckdb.get_source_for_service") as get_src:
        _run_gap_heal("svc-test")
        get_src.assert_not_called()

    assert any("FLA_DEV_NO_CRONS=1" in r.message for r in caplog.records)


# ── Merged from test_scheduler_branches.py ──────────────────────────────────────────────

from unittest.mock import MagicMock

import pytest

from backend.cron import scheduler as sched

# ── dev_mode_no_crons: env-var gating ──────────────────────────────────────


@pytest.mark.parametrize(
    "env_val,expected",
    [
        ("1", True),
        ("true", True),
        ("yes", True),
        ("TRUE", True),
        ("0", False),
        ("", False),
        ("no", False),
        ("anything-else", False),
    ],
)
def test_dev_mode_no_crons_recognises_truthy_values(env_val, expected, monkeypatch):
    """The env kill-switch accepts 1/true/yes (case-insensitive). Pinned
    because misspelling FLA_DEV_NO_CRONS=enabled would silently NOT
    disable crons — exactly the failure mode this env var exists to
    prevent."""
    monkeypatch.setenv("FLA_DEV_NO_CRONS", env_val)
    assert sched.dev_mode_no_crons() is expected


def test_dev_mode_no_crons_false_when_unset(monkeypatch):
    monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
    assert sched.dev_mode_no_crons() is False


# ── _display_label ─────────────────────────────────────────────────────────


def test_display_label_collapses_when_name_matches_id():
    """When service_name == service_id, the label is just the id —
    avoids ``svc-1 (svc-1)`` double-print."""
    src = {"service_name": "svc-1", "name": "svc-1"}
    assert sched._display_label(src, "svc-1") == "svc-1"


def test_display_label_includes_both_when_distinct():
    src = {"service_name": "My Service", "name": "svc-1"}
    assert sched._display_label(src, "svc-1") == "My Service (svc-1)"


# ── _claim_heavy_refresh: per-service throttle ─────────────────────────────


def test_claim_heavy_refresh_first_call_wins():
    """First call for a service in a fresh window claims and returns True."""
    # Use a unique service_id so other tests can't pollute.
    sid = "test-claim-heavy-1"
    sched._last_heavy_refresh.pop(sid, None)
    assert sched._claim_heavy_refresh(sid) is True


def test_claim_heavy_refresh_returns_false_when_throttled():
    """Second call within the throttle window returns False — line 108."""
    sid = "test-claim-heavy-2"
    sched._last_heavy_refresh[sid] = time.time()  # just claimed
    assert sched._claim_heavy_refresh(sid) is False


def test_claim_heavy_refresh_reopens_window_after_interval():
    sid = "test-claim-heavy-3"
    sched._last_heavy_refresh[sid] = time.time() - sched._HEAVY_REFRESH_INTERVAL_SEC - 10
    assert sched._claim_heavy_refresh(sid) is True


# ── _service_has_alerts: defaults to True on error ─────────────────────────


def test_service_has_alerts_returns_count_gt_zero():
    with patch("backend.core.metadata.count_alerts", return_value=3):
        assert sched._service_has_alerts("svc") is True


def test_service_has_alerts_returns_false_when_count_zero():
    with patch("backend.core.metadata.count_alerts", return_value=0):
        assert sched._service_has_alerts("svc") is False


def test_service_has_alerts_defaults_to_true_on_exception():
    """Line 128-129: if count_alerts raises (corrupt SQLite, etc.),
    fail-safe to True so the cron isn't silently disabled."""
    with patch("backend.core.metadata.count_alerts", side_effect=RuntimeError("corrupt")):
        assert sched._service_has_alerts("svc") is True


# ── _extract_log_text: None / empty branches ───────────────────────────────


def test_extract_log_text_returns_empty_for_none_run_id():
    """Line 147: a None run_id (start_cron_run failed) → empty string,
    never calls get_progress."""
    with patch("backend.cron_progress.get_progress") as mock_gp:
        assert sched._extract_log_text(None) == ""
    mock_gp.assert_not_called()


def test_extract_log_text_returns_empty_when_progress_empty():
    """An empty event list short-circuits to '' (avoids producing a
    bare '\\n'-only string)."""
    with patch("backend.cron_progress.get_progress", return_value=[]):
        assert sched._extract_log_text(42) == ""


# ── _elapsed_since: pretty-print branches ──────────────────────────────────


def test_elapsed_since_sub_minute():
    """Under 60s renders as ``Xs`` (one-decimal seconds)."""
    out = sched._elapsed_since(time.time() - 5)
    assert out.endswith("s") and "m" not in out


def test_elapsed_since_over_minute():
    """≥60s renders as ``XmYYs`` (minutes + two-digit zero-padded seconds)."""
    out = sched._elapsed_since(time.time() - 125)
    # Either "2m05s" or similar; assert the m+s shape.
    assert "m" in out
    assert out.endswith("s")


# ── _check_disk_space: OSError + low-space + happy ─────────────────────────


def test_check_disk_space_returns_true_on_oserror():
    """Line 276-279: shutil.disk_usage raising is treated as 'don't
    block' — let the job try and fail naturally if disk really is bad."""
    with patch("shutil.disk_usage", side_effect=OSError("permission")):
        ok, msg = sched._check_disk_space("/some/dir", "svc", "sync")
    assert ok is True
    assert msg == ""


def test_check_disk_space_blocks_when_below_hard_floor():
    """A near-empty disk → block the cron with a clear message."""
    usage = MagicMock(total=1_000_000_000, free=50_000_000)  # 5% free, well below 10% floor
    with (
        patch("shutil.disk_usage", return_value=usage),
        patch("os.path.isdir", return_value=True),
    ):
        ok, msg = sched._check_disk_space("/some/dir", "svc", "sync")
    assert ok is False
    assert "disk almost full" in msg


def test_check_disk_space_happy_path_returns_ok():
    """Plenty of free space → (True, "") proceeds normally."""
    usage = MagicMock(total=1_000_000_000_000, free=500_000_000_000)  # 50% free
    with (
        patch("shutil.disk_usage", return_value=usage),
        patch("os.path.isdir", return_value=True),
    ):
        ok, msg = sched._check_disk_space("/some/dir", "svc", "sync")
    assert ok is True
    assert msg == ""


# ── _check_buffer_backlog: thresholds + exception ──────────────────────────


def test_check_buffer_backlog_returns_empty_on_zero_files():
    """No files → no problem → empty string."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0},
    ):
        assert sched._check_buffer_backlog({"name": "s"}, "s", 5) == ""


def test_check_buffer_backlog_returns_empty_when_below_all_thresholds():
    """Few files, small bytes, fresh oldest → no warning. Line 329, 331."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": 1024, "oldest_age_seconds": 30},
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert out == ""


def test_check_buffer_backlog_warns_on_file_count():
    """Crossing the file-count threshold yields a warning string."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={
            "file_count": sched._BACKLOG_FILE_COUNT_WARN + 100,
            "total_bytes": 0,
            "oldest_age_seconds": 0,
        },
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert "buffer backlog" in out
    assert "files" in out


def test_check_buffer_backlog_warns_on_oldest_age():
    """oldest_age > 3x commit_interval → warning."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={
            "file_count": 10,
            "total_bytes": 0,
            "oldest_age_seconds": 60 * 60,  # 1 hour
        },
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert "oldest" in out


def test_check_buffer_backlog_warns_on_total_bytes():
    """>1 GiB un-committed → warning."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={
            "file_count": 10,
            "total_bytes": sched._BACKLOG_BYTES_WARN + 1,
            "oldest_age_seconds": 0,
        },
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert "MB on disk" in out


def test_check_buffer_backlog_returns_empty_on_probe_exception():
    """If the buffer probe itself fails, we annotate nothing — backlog
    probing must NEVER fail the commit, only annotate it (line 314-316)."""
    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        side_effect=RuntimeError("disk gone"),
    ):
        out = sched._check_buffer_backlog({"name": "s"}, "s", 5)
    assert out == ""


# ── Merged from test_scheduler_reschedule_or_add.py ──────────────────────────────────────────────

from unittest.mock import MagicMock

from backend.cron.jobs.duckdb_recycle import run_duckdb_recycle
from backend.cron.lifecycle import record_skip_row
from backend.cron.scheduler import Scheduler

SRC = {"name": "svc_test"}


def _scheduler():
    s = Scheduler()
    s._sched = MagicMock()
    s._add_job = MagicMock()
    return s


def _fn():
    return None


def test_new_job_is_added_tracked_and_reported_as_added():
    s = _scheduler()
    added = s._reschedule_or_add_job(_fn, "interval", seconds=30, id="job_a", args=["svc"], max_instances=1)
    assert added is True
    s._add_job.assert_called_once_with(_fn, "interval", seconds=30, id="job_a", args=["svc"], max_instances=1)
    assert "job_a" in s._job_ids
    s._sched.get_job.assert_not_called()


def test_tracked_interval_job_is_rescheduled_with_only_timing_kwargs():
    s = _scheduler()
    s._job_ids["job_a"] = "job_a"
    job = MagicMock()
    s._sched.get_job.return_value = job
    added = s._reschedule_or_add_job(_fn, "interval", minutes=5, jitter=30, id="job_a", args=["svc"])
    assert added is False
    job.reschedule.assert_called_once_with("interval", minutes=5)
    s._add_job.assert_not_called()


def test_tracked_cron_job_reschedules_hour_and_minute():
    s = _scheduler()
    s._job_ids["job_c"] = "job_c"
    job = MagicMock()
    s._sched.get_job.return_value = job
    s._reschedule_or_add_job(_fn, "cron", hour="3,9", minute=30, id="job_c", args=["svc"], misfire_grace_time=3600)
    job.reschedule.assert_called_once_with("cron", hour="3,9", minute=30)


def test_reschedule_kwargs_override_is_applied_verbatim():
    s = _scheduler()
    s._job_ids["job_a"] = "job_a"
    job = MagicMock()
    s._sched.get_job.return_value = job
    s._reschedule_or_add_job(
        _fn,
        "interval",
        seconds=60,
        jitter=15,
        reschedule_kwargs={"seconds": 60, "jitter": 15},
        id="job_a",
    )
    job.reschedule.assert_called_once_with("interval", seconds=60, jitter=15)


def test_missing_job_or_reschedule_error_is_swallowed():
    s = _scheduler()
    s._job_ids["job_a"] = "job_a"
    s._sched.get_job.return_value = None
    assert s._reschedule_or_add_job(_fn, "interval", seconds=1, id="job_a") is False

    boom = MagicMock()
    boom.reschedule.side_effect = RuntimeError("nope")
    s._sched.get_job.return_value = boom
    assert s._reschedule_or_add_job(_fn, "interval", seconds=1, id="job_a") is False
    s._add_job.assert_not_called()


# ── DuckDB recycle cron job & scheduler registration ─────────────────────────


def test_run_duckdb_recycle_skips_below_rss_threshold(monkeypatch):
    """The scheduler entry skips the recycle when RSS is below the threshold."""
    monkeypatch.setenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "999999999")
    detail = run_duckdb_recycle.__wrapped__()
    assert detail.startswith("skipped:")


def test_run_duckdb_recycle_runs_when_threshold_disabled(monkeypatch):
    """With no RSS threshold (0 = always run), the entry delegates to
    recycle_once rather than short-circuiting."""
    monkeypatch.setenv("DUCKDB_RECYCLE_RSS_THRESHOLD_MB", "0")
    with patch(
        "backend.cron.jobs.duckdb_recycle.recycle_once", return_value="interval: recycled 0/0 instance(s), freed ~0MB"
    ) as mock_recycle:
        detail = run_duckdb_recycle.__wrapped__()
    mock_recycle.assert_called_once_with(reason="interval")
    assert "recycled" in detail


def _registered_job_ids(monkeypatch, interval_env: str) -> list[str]:
    """Run _sync_jobs with no services + a mocked APScheduler; return the ids
    passed to add_job."""
    monkeypatch.setenv("DUCKDB_RECYCLE_INTERVAL_MIN", interval_env)
    monkeypatch.setattr("backend.config.list_configs", lambda: [])

    sched = Scheduler()
    mock_sched = MagicMock()
    sched._sched = mock_sched
    sched._job_ids = {}
    sched._sync_jobs()

    return [c.kwargs.get("id") for c in mock_sched.add_job.call_args_list]


def test_recycle_job_registered_when_interval_positive(monkeypatch):
    ids = _registered_job_ids(monkeypatch, "15")
    assert "duckdb_recycle" in ids

    sched = Scheduler()
    mock_sched = MagicMock()
    sched._sched = mock_sched
    sched._job_ids = {}
    monkeypatch.setenv("DUCKDB_RECYCLE_INTERVAL_MIN", "15")
    monkeypatch.setattr("backend.config.list_configs", lambda: [])
    sched._sync_jobs()
    recycle_calls = [c for c in mock_sched.add_job.call_args_list if c.kwargs.get("id") == "duckdb_recycle"]
    assert len(recycle_calls) == 1
    kw = recycle_calls[0].kwargs
    assert kw["max_instances"] == 1
    assert kw["coalesce"] is True
    assert kw["minutes"] == 15.0
    assert kw["executor"] == "maintenance"


def test_recycle_job_not_registered_when_interval_zero(monkeypatch):
    ids = _registered_job_ids(monkeypatch, "0")
    assert "duckdb_recycle" not in ids


def test_recycle_job_routes_to_maintenance_executor():
    """Job ID duckdb_recycle routes to maintenance executor threadpool."""
    sched = Scheduler()
    mock_sched = MagicMock()
    sched._sched = mock_sched
    sched._job_ids = {}

    sched._add_job(lambda: None, id="duckdb_recycle")
    assert mock_sched.add_job.call_args.kwargs["executor"] == "maintenance"


# ── Lifecycle record_skip_row ────────────────────────────────────────────────


def test_defaults_to_a_skipped_row_with_zero_duration(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)

    record_skip_row(SRC, "alerts", "No alerts configured")

    log.assert_called_once_with(
        SRC, "alerts", 0.0, "skipped", summary="No alerts configured", error_message=None, run_id=None
    )


def test_forwards_status_duration_error_and_run_id(monkeypatch):
    log = MagicMock()
    monkeypatch.setattr("backend.core.duckdb.log_cron_run", log)

    record_skip_row(
        SRC,
        "rollup_hour_heal",
        "Skipped due to low disk space",
        status="warning",
        duration_s=1.5,
        error_message="Disk space critically low",
        run_id=42,
    )

    log.assert_called_once_with(
        SRC,
        "rollup_hour_heal",
        1.5,
        "warning",
        summary="Skipped due to low disk space",
        error_message="Disk space critically low",
        run_id=42,
    )


# ── Merged from test_scheduler_timing.py ──────────────────────────────────────────────

import inspect

from backend.core.metadata.cron_log import CronLeaseHeldError


class TestRunServiceCronSignature:
    def test_start_time_is_str_not_float(self):
        """start_time in _run_log_discovery_cron must be str|None, not float."""
        from backend.cron.jobs.sync import _run_log_discovery_cron

        sig = inspect.signature(_run_log_discovery_cron)
        ann = sig.parameters["start_time"].annotation
        ann_str = str(ann)
        assert "str" in ann_str and "None" in ann_str
        assert "float" not in ann_str, "start_time became float — timing regression risk"

    def test_end_time_is_str_not_float(self):
        from backend.cron.jobs.sync import _run_log_discovery_cron

        sig = inspect.signature(_run_log_discovery_cron)
        ann = sig.parameters["end_time"].annotation
        ann_str = str(ann)
        assert "str" in ann_str and "None" in ann_str


class TestRunCommitUsesLocalStartTime:
    """_run_commit uses a local `start_time = time.time()` variable (correct).

    Verify it doesn't reference an outer scope start_time that could be str|None.
    """

    def test_run_commit_no_str_subtraction(self):
        """_run_commit should not raise TypeError even if called with no string start_time."""
        from backend.cron.jobs.commit import _run_commit

        mock_src = {
            "name": "test",
            "service_id": "test",
            "access_level": "read_write",
            "storage_mode": "cloud",
        }

        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
            patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("lock held")),
        ):
            # RuntimeError from start_cron_run causes early return — no TypeError expected
            try:
                _run_commit("test-service")
            except TypeError as exc:
                raise AssertionError(f"TypeError in _run_commit — likely str used in float arithmetic: {exc}") from exc


class TestLogCronRunDurationIsFloat:
    """Ensure log_cron_run receives a float duration, not a str|None."""

    def test_duration_passed_to_log_cron_run_is_float(self, monkeypatch):
        monkeypatch.delenv("FLA_DEV_NO_CRONS", raising=False)
        """Mock ingest to yield a done event and capture what log_cron_run receives."""
        from backend.cron.jobs.sync import _run_log_discovery_cron

        mock_src = {
            "name": "test",
            "service_id": "test",
            "service_name": "Test",
            "access_level": "read_write",
            "storage_mode": "cloud",
            "bucket": "b",
            "prefix": "",
            "region": "us-east-1",
            "log_period": 60,
            "provisioning": {},
        }

        captured_duration = {}

        def fake_log_cron_run(src, task, duration_s, status, **kwargs):
            captured_duration["value"] = duration_s
            captured_duration["type"] = type(duration_s).__name__

        def fake_ingest(**kwargs):
            yield {"type": "done", "new_files": 0}

        with (
            patch("backend.config.load_config", return_value={"provisioning": {}}),
            patch("backend.core.duckdb.get_source_for_service", return_value=mock_src),
            patch("backend.core.duckdb.start_cron_run", return_value=1),
            patch("backend.core.duckdb.log_cron_run", side_effect=fake_log_cron_run),
            patch("backend.core.duckdb.refresh_config_status"),
            patch("backend.core.ingest.ingest", side_effect=fake_ingest),
            patch("backend.cron_progress.cleanup_progress"),
            patch("backend.cron_progress.start_progress"),
            patch("backend.cron_progress.end_progress"),
            patch("backend.cron_progress.get_progress", return_value=[]),
            patch("backend.core.duckdb.backfill_fastly_edge_writes"),
            patch("backend.core.duckdb.reconcile_fastly_stats"),
            patch("backend.utils.usage_logger.flush_usage_log"),
            patch("backend.utils.usage_logger.run_usage_log_cleanup"),
            patch("backend.core.duckdb.update_cron_duration"),
        ):
            _run_log_discovery_cron("test", force=True, run_id=1)

        assert "value" in captured_duration, "log_cron_run was never called"
        assert isinstance(captured_duration["value"], float), (
            f"duration_s passed to log_cron_run was {captured_duration['type']}, expected float. "
            "This means start_time (str|None) was subtracted from time.time() instead of start_time_exec."
        )
        assert captured_duration["value"] >= 0.0


class TestIngestTimingVariableNames:
    """Verify the variable naming convention is correct in ingest source."""

    def test_start_time_exec_exists_in_ingest(self):
        """ingest() must define start_time_exec as the wall-clock timer."""
        import ast
        import pathlib

        src = pathlib.Path("backend/core/ingest.py").read_text()
        tree = ast.parse(src)

        # Find the ingest function
        ingest_fn = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "ingest")

        # Look for `start_time_exec = time.time()` assignment
        assignments = [
            node
            for node in ast.walk(ingest_fn)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "start_time_exec" for t in node.targets)
        ]
        assert assignments, "start_time_exec variable not found in ingest() — timing regression risk"

    def test_no_time_minus_start_time_in_ingest(self):
        """ingest() must not subtract the str|None `start_time` param from time.time()."""
        import ast
        import pathlib

        src = pathlib.Path("backend/core/ingest.py").read_text()
        tree = ast.parse(src)

        ingest_fn = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "ingest")

        # Find all BinOp subtractions where right side is a Name("start_time")
        bad_subtractions = [
            node
            for node in ast.walk(ingest_fn)
            if isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Sub)
            and isinstance(node.right, ast.Name)
            and node.right.id == "start_time"
        ]
        assert not bad_subtractions, (
            f"Found {len(bad_subtractions)} instance(s) of `... - start_time` in ingest(). "
            "Use start_time_exec (float) for wall-clock arithmetic, not start_time (str|None)."
        )

    def test_no_time_minus_start_time_in_scheduler(self):
        """_run_log_discovery_cron must not subtract the str|None start_time from time.time()."""
        import ast
        import pathlib

        # After the cron carve, _run_log_discovery_cron lives in backend/cron/jobs/sync.py.
        src = pathlib.Path("backend/cron/jobs/sync.py").read_text()
        tree = ast.parse(src)

        run_cron_fn = next(
            (
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == "_run_log_discovery_cron"
            ),
            None,
        )
        assert run_cron_fn is not None, "_run_log_discovery_cron not found in backend/cron/jobs/sync.py"

        bad_subtractions = [
            node
            for node in ast.walk(run_cron_fn)
            if isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Sub)
            and isinstance(node.right, ast.Name)
            and node.right.id == "start_time"
        ]
        assert not bad_subtractions, (
            f"Found {len(bad_subtractions)} instance(s) of `... - start_time` in _run_log_discovery_cron(). "
            "Use start_time_exec (float) for wall-clock arithmetic."
        )
