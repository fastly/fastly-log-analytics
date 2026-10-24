"""Tests for ``backend.cron`` — APScheduler wrapper + small jobs.

The big per-service jobs (``_run_log_discovery_cron``, ``_run_commit``,
``_run_optimize``, ``_run_expire_snapshots``, ``_run_ngwaf_bot_sync``,
``_run_service_alerts_evaluation``, ``_run_metadata_sync``) are heavy
orchestrators that exercise the full DuckDB+Iceberg+FOS stack; they're
covered indirectly by the integration tests in
[test_provision_lifecycle.py](tests/routers/test_provision_lifecycle.py)
and friends.

This file pins the **wrapper + smaller utilities** that the big jobs
share:

  - `_elapsed_since` — pure timer formatter
  - `_extract_log_text` — progress-store digest for log download
  - `get_scheduler` — singleton factory
  - `Scheduler.start` / `shutdown` / `get_job` — lifecycle
  - `Scheduler._sync_jobs` — config-driven job registration (the
    most important behaviour — what jobs land on the APScheduler
    depending on per-service config flags)
  - `_log_and_add_progress` — event icon/color formatting + storage
  - `_check_disk_space` / `_check_buffer_backlog` — scheduler helpers
"""

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
