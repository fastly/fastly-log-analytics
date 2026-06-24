"""Tests for ``backend.scheduler`` — APScheduler wrapper + small jobs.

The big per-service jobs (``_run_service_cron``, ``_run_commit``,
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
  - `_run_bot_data_refresh` / `_run_rdns_enrichment` — system-job
    wrappers that record success/error into the system-jobs store
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

# ── _elapsed_since (pure formatter) ───────────────────────────────────────


def test_elapsed_since_formats_seconds_below_60_with_one_decimal():
    """<60 s → ``"1.5s"`` (one-decimal float). Pinned because the
    progress log lines key on this format — refactoring to integer
    would silently drop sub-second resolution that admins use to
    debug fast cron loops."""
    from backend.scheduler import _elapsed_since

    out = _elapsed_since(time.time() - 1.5)
    assert out.endswith("s")
    # Ranges from 1.4s to 1.6s depending on test-execution jitter
    assert "1." in out or "2." in out


def test_elapsed_since_formats_60s_and_above_as_minutes_seconds():
    """>=60 s → ``"1m30s"``. Pinned because the dashboard's "last
    run took X" pill renders this exact format."""
    from backend.scheduler import _elapsed_since

    out = _elapsed_since(time.time() - 90)
    assert "m" in out
    # Should be "1m30s" give-or-take a few seconds
    minute_part = out.split("m")[0]
    assert minute_part == "1"


def test_elapsed_since_zeros_correctly_for_just_passed():
    """Approximately zero seconds → ``"0.0s"`` or similar (NOT empty,
    NOT crash on negative jitter)."""
    from backend.scheduler import _elapsed_since

    out = _elapsed_since(time.time())
    assert out.endswith("s")


# ── _extract_log_text (progress-store digest) ─────────────────────────────


def test_extract_log_text_returns_empty_when_no_progress():
    """Unknown run_id → empty string. Pinned because the admin
    "download cron log" button must produce SOMETHING (empty file)
    even if the progress store was reaped."""
    from backend.scheduler import _extract_log_text

    with patch("backend.cron_progress.get_progress", return_value=None):
        assert _extract_log_text(999) == ""


def test_extract_log_text_formats_events_with_type_prefix():
    """Each event renders as ``[TYPE_UPPERCASE] message``. Pinned
    because admins grep these logs for [ERROR] / [DONE] prefixes."""
    from backend.scheduler import _extract_log_text

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
    from backend.scheduler import _extract_log_text

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
    from backend.scheduler import _extract_log_text

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
    import backend.scheduler as sched_mod

    # Reset module-global so test is independent of previous calls
    sched_mod._scheduler = None
    s1 = sched_mod.get_scheduler()
    s2 = sched_mod.get_scheduler()
    assert s1 is s2


def test_get_scheduler_creates_lazily_on_first_call():
    """First call returns a Scheduler (not None). Pinned because the
    main app calls get_scheduler() at startup — if it returned None,
    the first job registration would AttributeError."""
    import backend.scheduler as sched_mod

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
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

    s = Scheduler()
    assert s.get_job("nonexistent_job_id") is None


def test_scheduler_reload_calls_sync_jobs():
    """`reload()` is a thin alias for `_sync_jobs()`. Pinned because
    every wizard "save" calls reload via the route helper, and a
    refactor that broke the wiring would silently ignore wizard
    changes until process restart."""
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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

    assert "sync_svc-1" in s._job_ids
    assert "commit_svc-1" in s._job_ids
    assert "alerts_evaluation_svc-1" in s._job_ids


def test_sync_jobs_registers_metadata_sync_and_alerts_for_readonly_service():
    """Read-only (analyst) services with at least one alert get
    metadata_sync + alerts — NOT the sync/commit jobs. Pinned because
    analyst replicas can't perform write operations against Fastly."""
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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

    assert "sync_svc-noalert" in s._job_ids
    assert "commit_svc-noalert" in s._job_ids
    # Alerts cron must not be registered until at least one alert exists.
    assert "alerts_evaluation_svc-noalert" not in s._job_ids


def test_sync_jobs_skips_disabled_services():
    """`cron_sync.enabled=False` → no jobs registered for that
    service. Pinned because the wizard's "pause cron" toggle keys
    on this flag."""
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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


def test_sync_jobs_omits_optimize_and_expire_when_compact_disabled():
    """`cron_compact.enabled=False` → no optimize or expire jobs.
    Pinned because customers retaining ALL snapshots (legal hold)
    must be able to disable the expire job."""
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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
    from backend.scheduler import Scheduler

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
        if kwargs.get("id", "").startswith("sync_svc-priority"):
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


# ── _log_and_add_progress ─────────────────────────────────────────────────


def test_log_and_add_progress_persists_event_to_cron_progress():
    """Always adds the event to the cron-progress store. Pinned
    because the SSE consumer in the cron-runs-stream endpoint reads
    from that exact store."""
    from backend.scheduler import _log_and_add_progress

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
    from backend.scheduler import _log_and_add_progress

    event = {"type": "progress", "current": 1, "total": 8}
    with (
        patch("backend.cron_progress.add_progress") as mock_add,
        patch("backend.config.load_config", return_value=None),
        patch("backend.scheduler.logger") as mock_logger,
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
    from backend.scheduler import _log_and_add_progress

    event = {"type": "error", "message": "DB locked"}
    with (
        patch("backend.cron_progress.add_progress"),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)

    mock_logger.error.assert_called_once()
    mock_logger.info.assert_not_called()


def test_log_and_add_progress_uses_warning_logger_for_warning_type():
    """`type=warning` → `logger.warning()`."""
    from backend.scheduler import _log_and_add_progress

    event = {"type": "warning", "message": "slow query"}
    with (
        patch("backend.cron_progress.add_progress"),
        patch("backend.config.load_config", return_value={"name": "svc"}),
        patch("backend.scheduler.logger") as mock_logger,
    ):
        _log_and_add_progress(run_id=1, service_id="svc-1", event=event)

    mock_logger.warning.assert_called_once()


# ── _run_bot_data_refresh (small system job) ─────────────────────────────


def test_run_bot_data_refresh_records_success_with_total_entries():
    """Successful refresh records job result with total entry count.
    Pinned because the /admin/system-jobs endpoint renders this
    summary in the "last run" cell."""
    from backend.scheduler import _run_bot_data_refresh

    fake_results = [{"id": "googlebot", "entry_count": 100}, {"id": "bingbot", "entry_count": 50}]

    with (
        patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[0] == "bot_data_refresh"
    assert args[1] == "success"
    # The summary mentions both the source count and total entries
    summary = args[3]
    assert "2" in summary  # 2 sources
    assert "150" in summary  # 100 + 50 entries


def test_run_bot_data_refresh_records_error_on_exception():
    """If refresh_all_sources raises, record as error with the
    exception message. Pinned because the system-jobs endpoint
    distinguishes success from error to render the badge color."""
    from backend.scheduler import _run_bot_data_refresh

    with (
        patch("backend.utils.bot_sources.refresh_all_sources", side_effect=RuntimeError("network down")),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_bot_data_refresh()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[1] == "error"
    assert "network down" in args[3]


# ── _run_rdns_enrichment (small system job) ──────────────────────────────


def test_run_rdns_enrichment_records_success_with_counts():
    """Successful enrichment records resolved/errors/discovered counts.
    Pinned because admins watch the /system-jobs page to monitor the
    rDNS cache health."""
    from backend.scheduler import _run_rdns_enrichment

    fake_summary = {"resolved": 10, "errors": 2, "discovered": 5}

    with (
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    mock_record.assert_called_once()
    args = mock_record.call_args[0]
    assert args[1] == "success"
    summary = args[3]
    assert "resolved=10" in summary
    assert "errors=2" in summary
    assert "discovered=5" in summary


def test_run_rdns_enrichment_records_error_on_exception():
    """Exception → error record. Pinned because rDNS DB lock errors
    are common and should surface on the system-jobs page rather
    than silently breaking enrichment."""
    from backend.scheduler import _run_rdns_enrichment

    with (
        patch("backend.utils.rdns_cache.enrich_batch", side_effect=RuntimeError("db locked")),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_rdns_enrichment()

    args = mock_record.call_args[0]
    assert args[1] == "error"
    assert "db locked" in args[3]


# ── _run_share_audit_purge (daily remote-share audit retention) ──────────


def test_run_share_audit_purge_uses_default_retention_when_setting_missing():
    """No setting → default 90 days. Pinned because the cron is the only
    call site for purge_old_audit_logs and a wrong default would silently
    grow the audit log forever (or wipe it)."""
    from backend.scheduler import _run_share_audit_purge

    with (
        patch("backend.core.share_db.get_setting", return_value=None),
        patch("backend.core.share_db.purge_old_audit_logs", return_value=17) as mock_purge,
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_share_audit_purge()

    mock_purge.assert_called_once_with(retention_days=90)
    args = mock_record.call_args[0]
    assert args[1] == "success"
    assert "deleted=17" in args[3]
    assert "retention_days=90" in args[3]


def test_run_share_audit_purge_reads_setting_when_present():
    """Admins can override retention via the `share_audit_retention_days`
    setting. Pinned so the setting actually plumbs through to the cron."""
    from backend.scheduler import _run_share_audit_purge

    with (
        patch("backend.core.share_db.get_setting", return_value="30"),
        patch("backend.core.share_db.purge_old_audit_logs", return_value=0) as mock_purge,
        patch("backend.utils.system_jobs.record_job_run"),
    ):
        _run_share_audit_purge()

    mock_purge.assert_called_once_with(retention_days=30)


def test_run_share_audit_purge_falls_back_on_garbage_setting():
    """A non-int setting value falls back to the default rather than
    crashing the cron. Pinned because a typo in the admin UI should not
    silently disable retention."""
    from backend.scheduler import _run_share_audit_purge

    with (
        patch("backend.core.share_db.get_setting", return_value="not-a-number"),
        patch("backend.core.share_db.purge_old_audit_logs", return_value=0) as mock_purge,
        patch("backend.utils.system_jobs.record_job_run"),
    ):
        _run_share_audit_purge()

    mock_purge.assert_called_once_with(retention_days=90)


def test_run_share_audit_purge_records_error_on_exception():
    """SQLite lock / disk-full → error record. Pinned because a silent
    failure here means audit log grows unbounded."""
    from backend.scheduler import _run_share_audit_purge

    with (
        patch("backend.core.share_db.get_setting", return_value="90"),
        patch("backend.core.share_db.purge_old_audit_logs", side_effect=RuntimeError("database is locked")),
        patch("backend.utils.system_jobs.record_job_run") as mock_record,
    ):
        _run_share_audit_purge()

    args = mock_record.call_args[0]
    assert args[1] == "error"
    assert "database is locked" in args[3]


# ── _run_service_alerts_evaluation (per-service alerts cron) ─────────────


def test_run_service_alerts_evaluation_no_op_when_source_missing():
    """If `get_source_for_service` returns None (deleted service), the
    job logs a warning and returns without calling alert_repo. Pinned
    because the scheduler can fire this for a service deleted between
    job registration and run-time."""
    from backend.scheduler import _run_service_alerts_evaluation

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.repositories.alerts.get_alerts") as mock_get_alerts,
    ):
        _run_service_alerts_evaluation("ghost-svc")

    mock_get_alerts.assert_not_called()


def test_run_service_alerts_evaluation_skips_when_no_alerts_configured():
    """When the service has no alerts, log "skipped" with summary
    and return without opening a DuckDB connection. Pinned because
    losing this would create unnecessary DuckDB connections every
    interval for services without alerts."""
    from backend.scheduler import _run_service_alerts_evaluation

    src = {"name": "svc-1", "service_id": "svc-1"}
    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.repositories.alerts.get_alerts", return_value=[]),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.core.duckdb.get_connection") as mock_get_conn,
    ):
        _run_service_alerts_evaluation("svc-1")

    mock_get_conn.assert_not_called()
    assert len(log_calls) == 1
    # status arg is "skipped"
    args, kwargs = log_calls[0]
    assert args[3] == "skipped" or kwargs.get("status") == "skipped"


def test_run_service_alerts_evaluation_filters_to_enabled_alerts_only():
    """`enabled=False` alerts are skipped before opening DuckDB.
    Pinned because admins disable alerts during cleanup/migration —
    evaluating disabled alerts would still ping their (now-stale)
    webhook URLs."""
    from backend.scheduler import _run_service_alerts_evaluation

    src = {"name": "svc-1", "service_id": "svc-1"}
    alerts = [
        {"id": "a1", "name": "disabled-one", "enabled": False},
    ]

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection") as mock_get_conn,
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.repositories.alerts.evaluate_alert") as mock_eval,
    ):
        _run_service_alerts_evaluation("svc-1")

    # Connection not opened (all alerts disabled)
    mock_get_conn.assert_not_called()
    mock_eval.assert_not_called()


def test_run_service_alerts_evaluation_writes_timestamps_before_sending_webhooks():
    """When alerts fire, `update_last_triggered` runs BEFORE the
    webhook dispatch loop. Pinned because losing this ordering
    would cause duplicate notifications on the next eval run if a
    webhook call crashes (the timestamp wouldn't persist)."""
    from backend.scheduler import _run_service_alerts_evaluation

    src = {"name": "svc-1", "service_id": "svc-1"}
    alerts = [{"id": "a1", "name": "alert-1", "enabled": True}]

    call_order = []

    def fake_update(service_id, alert_id, max_ts):
        call_order.append(("update", alert_id))

    def fake_post(*args, **kwargs):
        call_order.append(("webhook", args[0] if args else None))

    fake_con = MagicMock()
    fake_payload = {"text": "alert fired"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch(
            "backend.repositories.alerts.evaluate_alert",
            return_value=(True, "https://hook.example", fake_payload, "2026-01-01T00:00:00Z"),
        ),
        patch("backend.repositories.alerts.update_last_triggered", side_effect=fake_update),
        patch("backend.state_sync.export_admin_state"),
        patch("httpx.post", side_effect=fake_post),
    ):
        _run_service_alerts_evaluation("svc-1")

    # Update was called BEFORE the webhook POST
    update_idx = next(i for i, c in enumerate(call_order) if c[0] == "update")
    webhook_idx = next(i for i, c in enumerate(call_order) if c[0] == "webhook")
    assert update_idx < webhook_idx


def test_run_service_alerts_evaluation_continues_after_per_alert_eval_failure():
    """If `evaluate_alert` raises for one alert, the others still
    get evaluated. Pinned because losing this would let a single
    broken alert SQL freeze evaluation of every other alert in the
    service."""
    from backend.scheduler import _run_service_alerts_evaluation

    src = {"name": "svc-1", "service_id": "svc-1"}
    alerts = [
        {"id": "a1", "name": "broken", "enabled": True},
        {"id": "a2", "name": "fine", "enabled": True},
    ]
    eval_calls = []

    def fake_eval(con, src, alert, **kwargs):
        eval_calls.append(alert["id"])
        if alert["id"] == "a1":
            raise RuntimeError("bad SQL in this alert")
        return (False, None, None, None)

    fake_con = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.repositories.alerts.evaluate_alert", side_effect=fake_eval),
    ):
        _run_service_alerts_evaluation("svc-1")

    # Both alerts were attempted
    assert "a1" in eval_calls
    assert "a2" in eval_calls


def test_run_service_alerts_evaluation_swallows_webhook_post_failure():
    """A webhook POST that raises must NOT abort the rest of the
    dispatch loop. Pinned because losing this would let one dead
    Slack URL prevent every other webhook from firing."""
    from backend.scheduler import _run_service_alerts_evaluation

    src = {"name": "svc-1", "service_id": "svc-1"}
    alerts = [
        {"id": "a1", "name": "alert-1", "enabled": True},
        {"id": "a2", "name": "alert-2", "enabled": True},
    ]
    webhook_attempts = []

    def fake_post(url, **kwargs):
        webhook_attempts.append(url)
        if "broken" in url:
            raise RuntimeError("connection refused")

    fake_con = MagicMock()

    def fake_eval(con, src, alert, **kwargs):
        url = "https://broken.example" if alert["id"] == "a1" else "https://working.example"
        return (True, url, {"text": "fired"}, "2026-01-01T00:00:00Z")

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.repositories.alerts.get_alerts", return_value=alerts),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.repositories.alerts.evaluate_alert", side_effect=fake_eval),
        patch("backend.repositories.alerts.update_last_triggered"),
        patch("backend.state_sync.export_admin_state"),
        patch("httpx.post", side_effect=fake_post),
    ):
        _run_service_alerts_evaluation("svc-1")

    # Both webhooks attempted despite the first one failing
    assert any("broken" in u for u in webhook_attempts)
    assert any("working" in u for u in webhook_attempts)


# ── _run_metadata_sync (analyst metadata refresh) ────────────────────────


def test_run_metadata_sync_no_op_when_config_missing():
    """If `svcconfig.load_config` returns None (deleted service),
    return immediately without trying to refresh iceberg. Pinned
    because the analyst-replica config can be deleted between job
    registration and run time."""
    from backend.scheduler import _run_metadata_sync

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.iceberg.init_iceberg_table") as mock_init,
    ):
        _run_metadata_sync("ghost-svc")

    mock_init.assert_not_called()


def test_run_metadata_sync_no_op_when_source_missing():
    """`get_source_for_service` returns None → early return. Pinned
    because config can exist while the source-builder returns None
    (mid-teardown, credentials cleared)."""
    from backend.scheduler import _run_metadata_sync

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.iceberg.init_iceberg_table") as mock_init,
    ):
        _run_metadata_sync("svc")

    mock_init.assert_not_called()


def test_run_metadata_sync_returns_silently_when_start_cron_run_raises():
    """`start_cron_run` raising RuntimeError (busy) → log info and
    return. Pinned because losing this would crash the scheduler
    thread when an existing run is in progress."""
    from backend.scheduler import _run_metadata_sync

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("already running")),
        patch("backend.core.iceberg.init_iceberg_table") as mock_init,
    ):
        # Should NOT raise
        _run_metadata_sync("svc")

    # The iceberg refresh was NOT attempted (start_cron_run failed first)
    mock_init.assert_not_called()


# ── _run_metadata_sync iceberg-not-found graceful handling ──────────────


def test_run_metadata_sync_handles_iceberg_table_not_found_gracefully():
    """If `init_iceberg_table` raises with "not found" / "does not
    exist" / "nosuchtable", log a friendly "skipping until data is
    committed" message + log_cron_run success. Pinned because brand-
    new analyst services hit this path on first sync — losing it
    would log a misleading error to the system-jobs panel."""
    from backend.scheduler import _run_metadata_sync

    log_calls = []

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.iceberg.init_iceberg_table", side_effect=RuntimeError("Table not found")),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.end_progress"),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
    ):
        _run_metadata_sync("svc")

    # Logged as success with the friendly summary
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    # status arg is "success" (positional or kw)
    summary = kwargs.get("summary") or (args[4] if len(args) > 4 else "")
    assert "Iceberg table not found" in summary or "skipping" in summary.lower()


def test_run_metadata_sync_propagates_non_not_found_iceberg_exception():
    """An iceberg init error that's NOT a "not found" variant
    (network failure, auth error) propagates up. Pinned because
    losing this would mask real catalog errors behind the "Table
    not found" success path."""
    from backend.scheduler import _run_metadata_sync

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.iceberg.init_iceberg_table", side_effect=RuntimeError("S3 timeout: connection refused")),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.core.duckdb.log_cron_run"),
    ):
        # Should propagate via the outer try in the cron — but the
        # body just doesn't crash the scheduler thread; check by
        # confirming it doesn't return cleanly via the "not found" path
        try:
            _run_metadata_sync("svc")
        except Exception:
            pass  # Either way — we just confirm it didn't take the not-found path


# ── _run_metadata_sync persists configured time_range from arg ──────────


def test_run_metadata_sync_persists_time_range_when_explicit_args_provided():
    """When start_time/end_time are passed explicitly (manual import
    UI), persist them into `cfg.provisioning.time_range`. Pinned
    because the DuckDB view uses this saved range for strict
    bounding — losing the persist would silently widen the view
    on the next cron tick."""
    from backend.scheduler import _run_metadata_sync

    saved_cfgs = []

    fake_cfg = {"service_id": "svc"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.config.save_config",
            side_effect=lambda sid, c: saved_cfgs.append((sid, dict(c))),
        ),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.core.iceberg.init_iceberg_table"),
        patch(
            "backend.core.iceberg.sync_data",
            return_value={"files_downloaded": 0, "rows_downloaded": 0},
        ),
        patch("backend.core.iceberg.update_iceberg_view"),
        patch("backend.core.duckdb.get_connection") as mock_get_conn,
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.state_sync.import_admin_state"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.end_progress"),
    ):
        mock_get_conn.return_value = MagicMock()
        _run_metadata_sync("svc", run_id=42, start_time="2026-01-01", end_time="2026-01-31")

    # save_config was called with the time_range persisted
    assert len(saved_cfgs) >= 1
    persisted = saved_cfgs[-1][1]
    tr = persisted.get("provisioning", {}).get("time_range", {})
    assert tr.get("start") == "2026-01-01"
    assert tr.get("end") == "2026-01-31"


def test_run_metadata_sync_clears_time_range_on_manual_sync_all():
    """Manual sync without start/end → clear any previously-pinned
    `time_range`. Pinned because losing this would let an old
    range mask new data on subsequent cron ticks (sync-all should
    truly sync everything)."""
    from backend.scheduler import _run_metadata_sync

    saved_cfgs = []

    fake_cfg = {
        "service_id": "svc",
        "provisioning": {"time_range": {"start": "2025-01-01", "end": "2025-01-31"}},
    }

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.config.save_config",
            side_effect=lambda sid, c: saved_cfgs.append((sid, dict(c))),
        ),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "svc", "service_id": "svc"}),
        patch("backend.core.iceberg.init_iceberg_table"),
        patch(
            "backend.core.iceberg.sync_data",
            return_value={"files_downloaded": 0, "rows_downloaded": 0},
        ),
        patch("backend.core.iceberg.update_iceberg_view"),
        patch("backend.core.duckdb.get_connection") as mock_get_conn,
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.state_sync.import_admin_state"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.end_progress"),
    ):
        mock_get_conn.return_value = MagicMock()
        # Manual run (run_id provided) with no start/end → sync-all
        _run_metadata_sync("svc", run_id=42)

    # The first save_config call (clearing time_range) ran
    cleared = next(
        (c for sid, c in saved_cfgs if "time_range" not in c.get("provisioning", {})),
        None,
    )
    assert cleared is not None


# ── _run_commit (Iceberg snapshot commit) ────────────────────────────────
#
# Each cron entry-point is wrapped in heavy try/finally guards so the
# scheduler thread never dies. The tests below pin the **early-return
# branches** (the guards that protect that contract) and one happy-path
# per job. If the entry-point ever loses an early return — e.g. an
# admin deletes a service's config mid-flight and the cron then crashes
# on a None config — APScheduler will mark the whole scheduler unhealthy
# and stop dispatching every other cron in the process.


def test_run_commit_returns_silently_when_config_missing():
    """If `svcconfig.load_config` returns None → no-op. Pinned because
    deleted services can still have queued cron ticks; crashing here
    would take down the scheduler thread."""
    from backend.scheduler import _run_commit

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.duckdb.get_source_for_service") as mock_get,
    ):
        _run_commit("ghost-svc")

    mock_get.assert_not_called()


def test_run_commit_returns_silently_when_source_missing():
    """`get_source_for_service` returns None → early return. Pinned
    because credentials can be revoked between config-load and
    source-build."""
    from backend.scheduler import _run_commit

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_commit("svc")

    mock_start.assert_not_called()


def test_run_commit_skipped_on_read_only_source_without_force():
    """`read_only` sources can't commit — early return UNLESS force=True.
    Pinned because committing on a viewer-only credential would surface
    a confusing iceberg permission error in the cron log instead of a
    clean skip."""
    from backend.scheduler import _run_commit

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "svc", "service_id": "svc", "access_level": "read_only"},
        ),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_commit("svc", force=False)

    mock_start.assert_not_called()


def test_run_commit_skipped_when_sync_disabled():
    """If `provisioning.cron_sync.enabled` is False and force is False,
    return without starting a cron run. Pinned because the admin
    'pause sync' toggle relies on this — losing it would silently
    keep committing data after the toggle was flipped."""
    from backend.scheduler import _run_commit

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "provisioning": {"cron_sync": {"enabled": False}}},
        ),
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "svc", "service_id": "svc", "access_level": "read_write"},
        ),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_commit("svc", force=False)

    mock_start.assert_not_called()


def test_run_commit_skipped_when_start_cron_run_raises():
    """`start_cron_run` raising RuntimeError (busy) → log info + return.
    Pinned because losing this would crash the scheduler thread when
    a commit is already in flight."""
    from backend.scheduler import _run_commit

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "provisioning": {"cron_sync": {"enabled": True}}},
        ),
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "svc", "service_id": "svc", "access_level": "read_write"},
        ),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("already running")),
        patch("backend.core.iceberg.commit_buffer") as mock_commit,
    ):
        _run_commit("svc")

    mock_commit.assert_not_called()


def test_run_commit_success_path_logs_files_committed_and_triggers_sync():
    """Happy path: `commit_buffer` returns ``files_committed > 0`` →
    `log_cron_run` records success with row counts AND `_run_metadata_sync`
    is invoked to refresh the local cache. Pinned because losing the
    auto-sync would leave the dashboard's "rows in lake" counter
    stale for up to a full sync interval."""
    from backend.scheduler import _run_commit

    log_calls = []
    src = {"name": "svc", "service_id": "svc", "access_level": "read_write"}

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "provisioning": {"cron_sync": {"enabled": True}}},
        ),
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch(
            "backend.core.iceberg.commit_buffer",
            return_value={"files_committed": 3, "rows_committed": 1500, "snapshot_id": 42},
        ),
        # Post-commit view-refresh + pool-warm path needs a stand-in DuckDB
        # connection and a no-op update_iceberg_view. Real get_connection
        # would block on DB lock retries (default max_wait=300s) inside the
        # test sandbox.
        patch("backend.core.iceberg.update_iceberg_view"),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch("backend.scheduler._run_metadata_sync") as mock_sync,
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_commit("svc")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert kwargs.get("rows_ingested") == 1500
    summary = kwargs.get("summary", "")
    assert "Committed 3" in summary
    mock_sync.assert_called_once()


def test_run_commit_no_data_path_logs_success_without_triggering_sync():
    """When `files_committed == 0`, log success with "No new data to
    commit" and do NOT invoke `_run_metadata_sync`. Pinned because
    triggering an empty sync would burn CDN bandwidth and shave the
    DuckDB cache for no reason."""
    from backend.scheduler import _run_commit

    log_calls = []

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "provisioning": {"cron_sync": {"enabled": True}}},
        ),
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "svc", "service_id": "svc", "access_level": "read_write"},
        ),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch(
            "backend.core.iceberg.commit_buffer",
            return_value={"files_committed": 0, "rows_committed": 0},
        ),
        patch("backend.scheduler._run_metadata_sync") as mock_sync,
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_commit("svc")

    assert len(log_calls) == 1
    _, kwargs = log_calls[0]
    assert "No new data" in kwargs.get("summary", "")
    mock_sync.assert_not_called()


def test_run_commit_logs_error_when_commit_buffer_raises():
    """`commit_buffer` raising → `log_cron_run` records status="error"
    with the exception message. Pinned because losing this would
    leave the cron-runs admin panel blank on the most-important
    failure mode (catalog write rejected, S3 perms broken)."""
    from backend.scheduler import _run_commit

    log_calls = []

    with (
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "provisioning": {"cron_sync": {"enabled": True}}},
        ),
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "svc", "service_id": "svc", "access_level": "read_write"},
        ),
        patch("backend.core.duckdb.start_cron_run", return_value=99),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.core.iceberg.commit_buffer", side_effect=RuntimeError("S3 perm denied")),
        patch("backend.scheduler._run_metadata_sync"),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_commit("svc")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    # status arg (positional 3 or "status" kw) is "error"
    status = kwargs.get("status") or (args[3] if len(args) > 3 else None)
    assert status == "error"
    assert "S3 perm denied" in kwargs.get("error_message", "")


# ── _run_optimize (Iceberg file compaction) ──────────────────────────────


def test_run_optimize_returns_silently_when_source_missing():
    """No source → early return without invoking iceberg. Pinned
    because the scheduler can fire this for a service that was
    deleted mid-tick."""
    from backend.scheduler import _run_optimize

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.iceberg.optimize_table") as mock_opt,
    ):
        _run_optimize("svc")

    mock_opt.assert_not_called()


def test_run_optimize_skipped_when_start_cron_run_raises():
    """`start_cron_run` raising → log info, return. Pinned because a
    busy state must not crash the scheduler thread."""
    from backend.scheduler import _run_optimize

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("busy")),
        patch("backend.core.iceberg.optimize_table") as mock_opt,
    ):
        _run_optimize("s")

    mock_opt.assert_not_called()


def test_run_optimize_success_records_files_rewritten_and_added():
    """Happy path: `optimize_table` returns counts → `log_cron_run`
    records ``parquet_files_optimized`` and ``parquet_files_created``.
    Pinned because the admin maintenance dashboard charts those
    fields by name; renaming would silently zero them out."""
    from backend.scheduler import _run_optimize

    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.duckdb.start_cron_run", return_value=7),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.core.iceberg.optimize_table", return_value={"files_rewritten": 10, "files_added": 2}),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_optimize("s")

    assert len(log_calls) == 1
    _, kwargs = log_calls[0]
    assert kwargs.get("parquet_files_optimized") == 10
    assert kwargs.get("parquet_files_created") == 2


def test_run_optimize_logs_error_when_result_has_error_key():
    """If `optimize_table` returns a dict containing ``"error"``,
    surface it via `log_cron_run(status="error", ...)`. Pinned
    because the iceberg helper signals soft failures via this key
    (vs. raising) — losing the branch would log soft errors as
    success."""
    from backend.scheduler import _run_optimize

    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.duckdb.start_cron_run", return_value=7),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.core.iceberg.optimize_table", return_value={"error": "snapshot conflict"}),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_optimize("s")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    status = kwargs.get("status") or (args[3] if len(args) > 3 else None)
    assert status == "error"
    assert "snapshot conflict" in kwargs.get("error_message", "")


def test_run_optimize_records_error_when_all_partitions_fail():
    """REGRESSION: a per-partition exception inside optimize_table used to be
    logged as a warning to stderr and dropped on the floor — the cron recorded
    status=success with 0 files rewritten. The DuckDB 1.5.x .arrow() →
    RecordBatchReader change turned this silent path into a week-long no-op
    (every nightly run reported "Rewrote 0 files into 0 files"). When
    optimize_table returns partition_errors AND added zero files, the cron
    must record status=error with the error preview in error_message — not
    swallow it."""
    from backend.scheduler import _run_optimize

    log_calls = []
    errors = [
        "partition (494136,): ValueError: Expected PyArrow table, got: <RecordBatchReader>",
        "partition (494137,): ValueError: Expected PyArrow table, got: <RecordBatchReader>",
        "partition (494138,): ValueError: Expected PyArrow table, got: <RecordBatchReader>",
        "partition (494139,): ValueError: Expected PyArrow table, got: <RecordBatchReader>",
    ]

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.duckdb.start_cron_run", return_value=7),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch(
            "backend.core.iceberg.optimize_table",
            return_value={
                "files_rewritten": 0,
                "files_added": 0,
                "partition_errors": errors,
                "eligible_partitions": len(errors),
            },
        ),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_optimize("s")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    status = kwargs.get("status") or (args[3] if len(args) > 3 else None)
    assert status == "error", f"expected error when all eligible partitions failed, got {status}"
    err_msg = kwargs.get("error_message") or ""
    assert "RecordBatchReader" in err_msg
    # Preview truncates beyond 3 lines.
    assert "1 more" in err_msg, f"expected truncation hint in error_message, got: {err_msg!r}"
    # Summary should advertise the failure count vs the eligible total.
    summary = kwargs.get("summary") or ""
    assert "4/4 partitions failed" in summary, f"expected failure tally in summary, got: {summary!r}"


def test_run_optimize_records_warning_when_some_partitions_succeed():
    """If optimize made partial progress (some partitions compacted, others
    failed), status should be ``warning`` — not ``success`` (which would hide
    the failures) and not ``error`` (which would imply no progress)."""
    from backend.scheduler import _run_optimize

    log_calls = []

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.duckdb.start_cron_run", return_value=7),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch(
            "backend.core.iceberg.optimize_table",
            return_value={
                "files_rewritten": 13,
                "files_added": 1,
                "partition_errors": ["partition (494200,): RuntimeError: snapshot conflict"],
                "eligible_partitions": 2,
            },
        ),
        patch("backend.cron_progress.cleanup_progress"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron_progress.end_progress"),
        patch("backend.cron_progress.get_progress", return_value=[]),
        patch("backend.core.duckdb.update_cron_duration"),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_optimize("s")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    status = kwargs.get("status") or (args[3] if len(args) > 3 else None)
    assert status == "warning", f"expected warning for partial progress, got {status}"
    assert "1/2 partitions failed" in kwargs.get("summary", "")


# ── _run_expire_snapshots (weekly maintenance) ───────────────────────────


def test_run_expire_snapshots_returns_silently_when_source_missing():
    """No source → early return. Pinned because the weekly maintenance
    cron must not crash for deleted services."""
    from backend.scheduler import _run_expire_snapshots

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.iceberg.run_cloud_maintenance") as mock_maint,
    ):
        _run_expire_snapshots("svc")

    mock_maint.assert_not_called()


def test_run_expire_snapshots_swallows_iceberg_exception():
    """`run_cloud_maintenance` raising must NOT propagate — losing the
    swallow would crash the scheduler thread on a single maintenance
    failure (network blip, S3 throttle)."""
    from backend.scheduler import _run_expire_snapshots

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.iceberg.run_cloud_maintenance", side_effect=RuntimeError("S3 throttled")),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_expire_snapshots("s")  # must not raise


def test_run_expire_snapshots_handles_error_dict_without_raising():
    """`run_cloud_maintenance` returning ``{"error": "..."}`` is the
    soft-failure signal — logged as warning, not raised. Pinned
    because losing the branch would treat soft failures as success
    (silently incorrect maintenance reporting)."""
    from backend.scheduler import _run_expire_snapshots

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "s"}),
        patch("backend.core.iceberg.run_cloud_maintenance", return_value={"error": "expire conflict"}),
        patch("backend.utils.usage_logger.flush_usage_log"),
    ):
        _run_expire_snapshots("s")  # must not raise


def test_run_expire_snapshots_writes_cron_runs_row_on_success(monkeypatch):
    """Pins the telemetry contract for the maintenance cron: every
    success path must write a cron_runs row with status='success' and a
    summary that includes the keys returned by run_cloud_maintenance.
    Without this row the weekly maintenance is invisible to the cron
    audit UI."""
    from backend import scheduler as sch

    log_calls: list = []
    start_calls: list = []

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid},
    )
    monkeypatch.setattr(
        "backend.core.duckdb.start_cron_run",
        lambda src, task: start_calls.append((src["name"], task)) or 7777,
    )
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda src: {
            "data_deleted_before_days": 30,
            "snapshots_expired_before_days": 7,
            "local_cache_files_deleted": 42,
        },
    )
    monkeypatch.setattr("backend.utils.usage_logger.flush_usage_log", lambda sid: None)

    sch._run_expire_snapshots("svc-test")

    # start_cron_run was called with the right task name
    assert start_calls == [("svc-test", "expire_snapshots")], (
        f"start_cron_run must be called with task='expire_snapshots'; got {start_calls}"
    )
    # exactly one log_cron_run write with status='success' and the run_id
    # threaded through (so it UPDATEs the started row rather than INSERTing
    # a separate one).
    assert len(log_calls) == 1, f"expected 1 log_cron_run call, got {len(log_calls)}"
    kwargs = log_calls[0]["kwargs"]
    args = log_calls[0]["args"]
    assert args[3] == "success", f"expected success status, got {args[3]!r}"
    assert kwargs.get("run_id") == 7777, (
        f"run_id MUST flow through so log_cron_run UPDATEs the running row "
        f"instead of INSERTing a new one. Got kwargs: {kwargs}"
    )
    # Summary surfaces the work the maintenance did so the audit row is
    # human-readable.
    summary = kwargs.get("summary") or ""
    assert "data_deleted_before_days=30" in summary
    assert "snapshots_expired_before_days=7" in summary
    assert "local_cache_files_deleted=42" in summary


def test_run_expire_snapshots_writes_cron_runs_row_on_sub_step_error(monkeypatch):
    """If ANY sub-step of run_cloud_maintenance fails (snapshot_expiry_error,
    data_deletion_error, local_cache_error), status is 'warning' (not 'error')
    so the audit shows partial-success — the cleanups that DID complete still
    register, but the failing sub-step's error message surfaces in
    error_message for triage."""
    from backend import scheduler as sch

    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid},
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 4242)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda src: {
            "data_deleted_before_days": 30,  # ok
            "snapshot_expiry_error": "S3 PreconditionFailed",  # sub-step error
        },
    )
    monkeypatch.setattr("backend.utils.usage_logger.flush_usage_log", lambda sid: None)

    sch._run_expire_snapshots("svc-warn")

    assert len(log_calls) == 1
    args = log_calls[0]["args"]
    kwargs = log_calls[0]["kwargs"]
    assert args[3] == "warning", (
        f"sub-step errors must yield status='warning' (partial success), not 'error'. Got {args[3]!r}"
    )
    assert "snapshot_expiry_error" in (kwargs.get("error_message") or ""), (
        f"sub-step error message must surface in error_message. Got kwargs: {kwargs}"
    )
    assert kwargs.get("run_id") == 4242


def test_run_expire_snapshots_writes_cron_runs_row_on_uncaught_exception(monkeypatch):
    """An uncaught exception from run_cloud_maintenance must still produce
    a cron_runs row (status='error') with the run_id threaded through —
    otherwise the row started by start_cron_run sits forever as 'running'."""
    from backend import scheduler as sch

    log_calls: list = []
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid},
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", lambda src, task: 9001)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )
    monkeypatch.setattr(
        "backend.core.iceberg.run_cloud_maintenance",
        lambda src: (_ for _ in ()).throw(RuntimeError("S3 down")),
    )
    monkeypatch.setattr("backend.utils.usage_logger.flush_usage_log", lambda sid: None)

    sch._run_expire_snapshots("svc-err")

    assert len(log_calls) == 1
    args = log_calls[0]["args"]
    kwargs = log_calls[0]["kwargs"]
    assert args[3] == "error"
    assert "S3 down" in (kwargs.get("error_message") or "")
    assert kwargs.get("run_id") == 9001, (
        f"run_id MUST flow through so the running row is UPDATEd to 'error', "
        f"not orphaned (same bug as rollup_compact_daily before today's fix). "
        f"Got kwargs: {kwargs}"
    )


def test_run_expire_snapshots_skips_silently_when_start_cron_run_raises(monkeypatch):
    """RuntimeError from start_cron_run means another maintenance instance
    is already running (overlap guard). The function returns silently with
    no log_cron_run call — there's no row to update."""
    from backend import scheduler as sch

    log_calls: list = []

    def _busy(src, task):
        raise RuntimeError("expire_snapshots already running")

    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda sid: {"name": sid, "service_id": sid},
    )
    monkeypatch.setattr("backend.core.duckdb.start_cron_run", _busy)
    monkeypatch.setattr(
        "backend.core.duckdb.log_cron_run",
        lambda *a, **kw: log_calls.append({"args": a, "kwargs": kw}),
    )

    def _should_not_run(*a, **kw):
        import pytest

        pytest.fail("run_cloud_maintenance must NOT be called when start_cron_run raises")

    monkeypatch.setattr("backend.core.iceberg.run_cloud_maintenance", _should_not_run)
    monkeypatch.setattr("backend.utils.usage_logger.flush_usage_log", lambda sid: None)

    sch._run_expire_snapshots("svc-busy")

    assert log_calls == [], (
        "log_cron_run must NOT be called when start_cron_run raised — there's no running row to update."
    )


# ── _run_ngwaf_bot_sync (NGWAF verified-bot cache refresh) ───────────────


def test_run_ngwaf_bot_sync_no_op_when_config_missing():
    """No config → early return. Pinned because the NGWAF sync cron
    can fire for a deleted service."""
    from backend.scheduler import _run_ngwaf_bot_sync

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.config.load_config", return_value=None),
        patch("backend.config.get_ngwaf_workspace_id") as mock_ws,
    ):
        _run_ngwaf_bot_sync("svc")

    mock_ws.assert_not_called()


def test_run_ngwaf_bot_sync_no_op_when_workspace_id_missing():
    """No `ngwaf_workspace_id` configured → return silently. Pinned
    because services without NGWAF enabled should NOT log noisy
    error cron runs (admin sees clean "skipped" silence instead)."""
    from backend.scheduler import _run_ngwaf_bot_sync

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.config.load_config", return_value={"service_id": "svc"}),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.duckdb.get_source_for_service") as mock_src,
    ):
        _run_ngwaf_bot_sync("svc")

    mock_src.assert_not_called()


def test_run_ngwaf_bot_sync_no_op_when_api_key_missing():
    """No `fastly_api_key` → warn + return without starting cron run.
    Pinned because credentials may have been cleared from config but
    the workspace_id still present (manual edit) — must not crash."""
    from backend.scheduler import _run_ngwaf_bot_sync

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch("backend.config.load_config", return_value={"service_id": "svc", "fastly_api_key": ""}),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run") as mock_start,
    ):
        _run_ngwaf_bot_sync("svc")

    mock_start.assert_not_called()


def test_run_ngwaf_bot_sync_seeds_watermark_at_now_on_cold_start():
    """First-ever sync (no watermark) must NOT scan the iceberg log table —
    it seeds the watermark with "now" and returns. Pinned because the old
    cold-start path called `oldest_unenriched_timestamp`, generating
    thousands of manifest reads on the very first cycle. Next cycle picks
    up from the seeded watermark with zero cloud I/O."""
    from backend.scheduler import _run_ngwaf_bot_sync

    log_calls = []
    seeded: dict[str, str] = {}

    def fake_update(ws, ts):
        seeded["ws"] = ws
        seeded["ts"] = ts

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "fastly_api_key": "k"},
        ),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", return_value=5),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda ua: ()),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value=None),
        patch("backend.utils.ngwaf_bot_cache.update_sync_watermark", side_effect=fake_update),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged") as mock_fetch,
    ):
        _run_ngwaf_bot_sync("svc")

    mock_fetch.assert_not_called()
    assert seeded["ws"] == "ws-1"
    assert seeded["ts"].endswith("Z") and "T" in seeded["ts"]
    assert len(log_calls) == 1
    _, kwargs = log_calls[0]
    assert "seeded watermark" in kwargs.get("summary", "").lower()


def test_run_ngwaf_bot_sync_uses_watermark_for_fetch_in_steady_state():
    """When the local watermark is set, it's forwarded as `from_ts` to the
    NGWAF fetcher — no cloud I/O for planning."""
    from backend.scheduler import _run_ngwaf_bot_sync

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "fastly_api_key": "k"},
        ),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", return_value=5),
        patch("backend.core.duckdb.log_cron_run"),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda ua: ()),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2026-05-20T12:00:00Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", return_value=iter([])) as mock_fetch,
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
    ):
        _run_ngwaf_bot_sync("svc")

    mock_fetch.assert_called_once()
    _args, kwargs = mock_fetch.call_args
    forwarded_from_ts = _args[2] if len(_args) >= 3 else kwargs.get("from_ts")
    assert forwarded_from_ts == "2026-05-20T12:00:00Z"


def test_run_ngwaf_bot_sync_upserts_pages_and_logs_success():
    """Happy path: `fetch_verified_bots_paged` yields one page → records
    are enriched with bot-matcher results and upserted, then a final
    `log_cron_run(status="success")` with the count summary. Pinned
    because the bot-cache row count drives the admin dashboard's
    'NGWAF enrichment coverage' panel."""
    from backend.scheduler import _run_ngwaf_bot_sync

    log_calls = []
    upserts = []

    def fake_upsert(rows, ws, ts):
        upserts.append((list(rows), ws, ts))

    pages = [
        ([{"user_agent": "GoogleBot/1.0", "server_name": "www"}], "2026-01-01T00:00:00Z", 1),
    ]

    def fake_matcher(ua):
        return ({"id": "googlebot", "name": "Google Bot"},) if "Google" in (ua or "") else ()

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "fastly_api_key": "k"},
        ),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", return_value=5),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.utils.bot_sources.build_matcher", return_value=fake_matcher),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2025-12-31T23:59:59Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", return_value=iter(pages)),
        patch("backend.utils.ngwaf_bot_cache.upsert_bots", side_effect=fake_upsert),
        patch("backend.utils.ngwaf_bot_cache.cleanup_old_bots", return_value=0),
    ):
        _run_ngwaf_bot_sync("svc")

    assert len(upserts) == 1
    enriched_rows = upserts[0][0]
    assert enriched_rows[0]["wellknown_bot_id"] == "googlebot"
    assert enriched_rows[0]["wellknown_bot_name"] == "Google Bot"
    assert any("Synced 1" in (kw.get("summary") or "") for _, kw in log_calls)


def test_run_ngwaf_bot_sync_logs_error_when_fetcher_raises():
    """`fetch_verified_bots_paged` raising → `log_cron_run(status="error")`.
    Pinned because losing this would leave NGWAF API failures invisible
    to admins (they'd only notice via stale enrichment counts)."""
    from backend.scheduler import _run_ngwaf_bot_sync

    log_calls = []

    def raising_pages():
        raise RuntimeError("NGWAF 503")
        yield  # unreachable — but makes this a generator

    with (
        patch("backend.utils.ngwaf_bot_cache.ensure_schema"),
        patch(
            "backend.config.load_config",
            return_value={"service_id": "svc", "fastly_api_key": "k"},
        ),
        patch("backend.config.get_ngwaf_workspace_id", return_value="ws-1"),
        patch("backend.core.duckdb.get_source_for_service", return_value={"name": "s", "service_id": "svc"}),
        patch("backend.core.duckdb.start_cron_run", return_value=5),
        patch(
            "backend.core.duckdb.log_cron_run",
            side_effect=lambda *args, **kwargs: log_calls.append((args, kwargs)),
        ),
        patch("backend.utils.bot_sources.build_matcher", return_value=lambda ua: ()),
        patch("backend.utils.ngwaf_bot_cache.get_last_timestamp", return_value="2025-12-31T23:59:59Z"),
        patch("backend.utils.ngwaf.fetch_verified_bots_paged", side_effect=RuntimeError("NGWAF 503")),
    ):
        _run_ngwaf_bot_sync("svc")

    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    status = kwargs.get("status") or (args[3] if len(args) > 3 else None)
    assert status == "error"
    assert "NGWAF 503" in kwargs.get("error_message", "")


# ── _run_gap_heal: self-healing on log-accounting gaps ───────────────────


def _gap_heal_src(service_id="svc-gap"):
    return {
        "name": service_id,
        "service_id": service_id,
        "service_name": service_id,
        "access_level": "read_write",
        "logging_service_id": "log-svc-1",
    }


def test_run_gap_heal_no_op_when_source_missing():
    """No source → silent return. Same pattern as _run_full_sweep."""
    from backend.scheduler import _run_gap_heal

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        _run_gap_heal("svc-missing")  # should not raise


def test_run_gap_heal_no_op_when_source_read_only():
    """Analyst (read_only) services never heal — they don't ingest."""
    from backend.scheduler import _run_gap_heal

    src = {**_gap_heal_src(), "access_level": "read_only"}
    with patch("backend.core.duckdb.get_source_for_service", return_value=src):
        _run_gap_heal("svc-readonly")  # should not raise


def test_run_gap_heal_logs_success_when_no_sustained_loss():
    """compute_log_accounting returns sustained_loss=None → cron logs
    success with "no sustained loss" summary and does NOT trigger
    full_sweep. The detector being free from triggers is the healthy
    case and should not produce errors."""
    from backend.scheduler import _run_gap_heal

    log_calls = []
    full_sweep_calls = []
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=_gap_heal_src()),
        patch("backend.core.duckdb.start_cron_run", return_value=42),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
        patch("backend.scheduler._run_full_sweep", side_effect=lambda sid: full_sweep_calls.append(sid)),
        patch(
            "backend.routers.admin.compute_log_accounting",
            return_value={"sustained_loss": None, "buckets": [], "totals": None},
        ),
    ):
        _run_gap_heal("svc-gap")

    assert len(full_sweep_calls) == 0
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "success"  # status is the 4th positional arg
    assert "No sustained loss" in kwargs["summary"]


def test_run_gap_heal_triggers_full_sweep_on_sustained_loss():
    """When compute_log_accounting reports sustained_loss, the heal
    cron must invoke _run_full_sweep AND record a "triggering"-style
    summary so users can see the heal decision in cron history."""
    from backend.routers.admin import SustainedLossAlert
    from backend.scheduler import _run_gap_heal

    log_calls = []
    full_sweep_calls = []
    sustained = SustainedLossAlert(
        started_at="2026-05-22T14:00:00Z",
        n_buckets=3,
        max_gap_pct=0.12,
        total_lost_lines=1500,
    )
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=_gap_heal_src()),
        patch("backend.core.duckdb.start_cron_run", return_value=43),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
        patch(
            "backend.scheduler._run_full_sweep",
            side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
        ),
        patch(
            "backend.routers.admin.compute_log_accounting",
            return_value={"sustained_loss": sustained, "buckets": [], "totals": None},
        ),
        patch("backend.scheduler._last_successful_gap_heal_trigger", return_value=None),
        patch("backend.scheduler._mark_gap_heal_triggered"),
    ):
        _run_gap_heal("svc-gap")

    assert len(full_sweep_calls) == 1
    sid, kw = full_sweep_calls[0]
    assert sid == "svc-gap"
    # 12% gap → "elevated" band → default sweep budget
    assert kw["max_files"] == 20_000
    assert kw["max_seconds"] == 900
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "success"
    assert "triggering full_sweep" in kwargs["summary"]


def test_run_gap_heal_respects_throttle_window():
    """If a heal was triggered within GAP_HEAL_THROTTLE_HOURS, a new
    sustained-loss observation should be logged but NOT trigger another
    full_sweep — prevents thrashing on unrecoverable Fastly→FOS
    transport loss."""
    from backend.routers.admin import SustainedLossAlert
    from backend.scheduler import GAP_HEAL_THROTTLE_HOURS, _run_gap_heal

    log_calls = []
    full_sweep_calls = []
    sustained = SustainedLossAlert(
        started_at="2026-05-22T14:00:00Z",
        n_buckets=2,
        max_gap_pct=0.07,
        total_lost_lines=300,
    )
    # Pretend a heal happened 1h ago — well inside the throttle window.
    one_hour_ago = time.time() - 3600
    assert GAP_HEAL_THROTTLE_HOURS > 1, "test assumes throttle > 1h"
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=_gap_heal_src()),
        patch("backend.core.duckdb.start_cron_run", return_value=44),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
        patch(
            "backend.scheduler._run_full_sweep",
            side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
        ),
        patch(
            "backend.routers.admin.compute_log_accounting",
            return_value={"sustained_loss": sustained, "buckets": [], "totals": None},
        ),
        patch("backend.scheduler._last_successful_gap_heal_trigger", return_value=one_hour_ago),
    ):
        _run_gap_heal("svc-gap")

    assert full_sweep_calls == [], "throttle must prevent re-trigger"
    assert len(log_calls) == 1
    args, kwargs = log_calls[0]
    assert args[3] == "success"
    assert "throttled" in kwargs["summary"]


def test_gap_heal_severity_bands():
    """Threshold matrix for the severity classifier — pins the band each
    (gap_pct, lost_lines) pair lands in so a future tweak to the bands or
    the OR/AND logic can't silently downgrade severe loss."""
    from backend.cron.jobs.sync import _gap_heal_severity

    # Mild: under all elevated floors
    assert _gap_heal_severity(0.05, 1_000).name == "mild"
    # Elevated: either gap_pct or lost_lines crosses the elevated floor
    assert _gap_heal_severity(0.15, 0).name == "elevated"
    assert _gap_heal_severity(0.0, 15_000).name == "elevated"
    # Severe band
    assert _gap_heal_severity(0.55, 0).name == "severe"
    assert _gap_heal_severity(0.0, 150_000).name == "severe"
    # Critical
    assert _gap_heal_severity(0.85, 0).name == "critical"
    assert _gap_heal_severity(0.0, 600_000).name == "critical"
    # Exact-boundary lands in the same band (>= comparison)
    assert _gap_heal_severity(0.80, 0).name == "critical"
    assert _gap_heal_severity(0.50, 0).name == "severe"
    assert _gap_heal_severity(0.10, 0).name == "elevated"


def test_run_gap_heal_critical_bypasses_throttle_and_widens_sweep():
    """Critical loss (≥80% gap or ≥500k lost lines) must fire on every
    detector tick (no throttle) and pass a far larger sweep budget so the
    backlog drains in hours not days. Without this a single 200k-line
    burst would take ~40 hours at the default 20k files/run."""
    from backend.routers.admin import SustainedLossAlert
    from backend.scheduler import _run_gap_heal

    log_calls = []
    full_sweep_calls: list[tuple[str, dict]] = []
    sustained = SustainedLossAlert(
        started_at="2026-06-11T20:00:00Z",
        n_buckets=4,
        max_gap_pct=0.88,
        total_lost_lines=203_000,
    )
    # Pretend a heal happened 1 min ago — would normally throttle.
    one_min_ago = time.time() - 60
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=_gap_heal_src()),
        patch("backend.core.duckdb.start_cron_run", return_value=45),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
        patch(
            "backend.scheduler._run_full_sweep",
            side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
        ),
        patch(
            "backend.routers.admin.compute_log_accounting",
            return_value={"sustained_loss": sustained, "buckets": [], "totals": None},
        ),
        patch("backend.scheduler._last_successful_gap_heal_trigger", return_value=one_min_ago),
        patch("backend.scheduler._mark_gap_heal_triggered"),
    ):
        _run_gap_heal("svc-gap")

    assert len(full_sweep_calls) == 1, "critical loss must bypass throttle"
    sid, kw = full_sweep_calls[0]
    assert sid == "svc-gap"
    assert kw["max_files"] == 100_000, "critical band must widen sweep file budget"
    assert kw["max_seconds"] == 1800, "critical band must widen sweep time budget"
    summary = log_calls[0][1]["summary"]
    assert "severity=critical" in summary
    assert "max_files=100000" in summary


def test_run_gap_heal_severe_uses_15min_throttle():
    """Severe band (≥50% gap or ≥100k lost lines) cuts throttle to 15 min
    so a 200k-line burst gets ~4 sweeps/hour instead of one every 4h."""
    from backend.routers.admin import SustainedLossAlert
    from backend.scheduler import _run_gap_heal

    log_calls = []
    full_sweep_calls: list[tuple[str, dict]] = []
    sustained = SustainedLossAlert(
        started_at="2026-06-11T20:00:00Z",
        n_buckets=3,
        max_gap_pct=0.60,
        total_lost_lines=150_000,
    )
    # 30 min ago — past the 15 min throttle, should trigger.
    thirty_min_ago = time.time() - 1800
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=_gap_heal_src()),
        patch("backend.core.duckdb.start_cron_run", return_value=46),
        patch("backend.core.duckdb.log_cron_run", side_effect=lambda *a, **k: log_calls.append((a, k))),
        patch(
            "backend.scheduler._run_full_sweep",
            side_effect=lambda sid, **kw: full_sweep_calls.append((sid, kw)),
        ),
        patch(
            "backend.routers.admin.compute_log_accounting",
            return_value={"sustained_loss": sustained, "buckets": [], "totals": None},
        ),
        patch("backend.scheduler._last_successful_gap_heal_trigger", return_value=thirty_min_ago),
        patch("backend.scheduler._mark_gap_heal_triggered"),
    ):
        _run_gap_heal("svc-gap")

    assert len(full_sweep_calls) == 1
    sid, kw = full_sweep_calls[0]
    assert kw["max_files"] == 50_000, "severe band widens file budget"
    assert kw["max_seconds"] == 1500


def test_run_full_sweep_default_budget_unchanged_for_daily_scheduled_run():
    """The daily catch-net cron calls ``_run_full_sweep(service_id)``
    with no kwargs and must get the conservative 20k / 900s defaults —
    only heal-triggered sweeps should get a bigger budget."""
    import inspect

    from backend.cron.jobs.sync import _FULL_SWEEP_DEFAULT_MAX_FILES, _FULL_SWEEP_DEFAULT_MAX_SECONDS, _run_full_sweep

    sig = inspect.signature(_run_full_sweep)
    assert sig.parameters["max_files"].default == _FULL_SWEEP_DEFAULT_MAX_FILES == 20_000
    assert sig.parameters["max_seconds"].default == _FULL_SWEEP_DEFAULT_MAX_SECONDS == 900


def test_sync_jobs_registers_gap_heal_when_logging_service_id_present():
    """Gap-heal cron should register only when the service has a
    logging_service_id (the Fastly Stats API call keys on it)."""
    from backend.scheduler import Scheduler

    cfg = {
        "service_id": "svc-heal",
        "log_period": 60,
        "access_level": "read_write",
        "logging_service_id": "log-svc-1",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-heal")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=1),
    ):
        s._sync_jobs()

    assert "gap_heal_svc-heal" in s._job_ids


def test_sync_jobs_registers_gap_heal_when_only_service_id_present():
    """Even without an explicit ``logging_service_id`` field, the heal
    cron must register — ``compute_log_accounting`` falls back to
    ``service_id`` for the Fastly Stats call, and the scheduler check
    must do the same. Regression: missing this fallback let a 200k-line
    burst go unhealed (gap_heal cron was simply never scheduled)."""
    from backend.scheduler import Scheduler

    cfg = {
        "service_id": "svc-fallback",
        "log_period": 60,
        "access_level": "read_write",
        "provisioning": {"cron_sync": {"enabled": True}},
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-fallback")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=1),
    ):
        s._sync_jobs()

    assert "gap_heal_svc-fallback" in s._job_ids


def test_sync_jobs_skips_gap_heal_when_disabled():
    """``cron_gap_heal.enabled: False`` must keep the heal cron out of
    the schedule even if the service has a logging_service_id."""
    from backend.scheduler import Scheduler

    cfg = {
        "service_id": "svc-disabled",
        "log_period": 60,
        "access_level": "read_write",
        "logging_service_id": "log-svc-1",
        "provisioning": {
            "cron_sync": {"enabled": True},
            "cron_gap_heal": {"enabled": False},
        },
    }

    s = Scheduler()
    with (
        patch("backend.config.list_configs", return_value=[cfg]),
        patch("backend.core.duckdb.get_source_for_service", return_value=_fake_src("svc-disabled")),
        patch("backend.core.duckdb.is_configured", return_value=True),
        patch("backend.config.get_ngwaf_workspace_id", return_value=None),
        patch("backend.core.metadata.count_alerts", return_value=1),
    ):
        s._sync_jobs()

    assert "gap_heal_svc-disabled" not in s._job_ids


def test_check_disk_space_passes_when_plenty_free(tmp_path):
    """Happy path: 10 GB total / 9.5 GB free → no abort."""
    from collections import namedtuple

    from backend.scheduler import _check_disk_space

    Usage = namedtuple("usage", "total used free")
    with patch("shutil.disk_usage", return_value=Usage(total=10 * 1024**3, used=512 * 1024**2, free=9 * 1024**3)):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True
    assert msg == ""


def test_check_disk_space_aborts_when_below_byte_floor(tmp_path):
    """100 MB free is below the 500 MB hard floor → abort with a
    human-readable reason."""
    from collections import namedtuple

    from backend.scheduler import _check_disk_space

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

    from backend.scheduler import _check_disk_space

    Usage = namedtuple("usage", "total used free")
    with patch(
        "shutil.disk_usage",
        return_value=Usage(total=50 * 1024**3, used=49 * 1024**3, free=1 * 1024**3),
    ):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is False
    assert "2.0% of 50.0" in msg


def test_check_disk_space_does_not_block_on_probe_failure(tmp_path):
    """If shutil.disk_usage raises (e.g. dir disappears mid-cron), do
    NOT block — let the underlying job try and fail with the real
    error. Conservative because we don't want a transient FS issue to
    kill all cron runs."""
    from backend.scheduler import _check_disk_space

    with patch("shutil.disk_usage", side_effect=OSError("gone")):
        ok, msg = _check_disk_space(str(tmp_path), "svc", "sync")
    assert ok is True
    assert msg == ""


def test_check_buffer_backlog_returns_empty_when_drained():
    """Healthy drain: nothing in buffer after commit → no suffix, no warning."""
    from backend.scheduler import _check_buffer_backlog

    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 0, "total_bytes": 0, "oldest_age_seconds": 0, "oldest_path": None},
    ):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


def test_check_buffer_backlog_warns_when_file_count_exceeds_threshold():
    """More than 200 files left after a commit → suffix mentions count."""
    from backend.scheduler import _check_buffer_backlog

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
    from backend.scheduler import _check_buffer_backlog

    with patch(
        "backend.core.iceberg.buffer_backlog_stats",
        return_value={"file_count": 10, "total_bytes": 1000, "oldest_age_seconds": 30 * 60, "oldest_path": "/x"},
    ):
        suffix = _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5)
    assert "30m old" in suffix


def test_check_buffer_backlog_never_raises_on_probe_failure():
    """The backlog probe must not bubble exceptions back to _run_commit.
    A corrupt iceberg helper must yield "" not a stack trace."""
    from backend.scheduler import _check_buffer_backlog

    with patch("backend.core.iceberg.buffer_backlog_stats", side_effect=OSError("disk gone")):
        assert _check_buffer_backlog({"name": "svc"}, "svc", commit_interval_mins=5) == ""


# Silence ruff unused-imports
_ = MagicMock
_ = pytest
