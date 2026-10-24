"""Scheduler._reschedule_or_add_job: the shared reschedule-if-tracked-else-add step."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
