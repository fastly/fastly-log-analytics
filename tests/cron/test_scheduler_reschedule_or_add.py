"""Scheduler._reschedule_or_add_job: the shared reschedule-if-tracked-else-add step."""

from __future__ import annotations

from unittest.mock import MagicMock

from backend.cron.scheduler import Scheduler


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
