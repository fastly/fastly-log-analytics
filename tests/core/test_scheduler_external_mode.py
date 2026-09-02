"""External (RedBeat/Celery) scheduler mode: _add_job translation rules.

Pins three v3.0.0 regressions:
- celery's crontab defaults minute='*' (APScheduler defaults minute=0), so a
  daily hour=2 job silently became a 60-runs-per-hour job.
- scheduling a function with no registered Celery task made beat fire
  KeyError forever; _add_job must refuse instead.
- RedBeat entry.save() upserts, so re-adding an existing job updates its
  schedule (interval changes take effect on reload).
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def external_scheduler(monkeypatch):
    from backend.cron.scheduler import Scheduler

    sched = Scheduler.__new__(Scheduler)
    sched.mode = "external"
    sched._job_ids = {}
    return sched


def _fake_task(name="backend.cron.jobs.fake._run_fake_celery"):
    task = MagicMock()
    task.name = name

    def fn(service_id):
        return None

    fn.celery_task = task
    return fn


def test_cron_trigger_defaults_minute_to_zero_when_hour_given(external_scheduler):
    captured = {}

    class FakeEntry:
        def __init__(self, job_id, task_name, schedule, args=None, app=None):
            captured["schedule"] = schedule

        def save(self):
            pass

    with patch("redbeat.RedBeatSchedulerEntry", FakeEntry):
        external_scheduler._add_job(_fake_task(), "cron", hour=2, id="full_sync_svc1", args=["svc"])

    sched = captured["schedule"]
    # celery crontab stores minute as a set of ints; hour=2 minute must be {0}
    assert sched._orig_minute == 0 or sched.minute == {0}, (
        f"daily job scheduled with minute={getattr(sched, '_orig_minute', sched.minute)!r} — "
        "would fire 60x per hour under celery's minute='*' default"
    )
    assert sched.hour == {2}


def test_cron_trigger_without_hour_keeps_wildcard_minute(external_scheduler):
    captured = {}

    class FakeEntry:
        def __init__(self, job_id, task_name, schedule, args=None, app=None):
            captured["schedule"] = schedule

        def save(self):
            pass

    with patch("redbeat.RedBeatSchedulerEntry", FakeEntry):
        external_scheduler._add_job(_fake_task(), "cron", minute=5, id="full_sync_svc2", args=["svc"])

    assert captured["schedule"].minute == {5}


def test_refuses_function_without_celery_task(external_scheduler, caplog):
    def undecorated(service_id):
        return None

    with patch("redbeat.RedBeatSchedulerEntry") as entry_cls:
        external_scheduler._add_job(undecorated, "interval", seconds=30, id="gap_heal_svc1", args=["svc"])

    entry_cls.assert_not_called()
    assert "gap_heal_svc1" not in external_scheduler._job_ids
    assert any("NOT scheduled" in r.message for r in caplog.records)


def test_add_job_upserts_existing_entry(external_scheduler):
    saves = []

    class FakeEntry:
        def __init__(self, job_id, task_name, schedule, args=None, app=None):
            self.schedule = schedule

        def save(self):
            saves.append(self.schedule.run_every.total_seconds())

    with patch("redbeat.RedBeatSchedulerEntry", FakeEntry):
        external_scheduler._add_job(_fake_task(), "interval", seconds=30, id="log_discovery_svc1", args=["svc"])
        external_scheduler._add_job(_fake_task(), "interval", seconds=120, id="log_discovery_svc1", args=["svc"])

    assert saves == [30.0, 120.0]


def test_external_mode_routes_pod_local_jobs_to_apscheduler(external_scheduler):
    """The backend-local/worker split: only the ledger/FOS job family goes to
    RedBeat; everything else (rollups, compaction, alerts, snapshots — jobs
    that open the pod-local .duckdb) stays on this process's APScheduler,
    even in external mode. A pod-local job on a worker either fights the
    backend's readers for the single-writer file lock or samples the wrong
    process (metric_snapshot)."""
    from unittest.mock import MagicMock

    external_scheduler._sched = MagicMock()
    with patch("redbeat.RedBeatSchedulerEntry") as entry_cls:
        external_scheduler._add_job(_fake_task(), "interval", seconds=120, id="local_compact_svc1", args=["svc"])
        external_scheduler._add_job(_fake_task(), "interval", seconds=60, id="metric_snapshot", args=[])

    entry_cls.assert_not_called()
    assert external_scheduler._sched.add_job.call_count == 2
    assert external_scheduler._job_ids["local_compact_svc1"] == "local_compact_svc1"

    # And the ledger family still goes to RedBeat, not APScheduler.
    external_scheduler._sched.reset_mock()
    with patch("redbeat.RedBeatSchedulerEntry") as entry_cls:
        external_scheduler._add_job(_fake_task(), "interval", seconds=60, id="log_discovery_svc1", args=["svc"])
    entry_cls.assert_called_once()
    external_scheduler._sched.add_job.assert_not_called()
