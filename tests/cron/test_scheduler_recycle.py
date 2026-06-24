"""The DuckDB recycle job is registered only when DUCKDB_RECYCLE_INTERVAL_MIN>0."""

from unittest.mock import MagicMock

from backend.cron.scheduler import Scheduler


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

    # And it must be the singleton, coalescing flavour.
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


def test_recycle_job_not_registered_when_interval_zero(monkeypatch):
    ids = _registered_job_ids(monkeypatch, "0")
    assert "duckdb_recycle" not in ids
