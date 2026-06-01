"""Tests for backend.utils.system_jobs.

In-memory store for global (non-service) scheduler job statuses. The admin
UI's system-jobs panel reads from here. The store is global module state,
so we reset it per-test to keep the suite deterministic.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from backend.utils import system_jobs


def _reset() -> None:
    with system_jobs._lock:
        system_jobs._status.clear()


def setup_function(_fn) -> None:
    _reset()


def test_initial_status_is_empty():
    assert system_jobs.get_system_job_status() == {}


def test_record_job_run_writes_snapshot():
    system_jobs.record_job_run("rdns", status="success", duration_s=1.234, detail="processed 12")
    snap = system_jobs.get_system_job_status()
    assert "rdns" in snap
    entry = snap["rdns"]
    assert entry["status"] == "success"
    assert entry["duration_s"] == 1.23  # rounded to 2dp
    assert entry["detail"] == "processed 12"
    assert entry["last_run_at"].endswith("Z")


def test_record_job_run_overwrites_previous():
    system_jobs.record_job_run("rdns", "success", 1.0)
    system_jobs.record_job_run("rdns", "error", 2.0, detail="boom")
    snap = system_jobs.get_system_job_status()
    assert snap["rdns"]["status"] == "error"
    assert snap["rdns"]["duration_s"] == 2.0
    assert snap["rdns"]["detail"] == "boom"


def test_get_returns_a_copy_not_live_state():
    """The returned dict must not let callers mutate the internal store."""
    system_jobs.record_job_run("rdns", "success", 0.5)
    snap = system_jobs.get_system_job_status()
    snap.pop("rdns")
    # Original store still has it
    assert "rdns" in system_jobs.get_system_job_status()


def test_last_run_at_format():
    fixed = datetime(2026, 5, 15, 12, 30, 45, tzinfo=UTC)
    with patch("backend.utils.system_jobs.datetime") as m:
        m.now.return_value = fixed
        system_jobs.record_job_run("bots", "success", 0.1)
    assert system_jobs.get_system_job_status()["bots"]["last_run_at"] == "2026-05-15T12:30:45Z"
