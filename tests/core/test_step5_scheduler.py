import time

import pytest

from backend.core.metadata.base import get_con
from backend.core.metadata.cron_log import cron_busy, start_cron_run


def test_orphaned_row_stalls_ingestion_healed_automatically(monkeypatch):
    service_id = "test-scheduler-svc"
    task = "sync"

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM cron_runs WHERE service_id=?", (service_id,))
    cur.execute("DELETE FROM job_runs WHERE service_id=?", (service_id,))
    con.commit()

    # Simulate an orphaned row from an old crash
    # 1. An old job_runs lease that is expired
    now = time.time()
    old_time = now - 1000  # 1000 seconds ago

    cur.execute(
        "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) VALUES (?, ?, ?, ?, 60, 'running')",
        (service_id, task, old_time, old_time),
    )
    # 2. An old cron_runs row stuck in 'running'
    cur.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status) VALUES (?, ?, ?, 0, 'running')",
        (service_id, task, "2026-08-27 10:00:00Z"),
    )
    con.commit()

    # Verify that it is NOT busy because the heartbeat expired
    assert not cron_busy(service_id)

    # Start a new cron run. It should automatically heal the orphaned row
    new_run_id = start_cron_run(service_id, task)

    # Check that the old cron_runs row was marked as error
    cur.execute(
        "SELECT status, error_message FROM cron_runs WHERE service_id=? AND task=? AND id != ?",
        (service_id, task, new_run_id),
    )
    old_cron = cur.fetchone()
    assert old_cron["status"] == "error"
    assert "Process interrupted" in old_cron["error_message"]

    # Check that the old job_runs row was marked as error
    cur.execute(
        "SELECT status FROM job_runs WHERE service_id=? AND job_name=? AND heartbeat_at = ?",
        (service_id, task, old_time),
    )
    old_job = cur.fetchone()
    assert old_job["status"] == "reaped"

    # We should have a new active job_runs lease
    cur.execute(
        "SELECT status FROM job_runs WHERE service_id=? AND job_name=? AND status='running'", (service_id, task)
    )
    active_jobs = cur.fetchall()
    assert len(active_jobs) == 1


def test_concurrent_run_blocked():
    service_id = "test-scheduler-svc"
    task = "sync"

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM cron_runs WHERE service_id=?", (service_id,))
    cur.execute("DELETE FROM job_runs WHERE service_id=?", (service_id,))
    con.commit()

    start_cron_run(service_id, task)
    assert cron_busy(service_id)

    with pytest.raises(RuntimeError, match="already running"):
        start_cron_run(service_id, task)
