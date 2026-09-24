"""start_cron_run's lease acquisition must be atomic under concurrent
callers — not just correct for a single caller.

The pre-fix implementation was check-then-insert (SELECT count, then
INSERT), which only stayed race-free because a single SQLite writer
connection serializes both statements inside one transaction. Under a
Postgres metadata backend (autocommit=True, no implicit cross-statement
transaction), two pods racing the same (service_id, task) could both pass
the busy check and both insert a 'running' lease. The fix makes lease
acquisition one atomic statement — INSERT ... ON CONFLICT (service_id,
job_name) WHERE status='running' DO NOTHING — using the partial unique
index idx_job_runs_running_lease (migration 019 / base.py _SCHEMA), and
reads rowcount to learn whether it won.

These tests run against a real Postgres metadata backend (the
isolate_metadata_db fixture is autouse), scoped to this xdist worker's own
schema. They drive genuinely concurrent threads at the same lease to prove
the unique index — not application-level locking — is what enforces mutual
exclusion.
"""

from __future__ import annotations

import threading

import pytest

from backend.core.metadata.base import get_con
from backend.core.metadata.cron_log import start_cron_run


def test_start_cron_run_second_call_raises_already_running():
    sid = "svc-lease-basic"
    run_id = start_cron_run(sid, "log_discovery")
    assert run_id > 0

    with pytest.raises(RuntimeError, match="already running"):
        start_cron_run(sid, "log_discovery")


def test_start_cron_run_different_task_same_service_both_succeed():
    sid = "svc-lease-different-task"
    a = start_cron_run(sid, "log_discovery")
    b = start_cron_run(sid, "commit")
    assert a > 0 and b > 0 and a != b


def test_start_cron_run_different_service_same_task_both_succeed():
    a = start_cron_run("svc-lease-a", "log_discovery")
    b = start_cron_run("svc-lease-b", "log_discovery")
    assert a > 0 and b > 0


def test_lease_index_exists_and_is_unique_partial():
    """Pin the schema contract the atomic INSERT depends on — if a future
    schema edit drops or weakens this index, the ON CONFLICT target in
    start_cron_run silently stops matching and every call falls through to
    a plain INSERT (no mutual exclusion, no error).

    Queries ``pg_indexes`` (schema-qualified by ``current_schema()`` — this
    worker's isolated Postgres schema, see ``tests/conftest.py``'s
    ``_pg_worker_schema``) rather than SQLite's ``sqlite_master``.
    """
    con = get_con("svc-lease-index-check")
    row = con.execute(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
        "AND indexname = 'idx_job_runs_running_lease'"
    ).fetchone()
    assert row is not None, "idx_job_runs_running_lease is missing"
    indexdef = row["indexdef"] if hasattr(row, "keys") else row[0]
    assert "UNIQUE" in indexdef.upper()
    assert "STATUS" in indexdef.upper() and "RUNNING" in indexdef.upper()


def test_start_cron_run_exactly_one_of_two_concurrent_callers_wins():
    """The actual race test: fire two threads at start_cron_run for the
    SAME (service_id, task) as close to simultaneously as possible. Before
    this fix, both could observe 'not busy' between the check and the
    insert; the unique-index-backed atomic insert must let exactly one
    through regardless of thread interleaving."""
    sid = "svc-lease-race"
    task = "log_discovery"
    results: list[tuple[bool, str]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def _attempt():
        try:
            barrier.wait(timeout=5)
        except threading.BrokenBarrierError:
            pass
        try:
            run_id = start_cron_run(sid, task)
            with lock:
                results.append((True, str(run_id)))
        except RuntimeError as e:
            with lock:
                results.append((False, str(e)))

    threads = [threading.Thread(target=_attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(results) == 2
    wins = [r for r in results if r[0]]
    losses = [r for r in results if not r[0]]
    assert len(wins) == 1, f"expected exactly 1 winner, got {results}"
    assert len(losses) == 1
    assert "already running" in losses[0][1]
