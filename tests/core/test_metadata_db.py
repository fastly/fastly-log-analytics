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

import sqlite3
import stat
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend import config
from backend.core import metadata as metadata_db
from backend.core import quarantine
from backend.core import share_db as share_db_connection
from backend.core.metadata import base, ingest_log, pg_connection, pg_schema, state, usage_log, usage_log_db
from backend.core.metadata.base import InvalidServiceIdError, get_con
from backend.core.metadata.cron_log import start_cron_run
from backend.core.metadata.quarantine import (
    delete_quarantine_evidence,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    get_quarantine_storage_total,
    list_quarantine_evidence,
)


# ── Merged from tests/core/test_metadata_db.py ──
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


def _con(service_id: str):
    return metadata_db.get_con(service_id)


def test_merge_audit_noop_when_empty():
    state.merge_audit_for_service("svc-merge-empty", [])
    # Sanity: didn't touch the DB at all (audit_logs still empty).
    con = _con("svc-merge-empty")
    rows = con.execute("SELECT count(*) FROM audit_logs").fetchone()
    assert rows[0] == 0


def test_merge_audit_inserts_new_rows():
    sid = "svc-merge-new"
    rows = [
        {
            "timestamp": "2026-06-12T10:00:00Z",
            "source_name": "fos",
            "event_type": "ingest_start",
            "details": "{}",
            "actor": "cron",
        },
        {
            "timestamp": "2026-06-12T10:01:00Z",
            "source_name": "fos",
            "event_type": "ingest_done",
            "details": "{}",
            "actor": "cron",
        },
    ]
    state.merge_audit_for_service(sid, rows)
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 2


def test_merge_audit_dedups_on_composite_key():
    """If the (timestamp, source_name, event_type, actor) tuple already
    exists in the local DB, the merge skips that row rather than creating
    a duplicate."""
    sid = "svc-merge-dedup"
    row = {
        "timestamp": "2026-06-12T10:00:00Z",
        "source_name": "fos",
        "event_type": "ingest_start",
        "details": "{}",
        "actor": "cron",
    }
    state.merge_audit_for_service(sid, [row])
    state.merge_audit_for_service(sid, [row])  # second call → dedup, skip
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 1


def test_merge_audit_distinguishes_by_actor():
    """Same timestamp + source_name + event_type but different actor →
    treated as a distinct event and inserted (the local analyst and the
    cron writer can both stamp ``ingest_start`` at the same second
    legitimately)."""
    sid = "svc-merge-actor"
    base = {
        "timestamp": "2026-06-12T10:00:00Z",
        "source_name": "fos",
        "event_type": "ingest_start",
        "details": "{}",
    }
    state.merge_audit_for_service(sid, [{**base, "actor": "cron"}, {**base, "actor": "analyst"}])
    con = _con(sid)
    n = con.execute("SELECT count(*) FROM audit_logs").fetchone()[0]
    assert n == 2


# ── Merged from tests/core/test_metadata_db_audit.py ──
SVC = "test-audit-svc"


def test_record_then_list_returns_the_row():
    """The basic happy path: append one entry, read it back. Fields
    must round-trip verbatim and the details dict must come back
    parsed (JSON-string in storage, dict on the read side)."""
    metadata_db.record_scoring_audit(
        SVC,
        "scoring_enabled",
        details={"matrix_version": "v1", "scoring_service_id": "scorer-1"},
    )

    rows = metadata_db.list_scoring_audit(SVC)
    assert len(rows) == 1
    row = rows[0]
    assert row["action"] == "scoring_enabled"
    assert row["actor"] == "operator"  # default actor
    assert row["details"] == {"matrix_version": "v1", "scoring_service_id": "scorer-1"}
    assert "timestamp" in row
    assert "id" in row


def test_record_multiple_rows_returns_newest_first():
    """Multiple sequential appends → DESC ordering. We rely on the
    id-DESC tiebreaker rather than timestamp-DESC alone because
    sqlite's ``datetime('now')`` resolves to whole seconds and the
    autoincrement id is the only monotonic guarantee within a second."""
    metadata_db.record_scoring_audit(SVC, "scoring_enabled")
    metadata_db.record_scoring_audit(SVC, "threshold_committed", details={"new_threshold": 80})
    metadata_db.record_scoring_audit(SVC, "matrix_retrained", details={"matrix_version": "v2"})

    rows = metadata_db.list_scoring_audit(SVC)
    actions = [r["action"] for r in rows]
    assert actions == ["matrix_retrained", "threshold_committed", "scoring_enabled"]


def test_list_limit_truncates_to_n_rows():
    """``limit`` is enforced server-side. Insert 5, ask for 2, get the
    2 newest."""
    for i in range(5):
        metadata_db.record_scoring_audit(SVC, f"action_{i}", details={"i": i})
    rows = metadata_db.list_scoring_audit(SVC, limit=2)
    assert len(rows) == 2
    # Newest first: action_4 then action_3
    assert rows[0]["action"] == "action_4"
    assert rows[1]["action"] == "action_3"


def test_list_since_filter_excludes_older_rows():
    """``since`` is an ISO timestamp lower bound. Insert a row, capture
    a cut-off timestamp, and insert another. The first row must be filtered out."""
    metadata_db.record_scoring_audit(SVC, "older_action")
    con = metadata_db.get_con(SVC)
    con.execute("UPDATE scoring_audit SET timestamp = datetime('now', '-10 seconds') WHERE action = 'older_action'")
    con.commit()
    # ``scoring_audit.timestamp`` is TEXT (an ISO-like string on both
    # backends — see ``list_scoring_audit``'s ``timestamp >= ?`` string
    # comparison). Under Postgres, a bare ``SELECT datetime(...)``
    # expression round-trips as a real ``datetime.datetime`` object
    # (psycopg auto-converts), not a string — comparing that object
    # against the TEXT column raises ``UndefinedFunction: operator does
    # not exist: text >= timestamp`` (silently swallowed by
    # ``list_scoring_audit``'s best-effort except, which is what actually
    # produced the empty-list failure here rather than a traceback).
    # ``str()`` normalizes it to the same space-separated ISO format
    # Postgres already used to populate the column via assignment cast,
    # so the text comparison is well-formed under both backends.
    cutoff = str(con.execute("SELECT datetime('now', '-5 seconds') AS now").fetchone()["now"])
    metadata_db.record_scoring_audit(SVC, "newer_action")

    rows = metadata_db.list_scoring_audit(SVC, since=cutoff)
    actions = [r["action"] for r in rows]
    assert actions == ["newer_action"]
    assert "older_action" not in actions


def test_list_since_filter_with_future_timestamp_returns_empty():
    """A ``since`` far in the future → no rows match → empty list (NOT
    an error). The admin UI polls with ``since=<last_seen_ts>`` and
    expects an empty array when nothing new has happened."""
    metadata_db.record_scoring_audit(SVC, "anything")
    rows = metadata_db.list_scoring_audit(SVC, since="2099-01-01T00:00:00")
    assert rows == []


def test_record_swallows_malformed_service_id():
    """``record_scoring_audit`` is best-effort by design — every
    SQLite failure path is logged at DEBUG and swallowed so the
    operator hot-path (e.g. /scoring/threshold PUT) doesn't 500
    because the audit append racing-conditioned a WAL writer.

    ``service_id=None`` hits the ``db_path`` TypeError guard, which
    bubbles up as TypeError (not sqlite3.Error). Since the function
    only catches ``sqlite3.Error``, this would raise — pin the
    contract so a future refactor that broadens the except clause
    keeps the swallow semantics consistent.

    Today: passing None RAISES TypeError. The test documents that —
    if/when the function broadens to ``except Exception``, update the
    assertion to ``records_no_exception`` and the production code
    aligns with the docstring."""
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(None, "scoring_enabled")  # type: ignore[arg-type]


def test_record_handles_unjsonable_details_gracefully():
    """``json.dumps`` will raise on non-serializable details (e.g. a
    raw set, or a datetime without isoformat). Today the implementation
    catches only ``sqlite3.Error`` so a TypeError from json.dumps
    propagates — pin this so callers don't accidentally pass an
    un-jsonable dict and silently drop the audit entry without
    knowing."""
    with pytest.raises(TypeError):
        metadata_db.record_scoring_audit(
            SVC,
            "weird_action",
            details={"a_set": {1, 2, 3}},  # type: ignore[dict-item]
        )


def test_list_returns_empty_on_brand_new_service():
    """A service whose metadata DB has never seen a scoring_audit
    insert → empty list (NOT an error). The first call to ``get_con``
    creates the table via _init_schema, so the SELECT returns 0 rows."""
    rows = metadata_db.list_scoring_audit("freshly-minted-svc")
    assert rows == []


# ── Merged from tests/core/test_metadata_db_concurrency.py ──
def test_concurrent_inserts_no_lock_no_loss():
    """8 threads each insert 50 distinct files for the same service.

    All 400 rows must land in the table; no thread may raise
    ``OperationalError: database is locked``.
    """
    sid = "svc-concurrent"
    n_threads = 8
    rows_per_thread = 50

    # Warm the DB so the init lock isn't serialising all 8 cold-start threads
    # (under CI load, 8 sequential connect+PRAGMA+schema > 10s timeout).
    metadata_db.get_con(sid)

    def worker(thread_id: int) -> int:
        rows = [(f"thread-{thread_id}/file-{i}.gz", 100, 4096) for i in range(rows_per_thread)]
        # If WAL is dropped or the connection pool serialises poorly, this
        # raises sqlite3.OperationalError("database is locked"). The test
        # propagates the exception so failure is loud.
        metadata_db.insert_ingested_files(sid, rows)
        return rows_per_thread

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        totals = list(ex.map(worker, range(n_threads)))

    assert sum(totals) == n_threads * rows_per_thread

    # Every distinct file must be present. The unique constraint on
    # (file_name, source_name) means a missed insert == data loss.
    con = metadata_db.get_con(sid)
    count = con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert count == n_threads * rows_per_thread, (
        f"expected {n_threads * rows_per_thread} rows, found {count} — possible lost write"
    )


def test_concurrent_inserts_with_overlap_dedup_via_upsert():
    """Same file inserted by multiple threads must dedup via the upsert clause.

    Bytes/row_count get updated to the last writer's value (which is fine —
    the production code only inserts after a successful ingest, and the
    last-writer-wins value is the correct truth). What matters: no row
    duplication, no constraint violation, no lock error.
    """
    sid = "svc-concurrent-overlap"
    n_threads = 6
    files = [f"shared/f-{i}.gz" for i in range(20)]

    def worker(thread_id: int) -> None:
        # Every thread inserts the SAME 20 files
        rows = [(fname, 100 + thread_id, 1000 + thread_id) for fname in files]
        metadata_db.insert_ingested_files(sid, rows)

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        list(ex.map(worker, range(n_threads)))

    con = metadata_db.get_con(sid)
    count = con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert count == len(files), f"expected dedup to {len(files)} rows, found {count} — upsert clause regressed"


def test_teardown_then_get_con_recreates_schema():
    """``teardown()`` used to remove the per-service SQLite file (and
    WAL/SHM/journal); the next ``get_con`` would lazily re-create it, and
    the pre-teardown row was gone.

    Under the shared Postgres metadata store there is no per-service file to
    delete — every service's rows live in the SAME tables — so
    ``teardown()`` is a no-op there (see its docstring in
    ``backend/core/metadata/base.py``, and this task's report for why a new
    destructive row-deletion semantic was NOT invented here without an
    explicit product decision to do so). Pin the CURRENT contract instead:
    ``teardown()`` doesn't raise, doesn't delete this service's rows, and
    ``get_con``/inserts keep working normally afterwards.
    """
    sid = "svc-teardown-recreate"

    metadata_db.insert_ingested_files(sid, [("seed.gz", 1, 100)])

    metadata_db.teardown(sid)  # must not raise

    metadata_db.insert_ingested_files(sid, [("post-teardown.gz", 2, 200)])
    con = metadata_db.get_con(sid)
    rows = con.execute("SELECT file_name FROM ingested_files WHERE source_name = ?", (sid,)).fetchall()
    names = {r[0] for r in rows}
    assert "post-teardown.gz" in names
    # No per-service file to delete under Postgres — the pre-teardown row
    # is expected to survive (unlike the old SQLite-file-delete contract).
    assert "seed.gz" in names


def test_concurrent_teardown_and_writes_no_lost_post_teardown_data():
    """If thread A calls teardown(sid) while thread B is in the middle of
    inserting, the worst acceptable outcome is: B's in-flight write may be
    lost (the file got deleted under it), BUT a subsequent get_con+insert
    by *any* thread must succeed and persist correctly. No deadlock, no
    persistent broken state.

    Coordinated with a lock to avoid hard C-level sqlite3 segfaults under
    Linux xdist when physical WAL/SHM file unlinks hit exactly mid-write.
    """
    import threading

    sid = "svc-teardown-race"
    lock = threading.Lock()

    # Seed once so the file exists
    metadata_db.insert_ingested_files(sid, [("warmup.gz", 1, 100)])

    def writer() -> None:
        with lock:
            try:
                metadata_db.insert_ingested_files(sid, [(f"writer-{i}.gz", 1, 100) for i in range(20)])
            except Exception:
                # Acceptable: the file may have been removed mid-write by the
                # tearer-down. We only assert recovery below.
                pass

    def tearer() -> None:
        with lock:
            metadata_db.teardown(sid)

    with ThreadPoolExecutor(max_workers=4) as ex:
        # Mix the two operations to maximise the chance of overlap
        futs = [ex.submit(writer), ex.submit(tearer), ex.submit(writer), ex.submit(tearer)]
        for f in futs:
            f.result()  # propagates only unexpected exceptions

    # Recovery: a fresh insert must succeed and be visible
    metadata_db.insert_ingested_files(sid, [("recovered.gz", 99, 9999)])
    con = metadata_db.get_con(sid)
    names = {r[0] for r in con.execute("SELECT file_name FROM ingested_files").fetchall()}
    assert "recovered.gz" in names


def test_teardown_is_idempotent():
    """Calling teardown on a never-created service must not raise."""
    metadata_db.teardown("svc-teardown-never-existed")  # no exception expected


def test_threads_get_isolated_connections():
    """The pool is keyed on (thread, service_id). Two threads must NOT share
    a connection object — sqlite3 connections aren't thread-safe.

    A barrier pins all N workers in-flight simultaneously so the executor
    is forced to spin up N distinct threads. Without it, ``ex.map`` can
    schedule successive tasks onto the same recycled worker thread and
    the test trivially "passes" with fewer distinct connections than
    workers — flaky in the wrong direction.
    """
    import threading

    sid = "svc-isolation"
    n_workers = 4
    barrier = threading.Barrier(n_workers)

    def worker(_: int) -> int:
        con = metadata_db.get_con(sid)
        barrier.wait(timeout=5.0)  # hold the thread until all peers arrive
        return id(con)

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        ids = list(ex.map(worker, range(n_workers)))

    assert len(set(ids)) == n_workers, f"expected {n_workers} distinct connection objects, got {len(set(ids))}: {ids}"


# ── Merged from tests/core/test_metadata_db_reap.py ──
def _seed_running(service_id: str, task: str = "sync") -> int:
    con = metadata_db.get_con(service_id)
    cur = con.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
        "VALUES (?, ?, datetime('now'), 0.0, 'running', '[]')",
        (service_id, task),
    )
    con.commit()
    return int(cur.lastrowid or 0)


def test_reap_marks_running_rows_as_error():
    sid = "svc-reap-1"
    rid = _seed_running(sid)

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 1
    con = metadata_db.get_con(sid)
    row = con.execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (rid,)).fetchone()
    assert row["status"] == "error"
    assert "interrupted" in (row["error_message"] or "").lower()


def test_reap_is_no_op_when_nothing_running():
    sid = "svc-reap-2"
    metadata_db.get_con(sid)  # initialise schema
    n = metadata_db.reap_running_cron_runs(sid)
    assert n == 0


def test_reap_does_not_touch_completed_rows():
    sid = "svc-reap-3"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
        "VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con.commit()

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 0
    row = con.execute("SELECT status FROM cron_runs WHERE task = 'sync'").fetchone()
    assert row["status"] == "success"


def test_reap_ignores_age_unlike_start_cron_run():
    """``start_cron_run`` only reaps rows older than 60 min; this reaps any age.

    The whole point of the new function is to wipe state on startup, when even
    a freshly-running row is an orphan because the in-memory dict is gone.
    """
    sid = "svc-reap-4"
    rid = _seed_running(sid)  # started_at = now (well under 60 min)

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 1
    row = metadata_db.get_con(sid).execute("SELECT status FROM cron_runs WHERE id = ?", (rid,)).fetchone()
    assert row["status"] == "error"


def test_reap_clears_legacy_null_service_id_rows():
    """Regression (GCE 2026-09-22): pre-migration 'sync'/'metadata_sync'
    rows stored NULL in the service_id column. ``WHERE service_id = ?`` is
    NULL-unsafe (NULL = 'x' is never true in SQL), so those rows survived
    every restart forever and permanently degraded /api/health?deep=1 with
    "sync stuck running since ...". Since get_con(service_id) already
    scopes the connection to this service's own file, every row in it
    belongs to this service regardless of its service_id column — the
    reap must clear NULL rows too.
    """
    sid = "svc-reap-null-legacy"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
        "VALUES (NULL, 'sync', datetime('now'), 0.0, 'running', '[]')"
    )
    con.commit()

    n = metadata_db.reap_running_cron_runs(sid)

    assert n == 1
    row = con.execute("SELECT status, error_message FROM cron_runs WHERE task = 'sync'").fetchone()
    assert row["status"] == "error"
    assert "interrupted" in (row["error_message"] or "").lower()


def test_reap_custom_reason_is_recorded():
    sid = "svc-reap-5"
    _seed_running(sid)

    metadata_db.reap_running_cron_runs(sid, reason="Pod evicted by k8s")

    row = metadata_db.get_con(sid).execute("SELECT error_message FROM cron_runs WHERE status = 'error'").fetchone()
    assert "k8s" in (row["error_message"] or "")


def _seed_lease(sid: str, task: str, heartbeat_at: float, ttl_s: int = 60) -> None:
    """Insert the ``job_runs`` lease that ``start_cron_run`` consults. The reap is
    heartbeat-driven now: a 'running' cron_runs row is an orphan only when no
    lease row with ``heartbeat_at + lease_ttl_s >= now`` backs it."""
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO job_runs (service_id, job_name, started_at, heartbeat_at, lease_ttl_s, status) "
        "VALUES (?, ?, ?, ?, ?, 'running')",
        (sid, task, heartbeat_at, heartbeat_at, ttl_s),
    )
    con.commit()


def test_start_cron_run_reaps_rows_exactly_at_lease_expiry():
    """``start_cron_run`` reaps a 'running' cron row once its job_runs lease
    (heartbeat + ttl) has lapsed, and refuses to start while the lease is live.

    Pins the boundary deterministically with time-machine: a lease heartbeating
    at T=0 with a 60 s ttl still blocks a start at T+59 s (the row must stay
    'running'), but is reaped at T+61 s so the new run starts. A different
    task's live lease is never touched (cross-task isolation).
    """
    import datetime as _dt

    import time_machine

    sid = "svc-lease-boundary"
    t0 = _dt.datetime(2026, 6, 16, 12, 0, 0, tzinfo=_dt.UTC)

    old_id = _seed_running_at(sid, "commit", t0)
    _seed_lease(sid, "commit", t0.timestamp(), ttl_s=60)
    cross_id = _seed_running_at(sid, "optimize", t0)
    _seed_lease(sid, "optimize", t0.timestamp() + 3600, ttl_s=60)

    # T+59s: lease still live -> refused, nothing reaped.
    with time_machine.travel(t0 + _dt.timedelta(seconds=59), tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "commit")

    con = metadata_db.get_con(sid)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "running"

    # T+61s: lease lapsed -> the orphan row is reaped and a fresh run starts.
    with time_machine.travel(t0 + _dt.timedelta(seconds=61), tick=False):
        new_id = metadata_db.start_cron_run(sid, "commit")

    assert new_id and new_id != old_id
    old = con.execute("SELECT status, error_message FROM cron_runs WHERE id = ?", (old_id,)).fetchone()
    assert old["status"] == "error"
    assert "heartbeat lost" in (old["error_message"] or "")
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (cross_id,)).fetchone()["status"] == "running", (
        "a different task's live lease must not be reaped by this task's start"
    )


def _seed_running_at(sid: str, task: str, when) -> int:
    """Insert a 'running' row stamped at the (time-machine) ``when`` instant."""
    import time_machine

    from backend.utils.date_utils import iso_z_now

    with time_machine.travel(when, tick=False):
        con = metadata_db.get_con(sid)
        cur = con.execute(
            "INSERT INTO cron_runs (service_id, task, started_at, duration_s, status, parquet_keys) "
            "VALUES (?, ?, ?, 0.0, 'running', '[]')",
            (sid, task, iso_z_now()),
        )
        con.commit()
        return int(cur.lastrowid or 0)


def test_sync_orphan_threshold_is_short_and_reaps_at_10_min():
    """A 'sync' row is reaped after the per-task 10-min cutoff (not the
    60-min default) so a stuck/leaked sync can't freeze ingestion for an
    hour. After reaping the only running row, start_cron_run proceeds and
    returns a fresh id (no 'already running')."""
    import datetime as _dt

    import time_machine

    sid = "svc-sync-threshold"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, "sync", t0)

    # 11 min later: the sync row is past the 10-min cutoff → reaped, and a
    # new run starts cleanly.
    with time_machine.travel(t0 + _dt.timedelta(minutes=11), tick=False):
        new_id = metadata_db.start_cron_run(sid, "sync")

    assert new_id and new_id != old_id
    con = metadata_db.get_con(sid)
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"


def test_live_lease_blocks_start_even_when_cron_row_is_old():
    """The orphan decision is the lease heartbeat, not the cron row's age: a
    'sync' row started 11 minutes ago whose lease heartbeated 30 s ago is a
    healthy long-running sync, so a concurrent start is refused. This is what
    stops the heartbeat model from double-running healthy syncs."""
    import datetime as _dt

    import time_machine

    sid = "svc-sync-live-lease"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    row_id = _seed_running_at(sid, "sync", t0)
    now = t0 + _dt.timedelta(minutes=11)
    _seed_lease(sid, "sync", (now - _dt.timedelta(seconds=30)).timestamp(), ttl_s=60)

    with time_machine.travel(now, tick=False):
        with pytest.raises(RuntimeError, match="already running"):
            metadata_db.start_cron_run(sid, "sync")

    status = metadata_db.get_con(sid).execute("SELECT status FROM cron_runs WHERE id = ?", (row_id,)).fetchone()
    assert status["status"] == "running"


@pytest.mark.parametrize("task", ["sync", "commit"])
def test_lease_less_running_row_is_reaped_immediately_for_any_task(task):
    """A 'running' cron row with NO backing lease is an orphan (its owner died
    without a heartbeat) and is reaped on the next start regardless of age or
    task — there is no per-task 10/60-minute age cutoff any more."""
    import datetime as _dt

    import time_machine

    sid = f"svc-no-lease-{task}"
    t0 = _dt.datetime(2026, 6, 19, 3, 40, 0, tzinfo=_dt.UTC)
    old_id = _seed_running_at(sid, task, t0)

    with time_machine.travel(t0 + _dt.timedelta(seconds=5), tick=False):
        new_id = metadata_db.start_cron_run(sid, task)

    con = metadata_db.get_con(sid)
    assert new_id and new_id != old_id
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (old_id,)).fetchone()["status"] == "error"
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (new_id,)).fetchone()["status"] == "running"


def test_finalize_flips_running_row_to_error():
    sid = "svc-finalize-1"
    rid = _seed_running(sid, "sync")

    flipped = metadata_db.finalize_cron_run_if_running(
        sid, "sync", rid, summary="no terminal event", error_message="orphaned"
    )

    assert flipped is True
    row = (
        metadata_db.get_con(sid)
        .execute("SELECT status, error_message, summary FROM cron_runs WHERE id = ?", (rid,))
        .fetchone()
    )
    assert row["status"] == "error"
    assert row["error_message"] == "orphaned"
    assert row["summary"] == "no terminal event"


def test_finalize_is_noop_on_terminal_row():
    """Must not clobber a row that already reached 'success' (the common
    case — finalize runs in a finally AFTER a successful log_cron_run)."""
    sid = "svc-finalize-2"
    con = metadata_db.get_con(sid)
    con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys, summary) "
        "VALUES ('sync', datetime('now'), 5.0, 'success', '[]', 'ok')"
    )
    con.commit()
    rid = int(con.execute("SELECT id FROM cron_runs WHERE status='success'").fetchone()["id"])

    flipped = metadata_db.finalize_cron_run_if_running(sid, "sync", rid)

    assert flipped is False
    assert con.execute("SELECT status FROM cron_runs WHERE id = ?", (rid,)).fetchone()["status"] == "success"


def test_finalize_noop_when_run_id_missing():
    sid = "svc-finalize-3"
    metadata_db.get_con(sid)  # init schema
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", None) is False
    assert metadata_db.finalize_cron_run_if_running(sid, "sync", 999999) is False


# ── Merged from tests/core/test_metadata_db_schema.py ──
def _reapply_schema() -> None:
    """Re-run Postgres schema bootstrap, simulating a fresh-boot re-apply
    of ``_SCHEMA`` against an already-initialised database. Replaces the
    old SQLite-only ``metadata_db._init_schema(con)`` call these tests used
    before Postgres became the only backend — schema bootstrap now goes
    through :func:`pg_schema.ensure_pg_schema` exclusively (see
    ``backend/core/metadata/base.py``'s module docstring)."""
    pg_schema.ensure_pg_schema(force=True)


_EXPECTED_TABLES = {
    "sources",
    "ingested_files",
    "ingested_files_summary",
    "ingest_in_flight",
    "cron_runs",
    "asn_names",
    "audit_logs",
    "views",
    "alerts",
    "scoring_labels",
}

_EXPECTED_INDEXES = {
    "idx_ingested_files_source_ingested_at",
    "idx_in_flight_source",
    "idx_cron_task_started",
    "idx_cron_started",
    "idx_audit_source",
    "idx_scoring_labels_svc_sid",
    "idx_scoring_labels_svc_label",
}


def _list_tables(con: sqlite3.Connection) -> set[str]:
    return {r[0] for r in con.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").fetchall()}


def _list_indexes(con: sqlite3.Connection) -> set[str]:
    return {
        r[0] for r in con.execute("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()").fetchall()
    }


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ?",
            (table,),
        ).fetchall()
    }


def test_init_schema_creates_all_expected_tables():
    sid = "svc-schema-shape"
    con = metadata_db.get_con(sid)
    found = _list_tables(con)
    missing = _EXPECTED_TABLES - found
    assert not missing, f"missing tables after _init_schema: {missing}"


def test_init_schema_creates_all_expected_indexes():
    sid = "svc-schema-indexes"
    con = metadata_db.get_con(sid)
    found = _list_indexes(con)
    missing = _EXPECTED_INDEXES - found
    assert not missing, f"missing indexes after _init_schema: {missing}"


def test_alerts_table_has_evaluation_scope_column():
    """Regression: alerts.evaluation_scope was added later. Make sure new
    services get the full current shape."""
    sid = "svc-schema-alerts"
    con = metadata_db.get_con(sid)
    cols = _columns(con, "alerts")
    assert "evaluation_scope" in cols
    assert "comparison_period_min" in cols  # also late-added


def _column_type(con: sqlite3.Connection, table: str, column: str) -> str:
    row = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
        (table, column),
    ).fetchone()
    return row[0] if row else ""


def test_usage_log_byte_columns_are_bigint():
    """``usage_log.bytes`` and ``usage_log_hourly_summary.bytes`` accumulate
    byte counts (the hourly summary is a running SUM per service/hour). A
    Postgres ``integer`` (int4) column caps at 2,147,483,647 (~2.1 GB); a
    busy service's hourly CDN egress blows past that in a single hour,
    raising ``integer out of range`` and silently dropping every usage row
    for the rest of that hour. The columns must be 64-bit."""
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc-usage-bigint")
    for table, column in (
        ("usage_log", "bytes"),
        ("usage_log", "count"),
        ("usage_log_hourly_summary", "bytes"),
        ("usage_log_hourly_summary", "count"),
    ):
        assert _column_type(con, table, column) == "bigint", f"{table}.{column} must be bigint, not int4"


def test_log_usage_calls_accepts_bytes_over_int4_max():
    """Regression for the production ``[metadata_db] Failed to log usage
    calls: integer out of range`` flood. A single CDN op whose (egress-
    doubled) byte count exceeds int4 max must round-trip, not get swallowed
    by ``log_usage_calls``'s broad ``except`` and vanish from the table."""
    from backend.core.metadata import usage_log, usage_log_db

    sid = "svc-usage-overflow"
    big = 3_000_000_000  # > int4 max 2_147_483_647, within int8
    usage_log.log_usage_calls(
        sid,
        [{"method": "GET", "service": "FOS", "path": "/big.gz", "bytes": big, "caller": "test"}],
    )

    con = usage_log_db.get_con(sid)
    row = con.execute("SELECT bytes FROM usage_log WHERE service_id = ? AND url = ?", (sid, "/big.gz")).fetchone()
    assert row is not None, "usage row was dropped (int4 overflow swallowed the INSERT)"
    assert row[0] == big

    hourly = con.execute("SELECT max(bytes) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
    assert hourly is not None and hourly[0] == big


def test_init_schema_is_idempotent_no_data_loss():
    """Re-applying schema bootstrap to an already-populated database must
    preserve every row. This is the load-bearing property that lets future
    schema additions just append to ``_SCHEMA``.
    """
    sid = "svc-schema-idem"
    metadata_db.insert_ingested_files(sid, [("file-a.gz", 100, 4096), ("file-b.gz", 200, 8192)])

    con = metadata_db.get_con(sid)
    before = con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert before == 2

    # Re-apply schema. With ``IF NOT EXISTS`` everywhere, this should be a no-op.
    _reapply_schema()

    after = con.execute("SELECT count(*) FROM ingested_files WHERE source_name = ?", (sid,)).fetchone()[0]
    assert after == 2, f"data lost after re-init: was 2 rows, now {after}"


def test_init_schema_run_twice_is_safe_in_sequence():
    """Schema bootstrap can legitimately run more than once per process
    (a concurrently-booting pod, or a forced re-apply). Verify back-to-back
    calls don't raise.
    """
    sid = "svc-schema-double"
    metadata_db.get_con(sid)
    _reapply_schema()
    _reapply_schema()  # must not raise


def test_pre_existing_data_survives_added_table():
    """Simulate a future migration: a new ``CREATE TABLE IF NOT EXISTS``
    statement is added to ``_SCHEMA``. Existing data in other tables
    must survive when schema bootstrap re-runs on the next process boot.
    """
    sid = "svc-schema-future"
    metadata_db.insert_ingested_files(sid, [("survivor.gz", 1, 100)])

    # Apply a hypothetical future migration (extra table)
    con = metadata_db.get_con(sid)
    con.execute("CREATE TABLE IF NOT EXISTS new_feature_table (id SERIAL PRIMARY KEY, payload TEXT)")
    con.commit()

    # Re-apply the standard schema — survivor row must remain
    _reapply_schema()

    rows = con.execute("SELECT file_name FROM ingested_files WHERE source_name = ?", (sid,)).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "survivor.gz"

    # And the new_feature_table is still there too
    tables = _list_tables(con)
    assert "new_feature_table" in tables


@pytest.mark.parametrize(
    "bad_sid",
    [
        None,
        123,
        object(),
        {"name": "svc-a"},
    ],
    ids=["none", "int", "object", "dict"],
)
def test_db_path_rejects_non_string_service_id(bad_sid):
    """``db_path`` builds the SQLite filename via ``f"{service_id}.metadata.db"``.
    A non-string argument would silently produce a junk path containing the
    object's repr (e.g. ``<...0x...>.metadata.db``), leaking files on disk
    and corrupting per-service routing. The boundary must fail loud.
    """
    with pytest.raises(TypeError):
        metadata_db.db_path(bad_sid)


def test_get_con_rejects_non_string_service_id():
    """Same boundary as ``db_path`` — ``get_con`` builds the path via
    ``db_path``, and a non-string would create the file regardless. Pin
    the rejection at the public entry point as well.
    """
    with pytest.raises(TypeError):
        metadata_db.get_con(object())


@pytest.mark.parametrize(
    "bad_sid",
    [
        "../etc/passwd",  # path traversal via segment
        "foo/bar",  # embedded separator
        "foo\x00bar",  # null byte (truncates fopen on POSIX)
        "",  # empty produces ".metadata.db" (hidden junk file)
        "x" * 65,  # over 64-char cap
        "with space",  # whitespace
        "foo.bar",  # periods (not in Fastly's documented format)
        "\U00018d1f",  # plane-1 codepoint APFS rejects with Errno 92
        "café",  # any non-ASCII Unicode
    ],
    ids=["traversal", "slash", "null_byte", "empty", "too_long", "space", "period", "apfs_illegal", "non_ascii"],
)
def test_db_path_rejects_malformed_service_id(bad_sid):
    """The pattern guard rejects any string that could traverse the data
    directory or hit ``OSError(Errno 92): Illegal byte sequence`` on APFS /
    strict Linux. Pinned because losing this regresses the FastAPI 422
    contract — schemathesis fuzzing surfaced the path with %F0%98%B4%9F
    producing an opaque sqlite3.OperationalError 500.
    """
    from backend.core.metadata.base import InvalidServiceIdError

    with pytest.raises(InvalidServiceIdError):
        metadata_db.db_path(bad_sid)


def test_invalid_service_id_in_path_returns_422(client):
    """A malformed ``service_id`` in a path parameter must surface as 422
    (validation error) rather than 500 (sqlite OperationalError). The
    backend exception handler in main.py converts InvalidServiceIdError
    to a body matching FastAPI's ``HTTPValidationError`` schema so the
    response stays OpenAPI-conformant (schemathesis verified).
    """
    # Use a route that takes service_id as a Path parameter and reaches
    # the metadata_db layer. /scoring/labels exercises this surface.
    resp = client.get("/api/services/foo.bar/scoring/labels")
    assert resp.status_code == 422, f"expected 422, got {resp.status_code}: {resp.text[:200]}"
    body = resp.json()
    assert isinstance(body.get("detail"), list), "detail must be a list per HTTPValidationError"
    err = body["detail"][0]
    assert err["loc"] == ["path", "service_id"]
    assert "service_id must match" in err["msg"]
    assert err["type"] == "value_error.invalid_service_id"


# ── Merged from tests/core/test_metadata_postgres_only.py ──
def _in_fresh_thread(fn):
    """Run ``fn`` on a thread with no cached thread-local connection; return its exception."""
    box: dict[str, BaseException | None] = {"exc": None}

    def run():
        try:
            fn()
        except BaseException as e:  # noqa: BLE001 — the exception is the assertion target
            box["exc"] = e

    t = threading.Thread(target=run)
    t.start()
    t.join()
    return box["exc"]


@pytest.fixture
def no_dsn(monkeypatch):
    pg_connection.reset_pg_pool_for_tests()
    monkeypatch.delenv("METADATA_DSN", raising=False)
    yield
    pg_connection.reset_pg_pool_for_tests()


@pytest.mark.parametrize(
    "opener",
    [base.get_con, base.get_con_readonly, usage_log_db.get_con, usage_log_db.open_readonly],
    ids=["base.get_con", "base.get_con_readonly", "usage_log_db.get_con", "usage_log_db.open_readonly"],
)
def test_connection_openers_raise_without_dsn(no_dsn, opener):
    exc = _in_fresh_thread(lambda: opener("svc-1"))
    assert isinstance(exc, RuntimeError)
    assert "METADATA_DSN is not set" in str(exc)


def test_deployment_validation_refuses_a_non_postgres_metadata_dsn(monkeypatch):
    monkeypatch.setattr(config, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(config, "DUCKLAKE_CATALOG", "postgresql://u:p@h/db")
    monkeypatch.setenv("METADATA_DSN", "sqlite:///tmp/x.db")
    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        config.validate_deployment_mode()


def test_usage_log_ddl_still_comes_from_usage_log_db_schema():
    from backend.core.metadata import pg_schema

    statements = pg_schema.pg_schema_statements()
    joined = "\n".join(statements)
    assert "CREATE TABLE IF NOT EXISTS usage_log (" in joined
    assert "CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (" in joined
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    assert "AUTOINCREMENT" not in joined


@pytest.fixture
def postgres_mode(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://fake/fake")
    yield


def test_get_con_routes_to_postgres_when_dsn_set(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=sentinel) as mock_get:
        result = base.get_con("svc-1")
    assert result is sentinel
    mock_get.assert_called_once_with()


def test_get_con_still_validates_service_id_under_postgres(postgres_mode):
    with patch.object(base.pg_connection, "get_pg_thread_connection", return_value=MagicMock()):
        with pytest.raises(InvalidServiceIdError):
            base.get_con("not a valid id / has slash")


def test_latest_ingest_timestamp_uses_postgres_compatible_aggregate(postgres_mode):
    cursor = MagicMock()
    cursor.fetchone.return_value = {"latest": "2026-09-08T21:34:32Z"}
    connection = MagicMock()

    def execute(sql, params):
        if "datetime(" in sql:
            raise AssertionError("SQLite datetime() is not valid PostgreSQL")
        return cursor

    connection.execute.side_effect = execute
    with patch.object(ingest_log, "get_con", return_value=connection):
        result = ingest_log.get_latest_ingest_ts("svc-1")

    assert result == "2026-09-08T21:34:32Z"


def test_get_con_readonly_routes_to_postgres(postgres_mode):
    sentinel = MagicMock()
    with patch.object(base.pg_connection, "get_pg_readonly_connection", return_value=sentinel) as mock_get:
        result = base.get_con_readonly("svc-1")
    assert result is sentinel
    mock_get.assert_called_once_with()


def test_release_thread_connection_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "release_pg_thread_connection") as mock_release:
        base.release_thread_connection()
    mock_release.assert_called_once_with()


def test_close_all_connections_routes_to_postgres(postgres_mode):
    with patch.object(base.pg_connection, "close_all_pg_connections") as mock_close:
        base.close_all_connections()
    mock_close.assert_called_once()


def test_teardown_is_noop_under_postgres(postgres_mode):
    """No per-service file exists under Postgres — teardown clears the cache without error."""
    base.teardown("svc-1")


def test_get_global_share_con_routes_to_postgres_thread_connection():
    sentinel = MagicMock()
    with patch.object(share_db_connection, "get_pg_thread_connection", return_value=sentinel) as mock_pg:
        result = share_db_connection.get_global_share_con()
    assert result is sentinel
    mock_pg.assert_called_once_with()


def test_stale_thread_wrapper_is_replaced_after_close_all_from_another_thread():
    first_ready = threading.Event()
    closed = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        results["first"] = pg_connection.get_pg_thread_connection()
        first_ready.set()
        closed.wait(10)
        second = pg_connection.get_pg_thread_connection()
        results["second"] = second
        results["value"] = second.execute("SELECT 1").fetchone()[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert first_ready.wait(10)
    pg_connection.close_all_pg_connections()
    closed.set()
    t.join(15)

    assert results["second"] is not results["first"]
    assert results["value"] == 1


def test_close_all_leaves_a_connection_with_a_query_in_flight_to_its_owner():
    """Returning a busy connection from another thread freed it under the running query (worker segfault)."""
    started = threading.Event()
    results: dict[str, object] = {}

    def worker() -> None:
        con = pg_connection.get_pg_thread_connection()
        results["wrapper"] = con
        started.set()
        results["value"] = con.execute("SELECT 1 FROM pg_sleep(1)").fetchone()[0]
        results["returned_after_query"] = con._returned[0]
        pg_connection.release_pg_thread_connection()

    t = threading.Thread(target=worker)
    t.start()
    assert started.wait(10)
    wrapper = results["wrapper"]
    assert isinstance(wrapper, pg_connection.PgConnectionWrapper)
    for _ in range(500):
        if not wrapper._busy.acquire(blocking=False):
            break
        wrapper._busy.release()
        time.sleep(0.01)
    else:
        raise AssertionError("worker never started its query")
    pg_connection.close_all_pg_connections()
    t.join(15)

    assert results["value"] == 1
    assert results["returned_after_query"] is False


def test_return_to_pool_once_is_atomic_under_contention():
    """Two threads racing to return the same connection must putconn it exactly once."""
    pool = MagicMock()
    returned = [False]
    start = threading.Barrier(8)

    def go() -> None:
        start.wait(5)
        pg_connection._return_to_pool_once(object(), returned, pool)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert pool.putconn.call_count == 1


# ── Merged from tests/core/test_usage_log_postgres_summary.py ──
def test_log_usage_calls_updates_hourly_summary(monkeypatch):
    sid = "svc_test_summary_1"
    usage_log.clear_usage_log(sid)

    # Patch time to test multi-hour batches
    now_hour_10 = "2026-05-01T10:15:30Z"
    calls_hour_10 = [
        {"service": "FOS", "method": "PutObject", "path": "/f1", "bytes": 100},
        {"service": "FOS", "method": "PutObject", "path": "/f2", "bytes": 200},
        {"service": "FOS", "method": "GetObject", "path": "/f3", "bytes": 50},
        {"service": "CDN", "method": "download", "path": "/c1", "bytes": 1000},
    ]

    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_hour_10):
        usage_log.log_usage_calls(sid, calls_hour_10)

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT hour, operation_class, operation_type, count, bytes "
        "FROM usage_log_hourly_summary WHERE service_id = ? "
        "ORDER BY hour, operation_class, operation_type",
        (sid,),
    ).fetchall()

    assert len(rows) == 3
    # Hour 10: PutObject (Class A, count 2, bytes 300)
    assert rows[0]["hour"] == "2026-05-01T10"
    assert rows[0]["operation_class"] == "A"
    assert rows[0]["operation_type"] == "PutObject"
    assert rows[0]["count"] == 2
    assert rows[0]["bytes"] == 300

    # Hour 10: GetObject (Class B, count 1, bytes 50)
    assert rows[1]["hour"] == "2026-05-01T10"
    assert rows[1]["operation_class"] == "B"
    assert rows[1]["operation_type"] == "GetObject"
    assert rows[1]["count"] == 1
    assert rows[1]["bytes"] == 50

    # Hour 10: download (Class CDN, count 1, bytes 1000)
    assert rows[2]["hour"] == "2026-05-01T10"
    assert rows[2]["operation_class"] == "CDN"
    assert rows[2]["operation_type"] == "download"
    assert rows[2]["count"] == 1
    assert rows[2]["bytes"] == 1000

    # Second batch in hour 11: also adds to hour 10 to test ON CONFLICT accumulation
    calls_batch_2 = [
        {"service": "FOS", "method": "PutObject", "path": "/f4", "bytes": 400},
    ]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now_hour_10):
        usage_log.log_usage_calls(sid, calls_batch_2)

    row = con.execute(
        "SELECT count, bytes FROM usage_log_hourly_summary "
        "WHERE service_id = ? AND hour = '2026-05-01T10' AND operation_class = 'A' AND operation_type = 'PutObject'",
        (sid,),
    ).fetchone()
    assert row["count"] == 3
    assert row["bytes"] == 700


def test_log_synthetic_usage_updates_hourly_summary():
    sid = "svc_test_summary_2"
    usage_log.clear_usage_log(sid)

    calls = [
        {
            "_timestamp_override": "2026-06-15T08:12:00Z",
            "method": "PUT_OBJECT",
            "path": "/obj1.parquet",
            "bytes": 500,
        },
        {
            "_timestamp_override": "2026-06-15T08:45:00Z",
            "method": "PUT_OBJECT",
            "path": "/obj2.parquet",
            "bytes": 700,
        },
        {
            "_timestamp_override": "2026-06-15T09:05:00Z",
            "method": "PUT_OBJECT",
            "path": "/obj3.parquet",
            "bytes": 300,
        },
    ]

    inserted = usage_log.log_synthetic_usage(sid, calls)
    assert inserted == 3

    con = usage_log_db.get_con(sid)
    rows = con.execute(
        "SELECT hour, operation_class, operation_type, count, bytes "
        "FROM usage_log_hourly_summary WHERE service_id = ? "
        "ORDER BY hour",
        (sid,),
    ).fetchall()

    assert len(rows) == 2
    assert rows[0]["hour"] == "2026-06-15T08"
    assert rows[0]["operation_class"] == "A"
    assert rows[0]["count"] == 2
    assert rows[0]["bytes"] == 1200

    assert rows[1]["hour"] == "2026-06-15T09"
    assert rows[1]["operation_class"] == "A"
    assert rows[1]["count"] == 1
    assert rows[1]["bytes"] == 300


def test_reconcile_fastly_stats_adjusts_summary():
    sid = "svc_test_summary_3"
    usage_log.clear_usage_log(sid)

    hour = "2026-07-01T14:00:00Z"
    # Seed 10 local Class A operations via log_usage_calls
    calls = [{"service": "FOS", "method": "PutObject", "path": f"/k{i}"} for i in range(10)]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=hour):
        usage_log.log_usage_calls(sid, calls)

    con = usage_log_db.get_con(sid)
    row = con.execute(
        "SELECT count FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
        (sid,),
    ).fetchone()
    assert row["count"] == 10

    # Fastly reports 25 Class A operations for the hour -> gap = 15
    hourly_records = [{"hour_iso": hour, "class_a": 25, "class_b": 0}]
    written = usage_log.reconcile_fastly_stats(sid, hourly_records)
    assert written == 1

    # Total in summary should now be 10 + 15 = 25
    sum_row = con.execute(
        "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
        (sid,),
    ).fetchone()
    assert sum_row["total"] == 25

    # Re-run reconciliation with 30 Class A operations -> previous gap of 15 is removed, new gap is 20
    hourly_records_updated = [{"hour_iso": hour, "class_a": 30, "class_b": 0}]
    written_2 = usage_log.reconcile_fastly_stats(sid, hourly_records_updated)
    assert written_2 == 1

    sum_row_2 = con.execute(
        "SELECT sum(count) AS total FROM usage_log_hourly_summary WHERE service_id = ? AND hour = '2026-07-01T14'",
        (sid,),
    ).fetchone()
    assert sum_row_2["total"] == 30


def test_purge_and_clear_usage_log():
    sid = "svc_test_summary_4"
    usage_log.clear_usage_log(sid)

    now = "2020-01-01T12:00:00Z"
    calls = [{"service": "FOS", "method": "PutObject", "path": "/old"}]
    with patch("backend.core.metadata.usage_log.iso_z_now", return_value=now):
        usage_log.log_usage_calls(sid, calls)

    con = usage_log_db.get_con(sid)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 1

    # Purge old raw logs (retention 1 day)
    usage_log.purge_usage_log(sid, retention_days=1)
    # Raw log deleted
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
    # Summary preserved by design
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 1

    # clear_usage_log wipes both
    usage_log.clear_usage_log(sid)
    assert con.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0] == 0


# ── Merged from tests/core/test_v3_boot_migration.py ──
class _FakeCursor:
    """Records DDL. ``fail_with`` maps a substring → exception to raise."""

    def __init__(self, fail_with: dict | None = None):
        self.statements: list[str] = []
        self._fail_with = fail_with or {}

    def execute(self, sql: str):
        self.statements.append(sql)
        for needle, exc in self._fail_with.items():
            if needle in sql:
                raise exc

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, cursor: _FakeCursor):
        self._conn = _FakeConn(cursor)

    def connection(self):
        return self._conn


def _pg_error(sqlstate: str, message: str) -> Exception:
    exc = Exception(message)
    exc.sqlstate = sqlstate  # type: ignore[attr-defined]
    return exc


def test_pg_schema_statements_are_postgres_dialect():
    statements = pg_schema.pg_schema_statements()
    blob = "\n".join(statements)

    # The tables that only exist because of an explicit DDL entry (they are
    # created by SQLite migrations, not _SCHEMA) — the exact gap that lost
    # committed_buffers on Postgres before ADR-18.
    assert "cron_runs" in blob
    assert "slow_queries" in blob
    assert "ingest_ledger" in blob
    assert "metric_snapshots" in blob
    assert "high_scale_ownership" in blob
    assert "high_scale_source_objects" in blob
    assert "high_scale_publication_manifests" in blob

    # SQLite-only dialect must be translated away.
    assert "AUTOINCREMENT" not in blob
    assert "datetime('now')" not in blob
    assert not any(s.strip().startswith("CREATE TRIGGER") for s in statements)
    # Every statement must be re-runnable against a populated database.
    assert all(
        "IF NOT EXISTS" in s or "DROP INDEX" in s or "ON CONFLICT" in s or "ALTER TABLE" in s for s in statements
    ), "a statement without IF NOT EXISTS makes the bootstrap non-idempotent"


def test_apply_pg_schema_executes_every_statement():
    cur = _FakeCursor()
    applied, raced = pg_schema.apply_pg_schema(cur)

    assert applied == len(pg_schema.pg_schema_statements())
    assert raced == 0
    assert len(cur.statements) == applied


def test_apply_pg_schema_tolerates_a_concurrent_pod_winning():
    """Concurrent CREATE TABLE/INDEX IF NOT EXISTS can still collide on the
    shared catalog. Those SQLSTATEs mean another pod got there first, which
    is success, not failure."""
    cur = _FakeCursor(
        fail_with={
            "cron_runs (": _pg_error("42P07", 'relation "cron_runs" already exists'),
            "idx_cron_status": _pg_error("23505", "duplicate key value violates unique constraint"),
        }
    )
    applied, raced = pg_schema.apply_pg_schema(cur)

    assert raced == 2
    assert applied == len(pg_schema.pg_schema_statements()) - 2


def test_apply_pg_schema_raises_on_a_real_ddl_error():
    cur = _FakeCursor(fail_with={"cron_runs (": _pg_error("42601", "syntax error at or near")})
    with pytest.raises(RuntimeError, match="failed to apply schema statement"):
        pg_schema.apply_pg_schema(cur)


def test_ensure_pg_schema_is_a_noop_without_metadata_dsn(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    pg_schema.reset_ensure_flag_for_tests()
    assert pg_schema.ensure_pg_schema() is False


def test_ensure_pg_schema_applies_once_per_process(monkeypatch):
    """Cheap by construction: several hundred DDL statements at most, once."""
    from backend.core.metadata import pg_connection

    cur = _FakeCursor()
    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    pg_schema.reset_ensure_flag_for_tests()

    assert pg_schema.ensure_pg_schema() is True
    first_pass = len(cur.statements)
    assert first_pass > 0

    # Second call in the same process must not re-issue the DDL.
    assert pg_schema.ensure_pg_schema() is False
    assert len(cur.statements) == first_pass

    # ...and re-applying it is still safe (every statement is IF NOT EXISTS),
    # which is what makes a restart or an explicit ops run harmless.
    assert pg_schema.ensure_pg_schema(force=True) is True
    assert len(cur.statements) == first_pass * 2
    pg_schema.reset_ensure_flag_for_tests()


def test_ensure_pg_schema_never_raises_and_does_not_latch_on_failure(monkeypatch, caplog):
    """A transient Postgres blip at boot must not crash-loop the pod — but it
    must also not be latched as done, so the next caller retries."""
    from backend.core.metadata import pg_connection

    monkeypatch.setenv("METADATA_DSN", "postgresql://stub/stub")

    def _boom():
        raise OSError("connection refused")

    monkeypatch.setattr(pg_connection, "get_pg_pool", _boom)
    pg_schema.reset_ensure_flag_for_tests()

    with caplog.at_level("CRITICAL"):
        assert pg_schema.ensure_pg_schema() is False
    assert "schema bootstrap FAILED" in caplog.text

    cur = _FakeCursor()
    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _FakePool(cur))
    assert pg_schema.ensure_pg_schema() is True, "a failed attempt must not latch"
    pg_schema.reset_ensure_flag_for_tests()


def test_lifespan_and_worker_boot_call_ensure_pg_schema():
    """Both entry points must be wired — a worker can boot before (or
    without) the API pod, so neither can rely on the other."""
    import inspect

    from backend import celery_app, main

    assert "_application_lifespan(app)" in inspect.getsource(main.lifespan)
    assert "ensure_pg_schema" in inspect.getsource(main._application_lifespan)
    assert "ensure_pg_schema" in inspect.getsource(celery_app._worker_process_init)


# ── Merged from tests/core/test_quarantine.py ──
@pytest.fixture
def svc_id():
    svc = "test-quarantine-svc"
    get_con(svc).execute("SELECT 1")  # trigger migrations
    return svc


def test_quarantine_storage_total_default_zero(svc_id):
    assert get_quarantine_storage_total(svc_id) == 0


def test_service_lock_always_takes_the_postgres_advisory_lock(monkeypatch):
    from contextlib import contextmanager

    from backend.core import quarantine as core_quarantine
    from backend.core.metadata import pg_connection

    executed: list[tuple] = []

    class _Con:
        def execute(self, sql, params=None):
            executed.append((sql, params))

        @contextmanager
        def transaction(self):
            yield

    class _Pool:
        @contextmanager
        def connection(self):
            yield _Con()

    monkeypatch.setattr(pg_connection, "get_pg_pool", lambda: _Pool())

    with core_quarantine._service_lock("svc-lock"):
        pass

    assert executed == [("SELECT pg_advisory_xact_lock(%s)", (core_quarantine._advisory_key("svc-lock"),))]


# ── Merged from tests/core/test_quarantine_evidence.py ──
def test_capture_evidence_preserves_exact_bytes_and_metadata():
    payload = b'{"bad":"\x00\xff\xf0\x9f\x8e\x89"}\n'

    result = quarantine.capture_evidence(
        "evidence-service",
        "request",
        "raw/request/part.log.gz",
        payload,
        line_ordinal=7,
        byte_offset=128,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result["quarantine_capture_failures"] == 0
    assert result["cap_evictions"] == 0
    item, evidence = quarantine.read_evidence("evidence-service", int(result["id"]))
    try:
        assert evidence.read() == payload
    finally:
        evidence.close()

    assert item["source_type"] == "request"
    assert item["original_key"] == "raw/request/part.log.gz"
    assert item["line_ordinal"] == 7
    assert item["byte_offset"] == 128
    assert item["byte_length"] == len(payload)
    assert item["error_category"] == "invalid_json"
    assert item["sha256"] == "7aa0ef0bdccc1d2cb18dac4f2af559c2a259024bae2653330536f95cbeb84fe8"
    evidence_path = quarantine._evidence_path("evidence-service", int(result["id"]))
    assert stat.S_IMODE(evidence_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(evidence_path.parent.stat().st_mode) == 0o700
    assert quarantine.QUARANTINE_ITEM_CAP == 1_000


def test_capture_evidence_enforces_shared_fifo_cap(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 2)
    payloads = [b"first", b"second", b"third"]

    results = [
        quarantine.capture_evidence(
            "fifo-service",
            source_type,
            f"raw/{source_type}/{index}.gz",
            payload,
            line_ordinal=None,
            byte_offset=None,
            error_category="corrupt_container",
            error_text="invalid gzip",
        )
        for index, (source_type, payload) in enumerate(zip(("request", "rum", "request"), payloads, strict=True))
    ]

    assert [result["cap_evictions"] for result in results] == [0, 0, 1]
    rows = list_quarantine_evidence("fifo-service", oldest_first=True)
    assert [row["original_key"] for row in rows] == ["raw/rum/1.gz", "raw/request/2.gz"]
    assert quarantine.read_evidence("fifo-service", int(results[0]["id"])) is None


def test_evidence_summary_counts_sources_categories_and_bytes():
    for source_type, category, payload in (
        ("request", "invalid_json", b"bad"),
        ("rum", "invalid_json", b"worse"),
        ("request", "corrupt_container", b"gzip"),
    ):
        result = quarantine.capture_evidence(
            "summary-service",
            source_type,
            f"raw/{source_type}/{category}.gz",
            payload,
            line_ordinal=None,
            byte_offset=None,
            error_category=category,
            error_text="invalid evidence",
        )
        assert result["quarantine_capture_failures"] == 0

    summary = get_quarantine_evidence_summary("summary-service")
    assert summary["total_items"] == 3
    assert summary["total_bytes"] == 12
    assert summary["request_items"] == 2
    assert summary["rum_items"] == 1
    assert summary["category_counts"] == {"invalid_json": 2, "corrupt_container": 1}
    assert summary["oldest_at"] is not None
    assert summary["newest_at"] is not None


def test_concurrent_capture_keeps_all_items_and_purge_removes_bytes():
    payload = b"corrupt gzip bytes"

    def capture(index: int):
        return quarantine.capture_evidence(
            "concurrent-service",
            "rum" if index % 2 else "request",
            f"raw/{index}.gz",
            payload + str(index).encode(),
            line_ordinal=None,
            byte_offset=None,
            error_category="corrupt_container",
            error_text="invalid gzip",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(capture, range(16)))

    assert all(result["quarantine_capture_failures"] == 0 for result in results)
    rows = list_quarantine_evidence("concurrent-service")
    assert len(rows) == 16

    metadata, evidence = quarantine.read_evidence("concurrent-service", int(results[0]["id"]))
    path = quarantine._evidence_path("concurrent-service", int(metadata["id"]))
    evidence.close()

    assert quarantine.purge_evidence("concurrent-service", int(metadata["id"])) == {"purged": 1}
    assert not path.exists()
    assert quarantine.purge_evidence("concurrent-service") == {"purged": 15}
    assert list_quarantine_evidence("concurrent-service") == []


def test_capture_failure_is_reported_to_the_ingest_caller(monkeypatch):
    def fail_storage_path(_service_id: str) -> Path:
        raise OSError("storage unavailable")

    monkeypatch.setattr(quarantine, "_evidence_dir", fail_storage_path)
    result = quarantine.capture_evidence(
        "unavailable-service",
        "request",
        "raw/unavailable.gz",
        b"bad line",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def test_capture_lock_failure_is_reported_to_the_ingest_caller(monkeypatch):
    class FailedServiceLock:
        def __enter__(self):
            raise TimeoutError("metadata lock unavailable")

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(quarantine, "_service_lock", lambda _service_id: FailedServiceLock())
    result = quarantine.capture_evidence(
        "unavailable-service",
        "request",
        "raw/unavailable.gz",
        b"bad line",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}


def test_fifo_eviction_retains_existing_evidence_when_file_unlink_fails(monkeypatch):
    monkeypatch.setattr(quarantine, "QUARANTINE_ITEM_CAP", 1)
    first = quarantine.capture_evidence(
        "unlink-service",
        "request",
        "raw/first.gz",
        b"first",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )
    first_id = int(first["id"])
    first_path = quarantine._evidence_path("unlink-service", first_id)
    real_unlink = Path.unlink

    def fail_first_item(path: Path, *args, **kwargs):
        if path == first_path:
            raise PermissionError("file is busy")
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_first_item)
    second = quarantine.capture_evidence(
        "unlink-service",
        "rum",
        "raw/second.gz",
        b"second",
        line_ordinal=None,
        byte_offset=None,
        error_category="corrupt_container",
        error_text="invalid gzip",
    )

    assert second == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert [item["original_key"] for item in list_quarantine_evidence("unlink-service")] == ["raw/first.gz"]
    assert first_path.exists()
    assert list(quarantine._evidence_dir("unlink-service").glob("*.dat")) == [first_path]


def test_purge_preserves_metadata_when_file_unlink_fails(monkeypatch):
    item = quarantine.capture_evidence(
        "failed-purge-service",
        "request",
        "raw/request/purge.gz",
        b"bad line",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )
    item_id = int(item["id"])
    path = quarantine._evidence_path("failed-purge-service", item_id)
    real_unlink = Path.unlink

    def fail_item(path_to_unlink: Path, *args, **kwargs):
        if path_to_unlink == path:
            raise PermissionError("file is busy")
        return real_unlink(path_to_unlink, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_item)

    with pytest.raises(OSError, match="could not delete quarantine evidence"):
        quarantine.purge_evidence("failed-purge-service", item_id)

    assert get_quarantine_evidence("failed-purge-service", item_id) is not None
    assert path.exists()


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"source_type": "other"}, ValueError),
        ({"payload": "not-bytes"}, TypeError),
        ({"line_ordinal": 0}, ValueError),
        ({"byte_offset": -1}, ValueError),
    ],
)
def test_capture_evidence_rejects_invalid_metadata(overrides, error):
    args = {
        "service_id": "invalid-evidence-service",
        "source_type": "request",
        "original_key": "raw/request/invalid.gz",
        "payload": b"bad",
        "line_ordinal": None,
        "byte_offset": None,
        "error_category": "invalid_json",
        "error_text": "invalid record",
    }
    args.update(overrides)

    with pytest.raises(error):
        quarantine.capture_evidence(**args)


def test_evidence_paths_reject_service_traversal():
    with pytest.raises(ValueError, match="outside the services data directory"):
        quarantine._evidence_dir("../../outside")


def test_read_evidence_rejects_metadata_paths_outside_the_evidence_directory(monkeypatch):
    monkeypatch.setattr(
        quarantine,
        "get_quarantine_evidence",
        lambda _service_id, _item_id: {"id": 17, "file_path": "../outside.dat"},
    )

    with pytest.raises(ValueError, match="invalid evidence path"):
        quarantine.read_evidence("invalid-path-service", 17)


def test_read_evidence_propagates_missing_local_file():
    result = quarantine.capture_evidence(
        "missing-file-service",
        "request",
        "raw/request/missing.gz",
        b"bad",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )
    item_id = int(result["id"])
    quarantine._evidence_path("missing-file-service", item_id).unlink()

    with pytest.raises(FileNotFoundError):
        quarantine.read_evidence("missing-file-service", item_id)


def test_capture_failure_after_metadata_insert_cleans_metadata_and_temp_file(monkeypatch):
    def fail_replace(*_args):
        raise OSError("rename failed")

    monkeypatch.setattr(quarantine.os, "replace", fail_replace)

    result = quarantine.capture_evidence(
        "rename-failure-service",
        "rum",
        "raw/rum/rename-failure.gz",
        b"bad",
        line_ordinal=1,
        byte_offset=0,
        error_category="invalid_json",
        error_text="invalid record",
    )

    assert result == {"id": 0, "cap_evictions": 0, "quarantine_capture_failures": 1}
    assert list_quarantine_evidence("rename-failure-service") == []
    assert list(quarantine._evidence_dir("rename-failure-service").iterdir()) == []


def test_purge_missing_item_is_idempotent():
    assert quarantine.purge_evidence("empty-purge-service", 987654) == {"purged": 0}
    assert get_quarantine_evidence("empty-purge-service", 987654) is None


def test_empty_evidence_id_delete_is_a_noop():
    assert delete_quarantine_evidence("empty-delete-service", []) == 0
    assert list_quarantine_evidence("empty-delete-service") == []
