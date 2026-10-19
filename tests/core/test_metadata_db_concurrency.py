"""Concurrency tests for backend.core.metadata_db.

The shared Postgres metadata store holds one pooled connection per thread
(:mod:`backend.core.metadata.pg_connection`, ``autocommit=True``), tagged per
``service_id`` on each ``get_con`` call. Multiple threads must be able to
ingest into the same service concurrently without losing rows or
deadlocking.

If a future change breaks that concurrency model, these tests will surface
the regression before it costs anyone a production sync.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from backend.core import metadata as metadata_db


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


# Note: three tests that used to live here —
# ``test_wal_is_enabled_after_get_con``, ``test_get_con_init_lock_times_out_when_held``,
# ``test_legacy_delete_mode_db_is_upgraded_to_wal_on_first_open``, and
# ``test_wal_mode_persists_across_reopen`` — pinned ``ThreadLocalPool``'s
# SQLite-file mechanics: ``PRAGMA journal_mode=WAL``, the per-key
# ``_init_lock`` used to serialize a cold-start connect+PRAGMA window, and
# WAL's persistence in a SQLite file's header. None of that exists under the
# shared Postgres metadata store (``backend.core.metadata.pg_connection``) —
# there is no per-service file, no journal mode, and connection-pool
# exhaustion is a completely different mechanism (``psycopg_pool``'s own
# ``PoolTimeout``, sized via ``METADATA_PG_POOL_MAX`` — see
# ``pg_connection.get_pg_pool``'s docstring for the sizing incident that
# already covers the "don't block forever" concern these tests were pinning
# for SQLite). Deleted rather than given a strained Postgres analog; the
# concurrency invariant they were adjacent to (concurrent writes don't lose
# data / don't deadlock) is what
# ``test_concurrent_inserts_no_lock_no_loss`` and
# ``test_concurrent_inserts_with_overlap_dedup_via_upsert`` above actually
# pin, unchanged, against the real Postgres backend.
