"""Concurrency tests for backend.core.metadata_db.

The per-service SQLite layer holds the connection pool keyed by
``(thread, service_id)`` and runs in WAL + ``synchronous=NORMAL`` mode.
That should let multiple threads ingest into the same service file without
``database is locked`` errors and without losing rows.

If a future change drops WAL or introduces a long-held writer lock, these
tests will surface the regression before it costs anyone a production sync.
"""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

from backend.core import metadata as metadata_db


def test_wal_is_enabled_after_get_con():
    """Loss of WAL would re-introduce writer-vs-reader contention. Pin it."""
    sid = "svc-wal-pragma"
    con = metadata_db.get_con(sid)
    mode = con.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal", f"expected WAL journal mode, got {mode!r}"


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
    """teardown() removes the .db file (and WAL/SHM/journal). The very next
    ``get_con`` from any thread must lazily re-create the file with the
    full schema — no leftover state, no missing-table errors on first
    insert.
    """
    sid = "svc-teardown-recreate"

    # Seed something so the file definitely exists pre-teardown
    metadata_db.insert_ingested_files(sid, [("seed.gz", 1, 100)])
    db_path = metadata_db.db_path(sid)
    assert os.path.exists(db_path)

    metadata_db.teardown(sid)
    # All variants of the SQLite file must be gone
    for suffix in ("", "-wal", "-shm", "-journal"):
        assert not os.path.exists(db_path + suffix), f"{db_path + suffix} still exists after teardown"

    # Reopening must succeed (no errors) and the schema must be back —
    # i.e. the next insert hits a fully-formed ingested_files table, not
    # a "no such table" error from a stale path-initialised cache entry.
    metadata_db.insert_ingested_files(sid, [("post-teardown.gz", 2, 200)])
    con = metadata_db.get_con(sid)
    rows = con.execute("SELECT file_name FROM ingested_files").fetchall()
    names = {r[0] for r in rows}
    assert "post-teardown.gz" in names
    # The pre-teardown row must NOT survive — file was deleted
    assert "seed.gz" not in names


def test_concurrent_teardown_and_writes_no_lost_post_teardown_data():
    """If thread A calls teardown(sid) while thread B is in the middle of
    inserting, the worst acceptable outcome is: B's in-flight write may be
    lost (the file got deleted under it), BUT a subsequent get_con+insert
    by *any* thread must succeed and persist correctly. No deadlock, no
    persistent broken state.
    """
    sid = "svc-teardown-race"

    # Seed once so the file exists
    metadata_db.insert_ingested_files(sid, [("warmup.gz", 1, 100)])

    def writer() -> None:
        try:
            metadata_db.insert_ingested_files(sid, [(f"writer-{i}.gz", 1, 100) for i in range(20)])
        except Exception:
            # Acceptable: the file may have been removed mid-write by the
            # tearer-down. We only assert recovery below.
            pass

    def tearer() -> None:
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


def test_get_con_init_lock_times_out_when_held(monkeypatch):
    """``_init_lock`` must NOT block forever — a stuck thread inside the
    connect+PRAGMA window once wedged every other cron tick for 10+ minutes
    (incident 2026-05-21). With a 10s acquire timeout, the caller now sees
    a clean ``OperationalError`` and the swallowing try/except up the stack
    keeps the rest of the cron alive.
    """
    import sqlite3
    import threading
    import time

    # Wrap the lock to fail fast on timeout=10
    real_lock = metadata_db._init_lock

    class QuickTimeoutLock:
        def acquire(self, blocking=True, timeout=-1):
            if timeout == 10:
                timeout = 0.05
            return real_lock.acquire(blocking, timeout)

        def release(self):
            real_lock.release()

        def __enter__(self):
            return real_lock.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return real_lock.__exit__(exc_type, exc_val, exc_tb)

    monkeypatch.setattr(metadata_db, "_init_lock", QuickTimeoutLock())

    # Acquire _init_lock from another thread and hold it. A test-thread
    # acquire+release of an RLock would simply pass through reentrantly.
    holder_acquired = threading.Event()
    holder_release = threading.Event()

    def _hold_lock() -> None:
        with real_lock:
            holder_acquired.set()
            holder_release.wait(timeout=30)

    holder = threading.Thread(target=_hold_lock, name="init-lock-holder", daemon=True)
    holder.start()
    try:
        assert holder_acquired.wait(timeout=5), "holder thread never acquired _init_lock"

        # New thread + brand-new service id => guaranteed to miss the
        # thread-local pool and hit the _init_lock acquire path.
        sid = "svc-init-lock-timeout"
        result: dict = {}

        def _try_get() -> None:
            start = time.monotonic()
            try:
                metadata_db.get_con(sid)
                result["ok"] = True
            except sqlite3.OperationalError as e:
                result["err"] = str(e)
            finally:
                result["elapsed"] = time.monotonic() - start

        contender = threading.Thread(target=_try_get, name="init-lock-contender", daemon=True)
        contender.start()
        contender.join(timeout=15)

        assert not contender.is_alive(), "contender did not return within 15s — _init_lock acquire is unbounded"
        assert "err" in result, f"expected OperationalError; got result={result}"
        assert "_init_lock contended" in result["err"], (
            f"error message must name the lock for debuggability; got {result['err']!r}"
        )
        # Must fire near the 0.05s timeout, not the SQLite 30s timeout or full 10s.
        assert result["elapsed"] <= 2.0, (
            f"acquire fired at {result['elapsed']:.2f}s — expected to be very fast under mock timeout."
        )
    finally:
        holder_release.set()
        holder.join(timeout=5)


# ── journal-mode mismatch on legacy DB (audit follow-up) ────────────────────


def test_legacy_delete_mode_db_is_upgraded_to_wal_on_first_open():
    """A pre-existing service DB file in journal_mode=DELETE (the SQLite
    default before our pool started forcing WAL) must be upgraded to
    WAL the first time the pool opens it. Pinned because the upgrade
    is silent — without this test a regression that dropped the pragma
    would re-introduce writer-vs-reader contention on any service whose
    metadata.db was created before the WAL switchover.

    Uses the autouse ``isolate_metadata_db`` sandbox via
    ``metadata.base.db_path()`` rather than a tmp_path override.
    """
    import sqlite3

    from backend.core.metadata import base as metadata_base

    sid = "svc-legacy-upgrade"
    legacy_path = metadata_base.db_path(sid)

    # 1. Build a legacy DB file in DELETE mode and seed it with a row.
    legacy = sqlite3.connect(legacy_path)
    try:
        legacy.execute("PRAGMA journal_mode = DELETE")
        before = legacy.execute("PRAGMA journal_mode").fetchone()[0]
        assert before.lower() == "delete", f"setup failed: legacy mode is {before!r}"
        legacy.execute("CREATE TABLE legacy_marker (id INTEGER)")
        legacy.execute("INSERT INTO legacy_marker VALUES (1)")
        legacy.commit()
    finally:
        legacy.close()

    # 2. Open via the pool — must upgrade DELETE → WAL.
    con = metadata_db.get_con(sid)
    mode = con.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal", f"pool did not upgrade legacy DELETE → WAL; got {mode!r}"

    # 3. Legacy row must still be readable (no data loss in the upgrade).
    # The pool's connection uses sqlite3.Row factory — coerce to tuples
    # for the comparison.
    rows = [tuple(r) for r in con.execute("SELECT id FROM legacy_marker").fetchall()]
    assert rows == [(1,)], f"legacy row lost across WAL upgrade: {rows!r}"


def test_wal_mode_persists_across_reopen():
    """journal_mode = WAL is baked into the SQLite file header once switched
    — pin that the persisted value survives a raw sqlite3.connect that
    issues no pragmas, proving the pool's WAL pragma persisted to the
    file header (not just to the in-memory connection).
    """
    import sqlite3

    from backend.core.metadata import base as metadata_base

    sid = "svc-wal-persist"
    db_path = metadata_base.db_path(sid)

    # First open via the pool → WAL.
    con = metadata_db.get_con(sid)
    first_mode = con.execute("PRAGMA journal_mode").fetchone()[0]
    assert first_mode.lower() == "wal"

    # Open the same file with raw sqlite3 (no pragmas applied) — the
    # header alone should report WAL.
    raw = sqlite3.connect(db_path)
    try:
        raw_mode = raw.execute("PRAGMA journal_mode").fetchone()[0]
        assert raw_mode.lower() == "wal", f"WAL did not persist in file header — raw open reports {raw_mode!r}."
    finally:
        raw.close()
