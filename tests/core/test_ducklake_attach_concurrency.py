"""Regression tests for the concurrent-attach DuckLake poisoning bug.

Live incident (v3.0.0-beta1 dashboard investigation, 2026-09-03): every pooled
connection built while several requests concurrently triggered a fresh
``get_connection()`` came back from ``_ducklake_attach`` reporting success
(no exception), but ``ducklake_snapshots('lake')`` on those SAME
connections then failed forever with ``Catalog "__ducklake_metadata_lake"
does not exist`` — silently poisoning the connection for the rest of its
life in the pool. Reproduced empirically: N threads each opening their own
connection and calling ``_ducklake_attach`` concurrently corrupted every
one of them; serializing the attach with a lock made all of them succeed,
every time. This pins that fix.

Also covers the companion bug in ``_update_iceberg_view_locked``: it used
to unconditionally re-attach ``lake`` with ``read_only=False`` even on a
connection where ``lake`` was already attached read-only, which throws
"database with name 'lake' already exists" — swallowed as a false
success by ``_ducklake_attach``'s broad match, but the mode-mismatched
re-attach attempt is what triggers the corruption in the first place.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from backend.core.duckdb import get_connection
from backend.core.iceberg.buffer import _commit_buffer_impl


def _make_committed_source(tmp_path, name: str) -> dict:
    """A service with one real committed row — matches the production
    scenario (an already-initialized DuckLake catalog with real
    snapshots), which is what actually raced in the live incident. A
    totally fresh, never-written-to local-file catalog has its own
    separate pre-create quirk unrelated to this bug."""
    cache = tmp_path / f"cache_{name}"
    (cache / "buffer").mkdir(parents=True)
    src = {
        "name": name,
        "service_id": name,
        "fos_local_warehouse": True,
        "_cache_dir_override": str(cache),
        "duckdb_path": str(tmp_path / f"{name}.duckdb"),
    }
    ts = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    cols = {
        "timestamp": pa.array([ts], type=pa.timestamp("us", tz="UTC")),
        "ip": pa.array(["1.2.3.4"]),
        "_source_file": pa.array(["s3://b/raw/a.gz"]),
    }
    path = os.path.join(str(cache), "buffer", "batch_a.parquet")
    pq.write_table(pa.table(cols), path)
    assert _commit_buffer_impl(src)["rows_committed"] == 1
    return src


def test_concurrent_get_connection_all_attach_successfully(tmp_path):
    """N threads racing to build a fresh read-only pooled connection for
    the same already-initialized service must ALL end up with a working
    ``lake`` catalog — not just a non-throwing ``ATTACH`` statement.

    Before the ``_attach_lock`` fix, this reproduced the corruption on
    every racing connection, every run: ``_ducklake_attach`` returned
    True for all of them, but ``ducklake_snapshots('lake')`` then failed
    with ``Catalog "__ducklake_metadata_lake" does not exist`` on all of
    them too.
    """
    name = f"race{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    n_threads = 2
    results: list[tuple[int, str, object]] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        con = get_connection(source=src, read_only=True)
        try:
            row = con.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            with lock:
                results.append((i, "ok", row))
        except Exception as e:  # noqa: BLE001 - recording the failure IS the assertion
            with lock:
                results.append((i, "fail", str(e)))
        finally:
            con.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == n_threads
    failures = [r for r in results if r[1] == "fail"]
    assert not failures, f"lake catalog corrupted on {len(failures)}/{n_threads} racing connections: {failures}"


def test_ducklake_attach_reuses_existing_matching_lake_alias(tmp_path):
    """A pooled checkout may call _ducklake_attach on a connection whose
    previous release failed to detach ``lake``. That path must verify and
    reuse the existing alias instead of issuing a second ATTACH, which can
    fatal the DuckDB connection."""
    from backend.core.iceberg._ducklake import _ducklake_attach

    name = f"reattach{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    con = get_connection(source=src, read_only=True)
    try:
        first = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert first is not None

        assert _ducklake_attach(con, src, read_only=True) is True

        second = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert second == first
    finally:
        con.close()


def test_readonly_attach_reuses_live_readwrite_attach_without_detaching(tmp_path):
    """A read-only caller must never evict a live read-write attach.

    ``duckdb.connect()`` calls against the SAME service .duckdb file share
    one underlying DuckDB instance/attach namespace — attaching ``lake``
    on one "connection" makes it visible (and, pre-fix, contestable) on
    every other connection to that same file. The pre-fix
    ``_ducklake_attach`` unconditionally detached on ANY mode mismatch, so
    a plain ``get_connection(read_only=True)`` racing a live writer
    (ingest/buffer/commit, or the startup Iceberg-view pre-warm, which
    both need ``read_only=False``) would rip the writer's in-flight attach
    out from under it — reproducing DuckDB core's own
    ``ResourceInUseException``: "Unique file handle conflict ... in the
    process of being detached". Observed in production at GCE cold start,
    surviving the process-wide ``_attach_lock`` and the retry-budget
    extension because it isn't a transient timing race — it is the reader
    itself destroying the writer's still-needed attach.

    Fix: a read-only caller that finds ``lake`` already attached
    read-write must reuse it as-is (a write-mode attach fully supports
    SELECT) instead of downgrading it. Only a genuine write caller that
    finds a read-only attach still needs to upgrade in place.
    """
    from backend.core.iceberg._ducklake import _ducklake_attach
    from backend.core.iceberg.buffer import _ducklake_write_connection

    name = f"rwreuse{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # Simulate the live writer via the SAME mechanism production uses
    # (`_commit_buffer_impl`): a dedicated connection to the service
    # .duckdb file with `lake` attached read-write, held open for the
    # duration of the "commit".
    with _ducklake_write_connection(src) as writer:
        before = writer.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
        assert before == (False,), "writer must hold a read-write lake attach"

        # A second connection to the SAME db_path (the reader) must be able
        # to attach read-only WITHOUT detaching the writer's live attach.
        reader = get_connection(source=src, read_only=True)
        try:
            after = writer.execute("SELECT readonly FROM duckdb_databases() WHERE database_name = 'lake'").fetchone()
            assert after == (False,), "reader must not have downgraded/detached the writer's live read-write attach"

            # The writer must still be fully usable after the reader raced it.
            row = writer.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            assert row is not None

            # The reader itself must also see a working (shared, read-write
            # mode) catalog rather than erroring out.
            assert _ducklake_attach(reader, src, read_only=True) is True
            reader_row = reader.execute(
                "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
            ).fetchone()
            assert reader_row == row
        finally:
            reader.close()


def test_pool_release_attempts_ducklake_internal_alias_detach():
    """Returning a pooled connection must release both the public ``lake``
    alias and DuckLake's internal metadata alias. Leaving the internal
    alias/file handle pinned caused the next checkout's ATTACH on a sibling
    connection to fail with a unique file-handle conflict."""
    from backend.core.duckdb_pool import _Pool

    class FakeConnection:
        def __init__(self):
            self.commands: list[str] = []

        def execute(self, sql: str):
            self.commands.append(sql)
            if sql == "DETACH lake":
                return self
            if sql == "DETACH __ducklake_metadata_lake":
                return self
            raise AssertionError(f"unexpected SQL: {sql}")

    pool = _Pool("fake", max_size=1)
    pool._in_use = 1
    con = FakeConnection()

    pool.release(con)  # type: ignore[arg-type]

    assert con.commands[:2] == ["DETACH lake", "DETACH __ducklake_metadata_lake"]


def test_update_iceberg_view_locked_skips_reattach_when_lake_already_attached(tmp_path, monkeypatch):
    """A connection that already has ``lake`` attached must not be
    re-attached — a mode-mismatched re-attach (read_only=False on an
    already read_only=True-attached alias) is what corrupted the
    catalog in the live incident."""
    from backend.core.iceberg import _ducklake as ducklake_mod
    from backend.core.iceberg import view as view_mod

    name = f"skip{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # get_connection() itself already attaches lake read-only for a pooled
    # connection (mirroring the real request-context path), so by the time
    # _update_iceberg_view_locked runs, lake is already attached.
    con = get_connection(source=src, read_only=True)
    try:
        attach_calls: list[bool] = []
        real_attach = ducklake_mod._ducklake_attach

        def spy_attach(con_arg, source_arg, read_only=False):
            attach_calls.append(read_only)
            return real_attach(con_arg, source_arg, read_only=read_only)

        # _update_iceberg_view_locked does `from
        # backend.core.iceberg._ducklake import _ducklake_attach` as a
        # LOCAL import inside the function body, re-binding the name on
        # every call — patching `view_mod._ducklake_attach` would never be
        # observed, so patch the source module's attribute instead.
        monkeypatch.setattr(ducklake_mod, "_ducklake_attach", spy_attach)

        # lake is already attached — _update_iceberg_view_locked must not
        # re-attach it (that mode-mismatched re-attach is what corrupted
        # the catalog in the live incident).
        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == [], "lake already attached — _ducklake_attach must not be called again"

        # Calling it again is equally a no-op on the attach front.
        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == []

        # The connection is genuinely still functional afterward.
        row = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        con.close()


def test_ducklake_detach_is_serialized_by_the_attach_lock():
    """``_ducklake_detach`` (used by pool release, commit_buffer's
    read-write reattach, and legacy adoption) must acquire the SAME
    process-wide ``_attach_lock`` that ``_ducklake_attach`` does.

    Before this fix, ``release()``/``_ducklake_write_connection()``/
    ``adopt_iceberg_to_ducklake()`` each called a raw, unlocked
    ``con.execute("DETACH lake")``. That raced with any OTHER connection
    concurrently inside a locked ``_ducklake_attach`` call and ripped
    ``lake`` out from under it mid-operation — reproduced in production as
    "Catalog Error: Schema with name lake does not exist!" commit
    failures on connections that had just successfully attached moments
    earlier. This test proves ``_ducklake_detach`` cannot run while
    another caller holds ``_attach_lock``.
    """
    from backend.core.iceberg import _ducklake as ducklake_mod

    class FakeConnection:
        def __init__(self):
            self.commands: list[str] = []

        def execute(self, sql: str):
            self.commands.append(sql)
            return self

    fake_con = FakeConnection()
    release_event = threading.Event()
    detach_started = threading.Event()
    detach_done = threading.Event()

    def holder():
        ducklake_mod._attach_lock.acquire()
        try:
            release_event.wait(timeout=5)
        finally:
            ducklake_mod._attach_lock.release()

    def detacher():
        detach_started.set()
        ducklake_mod._ducklake_detach(fake_con, aliases=("lake",), service_id="test")
        detach_done.set()

    holder_thread = threading.Thread(target=holder)
    holder_thread.start()
    try:
        # Wait until the holder thread definitely has the lock.
        for _ in range(200):
            if ducklake_mod._attach_lock.locked():
                break
            import time as _time

            _time.sleep(0.005)
        assert ducklake_mod._attach_lock.locked()

        detach_thread = threading.Thread(target=detacher)
        detach_thread.start()
        detach_started.wait(timeout=2)
        # The detach must NOT have executed its DETACH statement yet —
        # it's blocked waiting for the lock the holder thread is holding.
        assert fake_con.commands == [], "DETACH ran without waiting for the process-wide attach lock"
        assert not detach_done.is_set()

        release_event.set()
        detach_thread.join(timeout=5)
        assert detach_done.is_set()
        assert fake_con.commands == ["DETACH lake"]
    finally:
        holder_thread.join(timeout=5)


def test_attach_retry_releases_lock_during_backoff_and_succeeds(monkeypatch):
    """After a 'unique file handle conflict' (a DIFFERENT, still-busy
    connection holding the real attach open), the retry loop must:

    1. Release ``_attach_lock`` during its backoff sleep — otherwise an
       unrelated THIRD connection's attach attempt is forced to queue
       behind this one's wait for a lock it isn't even contending for.
    2. Eventually succeed once the conflict clears, using the extended
       retry budget (previously 5 attempts/~7.5s total, which production
       showed was too short for realistic ~10-20s cron-overlap windows).
    """
    from backend.core.iceberg import _ducklake as ducklake_mod

    monkeypatch.setattr(ducklake_mod, "_ATTACH_CONFLICT_RETRY_SLEEP_S", 0.2)
    monkeypatch.setattr(ducklake_mod, "_ATTACH_CONFLICT_RETRY_ATTEMPTS", 10)

    class FakeConn:
        def __init__(self):
            self.commands: list[str] = []
            self.attach_attempts = 0

        def execute(self, sql: str):
            self.commands.append(sql)
            if sql.startswith("ATTACH 'ducklake:") and " AS lake " in sql:
                self.attach_attempts += 1
                if self.attach_attempts <= 3:
                    raise RuntimeError("Unique file handle conflict: already attached by database xyz")
            return self

        def fetchone(self):
            return None

    fake_con = FakeConn()
    lock_acquired_during_sleep = threading.Event()

    def foreign_acquirer() -> None:
        # Give the retry loop a moment to hit its first conflict + sleep.
        time.sleep(0.05)
        if ducklake_mod._attach_lock.acquire(timeout=1.0):
            lock_acquired_during_sleep.set()
            ducklake_mod._attach_lock.release()

    foreign_thread = threading.Thread(target=foreign_acquirer)
    foreign_thread.start()
    try:
        result = ducklake_mod._ducklake_attach(fake_con, {"service_id": "test"}, read_only=False)
    finally:
        foreign_thread.join(timeout=5)

    assert result is True
    assert fake_con.attach_attempts == 4, "expected 3 conflicts then a succeeding 4th ATTACH attempt"
    assert lock_acquired_during_sleep.is_set(), (
        "a foreign connection could not acquire _attach_lock while this call was sleeping in its "
        "retry backoff — the lock is being held across the whole wait instead of released"
    )
    # Sanity: not held after return either.
    assert not ducklake_mod._attach_lock.locked()


def test_update_iceberg_view_locked_attaches_matching_connection_mode(tmp_path, monkeypatch):
    """When ``lake`` is NOT yet attached on the connection,
    ``_update_iceberg_view_locked`` must attach it matching the
    connection's actual read-only mode — not hardcode ``read_only=False``
    (which raises on a genuinely read-only connection and, on a
    connection where some OTHER alias attach already exists, is the
    mode-mismatched re-attach that corrupts the catalog)."""
    import duckdb

    from backend.core.iceberg import _ducklake as ducklake_mod
    from backend.core.iceberg import view as view_mod

    name = f"raw{uuid.uuid4().hex[:8]}"
    src = _make_committed_source(tmp_path, name)

    # A bare read-only connection to the same file, bypassing
    # get_connection() entirely, so lake is genuinely not attached yet.
    con = duckdb.connect(src["duckdb_path"], read_only=True)
    try:
        attach_calls: list[bool] = []
        real_attach = ducklake_mod._ducklake_attach

        def spy_attach(con_arg, source_arg, read_only=False):
            attach_calls.append(read_only)
            return real_attach(con_arg, source_arg, read_only=read_only)

        # _update_iceberg_view_locked does `from
        # backend.core.iceberg._ducklake import _ducklake_attach` as a
        # LOCAL import inside the function body, re-binding the name on
        # every call — patching `view_mod._ducklake_attach` would never be
        # observed, so patch the source module's attribute instead.
        monkeypatch.setattr(ducklake_mod, "_ducklake_attach", spy_attach)

        view_mod._update_iceberg_view_locked(con, src)
        assert attach_calls == [True], (
            f"expected a single read_only=True attach to match the RO connection, got {attach_calls}"
        )

        row = con.execute(
            "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"
        ).fetchone()
        assert row is not None
    finally:
        con.close()
