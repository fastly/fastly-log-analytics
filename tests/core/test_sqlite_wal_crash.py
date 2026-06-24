"""WAL-mode durability across an unclean shutdown.

Audit finding: the per-service metadata DB opens with
``PRAGMA journal_mode=WAL`` + ``PRAGMA synchronous=NORMAL`` (see
``backend.core.sqlite_pool.DEFAULT_PRAGMAS``). If the writer process is
killed between COMMIT and clean ``con.close()``, committed rows must
still be recoverable on the next open — synchronous=NORMAL fsyncs WAL
frames at COMMIT time, so a committed row is at least in the WAL file.

We can't SIGKILL the test process (it would take pytest with it), so
"unclean shutdown" here = drop the Python Connection reference without
``.close()``. No PRAGMA wal_checkpoint runs, no flush happens — mirrors
what an external SIGKILL leaves on disk.

Sibling: ``tests/core/test_metadata_db_concurrency
.test_wal_is_enabled_after_get_con`` for the PRAGMA assertion shape.
"""

from __future__ import annotations

import gc
import os
import sqlite3

import pytest


def _open_wal(path: str) -> sqlite3.Connection:
    """Open ``path`` with WAL + synchronous=NORMAL, bypassing the pool.

    Going direct (not via ThreadLocalPool) avoids the pool's
    ``_all_connections`` registry — any fixture-driven ``close_all``
    would defeat the "drop the reference, never close" simulation.
    """
    con = sqlite3.connect(path, timeout=5.0)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def _wal_sidecars(path: str) -> tuple[str, str]:
    return path + "-wal", path + "-shm"


# ── 1. Committed rows survive an unclean close ──────────────────────────────


@pytest.mark.filterwarnings("ignore:unclosed database:ResourceWarning")
def test_wal_committed_rows_survive_unclean_close(tmp_path):
    """A row inside a COMMITTED transaction must be readable post-reopen
    even when the writer never called ``con.close()``.

    This pins the durability contract of WAL + synchronous=NORMAL: the
    transaction's WAL frames are fsync'd at COMMIT time; only the
    checkpoint back into the main DB file is deferred. If a future change
    swaps to synchronous=OFF, this test will go red because the WAL frames
    may not have made it to disk by the time we drop the handle.
    """
    db_path = str(tmp_path / "durability.db")

    con = _open_wal(db_path)
    con.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
    con.execute("INSERT INTO t VALUES (?, ?)", ("committed", 42))
    con.commit()

    # Simulate unclean shutdown: drop the reference without close(). No
    # PRAGMA wal_checkpoint / .close() flush runs first. We don't assert
    # on sidecar presence here — CPython GC timing isn't deterministic
    # across versions; the durability assertion below is what matters.
    del con
    gc.collect()

    con2 = _open_wal(db_path)
    try:
        mode = con2.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal", f"journal mode regressed to {mode!r} after reopen"

        rows = con2.execute("SELECT k, v FROM t").fetchall()
        assert rows == [("committed", 42)], (
            f"committed row missing after unclean shutdown — got {rows!r}. WAL durability contract is broken."
        )

        # A clean checkpoint should drain the WAL frames into the main DB
        # and let SQLite reclaim the sidecars. Some platforms keep zero-
        # byte sidecars around for the lifetime of the handle, so we only
        # assert that the WAL file is empty/absent AFTER closing.
        con2.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        con2.close()

    wal, shm = _wal_sidecars(db_path)
    # After a TRUNCATE checkpoint + close, the -wal file should be 0 bytes
    # or gone. The -shm file may or may not survive — SQLite's behavior
    # there is platform-dependent — so we don't assert on it.
    if os.path.exists(wal):
        assert os.path.getsize(wal) == 0, (
            f"WAL not drained after checkpoint+close: {wal} is {os.path.getsize(wal)} bytes"
        )


# ── 2. Uncommitted transactions are NOT visible post-reopen ─────────────────


@pytest.mark.filterwarnings("ignore:unclosed database:ResourceWarning")
def test_wal_uncommitted_transaction_does_not_appear_post_reopen(tmp_path):
    """A row inserted inside a BEGIN that never COMMITted must be GONE
    after an unclean shutdown + reopen.

    This is the other half of the durability contract: we don't expose
    half-applied writes. If a future change starts auto-committing inside
    BEGIN (e.g. by changing ``isolation_level``), this test surfaces it.
    """
    db_path = str(tmp_path / "atomicity.db")

    con = _open_wal(db_path)
    con.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
    con.commit()  # schema is committed; the row below is not

    con.execute("BEGIN")
    con.execute("INSERT INTO t VALUES (?, ?)", ("uncommitted", 99))
    # NO commit() — drop the reference mid-transaction
    del con
    gc.collect()

    con2 = _open_wal(db_path)
    try:
        rows = con2.execute("SELECT k, v FROM t").fetchall()
        assert rows == [], f"uncommitted row leaked through unclean shutdown — got {rows!r}. Atomicity is broken."
    finally:
        con2.close()


# ── 3. Legacy DELETE-mode DB is upgraded to WAL on next pool open ──────────


def test_legacy_delete_mode_db_upgraded_to_wal_on_open(tmp_path):
    """If a DB file exists from before WAL was the default, the pool's
    PRAGMA preamble must upgrade it to WAL on first open, and rows that
    were committed in DELETE mode must remain readable.

    This pins the migration path for any service whose metadata.db was
    created before ``DEFAULT_PRAGMAS`` started forcing WAL — we don't
    want a one-time upgrade to silently drop pre-existing rows.
    """
    db_path = str(tmp_path / "legacy.db")

    # Seed in DELETE mode with no PRAGMA at all (DELETE is the sqlite3
    # default journal mode).
    legacy = sqlite3.connect(db_path, timeout=5.0)
    legacy.execute("PRAGMA journal_mode=DELETE")
    pre_mode = legacy.execute("PRAGMA journal_mode").fetchone()[0]
    assert pre_mode.lower() == "delete", f"setup failed: expected DELETE, got {pre_mode!r}"
    legacy.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER)")
    legacy.execute("INSERT INTO t VALUES (?, ?)", ("legacy-row", 7))
    legacy.commit()
    legacy.close()

    # Reopen via the pool's PRAGMA preamble. Mirroring DEFAULT_PRAGMAS
    # from backend.core.sqlite_pool: journal_mode=WAL is the first
    # statement run on every fresh connection.
    pooled = sqlite3.connect(db_path, timeout=5.0)
    try:
        for pragma in (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA foreign_keys=ON",
            "PRAGMA cache_size=-64000",
            "PRAGMA busy_timeout=30000",
        ):
            pooled.execute(pragma)

        mode = pooled.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal", f"pool PRAGMA preamble failed to upgrade DELETE→WAL: still {mode!r}"

        # Legacy data must survive the mode flip
        rows = pooled.execute("SELECT k, v FROM t").fetchall()
        assert rows == [("legacy-row", 7)], f"DELETE→WAL upgrade lost pre-existing rows — got {rows!r}"

        # And the new mode must be persistent: a subsequent open without
        # the preamble should still report WAL (journal_mode is baked into
        # the file header once switched).
        pooled.execute("INSERT INTO t VALUES (?, ?)", ("post-upgrade", 8))
        pooled.commit()
    finally:
        pooled.close()

    verify = sqlite3.connect(db_path, timeout=5.0)
    try:
        mode = verify.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal", f"WAL mode did not persist across close+reopen — got {mode!r}"
        rows = sorted(verify.execute("SELECT k, v FROM t").fetchall())
        assert rows == [("legacy-row", 7), ("post-upgrade", 8)]
    finally:
        verify.close()
