"""Self-healing DuckDB connection wrapper.

Pins the contract: on file-level corruption the wrapper deletes the
offending file and reopens an empty database. On query errors it does
NOT delete anything — that would mask real bugs.

See backend/core/duckdb.py::get_safe_duckdb_connection.
"""

from __future__ import annotations

import os

import duckdb
import pytest

from backend.core.duckdb import _is_corruption_error, get_safe_duckdb_connection


def test_random_bytes_file_is_replaced_with_empty_db(tmp_path):
    """A file full of garbage triggers recovery; caller gets a working connection."""
    db_path = str(tmp_path / "corrupt.duckdb")
    with open(db_path, "wb") as f:
        f.write(b"NOT A VALID DUCKDB FILE\x00\x01\x02" * 4096)

    original_size = os.path.getsize(db_path)

    with get_safe_duckdb_connection(db_path) as con:
        rows = con.execute("SELECT 1").fetchone()
        assert rows == (1,)
        # The fresh DB should be usable for arbitrary DDL/DML
        con.execute("CREATE TABLE t(x INTEGER)")
        con.execute("INSERT INTO t VALUES (42)")
        assert con.execute("SELECT x FROM t").fetchone() == (42,)

    # File was replaced — should now be a real (small) DuckDB file
    assert os.path.exists(db_path)
    assert os.path.getsize(db_path) != original_size


def test_missing_file_is_created(tmp_path):
    """No file at the path — DuckDB just creates it, no recovery needed."""
    db_path = str(tmp_path / "fresh.duckdb")
    assert not os.path.exists(db_path)

    with get_safe_duckdb_connection(db_path) as con:
        con.execute("SELECT 1").fetchone()

    assert os.path.exists(db_path)


def test_good_file_round_trips(tmp_path):
    """Recovery path must not fire for healthy databases."""
    db_path = str(tmp_path / "good.duckdb")
    with get_safe_duckdb_connection(db_path) as con:
        con.execute("CREATE TABLE t(x INTEGER)")
        con.execute("INSERT INTO t VALUES (1), (2), (3)")

    # Re-open and read back — data must survive
    with get_safe_duckdb_connection(db_path) as con:
        rows = con.execute("SELECT x FROM t ORDER BY x").fetchall()
        assert rows == [(1,), (2,), (3,)]


def test_query_errors_are_not_swallowed(tmp_path):
    """Binder/Parser errors propagate; the wrapper only handles open-time corruption."""
    db_path = str(tmp_path / "ok.duckdb")
    with get_safe_duckdb_connection(db_path) as con:
        with pytest.raises(duckdb.Error):
            con.execute("SELECT no_such_column FROM no_such_table").fetchone()

    # File should still exist and be reusable — wrapper must not have deleted it
    assert os.path.exists(db_path)
    with get_safe_duckdb_connection(db_path) as con:
        assert con.execute("SELECT 1").fetchone() == (1,)


def test_read_only_corruption_raises_not_recovers(tmp_path):
    """Read-only callers must NOT auto-delete — let the writer side repair."""
    db_path = str(tmp_path / "corrupt_ro.duckdb")
    with open(db_path, "wb") as f:
        f.write(b"GARBAGE" * 4096)

    with pytest.raises(duckdb.Error):
        with get_safe_duckdb_connection(db_path, read_only=True) as con:
            con.execute("SELECT 1").fetchone()

    # File must still exist — read-only path must not have nuked it
    assert os.path.exists(db_path)


def test_is_corruption_error_only_matches_file_damage():
    """Negative case: query-time exceptions are not corruption."""
    # Build a real BinderException by running an invalid query
    con = duckdb.connect(":memory:")
    try:
        try:
            con.execute("SELECT * FROM nope").fetchone()
        except Exception as e:
            assert not _is_corruption_error(e), (
                f"BinderException must not be classified as corruption: {type(e).__name__}"
            )
    finally:
        con.close()


def test_wal_sidecar_is_also_removed(tmp_path):
    """Recovery must clean up the .wal sidecar so the next open is clean."""
    db_path = str(tmp_path / "with_wal.duckdb")
    wal_path = db_path + ".wal"

    # Seed a corrupt main file AND a stale .wal
    with open(db_path, "wb") as f:
        f.write(b"NOT A VALID DUCKDB FILE" * 4096)
    with open(wal_path, "wb") as f:
        f.write(b"stale wal junk" * 1024)

    with get_safe_duckdb_connection(db_path) as con:
        con.execute("SELECT 1").fetchone()

    # WAL must be gone (or rewritten by DuckDB to a benign empty form)
    if os.path.exists(wal_path):
        assert b"stale wal junk" not in open(wal_path, "rb").read()
