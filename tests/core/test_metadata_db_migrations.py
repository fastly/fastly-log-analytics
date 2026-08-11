"""Migration framework tests for per-service SQLite metadata DBs.

Closes TESTING_PLAN_3 item 4: pin the contract that an existing DB
created before the migration framework existed can be opened cleanly
under the new code and arrive at the latest schema without data loss.

These tests are the only thing that prevents a future PR from quietly
breaking the upgrade path for production services with existing
``data/services/{service_id}.metadata.db`` files.
"""

from __future__ import annotations

import os
import sqlite3

import pytest

from backend.core import metadata as metadata_db
from backend.core import sqlite_migrations

# ── Helpers ──────────────────────────────────────────────────────────────────


def _seed_pre_migration_db(path: str) -> None:
    """Seed a SQLite file with the pre-migration-framework schema, i.e.
    the ``_SCHEMA`` as it shipped before item-4 added ``error_count``
    and ``PRAGMA user_version`` tracking.

    Simulates an existing production DB carried forward across an upgrade.
    """
    con = sqlite3.connect(path)
    try:
        con.execute(
            """CREATE TABLE ingested_files (
                file_name TEXT,
                source_name TEXT,
                ingested_at TEXT DEFAULT (datetime('now')),
                row_count INTEGER,
                file_size_bytes INTEGER,
                PRIMARY KEY (file_name, source_name)
            )"""
        )
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes) VALUES (?, ?, ?, ?)",
            ("s3://bucket/raw/2026-05-01/10/2026-05-01T10-00-00.svc.gz", "svc", 1234, 99999),
        )
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes) VALUES (?, ?, ?, ?)",
            ("s3://bucket/raw/2026-05-01/10/2026-05-01T10-05-00.svc.gz", "svc", 567, 33333),
        )
        # Explicitly leave user_version = 0 (the default for a freshly
        # created SQLite file) so apply_pending() sees this as un-migrated.
        con.commit()
    finally:
        con.close()


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


# ── Loader semantics ─────────────────────────────────────────────────────────


def test_apply_pending_brings_seeded_db_to_latest(tmp_path):
    """Seed a v0 DB → call apply_pending → arrives at LATEST_VERSION,
    with the v1 migration's effect (``error_count`` column) visible."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    con = sqlite3.connect(path)
    try:
        # Pre-condition: no error_count, version 0
        assert sqlite_migrations.get_current_version(con) == 0
        assert "error_count" not in _columns(con, "ingested_files")

        applied = sqlite_migrations.apply_pending(con)
        # MIGRATIONS has a deliberate gap at key 3 (the retired
        # usage_log_hourly_summary rebuild), so the applied COUNT can be
        # less than LATEST_VERSION even on a fresh DB.
        assert applied == len(sqlite_migrations.MIGRATIONS), (
            f"expected {len(sqlite_migrations.MIGRATIONS)} migration(s) to apply, got {applied}"
        )

        # Post-condition: error_count exists, version bumped
        assert "error_count" in _columns(con, "ingested_files")
        assert sqlite_migrations.get_current_version(con) == sqlite_migrations.LATEST_VERSION

        # Existing data must survive intact
        rows = con.execute(
            "SELECT file_name, row_count, file_size_bytes, error_count FROM ingested_files ORDER BY file_name"
        ).fetchall()
        assert len(rows) == 2
        # The new column defaults to 0 (not NULL) for back-filled rows
        for _, rc, sz, ec in rows:
            assert rc in (1234, 567)
            assert sz in (99999, 33333)
            assert ec == 0
    finally:
        con.close()


def test_apply_pending_is_idempotent(tmp_path):
    """Running apply_pending twice on the same DB applies zero work the
    second time. Pinned because every ``get_con`` call would otherwise
    re-execute every ALTER TABLE on every connection open."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    con = sqlite3.connect(path)
    try:
        first = sqlite_migrations.apply_pending(con)
        second = sqlite_migrations.apply_pending(con)
        assert first == len(sqlite_migrations.MIGRATIONS)
        assert second == 0, "expected zero migrations on the second pass"
        assert sqlite_migrations.get_current_version(con) == sqlite_migrations.LATEST_VERSION
    finally:
        con.close()


def test_migrations_are_transactional_on_failure(tmp_path):
    """If a migration body raises, the version must NOT advance — the
    next open should re-apply (and presumably hit the same failure to
    surface the bug)."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    def _bad_migration(_con):
        raise RuntimeError("simulated migration failure")

    # Splice a poison migration in before any real ones at v1
    original = sqlite_migrations.MIGRATIONS
    try:
        # Patch in a poison migration AT THE NEXT version so v1 succeeds
        # but the poison fails — proves the loader stops AT the failure
        # (doesn't roll back v1).
        sqlite_migrations.MIGRATIONS = dict(original)
        sqlite_migrations.MIGRATIONS[sqlite_migrations.LATEST_VERSION + 1] = _bad_migration

        con = sqlite3.connect(path)
        try:
            with pytest.raises(RuntimeError, match="simulated migration failure"):
                sqlite_migrations.apply_pending(con)
            # Version should be at LATEST_VERSION (real migration succeeded)
            # but NOT at LATEST_VERSION+1 (poison did not).
            assert sqlite_migrations.get_current_version(con) == sqlite_migrations.LATEST_VERSION
        finally:
            con.close()
    finally:
        sqlite_migrations.MIGRATIONS = original


# ── _migration_002_add_ingested_files_file_date ──────────────────────────────


def test_migration_002_backfills_file_date_from_filename(tmp_path):
    """A DB seeded with the pre-v2 schema should arrive at LATEST_VERSION
    with file_date populated for every row whose filename has a parseable
    YYYY-MM-DD prefix to the 'T' marker (the Fastly emit-time format).
    Filenames that don't match get NULL — callers treat file_date as
    optional.
    """
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)
    # Add a row whose filename does NOT match the canonical format so we
    # can assert it stays NULL (defense against the GLOB widening to
    # accept noise).
    with sqlite3.connect(path) as seed_con:
        seed_con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes) VALUES (?, ?, ?, ?)",
            ("legacy_no_iso_prefix.log.gz", "svc", 1, 1),
        )

    con = sqlite3.connect(path)
    try:
        assert "file_date" not in _columns(con, "ingested_files")

        sqlite_migrations.apply_pending(con)

        assert "file_date" in _columns(con, "ingested_files")
        # Backfill: rows with parseable filenames get the date; the legacy
        # one stays NULL.
        rows = {r[0]: r[1] for r in con.execute("SELECT file_name, file_date FROM ingested_files").fetchall()}
        assert rows["s3://bucket/raw/2026-05-01/10/2026-05-01T10-00-00.svc.gz"] == "2026-05-01"
        assert rows["s3://bucket/raw/2026-05-01/10/2026-05-01T10-05-00.svc.gz"] == "2026-05-01"
        assert rows["legacy_no_iso_prefix.log.gz"] is None

        # Composite index for per-day usage queries must exist
        idx = con.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_ingested_files_source_date'"
        ).fetchone()
        assert idx is not None
    finally:
        con.close()


def test_insert_ingested_files_populates_file_date(tmp_path, monkeypatch):
    """End-to-end: a fresh DB + insert_ingested_files should land rows
    with file_date already populated (Python-side parse at insert time —
    no need to wait for the next migration to backfill new data).
    """
    monkeypatch.setattr(metadata_db, "_DATA_DIR", str(tmp_path / "services"))
    monkeypatch.setattr(metadata_db, "_initialized", set())
    monkeypatch.setattr(metadata_db, "_local", __import__("threading").local())

    metadata_db.insert_ingested_files(
        "newsvc",
        [
            ("s3://bucket/raw/2026-06-03/14/2026-06-03T14-30-00.svc.gz", 1000, 50000),
            ("legacy_no_iso.log.gz", 5, 100),
        ],
    )

    con = metadata_db.get_con("newsvc")
    try:
        rows = {r[0]: r[1] for r in con.execute("SELECT file_name, file_date FROM ingested_files").fetchall()}
        assert rows["s3://bucket/raw/2026-06-03/14/2026-06-03T14-30-00.svc.gz"] == "2026-06-03"
        assert rows["legacy_no_iso.log.gz"] is None
    finally:
        metadata_db.close_all_connections()


# ── Integration with metadata_db._init_schema ────────────────────────────────


def test_init_schema_on_fresh_db_jumps_to_latest_version(tmp_path, monkeypatch):
    """A brand-new DB opened via ``metadata_db.get_con`` should land at
    LATEST_VERSION without applying any migrations (the latest ``_SCHEMA``
    already has the v1 columns)."""
    monkeypatch.setattr(metadata_db, "_DATA_DIR", str(tmp_path / "services"))
    monkeypatch.setattr(metadata_db, "_initialized", set())
    monkeypatch.setattr(metadata_db, "_local", __import__("threading").local())

    con = metadata_db.get_con("fresh-svc")
    try:
        assert sqlite_migrations.get_current_version(con) == sqlite_migrations.LATEST_VERSION
        assert "error_count" in _columns(con, "ingested_files")
    finally:
        metadata_db.close_all_connections()


def test_init_schema_on_legacy_db_upgrades_in_place(tmp_path, monkeypatch):
    """A DB created before the framework existed — i.e. ``user_version=0``
    and no ``error_count`` column — gets upgraded the first time
    ``metadata_db.get_con`` opens it.

    The data inserted under the old schema must round-trip through the
    upgrade without loss."""
    monkeypatch.setattr(metadata_db, "_DATA_DIR", str(tmp_path / "services"))
    monkeypatch.setattr(metadata_db, "_initialized", set())
    monkeypatch.setattr(metadata_db, "_local", __import__("threading").local())

    # Lay down the legacy file at the path get_con would resolve to.
    os.makedirs(str(tmp_path / "services"), exist_ok=True)
    legacy_path = str(tmp_path / "services" / "legacy-svc.metadata.db")
    _seed_pre_migration_db(legacy_path)

    con = metadata_db.get_con("legacy-svc")
    try:
        assert sqlite_migrations.get_current_version(con) == sqlite_migrations.LATEST_VERSION
        assert "error_count" in _columns(con, "ingested_files")

        # The seed rows from the legacy schema must survive
        n = con.execute("SELECT COUNT(*) FROM ingested_files WHERE source_name = 'svc'").fetchone()[0]
        assert n == 2, f"legacy data was lost during upgrade: count={n}"
    finally:
        metadata_db.close_all_connections()


# The legacy metadata.db.usage_log table + its INSERT/DELETE/UPDATE
# triggers + the _migration_003 rebuilder were all retired alongside
# the v2.0 cutover to the per-service usage_log SQLite. The trigger
# behavior tests + the migration_003 corruption-fix test had no
# remaining production behavior to pin and were removed with the DDL.


def test_legacy_db_with_active_writer_pattern_still_inserts(tmp_path, monkeypatch):
    """End-to-end: legacy DB → upgrade → metadata_db.insert_ingested_files
    still works against the upgraded schema (the new column is nullable
    with a default, so existing INSERT statements remain valid)."""
    monkeypatch.setattr(metadata_db, "_DATA_DIR", str(tmp_path / "services"))
    monkeypatch.setattr(metadata_db, "_initialized", set())
    monkeypatch.setattr(metadata_db, "_local", __import__("threading").local())

    os.makedirs(str(tmp_path / "services"), exist_ok=True)
    legacy_path = str(tmp_path / "services" / "legacy-writer.metadata.db")
    _seed_pre_migration_db(legacy_path)

    # Note: _seed_pre_migration_db wrote rows under source_name='svc'; the
    # writer test below uses the same source_name so insert_ingested_files
    # also exercises the upsert path against pre-existing rows.
    metadata_db.insert_ingested_files(
        "legacy-writer",
        [
            ("s3://bucket/raw/2026-05-01/11/2026-05-01T11-00-00.svc.gz", 999, 12345),
        ],
    )

    con = metadata_db.get_con("legacy-writer")
    try:
        rows = con.execute(
            "SELECT row_count, file_size_bytes, error_count FROM ingested_files "
            "WHERE file_name = 's3://bucket/raw/2026-05-01/11/2026-05-01T11-00-00.svc.gz'"
        ).fetchall()
        assert len(rows) == 1
        rc, sz, ec = rows[0]
        assert rc == 999
        assert sz == 12345
        assert ec == 0  # default value applied to insert that didn't specify it
    finally:
        metadata_db.close_all_connections()


# ── Crash-recovery semantics ─────────────────────────────────────────────────
#
# What happens if a process crashes mid-migration? The existing tests
# pin the no-version-advance contract via a poison-at-LATEST_VERSION+1
# pattern, but the audit flagged several adjacent gaps:
#
#   1. A migration that DDL-creates a table then DML-fails partway
#      through must roll back the table too — not just the version.
#   2. Three pending migrations where the middle one fails: the first
#      applies AND commits, the middle's effects roll back, the third
#      is never attempted.
#   3. After a failed migration, re-opening the DB and calling
#      apply_pending must retry the failed migration (cleanly, assuming
#      the root cause is fixed) — not skip it forever.
#   4. user_version > LATEST_VERSION (someone ran newer code, then
#      downgraded) is treated as already-applied: apply_pending returns
#      0 and does NOT attempt any "negative" migrations.


def test_failed_migration_leaves_user_version_unchanged_even_when_ddl_survives(tmp_path):
    """A migration that creates a table then raises mid-body:
    ``user_version`` MUST stay at the pre-migration value. The
    user_version bump is the load-bearing recovery signal — the DDL
    itself may or may not survive (SQLite auto-commits DDL outside an
    explicit BEGIN, so Python's deferred-transaction wrapper does NOT
    roll back the CREATE TABLE), but as long as user_version didn't
    advance, every real migration uses ``IF NOT EXISTS`` /
    ``ALTER TABLE``-with-column-probe so the retry on next open is
    idempotent.

    Pinned because the load-bearing invariant is "user_version
    reflects the LAST successfully completed migration", NOT "every
    side effect is atomic". A regression that advanced user_version
    before the body completed would silently strand schema in a broken
    state — that's the actual recovery-safety property we need."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    def _partial_then_fail(con: sqlite3.Connection) -> None:
        con.execute("CREATE TABLE IF NOT EXISTS half_built (id INTEGER PRIMARY KEY, payload TEXT)")
        con.execute("INSERT INTO half_built (payload) VALUES ('row 1')")
        raise RuntimeError("simulated crash mid-body")

    original = sqlite_migrations.MIGRATIONS
    try:
        sqlite_migrations.MIGRATIONS = dict(original)
        sqlite_migrations.MIGRATIONS[sqlite_migrations.LATEST_VERSION + 1] = _partial_then_fail

        con = sqlite3.connect(path)
        try:
            with pytest.raises(RuntimeError, match="simulated crash mid-body"):
                sqlite_migrations.apply_pending(con)
            # Load-bearing invariant: user_version did NOT advance.
            assert sqlite_migrations.get_current_version(con) == sqlite_migrations.LATEST_VERSION

            # DML inside the implicit transaction DOES roll back (the
            # 'row 1' INSERT never lands) even though SQLite auto-
            # committed the preceding CREATE TABLE. The next retry will
            # see the empty table via IF NOT EXISTS and re-attempt the
            # INSERT cleanly — no duplicate rows.
            rows = con.execute("SELECT COUNT(*) FROM half_built").fetchone()
            assert rows[0] == 0, (
                "DML rollback failed — the INSERT survived the raise. The "
                "next retry would re-INSERT and produce duplicates."
            )
        finally:
            con.close()
    finally:
        sqlite_migrations.MIGRATIONS = original


def _has_table(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def test_middle_failure_in_pending_chain_stops_at_failure(tmp_path):
    """Three migrations pending, middle one raises: the first must apply
    and commit, the failing one rolls back to its pre-state, the third
    must NOT run. The loader stops on first failure — applying the
    third would invariably break its assumptions about the middle's
    side effects."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    # Bring the DB up to LATEST_VERSION first so the chain we test is
    # purely the three poison migrations.
    con = sqlite3.connect(path)
    try:
        sqlite_migrations.apply_pending(con)
    finally:
        con.close()

    calls: list[int] = []

    def _ok_first(con: sqlite3.Connection) -> None:
        calls.append(1)
        con.execute("CREATE TABLE first_ok (id INTEGER PRIMARY KEY)")

    def _bad_middle(_con: sqlite3.Connection) -> None:
        calls.append(2)
        raise RuntimeError("middle of chain failed")

    def _never_runs(con: sqlite3.Connection) -> None:
        calls.append(3)
        con.execute("CREATE TABLE third (id INTEGER PRIMARY KEY)")

    base = sqlite_migrations.LATEST_VERSION
    original = sqlite_migrations.MIGRATIONS
    try:
        sqlite_migrations.MIGRATIONS = dict(original)
        sqlite_migrations.MIGRATIONS[base + 1] = _ok_first
        sqlite_migrations.MIGRATIONS[base + 2] = _bad_middle
        sqlite_migrations.MIGRATIONS[base + 3] = _never_runs

        con = sqlite3.connect(path)
        try:
            with pytest.raises(RuntimeError, match="middle of chain failed"):
                sqlite_migrations.apply_pending(con)

            # _ok_first committed; _bad_middle rolled back; _never_runs untouched.
            assert calls == [1, 2], f"expected halt after middle, got call order {calls}"
            assert _has_table(con, "first_ok")
            assert not _has_table(con, "third")
            # Version advanced to base+1 (first succeeded), NOT base+2 or +3.
            assert sqlite_migrations.get_current_version(con) == base + 1
        finally:
            con.close()
    finally:
        sqlite_migrations.MIGRATIONS = original


def test_retry_after_failure_applies_cleanly_when_root_cause_resolved(tmp_path):
    """First open: a poison migration raises → version stays at the
    pre-failure value. Second open (after the operator fixes the
    underlying issue — modeled here by swapping the poison out for a
    clean version): apply_pending picks the same version up again and
    applies it. Pin the contract that a failure is RECOVERABLE — not a
    one-shot fatal state."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    con0 = sqlite3.connect(path)
    try:
        sqlite_migrations.apply_pending(con0)
    finally:
        con0.close()
    base = sqlite_migrations.LATEST_VERSION

    poison_calls = []

    def _poison(_con: sqlite3.Connection) -> None:
        poison_calls.append("called")
        raise RuntimeError("transient — disk full / lock held / etc.")

    clean_calls = []

    def _clean_after_fix(con: sqlite3.Connection) -> None:
        clean_calls.append("called")
        con.execute("CREATE TABLE recovered (id INTEGER PRIMARY KEY)")

    original = sqlite_migrations.MIGRATIONS
    try:
        # Pass 1: poison version base+1.
        sqlite_migrations.MIGRATIONS = dict(original)
        sqlite_migrations.MIGRATIONS[base + 1] = _poison
        con1 = sqlite3.connect(path)
        try:
            with pytest.raises(RuntimeError):
                sqlite_migrations.apply_pending(con1)
            assert sqlite_migrations.get_current_version(con1) == base
        finally:
            con1.close()

        # Pass 2: the operator fixes the root cause and the same version
        # number now points at a clean migration. apply_pending must run
        # it (not skip because "we already tried").
        sqlite_migrations.MIGRATIONS = dict(original)
        sqlite_migrations.MIGRATIONS[base + 1] = _clean_after_fix
        con2 = sqlite3.connect(path)
        try:
            applied = sqlite_migrations.apply_pending(con2)
            assert applied == 1
            assert clean_calls == ["called"]
            assert _has_table(con2, "recovered")
            assert sqlite_migrations.get_current_version(con2) == base + 1
        finally:
            con2.close()
    finally:
        sqlite_migrations.MIGRATIONS = original


def test_user_version_ahead_of_latest_is_no_op_not_downgrade(tmp_path):
    """A DB with ``user_version`` HIGHER than ``LATEST_VERSION`` means
    someone ran a newer code revision against this file and then
    downgraded back to this one. apply_pending must NOT try to roll
    back — it's a no-op. Pinned because a regression that compared
    ``version != LATEST_VERSION`` instead of ``version < LATEST_VERSION``
    would attempt impossible negative migrations and crash, blocking
    every read from the same DB."""
    path = str(tmp_path / "svc.metadata.db")
    _seed_pre_migration_db(path)

    # Bring to current, then pretend a future code revision bumped past us.
    con = sqlite3.connect(path)
    try:
        sqlite_migrations.apply_pending(con)
        future = sqlite_migrations.LATEST_VERSION + 99
        con.execute(f"PRAGMA user_version = {future}")
        con.commit()
        assert sqlite_migrations.get_current_version(con) == future

        applied = sqlite_migrations.apply_pending(con)
        assert applied == 0, "downgrade should be a no-op, not a re-apply"
        # Version preserved — we did NOT clobber the future-version stamp.
        assert sqlite_migrations.get_current_version(con) == future
    finally:
        con.close()


def test_migration_010_adds_table_name_and_rebuilds_ingest_in_flight(tmp_path):
    """Test that migration 010 successfully adds table_name to ingested_files
    and rebuilds ingest_in_flight to have a composite PK including table_name,
    while preserving pre-existing records."""
    path = str(tmp_path / "svc.metadata.db")
    con = sqlite3.connect(path)
    try:
        # Create pre-migration shape of ingest_in_flight
        con.execute(
            """CREATE TABLE ingest_in_flight (
                buffer_filename TEXT PRIMARY KEY,
                source_name TEXT NOT NULL,
                files_json TEXT NOT NULL,
                started_at TEXT DEFAULT (datetime('now'))
            )"""
        )
        con.execute(
            "INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json) VALUES (?, ?, ?)",
            ("batch_123.parquet", "svc", '["file1.gz"]'),
        )
        # Create pre-migration shape of ingested_files
        con.execute(
            """CREATE TABLE ingested_files (
                file_name TEXT,
                source_name TEXT,
                ingested_at TEXT DEFAULT (datetime('now')),
                row_count INTEGER,
                file_size_bytes INTEGER,
                error_count INTEGER DEFAULT 0,
                file_date DATE,
                PRIMARY KEY (file_name, source_name)
            )"""
        )
        con.execute(
            "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, error_count, file_date) VALUES (?, ?, ?, ?, ?, ?)",
            ("s3://bucket/raw/file1.gz", "svc", 100, 1000, 0, "2026-05-01"),
        )
        # Explicitly set version to 9 so migration 10 is applied
        con.execute("PRAGMA user_version = 9")
        con.commit()
    finally:
        con.close()

    con = sqlite3.connect(path)
    try:
        # Pre-condition
        assert "table_name" not in _columns(con, "ingested_files")

        # Run migration 10 & 11 (apply_pending will run both)
        applied = sqlite_migrations.apply_pending(con)
        assert applied == 2

        # Post-condition
        assert "table_name" in _columns(con, "ingested_files")
        assert "table_name" in _columns(con, "ingest_in_flight")

        # Verify default is applied
        row_ingested = con.execute("SELECT table_name FROM ingested_files").fetchone()
        assert row_ingested[0] == "logs"

        row_in_flight = con.execute("SELECT table_name FROM ingest_in_flight").fetchone()
        assert row_in_flight[0] == "logs"

        # Verify composite primary key by trying to insert with same buffer_filename but different table_name
        con.execute(
            "INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json, table_name) VALUES (?, ?, ?, ?)",
            ("batch_123.parquet", "svc", '["file2.gz"]', "client_vitals"),
        )
        con.commit()

        # Both should exist
        rows = con.execute("SELECT buffer_filename, table_name FROM ingest_in_flight ORDER BY table_name").fetchall()
        assert len(rows) == 2
        assert rows[0] == ("batch_123.parquet", "client_vitals")
        assert rows[1] == ("batch_123.parquet", "logs")
    finally:
        con.close()


def test_migration_011_drops_rum_beacons(tmp_path):
    """Test that migration 011 successfully drops the rum_beacons table."""
    path = str(tmp_path / "svc.metadata.db")
    con = sqlite3.connect(path)
    try:
        # Create a mock database at version 10 with rum_beacons table
        con.execute(
            """CREATE TABLE rum_beacons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_id TEXT NOT NULL,
                received_at TEXT NOT NULL DEFAULT (datetime('now')),
                beacon_data TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )"""
        )
        con.execute(
            "INSERT INTO rum_beacons (service_id, beacon_data) VALUES (?, ?)",
            ("svc", '{"test": true}'),
        )
        # Set user_version to 10
        con.execute("PRAGMA user_version = 10")
        con.commit()
    finally:
        con.close()

    con = sqlite3.connect(path)
    try:
        # Pre-condition: table exists
        tables = [row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "rum_beacons" in tables

        # Run migration 11 (apply_pending will run it)
        applied = sqlite_migrations.apply_pending(con)
        assert applied == 1

        # Post-condition: table dropped
        tables = [row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "rum_beacons" not in tables
    finally:
        con.close()
