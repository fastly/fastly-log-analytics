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

from backend.core import metadata_db, sqlite_migrations

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
