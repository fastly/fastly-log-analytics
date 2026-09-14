"""Tests for trigger migrations, small cache self-healing, and usage_log optimizations."""

from __future__ import annotations

import sqlite3
import threading
from unittest.mock import MagicMock

from backend.core.metadata import slow_queries, usage_log_db
from backend.core.metadata.usage_log import clear_usage_log
from backend.core.sqlite_pool import open_small_cache_db


def test_usage_log_trigger_migration(tmp_path, monkeypatch):
    """Verify that an outdated trigger definition is dropped and recreated during _init_schema."""
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    sid = "test_migrate_svc"
    db_file = usage_log_db.db_path(sid)

    # 1. Seed the database with the tables and the OLD version of the trigger (lacking reconciliation condition)
    con = sqlite3.connect(db_file)
    try:
        con.execute(
            """CREATE TABLE usage_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                service_id TEXT,
                operation_class TEXT,
                operation_type TEXT,
                url TEXT,
                status TEXT,
                duration_ms REAL,
                function_name TEXT,
                process_context TEXT,
                bytes INTEGER,
                count INTEGER NOT NULL DEFAULT 1
            )"""
        )
        con.execute(
            """CREATE TABLE usage_log_hourly_summary (
                service_id TEXT NOT NULL,
                hour TEXT NOT NULL,
                operation_class TEXT NOT NULL DEFAULT '',
                operation_type TEXT NOT NULL DEFAULT '',
                count INTEGER NOT NULL DEFAULT 0,
                bytes INTEGER NOT NULL DEFAULT 0,
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (service_id, hour, operation_class, operation_type)
            )"""
        )
        # Create old, unconditional trigger
        con.execute(
            """CREATE TRIGGER trg_usage_log_summary_delete
            AFTER DELETE ON usage_log
            WHEN OLD.timestamp IS NOT NULL AND length(OLD.timestamp) >= 13 AND OLD.service_id IS NOT NULL
            BEGIN
                UPDATE usage_log_hourly_summary
                SET count = count - COALESCE(OLD.count, 1),
                    bytes = bytes - COALESCE(OLD.bytes, 0),
                    last_updated = datetime('now')
                WHERE service_id = OLD.service_id
                  AND hour = substr(OLD.timestamp, 1, 13)
                  AND operation_class = COALESCE(OLD.operation_class, '')
                  AND operation_type = COALESCE(OLD.operation_type, '');
            END"""
        )
        con.commit()
    finally:
        con.close()

    # Verify seed state
    con = sqlite3.connect(db_file)
    try:
        sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_usage_log_summary_delete'"
        ).fetchone()[0]
        assert "fastly.reconciliation" not in sql
    finally:
        con.close()

    # 2. Trigger pool initialization/schema-init
    # We clear _initialized so our schema-init callback is guaranteed to run
    usage_log_db._initialized.clear()
    con = usage_log_db.get_con(sid)
    try:
        # Verify that the trigger was dropped and recreated with the new condition
        sql = con.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_usage_log_summary_delete'"
        ).fetchone()[0]
        assert "fastly.reconciliation" in sql
    finally:
        usage_log_db.close_all_connections()


def test_small_cache_db_corruption_self_healing(tmp_path):
    """Verify that open_small_cache_db transparently self-heals corrupted SQLite files."""
    db_file = tmp_path / "corrupt_cache.db"
    ddl = "CREATE TABLE cache_t (id INTEGER PRIMARY KEY, key TEXT)"

    # 1. Corrupt the file with random garbage bytes
    db_file.write_bytes(b"THIS IS NOT A VALID SQLITE FILE - JUST RANDOM GARBAGE BYTES")

    # 2. Try opening the DB with our open_small_cache_db helper
    # It must detect the corruption, delete the bad files, and successfully recreate a clean DB
    con = open_small_cache_db(db_file, ddl=ddl, check_same_thread=True, timeout=1.0)
    try:
        # Verify the table exists and can be written to
        con.execute("INSERT INTO cache_t (key) VALUES ('test')")
        con.commit()
        rows = con.execute("SELECT key FROM cache_t").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "test"
    finally:
        con.close()


def test_usage_log_purging_and_trigger_restricton(tmp_path, monkeypatch):
    """Verify raw log deletes do not decrement hourly summary, but reconciliation deletes do."""
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    sid = "test_purge_svc"

    # Init DB
    usage_log_db._initialized.clear()
    con = usage_log_db.get_con(sid)
    try:
        # 1. Insert raw logs (function_name = 'api.sync' or NULL) and a reconciliation log row
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("2026-05-01T10:00:00Z", sid, "A", "CDN", 100, "api.sync"),
        )
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("2026-05-01T10:15:00Z", sid, "A", "CDN", 50, "fastly.reconciliation"),
        )
        con.commit()

        # Check summaries aggregated both (150 total)
        row = con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 150

        # 2. Delete raw log row (simulating retention purging)
        con.execute("DELETE FROM usage_log WHERE function_name = 'api.sync'")
        con.commit()

        # The summary MUST remain untouched (still 150)
        row = con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 150

        # 3. Delete the reconciliation row (simulating gap recompute)
        con.execute("DELETE FROM usage_log WHERE function_name = 'fastly.reconciliation'")
        con.commit()

        # The summary MUST be decremented by the reconciliation row count (150 - 50 = 100)
        row = con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 100
    finally:
        usage_log_db.close_all_connections()


def test_clear_usage_log_wipes_both_tables(tmp_path, monkeypatch):
    """Verify that clear_usage_log explicitly truncates both usage_log and usage_log_hourly_summary."""
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    sid = "test_clear_svc"

    usage_log_db._initialized.clear()
    con = usage_log_db.get_con(sid)
    try:
        con.execute(
            """INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("2026-05-01T10:00:00Z", sid, "A", "CDN", 100, "api.sync"),
        )
        con.commit()

        # Summaries aggregated
        row = con.execute("SELECT count FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()
        assert row and row[0] == 100
    finally:
        usage_log_db.close_all_connections()

    # Call clear_usage_log
    clear_usage_log(sid)

    # Connect again and verify both are empty
    con = usage_log_db.get_con(sid)
    try:
        assert con.execute("SELECT count(*) FROM usage_log").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM usage_log_hourly_summary").fetchone()[0] == 0
    finally:
        usage_log_db.close_all_connections()


def test_flush_releases_postgres_thread_connection_after_success(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)

    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-flush"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    con.executemany.assert_called_once()
    con.commit.assert_called_once_with()
    release.assert_called_once_with()


def test_flush_releases_connection_when_write_fails(monkeypatch):
    con = MagicMock()
    con.executemany.side_effect = RuntimeError("metadata unavailable")
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)

    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-flush-error"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()


def test_flush_releases_on_the_short_lived_worker_thread(monkeypatch):
    con = MagicMock()
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con)
    released_on = []

    def release():
        released_on.append(threading.current_thread())

    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-thread"] = [{"query_id": "q1"}]

    worker = threading.Thread(target=slow_queries._flush_all)
    worker.start()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert released_on == [worker]


def test_flush_without_connection_keeps_best_effort_behavior(monkeypatch):
    monkeypatch.setattr(
        slow_queries,
        "get_con",
        MagicMock(side_effect=RuntimeError("metadata unavailable")),
    )
    release = MagicMock()
    monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
    with slow_queries._buffer_lock:
        slow_queries._buffer["svc-no-connection"] = [{"query_id": "q1"}]

    slow_queries._flush_all()

    release.assert_called_once_with()
