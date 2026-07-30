"""Unit tests for the usage_log_db index self-healing and covering index migration.

Covers:
- Detects old, non-covering idx_usage_reconcile index.
- Drops and re-creates it as a fully covering index.
- Preserves existing data in usage_log during the migration.
"""

from __future__ import annotations

import os
import sqlite3

from backend.core.metadata import usage_log_db


def test_index_upgrade_drops_and_recreates_covering_index(tmp_path):
    # 1. Create a temp SQLite file and initialize it with the old non-covering schema
    db_file = os.path.join(tmp_path, "test_upgrade.db")
    con = sqlite3.connect(db_file)

    # Core table
    con.execute("""
        CREATE TABLE usage_log (
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
        )
    """)

    # Old non-covering index
    con.execute("""
        CREATE INDEX idx_usage_reconcile ON usage_log(service_id, operation_class, timestamp)
    """)

    # Seed some dummy data to ensure data is preserved during migration
    con.execute("""
        INSERT INTO usage_log (service_id, operation_class, timestamp, function_name, count)
        VALUES ('svc-test', 'A', '2026-07-22T15:00:00Z', 'fastly.edge', 10)
    """)
    con.commit()

    # Verify the old index has exactly 3 columns
    cols_before = [row[2] for row in con.execute("PRAGMA index_info('idx_usage_reconcile')").fetchall()]
    assert cols_before == ["service_id", "operation_class", "timestamp"]

    # 2. Run the schema initializer to trigger the self-healing migration
    usage_log_db._init_schema(con)

    # 3. Verify the data is preserved
    rows = con.execute("SELECT service_id, count FROM usage_log").fetchall()
    assert len(rows) == 1
    assert rows[0] == ("svc-test", 10)

    # 4. Verify the index was upgraded to a covering index (now has 5 columns)
    cols_after = [row[2] for row in con.execute("PRAGMA index_info('idx_usage_reconcile')").fetchall()]
    assert cols_after == ["service_id", "operation_class", "timestamp", "function_name", "count"]

    con.close()
