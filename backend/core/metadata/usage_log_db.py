"""Dedicated per-service SQLite file for the FOS/CDN usage_log table.

This file used to live as a table inside ``data/services/<sid>.metadata.db``
alongside ``audit_logs``, ``views``, ``scoring_labels``, etc. Per the
2026-06-11 perf audit, the cron_sync writer's hot path holds the
metadata.db WAL writer lock for ~3 s per tick (200+ FOS/CDN rows × per-
row AFTER trigger maintaining ``usage_log_hourly_summary``), and during
that window every admin-page reader on the SAME metadata.db blocked on
``SQLITE_BUSY``-with-30s-timeout retries → 6-56× endpoint slowdown.

WAL allows concurrent readers + one writer, but only *within a single
database file*. Moving the high-write surface (``usage_log`` + its
``usage_log_hourly_summary`` rollup + the 3 triggers wiring them
together) into its own SQLite file means the cron writer can churn
freely without ever touching the lock the admin endpoints care about.

Mirrors the pattern in ``backend/utils/rdns_cache.py`` and
``backend/utils/ngwaf_bot_cache.py`` — both already use dedicated
SQLite files with the same WAL / NORMAL / cache_size pragmas, and both
already expose a read-only open helper so reader paths never contend
with the writer.

Public surface
--------------
- :func:`get_con` — read-write thread-local connection (write path).
- :func:`open_readonly` — short-lived read-only connection per call;
  used by the request hot path. URI ``mode=ro`` means the open call
  cannot acquire the writer lock under any circumstances, so a slow
  reader can never block a cron commit.
- :func:`teardown` — drop the file + WAL/SHM siblings.
- :func:`close_all_connections` — pytest fixture support.

Cross-table joins
-----------------
None — usage_log and usage_log_hourly_summary only reference each other,
and the existing SQL in :mod:`backend.core.metadata.usage_log` does not
join either to ``audit_logs`` / ``views`` / ``scoring_labels`` / etc.
The split is therefore self-contained.
"""

from __future__ import annotations

import logging
import os
import threading

from backend.core.metadata import pg_connection
from backend.core.metadata.base import _DATA_DIR, _validate_service_id_or_raise

logger = logging.getLogger(__name__)

# Kept as module-level attributes for backward compatibility with tests/conftest.py
_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()


def db_path(service_id: str) -> str:
    """Absolute path to the per-service usage_log SQLite file (legacy helper)."""
    _validate_service_id_or_raise(service_id)
    return os.path.join(_DATA_DIR, f"{service_id}.usage_log.db")


def get_con(service_id: str) -> pg_connection.PgConnectionWrapper:
    """Read-write thread-local connection to the metadata Postgres database."""
    _validate_service_id_or_raise(service_id)
    wrapper = pg_connection.get_pg_thread_connection()
    wrapper._service_id = service_id
    return wrapper


def open_readonly(service_id: str) -> pg_connection.PgConnectionWrapper:
    """Open a short-lived read-only connection to the metadata Postgres database."""
    _validate_service_id_or_raise(service_id)
    wrapper = pg_connection.get_pg_readonly_connection()
    wrapper._service_id = service_id
    return wrapper


def release_thread_connection() -> None:
    """Release the thread connection back to the pool."""
    pg_connection.release_pg_thread_connection()


def close_all_connections() -> None:
    """Close all open Postgres connections in the pool."""
    pg_connection.close_all_pg_connections()


def teardown(service_id: str) -> None:
    """No-op under Postgres — rows are scoped by service_id."""
    _validate_service_id_or_raise(service_id)


# ── Schema ────────────────────────────────────────────────────────────────────
# Exact copies of the table / index / trigger definitions that used to
# live in backend.core.metadata.base._SCHEMA. Kept identical so the
# migration is a row-copy with no schema translation.

_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS usage_log (
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
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_timestamp ON usage_log(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_usage_dedup ON usage_log(service_id, function_name, url)",
    "CREATE INDEX IF NOT EXISTS idx_usage_reconcile ON usage_log(service_id, operation_class, timestamp, function_name, count)",
    "CREATE INDEX IF NOT EXISTS idx_usage_process_context_ts ON usage_log(process_context, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_usage_service_ts ON usage_log(service_id, timestamp, operation_class, count, bytes)",
    """CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (
        service_id TEXT NOT NULL,
        hour TEXT NOT NULL,
        operation_class TEXT NOT NULL DEFAULT '',
        operation_type TEXT NOT NULL DEFAULT '',
        count INTEGER NOT NULL DEFAULT 0,
        bytes INTEGER NOT NULL DEFAULT 0,
        last_updated TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (service_id, hour, operation_class, operation_type)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_hourly_svc_hour ON usage_log_hourly_summary(service_id, hour)",
    """CREATE TRIGGER IF NOT EXISTS trg_usage_log_summary_insert
    AFTER INSERT ON usage_log
    WHEN NEW.timestamp IS NOT NULL AND length(NEW.timestamp) >= 13 AND NEW.service_id IS NOT NULL
    BEGIN
        INSERT INTO usage_log_hourly_summary
            (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
        VALUES (NEW.service_id, substr(NEW.timestamp, 1, 13),
                COALESCE(NEW.operation_class, ''), COALESCE(NEW.operation_type, ''),
                COALESCE(NEW.count, 1), COALESCE(NEW.bytes, 0), datetime('now'))
        ON CONFLICT(service_id, hour, operation_class, operation_type)
        DO UPDATE SET count = count + excluded.count,
                      bytes = bytes + excluded.bytes,
                      last_updated = excluded.last_updated;
    END""",
    """CREATE TRIGGER IF NOT EXISTS trg_usage_log_summary_delete
    AFTER DELETE ON usage_log
    WHEN OLD.timestamp IS NOT NULL AND length(OLD.timestamp) >= 13 AND OLD.service_id IS NOT NULL
      AND OLD.function_name = 'fastly.reconciliation'
    BEGIN
        UPDATE usage_log_hourly_summary
        SET count = count - COALESCE(OLD.count, 1),
            bytes = bytes - COALESCE(OLD.bytes, 0),
            last_updated = datetime('now')
        WHERE service_id = OLD.service_id
          AND hour = substr(OLD.timestamp, 1, 13)
          AND operation_class = COALESCE(OLD.operation_class, '')
          AND operation_type = COALESCE(OLD.operation_type, '');
    END""",
    """CREATE TRIGGER IF NOT EXISTS trg_usage_log_summary_update
    AFTER UPDATE ON usage_log
    WHEN NEW.timestamp IS NOT NULL AND length(NEW.timestamp) >= 13 AND NEW.service_id IS NOT NULL
      AND (OLD.count IS NOT NEW.count OR OLD.bytes IS NOT NEW.bytes
           OR OLD.timestamp IS NOT NEW.timestamp
           OR OLD.operation_class IS NOT NEW.operation_class
           OR OLD.operation_type IS NOT NEW.operation_type
           OR OLD.service_id IS NOT NEW.service_id)
    BEGIN
        UPDATE usage_log_hourly_summary
        SET count = count - COALESCE(OLD.count, 1),
            bytes = bytes - COALESCE(OLD.bytes, 0),
            last_updated = datetime('now')
        WHERE service_id = OLD.service_id
          AND hour = substr(OLD.timestamp, 1, 13)
          AND operation_class = COALESCE(OLD.operation_class, '')
          AND operation_type = COALESCE(OLD.operation_type, '');
        INSERT INTO usage_log_hourly_summary
            (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
        VALUES (NEW.service_id, substr(NEW.timestamp, 1, 13),
                COALESCE(NEW.operation_class, ''), COALESCE(NEW.operation_type, ''),
                COALESCE(NEW.count, 1), COALESCE(NEW.bytes, 0), datetime('now'))
        ON CONFLICT(service_id, hour, operation_class, operation_type)
        DO UPDATE SET count = count + excluded.count,
                      bytes = bytes + excluded.bytes,
                      last_updated = excluded.last_updated;
    END""",
    """CREATE TABLE IF NOT EXISTS telemetry_queries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        page_load_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        engine TEXT NOT NULL,
        sql_query TEXT NOT NULL,
        duration_ms REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_queries_page ON telemetry_queries (page_load_id)",
    """CREATE TABLE IF NOT EXISTS telemetry_sections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        page_load_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        section_name TEXT NOT NULL,
        duration_ms REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_sections_page ON telemetry_sections (page_load_id)",
]
