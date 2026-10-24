"""Connection facade for the FOS/CDN ``usage_log`` tables, plus their DDL.

``usage_log`` and its ``usage_log_hourly_summary`` rollup once lived in a dedicated
per-service SQLite file so the cron writer's hot path never contended with admin
readers on ``metadata.db``. Metadata is Postgres-only now (ADR-22): every function
below hands out the same pooled Postgres connections as
:mod:`backend.core.metadata.base`, and rows are scoped by their ``service_id`` column.

What is still load-bearing here:

- :func:`get_con` / :func:`open_readonly` / :func:`close_all_connections` — the
  import surface ``usage_log``, ``ingest_log``, ``reconciliation`` and telemetry
  call (and tests monkeypatch).
- :data:`_SCHEMA` — the table/index definitions. ``pg_schema.pg_schema_statements``
  translates them to Postgres DDL (and drops the SQLite triggers, whose rollup
  maintenance the Postgres write path does itself), so this list IS the source of
  the ``usage_log`` tables; deleting it would drop them from the bootstrap.
- :func:`db_path` — the legacy file path, kept for the contract-backend script.
"""

from __future__ import annotations

import logging
import os

from backend.core.metadata.base import (
    _DATA_DIR,
    _validate_service_id_or_raise,
    close_all_connections,
    get_con,
)
from backend.core.metadata.base import (
    get_con_readonly as open_readonly,
)

__all__ = [
    "_SCHEMA",
    "close_all_connections",
    "db_path",
    "get_con",
    "open_readonly",
]

logger = logging.getLogger(__name__)


def db_path(service_id: str) -> str:
    """Absolute path to the per-service usage_log SQLite file (legacy helper)."""
    _validate_service_id_or_raise(service_id)
    return os.path.join(_DATA_DIR, f"{service_id}.usage_log.db")


# ── Schema ────────────────────────────────────────────────────────────────────
# SQLite-dialect table / index / trigger definitions; ``pg_schema`` rewrites them to
# Postgres DDL (see the module docstring).

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
    "CREATE INDEX IF NOT EXISTS idx_telemetry_queries_ts ON telemetry_queries (timestamp)",
    """CREATE TABLE IF NOT EXISTS telemetry_sections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        page_load_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        section_name TEXT NOT NULL,
        duration_ms REAL NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_sections_page ON telemetry_sections (page_load_id)",
    "CREATE INDEX IF NOT EXISTS idx_telemetry_sections_ts ON telemetry_sections (timestamp)",
]
