"""Thread-local connection for the global share DB against PostgreSQL.

Under PostgreSQL standard mode, share_db tables live in the same database
as metadata tables and share the thread's connection.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from backend.core.metadata.pg_connection import (
    close_all_pg_connections,
    get_pg_thread_connection,
)

logger = logging.getLogger(__name__)

# ── Locations ────────────────────────────────────────────────────────────────

_DATA_DIR = "data/system"
_DB_FILENAME = "remote_share.db"


def db_path(_key: str | None = None) -> str:
    """Absolute path to the legacy global share DB file (compatibility helper)."""
    base = os.environ.get("REMOTE_SHARE_DB_DIR") or _DATA_DIR
    return os.path.join(base, _DB_FILENAME)


def get_global_share_con() -> Any:
    """Return a thread-local connection to the global share DB (PostgreSQL)."""
    return get_pg_thread_connection()


def close_all_connections() -> None:
    """Close every open connection in the pool."""
    close_all_pg_connections()


def reset_for_tests() -> None:
    """No-op under Postgres — test worker isolation is schema-based."""
    pass
