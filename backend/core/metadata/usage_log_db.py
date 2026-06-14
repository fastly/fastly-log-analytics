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
- :func:`migrate_from_metadata_db` — one-shot copy from the legacy
  metadata.db tables; idempotent, no-op when the destination already
  has rows.

Cross-table joins
-----------------
None — usage_log and usage_log_hourly_summary only reference each other,
and the existing SQL in :mod:`backend.core.metadata.usage_log` does not
join either to ``audit_logs`` / ``views`` / ``scoring_labels`` / etc.
The split is therefore self-contained.

The legacy table in metadata.db is left intact for one release as a
rollback backstop; readers and writers no longer touch it. The next
release can drop it.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading

from backend.core.metadata.base import _DATA_DIR, _SERVICE_ID_RE, InvalidServiceIdError

logger = logging.getLogger(__name__)

_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()
_all_connections: list[sqlite3.Connection] = []
_all_connections_lock = threading.Lock()


def db_path(service_id: str) -> str:
    """Absolute path to the per-service usage_log SQLite file.

    Same validation as :func:`backend.core.metadata.base.db_path` —
    rejects non-string / out-of-charset service_ids at the boundary so a
    bad caller can't silently spawn `<...0x...>.usage_log.db`.
    """
    if not isinstance(service_id, str):
        raise TypeError(f"service_id must be a string, got {type(service_id).__name__}: {service_id!r}")
    if not _SERVICE_ID_RE.match(service_id):
        raise InvalidServiceIdError(f"service_id must match {_SERVICE_ID_RE.pattern!r}; got {service_id!r}")
    return os.path.join(_DATA_DIR, f"{service_id}.usage_log.db")


def _connections() -> dict[str, sqlite3.Connection]:
    if not hasattr(_local, "usage_log_conns"):
        _local.usage_log_conns = {}
    return _local.usage_log_conns


def get_con(service_id: str) -> sqlite3.Connection:
    """Read-write thread-local connection to the per-service usage_log.db.

    Lazily creates the file + applies the schema on first use per
    (thread, service_id). Mirrors the lock/init pattern in
    :func:`backend.core.metadata.base.get_con` — :data:`_init_lock` is
    held across connect+PRAGMA so concurrent first-opens don't collide
    on ``PRAGMA journal_mode=WAL``.
    """
    pool = _connections()
    con = pool.get(service_id)
    if con is not None:
        return con

    path = db_path(service_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not _init_lock.acquire(timeout=10):
        raise sqlite3.OperationalError(
            f"usage_log_db._init_lock contended >10s for {service_id} — another thread is stuck inside connect+PRAGMA"
        )
    try:
        from backend.utils.sqlite_profiler import InstrumentedConnection

        con = sqlite3.connect(path, timeout=30.0, factory=InstrumentedConnection)
        # Stash service_id on the connection so the Live Query Monitor's
        # sqlite_profiler can surface it in the `service` column for the
        # dedicated per-service usage_log.db connections (same mechanism
        # as ``metadata.base.get_con`` — see that comment for context).
        # Without this, usage_log writes show up as `service: null` on
        # /admin/queries even though they target a specific service's db.
        con._service_id = service_id  # type: ignore[attr-defined]
        with _all_connections_lock:
            _all_connections.append(con)
        try:
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA foreign_keys=ON")
            # 64 MB page cache, matching base.py rationale. usage_log is
            # the largest hot table in this process (millions of rows on
            # active services); the SUM(CASE) aggregate over a 24h window
            # for the /admin/usage-log page pays for the cache header
            # multiple times over.
            con.execute("PRAGMA cache_size=-64000")
            con.execute("PRAGMA busy_timeout=30000")

            if path not in _initialized:
                _init_schema(con)
                _initialized.add(path)
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            raise
    finally:
        _init_lock.release()

    pool[service_id] = con
    return con


def open_readonly(service_id: str) -> sqlite3.Connection:
    """Open a short-lived read-only connection.

    The ``mode=ro`` URI guarantees the open call cannot acquire the
    writer lock — readers on this path can never block the cron writer
    even if they hold the connection for a long time. Caller is
    responsible for closing.

    File-must-exist semantics: ``mode=ro`` raises ``OperationalError``
    when the file isn't there yet. Callers should treat that as "no
    rows yet" and return an empty result (the writer creates the file
    on first ``log_usage_calls`` call).
    """
    path = db_path(service_id)
    uri = f"file:{path}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    return con


def close_all_connections() -> None:
    with _all_connections_lock:
        for con in _all_connections:
            try:
                con.close()
            except Exception:
                pass
        _all_connections.clear()


def teardown(service_id: str) -> None:
    """Close any thread-local connection and delete the file + WAL siblings."""
    pool = _connections()
    con = pool.pop(service_id, None)
    if con is not None:
        try:
            con.close()
        except Exception:
            pass

    path = db_path(service_id)
    _initialized.discard(path)
    for suffix in ("", "-wal", "-shm", "-journal"):
        target = path + suffix
        try:
            if os.path.exists(target):
                os.remove(target)
        except OSError as e:
            logger.debug("[usage_log_db] could not remove %s: %s", target, e)


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
    "CREATE INDEX IF NOT EXISTS idx_usage_reconcile ON usage_log(service_id, operation_class, timestamp)",
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
]


def _init_schema(con: sqlite3.Connection) -> None:
    for stmt in _SCHEMA:
        con.execute(stmt)
    con.commit()
